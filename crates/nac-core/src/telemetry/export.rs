use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use super::{
    Observation, RuntimeMetadata, TelemetryEvent, JSON_LINE_PREFIX, MAX_EXPORT_QUEUE_CAPACITY,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryExportError;

pub trait TelemetryExporter: Send + Sync + 'static {
    fn export(&self, event: &TelemetryEvent) -> Result<(), TelemetryExportError>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExportStats {
    pub accepted: u64,
    pub dropped: u64,
    pub exported: u64,
    pub failures: u64,
}

#[derive(Default)]
struct AtomicExportStats {
    accepted: AtomicU64,
    dropped: AtomicU64,
    exported: AtomicU64,
    failures: AtomicU64,
}

#[derive(Clone)]
pub struct TelemetryRecorder {
    sender: Option<mpsc::SyncSender<Observation>>,
    stats: Arc<AtomicExportStats>,
}

impl TelemetryRecorder {
    pub fn disabled() -> Self {
        Self {
            sender: None,
            stats: Arc::new(AtomicExportStats::default()),
        }
    }

    pub fn bounded(
        exporter: Arc<dyn TelemetryExporter>,
        runtime: RuntimeMetadata,
        capacity: usize,
    ) -> Self {
        let capacity = capacity.clamp(1, MAX_EXPORT_QUEUE_CAPACITY);
        let (sender, receiver) = mpsc::sync_channel::<Observation>(capacity);
        let stats = Arc::new(AtomicExportStats::default());
        let worker_stats = Arc::clone(&stats);
        let _ = std::thread::Builder::new()
            .name("nac-telemetry-export".to_string())
            .spawn(move || {
                while let Ok(observation) = receiver.recv() {
                    let event = observation.into_event(runtime.clone());
                    if exporter.export(&event).is_ok() {
                        worker_stats.exported.fetch_add(1, Ordering::Relaxed);
                    } else {
                        worker_stats.failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        Self {
            sender: Some(sender),
            stats,
        }
    }

    pub(super) fn is_enabled(&self) -> bool {
        self.sender.is_some()
    }

    pub(super) fn record(&self, observation: Observation) {
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        match sender.try_send(observation) {
            Ok(()) => {
                self.stats.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_)) => {
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn stats(&self) -> ExportStats {
        ExportStats {
            accepted: self.stats.accepted.load(Ordering::Relaxed),
            dropped: self.stats.dropped.load(Ordering::Relaxed),
            exported: self.stats.exported.load(Ordering::Relaxed),
            failures: self.stats.failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
pub struct InMemoryExporter {
    events: Mutex<Vec<TelemetryEvent>>,
}

impl InMemoryExporter {
    pub fn events(&self) -> Vec<TelemetryEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl TelemetryExporter for InMemoryExporter {
    fn export(&self, event: &TelemetryEvent) -> Result<(), TelemetryExportError> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
        Ok(())
    }
}

struct JsonStderrExporter;

impl TelemetryExporter for JsonStderrExporter {
    fn export(&self, event: &TelemetryEvent) -> Result<(), TelemetryExportError> {
        let line = serde_json::to_vec(event).map_err(|_| TelemetryExportError)?;
        let mut stderr = std::io::stderr().lock();
        stderr
            .write_all(JSON_LINE_PREFIX.as_bytes())
            .map_err(|_| TelemetryExportError)?;
        stderr.write_all(&line).map_err(|_| TelemetryExportError)?;
        stderr.write_all(b"\n").map_err(|_| TelemetryExportError)
    }
}

pub(super) fn stderr_recorder(runtime: RuntimeMetadata, capacity: usize) -> TelemetryRecorder {
    TelemetryRecorder::bounded(Arc::new(JsonStderrExporter), runtime, capacity)
}
