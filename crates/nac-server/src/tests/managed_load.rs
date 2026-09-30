use super::*;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;

const DEFAULT_SEED: u64 = 0xA11_0112;
const VARIANTS: [usize; 3] = [1, 2, 4];
const PHASE_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PlannedOrchestrator {
    ordinal: usize,
    session_id: String,
    description: String,
    prompt: String,
    worker_thread: String,
    worker_action: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct LogicalPlan {
    seed: u64,
    orchestrators: Vec<PlannedOrchestrator>,
}

impl LogicalPlan {
    fn new(seed: u64, orchestrator_count: usize) -> Self {
        let orchestrators = (0..orchestrator_count)
            .map(|ordinal| PlannedOrchestrator {
                ordinal,
                session_id: format!("all112-{orchestrator_count}-{seed:08x}-{ordinal}"),
                description: fixed_payload(
                    &format!("ALL112_DESCRIPTION:{orchestrator_count}:{seed:08x}:{ordinal}"),
                    96,
                ),
                prompt: fixed_payload(
                    &format!(
                        "ALL112_ORCHESTRATOR_PROMPT:{orchestrator_count}:{seed:08x}:{ordinal}"
                    ),
                    256,
                ),
                worker_thread: format!("worker-{ordinal}"),
                worker_action: fixed_payload(
                    &format!("ALL112_WORKER_ACTION:{orchestrator_count}:{seed:08x}:{ordinal}"),
                    192,
                ),
            })
            .collect();
        Self {
            seed,
            orchestrators,
        }
    }
}

fn fixed_payload(prefix: &str, bytes: usize) -> String {
    assert!(prefix.len() <= bytes);
    let mut value = prefix.to_string();
    value.extend(std::iter::repeat_n('x', bytes - prefix.len()));
    value
}

struct PhaseGate {
    expected: usize,
    state: Mutex<PhaseGateState>,
    ready: Condvar,
}

struct PhaseGateState {
    arrived: usize,
    released: bool,
}

impl PhaseGate {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            state: Mutex::new(PhaseGateState {
                arrived: 0,
                released: false,
            }),
            ready: Condvar::new(),
        }
    }

    fn arrive_and_wait(&self, label: &str) {
        let deadline = Instant::now() + PHASE_TIMEOUT;
        let mut state = self.state.lock().unwrap();
        state.arrived += 1;
        self.ready.notify_all();
        while !state.released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "timed out at {label} barrier");
            let (next, timeout) = self.ready.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(!timeout.timed_out(), "timed out at {label} barrier");
        }
    }

    fn wait_until_ready(&self, label: &str) {
        let deadline = Instant::now() + PHASE_TIMEOUT;
        let mut state = self.state.lock().unwrap();
        while state.arrived < self.expected {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "timed out waiting for {label}");
            let (next, timeout) = self.ready.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(!timeout.timed_out(), "timed out waiting for {label}");
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = true;
        self.ready.notify_all();
    }
}

#[derive(Clone, Debug, Serialize)]
struct ModelRequest {
    phase: &'static str,
    ordinal: Option<usize>,
}

#[derive(Clone, Debug, Serialize)]
struct ProbeSample {
    phase: &'static str,
    route: &'static str,
    status: u16,
    latency_us: u128,
}

struct DeterministicModel {
    base_url: String,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    initial_gate: Arc<PhaseGate>,
    worker_gate: Arc<PhaseGate>,
    handle: thread::JoinHandle<()>,
}

impl DeterministicModel {
    fn start(plan: &LogicalPlan) -> Self {
        Self::start_with_request_count(plan, plan.orchestrators.len() * 4)
    }

    fn start_with_request_count(plan: &LogicalPlan, request_count: usize) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind deterministic model");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let expected = plan.orchestrators.len();
        let initial_gate = Arc::new(PhaseGate::new(expected));
        let worker_gate = Arc::new(PhaseGate::new(expected));
        let initial_gate_for_server = Arc::clone(&initial_gate);
        let worker_gate_for_server = Arc::clone(&worker_gate);
        let requests = Arc::new(Mutex::new(Vec::with_capacity(expected * 4)));
        let requests_for_thread = Arc::clone(&requests);
        let plan = plan.clone();
        let handle = thread::spawn(move || {
            let mut handlers = Vec::with_capacity(request_count);
            for _ in 0..request_count {
                let (stream, _) = listener
                    .accept()
                    .expect("accept deterministic model request");
                let plan = plan.clone();
                let initial_gate = Arc::clone(&initial_gate_for_server);
                let worker_gate = Arc::clone(&worker_gate_for_server);
                let requests = Arc::clone(&requests_for_thread);
                handlers.push(thread::spawn(move || {
                    handle_model_request(stream, &plan, &initial_gate, &worker_gate, &requests)
                }));
            }
            for handler in handlers {
                handler.join().expect("deterministic model handler");
            }
        });
        Self {
            base_url,
            requests,
            initial_gate,
            worker_gate,
            handle,
        }
    }

    async fn wait_for_initial_requests(&self) {
        let gate = Arc::clone(&self.initial_gate);
        tokio::task::spawn_blocking(move || gate.wait_until_ready("orchestrator-start requests"))
            .await
            .unwrap();
    }

    fn release_initial_requests(&self) {
        self.initial_gate.release();
    }

    async fn wait_for_worker_requests(&self) {
        let gate = Arc::clone(&self.worker_gate);
        tokio::task::spawn_blocking(move || gate.wait_until_ready("worker requests"))
            .await
            .unwrap();
    }

    fn release_worker_requests(&self) {
        self.worker_gate.release();
    }

    fn finish(self) -> Vec<ModelRequest> {
        self.handle.join().expect("deterministic model server");
        Arc::try_unwrap(self.requests)
            .expect("model request log still shared")
            .into_inner()
            .unwrap()
    }
}

fn handle_model_request(
    mut stream: TcpStream,
    plan: &LogicalPlan,
    initial_gate: &PhaseGate,
    worker_gate: &PhaseGate,
    requests: &Mutex<Vec<ModelRequest>>,
) {
    let body = read_http_body(&mut stream);
    let body_text = String::from_utf8_lossy(&body);
    let orchestrator_ordinal = plan.orchestrators.iter().find_map(|entry| {
        body_text
            .contains(&format!(
                "ALL112_ORCHESTRATOR_PROMPT:{}:{:08x}:{}",
                plan.orchestrators.len(),
                plan.seed,
                entry.ordinal
            ))
            .then_some(entry.ordinal)
    });
    let worker_ordinal = plan.orchestrators.iter().find_map(|entry| {
        body_text
            .contains(&format!(
                "ALL112_WORKER_ACTION:{}:{:08x}:{}",
                plan.orchestrators.len(),
                plan.seed,
                entry.ordinal
            ))
            .then_some(entry.ordinal)
    });

    let (phase, ordinal, response) = if body_text.contains("function_call_output") {
        let ordinal = orchestrator_ordinal.expect("resumed orchestrator request has an ordinal");
        (
            "orchestrator-final",
            Some(ordinal),
            text_response(&fixed_payload(
                &format!("ALL112_ORCHESTRATOR_REPORT:{ordinal}"),
                160,
            )),
        )
    } else if body_text.contains("ALL112_WORKER_ACTION:") {
        let ordinal = worker_ordinal.expect("worker request has an ordinal");
        worker_gate.arrive_and_wait("worker");
        (
            "worker",
            Some(ordinal),
            text_response(&fixed_payload(
                &format!("ALL112_WORKER_RESULT:{ordinal}"),
                128,
            )),
        )
    } else if body_text.contains("ALL112_ORCHESTRATOR_PROMPT:") {
        let ordinal = orchestrator_ordinal.expect("orchestrator request has an ordinal");
        initial_gate.arrive_and_wait("orchestrator-start");
        let entry = &plan.orchestrators[ordinal];
        (
            "orchestrator-start",
            Some(ordinal),
            tool_response(
                &format!("all112-call-{ordinal}"),
                "thread",
                &serde_json::json!({
                    "name": entry.worker_thread,
                    "action": entry.worker_action,
                })
                .to_string(),
            ),
        )
    } else {
        (
            "parent-completion",
            None,
            text_response(&fixed_payload("ALL112_PARENT_ACK", 96)),
        )
    };
    requests
        .lock()
        .unwrap()
        .push(ModelRequest { phase, ordinal });
    write_http_json(&mut stream, &response);
}

fn read_http_body(stream: &mut TcpStream) -> Vec<u8> {
    stream
        .set_read_timeout(Some(PHASE_TIMEOUT))
        .expect("set request read timeout");
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream
            .read(&mut buffer)
            .expect("read deterministic request");
        assert!(read > 0, "model request ended before headers");
        request.extend_from_slice(&buffer[..read]);
        if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        assert!(
            request.len() < 128 * 1024,
            "model request headers too large"
        );
    };
    let headers = String::from_utf8_lossy(&request[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        })
        .expect("model request content-length");
    while request.len() - header_end < content_length {
        let read = stream.read(&mut buffer).expect("read deterministic body");
        assert!(read > 0, "model request ended before body");
        request.extend_from_slice(&buffer[..read]);
    }
    request[header_end..header_end + content_length].to_vec()
}

fn text_response(text: &str) -> String {
    serde_json::json!({
        "status": "completed",
        "output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}],
        "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}
    })
    .to_string()
}

fn tool_response(call_id: &str, name: &str, arguments: &str) -> String {
    serde_json::json!({
        "status": "completed",
        "output": [{
            "type": "function_call",
            "call_id": call_id,
            "name": name,
            "arguments": arguments,
        }],
        "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}
    })
    .to_string()
}

fn write_http_json(stream: &mut TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}

trait LoadStoreAdapter {
    fn identity(&self) -> &'static str;
    fn create_manager(&self, root: &Path, worker: &Path) -> SessionManager;
    fn assert_integrity(&self, store_path: &Path);
    fn configuration(&self, store_path: &Path) -> StoreConfiguration;
    fn checkpoint(&self, store_path: &Path) -> CheckpointEvidence;
}

struct SqliteLoadStore;

impl LoadStoreAdapter for SqliteLoadStore {
    fn identity(&self) -> &'static str {
        "sqlite-wal"
    }

    fn create_manager(&self, root: &Path, worker: &Path) -> SessionManager {
        SessionManager::new_unowned_fixture(ServerOptions {
            root_cwd: root.to_path_buf(),
            store_path: Some(root.join("store.db")),
            worker_executable: Some(worker.to_path_buf()),
            managed_host: None,
        })
        .expect("managed-load session manager")
    }

    fn assert_integrity(&self, store_path: &Path) {
        let connection = rusqlite::Connection::open(store_path).unwrap();
        let quick_check: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(quick_check, "ok");
        let foreign_key_errors: i64 = connection
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(foreign_key_errors, 0);
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    }

    fn configuration(&self, store_path: &Path) -> StoreConfiguration {
        let connection = rusqlite::Connection::open(store_path).unwrap();
        StoreConfiguration {
            engine: "sqlite",
            journal_mode: connection
                .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap(),
            synchronous: connection
                .query_row("PRAGMA synchronous", [], |row| row.get(0))
                .unwrap(),
            mmap_size: connection
                .query_row("PRAGMA mmap_size", [], |row| row.get(0))
                .unwrap(),
            schema_version: connection
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap(),
            page_count: connection
                .query_row("PRAGMA page_count", [], |row| row.get(0))
                .unwrap(),
            page_size: connection
                .query_row("PRAGMA page_size", [], |row| row.get(0))
                .unwrap(),
            database_bytes: file_size(store_path),
            wal_bytes: file_size(&PathBuf::from(format!("{}-wal", store_path.display()))),
            shm_bytes: file_size(&PathBuf::from(format!("{}-shm", store_path.display()))),
        }
    }

    fn checkpoint(&self, store_path: &Path) -> CheckpointEvidence {
        let connection = rusqlite::Connection::open(store_path).unwrap();
        let started = Instant::now();
        let (busy, log_frames, checkpointed_frames) = connection
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        let duration = started.elapsed();
        assert_eq!(busy, 0, "passive checkpoint must not remain busy");
        nac_core::telemetry::emit_store_duration(
            nac_core::telemetry::StoreOperation::Checkpoint,
            nac_core::telemetry::Correlation::default(),
            duration,
            nac_core::telemetry::TelemetryOutcome::Ok,
            None,
        );
        CheckpointEvidence {
            busy,
            log_frames,
            checkpointed_frames,
            duration_us: duration.as_micros(),
        }
    }
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |metadata| metadata.len())
}

#[derive(Serialize)]
struct StoreConfiguration {
    engine: &'static str,
    journal_mode: String,
    synchronous: i64,
    mmap_size: i64,
    schema_version: i64,
    page_count: i64,
    page_size: i64,
    database_bytes: u64,
    wal_bytes: u64,
    shm_bytes: u64,
}

#[derive(Serialize)]
struct CheckpointEvidence {
    busy: i64,
    log_frames: i64,
    checkpointed_frames: i64,
    duration_us: u128,
}

#[derive(Default, Serialize)]
struct LatencyDistribution {
    count: usize,
    min: u64,
    p50: u64,
    p95: u64,
    max: u64,
}

impl LatencyDistribution {
    fn from_values(mut values: Vec<u64>) -> Self {
        if values.is_empty() {
            return Self::default();
        }
        values.sort_unstable();
        let percentile = |numerator: usize| {
            let index = (values.len().saturating_sub(1) * numerator) / 100;
            values[index]
        };
        Self {
            count: values.len(),
            min: values[0],
            p50: percentile(50),
            p95: percentile(95),
            max: *values.last().unwrap(),
        }
    }
}

#[derive(Serialize)]
struct TelemetryEvidence {
    store_latency_us: BTreeMap<String, LatencyDistribution>,
    api_latency_us: BTreeMap<String, LatencyDistribution>,
    max_connection_active: u64,
    max_persistence_queue_active: u64,
    max_orchestrators_active: u64,
    max_child_processes_active: u64,
    child_process_started: BTreeSet<u32>,
    child_process_stopped: BTreeSet<u32>,
    max_cpu_time_us: u64,
    max_resident_memory_bytes: u64,
    accepted: u64,
    dropped: u64,
    exported: u64,
    failures: u64,
}

#[derive(Serialize)]
struct VariantEvidence {
    seed: u64,
    orchestrators: usize,
    store: &'static str,
    elapsed_ms: u128,
    injected_phase_delay_ms: u128,
    logical_plan: LogicalPlan,
    model_requests: Vec<ModelRequest>,
    probe_samples: Vec<ProbeSample>,
    transcript_counts: Vec<usize>,
    event_counts: Vec<usize>,
    worker_episode_counts: Vec<usize>,
    completion_inbox_count: usize,
    build_id: &'static str,
    source_revision: &'static str,
    store_configuration: StoreConfiguration,
    checkpoint: CheckpointEvidence,
    telemetry: TelemetryEvidence,
}

#[derive(Serialize)]
struct BusyConflictEvidence {
    held_ms: u128,
    blocked_while_held: bool,
    append_wait_ms: u128,
}

#[derive(Serialize)]
struct AppendFailureEvidence {
    terminal_status: ManagedOrchestratorStatus,
    transcript_rows: usize,
    completion_deliveries: usize,
    recovery_cleared: bool,
}

#[derive(Serialize)]
struct MonitorFailureEvidence {
    status_before_recovery: ManagedOrchestratorStatus,
    status_after_recovery: ManagedOrchestratorStatus,
    completion_deliveries: usize,
}

#[derive(Serialize)]
struct WorkerInterruptionEvidence {
    terminal_status: ManagedOrchestratorStatus,
    child_process_started: BTreeSet<u32>,
    child_process_stopped: BTreeSet<u32>,
    retained_dispatch_status: String,
}

#[derive(Serialize)]
struct RestartRecoveryEvidence {
    terminal_status: ManagedOrchestratorStatus,
    completion_deliveries: usize,
    recovery_status: String,
    child_has_active_operation: bool,
}

#[derive(Serialize)]
struct FaultEvidence {
    busy_conflict: BusyConflictEvidence,
    append_failure: AppendFailureEvidence,
    monitor_failure: MonitorFailureEvidence,
    worker_interruption: WorkerInterruptionEvidence,
    host_interruption_restart: RestartRecoveryEvidence,
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "bounded repo-level scenario; run with make test-managed-load"]
async fn managed_load_scenario() {
    let _env_lock = SERVER_MODEL_ENV_LOCK.lock().unwrap();
    let worker = PathBuf::from(
        std::env::var_os("NAC_MANAGED_LOAD_WORKER")
            .expect("NAC_MANAGED_LOAD_WORKER is set by make test-managed-load"),
    );
    assert!(
        worker.is_file(),
        "worker binary does not exist: {}",
        worker.display()
    );
    let seed = std::env::var("NAC_MANAGED_LOAD_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_SEED);
    let adapter = SqliteLoadStore;
    let artifact_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/managed-load");
    std::fs::create_dir_all(&artifact_root).unwrap();

    for count in VARIANTS {
        let evidence = run_variant(&adapter, &worker, seed, count, Duration::ZERO).await;
        let artifact =
            artifact_root.join(format!("all-112-seed-{seed}-orchestrators-{count}.json"));
        write_secret_safe_artifact(&artifact, &evidence);
        eprintln!("ALL-112 artifact: {}", artifact.display());
    }
    let slow_seed = seed ^ 0x510;
    let slow_evidence =
        run_variant(&adapter, &worker, slow_seed, 1, Duration::from_millis(100)).await;
    let slow_artifact = artifact_root.join(format!("all-112-seed-{slow_seed}-fault-slow-io.json"));
    write_secret_safe_artifact(&slow_artifact, &slow_evidence);
    eprintln!("ALL-112 artifact: {}", slow_artifact.display());

    let fault_evidence = FaultEvidence {
        busy_conflict: exercise_busy_conflict(&adapter, seed),
        append_failure: exercise_append_failure(&adapter, &worker, seed).await,
        monitor_failure: exercise_monitor_failure(&adapter, &worker, seed).await,
        worker_interruption: exercise_worker_interruption(&adapter, &worker, seed).await,
        host_interruption_restart: exercise_restart_recovery(&adapter, &worker, seed).await,
    };
    let fault_artifact = artifact_root.join(format!("all-112-seed-{seed}-faults.json"));
    write_secret_safe_artifact(&fault_artifact, &fault_evidence);
    eprintln!("ALL-112 artifact: {}", fault_artifact.display());
}

fn exercise_busy_conflict(adapter: &dyn LoadStoreAdapter, seed: u64) -> BusyConflictEvidence {
    let root = temp_root(&format!("managed_load_busy_{seed}"));
    seed_load_parent(&root, "https://api.openai.com/v1".to_string());
    let store_path = root.join("store.db");
    let blocker = rusqlite::Connection::open(&store_path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (result_sender, result_receiver) = std::sync::mpsc::channel();
    let (ready_sender, ready_receiver) = std::sync::mpsc::channel();
    let contender_path = store_path.clone();
    let contender = thread::spawn(move || {
        let started = Instant::now();
        ready_sender.send(()).unwrap();
        let result = nac_core::store::append_thread_event(
            &contender_path,
            "all112-parent",
            "busy-conflict",
            r#"{"type":"all112_busy_conflict"}"#,
        );
        result_sender.send((started.elapsed(), result)).unwrap();
    });
    ready_receiver.recv_timeout(PHASE_TIMEOUT).unwrap();
    let held = Duration::from_millis(75);
    thread::sleep(held);
    let blocked_while_held = result_receiver.try_recv().is_err();
    blocker.execute_batch("COMMIT").unwrap();
    let (waited, result) = result_receiver.recv_timeout(PHASE_TIMEOUT).unwrap();
    result.unwrap();
    contender.join().unwrap();
    adapter.assert_integrity(&store_path);
    let evidence = BusyConflictEvidence {
        held_ms: held.as_millis(),
        blocked_while_held,
        append_wait_ms: waited.as_millis(),
    };
    assert!(evidence.blocked_while_held);
    assert!(evidence.append_wait_ms >= evidence.held_ms);
    let _ = std::fs::remove_dir_all(root);
    evidence
}

async fn exercise_append_failure(
    adapter: &dyn LoadStoreAdapter,
    worker: &Path,
    seed: u64,
) -> AppendFailureEvidence {
    let root = temp_root(&format!("managed_load_append_failure_{seed}"));
    let nac_home = root.join("nac-home");
    std::fs::create_dir_all(&nac_home).unwrap();
    let _env = ScopedModelEnv::isolated(&nac_home, Some("all112-append-failure-key"));
    let (base_url, requests) = scripted_direct_responses(&["parent acknowledged append failure"]);
    seed_load_parent(&root, base_url);
    let plan = LogicalPlan::new(seed ^ 0xA99, 1);
    seed_planned_orchestrators(&root.join("store.db"), &plan);
    let store_path = root.join("store.db");
    rusqlite::Connection::open(&store_path)
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER all112_fail_transcript_append
             BEFORE INSERT ON thread_events
             WHEN NEW.session_id = '{}' AND NEW.thread_name = '__orchestrator__'
             BEGIN SELECT RAISE(ABORT, 'injected ALL-112 append failure'); END;",
            plan.orchestrators[0].session_id
        ))
        .unwrap();
    let manager = adapter.create_manager(&root, worker);
    let parent = manager.attach_session("all112-parent").await.unwrap();
    let response =
        start_planned_orchestrator(router(manager.clone()), &plan.orchestrators[0]).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    wait_for_relation_status(
        &store_path,
        &plan.orchestrators[0].session_id,
        ManagedOrchestratorStatus::Failed,
    )
    .await;
    wait_for_parent_idle(&parent).await;
    assert_eq!(requests.recv_timeout(PHASE_TIMEOUT).unwrap(), 0);
    let relation =
        nac_core::store::load_managed_orchestrator(&store_path, &plan.orchestrators[0].session_id)
            .unwrap()
            .unwrap();
    let transcript_rows = nac_core::store::TranscriptLogWriter::new(&store_path)
        .unwrap()
        .read_from(&plan.orchestrators[0].session_id, 0)
        .unwrap()
        .len();
    let inbox = nac_core::store::list_session_inbox(&store_path, "all112-parent").unwrap();
    let recovery_cleared =
        nac_core::store::load_run_recovery(&store_path, &plan.orchestrators[0].session_id)
            .unwrap()
            .is_none();
    adapter.assert_integrity(&store_path);
    let evidence = AppendFailureEvidence {
        terminal_status: relation.status,
        transcript_rows,
        completion_deliveries: inbox.len(),
        recovery_cleared,
    };
    assert_eq!(evidence.transcript_rows, 0);
    assert_eq!(evidence.completion_deliveries, 1);
    assert!(evidence.recovery_cleared);
    drop(parent);
    drop(manager);
    let _ = std::fs::remove_dir_all(root);
    evidence
}

async fn exercise_monitor_failure(
    adapter: &dyn LoadStoreAdapter,
    worker: &Path,
    seed: u64,
) -> MonitorFailureEvidence {
    let root = temp_root(&format!("managed_load_monitor_failure_{seed}"));
    let nac_home = root.join("nac-home");
    std::fs::create_dir_all(&nac_home).unwrap();
    let _env = ScopedModelEnv::isolated(&nac_home, Some("all112-monitor-failure-key"));
    let (base_url, requests) = scripted_direct_responses(&[
        "orchestrator completed before monitor recovery",
        "parent acknowledged monitor recovery",
    ]);
    seed_load_parent(&root, base_url);
    let plan = LogicalPlan::new(seed ^ 0xB77, 1);
    seed_planned_orchestrators(&root.join("store.db"), &plan);
    let store_path = root.join("store.db");
    let manager = adapter.create_manager(&root, worker);
    let parent = manager.attach_session("all112-parent").await.unwrap();
    crate::delegation_runtime::inject_managed_monitor_failures(1);
    let response =
        start_planned_orchestrator(router(manager.clone()), &plan.orchestrators[0]).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let requests = tokio::task::spawn_blocking(move || {
        assert_eq!(requests.recv_timeout(PHASE_TIMEOUT).unwrap(), 0);
        requests
    })
    .await
    .unwrap();
    tokio::time::timeout(PHASE_TIMEOUT, async {
        while crate::delegation_runtime::pending_managed_monitor_failures() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let service = manager
        .attach_session(&plan.orchestrators[0].session_id)
        .await
        .unwrap();
    tokio::time::timeout(PHASE_TIMEOUT, async {
        while service.has_active_operation() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let before =
        nac_core::store::load_managed_orchestrator(&store_path, &plan.orchestrators[0].session_id)
            .unwrap()
            .unwrap();
    assert_eq!(before.status, ManagedOrchestratorStatus::Running);
    let after = manager
        .monitor_managed_orchestrator(&plan.orchestrators[0].session_id, 1)
        .await
        .unwrap();
    wait_for_parent_idle(&parent).await;
    tokio::task::spawn_blocking(move || {
        assert_eq!(requests.recv_timeout(PHASE_TIMEOUT).unwrap(), 1);
    })
    .await
    .unwrap();
    let inbox = nac_core::store::list_session_inbox(&store_path, "all112-parent").unwrap();
    adapter.assert_integrity(&store_path);
    let evidence = MonitorFailureEvidence {
        status_before_recovery: before.status,
        status_after_recovery: after.status,
        completion_deliveries: inbox.len(),
    };
    assert_eq!(
        evidence.status_after_recovery,
        ManagedOrchestratorStatus::Completed
    );
    assert_eq!(evidence.completion_deliveries, 1);
    drop(parent);
    drop(manager);
    let _ = std::fs::remove_dir_all(root);
    evidence
}

async fn exercise_worker_interruption(
    adapter: &dyn LoadStoreAdapter,
    worker: &Path,
    seed: u64,
) -> WorkerInterruptionEvidence {
    let root = temp_root(&format!("managed_load_worker_interruption_{seed}"));
    let nac_home = root.join("nac-home");
    std::fs::create_dir_all(&nac_home).unwrap();
    let _env = ScopedModelEnv::isolated(&nac_home, Some("all112-worker-interruption-key"));
    let plan = LogicalPlan::new(seed ^ 0xC55, 1);
    let model = DeterministicModel::start_with_request_count(&plan, 3);
    seed_load_parent(&root, model.base_url.clone());
    seed_planned_orchestrators(&root.join("store.db"), &plan);
    let store_path = root.join("store.db");
    let exporter = Arc::new(nac_core::telemetry::InMemoryExporter::default());
    let identity = build_identity::current();
    let recorder = nac_core::telemetry::TelemetryRecorder::bounded(
        exporter.clone(),
        nac_core::telemetry::RuntimeMetadata::sqlite(
            identity.build_id,
            identity.source_revision,
            nac_core::store::schema_version(),
            Some("all112-host"),
            Some("all112-worker-interruption"),
        ),
        nac_core::telemetry::MAX_EXPORT_QUEUE_CAPACITY,
    );
    let _telemetry = nac_core::telemetry::install_test_recorder(recorder.clone());
    let manager = adapter.create_manager(&root, worker);
    let parent = manager.attach_session("all112-parent").await.unwrap();
    let response =
        start_planned_orchestrator(router(manager.clone()), &plan.orchestrators[0]).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    model.wait_for_initial_requests().await;
    model.release_initial_requests();
    model.wait_for_worker_requests().await;

    let cancelled = tokio::time::timeout(
        PHASE_TIMEOUT,
        manager
            .delegation()
            .cancel_managed_orchestrator("all112-parent", &plan.orchestrators[0].session_id),
    )
    .await
    .expect("managed worker interruption cancellation should not hang")
    .unwrap();
    assert_eq!(cancelled.status, ManagedOrchestratorStatus::Cancelled);
    model.release_worker_requests();
    wait_for_parent_idle(&parent).await;
    let model_requests = model.finish();
    assert_eq!(model_requests.len(), 3);

    let dispatches = nac_core::store::thread_dispatches(
        &store_path,
        &plan.orchestrators[0].session_id,
        &plan.orchestrators[0].worker_thread,
    )
    .unwrap();
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].status, "cancelled");
    let telemetry = drain_telemetry(&recorder, &exporter).await;
    assert_eq!(telemetry.child_process_started.len(), 1);
    assert_eq!(
        telemetry.child_process_started,
        telemetry.child_process_stopped
    );
    assert_eq!(telemetry.dropped, 0);
    assert_eq!(telemetry.failures, 0);
    let inbox = nac_core::store::list_session_inbox(&store_path, "all112-parent").unwrap();
    assert_eq!(inbox.len(), 1);
    assert!(
        nac_core::store::load_run_recovery(&store_path, &plan.orchestrators[0].session_id)
            .unwrap()
            .is_none()
    );
    adapter.assert_integrity(&store_path);
    let evidence = WorkerInterruptionEvidence {
        terminal_status: cancelled.status,
        child_process_started: telemetry.child_process_started,
        child_process_stopped: telemetry.child_process_stopped,
        retained_dispatch_status: dispatches[0].status.clone(),
    };
    drop(parent);
    drop(manager);
    let _ = std::fs::remove_dir_all(root);
    evidence
}

async fn exercise_restart_recovery(
    adapter: &dyn LoadStoreAdapter,
    worker: &Path,
    seed: u64,
) -> RestartRecoveryEvidence {
    let root = temp_root(&format!("managed_load_restart_recovery_{seed}"));
    let nac_home = root.join("nac-home");
    std::fs::create_dir_all(&nac_home).unwrap();
    let _env = ScopedModelEnv::isolated(&nac_home, Some("all112-restart-recovery-key"));
    let (base_url, requests) =
        scripted_direct_responses(&["parent acknowledged interrupted orchestrator"]);
    seed_load_parent(&root, base_url);
    let plan = LogicalPlan::new(seed ^ 0xD33, 1);
    seed_planned_orchestrators(&root.join("store.db"), &plan);
    let store_path = root.join("store.db");
    let first = adapter.create_manager(&root, worker);
    nac_core::store::begin_managed_orchestrator_run(
        &store_path,
        &plan.orchestrators[0].session_id,
        "all112-abandoned-run",
        ManagedOrchestratorExecutionMode::Background,
    )
    .unwrap();
    nac_core::store::TranscriptLogWriter::new(&store_path)
        .unwrap()
        .append_run_prompt(
            &plan.orchestrators[0].session_id,
            0,
            &Message::User {
                content: "deterministic host interruption".to_string(),
            },
            "all112-abandoned-run",
        )
        .unwrap();
    drop(first);

    let rebuilt = adapter.create_manager(&root, worker);
    let parent = rebuilt.attach_session("all112-parent").await.unwrap();
    tokio::task::spawn_blocking(move || {
        assert_eq!(requests.recv_timeout(PHASE_TIMEOUT).unwrap(), 0);
    })
    .await
    .unwrap();
    wait_for_relation_status(
        &store_path,
        &plan.orchestrators[0].session_id,
        ManagedOrchestratorStatus::Interrupted,
    )
    .await;
    wait_for_parent_idle(&parent).await;
    rebuilt.snapshot("all112-parent").await.unwrap();
    let relation =
        nac_core::store::load_managed_orchestrator(&store_path, &plan.orchestrators[0].session_id)
            .unwrap()
            .unwrap();
    let inbox = nac_core::store::list_session_inbox(&store_path, "all112-parent").unwrap();
    let recovery =
        nac_core::store::load_run_recovery(&store_path, &plan.orchestrators[0].session_id)
            .unwrap()
            .expect("restart recovery marker remains as the durable interruption warning");
    let child = rebuilt
        .attach_session(&plan.orchestrators[0].session_id)
        .await
        .unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(relation.completion_inbox_id, Some(inbox[0].id));
    assert_eq!(
        recovery.status,
        nac_core::store::RunRecoveryStatus::Interrupted
    );
    assert!(!child.has_active_operation());
    adapter.assert_integrity(&store_path);
    let evidence = RestartRecoveryEvidence {
        terminal_status: relation.status,
        completion_deliveries: inbox.len(),
        recovery_status: "interrupted".to_string(),
        child_has_active_operation: child.has_active_operation(),
    };
    drop(child);
    drop(parent);
    drop(rebuilt);
    let _ = std::fs::remove_dir_all(root);
    evidence
}

async fn start_planned_orchestrator(app: Router, entry: &PlannedOrchestrator) -> Response {
    post_json(
        app,
        "/sessions/all112-parent/orchestrators",
        serde_json::json!({
            "description": entry.description,
            "prompt": entry.prompt,
            "orchestrator_session_id": entry.session_id,
            "background": true,
        }),
    )
    .await
}

async fn wait_for_relation_status(
    store_path: &Path,
    orchestrator_session_id: &str,
    expected: ManagedOrchestratorStatus,
) {
    tokio::time::timeout(PHASE_TIMEOUT, async {
        loop {
            let status =
                nac_core::store::load_managed_orchestrator(store_path, orchestrator_session_id)
                    .unwrap()
                    .unwrap()
                    .status;
            if status == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("managed orchestrator did not reach expected status {expected:?}"));
}

async fn wait_for_parent_idle(parent: &nac_core::session_service::SessionService) {
    tokio::time::timeout(PHASE_TIMEOUT, async {
        while parent.has_active_operation() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("parent completion delivery should become idle");
}

fn write_secret_safe_artifact(path: &Path, value: &impl Serialize) {
    let bytes = serde_json::to_vec_pretty(value).unwrap();
    for secret in [
        "all112-deterministic-key",
        "all112-append-failure-key",
        "all112-monitor-failure-key",
        "all112-worker-interruption-key",
        "all112-restart-recovery-key",
    ] {
        assert!(
            !bytes
                .windows(secret.len())
                .any(|window| window == secret.as_bytes()),
            "artifact contains an injected API credential"
        );
    }
    std::fs::write(path, bytes).unwrap();
}

async fn run_variant(
    adapter: &dyn LoadStoreAdapter,
    worker: &Path,
    seed: u64,
    orchestrator_count: usize,
    phase_delay: Duration,
) -> VariantEvidence {
    let root = temp_root(&format!("managed_load_{orchestrator_count}_{seed}"));
    let nac_home = root.join("nac-home");
    std::fs::create_dir_all(&nac_home).unwrap();
    let _env = ScopedModelEnv::isolated(&nac_home, Some("all112-deterministic-key"));
    let identity = build_identity::current();
    let exporter = Arc::new(nac_core::telemetry::InMemoryExporter::default());
    let recorder = nac_core::telemetry::TelemetryRecorder::bounded(
        exporter.clone(),
        nac_core::telemetry::RuntimeMetadata::sqlite(
            identity.build_id,
            identity.source_revision,
            nac_core::store::schema_version(),
            Some("all112-host"),
            Some("all112-managed-load"),
        ),
        nac_core::telemetry::MAX_EXPORT_QUEUE_CAPACITY,
    );
    let _telemetry = nac_core::telemetry::install_test_recorder(recorder.clone());
    let plan = LogicalPlan::new(seed, orchestrator_count);
    assert_eq!(plan, LogicalPlan::new(seed, orchestrator_count));
    let model = DeterministicModel::start(&plan);
    seed_load_parent(&root, model.base_url.clone());
    seed_planned_orchestrators(&root.join("store.db"), &plan);
    let manager = adapter.create_manager(&root, worker);
    // Model the production sessions as attached for the whole burst. The
    // primary lane controls cache lifetime explicitly so it measures the
    // intended orchestration concurrency rather than cache-eviction timing.
    let parent_service = manager.attach_session("all112-parent").await.unwrap();
    let mut orchestrator_services = Vec::with_capacity(orchestrator_count);
    for entry in &plan.orchestrators {
        orchestrator_services.push(manager.attach_session(&entry.session_id).await.unwrap());
    }
    let app = router(manager.clone());
    let started = Instant::now();

    let mut launches = tokio::task::JoinSet::new();
    for entry in &plan.orchestrators {
        let app = app.clone();
        let body = serde_json::json!({
            "description": entry.description,
            "prompt": entry.prompt,
            "orchestrator_session_id": entry.session_id,
            "background": true,
        });
        launches.spawn(async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sessions/all112-parent/orchestrators")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
        });
    }
    model.wait_for_initial_requests().await;
    let mut probe_samples = probe_pair(&app, "orchestrator-barrier").await;
    tokio::time::sleep(phase_delay).await;
    model.release_initial_requests();
    model.wait_for_worker_requests().await;
    probe_samples.extend(probe_pair(&app, "worker-barrier").await);
    tokio::time::sleep(phase_delay).await;
    model.release_worker_requests();
    while let Some(response) = launches.join_next().await {
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let record: ManagedOrchestratorRecord =
            serde_json::from_slice(&response_body(response).await).unwrap();
        assert_eq!(record.status, ManagedOrchestratorStatus::Running);
        assert_eq!(record.generation, 1);
    }

    tokio::time::timeout(PHASE_TIMEOUT, async {
        loop {
            let relations = nac_core::store::list_managed_orchestrators(
                &root.join("store.db"),
                "all112-parent",
            )
            .unwrap();
            let inbox =
                nac_core::store::list_session_inbox(&root.join("store.db"), "all112-parent")
                    .unwrap();
            if relations.len() == orchestrator_count
                && relations.iter().all(|record| {
                    record.status == ManagedOrchestratorStatus::Completed
                        && record.generation == 1
                        && record.completion_inbox_id.is_some()
                })
                && inbox.len() == orchestrator_count
                && inbox
                    .iter()
                    .all(|item| item.status == nac_core::store::InboxStatus::Delivered)
                && !parent_service.has_active_operation()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("managed-load variant should settle");

    let store_path = root.join("store.db");
    let writer = nac_core::store::TranscriptLogWriter::new(&store_path).unwrap();
    let mut transcript_counts = Vec::new();
    let mut event_counts = Vec::new();
    let mut worker_episode_counts = Vec::new();
    for entry in &plan.orchestrators {
        let transcript = writer.read_from(&entry.session_id, 0).unwrap();
        for pair in transcript.windows(2) {
            assert_eq!(pair[1].0, pair[0].0 + 1, "transcript index gap");
        }
        transcript_counts.push(transcript.len());
        let (events, _) = nac_core::store::load_thread_events_page(
            &store_path,
            &entry.session_id,
            &entry.worker_thread,
            None,
            1_000,
        )
        .unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.id)
                .collect::<BTreeSet<_>>()
                .len(),
            events.len(),
            "worker event IDs must be unique"
        );
        event_counts.push(events.len());
        let episodes =
            nac_core::store::thread_read(&store_path, &entry.session_id, &entry.worker_thread)
                .unwrap();
        assert_eq!(episodes.len(), 1);
        worker_episode_counts.push(episodes.len());
        assert!(
            nac_core::store::load_run_recovery(&store_path, &entry.session_id)
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(transcript_counts, vec![4; orchestrator_count]);
    assert_eq!(event_counts, vec![5; orchestrator_count]);
    assert_eq!(worker_episode_counts, vec![1; orchestrator_count]);
    adapter.assert_integrity(&store_path);
    let store_configuration = adapter.configuration(&store_path);
    let checkpoint = adapter.checkpoint(&store_path);
    let inbox = nac_core::store::list_session_inbox(&store_path, "all112-parent").unwrap();
    assert_eq!(
        inbox
            .iter()
            .map(|item| item.id)
            .collect::<BTreeSet<_>>()
            .len(),
        orchestrator_count,
        "each managed completion must have one distinct durable inbox delivery"
    );
    let model_requests = model.finish();
    assert_eq!(model_requests.len(), orchestrator_count * 4);
    let phase_counts = model_requests
        .iter()
        .fold(BTreeMap::new(), |mut counts, request| {
            *counts.entry(request.phase).or_insert(0_usize) += 1;
            counts
        });
    assert_eq!(
        phase_counts.get("orchestrator-start"),
        Some(&orchestrator_count)
    );
    assert_eq!(phase_counts.get("worker"), Some(&orchestrator_count));
    assert_eq!(
        phase_counts.get("orchestrator-final"),
        Some(&orchestrator_count)
    );
    assert_eq!(
        phase_counts.get("parent-completion"),
        Some(&orchestrator_count)
    );
    let telemetry = drain_telemetry(&recorder, &exporter).await;
    assert_eq!(telemetry.dropped, 0);
    assert_eq!(telemetry.failures, 0);
    assert!(telemetry.max_orchestrators_active >= orchestrator_count as u64);
    assert!(telemetry.max_child_processes_active >= orchestrator_count as u64);
    assert!(telemetry.max_connection_active >= orchestrator_count as u64);
    assert!(telemetry.max_persistence_queue_active >= orchestrator_count as u64);
    assert_eq!(
        telemetry
            .store_latency_us
            .get("terminal_settlement")
            .map(|distribution| distribution.count),
        Some(orchestrator_count)
    );
    assert_eq!(
        telemetry.child_process_started,
        telemetry.child_process_stopped
    );
    assert_eq!(telemetry.child_process_started.len(), orchestrator_count);
    let evidence = VariantEvidence {
        seed,
        orchestrators: orchestrator_count,
        store: adapter.identity(),
        elapsed_ms: started.elapsed().as_millis(),
        injected_phase_delay_ms: phase_delay.as_millis(),
        logical_plan: plan,
        model_requests,
        probe_samples,
        transcript_counts,
        event_counts,
        worker_episode_counts,
        completion_inbox_count: inbox.len(),
        build_id: identity.build_id,
        source_revision: identity.source_revision,
        store_configuration,
        checkpoint,
        telemetry,
    };
    drop(orchestrator_services);
    drop(parent_service);
    drop(manager);
    let _ = std::fs::remove_dir_all(root);
    evidence
}

async fn drain_telemetry(
    recorder: &nac_core::telemetry::TelemetryRecorder,
    exporter: &nac_core::telemetry::InMemoryExporter,
) -> TelemetryEvidence {
    let deadline = Instant::now() + Duration::from_secs(2);
    let stats = loop {
        let stats = recorder.stats();
        if stats.exported + stats.failures >= stats.accepted {
            break stats;
        }
        assert!(Instant::now() < deadline, "telemetry export did not drain");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let events = exporter.events();
    let mut store_latencies: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut api_latencies: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut evidence = TelemetryEvidence {
        store_latency_us: BTreeMap::new(),
        api_latency_us: BTreeMap::new(),
        max_connection_active: 0,
        max_persistence_queue_active: 0,
        max_orchestrators_active: 0,
        max_child_processes_active: 0,
        child_process_started: BTreeSet::new(),
        child_process_stopped: BTreeSet::new(),
        max_cpu_time_us: 0,
        max_resident_memory_bytes: 0,
        accepted: stats.accepted,
        dropped: stats.dropped,
        exported: stats.exported,
        failures: stats.failures,
    };
    for event in events {
        if let (Some(operation), Some(duration)) = (event.operation, event.duration_us) {
            store_latencies
                .entry(serde_label(operation))
                .or_default()
                .push(duration);
        }
        if event.name == nac_core::telemetry::TelemetryName::HttpRequestDuration {
            if let (Some(route), Some(duration)) = (event.route, event.duration_us) {
                api_latencies.entry(route).or_default().push(duration);
            }
        }
        let value = event.value.unwrap_or(0);
        match (event.name, event.activity) {
            (nac_core::telemetry::TelemetryName::StoreConnectionActive, _) => {
                evidence.max_connection_active = evidence.max_connection_active.max(value);
            }
            (nac_core::telemetry::TelemetryName::PersistenceQueueActive, _) => {
                evidence.max_persistence_queue_active =
                    evidence.max_persistence_queue_active.max(value);
            }
            (
                nac_core::telemetry::TelemetryName::RuntimeActivityActive,
                Some(nac_core::telemetry::RuntimeActivity::Orchestrator),
            ) => evidence.max_orchestrators_active = evidence.max_orchestrators_active.max(value),
            (
                nac_core::telemetry::TelemetryName::RuntimeActivityActive,
                Some(nac_core::telemetry::RuntimeActivity::ChildProcess),
            ) => {
                evidence.max_child_processes_active =
                    evidence.max_child_processes_active.max(value);
            }
            _ => {}
        }
        if event.name == nac_core::telemetry::TelemetryName::ChildProcess {
            if let Some(pid) = event.pid {
                match event.outcome {
                    Some(nac_core::telemetry::TelemetryOutcome::Started) => {
                        evidence.child_process_started.insert(pid);
                    }
                    Some(nac_core::telemetry::TelemetryOutcome::Stopped) => {
                        evidence.child_process_stopped.insert(pid);
                    }
                    _ => {}
                }
            }
        }
        evidence.max_cpu_time_us = evidence.max_cpu_time_us.max(event.cpu_time_us.unwrap_or(0));
        evidence.max_resident_memory_bytes = evidence
            .max_resident_memory_bytes
            .max(event.resident_memory_bytes.unwrap_or(0));
    }
    evidence.store_latency_us = store_latencies
        .into_iter()
        .map(|(name, values)| (name, LatencyDistribution::from_values(values)))
        .collect();
    evidence.api_latency_us = api_latencies
        .into_iter()
        .map(|(name, values)| (name, LatencyDistribution::from_values(values)))
        .collect();
    evidence
}

fn serde_label(value: impl Serialize) -> String {
    serde_json::to_value(value)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

async fn probe_pair(app: &Router, phase: &'static str) -> Vec<ProbeSample> {
    let mut samples = Vec::new();
    for route in ["/healthz", "/readyz"] {
        let started = Instant::now();
        let response = get_response(app.clone(), route, None).await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{route} failed at {phase}"
        );
        samples.push(ProbeSample {
            phase,
            route,
            status: response.status().as_u16(),
            latency_us: started.elapsed().as_micros(),
        });
    }
    samples
}

fn seed_planned_orchestrators(store_path: &Path, plan: &LogicalPlan) {
    let parent = sessions::load_session(store_path, "all112-parent").unwrap();
    for entry in &plan.orchestrators {
        let mut orchestrator = sessions::new_snapshot(
            entry.session_id.clone(),
            parent.cwd.clone(),
            parent.model.clone(),
            parent.base_url.clone(),
            parent.backend,
            parent.reasoning_effort,
            parent.sandbox_spec.clone(),
            parent.ssh.clone(),
            Vec::new(),
            parent.api_key_env.clone(),
            parent.extra_headers.clone(),
        );
        orchestrator.behavior = sessions::SessionBehavior::Orchestrator;
        orchestrator.project_id = parent.project_id.clone();
        orchestrator.light_model = parent.light_model.clone();
        orchestrator.orchestrator_compaction_threshold = parent.orchestrator_compaction_threshold;
        nac_core::store::create_managed_orchestrator_session(
            store_path,
            &orchestrator,
            "all112-parent",
            &entry.description,
        )
        .unwrap();
    }
}

fn seed_load_parent(root: &Path, base_url: String) {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut snapshot = sessions::new_snapshot(
        "all112-parent".to_string(),
        workspace,
        "model-a".to_string(),
        base_url,
        BackendKind::OpenAiResponses,
        Some(ReasoningEffort::Medium),
        None,
        None,
        Vec::new(),
        Some("OPENAI_API_KEY".to_string()),
        BTreeMap::new(),
    );
    snapshot.behavior = sessions::SessionBehavior::DirectWithOrchestrator;
    sessions::create_session(&root.join("store.db"), &snapshot).unwrap();
}
