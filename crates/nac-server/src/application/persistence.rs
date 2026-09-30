use std::path::Path;

use anyhow::Result;
use nac_core::sessions::StoreProcessLease;

#[cfg(test)]
std::thread_local! {
    static BYPASS_STORE_OWNERSHIP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Application ownership of the selected store for one serving lifetime.
///
/// Core owns the generic crash-safe lease primitive. The server application
/// owns when to acquire it and retains it across every delivery surface.
pub(crate) struct StoreOwnership {
    _lease: Option<StoreProcessLease>,
}

impl StoreOwnership {
    pub(crate) fn acquire(store_path: &Path) -> Result<Self> {
        #[cfg(test)]
        if BYPASS_STORE_OWNERSHIP.with(std::cell::Cell::get) {
            return Ok(Self { _lease: None });
        }
        Ok(Self {
            _lease: Some(StoreProcessLease::try_acquire(store_path).map_err(anyhow::Error::new)?),
        })
    }
}

#[cfg(test)]
pub(crate) fn without_store_ownership<T>(operation: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            BYPASS_STORE_OWNERSHIP.with(|bypass| bypass.set(self.0));
        }
    }

    BYPASS_STORE_OWNERSHIP.with(|bypass| {
        let reset = Reset(bypass.replace(true));
        let result = operation();
        drop(reset);
        result
    })
}
