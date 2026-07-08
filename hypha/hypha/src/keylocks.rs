//! Per-key async lock table — the shared serialization primitive (§7, generalized). Every path
//! that writes a key's remote body or tombstone (inline durable finalize now; background cached
//! reconcile and GC later) takes this lock, so same-key operations never overlap or reorder while
//! distinct keys run fully in parallel. This is the N-worker generalization of the doc's "single
//! reconcile task".
//!
//! The table stores **weak** references, so it never keeps a mutex alive: the returned
//! `OwnedMutexGuard` is the only strong owner, and when the last guard for a key drops, the mutex
//! frees and its map entry becomes a dangling `Weak`. Two concurrent lockers of the same key both
//! upgrade the *same* live `Weak`, so they serialize; a locker arriving after all guards dropped
//! upgrades a dead `Weak`, gets `None`, and installs a fresh mutex. No `Drop` impl and no
//! strong-count reasoning — correctness follows directly from `Weak::upgrade`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Sweep dangling `Weak`s from the table once every this many acquisitions, bounding its size to
/// the set of concurrently-held keys plus at most this many idle entries.
const SWEEP_INTERVAL: usize = 4096;

#[derive(Clone, Default)]
pub struct KeyLocks {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    table: HashMap<String, Weak<AsyncMutex<()>>>,
    since_sweep: usize,
}

impl KeyLocks {
    /// Acquire the lock for `key`, awaiting any current holder. Hold the returned guard for the
    /// critical section; dropping it releases the lock.
    pub async fn lock(&self, key: &str) -> OwnedMutexGuard<()> {
        let mutex = {
            let mut inner = self.inner.lock().unwrap();

            inner.since_sweep += 1;
            if inner.since_sweep >= SWEEP_INTERVAL {
                inner.table.retain(|_, w| w.strong_count() > 0);
                inner.since_sweep = 0;
            }

            // Reuse the live mutex if another holder/waiter exists, else install a fresh one.
            match inner.table.get(key).and_then(Weak::upgrade) {
                Some(m) => m,
                None => {
                    let m = Arc::new(AsyncMutex::new(()));
                    inner.table.insert(key.to_string(), Arc::downgrade(&m));
                    m
                }
            }
        };
        mutex.lock_owned().await
    }
}
