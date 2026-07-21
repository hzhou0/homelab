//! Per-key async lock table — the shared serialization primitive (§4). Same-key holders never
//! overlap or reorder while distinct keys run fully in parallel. Instantiated twice: the *write*
//! lock (conditional writes, the durable finalize, GC tombstone transitions) and — in phase 4 —
//! the reconcile-only *upload* lock, kept separate so a replication upload only ever blocks other
//! reconciles of its key, never a client's conditional PUT.
//!
//! The table stores **weak** references, so it never keeps a mutex alive: the [`Guard`] returned
//! by `lock`/`try_lock` is the only strong owner. Two concurrent lockers of the same key both
//! upgrade the *same* live `Weak`, so they serialize; a locker arriving after all guards dropped
//! upgrades a dead `Weak`, gets `None`, and installs a fresh mutex.
//!
//! Cleanup is **remove-on-drop**, O(1), not a periodic sweep: when a guard drops it releases the
//! async mutex, then — under the table lock — removes the key iff it is the sole remaining owner
//! (`strong_count == 1`, i.e. no other holder or parked waiter). So the table holds exactly the
//! set of currently held-or-awaited keys, with no dangling entries to sweep. The one backstop for
//! the essentially-impossible orphan (a `lock` future cancelled between install and acquire — the
//! fresh-mutex acquire never suspends, so this can't actually happen) is that `mutex_for`
//! overwrites any dead `Weak` it finds, so a stray entry self-heals on the key's next use.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

type Table = HashMap<String, Weak<AsyncMutex<()>>>;

#[derive(Clone, Default)]
pub struct KeyLocks {
    table: Arc<Mutex<Table>>,
}

impl KeyLocks {
    /// Acquire the lock for `key`, awaiting any current holder. Hold the returned guard for the
    /// critical section; dropping it releases the lock and evicts the key's now-idle entry.
    pub async fn lock(&self, key: &str) -> Guard {
        let arc = self.mutex_for(key);
        let inner = arc.clone().lock_owned().await;
        self.guard(key, arc, inner)
    }

    /// Acquire the lock only if free. Lock-free read paths use this to repair a leftover
    /// transition mark opportunistically (§7): a *held* lock means the marking writer is alive
    /// mid-bracket — nothing to repair, and a read must not queue behind it.
    pub fn try_lock(&self, key: &str) -> Option<Guard> {
        let arc = self.mutex_for(key);
        let inner = arc.clone().try_lock_owned().ok()?;
        Some(self.guard(key, arc, inner))
    }

    fn guard(&self, key: &str, arc: Arc<AsyncMutex<()>>, inner: OwnedMutexGuard<()>) -> Guard {
        Guard {
            inner: Some(inner),
            arc,
            key: key.to_string(),
            table: self.table.clone(),
        }
    }

    #[cfg(test)]
    fn entries(&self) -> usize {
        self.table.lock().unwrap().len()
    }

    fn mutex_for(&self, key: &str) -> Arc<AsyncMutex<()>> {
        let mut table = self.table.lock().unwrap();
        // Reuse the live mutex if another holder/waiter exists, else install a fresh one —
        // overwriting a dead `Weak` if one lingers (the remove-on-drop backstop).
        match table.get(key).and_then(Weak::upgrade) {
            Some(m) => m,
            None => {
                let m = Arc::new(AsyncMutex::new(()));
                table.insert(key.to_string(), Arc::downgrade(&m));
                m
            }
        }
    }
}

/// Owns a held per-key lock; releasing it (drop) frees the async mutex and evicts the key's table
/// entry once no other holder or waiter remains. Returned by [`KeyLocks::lock`]/`try_lock`.
#[must_use = "dropping the guard immediately releases the lock"]
pub struct Guard {
    /// `Option` so `drop` can release the async mutex *before* counting owners — the released
    /// guard's own strong ref must be gone for `strong_count == 1` to mean "only us left".
    inner: Option<OwnedMutexGuard<()>>,
    arc: Arc<AsyncMutex<()>>,
    key: String,
    table: Arc<Mutex<Table>>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        // Hold the table lock across the whole sequence: it fences out `mutex_for`, so no new
        // locker can upgrade our `Weak` between the owner count and the remove. Waking a parked
        // waiter (below) doesn't change the count — it already holds its own `arc` clone.
        let mut table = self.table.lock().unwrap();
        drop(self.inner.take());
        if Arc::strong_count(&self.arc) == 1 {
            // Sole owner: the entry is dead to everyone but us. Remove it — but only if it is
            // still *our* mutex, never a newer epoch installed under the same key.
            if table
                .get(&self.key)
                .is_some_and(|w| Weak::as_ptr(w) == Arc::as_ptr(&self.arc))
            {
                table.remove(&self.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    use std::time::Duration;

    #[tokio::test]
    async fn drop_evicts_idle_key() {
        let locks = KeyLocks::default();
        let g = locks.lock("k").await;
        assert_eq!(locks.entries(), 1);
        drop(g);
        assert_eq!(
            locks.entries(),
            0,
            "sole holder must evict its entry on drop"
        );
    }

    #[tokio::test]
    async fn distinct_keys_dont_block_or_share() {
        let locks = KeyLocks::default();
        // Acquiring b while holding a must not block (different mutexes).
        let a = locks.lock("a").await;
        let b = locks.lock("b").await;
        assert_eq!(locks.entries(), 2);
        drop(a);
        assert_eq!(locks.entries(), 1);
        drop(b);
        assert_eq!(locks.entries(), 0);
    }

    #[tokio::test]
    async fn try_lock_reflects_held_state_and_cleans_up() {
        let locks = KeyLocks::default();
        let held = locks.lock("k").await;
        assert!(locks.try_lock("k").is_none(), "held key must fail try_lock");
        drop(held);
        let got = locks.try_lock("k").expect("free key must try_lock");
        assert_eq!(locks.entries(), 1);
        drop(got);
        assert_eq!(locks.entries(), 0);
    }

    // A parked waiter shares the live mutex, so the entry must survive the first holder's drop and
    // be evicted only when the *last* owner (the waiter, once promoted) releases.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waiter_keeps_entry_until_last_release() {
        let locks = KeyLocks::default();
        let held = locks.lock("k").await;

        let l2 = locks.clone();
        let waiter = tokio::spawn(async move {
            let _g = l2.lock("k").await; // parks behind `held`, then acquires
        });
        // Let the spawned task reach the parked lock (its `arc` clone is now live).
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(locks.entries(), 1, "waiter and holder share one entry");

        drop(held); // waiter is promoted to holder
        waiter.await.unwrap(); // waiter drops its guard
        assert_eq!(locks.entries(), 0, "last owner evicts the entry");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_key_holders_never_overlap() {
        let locks = KeyLocks::default();
        let inside = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let l = locks.clone();
            let inside = inside.clone();
            handles.push(tokio::spawn(async move {
                let _g = l.lock("same").await;
                assert!(
                    !inside.swap(true, SeqCst),
                    "two holders in the critical section"
                );
                tokio::time::sleep(Duration::from_millis(2)).await;
                inside.store(false, SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(locks.entries(), 0, "no entries leak after contention");
    }
}
