//! Per-key async lock table — the shared serialization primitive (§4). Same-key holders never
//! overlap or reorder while distinct keys run fully in parallel. Instantiated twice: the *write*
//! lock (conditional writes, the durable finalize, GC tombstone transitions) and — in phase 4 —
//! the reconcile-only *upload* lock, kept separate so a replication upload only ever excludes other
//! reconciles of its key, never a client's conditional PUT.
//!
//! The table stores **weak** references, so it never keeps a mutex alive: the [`KeyGuard`] returned
//! by `lock`/`try_lock` is the only strong owner. Two concurrent lockers of the same key both
//! upgrade the *same* live `Weak`, so they serialize; a locker arriving after all guards dropped
//! upgrades a dead `Weak`, gets `None`, and installs a fresh mutex.
//!
//! Cleanup is **remove-on-drop**, O(1), not a periodic sweep: when a guard drops it releases the
//! async mutex, then removes the key iff it is the sole remaining owner (`strong_count == 1`, i.e.
//! no other holder or parked waiter). So the table holds exactly the set of currently
//! held-or-awaited keys, with no dangling entries to sweep. The one backstop for the
//! essentially-impossible orphan (a `lock` future cancelled between install and acquire — the
//! fresh-mutex acquire never suspends, so this can't actually happen) is that `mutex_for`
//! overwrites any dead `Weak` it finds, so a stray entry self-heals on the key's next use.
//!
//! `DashMap` rather than one `Mutex<HashMap>`: every write op takes and releases a key lock, so a
//! single table lock is a process-wide serialization point on the write path — sharding scopes each
//! acquisition to one of the map's shards. Keys are `Arc<str>` shared with the table entry, so a
//! second locker of a held key allocates nothing.

use std::sync::{Arc, Weak};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

type LockTable = DashMap<Arc<str>, Weak<AsyncMutex<()>>>;

#[derive(Clone, Default)]
pub struct KeyLocks {
    table: Arc<LockTable>,
}

impl KeyLocks {
    pub async fn lock(&self, key: &str) -> KeyGuard {
        let (key, arc) = self.mutex_for(key);
        let inner = arc.clone().lock_owned().await;
        self.guard(key, arc, inner)
    }

    /// Acquire the lock only if free — for callers whose work is redundant when someone else is
    /// already doing it, so a `None` is a reason to drop the attempt, not to retry. Lock-free read
    /// paths repair a leftover transition mark this way (§7): a *held* lock means the marking writer
    /// is alive mid-bracket, so there is nothing to repair and a read must not queue behind it. The
    /// reconcile sweep coalesces same-key uploads onto the in-flight one the same way
    /// ([`crate::replication`]).
    pub fn try_lock(&self, key: &str) -> Option<KeyGuard> {
        let (key, arc) = self.mutex_for(key);
        let inner = arc.clone().try_lock_owned().ok()?;
        Some(self.guard(key, arc, inner))
    }

    fn guard(
        &self,
        key: Arc<str>,
        arc: Arc<AsyncMutex<()>>,
        inner: OwnedMutexGuard<()>,
    ) -> KeyGuard {
        KeyGuard {
            inner: Some(inner),
            arc,
            key,
            table: self.table.clone(),
        }
    }

    #[cfg(test)]
    fn entries(&self) -> usize {
        self.table.len()
    }

    /// The mutex for `key` plus the table's own copy of the key, both under the shard lock: reuse
    /// the live one if another holder/waiter exists, else install a fresh one — overwriting a dead
    /// `Weak` if one lingers (the remove-on-drop backstop).
    fn mutex_for(&self, key: &str) -> (Arc<str>, Arc<AsyncMutex<()>>) {
        // Fast path — a present entry is resolved (and, if dead, replaced) in place, so the common
        // case of contending for a held key borrows the table's key instead of allocating one.
        if let Some(mut entry) = self.table.get_mut(key) {
            if let Some(m) = entry.value().upgrade() {
                return (entry.key().clone(), m);
            }
            let m = Arc::new(AsyncMutex::new(()));
            *entry.value_mut() = Arc::downgrade(&m);
            return (entry.key().clone(), m);
        }
        // Absent: only the entry API inserts atomically, and it needs an owned key — the one
        // allocation, paid once per key per idle period and shared with the guard. A racing
        // installer is resolved here, not overwritten.
        match self.table.entry(Arc::from(key)) {
            Entry::Occupied(mut occupied) => {
                if let Some(m) = occupied.get().upgrade() {
                    return (occupied.key().clone(), m);
                }
                let m = Arc::new(AsyncMutex::new(()));
                let key = occupied.key().clone();
                occupied.insert(Arc::downgrade(&m));
                (key, m)
            }
            Entry::Vacant(vacant) => {
                let m = Arc::new(AsyncMutex::new(()));
                let key = vacant.key().clone();
                vacant.insert(Arc::downgrade(&m));
                (key, m)
            }
        }
    }
}

/// Owns a held per-key lock; releasing it (drop) frees the async mutex and evicts the key's table
/// entry once no other holder or waiter remains.
#[must_use = "dropping the guard immediately releases the lock"]
pub struct KeyGuard {
    /// `Option` so `drop` can release the async mutex *before* counting owners — the released
    /// guard's own strong ref must be gone for `strong_count == 1` to mean "only us left".
    inner: Option<OwnedMutexGuard<()>>,
    arc: Arc<AsyncMutex<()>>,
    /// Shared with the table's own key, so holding a guard costs a refcount, not an allocation.
    key: Arc<str>,
    table: Arc<LockTable>,
}

impl Drop for KeyGuard {
    fn drop(&mut self) {
        // Release first: waking a parked waiter can't change the owner count (it already holds its
        // own `arc` clone), and our own reference must be gone before the count means anything.
        drop(self.inner.take());
        // Both conditions are evaluated under the shard's write lock, so a locker that upgrades our
        // `Weak` concurrently either gets there first — bumping the count, and we decline — or
        // finds the entry already gone and installs its own. The pointer check keeps us from
        // evicting a newer epoch installed under the same key.
        self.table.remove_if(&self.key, |_, weak| {
            Arc::strong_count(&self.arc) == 1 && Weak::as_ptr(weak) == Arc::as_ptr(&self.arc)
        });
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
            let _g = l2.lock("k").await;
        });
        // Let the spawned task reach the parked lock (its `arc` clone is now live).
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(locks.entries(), 1, "waiter and holder share one entry");

        drop(held);
        waiter.await.unwrap();
        assert_eq!(locks.entries(), 0, "last owner evicts the entry");
    }

    // Many keys across many shards, each contended: exclusion must hold per key, and every entry
    // must be evicted — the install/evict race is the whole risk of resolving a key under one
    // shard lock rather than one table lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_keys_stay_exclusive_and_drain() {
        let locks = KeyLocks::default();
        let inside: Arc<Vec<AtomicBool>> =
            Arc::new((0..16).map(|_| AtomicBool::new(false)).collect());
        let mut handles = Vec::new();
        for i in 0..128 {
            let l = locks.clone();
            let inside = inside.clone();
            handles.push(tokio::spawn(async move {
                let k = i % 16;
                for _ in 0..8 {
                    let _g = l.lock(&format!("key/{k}")).await;
                    assert!(!inside[k].swap(true, SeqCst), "two holders on key/{k}");
                    tokio::task::yield_now().await;
                    inside[k].store(false, SeqCst);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            locks.entries(),
            0,
            "every key evicted after the last release"
        );
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
