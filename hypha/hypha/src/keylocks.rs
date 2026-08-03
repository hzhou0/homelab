//! Sharded per-key async locks.
//!
//! Keys are fully-qualified — `{bucket}\0{key}` for object-scoped locks and
//! `{bucket}\0{upload_id}\0{part}` for multipart parts. Bucket always rides the key, so the same
//! key in two buckets never serializes against each other. Weak table entries let idle keys
//! disappear on guard drop without a sweep. Separate table instances keep reconcile uploads from
//! serializing client writes.
//!
//! Two tables with one shape. [`KeyLocks`] is a plain per-key mutex: exclusive only, which is all a
//! write, a reconcile upload, or an MPU part needs. [`CreateLocks`] is the one RwLock — every
//! in-flight MPU create holds it shared, and only the orphan sweep ([`crate::gc::debris`]) takes it
//! exclusively, so a create serializes nothing but the sweep.

use std::sync::{Arc, Weak};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard,
    RwLock as AsyncRwLock,
};

type LockTable<L> = DashMap<Arc<str>, Weak<L>>;

/// The lock for `fqn` plus the table's own copy of the key, both under the shard lock: reuse the
/// live one if another holder/waiter exists, else install a fresh one — overwriting a dead `Weak`
/// if one lingers (the remove-on-drop backstop).
fn entry_for<L>(table: &LockTable<L>, fqn: &str, new: impl FnOnce() -> L) -> (Arc<str>, Arc<L>) {
    // Fast path — a present entry is resolved (and, if dead, replaced) in place, so the common case
    // of contending for a held key borrows the table's key instead of allocating one.
    if let Some(mut entry) = table.get_mut(fqn) {
        if let Some(m) = entry.value().upgrade() {
            return (entry.key().clone(), m);
        }
        let m = Arc::new(new());
        *entry.value_mut() = Arc::downgrade(&m);
        return (entry.key().clone(), m);
    }
    // Absent: only the entry API inserts atomically, and it needs an owned key — the one
    // allocation, paid once per key per idle period and shared with the guard. A racing installer
    // is resolved here, not overwritten.
    match table.entry(Arc::from(fqn)) {
        Entry::Occupied(mut occupied) => {
            if let Some(m) = occupied.get().upgrade() {
                return (occupied.key().clone(), m);
            }
            let m = Arc::new(new());
            let key = occupied.key().clone();
            occupied.insert(Arc::downgrade(&m));
            (key, m)
        }
        Entry::Vacant(vacant) => {
            let m = Arc::new(new());
            let key = vacant.key().clone();
            vacant.insert(Arc::downgrade(&m));
            (key, m)
        }
    }
}

/// The fully-qualified lock key for an object-scoped lock. Bucket is a mandatory component so a key
/// in one bucket can never contend with the same key in another.
fn lock_fqn(bucket: &str, key: &str) -> String {
    format!("{bucket}\0{key}")
}

/// Per-key exclusive locks — the write, reconcile-upload, and MPU-part tables. Nobody ever takes a
/// shared side, so a mutex is the whole story.
#[derive(Clone, Default)]
pub struct KeyLocks {
    table: Arc<LockTable<AsyncMutex<()>>>,
}

impl KeyLocks {
    pub async fn lock(&self, bucket: &str, key: &str) -> KeyGuard {
        let fqn = lock_fqn(bucket, key);
        let (key, arc) = entry_for(&self.table, &fqn, || AsyncMutex::new(()));
        let inner = arc.clone().lock_owned().await;
        hold(self.table.clone(), key, arc, inner)
    }

    /// Acquire the lock only if free — for callers whose work is redundant when someone else is
    /// already doing it, so a `None` is a reason to drop the attempt, not to retry. Lock-free read
    /// paths repair a leftover transition mark this way: a *held* lock means the marking writer is
    /// alive mid-bracket, so there is nothing to repair and a read must not queue behind it. The
    /// reconcile sweep coalesces same-key uploads onto the in-flight one the same way
    /// ([`crate::replication`]).
    pub fn try_lock(&self, bucket: &str, key: &str) -> Option<KeyGuard> {
        let fqn = lock_fqn(bucket, key);
        let (key, arc) = entry_for(&self.table, &fqn, || AsyncMutex::new(()));
        let inner = arc.clone().try_lock_owned().ok()?;
        Some(hold(self.table.clone(), key, arc, inner))
    }

    /// Multipart parts serialize on `(bucket, upload_id, part)` — the finer identity a part is
    /// known by once its upload exists.
    pub async fn lock_part(&self, bucket: &str, upload_id: &str, part_number: i32) -> KeyGuard {
        let fqn = format!("{bucket}\0{upload_id}\0{part_number}");
        let (key, arc) = entry_for(&self.table, &fqn, || AsyncMutex::new(()));
        let inner = arc.clone().lock_owned().await;
        hold(self.table.clone(), key, arc, inner)
    }

    #[cfg(test)]
    fn entries(&self) -> usize {
        self.table.len()
    }
}

/// The MPU create-window lock: held **shared** from before the remote `CreateMultipartUpload` until
/// the cache `u`-record is written — by the copy path too, whose native upload writes no record.
/// The orphan sweep's exclusive `try_lock` only wins against a create that is already done, which
/// is the whole handshake that tells a leak from an upload in flight. Held before the remote create
/// because the upload becomes listable the instant that returns.
#[derive(Clone, Default)]
pub struct CreateLocks {
    table: Arc<LockTable<AsyncRwLock<()>>>,
}

impl CreateLocks {
    /// Shared hold: many creators may proceed together.
    pub async fn read(&self, bucket: &str, key: &str) -> CreateReadGuard {
        let fqn = lock_fqn(bucket, key);
        let (key, arc) = entry_for(&self.table, &fqn, || AsyncRwLock::new(()));
        let inner = arc.clone().read_owned().await;
        hold(self.table.clone(), key, arc, inner)
    }

    /// The sweep's exclusive probe — succeeds only when no create is in flight on this key.
    pub fn try_lock(&self, bucket: &str, key: &str) -> Option<CreateWriteGuard> {
        let fqn = lock_fqn(bucket, key);
        let (key, arc) = entry_for(&self.table, &fqn, || AsyncRwLock::new(()));
        let inner = arc.clone().try_write_owned().ok()?;
        Some(hold(self.table.clone(), key, arc, inner))
    }

    #[cfg(test)]
    fn entries(&self) -> usize {
        self.table.len()
    }
}

/// Owns a held per-key lock; releasing it (drop) frees the async lock and evicts the key's table
/// entry once no other holder or waiter remains. Generic over the primitive and its owned guard so
/// the mutex and RwLock tables share one drop path.
#[must_use = "dropping the guard immediately releases the lock"]
pub struct Guard<L, G> {
    /// `Option` so `drop` can release the async lock *before* counting owners — the released
    /// guard's own strong ref must be gone for `strong_count == 1` to mean "only us left".
    inner: Option<G>,
    arc: Arc<L>,
    /// Shared with the table's own key, so holding a guard costs a refcount, not an allocation.
    key: Arc<str>,
    table: Arc<LockTable<L>>,
}

pub type KeyGuard = Guard<AsyncMutex<()>, OwnedMutexGuard<()>>;
pub type CreateReadGuard = Guard<AsyncRwLock<()>, OwnedRwLockReadGuard<()>>;
pub type CreateWriteGuard = Guard<AsyncRwLock<()>, OwnedRwLockWriteGuard<()>>;

fn hold<L, G>(table: Arc<LockTable<L>>, key: Arc<str>, arc: Arc<L>, inner: G) -> Guard<L, G> {
    Guard {
        inner: Some(inner),
        arc,
        key,
        table,
    }
}

impl<L, G> Drop for Guard<L, G> {
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
        let g = locks.lock("b", "k").await;
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
        let a = locks.lock("b", "a").await;
        let b = locks.lock("b", "b").await;
        assert_eq!(locks.entries(), 2);
        drop(a);
        assert_eq!(locks.entries(), 1);
        drop(b);
        assert_eq!(locks.entries(), 0);
    }

    #[tokio::test]
    async fn same_key_different_buckets_dont_share() {
        let locks = KeyLocks::default();
        let a = locks.lock("b1", "k").await;
        let b = locks.lock("b2", "k").await;
        assert_eq!(locks.entries(), 2);
        drop(a);
        drop(b);
        assert_eq!(locks.entries(), 0);
    }

    #[tokio::test]
    async fn try_lock_reflects_held_state_and_cleans_up() {
        let locks = KeyLocks::default();
        let held = locks.lock("b", "k").await;
        assert!(
            locks.try_lock("b", "k").is_none(),
            "held key must fail try_lock"
        );
        drop(held);
        let got = locks.try_lock("b", "k").expect("free key must try_lock");
        assert_eq!(locks.entries(), 1);
        drop(got);
        assert_eq!(locks.entries(), 0);
    }

    // A parked waiter shares the live mutex, so the entry must survive the first holder's drop and
    // be evicted only when the *last* owner (the waiter, once promoted) releases.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waiter_keeps_entry_until_last_release() {
        let locks = KeyLocks::default();
        let held = locks.lock("b", "k").await;

        let l2 = locks.clone();
        let waiter = tokio::spawn(async move {
            let _g = l2.lock("b", "k").await;
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
                    let _g = l.lock("b", &format!("key/{k}")).await;
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
                let _g = l.lock("b", "same").await;
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

    #[tokio::test]
    async fn part_locks_are_keyed_by_part_identity() {
        let locks = KeyLocks::default();
        let a = locks.lock_part("b", "u1", 1).await;
        let b = locks.lock_part("b", "u1", 2).await;
        assert_eq!(locks.entries(), 2);
        drop(a);
        drop(b);
        assert_eq!(locks.entries(), 0);
    }

    // The create lock's whole job is sharing creators with each other while staying exclusive to
    // the orphan sweep: concurrent reads must all succeed, but a try_lock (the sweep's probe) must
    // fail while any one of them holds, and succeed — and evict — once they are gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn creates_share_but_sweep_probe_is_exclusive() {
        let locks = CreateLocks::default();
        let r1 = locks.read("b", "k").await;
        let r2 = locks.read("b", "k").await;
        assert_eq!(locks.entries(), 1, "readers share one entry");
        assert!(
            locks.try_lock("b", "k").is_none(),
            "sweep probe must fail while a creator holds"
        );
        drop(r1);
        drop(r2);
        let w = locks.try_lock("b", "k").expect("free once creators drop");
        assert_eq!(locks.entries(), 1);
        drop(w);
        assert_eq!(locks.entries(), 0);
    }
}
