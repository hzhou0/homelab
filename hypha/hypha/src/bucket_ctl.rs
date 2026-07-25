//! The bucket-control actor: the sole writer of the cache substrate and the owner of per-bucket
//! restore (§7 *Buckets*).
//!
//! One client bucket is three physical buckets — `<remote><b>` (the source of truth for existence)
//! plus the `<data><b>`/`<meta><b>` cache projections. The cache runs unreplicated, and — by
//! assumption — a bucket's cache is lost whole or not at all (never partially). Its per-bucket sync
//! marker (`meta::sync_marker_key`) records namespace trust: present ⇒ the projections survived
//! intact and are authoritative; absent ⇒ the remote is the read source of truth until a restore
//! rebuilds the tombstone namespace and rewrites the marker.
//!
//! Rather than guard the three-way divergence with locks, every substrate *mutation* — CreateBucket,
//! DeleteBucket, provisioning, and restore — is funnelled through this one actor, which makes its
//! serialization structural: per-bucket-serial (one worker drains a bucket's requests in arrival
//! order) and cross-bucket-parallel (distinct buckets proceed at once, bounded by
//! [`MAX_CONCURRENT`]). Reads never enter here; they read the actor's published
//! [state map](BucketCtl::state) instead, which costs one atomic load and no lock.
//!
//! Client Create/Delete are request-reply and never coalesced — each returns the remote's own
//! result, so a double-delete's loser still sees `NoSuchBucket`. `Restore`s are fire-and-forget and
//! deduped: the op that triggered one already resolves from the remote meanwhile, so there is no
//! waiter, and a storm of restores for one bucket collapses to a single sweep.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use arc_swap::ArcSwap;

use tokio::sync::{mpsc, oneshot, Semaphore};

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::tier::Reconciler;

/// Cap on cache buckets being mutated at once. A slow DeleteBucket drain or a large restore holds a
/// slot for its duration, so this also bounds the head-of-line a lost-volume restore storm drains
/// against.
const MAX_CONCURRENT: usize = 16;

/// Every bucket this process has classified, published as one immutable map: readers take a single
/// atomic load, no lock, on a path every request crosses. Mutations are copy-on-write through
/// [`ArcSwap::rcu`] — a whole-map clone per bucket-lifecycle event, which is the right trade when
/// the map holds tens of entries and is read once or twice per request.
type BucketStates = Arc<ArcSwap<HashMap<String, BucketState>>>;

/// What the data plane knows about one bucket (§7). Absent from the map ⇒ unclassified: the gate
/// must probe. Keeping the three facts in one value is what makes a transition a single atomic
/// publish — `Ready` replacing `Restoring` can't be observed half-applied, and no ordering
/// convention between separate sets has to be maintained by hand.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct BucketState {
    phase: Option<Phase>,
    /// Both cache projections are known to exist, so a write can skip asking the actor.
    provisioned: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// A restore sweep is pending or in flight; the remote is the read source of truth.
    Restoring,
    /// Sync marker observed: the cache namespace is authoritative.
    Ready,
}

impl BucketState {
    pub fn is_ready(&self) -> bool {
        matches!(self.phase, Some(Phase::Ready))
    }

    pub fn is_restoring(&self) -> bool {
        matches!(self.phase, Some(Phase::Restoring))
    }
}

/// Apply `f` to one bucket's state and publish the result. The closure may run more than once (the
/// CAS retries under contention), so it must be pure. An entry that lands back at the default is
/// dropped, keeping the map to buckets this process actually knows something about.
fn update(states: &BucketStates, bucket: &str, f: impl Fn(&mut BucketState)) {
    states.rcu(|current| {
        let mut next = HashMap::clone(current);
        let entry = next.entry(bucket.to_string()).or_default();
        f(entry);
        if *entry == BucketState::default() {
            next.remove(bucket);
        }
        Arc::new(next)
    });
}

/// A restore is pending: claim `Restoring` unless the bucket is already `Ready` (a sweep that won
/// the race must not be walked back).
fn mark_restoring(state: &mut BucketState) {
    if !state.is_ready() {
        state.phase = Some(Phase::Restoring);
    }
}

/// The sweep is over. Only an unfinished `Restoring` is cleared — a success has already published
/// `Ready` in the same value, so this leaves it alone. Dropping to unclassified is what makes the
/// next access re-probe, and therefore re-trigger a failed sweep.
fn clear_restoring(state: &mut BucketState) {
    if state.is_restoring() {
        state.phase = None;
    }
}

/// The sync marker is present: the cache namespace is authoritative. Supersedes `Restoring` in the
/// same publish, so no reader can see the bucket as neither.
fn mark_ready(state: &mut BucketState) {
    state.phase = Some(Phase::Ready);
}

fn set_provisioned(state: &mut BucketState) {
    state.provisioned = true;
}

fn clear_provisioned(state: &mut BucketState) {
    state.provisioned = false;
}

/// Forget a bucket entirely — it no longer exists, so the next access re-classifies from scratch.
fn retire(states: &BucketStates, bucket: &str) {
    states.rcu(|current| {
        let mut next = HashMap::clone(current);
        next.remove(bucket);
        Arc::new(next)
    });
}

enum BucketMsg {
    Create {
        bucket: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Delete {
        bucket: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Provision {
        bucket: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Restore {
        bucket: String,
    },
}

/// Handle onto the actor. Cloneable and cheap — the queue sender plus the published state map — so
/// every `Hypha` clone shares one actor and one view of which buckets are cache-authoritative.
#[derive(Clone)]
pub struct BucketCtl {
    tx: mpsc::UnboundedSender<BucketMsg>,
    states: BucketStates,
}

impl BucketCtl {
    /// Client CreateBucket: push and await the remote create's own result.
    pub async fn create(&self, bucket: &str) -> Result<()> {
        self.request(|reply| BucketMsg::Create {
            bucket: bucket.to_string(),
            reply,
        })
        .await
    }

    /// Client DeleteBucket: push and await. The remote delete is the emptiness gate, so a non-empty
    /// bucket surfaces here as the remote's own error.
    pub async fn delete(&self, bucket: &str) -> Result<()> {
        self.request(|reply| BucketMsg::Delete {
            bucket: bucket.to_string(),
            reply,
        })
        .await
    }

    /// Ensure the bucket's cache projections exist so a write can land ahead of the restore sweep
    /// that would otherwise provision them (§7). Request-reply, but **coalesced**: an already-known
    /// bucket answers from the shared set without touching the queue, and concurrent first-callers
    /// for one bucket all wait on a single head+create pair. A flood of writes into a lost-volume
    /// bucket therefore costs the backend one provisioning round, not one per request.
    pub async fn provision(&self, bucket: &str) -> Result<()> {
        if self.state(bucket).provisioned {
            return Ok(());
        }
        self.request(|reply| BucketMsg::Provision {
            bucket: bucket.to_string(),
            reply,
        })
        .await
    }

    /// Trigger a background restore: fire-and-forget. The caller resolves from the remote meanwhile;
    /// a closed queue (actor gone at shutdown) is ignored.
    ///
    /// Also memoizes the bucket as restoring, which is what spares every *subsequent* op the gate's
    /// two-probe classification (§7). The worker clears the memo when the sweep ends — however it
    /// ends — so a failed sweep is re-probed and re-triggered by the next access, exactly as it was
    /// when the gate probed every time.
    pub fn restore(&self, bucket: &str) {
        // Memoize before sending: the reverse order races a worker that finishes first, and would
        // leave a memo no sweep will ever clear. A refused send rolls it back for the same reason.
        update(&self.states, bucket, mark_restoring);
        if self
            .tx
            .send(BucketMsg::Restore {
                bucket: bucket.to_string(),
            })
            .is_err()
        {
            update(&self.states, bucket, clear_restoring);
        }
    }

    /// This process's current view of a bucket — one atomic load, no lock. The gate reads it once
    /// and answers both "is the cache authoritative" and "is a restore already pending" from it,
    /// which is why they are one value rather than two sets (§7).
    pub fn state(&self, bucket: &str) -> BucketState {
        self.states.load().get(bucket).copied().unwrap_or_default()
    }

    /// Record a bucket as ready — used by the gate when it discovers a persisted sync marker that
    /// this process hadn't yet observed.
    pub fn mark_ready(&self, bucket: &str) {
        update(&self.states, bucket, mark_ready);
    }

    async fn request(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<()>>) -> BucketMsg,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(make(reply))
            .map_err(|_| Error::Backend("bucket-control actor is not running".into()))?;
        rx.await
            .map_err(|_| Error::Backend("bucket-control actor dropped the request".into()))?
    }
}

/// Spawn the actor and return its handle. The task runs until every handle is dropped, then drains
/// whatever is still queued before exiting.
pub fn spawn(tier: Reconciler) -> BucketCtl {
    let (tx, rx) = mpsc::unbounded_channel();
    let (done_tx, done_rx) = mpsc::unbounded_channel();
    let (prov_tx, prov_rx) = mpsc::unbounded_channel();
    let states: BucketStates = Arc::new(ArcSwap::from_pointee(HashMap::new()));
    let actor = Actor {
        rx,
        done_tx,
        done_rx,
        prov_tx,
        prov_rx,
        tier,
        states: states.clone(),
        sem: Arc::new(Semaphore::new(MAX_CONCURRENT)),
        slots: HashMap::new(),
        running: HashSet::new(),
        provisioning: HashMap::new(),
    };
    tokio::spawn(actor.run());
    BucketCtl { tx, states }
}

/// One bucket's queued-but-undispatched work. `Create`/`Delete` keep arrival order; `restore` is a
/// single flag — repeated restore triggers for a bucket collapse into one sweep.
#[derive(Default)]
struct Slot {
    pending: VecDeque<Client>,
    restore: bool,
}

impl Slot {
    fn is_empty(&self) -> bool {
        self.pending.is_empty() && !self.restore
    }
}

enum Client {
    Create(oneshot::Sender<Result<()>>),
    Delete(oneshot::Sender<Result<()>>),
}

struct Actor {
    rx: mpsc::UnboundedReceiver<BucketMsg>,
    /// Workers signal here when they finish a bucket's batch, so the actor can re-dispatch any work
    /// that arrived mid-drain. Held by the actor too, so it never closes on its own.
    done_tx: mpsc::UnboundedSender<String>,
    done_rx: mpsc::UnboundedReceiver<String>,
    /// Provisioning tasks report here. Separate from `done_tx` because provisioning deliberately
    /// runs *outside* the per-bucket worker: the worker for a lost-volume bucket is busy with the
    /// restore sweep, and a write must not queue behind it (§7 — serving is never gated).
    prov_tx: mpsc::UnboundedSender<(String, std::result::Result<(), String>)>,
    prov_rx: mpsc::UnboundedReceiver<(String, std::result::Result<(), String>)>,
    tier: Reconciler,
    states: BucketStates,
    sem: Arc<Semaphore>,
    slots: HashMap<String, Slot>,
    running: HashSet<String>,
    /// Buckets with provisioning in flight → everyone waiting on it. The entry *is* the coalescing:
    /// only the caller that creates it spawns the backend work.
    provisioning: HashMap<String, Vec<oneshot::Sender<Result<()>>>>,
}

impl Actor {
    async fn run(mut self) {
        let mut ext_open = true;
        loop {
            // Shutdown completes only once the queue is fully drained and nothing is in flight.
            if !ext_open
                && self.slots.is_empty()
                && self.running.is_empty()
                && self.provisioning.is_empty()
            {
                break;
            }
            tokio::select! {
                msg = self.rx.recv(), if ext_open => match msg {
                    Some(msg) => self.enqueue(msg),
                    None => ext_open = false,
                },
                Some(bucket) = self.done_rx.recv() => {
                    self.running.remove(&bucket);
                }
                Some((bucket, result)) = self.prov_rx.recv() => self.finish_provision(bucket, result),
            }
            self.dispatch();
        }
    }

    fn enqueue(&mut self, msg: BucketMsg) {
        match msg {
            BucketMsg::Create { bucket, reply } => self
                .slots
                .entry(bucket)
                .or_default()
                .pending
                .push_back(Client::Create(reply)),
            BucketMsg::Delete { bucket, reply } => self
                .slots
                .entry(bucket)
                .or_default()
                .pending
                .push_back(Client::Delete(reply)),
            BucketMsg::Provision { bucket, reply } => self.provision(bucket, reply),
            BucketMsg::Restore { bucket } => self.slots.entry(bucket).or_default().restore = true,
        }
    }

    /// Attach a waiter to the bucket's in-flight provisioning, starting it if this is the first.
    /// Provisioning bypasses the slot machinery — it neither serializes against the bucket's worker
    /// nor waits for it — so it is safe only because it exclusively *creates*, idempotently, and
    /// only for a bucket the readiness probe already saw on the remote. Bucket *lifecycle* (whether
    /// a bucket should exist at all) stays the workers' alone.
    fn provision(&mut self, bucket: String, reply: oneshot::Sender<Result<()>>) {
        if self
            .states
            .load()
            .get(&bucket)
            .is_some_and(|s| s.provisioned)
        {
            let _ = reply.send(Ok(()));
            return;
        }
        let waiters = self.provisioning.entry(bucket.clone()).or_default();
        waiters.push(reply);
        if waiters.len() > 1 {
            return;
        }
        let tier = self.tier.clone();
        let done = self.prov_tx.clone();
        tokio::spawn(async move {
            let result = async {
                ensure_cache_bucket(&tier.data, &bucket).await?;
                ensure_cache_bucket(&tier.meta, &bucket).await
            }
            .await;
            let _ = done.send((bucket, result.map_err(|e| e.to_string())));
        });
    }

    /// Publish one provisioning round's outcome to every caller that coalesced onto it.
    fn finish_provision(&mut self, bucket: String, result: std::result::Result<(), String>) {
        if result.is_ok() {
            update(&self.states, &bucket, set_provisioned);
        }
        for reply in self.provisioning.remove(&bucket).unwrap_or_default() {
            let _ = reply.send(result.clone().map_err(Error::Backend));
        }
    }

    /// Hand each bucket with pending work — and no worker already draining it — to a fresh worker,
    /// taking the whole batch with it. Work that arrives during the drain accumulates in a new slot
    /// and is dispatched when the worker signals done, so a bucket is never drained by two workers
    /// at once (per-bucket serialization) and its batches run in arrival order.
    fn dispatch(&mut self) {
        let ready: Vec<String> = self
            .slots
            .iter()
            .filter(|(bucket, slot)| !slot.is_empty() && !self.running.contains(*bucket))
            .map(|(bucket, _)| bucket.clone())
            .collect();
        for bucket in ready {
            let slot = self.slots.remove(&bucket).expect("ready bucket has a slot");
            self.running.insert(bucket.clone());
            let worker = Worker {
                tier: self.tier.clone(),
                states: self.states.clone(),
                sem: self.sem.clone(),
                done: self.done_tx.clone(),
            };
            tokio::spawn(worker.run(bucket, slot));
        }
    }
}

struct Worker {
    tier: Reconciler,
    states: BucketStates,
    sem: Arc<Semaphore>,
    done: mpsc::UnboundedSender<String>,
}

impl Worker {
    async fn run(self, bucket: String, mut slot: Slot) {
        // One permit for the whole batch bounds concurrent buckets, not concurrent backend calls.
        let _permit = self.sem.acquire().await.expect("semaphore is never closed");
        while let Some(client) = slot.pending.pop_front() {
            match client {
                Client::Create(reply) => {
                    let _ = reply.send(self.create(&bucket).await);
                }
                Client::Delete(reply) => {
                    let _ = reply.send(self.delete(&bucket).await);
                }
            }
        }
        if slot.restore {
            self.restore(&bucket).await;
        }
        let _ = self.done.send(bucket);
    }

    /// The remote create is the sole commit. A brand-new bucket resets the cache substrate first —
    /// draining any stale orphan from a prior incarnation and provisioning empty projections — then
    /// marks itself reconciled, since an empty namespace is trivially authoritative. A duplicate
    /// create of a live bucket returns the remote's result and leaves cache and marker untouched
    /// (it may be mid-restore). Safe without a lock: the actor is the sole writer (§7).
    async fn create(&self, bucket: &str) -> Result<()> {
        match self.tier.remote.head_bucket(bucket).await {
            Ok(()) => self.tier.remote.create_bucket(bucket).await,
            Err(Error::NoSuchBucket) => {
                self.reset_cache(bucket).await?;
                self.tier.remote.create_bucket(bucket).await?;
                self.mark_reconciled(bucket).await?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// The remote delete is the commit and the emptiness gate; cache is drained best-effort after —
    /// a leftover projection is a cache-without-remote orphan a later restore/GC drops.
    async fn delete(&self, bucket: &str) -> Result<()> {
        self.tier.remote.delete_bucket(bucket).await?;
        // The commit already landed, so everything this process believed about the bucket is stale
        // no matter how the drain fares — retire it before, not after, or a failed drain would leave
        // reads trusting a dead bucket (and a stale `Restoring` would keep serving from a remote
        // bucket that is gone instead of answering `NoSuchBucket`).
        retire(&self.states, bucket);
        drain_and_delete_if_exists(&self.tier.data, bucket).await?;
        drain_and_delete_if_exists(&self.tier.meta, bucket).await?;
        Ok(())
    }

    /// Rebuild a bucket's cache from the remote, then flip it authoritative. Skips a bucket the
    /// remote no longer holds (a stray trigger for a deleted bucket). On failure the bucket stays
    /// unready, so the next access re-triggers.
    async fn restore(&self, bucket: &str) {
        self.sweep(bucket).await;
        // However the sweep ended, it is no longer pending: dropping the memo is what makes the
        // next access re-classify — re-triggering a failed sweep, or resolving `Absent` for a
        // bucket the remote no longer holds. A success already published `Ready`, which this leaves
        // untouched.
        update(&self.states, bucket, clear_restoring);
    }

    async fn sweep(&self, bucket: &str) {
        // Triggers that arrived while another sweep (or a create) was flipping this bucket ready
        // would re-run a full sweep over an already-authoritative namespace.
        if self.states.load().get(bucket).is_some_and(|s| s.is_ready()) {
            return;
        }
        if self.tier.remote.head_bucket(bucket).await.is_err() {
            return;
        }
        if let Err(e) = self.provision(bucket).await {
            tracing::warn!(bucket, error = %e, "restore could not provision cache; retry on next access");
            return;
        }
        match self.tier.restore_bucket(bucket).await {
            Ok(()) => update(&self.states, bucket, mark_ready),
            Err(e) => {
                tracing::warn!(bucket, error = %e, "bucket restore failed; retry on next access")
            }
        }
    }

    async fn reset_cache(&self, bucket: &str) -> Result<()> {
        update(&self.states, bucket, clear_provisioned);
        drain_and_delete_if_exists(&self.tier.data, bucket).await?;
        drain_and_delete_if_exists(&self.tier.meta, bucket).await?;
        self.provision(bucket).await
    }

    /// Create both projections and memoize that they exist, so the data plane's `provision` fast
    /// path answers from the set instead of re-probing the backend.
    async fn provision(&self, bucket: &str) -> Result<()> {
        ensure_cache_bucket(&self.tier.data, bucket).await?;
        ensure_cache_bucket(&self.tier.meta, bucket).await?;
        update(&self.states, bucket, set_provisioned);
        Ok(())
    }

    /// Write the sync marker and record the bucket ready — its namespace matches the remote.
    async fn mark_reconciled(&self, bucket: &str) -> Result<()> {
        self.tier
            .meta
            .put_small(
                bucket,
                &meta::sync_marker_key(),
                Vec::new(),
                HashMap::new(),
                None,
                None,
            )
            .await?;
        update(&self.states, bucket, mark_ready);
        Ok(())
    }
}

/// Create the cache bucket if absent. A concurrent creator racing us is tolerated: a failed create
/// that nonetheless leaves the bucket present is success — the actor's own workers and its
/// provisioning tasks both land here.
async fn ensure_cache_bucket(backend: &hypha_core::Backend, bucket: &str) -> Result<()> {
    match backend.head_bucket(bucket).await {
        Ok(()) => Ok(()),
        Err(Error::NoSuchBucket) => match backend.create_bucket(bucket).await {
            Ok(()) => Ok(()),
            Err(create) => backend.head_bucket(bucket).await.map_err(|_| create),
        },
        Err(e) => Err(e),
    }
}

/// Empty a cache bucket then delete it, tolerating the bucket already being gone at either step.
async fn drain_and_delete_if_exists(backend: &hypha_core::Backend, bucket: &str) -> Result<()> {
    match backend.head_bucket(bucket).await {
        Ok(()) => {}
        Err(Error::NoSuchBucket) => return Ok(()),
        Err(e) => return Err(e),
    }
    loop {
        let page = backend.list(bucket, None, None, None, None, None).await?;
        let keys: Vec<String> = page
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|o| o.key)
            .collect();
        if keys.is_empty() {
            break;
        }
        // Keys go one at a time: `<meta>`'s twin/mpu keys carry the 0x01 control byte the batch
        // DeleteObjects XML body can't represent (§6). Buckets are rare, so the per-key cost is fine.
        let deletes = keys.iter().map(|k| backend.delete(bucket, k));
        futures::future::try_join_all(deletes).await?;
        if page.is_truncated != Some(true) {
            break;
        }
    }
    match backend.delete_bucket(bucket).await {
        Ok(()) | Err(Error::NoSuchBucket) => Ok(()),
        Err(e) => Err(e),
    }
}
