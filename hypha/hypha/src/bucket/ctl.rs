//! The bucket-control actor: the sole writer of the cache substrate, and where a bucket's phase
//! ([`super`]) is decided and published.
//!
//! Rather than guard the three-way divergence with locks, every substrate *mutation* — CreateBucket,
//! DeleteBucket, provisioning, and recovery — is funnelled through this one actor, which makes its
//! serialization structural: per-bucket-serial (one task drains a bucket's requests in arrival
//! order) and cross-bucket-parallel (distinct buckets proceed at once, bounded by
//! [`MAX_CONCURRENT`]). Reads never enter here; they read the actor's published
//! [state map](BucketCtl::state) instead, which costs one atomic load and no lock.
//!
//! Client Create/Delete are request-reply and never coalesced — each returns the remote's own
//! result, so a double-delete's loser still sees `NoSuchBucket`. Recoveries are fire-and-forget and
//! deduped: the op that triggered one already resolves from the remote meanwhile, so there is no
//! waiter, and a storm of triggers for one bucket collapses to a single pass.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::{Id, JoinError, JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use super::{rebuild, restore};
use crate::gc::orphans;
use crate::halt::{Invariant, Violation};
use crate::tier::Tiering;

/// Cap on cache buckets being mutated at once. A slow DeleteBucket drain or a large restore holds a
/// slot for its duration, so this also bounds the head-of-line a lost-volume restore storm drains
/// against.
const MAX_CONCURRENT: usize = 16;

/// How long a bucket whose recovery could not run waits before the pass is re-queued. Nothing else
/// re-triggers one: the state map is resolved once at startup and never re-probed, so a pass that
/// fails has to own its own retry or the bucket stays non-authoritative for the run.
const RECOVERY_RETRY: Duration = Duration::from_secs(5);

/// Every bucket this process has classified, published as one immutable map: readers take a single
/// atomic load, no lock, on a path every request crosses. Mutations are copy-on-write through
/// [`ArcSwap::rcu`] — a whole-map clone per bucket-lifecycle event, which is the right trade when
/// the map holds tens of entries and is read once or twice per request.
type BucketStates = Arc<ArcSwap<HashMap<String, BucketState>>>;

/// What the data plane knows about one bucket (§7). Keeping the facts in one value is what makes a
/// transition a single atomic publish — `Ready` replacing `Restoring` can't be observed half-applied,
/// and no ordering convention between separate sets has to be maintained by hand. It is also why
/// [`Self::accounted`] lives here rather than in [`crate::markers`]: retiring a deleted bucket then
/// drops its accounting for free, instead of leaving a second structure to remember to clear.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BucketState {
    readiness: Readiness,
    /// Both cache projections are known to exist, so a write can skip asking the actor.
    provisioned: bool,
    /// This run accounts for the bucket's pending-marker set (§6) — its clean marker was present at
    /// startup, a reconcile pass rebuilt the set, or this run created the bucket empty. Positive
    /// evidence only: **absence is the default**, so a bucket left dirty by an earlier crash and
    /// untouched since simply is not accounted and ends the run dirty.
    accounted: bool,
    /// The same claim for orphaned shadow bodies (§8), tracked separately because the two failures cost
    /// wildly different recoveries: an unaccounted pending set means a full two-cursor rebuild, an
    /// unaccounted shadow range means one prefix listing. Folding them together would let a few leaked
    /// bytes trigger the expensive pass.
    shadows_accounted: bool,
}

/// A bucket's source of truth for the data plane, and the whole of what the overlay branches on.
/// Each phase is a claim the rest of the crate relies on; [`super`] states them.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum Readiness {
    #[default]
    Absent,
    /// Sync marker absent: a restore is owed or in flight.
    Restoring,
    /// Sync marker observed.
    Ready,
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

fn set_restoring(state: &mut BucketState) {
    state.readiness = Readiness::Restoring;
}

/// The sync marker is present: the cache namespace is authoritative. Supersedes `Restoring` in the
/// same publish, so no reader can see the bucket as neither.
fn mark_ready(state: &mut BucketState) {
    state.readiness = Readiness::Ready;
}

fn set_provisioned(state: &mut BucketState) {
    state.provisioned = true;
}

fn clear_provisioned(state: &mut BucketState) {
    state.provisioned = false;
}

fn set_accounted(state: &mut BucketState) {
    state.accounted = true;
}

fn clear_accounted(state: &mut BucketState) {
    state.accounted = false;
}

fn set_shadows_accounted(state: &mut BucketState) {
    state.shadows_accounted = true;
}

fn clear_shadows_accounted(state: &mut BucketState) {
    state.shadows_accounted = false;
}

/// Forget a bucket entirely — it no longer exists, so the next access re-classifies from scratch and
/// the drain has nothing to vouch for (a clean marker written into a deleted bucket's projection
/// would resurrect it).
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
    Recover {
        bucket: String,
        pass: Recovery,
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
    /// Answers with the remote create's own result, so a duplicate create surfaces the remote's error.
    pub async fn create(&self, bucket: &str) -> Result<()> {
        self.request(|reply| BucketMsg::Create {
            bucket: bucket.to_string(),
            reply,
        })
        .await
    }

    /// The remote delete is the emptiness gate, so a non-empty bucket surfaces here as the remote's
    /// own error.
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

    /// Publishing `Restoring` is both the readiness answer for the window and the record that the
    /// bucket exists at all.
    pub(crate) fn restore(&self, bucket: &str) {
        update(&self.states, bucket, set_restoring);
        self.owe(bucket, Recovery::Restore);
    }

    /// The bucket stays `Ready` — its namespace is authoritative throughout, and only the pending
    /// index is in doubt.
    pub(crate) fn rebuild_pending(&self, bucket: &str) {
        self.owe(bucket, Recovery::RebuildPending);
    }

    fn owe(&self, bucket: &str, pass: Recovery) {
        // A closed queue means the actor is gone, i.e. shutdown. A bucket left `Restoring` serves
        // correctly, and the next run resolves it again from scratch.
        let _ = self.tx.send(BucketMsg::Recover {
            bucket: bucket.to_string(),
            pass,
        });
    }

    /// This process's current view of a bucket — one atomic load, no lock, on a path every request
    /// crosses. The overlay's whole classification (§7), with no backend call behind it: the map is
    /// resolved in full at startup ([`resolve_all`]) and maintained by Create/Delete since.
    pub fn readiness(&self, bucket: &str) -> Readiness {
        self.state(bucket).readiness
    }

    fn state(&self, bucket: &str) -> BucketState {
        self.states.load().get(bucket).copied().unwrap_or_default()
    }

    pub fn mark_ready(&self, bucket: &str) {
        update(&self.states, bucket, mark_ready);
    }

    /// Every bucket currently serving from its cache — the set the volume watchdog polls.
    pub(crate) fn ready(&self) -> Vec<String> {
        self.states
            .load()
            .iter()
            .filter(|(_, s)| s.readiness == Readiness::Ready)
            .map(|(bucket, _)| bucket.clone())
            .collect()
    }

    /// Record that this run accounts for `bucket`'s pending set (§6). Startup calls it for a bucket
    /// whose clean marker was present; the task calls it for a bucket a pass rebuilt or a create
    /// established empty.
    pub(crate) fn account_for(&self, bucket: &str) {
        update(&self.states, bucket, set_accounted);
    }

    /// Withdraw the accounting — the run can no longer vouch for the bucket's pending set, so it
    /// must end dirty. No evidence, no clean marker.
    pub(crate) fn unaccount(&self, bucket: &str) {
        update(&self.states, bucket, clear_accounted);
    }

    /// Every bucket this run accounts for, and no other — the drain's whole condition for writing a
    /// clean marker ([`crate::markers`]). A bucket deleted mid-run was retired from the map, so it
    /// cannot appear here.
    pub(crate) fn accounted(&self) -> Vec<String> {
        self.states
            .load()
            .iter()
            .filter(|(_, s)| s.accounted)
            .map(|(bucket, _)| bucket.clone())
            .collect()
    }

    /// The shadow-range equivalent (§8): its marker was present at startup, or this run's backstop
    /// sweep judged every shadow in it.
    pub(crate) fn account_shadows_for(&self, bucket: &str) {
        update(&self.states, bucket, set_shadows_accounted);
    }

    pub(crate) fn unaccount_shadows(&self, bucket: &str) {
        update(&self.states, bucket, clear_shadows_accounted);
    }

    pub(crate) fn shadows_accounted(&self) -> Vec<String> {
        self.states
            .load()
            .iter()
            .filter(|(_, s)| s.shadows_accounted)
            .map(|(bucket, _)| bucket.clone())
            .collect()
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

/// Resolve every bucket's state before the listener opens, and dispatch the recovery each one needs.
///
/// hypha owns both backends outright — nothing else creates a bucket in either — so the remote's
/// bucket list *is* the set of buckets, and one pass over it replaces the per-bucket probe the read
/// path used to pay on first touch. Buckets appear afterwards only through `CreateBucket`, which
/// publishes its own entry.
///
/// Reading both markers in one place is sound precisely because nothing is being served yet — no
/// marker can move underneath the decision, and there is no second raiser to reconcile with.
///
/// A cache bucket with no remote bucket is not resolved and never served: it is debris from a crash
/// between `delete`'s remote commit and its cache drain, and a later create of the same name resets
/// it.
///
/// The shadow sweeps it dispatches are returned rather than detached: each one only earns its bucket's
/// accounting by finishing, so the drain joins them before it reads that accounting back (§8).
pub(crate) async fn resolve_all(tier: &Tiering, buckets: &BucketCtl) -> Result<JoinSet<()>> {
    let mut sweeps = JoinSet::new();
    for (bucket, _) in tier.remote.list_buckets().await? {
        let synced = marker_present(tier, &bucket, &meta::sync_marker_key()).await?;
        // Durable mode has neither a pending set nor shadow bodies: it writes no clean markers of
        // either kind and reads none back.
        let accounted =
            tier.cached && take_marker(tier, &bucket, &meta::clean_marker_key()).await?;
        let shadows_accounted =
            tier.cached && take_marker(tier, &bucket, &meta::shadow_clean_marker_key()).await?;

        if !synced {
            buckets.restore(&bucket);
            continue;
        }
        buckets.mark_ready(&bucket);
        if accounted {
            buckets.account_for(&bucket);
        } else if tier.cached {
            buckets.rebuild_pending(&bucket);
        }
        // Not a `Recovery`: that slot is a single one per bucket, deliberately, so that "never both a
        // restore and a rebuild" is structural — and a bucket can owe a shadow sweep alongside either.
        // The sweep needs none of the slot's serialization anyway: it takes no lock, writes only in the
        // shadow range, and every reclaim it makes is idempotent.
        if shadows_accounted {
            buckets.account_shadows_for(&bucket);
        } else if tier.cached {
            orphans::dispatch_sweep(&mut sweeps, tier.clone(), buckets.clone(), bucket);
        }
    }
    Ok(sweeps)
}

async fn marker_present(tier: &Tiering, bucket: &str, key: &str) -> Result<bool> {
    match tier.meta.head(bucket, key).await {
        Ok(_) => Ok(true),
        // `NoSuchBucket`: the whole cache projection is gone — the volume loss R1 exists for.
        Err(Error::NotFound) | Err(Error::NoSuchBucket) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Read one of the two clean markers and delete it: from the moment hypha can take a write, no bucket
/// on disk claims to be clean. A marker that will not delete fails startup rather than being served
/// around — skipping the recovery on it now would skip it again next run, by which time real orphans
/// exist.
async fn take_marker(tier: &Tiering, bucket: &str, key: &str) -> Result<bool> {
    if !marker_present(tier, bucket, key).await? {
        return Ok(false);
    }
    tier.meta.delete(bucket, key).await?;
    Ok(true)
}

/// Spawn the actor and return its handle beside the task, which drains whatever is still queued —
/// pending Create/Delete complete, an in-flight recovery finishes — before exiting.
///
/// Shutdown arrives as `shutdown`, not as the queue closing: the actor keeps a sender of its own so a
/// recovery can re-queue itself, so its receiver never runs out of senders and closure is not a signal
/// it can observe.
pub fn spawn(tier: Tiering, shutdown: CancellationToken) -> (BucketCtl, JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let states: BucketStates = Arc::new(ArcSwap::from_pointee(HashMap::new()));
    let actor = BucketActor {
        rx,
        tx: tx.clone(),
        shutdown,
        tier,
        states: states.clone(),
        sem: Arc::new(Semaphore::new(MAX_CONCURRENT)),
        queued: HashMap::new(),
        batches: JoinSet::new(),
        running: HashMap::new(),
        provisions: JoinSet::new(),
        provisioning: HashMap::new(),
    };
    let task = tokio::spawn(actor.run());
    (BucketCtl { tx, states }, task)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Recovery {
    Restore,
    RebuildPending,
}

/// `Create`/`Delete` keep arrival order; the recovery is a **single slot**, which is what makes
/// "never both" structural. Nothing is ever merged into it: startup picks one pass per bucket, and
/// a retry re-queues that same pass.
#[derive(Default)]
struct QueuedWork {
    requests: VecDeque<LifecycleRequest>,
    recovery: Option<Recovery>,
}

impl QueuedWork {
    fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.recovery.is_none()
    }
}

/// Whether a recovery finished with the bucket resolved, or needs re-queueing.
#[derive(PartialEq, Eq)]
enum Outcome {
    Done,
    Retry,
}

enum LifecycleRequest {
    Create(oneshot::Sender<Result<()>>),
    Delete(oneshot::Sender<Result<()>>),
}

/// Stringified because it crosses a task boundary and is fanned out to every waiter, so it has to be
/// `Clone` — [`Error`] is not.
type ProvisionResult = std::result::Result<(), String>;

/// Everyone waiting on one bucket's in-flight provisioning. The entry *is* the coalescing: only the
/// caller that creates it spawns the backend work.
struct Provisioning {
    task: Id,
    waiters: Vec<oneshot::Sender<Result<()>>>,
}

struct BucketActor {
    rx: mpsc::UnboundedReceiver<BucketMsg>,
    /// Handed to each task so a recovery that could not run can re-queue itself.
    tx: mpsc::UnboundedSender<BucketMsg>,
    shutdown: CancellationToken,
    tier: Tiering,
    states: BucketStates,
    sem: Arc<Semaphore>,
    queued: HashMap<String, QueuedWork>,
    /// The bucket batches in flight. Joined rather than signalled over a channel because a *panicking*
    /// task sends nothing, and a completion the actor never observes would leave its bucket marked as
    /// draining for the rest of the run — every later request for it queued behind a task that is
    /// already gone. A join reports the panic instead.
    batches: JoinSet<()>,
    /// Which bucket each in-flight batch belongs to. Bucket-keyed because "is this bucket already
    /// draining?" is the question [`Self::dispatch`] asks; the reverse lookup is a scan over at most
    /// [`MAX_CONCURRENT`] entries and only a panic needs it.
    running: HashMap<String, Id>,
    /// Provisioning runs *outside* the per-bucket task — the task for a lost-volume bucket is busy with
    /// the restore sweep, and a write must not queue behind it (§7 — serving is never gated) — so it is
    /// its own set of tasks with its own waiters.
    provisions: JoinSet<ProvisionResult>,
    provisioning: HashMap<String, Provisioning>,
}

impl BucketActor {
    async fn run(mut self) {
        let mut ext_open = true;
        loop {
            // Shutdown completes only once the queue is fully drained and nothing is in flight: the
            // join sets abort whatever they still hold when they drop, so returning earlier would cut
            // a batch off mid-backend-call.
            if !ext_open
                && self.queued.is_empty()
                && self.running.is_empty()
                && self.provisioning.is_empty()
            {
                break;
            }
            // The join arms are guarded because an empty `JoinSet` yields `None` at once, which would
            // disable the branch — and with the external queue closed there would be nothing left for
            // `select!` to wait on.
            tokio::select! {
                msg = self.rx.recv(), if ext_open => match msg {
                    Some(msg) => self.enqueue(msg),
                    None => ext_open = false,
                },
                // Everything already queued still runs — this only stops new work being taken. A
                // recovery retry that fires after this lands in a queue nobody reads again, which is
                // the same outcome as the process ending before its 5 s backoff elapsed.
                () = self.shutdown.cancelled(), if ext_open => {
                    tracing::debug!("shutdown signalled; draining the bucket queue");
                    ext_open = false;
                }
                Some(done) = self.batches.join_next_with_id(), if !self.batches.is_empty() => {
                    self.finish_batch(done);
                }
                Some(done) = self.provisions.join_next_with_id(), if !self.provisions.is_empty() => {
                    self.finish_provision(done);
                }
            }
            self.dispatch();
        }
    }

    fn enqueue(&mut self, msg: BucketMsg) {
        match msg {
            BucketMsg::Create { bucket, reply } => self
                .queued
                .entry(bucket)
                .or_default()
                .requests
                .push_back(LifecycleRequest::Create(reply)),
            BucketMsg::Delete { bucket, reply } => self
                .queued
                .entry(bucket)
                .or_default()
                .requests
                .push_back(LifecycleRequest::Delete(reply)),
            BucketMsg::Provision { bucket, reply } => self.provision(bucket, reply),
            BucketMsg::Recover { bucket, pass } => {
                self.queued.entry(bucket).or_default().recovery = Some(pass)
            }
        }
    }

    /// Attach a waiter to the bucket's in-flight provisioning, starting it if this is the first.
    /// Provisioning bypasses the queueing machinery — it neither serializes against the bucket's
    /// task nor waits for it — so it is safe only because it exclusively *creates*,
    /// idempotently, and only for a bucket the readiness probe already saw on the remote. Bucket
    /// *lifecycle* (whether a bucket should exist at all) stays the tasks' alone.
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
        if let Some(inflight) = self.provisioning.get_mut(&bucket) {
            inflight.waiters.push(reply);
            return;
        }
        let tier = self.tier.clone();
        let provisioned = bucket.clone();
        let task = self
            .provisions
            .spawn(async move {
                async {
                    ensure_cache_bucket(&tier.data, &provisioned).await?;
                    ensure_cache_bucket(&tier.meta, &provisioned).await
                }
                .await
                .map_err(|e| e.to_string())
            })
            .id();
        self.provisioning.insert(
            bucket,
            Provisioning {
                task,
                waiters: vec![reply],
            },
        );
    }

    /// Answer the waiters and publish the bucket as provisioned if it landed. A task that panicked
    /// answers them with an error rather than leaving them to time out on a dropped `oneshot`, and —
    /// the part that matters — clears the entry, which otherwise coalesces every later caller onto a
    /// provisioning that will never report.
    fn finish_provision(&mut self, done: std::result::Result<(Id, ProvisionResult), JoinError>) {
        let (task, result) = match done {
            Ok((task, result)) => (task, result),
            Err(e) => (e.id(), Err(format!("provisioning task failed: {e}"))),
        };
        let Some(bucket) = self.provisioned_by(task) else {
            return;
        };
        if result.is_ok() {
            update(&self.states, &bucket, set_provisioned);
        }
        for reply in self
            .provisioning
            .remove(&bucket)
            .map(|p| p.waiters)
            .unwrap_or_default()
        {
            let _ = reply.send(result.clone().map_err(Error::Backend));
        }
    }

    fn provisioned_by(&self, task: Id) -> Option<String> {
        self.provisioning
            .iter()
            .find(|(_, p)| p.task == task)
            .map(|(bucket, _)| bucket.clone())
    }

    /// Release the bucket for its next batch. A panicking task leaves its batch's replies dropped —
    /// those callers see the actor as gone, which is the honest answer — but the bucket itself has to
    /// be released, or nothing would ever be dispatched for it again.
    fn finish_batch(&mut self, done: std::result::Result<(Id, ()), JoinError>) {
        let task = match done {
            Ok((task, ())) => task,
            Err(e) => {
                let bucket = self
                    .running
                    .iter()
                    .find(|(_, task)| **task == e.id())
                    .map(|(bucket, _)| bucket.as_str());
                tracing::error!(?bucket, error = %e, "bucket task did not finish; its batch is lost");
                e.id()
            }
        };
        self.running.retain(|_, running| *running != task);
    }

    /// Hand each bucket with pending work — and no task already draining it — to a fresh task, taking
    /// the whole batch with it. Work that arrives during the drain accumulates in a fresh entry and is
    /// dispatched once the batch is joined, so a bucket is never drained by two tasks at once
    /// (per-bucket serialization) and its batches run in arrival order.
    fn dispatch(&mut self) {
        let ready: Vec<String> = self
            .queued
            .iter()
            .filter(|(bucket, work)| !work.is_empty() && !self.running.contains_key(*bucket))
            .map(|(bucket, _)| bucket.clone())
            .collect();
        for bucket in ready {
            let work = self
                .queued
                .remove(&bucket)
                .expect("dispatchable bucket has queued work");
            let task = BucketTask {
                tier: self.tier.clone(),
                states: self.states.clone(),
                sem: self.sem.clone(),
                requeue: self.tx.clone(),
            };
            let spawned = self.batches.spawn(task.run(bucket.clone(), work)).id();
            self.running.insert(bucket, spawned);
        }
    }
}

struct BucketTask {
    tier: Tiering,
    states: BucketStates,
    sem: Arc<Semaphore>,
    requeue: mpsc::UnboundedSender<BucketMsg>,
}

impl BucketTask {
    async fn run(self, bucket: String, mut work: QueuedWork) {
        // One permit for the whole batch bounds concurrent buckets, not concurrent backend calls.
        let _permit = self.sem.acquire().await.expect("semaphore is never closed");
        while let Some(request) = work.requests.pop_front() {
            match request {
                LifecycleRequest::Create(reply) => {
                    let _ = reply.send(self.create(&bucket).await);
                }
                LifecycleRequest::Delete(reply) => {
                    let _ = reply.send(self.delete(&bucket).await);
                }
            }
        }
        if let Some(pass) = work.recovery {
            self.recover(&bucket, pass).await;
        }
    }

    /// The remote create is the sole commit, and the cache work all precedes it: an empty namespace
    /// is trivially authoritative, so the sync marker can be written before the bucket it vouches for
    /// exists. Everything published to the state map therefore follows the commit, which is what
    /// keeps a phase in the map equivalent to a bucket on the remote — a create that dies partway
    /// leaves cache projections nobody can reach, which the next create of the name resets.
    ///
    /// A duplicate create of a live bucket returns the remote's result and leaves cache and marker
    /// untouched (it may be mid-restore). Safe without a lock: the actor is the sole writer (§7).
    async fn create(&self, bucket: &str) -> Result<()> {
        match self.tier.remote.head_bucket(bucket).await {
            Ok(()) if self.state(bucket).readiness == Readiness::Absent => {
                // Invariant I6: hypha is the only writer of either backend, and the startup
                // resolution accounted for every bucket the remote held, so a remote bucket the map
                // has no phase for was created by something that is not this deployment.
                self.tier
                    .halt
                    .raise(Violation {
                        invariant: Invariant::ForeignBucket,
                        bucket: bucket.to_string(),
                        key: None,
                        detail:
                            "the remote holds a bucket hypha did not create and did not resolve \
                                 at startup; something other than this deployment is writing the \
                                 remote"
                                .to_string(),
                    })
                    .await
            }
            Ok(()) => self.tier.remote.create_bucket(bucket).await,
            Err(Error::NoSuchBucket) => {
                self.reset_cache(bucket).await?;
                self.write_sync_marker(bucket).await?;
                self.tier.remote.create_bucket(bucket).await?;
                update(&self.states, bucket, mark_ready);
                // A bucket this run created starts empty, so every write it can ever hold went
                // through the marker queue — which the drain proves empty before writing any clean
                // marker (§6). Accounted by construction, and only on this branch: a duplicate
                // create of a pre-existing bucket establishes nothing about a pending set that may
                // predate the run.
                update(&self.states, bucket, set_accounted);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn state(&self, bucket: &str) -> BucketState {
        self.states.load().get(bucket).copied().unwrap_or_default()
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

    /// Run `pass`, re-queueing it on the actor's own channel if it could not run.
    ///
    /// Re-queueing rather than sleeping in place: a retry that held the bucket's task would also hold
    /// its concurrency permit and block any Create/Delete behind it. The pass is unchanged on retry —
    /// the markers it was chosen from cannot have moved, since only this pass rewrites them.
    async fn recover(&self, bucket: &str, pass: Recovery) {
        if self.run_recovery(bucket, pass).await == Outcome::Retry {
            let requeue = self.requeue.clone();
            let bucket = bucket.to_string();
            tokio::spawn(async move {
                tokio::time::sleep(RECOVERY_RETRY).await;
                let _ = requeue.send(BucketMsg::Recover { bucket, pass });
            });
        }
    }

    async fn run_recovery(&self, bucket: &str, pass: Recovery) -> Outcome {
        if self.tier.remote.head_bucket(bucket).await.is_err() {
            // Deleted since startup resolved it. Retiring is what makes the map agree with the remote
            // again, and turns later requests into `NoSuchBucket` instead of remote reads.
            retire(&self.states, bucket);
            return Outcome::Done;
        }
        if let Err(e) = self.provision(bucket).await {
            tracing::warn!(bucket, error = %e, "recovery could not provision cache; retrying");
            return Outcome::Retry;
        }
        match pass {
            Recovery::Restore => self.run_restore(bucket).await,
            Recovery::RebuildPending => self.run_rebuild_pending(bucket).await,
        }
    }

    /// The sync marker is the commit: it is written only once the namespace is rebuilt, and the
    /// accounting rides along with it — a restore leaves the pending set empty and complete by
    /// construction ([`restore`]).
    async fn run_restore(&self, bucket: &str) -> Outcome {
        if let Err(e) = restore::namespace(&self.tier, bucket).await {
            tracing::warn!(bucket, error = %e, "namespace restore failed; retrying");
            return Outcome::Retry;
        }
        if let Err(e) = self.mark_reconciled(bucket).await {
            tracing::warn!(bucket, error = %e, "sync marker not written; retrying");
            return Outcome::Retry;
        }
        update(&self.states, bucket, set_accounted);
        tracing::info!(bucket, "namespace restore complete");
        Outcome::Done
    }

    async fn run_rebuild_pending(&self, bucket: &str) -> Outcome {
        match rebuild::pending_set(&self.tier, bucket).await {
            Ok(raised) => {
                update(&self.states, bucket, set_accounted);
                tracing::info!(bucket, markers = raised, "pending-set rebuild complete");
                Outcome::Done
            }
            Err(e) => {
                tracing::warn!(bucket, error = %e, "pending-set rebuild failed; retrying");
                Outcome::Retry
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

    async fn write_sync_marker(&self, bucket: &str) -> Result<()> {
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
        Ok(())
    }

    async fn mark_reconciled(&self, bucket: &str) -> Result<()> {
        self.write_sync_marker(bucket).await?;
        update(&self.states, bucket, mark_ready);
        Ok(())
    }
}

/// A concurrent creator racing us is tolerated: a failed create that nonetheless leaves the bucket
/// present is success — the actor's own tasks and its provisioning tasks both land here.
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
