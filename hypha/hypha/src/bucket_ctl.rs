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
//! DeleteBucket, and restore — is funnelled through this one actor, which makes its serialization
//! structural: per-bucket-serial (one worker drains a bucket's requests in arrival order) and
//! cross-bucket-parallel (distinct buckets proceed at once, bounded by [`MAX_CONCURRENT`]). Reads
//! never enter here; they consult the [ready set](BucketCtl::is_ready) instead.
//!
//! Client Create/Delete are request-reply and never coalesced — each returns the remote's own
//! result, so a double-delete's loser still sees `NoSuchBucket`. `Restore`s are fire-and-forget and
//! deduped: the op that triggered one already resolves from the remote meanwhile, so there is no
//! waiter, and a storm of restores for one bucket collapses to a single sweep.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot, Semaphore};

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::tier::Reconciler;

/// Cap on cache buckets being mutated at once. A slow DeleteBucket drain or a large restore holds a
/// slot for its duration, so this also bounds the head-of-line a lost-volume restore storm drains
/// against.
const MAX_CONCURRENT: usize = 16;

type ReadySet = Arc<Mutex<HashSet<String>>>;

enum BucketMsg {
    Create {
        bucket: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Delete {
        bucket: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Restore {
        bucket: String,
    },
}

/// Handle onto the actor. Cloneable and cheap — the queue sender plus the shared ready set — so
/// every `Hypha` clone shares one actor and one view of which buckets are cache-authoritative.
#[derive(Clone)]
pub struct BucketCtl {
    tx: mpsc::UnboundedSender<BucketMsg>,
    ready: ReadySet,
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

    /// Trigger a background restore: fire-and-forget. The caller resolves from the remote meanwhile;
    /// a closed queue (actor gone at shutdown) is ignored.
    pub fn restore(&self, bucket: &str) {
        let _ = self.tx.send(BucketMsg::Restore {
            bucket: bucket.to_string(),
        });
    }

    /// Whether the bucket's cache namespace is reconciled and authoritative. The read/write gate
    /// consults this before trusting a cache miss as a definitive answer (§7).
    pub fn is_ready(&self, bucket: &str) -> bool {
        self.ready.lock().unwrap().contains(bucket)
    }

    /// Record a bucket as ready — used by the gate when it discovers a persisted sync marker that
    /// this process hadn't yet observed.
    pub fn mark_ready(&self, bucket: &str) {
        self.ready.lock().unwrap().insert(bucket.to_string());
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
    let ready: ReadySet = Arc::new(Mutex::new(HashSet::new()));
    let actor = Actor {
        rx,
        done_tx,
        done_rx,
        tier,
        ready: ready.clone(),
        sem: Arc::new(Semaphore::new(MAX_CONCURRENT)),
        slots: HashMap::new(),
        running: HashSet::new(),
    };
    tokio::spawn(actor.run());
    BucketCtl { tx, ready }
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
    tier: Reconciler,
    ready: ReadySet,
    sem: Arc<Semaphore>,
    slots: HashMap<String, Slot>,
    running: HashSet<String>,
}

impl Actor {
    async fn run(mut self) {
        let mut ext_open = true;
        loop {
            // Shutdown completes only once the queue is fully drained and no worker is in flight.
            if !ext_open && self.slots.is_empty() && self.running.is_empty() {
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
            BucketMsg::Restore { bucket } => self.slots.entry(bucket).or_default().restore = true,
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
                ready: self.ready.clone(),
                sem: self.sem.clone(),
                done: self.done_tx.clone(),
            };
            tokio::spawn(worker.run(bucket, slot));
        }
    }
}

struct Worker {
    tier: Reconciler,
    ready: ReadySet,
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
        // The commit already landed, so the ready entry is stale no matter how the drain fares —
        // drop it before, not after, or a failed drain would leave reads trusting a dead bucket.
        self.ready.lock().unwrap().remove(bucket);
        drain_and_delete_if_exists(&self.tier.data, bucket).await?;
        drain_and_delete_if_exists(&self.tier.meta, bucket).await?;
        Ok(())
    }

    /// Rebuild a bucket's cache from the remote, then flip it authoritative. Skips a bucket the
    /// remote no longer holds (a stray trigger for a deleted bucket). On failure the bucket stays
    /// unready, so the next access re-triggers.
    async fn restore(&self, bucket: &str) {
        // Triggers that arrived while another sweep (or a create) was flipping this bucket ready
        // would re-run a full sweep over an already-authoritative namespace.
        if self.ready.lock().unwrap().contains(bucket) {
            return;
        }
        if self.tier.remote.head_bucket(bucket).await.is_err() {
            return;
        }
        if let Err(e) = ensure_cache_bucket(&self.tier.data, bucket).await {
            tracing::warn!(bucket, error = %e, "restore could not provision cache; retry on next access");
            return;
        }
        if let Err(e) = ensure_cache_bucket(&self.tier.meta, bucket).await {
            tracing::warn!(bucket, error = %e, "restore could not provision cache; retry on next access");
            return;
        }
        match self.tier.restore_bucket(bucket).await {
            Ok(()) => {
                self.ready.lock().unwrap().insert(bucket.to_string());
            }
            Err(e) => {
                tracing::warn!(bucket, error = %e, "bucket restore failed; retry on next access")
            }
        }
    }

    async fn reset_cache(&self, bucket: &str) -> Result<()> {
        drain_and_delete_if_exists(&self.tier.data, bucket).await?;
        drain_and_delete_if_exists(&self.tier.meta, bucket).await?;
        ensure_cache_bucket(&self.tier.data, bucket).await?;
        ensure_cache_bucket(&self.tier.meta, bucket).await
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
        self.ready.lock().unwrap().insert(bucket.to_string());
        Ok(())
    }
}

/// Create the cache bucket if absent. A concurrent creator racing us is tolerated: a failed create
/// that nonetheless leaves the bucket present is success. Idempotent and race-safe, so the restore
/// overlay's write path ([`Hypha::prepare_write`](crate::s3::Hypha)) may also call it to provision
/// ahead of a background restore without breaking the actor's ownership of bucket *lifecycle*.
pub(crate) async fn ensure_cache_bucket(backend: &hypha_core::Backend, bucket: &str) -> Result<()> {
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
