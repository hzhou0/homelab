//! Pending-marker obligations and the clean marker (§6/§7).
//!
//! A cached write acks on its **cache body write** — that is the commit. The bare-`K` pending marker
//! it owes is an *index* over the pending set, not the durability record: what makes a write pending
//! is a live body whose generation the remote lacks, or a delete-tombstone the remote has not
//! honoured, both derivable from cache and remote alone. So a marker that cannot be written delays
//! durability rather than losing the write, and none of the paths here may turn one into a client
//! error — repairing a write *behind* a returned error would finish a write hypha reported as
//! failed, which a client relying on that error cannot survive.
//!
//! Three pieces, in the order a run meets them:
//!
//! 1. **Startup** ([`Markers::startup`]) reads and deletes every bucket's clean marker before the
//!    listener opens, recording which were present. Every bucket therefore starts *dirty on disk*,
//!    with no bookkeeping of what a run "touched" — a run that has to remember what it touched can
//!    forget. A bucket whose marker was absent owes a reconcile pass, which this raises on the
//!    bucket-control actor rather than running here: rebuilding the pending set is the same
//!    traversal restore runs, and one queue is what keeps the two from becoming two passes over
//!    independent snapshots (§7, [`crate::bucket_ctl`]).
//! 2. **The queue** ([`Markers::owe`]) — a write hands its marker over and returns. The handover
//!    cannot block or fail, because the body is already committed and the ack can neither wait on
//!    the marker nor be turned into an error by it; that is the whole reason the queue is unbounded.
//! 3. **The drain** ([`Worker::run`]) writes clean markers, and only for buckets this run accounted
//!    for — marker present at startup, or a reconcile pass that landed during the run — and only if
//!    nothing was still owed when it sealed.
//!
//! **Quiescence.** The clean marker claims the pending set on disk is complete, so writing one while
//! a write still owes a marker turns a recoverable gap into a permanent one. Deciding that takes two
//! things, and neither is an observation of the queue's depth.
//!
//! *Ordering*: hyper's connection drain resolves only once every handler has returned and no new one
//! can start, and every other sender is handler-local — [`Markers::owe`] upgrades the weak handle,
//! sends, and drops it before returning. So after the drain nothing but [`Queue`] can enqueue, and
//! the [`Msg::Seal`] it sends is necessarily behind every marker of the run.
//!
//! *Intent*: the seal is a **message, not the channel closing**. The serving future owns the
//! `Lifecycle` that owns the `Queue`, so an aborted or panicking server closes the channel exactly
//! as a drain would — and a killed process that wrote clean markers on its way out would rob the
//! next run of the recovery scan that was supposed to catch what it dropped. Closure therefore ends
//! the worker; only a seal authorizes it to vouch for anything.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashSet;
use futures::StreamExt as _;
use tokio::sync::mpsc;

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::bucket_ctl::{BucketCtl, Trigger};
use crate::tier::Reconciler;

/// Markers taken off the queue per wake-up.
const BATCH: usize = 256;

/// What the queue carries: markers to write, and the one message that says the run is ending
/// *gracefully*. Closure alone cannot say that — the serving future owns the [`Queue`], so an
/// aborted task drops it and closes the channel exactly like a drain would. Only an explicit seal
/// distinguishes the two, and FIFO puts it behind every marker enqueued before it.
enum Msg {
    Owed(Repair),
    Seal,
}

/// A key owed a pending marker.
struct Repair {
    bucket: String,
    key: String,
    /// The marker payload — a body ETag for a PUT, the delete sentinel's for a cached delete.
    /// Diagnostic only: the sweep classifies K from the *data* body and CASes on the marker's own
    /// ETag, so any payload is as good as the last (§7).
    payload: String,
}

impl Repair {
    fn dedup_key(&self) -> (String, String) {
        (self.bucket.clone(), self.key.clone())
    }
}

struct Inner {
    tier: Reconciler,
    /// Weak on purpose: the service must not hold the channel open, or closing it would prove
    /// nothing. Senders are upgraded per write and dropped before the handler returns, so once the
    /// connections drain the serving loop's [`Queue`] is the only one left.
    tx: mpsc::WeakUnboundedSender<Msg>,
    /// The run's only per-bucket state: buckets whose pending set it accounts for, because their
    /// clean marker was present at startup or the recovery scan rebuilt it. Membership is the
    /// positive evidence the drain needs; **absence is the default**, so a bucket this run
    /// established nothing about — including one left dirty by an earlier crash and untouched since
    /// — simply is not in here and ends the run dirty.
    scanned: DashSet<String>,
}

/// The write path's handle: hand over owed markers, and (at startup) establish the per-bucket state
/// the drain later decides on.
#[derive(Clone)]
pub(crate) struct Markers {
    inner: Arc<Inner>,
}

pub(crate) fn spawn(
    tier: Reconciler,
    retry: Duration,
    concurrency: usize,
) -> (Markers, Queue, Worker) {
    // Unbounded because the enqueue sits on the write path *after* the commit: a bounded queue would
    // either block the ack behind the marker or shed it, and shedding needs a side channel to record
    // the loss — state whose only job is to be remembered on a failure path. An enqueue that cannot
    // fail needs none. Depth is an outage symptom rather than a tunable, and `markers_owed` (§10) is
    // where it shows.
    let (tx, rx) = mpsc::unbounded_channel();
    let inner = Arc::new(Inner {
        tier,
        tx: tx.downgrade(),
        scanned: DashSet::new(),
    });
    (
        Markers {
            inner: inner.clone(),
        },
        Queue(tx),
        Worker {
            inner,
            rx,
            retry: retry.max(Duration::from_millis(1)),
            concurrency: concurrency.max(1),
        },
    )
}

/// Holds the marker queue open for the life of the run.
///
/// Every other sender is short-lived and handler-local — [`Markers::owe`] upgrades the weak handle,
/// sends, and drops it before the handler returns — so once hyper's connection drain resolves this
/// is the only one left, and nothing can enqueue behind what it sends. That is what makes
/// [`Self::seal`] the last word: FIFO puts it after every marker of the run, with no join over
/// stray tasks needed, because nothing but a handler ever sends.
///
/// **Dropping this is not sealing it.** The serving future owns the `Lifecycle` that owns this, so
/// an aborted or panicking server drops it and closes the channel exactly as a drain would; if
/// closure alone authorized the clean markers, a killed process would write them on its way out and
/// the next run would skip its recovery scan. Only the explicit message says the run ended
/// gracefully.
pub(crate) struct Queue(mpsc::UnboundedSender<Msg>);

impl Queue {
    /// End the run gracefully: the worker writes clean markers only for a seal it actually received.
    pub(crate) fn seal(self) {
        let _ = self.0.send(Msg::Seal);
    }
}

impl Markers {
    /// Hand an acked write's marker to the queue. Called from the write path before it returns, so
    /// it must not block or fail: the body is already committed and the ack cannot wait on — or be
    /// turned into an error by — anything that happens to the marker. That is what the queue being
    /// unbounded buys, and the only reason it is.
    pub(crate) fn owe(&self, bucket: &str, key: &str, payload: String) {
        let Some(tx) = self.inner.tx.upgrade() else {
            // The channel closes only after every handler has returned (§7), so a live write cannot
            // reach this — but "cannot" is exactly what a clean marker must not assume. Withdrawing
            // the bucket's evidence is the whole remedy: no evidence, no clean marker.
            tracing::error!(bucket, key, "marker queue closed under a live write");
            self.inner.scanned.remove(bucket);
            return;
        };
        let _ = tx.send(Msg::Owed(Repair {
            bucket: bucket.to_string(),
            key: key.to_string(),
            payload,
        }));
    }

    /// Record that this run accounts for `bucket`'s pending set — the positive evidence the drain
    /// needs before it may write that bucket's clean marker (§6). Two things establish it: the
    /// bucket's marker was present at startup, or this run rebuilt the set (a reconcile pass, or
    /// creating the bucket empty).
    pub(crate) fn account_for(&self, bucket: &str) {
        self.inner.scanned.insert(bucket.to_string());
    }

    /// Read and delete every bucket's clean marker, then owe a reconcile pass for each that was
    /// absent. Runs before the listener opens: from the moment hypha can take a write, no bucket on
    /// disk claims to be clean.
    ///
    /// A marker that cannot be deleted fails startup rather than being served around — skipping the
    /// pass on a marker one then fails to delete would skip it again next run, by which time real
    /// orphans exist.
    ///
    /// The pass itself is the bucket-control actor's ([`crate::bucket_ctl`]), not this module's: a
    /// markerless bucket needs its pending set rebuilt from cache and remote, which is the same
    /// traversal restore already runs, and routing both through the actor is what keeps them one
    /// pass over one map rather than two racing snapshots. The actor records `account_for` when it
    /// lands. Triggers are fire-and-forget, so startup does not wait on them — a pass that fails
    /// leaves the bucket dirty and the next run tries again.
    pub(crate) async fn startup(&self, buckets: &BucketCtl) -> Result<()> {
        let clean = meta::clean_marker_key();
        for (bucket, _) in self.inner.tier.meta.list_buckets().await? {
            let present = match self.inner.tier.meta.head(&bucket, &clean).await {
                Ok(_) => {
                    self.inner.tier.meta.delete(&bucket, &clean).await?;
                    true
                }
                Err(Error::NotFound) => false,
                Err(e) => return Err(e),
            };
            if present {
                self.account_for(&bucket);
            } else {
                buckets.reconcile(&bucket, Trigger::Unaccounted);
            }
        }
        Ok(())
    }
}

/// Drains the repair queue for the life of the process, then seals.
pub(crate) struct Worker {
    inner: Arc<Inner>,
    rx: mpsc::UnboundedReceiver<Msg>,
    retry: Duration,
    concurrency: usize,
}

impl Worker {
    /// Write owed markers as they arrive, retrying failures on `retry`, until the channel closes.
    /// Then one final attempt — never a retry loop, since the drain does not wait out a backoff —
    /// and the clean markers.
    ///
    /// A marker still owed after that final attempt means the run did not end gracefully, so it
    /// vouches for *nothing*: the next run rescans every bucket rather than this one guessing which
    /// buckets the loss touched.
    pub(crate) async fn run(mut self) {
        let mut owed: HashMap<(String, String), Repair> = HashMap::new();
        let mut batch = Vec::with_capacity(BATCH);
        let mut sealed = false;
        'outer: loop {
            tokio::select! {
                // Batched because the queue is every write's path to its marker: one wake-up takes
                // whatever a burst deposited.
                n = self.rx.recv_many(&mut batch, BATCH) => {
                    if n == 0 {
                        break; // dropped rather than sealed — the run did not end gracefully
                    }
                    for msg in batch.drain(..) {
                        match msg {
                            Msg::Owed(r) => { owed.insert(r.dedup_key(), r); }
                            Msg::Seal => { sealed = true; }
                        }
                    }
                    self.write_all(&mut owed).await;
                    if sealed {
                        break 'outer;
                    }
                }
                () = tokio::time::sleep(self.retry), if !owed.is_empty() => {
                    self.write_all(&mut owed).await;
                }
            }
        }
        self.write_all(&mut owed).await;
        match (sealed, owed.is_empty()) {
            (true, true) => self.mark_clean().await,
            (true, false) => tracing::warn!(
                owed = owed.len(),
                "markers still owed at drain; no clean markers written"
            ),
            (false, _) => tracing::warn!("marker queue closed without a drain; no clean markers"),
        }
    }

    /// Write every owed marker, dropping the ones that land. Concurrent because a marker is on each
    /// acked write's durability path: serializing them would make the queue the write path's
    /// throughput ceiling.
    async fn write_all(&self, owed: &mut HashMap<(String, String), Repair>) {
        let failed: Vec<Repair> = futures::stream::iter(owed.drain().map(|(_, r)| r))
            .map(|r| async move {
                match self
                    .inner
                    .tier
                    .raise_marker(&r.bucket, &r.key, &r.payload)
                    .await
                {
                    Ok(()) => None,
                    Err(e) => {
                        tracing::warn!(bucket = r.bucket, key = r.key, error = %e, "marker write failed; retrying");
                        Some(r)
                    }
                }
            })
            .buffer_unordered(self.concurrency)
            .filter_map(|f| async move { f })
            .collect()
            .await;
        for r in failed {
            owed.insert(r.dedup_key(), r);
        }
    }

    /// Write the clean marker for each bucket this run accounted for, and for no other. Reached only
    /// on a graceful drain with nothing owed, so membership in `scanned` is the whole condition — a
    /// bucket left dirty by an earlier crash and untouched by this run is simply absent, and its
    /// orphans stay findable instead of buried.
    async fn mark_clean(&self) {
        let clean = meta::clean_marker_key();
        for bucket in self.inner.scanned.iter() {
            let bucket = bucket.key();
            if let Err(e) = self
                .inner
                .tier
                .meta
                .put_small(bucket, &clean, Vec::new(), HashMap::new(), None, None)
                .await
            {
                tracing::warn!(bucket, error = %e, "clean marker not written; next run scans");
            }
        }
    }
}
