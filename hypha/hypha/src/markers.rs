//! Pending-marker obligations and the clean marker (§6/§7 have the full rationale — a marker is an
//! index over the pending set, not the durability record, so none of the paths here may turn a
//! failed marker write into a client error).
//!
//! Three pieces, in the order a run meets them:
//!
//! 1. **Startup** reads and clears the clean markers, but does it in
//!    [`crate::bucket_ctl::resolve_all`], which is already reading the other marker of the same pair
//!    and is the only place that can act on the answer.
//! 2. **The queue** ([`Markers::owe`]) — a write hands its marker over and returns; the handover
//!    cannot block or fail, which is the whole reason the queue is unbounded.
//! 3. **The drain** ([`MarkerActor::run`]) writes clean markers, only for buckets this run accounted
//!    for, and only if nothing was still owed when it sealed.
//!
//! **Ordering**, the part not fully spelled out in §7: hyper's connection drain resolves only once
//! every handler has returned and no new one can start, and every other sender is handler-local —
//! [`Markers::owe`] upgrades the weak handle, sends, and drops it before returning. So after the
//! drain nothing but [`RunSeal`] can enqueue, and the [`MarkerMsg::Seal`] it sends is necessarily
//! behind every marker of the run. The seal is a **message, not the channel closing**: the serving
//! future owns the `Lifecycle` that owns the `RunSeal`, so an aborted or panicking server closes the
//! channel exactly as a drain would, and closure alone must not authorize a clean marker — only a
//! seal does.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use tokio::sync::mpsc;

use hypha_core::meta;

use crate::bucket_ctl::BucketCtl;
use crate::tier::Tiering;

const DRAIN_BATCH: usize = 256;

/// What the queue carries: markers to write, and the one message that says the run is ending
/// *gracefully*. Closure alone cannot say that — the serving future owns the [`RunSeal`], so an
/// aborted task drops it and closes the channel exactly like a drain would. Only an explicit seal
/// distinguishes the two, and FIFO puts it behind every marker enqueued before it.
enum MarkerMsg {
    Owed(OwedMarker),
    Seal,
}

struct OwedMarker {
    bucket: String,
    key: String,
    /// Diagnostic only: the sweep classifies K from the *data* body and CASes on the marker's own
    /// ETag, so any payload is as good as the last (§7).
    body_etag: String,
}

impl OwedMarker {
    fn dedup_key(&self) -> (String, String) {
        (self.bucket.clone(), self.key.clone())
    }
}

struct MarkerQueue {
    tier: Tiering,
    /// Weak on purpose: the service must not hold the channel open, or closing it would prove
    /// nothing. Senders are upgraded per write and dropped before the handler returns, so once the
    /// connections drain the serving loop's [`RunSeal`] is the only one left.
    tx: mpsc::WeakUnboundedSender<MarkerMsg>,
    /// Which buckets this run accounts for is per-bucket state, so it lives on the actor that owns
    /// per-bucket state ([`crate::bucket_ctl::BucketState`]) rather than in a second map here — the
    /// pass that earns the accounting is the actor's, and retiring a deleted bucket clears it there
    /// without this module having to know a bucket went away.
    buckets: BucketCtl,
}

/// The write path's handle: hand over owed markers, and (at startup) establish the per-bucket state
/// the drain later decides on.
#[derive(Clone)]
pub(crate) struct Markers {
    queue: Arc<MarkerQueue>,
}

pub(crate) fn spawn(
    tier: Tiering,
    buckets: BucketCtl,
    retry: Duration,
    concurrency: usize,
) -> (Markers, RunSeal, MarkerActor) {
    let cached = tier.cached;
    // Unbounded because the enqueue sits on the write path *after* the commit: a bounded queue would
    // either block the ack behind the marker or shed it, and shedding needs a side channel to record
    // the loss — state whose only job is to be remembered on a failure path. An enqueue that cannot
    // fail needs none. Depth is an outage symptom rather than a tunable, and `markers_owed` (§10) is
    // where it shows.
    let (tx, rx) = mpsc::unbounded_channel();
    let queue = Arc::new(MarkerQueue {
        tier,
        tx: tx.downgrade(),
        buckets,
    });
    (
        Markers {
            queue: queue.clone(),
        },
        RunSeal(tx),
        MarkerActor {
            queue,
            rx,
            retry: retry.max(Duration::from_millis(1)),
            concurrency: concurrency.max(1),
            cached,
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
pub(crate) struct RunSeal(mpsc::UnboundedSender<MarkerMsg>);

impl RunSeal {
    /// End the run gracefully: [`MarkerActor`] writes clean markers only for a seal it received.
    pub(crate) fn seal(self) {
        let _ = self.0.send(MarkerMsg::Seal);
    }
}

impl Markers {
    /// Hand an acked write's marker to the queue. Called from the write path before it returns, so
    /// it must not block or fail: the body is already committed and the ack cannot wait on — or be
    /// turned into an error by — anything that happens to the marker. That is what the queue being
    /// unbounded buys, and the only reason it is.
    pub(crate) fn owe(&self, bucket: &str, key: &str, body_etag: String) {
        let Some(tx) = self.queue.tx.upgrade() else {
            // The channel closes only after every handler has returned (§7), so a live write cannot
            // reach this — but "cannot" is exactly what a clean marker must not assume. Withdrawing
            // the bucket's evidence is the whole remedy: no evidence, no clean marker.
            tracing::error!(bucket, key, "marker queue closed under a live write");
            self.queue.buckets.unaccount(bucket);
            return;
        };
        let _ = tx.send(MarkerMsg::Owed(OwedMarker {
            bucket: bucket.to_string(),
            key: key.to_string(),
            body_etag,
        }));
    }
}

/// Owns the receiving end for the life of the process: writes markers as they arrive and, once a
/// [`RunSeal`] reaches it, the clean markers.
pub(crate) struct MarkerActor {
    queue: Arc<MarkerQueue>,
    rx: mpsc::UnboundedReceiver<MarkerMsg>,
    retry: Duration,
    concurrency: usize,
    /// Durable mode has no pending set, so it has nothing for a clean marker to vouch for and
    /// nothing that reads one back (`crate::recovery`) — writing them there would only leave
    /// per-bucket objects no code path ever consults.
    cached: bool,
}

impl MarkerActor {
    /// Write owed markers as they arrive, retrying failures on `retry`, until the channel closes.
    /// Then one final attempt — never a retry loop, since the drain does not wait out a backoff —
    /// and the clean markers.
    ///
    /// A marker still owed after that final attempt means the run did not end gracefully, so it
    /// vouches for *nothing*: the next run rescans every bucket rather than this one guessing which
    /// buckets the loss touched.
    pub(crate) async fn run(mut self) {
        let mut owed: HashMap<(String, String), OwedMarker> = HashMap::new();
        let mut batch = Vec::with_capacity(DRAIN_BATCH);
        let mut sealed = false;
        'outer: loop {
            tokio::select! {
                // Batched because the queue is every write's path to its marker: one wake-up takes
                // whatever a burst deposited.
                n = self.rx.recv_many(&mut batch, DRAIN_BATCH) => {
                    if n == 0 {
                        break; // dropped rather than sealed — the run did not end gracefully
                    }
                    for msg in batch.drain(..) {
                        match msg {
                            MarkerMsg::Owed(r) => { owed.insert(r.dedup_key(), r); }
                            MarkerMsg::Seal => { sealed = true; }
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
    async fn write_all(&self, owed: &mut HashMap<(String, String), OwedMarker>) {
        let failed: Vec<OwedMarker> = futures::stream::iter(owed.drain().map(|(_, r)| r))
            .map(|r| async move {
                match self
                    .queue
                    .tier
                    .raise_marker(&r.bucket, &r.key, &r.body_etag)
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
    /// on a graceful drain with nothing owed, so the accounting is the whole condition — a bucket
    /// left dirty by an earlier crash and untouched by this run is simply not accounted, and its
    /// orphans stay findable instead of buried.
    async fn mark_clean(&self) {
        if !self.cached {
            return;
        }
        let clean = meta::clean_marker_key();
        for bucket in self.queue.buckets.accounted() {
            let bucket = bucket.as_str();
            if let Err(e) = self
                .queue
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
