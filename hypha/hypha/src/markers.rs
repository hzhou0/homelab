//! Pending-marker delivery and clean-shutdown evidence.
//!
//! Marker failure cannot fail an already committed client write. The queue is therefore unbounded
//! and handler-local senders are weak. Graceful shutdown sends an explicit FIFO seal after request
//! drain; channel closure alone never authorizes clean markers because crashes close it too.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use tokio::sync::mpsc;

use hypha_core::error::Error;
use hypha_core::meta;

use crate::bucket::{BucketCtl, BucketStatus};
use crate::halt::{Invariant, Violation};
use crate::sealq::{self, Dedup};
use crate::tier::Tiering;

pub(crate) struct OwedMarker {
    bucket: String,
    key: String,
    /// PUT body ETag or the reserved DELETE token; the resulting marker ETag identifies the branch
    /// and is also its CAS handle.
    marker_body: String,
}

impl Dedup for OwedMarker {
    fn dedup_key(&self) -> (String, String) {
        (self.bucket.clone(), self.key.clone())
    }
}

struct MarkerQueue {
    tier: Tiering,
    /// Weak on purpose: the service must not hold the channel open, or closing it would prove
    /// nothing. Senders are upgraded per write and dropped before the handler returns, so once the
    /// connections drain the serving loop's [`RunSeal`] is the only one left.
    tx: mpsc::WeakUnboundedSender<sealq::Msg<OwedMarker>>,
    /// Which buckets this run accounts for is per-bucket state, so it lives on the actor that owns
    /// per-bucket state ([`crate::bucket`]) rather than in a second map here — the pass that earns
    /// the accounting is the actor's, and retiring a deleted bucket clears it there without this
    /// module having to know a bucket went away.
    buckets: BucketCtl,
    concurrency: usize,
    /// Durable mode has no pending set, so it has nothing for a clean marker to vouch for and
    /// nothing that reads one back (`crate::bucket`) — writing them there would only leave
    /// per-bucket objects no code path ever consults.
    cached: bool,
}

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
    // Unbounded because the enqueue sits on the write path *after* the commit: a bounded queue would
    // either block the ack behind the marker or shed it, and shedding needs a side channel to record
    // the loss — state whose only job is to be remembered on a failure path. An enqueue that cannot
    // fail needs none. Depth is an outage symptom rather than a tunable, and `markers_owed`  is
    // where it shows.
    let (tx, rx) = mpsc::unbounded_channel();
    let queue = Arc::new(MarkerQueue {
        cached: tier.cached,
        tier,
        tx: tx.downgrade(),
        buckets,
        concurrency: concurrency.max(1),
    });
    (
        Markers {
            queue: queue.clone(),
        },
        RunSeal::new(tx),
        MarkerActor {
            queue,
            rx,
            retry: retry.max(Duration::from_millis(1)),
        },
    )
}

/// See [`sealq::Seal`] for the mechanism and why dropping it must not seal.
pub(crate) type RunSeal = sealq::Seal<OwedMarker>;

impl Markers {
    /// Hand an acked write's marker to the queue. Called from the write path before it returns, so
    /// it must not block or fail: the body is already committed and the ack cannot wait on — or be
    /// turned into an error by — anything that happens to the marker. That is what the queue being
    /// unbounded buys, and the only reason it is.
    pub(crate) fn owe(&self, bucket: &str, key: &str, marker_body: String) {
        let Some(tx) = self.queue.tx.upgrade() else {
            // The channel closes only after every handler has returned , so a live write cannot
            // reach this — but "cannot" is exactly what a clean marker must not assume. Withdrawing
            // the bucket's evidence is the whole remedy: no evidence, no clean marker.
            tracing::error!(bucket, key, "marker queue closed under a live write");
            self.queue.buckets.unaccount(bucket);
            return;
        };
        let _ = tx.send(sealq::Msg::Owed(OwedMarker {
            bucket: bucket.to_string(),
            key: key.to_string(),
            marker_body,
        }));
    }
}

/// Owns the receiving end for the life of the process: writes markers as they arrive and, once a
/// [`RunSeal`] reaches it, the clean markers.
pub(crate) struct MarkerActor {
    queue: Arc<MarkerQueue>,
    rx: mpsc::UnboundedReceiver<sealq::Msg<OwedMarker>>,
    retry: Duration,
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
        let (sealed, left) =
            sealq::drain(&mut self.rx, self.retry, &mut owed, self.queue.as_ref()).await;
        // Both are flat zero in health : an owed marker at drain is the cache refusing small
        // writes, and every bucket left dirty is a rebuild the next run pays for before it serves.
        crate::metrics::buckets_dirty_at_drain(self.queue.dirty_at_drain(sealed && left == 0));
        match (sealed, left == 0) {
            (true, true) => self.queue.mark_clean().await,
            (true, false) => tracing::warn!(
                owed = left,
                "markers still owed at drain; no clean markers written"
            ),
            (false, _) => tracing::warn!("marker queue closed without a drain; no clean markers"),
        }
    }
}

impl sealq::Drain for MarkerQueue {
    type Payload = OwedMarker;

    async fn process(&self, owed: &mut HashMap<(String, String), OwedMarker>) {
        self.write_all(owed).await;
    }
}

impl MarkerQueue {
    /// Buckets this run will not vouch for: all of them unless the drain earned the right to write
    /// clean markers, and in that case the ones it never accounted for.
    fn dirty_at_drain(&self, clean: bool) -> usize {
        let live = self.buckets.ready().len();
        match clean {
            true => live.saturating_sub(self.buckets.accounted().len()),
            false => live,
        }
    }

    /// Write every owed marker, dropping the ones that land. Concurrent because a marker is on each
    /// acked write's durability path: serializing them would make the queue the write path's
    /// throughput ceiling.
    ///
    /// A marker whose bucket is **gone** is dropped rather than retried. It can never land — its
    /// `<meta>` projection was drained by the DeleteBucket  — and there is nothing left for it to
    /// index. Retrying it forever would be worse than useless: one permanently owed marker withholds
    /// the clean marker of *every* bucket at drain, so a single deleted bucket would send the next
    /// run into a full rebuild of buckets it had no reason to doubt.
    ///
    /// "Gone" is read from the **state map**, before the write rather than out of its error:
    /// `DeleteBucket` retires the bucket there before draining its projections , so the map has
    /// already caught up by the time a marker for it could be written — and a backend that re-creates
    /// the bucket a PUT addresses (SeaweedFS) would otherwise have this path resurrect a `<meta>`
    /// projection the delete had just drained, and never report a thing.
    ///
    /// The backend's own `NoSuchBucket` therefore means something narrower: the map still calls the
    /// bucket live, so its `<meta>` projection vanished underneath a running process — the cache
    /// volume loss of invariant **I6**. (The map is re-read there because a delete may have retired
    /// the bucket while the write was in flight.)
    async fn write_all(&self, owed: &mut HashMap<(String, String), OwedMarker>) {
        let failed: Vec<OwedMarker> = futures::stream::iter(owed.drain().map(|(_, r)| r))
            .map(|r| async move {
                if self.buckets.status(&r.bucket) == BucketStatus::Absent {
                    tracing::info!(
                        bucket = r.bucket,
                        key = r.key,
                        "marker dropped; its bucket was deleted"
                    );
                    return None;
                }
                match self
                    .tier
                    .raise_marker(&r.bucket, &r.key, &r.marker_body)
                    .await
                {
                    Ok(()) => None,
                    Err(Error::NoSuchBucket) => {
                        if self.buckets.status(&r.bucket) != BucketStatus::Absent {
                            self.tier
                                .halt
                                .raise(Violation {
                                    invariant: Invariant::CacheVolumeLost,
                                    bucket: r.bucket.clone(),
                                    key: Some(r.key.clone()),
                                    detail: "an owed marker's <meta> projection is gone while the \
                                             bucket is still live: the cache volume was lost under \
                                             a running process, so an acked write's marker can \
                                             never land and the pending set is short an entry the \
                                             run still vouches for"
                                        .to_string(),
                                })
                                .await
                        }
                        tracing::info!(
                            bucket = r.bucket,
                            key = r.key,
                            "marker dropped; its bucket was deleted"
                        );
                        None
                    }
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
        crate::metrics::markers_owed(owed.len());
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
        for bucket in self.buckets.accounted() {
            let bucket = bucket.as_str();
            if let Err(e) = self
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
