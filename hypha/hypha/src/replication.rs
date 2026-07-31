//! The cached-mode reconcile sweep (§7) — the remote propagation path for acked cache writes, a
//! continual duty of the active replica. A cached PUT or DELETE commits in the cache and owes a
//! bare-`K` pending marker ([`crate::markers`]); this task trails behind, executing the operation
//! encoded by each marker:
//!
//! - **PUT** → encrypt the current cache body and PUT it to the remote;
//! - **DELETE** → HEAD the remote generation and delete it with `If-Match`.
//!
//! The markers are the range-C tail of `<meta><b>` (bare `K`, above the `0x01` block), so **one flat
//! LIST** past [`meta::marker_scan_start_after`] enumerates the pending set — `O(pending)`, never
//! `O(evicted)`. Each key is handled under its **upload** lock (§4), distinct from the write lock a
//! client PUT takes, so a replication upload never queues a conditional write behind a multi-second
//! transfer; a key already uploading is skipped, not queued
//! ([`ReplicationTask::reconcile_key`]). The marker clear is conditional on the ETag the LIST
//! returned, so a PUT that landed a newer body mid-pass simply defers that key to the next pass
//! rather than being lost.
//!
//! The drain's shutdown token ends the loop between passes and lets the pass in flight finish (§9). A
//! process crash
//! loses nothing — acked bodies are on the cache, a marker that had not yet landed is rebuilt by the
//! recovery scan ([`crate::markers`]), and the next active resumes from the same LIST. Only losing
//! the cache *volume* with markers outstanding loses data (the bounded window, §7).

use std::time::Duration;

use futures::StreamExt as _;

use tokio_util::sync::CancellationToken;

use hypha_core::error::Result;
use hypha_core::meta;

use crate::bucket::BucketCtl;
use crate::tier::{Tiering, UploadOutcome};

const MARKER_PAGE: i32 = 1000;

pub struct ReplicationTask {
    tier: Tiering,
    buckets: BucketCtl,
    interval: Duration,
    concurrency: usize,
}

impl ReplicationTask {
    pub fn new(tier: Tiering, buckets: BucketCtl, interval: Duration, concurrency: usize) -> Self {
        Self {
            tier,
            buckets,
            interval,
            concurrency: concurrency.max(1),
        }
    }

    /// Run passes on `interval` until the drain signals. The wait is interruptible so the drain does
    /// not spend its budget on a sleeping task, while a pass already under way is left to finish: it
    /// writes bodies to the remote and clears their markers, and stopping between the two would leave
    /// the marker for the next run to redo.
    ///
    /// Each pass is best-effort: a bucket or key that errors is logged and the sweep moves on, since
    /// every transition is idempotent and re-attempted next pass.
    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(self.interval) => {}
            }
            let started = std::time::Instant::now();
            // The pending count is the pass's own census — the markers it enumerated — so it is the
            // size of the set *before* this pass drained it (§10).
            crate::metrics::reconcile_pass(self.pass().await, started.elapsed());
        }
    }

    /// One sweep across every bucket this process serves from its cache. The set comes from the
    /// state map, not from listing `<meta>` buckets: a projection can outlive the bucket it belonged
    /// to — a marker write already in flight when a `DeleteBucket` drains it re-creates it on a
    /// backend that creates the bucket a PUT addresses — and reconciling that debris would push its
    /// markers at a remote bucket that no longer exists. A bucket the map does not call `Ready` has
    /// no pending set to drain: markers are raised only by cached writes into a ready bucket, and a
    /// restore rebuilds its namespace with the set empty (§7).
    async fn pass(&self) -> usize {
        let mut pending = 0;
        for bucket in self.buckets.ready() {
            match self.reconcile_bucket(&bucket).await {
                Ok(seen) => pending += seen,
                Err(e) => {
                    tracing::warn!(bucket, error = %e, "reconcile pass for bucket failed; retrying next pass")
                }
            }
        }
        pending
    }

    /// Drain one bucket's pending markers. The flat LIST past the `0x01` block yields only range-C
    /// bare markers; a residual `0x01`-lead key (a boundary miscompare) is filtered defensively so a
    /// twin can never be mistaken for a marker.
    async fn reconcile_bucket(&self, bucket: &str) -> Result<usize> {
        let mut token: Option<String> = None;
        let mut first = true;
        let mut seen = 0;
        loop {
            let page = self
                .tier
                .meta
                .list(
                    bucket,
                    None,
                    None,
                    token.take(),
                    first.then(meta::marker_scan_start_after),
                    Some(MARKER_PAGE),
                )
                .await?;
            first = false;
            let markers: Vec<(String, String)> = page
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|o| {
                    let key = o.key?;
                    if key.starts_with(meta::CTRL as char) {
                        return None; // range A/B leaked past the boundary — not a marker
                    }
                    let m_etag = o.e_tag.unwrap_or_default().trim_matches('"').to_string();
                    Some((key, m_etag))
                })
                .collect();

            seen += markers.len();
            futures::stream::iter(markers)
                .for_each_concurrent(self.concurrency, |(key, m_etag)| async move {
                    if let Err(e) = self.reconcile_key(bucket, &key, &m_etag).await {
                        tracing::warn!(bucket, key = %key, error = %e, "reconcile of key failed; retrying next pass");
                    }
                })
                .await;

            match page.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        Ok(seen)
    }

    /// Reconcile one pending key under its upload lock (§7). The marker ETag selects the operation
    /// and remains its completion CAS, so a marker overwritten mid-pass is never cleared.
    ///
    /// `try_lock`, so pending passes for K **coalesce onto the in-flight one** instead of queuing
    /// behind it. Waiters would be redundant — the holder re-reads K's body under the lock, so it
    /// carries whatever landed while it ran, and anything it can't account for leaves the marker
    /// standing for the next pass. They would also be harmful: on a hot key each queued waiter
    /// re-uploads in turn, so the newest generation's upload starts only once the whole redundant
    /// queue drains, and a key written faster than it uploads never converges — an unbounded loss
    /// window. Unlike a rehydrate, an upload holds no write lock, so a client write can't cancel it.
    ///
    /// The bucket's write gate is held for the key, not for the pass: the sweep is a producer into
    /// the bucket's namespaces exactly as a client PUT is, so a `DeleteBucket` must be able to wait
    /// it out — but holding the gate across a whole bucket's markers would stall the delete behind
    /// an arbitrarily long drain. A key refused here belongs to a bucket already going away, whose
    /// `<meta>` (and this marker with it) the delete is about to drain.
    async fn reconcile_key(&self, bucket: &str, key: &str, m_etag: &str) -> Result<()> {
        let Some(_gate) = self.buckets.enter_write(bucket) else {
            return Ok(());
        };
        let Some(_up) = self.tier.upload_locks.try_lock(key) else {
            return Ok(());
        };
        let started = std::time::Instant::now();
        let outcome = self.transition_key(bucket, key, m_etag).await;
        crate::metrics::remote_upload(outcome.is_err(), started.elapsed());
        outcome
    }

    async fn transition_key(&self, bucket: &str, key: &str, m_etag: &str) -> Result<()> {
        if m_etag == meta::delete_marker_etag() {
            return self.tier.propagate_delete_locked(bucket, key, m_etag).await;
        }

        match self.tier.upload_locked(bucket, key).await? {
            UploadOutcome::Uploaded => self.tier.clear_marker_cas(bucket, key, m_etag).await,
            // An eviction tombstone is only ever written after its gates confirmed the remote holds
            // that generation (§8), and it replaced every generation before it — so whichever this
            // marker names, the obligation is discharged and nothing will ever discharge it again.
            // The CAS is what makes that safe to act on: a marker raised *since* the listing is a
            // different ETag, so it survives and this pass takes no view of it.
            UploadOutcome::SkippedTombstone(meta::TombKind::Evict) => {
                self.tier.clear_marker_cas(bucket, key, m_etag).await
            }
            // The bracket owns K and will raise its own marker on commit, so this one is superseded
            // rather than stranded — the one arm here that is routine.
            UploadOutcome::SkippedTombstone(meta::TombKind::Transit) => {
                tracing::debug!(bucket, key, "key is mid-bracket; marker left to its commit");
                Ok(())
            }
            // The generation this marker names no longer exists to upload, so its obligation is
            // undischargeable rather than pending, and nothing reads a PUT marker at an absent K.
            // Clearing it is what the CAS makes safe: the delete that removed K owes the marker that
            // supersedes this one, and if that marker has already landed the ETag has moved and the
            // CAS declines. A delete interrupted before its marker is recovered by R2 from the
            // remote-only sighting, which never consulted this marker either.
            UploadOutcome::Vanished => {
                tracing::debug!(bucket, key, "pending key vanished before its upload");
                self.tier.clear_marker_cas(bucket, key, m_etag).await
            }
        }
    }
}
