//! Propagates cached PUT and DELETE markers to the remote.
//!
//! Reconcile uses a separate upload lock so transfers do not block client writes. Marker deletion
//! is conditional on the generation listed, making concurrent overwrites defer safely to a later
//! pass.

use std::time::Duration;

use futures::StreamExt as _;

use tokio_util::sync::CancellationToken;

use hypha_core::error::Result;
use hypha_core::meta;

use crate::bucket::BucketCtl;
use crate::tier::{Tiering, UploadOutcome};

const MARKER_PAGE: i32 = 1000;

#[derive(Clone)]
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

    /// Shutdown interrupts only the wait between idempotent passes; an active pass finishes.
    pub async fn run(self, shutdown: CancellationToken) {
        // The backpressure counter was seeded once by `Lifecycle::startup`, ahead of the listener;
        // from here every raise/clear/drain keeps it exact (§7). Each pass only re-publishes the
        // oldest marker age — it has no atomic source, so it is sampled where the sweep already
        // enumerates the whole set.
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(self.interval) => {}
            }
            let started = std::time::Instant::now();
            let (pending, oldest_age_ms) = self.pass().await;
            crate::metrics::reconcile_pass(pending, started.elapsed());
            self.tier.pressure.publish_age(oldest_age_ms);
        }
    }

    /// One sweep across every bucket this process serves from its cache. The set comes from the
    /// state map, not from listing `<meta>` buckets: a projection can outlive the bucket it belonged
    /// to — a marker write already in flight when a `DeleteBucket` drains it re-creates it on a
    /// backend that creates the bucket a PUT addresses — and reconciling that debris would push its
    /// markers at a remote bucket that no longer exists. A bucket the map does not call `Ready` is
    /// therefore not drained: its markers are either that debris or leftovers a volume-loss restore
    /// has not yet repriced, and the census counts them only so the counter starts from the truth —
    /// they are removed by `drained` or by the sweep once the bucket is `Ready` again (§7).
    async fn pass(&self) -> (usize, u64) {
        let now = crate::tier::now_ms();
        let mut pending = 0;
        let mut oldest_age_ms = 0;
        for bucket in self.buckets.ready() {
            match self.reconcile_bucket(&bucket, now).await {
                Ok((seen, oldest)) => {
                    pending += seen;
                    oldest_age_ms = oldest_age_ms.max(oldest);
                }
                Err(e) => {
                    tracing::warn!(bucket, error = %e, "reconcile pass for bucket failed; retrying next pass")
                }
            }
        }
        (pending, oldest_age_ms)
    }

    /// Drain one bucket's pending markers. The flat LIST past the `0x01` block yields only range-C
    /// bare markers; a residual `0x01`-lead key (a boundary miscompare) is filtered defensively so a
    /// twin can never be mistaken for a marker. Also reports the oldest marker's age, sampled at
    /// enumeration — the pre-drain set, which is the conservative reading for the age gate (§7).
    async fn reconcile_bucket(&self, bucket: &str, now: i64) -> Result<(usize, u64)> {
        let mut token: Option<String> = None;
        let mut first = true;
        let mut seen = 0;
        let mut oldest = 0u64;
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
                    if let Some(lm) = o.last_modified.and_then(|t| t.to_millis().ok()) {
                        oldest = oldest.max((now - lm).max(0) as u64);
                    }
                    let m_etag = o.e_tag.unwrap_or_default().trim_matches('"').to_string();
                    Some((key, m_etag))
                })
                .collect();

            seen += markers.len();
            // A *task* per upload, not merely a future: the codecs encrypt on whichever task drives
            // them (§6), so uploads multiplexed onto this one would run the whole pass's crypto on
            // a single core no matter how wide `concurrency` was set. `buffer_unordered` still
            // bounds how many are in flight, since the spawn happens as the stream is polled.
            futures::stream::iter(markers)
                .map(|(key, m_etag)| {
                    let task = self.clone();
                    let bucket = bucket.to_string();
                    tokio::spawn(async move {
                        if let Err(e) = task.reconcile_key(&bucket, &key, &m_etag).await {
                            tracing::warn!(bucket, key = %key, error = %e, "reconcile of key failed; retrying next pass");
                        }
                    })
                })
                .buffer_unordered(self.concurrency)
                .for_each(|joined| async {
                    if let Err(e) = joined {
                        tracing::error!(bucket, error = %e, "reconcile of key did not finish");
                    }
                })
                .await;

            match page.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        Ok((seen, oldest))
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
        let Ok((_gate, _)) = self.buckets.enter_write(bucket) else {
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

/// Count the pending set and its oldest marker across every client bucket, without draining. The
/// count seeds the backpressure counter once, at startup, before the listener opens; it is exact
/// thereafter by raise/clear accounting, so the sweep never re-seeds it (§7).
///
/// Buckets come from the remote list — the same authority `resolve_all` classifies from — not from
/// the state map's ready set, because "markers live only in ready buckets" is not an invariant the
/// sweep can rely on: the marker actor raises into any non-Absent bucket, so a write admitted before
/// a volume-loss restore can leave its marker in a bucket the map no longer calls `Ready`.
pub(crate) async fn census(tier: &Tiering) -> (usize, u64) {
    let now = crate::tier::now_ms();
    let mut pending = 0;
    let mut oldest_age_ms = 0;
    let buckets = match tier.remote.list_buckets().await {
        Ok(buckets) => buckets,
        Err(e) => {
            tracing::warn!(error = %e, "backpressure census could not list buckets; seeding zero");
            return (0, 0);
        }
    };
    for (bucket, _) in buckets {
        match census_bucket(tier, &bucket, now).await {
            Ok((count, oldest)) => {
                pending += count;
                oldest_age_ms = oldest_age_ms.max(oldest);
            }
            Err(e) => {
                tracing::warn!(bucket, error = %e, "backpressure census for bucket failed")
            }
        }
    }
    (pending, oldest_age_ms)
}

async fn census_bucket(tier: &Tiering, bucket: &str, now: i64) -> Result<(usize, u64)> {
    let mut token: Option<String> = None;
    let mut first = true;
    let mut seen = 0usize;
    let mut oldest = 0u64;
    loop {
        let page = tier
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
        for o in page.contents.unwrap_or_default() {
            let Some(key) = o.key else {
                continue;
            };
            if key.starts_with(meta::CTRL as char) {
                continue; // range A/B leaked past the boundary — not a marker
            }
            seen += 1;
            if let Some(lm) = o.last_modified.and_then(|t| t.to_millis().ok()) {
                oldest = oldest.max((now - lm).max(0) as u64);
            }
        }
        match page.next_continuation_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    Ok((seen, oldest))
}
