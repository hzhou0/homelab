//! The cache-volume watchdog: the one failure a *running* process has to keep checking for.
//!
//! Startup resolves every bucket from its sync marker and is done with the question
//! ([`crate::bucket::resolve_all`]). That answer stays true for the rest of the run under one
//! assumption the run cannot simply hold — that the cache volume does not vanish underneath a live
//! process. A `Ready` bucket whose cache is gone answers **404 for objects that exist**, silently:
//! the phase's central claim ([`crate::bucket`]) turned into a lie. Every other divergence is either
//! impossible while hypha owns both backends or is caught by the pass that would act on it.
//!
//! Halting on the marker's disappearance is the correct outcome rather than a harsh one — the
//! process cannot re-derive what the volume held, and the restart it forces resolves the bucket as
//! `Restoring` and rebuilds it from the remote.
//!
//! **`Ready` is the whole set, and that is not a gap.** It is the only phase that claims anything
//! falsifiable about the cache. A `Restoring` bucket serves from the remote, so losing its volume
//! costs the restore its progress and nothing else — nothing acked lives only in the cache during
//! that window, and the pass is additive and idempotent. There is no assertion to invalidate, so
//! there is nothing to poll for.
//!
//! Deliberately *not* a repair. Flipping the bucket back to `Restoring` in place would be serving
//! through a volume loss the run has already answered 404s from, with no way to know which answers
//! were wrong or who saw them.

use std::time::Duration;

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::bucket::{BucketCtl, Readiness};
use crate::halt::{Invariant, Violation};
use crate::tier::Tiering;

pub(crate) struct VolumeWatch {
    tier: Tiering,
    buckets: BucketCtl,
    interval: Duration,
}

impl VolumeWatch {
    pub(crate) fn new(tier: Tiering, buckets: BucketCtl, interval: Duration) -> Self {
        VolumeWatch {
            tier,
            buckets,
            interval: interval.max(Duration::from_millis(1)),
        }
    }

    /// Polls until `liveness` drops, i.e. until the service does — the same weak-handle shutdown
    /// the reconcile sweep uses, so there is nothing to wire.
    pub(crate) async fn run(self, liveness: std::sync::Weak<()>) {
        loop {
            tokio::time::sleep(self.interval).await;
            if liveness.upgrade().is_none() {
                return;
            }
            for bucket in self.buckets.ready() {
                if let Err(e) = self.check(&bucket).await {
                    // A backend that cannot answer is not a volume loss. Retrying next tick is the
                    // whole handling: the check is a poll, so a lost tick costs one interval.
                    tracing::warn!(bucket, error = %e, "sync-marker check failed; retrying");
                }
            }
        }
    }

    async fn check(&self, bucket: &str) -> Result<()> {
        match self.tier.meta.head(bucket, &meta::sync_marker_key()).await {
            Ok(_) => return Ok(()),
            Err(Error::NotFound) | Err(Error::NoSuchBucket) => {}
            Err(e) => return Err(e),
        }
        // A `DeleteBucket` drops the bucket from the map *before* draining its cache projections, so
        // the one benign way to see this is to have raced one. Re-reading the map after the failed
        // HEAD is what tells the two apart — and it is enough, since nothing else removes the marker
        // and nothing rewrites it while a bucket is `Ready`.
        if self.buckets.readiness(bucket) != Readiness::Ready {
            return Ok(());
        }
        self.tier
            .halt
            .raise(Violation {
                invariant: Invariant::CacheVolumeLost,
                bucket: bucket.to_string(),
                key: None,
                detail: "the sync marker of a ready bucket is gone: its cache volume was lost \
                         under a running process, so an absent key can no longer be trusted as \
                         the object's absence"
                    .to_string(),
            })
            .await
    }
}
