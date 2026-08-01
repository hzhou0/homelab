//! Detects live loss of an authoritative cache namespace.
//!
//! A ready bucket may already have served false 404s before loss is detected, so the watcher halts
//! instead of attempting in-place repair. Restart reclassifies the bucket and restores remotely.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::bucket::{BucketCtl, BucketStatus};
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

    /// Polls until the drain signals. Interruptible mid-wait, since a watchdog asleep between polls is
    /// pure delay to a drain that is trying to bound itself — and the round it is *in* is a handful of
    /// HEADs, so finishing it costs nothing.
    pub(crate) async fn run(self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(self.interval) => {}
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
        if self.buckets.status(bucket) != BucketStatus::Ready {
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
