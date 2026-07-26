//! The cached-mode reconcile sweep (§7) — the upload path for acked cache writes, a continual duty
//! of the active replica. A cached PUT acks on the cache body write and owes a bare-`K` pending
//! marker ([`crate::markers`]); this task trails behind, enumerating those markers and pushing each
//! key's current cache state to the remote:
//!
//! - a **live body** → encrypt + PUT to the remote, then clear the marker (CAS on its ETag);
//! - a **delete-tombstone** → remote `DeleteObject`, clear the tombstone and the marker.
//!
//! The markers are the range-C tail of `<meta><b>` (bare `K`, above the `0x01` block), so **one flat
//! LIST** past [`meta::marker_scan_start_after`] enumerates the pending set — `O(pending)`, never
//! `O(evicted)`. Each key is handled under its **upload** lock (§4), distinct from the write lock a
//! client PUT takes, so a replication upload never queues a conditional write behind a multi-second
//! transfer. The marker clear is conditional on the ETag the LIST returned, so a PUT that landed a
//! newer body mid-pass simply defers that key to the next pass rather than being lost.
//!
//! Lifecycle is implicit: the task holds a `Weak` to the service's liveness sentinel (§3, `Hypha`)
//! and exits once the service drops, so no explicit shutdown channel is needed. A process crash
//! loses nothing — acked bodies are on the cache, a marker that had not yet landed is rebuilt by the
//! recovery scan ([`crate::markers`]), and the next active resumes from the same LIST. Only losing
//! the cache *volume* with markers outstanding loses data (the bounded window, §7).

use std::sync::Weak;
use std::time::Duration;

use futures::StreamExt as _;

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::tier::{Reconciler, UploadOutcome};

/// One LIST page of pending markers.
const MARKER_PAGE: i32 = 1000;

pub struct Reconcile {
    tier: Reconciler,
    interval: Duration,
    concurrency: usize,
}

impl Reconcile {
    pub fn new(tier: Reconciler, interval: Duration, concurrency: usize) -> Self {
        Self {
            tier,
            interval,
            concurrency: concurrency.max(1),
        }
    }

    /// Run passes on `interval` until the service drops (`liveness` no longer upgrades). Each pass is
    /// best-effort: a bucket or key that errors is logged and the sweep moves on, since every
    /// transition is idempotent and re-attempted next pass.
    pub async fn run(self, liveness: Weak<()>) {
        loop {
            tokio::time::sleep(self.interval).await;
            if liveness.upgrade().is_none() {
                break;
            }
            if let Err(e) = self.pass().await {
                tracing::warn!(error = %e, "reconcile pass could not enumerate buckets; retrying");
            }
        }
    }

    /// One sweep across every cache-backed bucket. The `<meta>` bucket exists if hypha provisioned
    /// the bucket, so listing those names is the bucket set to reconcile.
    async fn pass(&self) -> Result<()> {
        for (bucket, _) in self.tier.meta.list_buckets().await? {
            if let Err(e) = self.reconcile_bucket(&bucket).await {
                tracing::warn!(bucket, error = %e, "reconcile pass for bucket failed; retrying next pass");
            }
        }
        Ok(())
    }

    /// Drain one bucket's pending markers. The flat LIST past the `0x01` block yields only range-C
    /// bare markers; a residual `0x01`-lead key (a boundary miscompare) is filtered defensively so a
    /// twin can never be mistaken for a marker.
    async fn reconcile_bucket(&self, bucket: &str) -> Result<()> {
        let mut token: Option<String> = None;
        let mut first = true;
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
        Ok(())
    }

    /// Reconcile one pending key, under its upload lock (§7). Classify K's cache body once to pick the
    /// branch; the branch primitives re-read K under the lock and CAS every mutation, so any write
    /// that raced in between is either uploaded correctly or deferred, never lost.
    async fn reconcile_key(&self, bucket: &str, key: &str, m_etag: &str) -> Result<()> {
        let _up = self.tier.upload_locks.lock(key).await;

        let head = match self.tier.data.head(bucket, key).await {
            Ok(h) => h,
            // The body is gone (a delete already cleared it, or the volume was reset): the marker is
            // an orphan — clear it (CAS-guarded, so a concurrent rewrite is left alone).
            Err(Error::NotFound) => return self.tier.clear_marker_cas(bucket, key, m_etag).await,
            Err(e) => return Err(e),
        };
        let size = head.content_length().unwrap_or(0);
        let etag = head.e_tag().unwrap_or_default().trim_matches('"');

        // A client body can't masquerade as a sentinel here: cached PUT rejects any body equal to a
        // reserved sentinel at write time (`meta::is_reserved_sentinel`), so a sentinel classification
        // is always hypha's own tombstone (§6).
        match meta::classify_entry(size, etag) {
            Some(meta::TombKind::Delete) => {
                self.tier.propagate_delete_locked(bucket, key, m_etag).await
            }
            // Evict/transit with a marker shouldn't occur in cached steady state (GC is Phase 5, and
            // durability gates it): leave the marker for whatever transition owns it.
            Some(_) => Ok(()),
            None => match self.tier.upload_locked(bucket, key).await? {
                UploadOutcome::Uploaded | UploadOutcome::Vanished => {
                    self.tier.clear_marker_cas(bucket, key, m_etag).await
                }
                // A cached delete raced K to a tombstone between our classify and the upload's GET;
                // it rewrote the marker, so the delete branch handles it on a later pass.
                UploadOutcome::SkippedTombstone => Ok(()),
            },
        }
    }
}
