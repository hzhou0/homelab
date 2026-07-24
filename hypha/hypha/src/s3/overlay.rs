//! The restore overlay: one interface every data op routes through so a bucket mid-restore is served
//! from the remote instead of its half-rebuilt cache (§7 *Buckets*).
//!
//! A bucket's per-bucket sync marker (`meta::sync_marker_key`) records namespace trust. This module
//! turns that into a [`Readiness`] verdict and, from it, the source every op resolves against:
//!
//! - **Reads** ([`Hypha::resolve_key`], [`Hypha::project_remote_page`]) resolve a key's facts — and a
//!   LIST page's entries — from the cache tombstone namespace once `Ready`, or straight from the
//!   remote (facts off each object's authenticated tail trailer, §6) while `Restoring`.
//! - **Writes** ([`Hypha::prepare_write`]) are never gated: a write to a `Restoring` bucket first
//!   materializes its key from the remote into the cache, so the normal §4 bracket then runs against
//!   a correct tombstone. The background restore fills the rest and writes the marker.
//!
//! Keeping the cache-vs-remote fork behind these three entry points means the op handlers carry one
//! `match` each, and the whole overlay is one place to revisit when cached-mode (Phase 4) adds its
//! pending overlay to the `Restoring` arms.

use std::collections::HashMap;

use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use s3s::dto::*;
use s3s::S3Result;

use hypha_core::error::Error;
use hypha_core::meta;

use super::get::facts_from_tombstone;
use super::{ts_ms, Hypha};
use crate::tier::RemoteFacts;

/// Bounded fan-out for the per-key trailer reads a remote-served LIST page needs (§7).
const REMOTE_LIST_FANOUT: usize = 16;

/// Whether a bucket's cache namespace can be trusted right now.
pub(super) enum Readiness {
    /// Sync marker present: the cache is authoritative, an absent key is a definitive 404.
    Ready,
    /// Marker absent but the remote bucket exists: a restore has been kicked and the remote is the
    /// read source of truth meanwhile.
    Restoring,
    /// No remote bucket — the bucket does not exist.
    Absent,
}

/// The resolved state of a key, source-agnostic (§7). `Remote` and `CacheBody` both carry the
/// cache-side metadata (`md`) the facts share — empty when resolved from the remote mid-restore,
/// since the trailer carries facts and nothing else.
pub(super) enum KeyState {
    /// Client-visibly absent (deleted, or never existed).
    Absent,
    /// A remote-resident object (tombstoned, mid-bracket, or restore-time): serve/HEAD from remote.
    Remote {
        facts: RemoteFacts,
        md: HashMap<String, String>,
    },
    /// A live plaintext body in the cache (cached mode). Carries the HEAD it was resolved from so
    /// callers reuse its native size/ETag/mtime (boxed — it dwarfs the other variants).
    CacheBody {
        head: Box<HeadObjectOutput>,
        md: HashMap<String, String>,
    },
}

impl Hypha {
    /// Classify a bucket, kicking a background restore the first time an unreconciled-but-live bucket
    /// is seen. A persisted marker this process hadn't observed is adopted into the ready set, so the
    /// probe is paid once per bucket per lifetime.
    pub(super) async fn readiness(&self, bucket: &str) -> S3Result<Readiness> {
        if self.buckets.is_ready(bucket) {
            return Ok(Readiness::Ready);
        }
        match self.meta().head(bucket, &meta::sync_marker_key()).await {
            Ok(_) => {
                self.buckets.mark_ready(bucket);
                Ok(Readiness::Ready)
            }
            // Marker absent, or the `<meta>` bucket itself is gone: the cache is not authoritative.
            Err(Error::NotFound) | Err(Error::NoSuchBucket) => {
                if self.remote().head_bucket(bucket).await.is_ok() {
                    self.buckets.restore(bucket);
                    Ok(Readiness::Restoring)
                } else {
                    Ok(Readiness::Absent)
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Resolve a key's current state from whichever source is authoritative for its bucket right now.
    /// An absent *bucket* is `NoSuchBucket`; an absent *key* is [`KeyState::Absent`].
    pub(super) async fn resolve_key(&self, bucket: &str, key: &str) -> S3Result<KeyState> {
        match self.readiness(bucket).await? {
            Readiness::Absent => Err(Error::NoSuchBucket.into()),
            Readiness::Ready => self.resolve_key_cache(bucket, key).await,
            Readiness::Restoring => self.resolve_key_remote(bucket, key).await,
        }
    }

    /// Resolve a key from the cache tombstone namespace (§7): the classifier every cache-authoritative
    /// read shares — live body, eviction tombstone, delete tombstone, or a transition mark that
    /// resolves from the remote.
    async fn resolve_key_cache(&self, bucket: &str, key: &str) -> S3Result<KeyState> {
        let head = match self.data().head(bucket, key).await {
            Ok(h) => h,
            Err(Error::NotFound) => return Ok(KeyState::Absent),
            Err(e) => return Err(e.into()),
        };
        let md = head.metadata.clone().unwrap_or_default();
        match meta::tomb_kind(&md) {
            Some(meta::TombKind::Delete) => Ok(KeyState::Absent),
            Some(meta::TombKind::Evict) => Ok(KeyState::Remote {
                facts: facts_from_tombstone(key, &md)?,
                md,
            }),
            Some(meta::TombKind::Transit) => match self.resolve_transit(bucket, key).await? {
                None => Ok(KeyState::Absent),
                Some(facts) => Ok(KeyState::Remote { facts, md }),
            },
            None => Ok(KeyState::CacheBody {
                head: Box::new(head),
                md,
            }),
        }
    }

    /// Resolve a key straight from the remote — the read source of truth while the cache restores.
    /// Facts come off the object's tail trailer; user metadata is empty (the trailer carries none).
    async fn resolve_key_remote(&self, bucket: &str, key: &str) -> S3Result<KeyState> {
        match self.remote().head(bucket, key).await {
            Ok(h) => {
                let facts = self.tier.remote_facts(bucket, key, &h).await?;
                Ok(KeyState::Remote {
                    facts,
                    md: HashMap::new(),
                })
            }
            Err(Error::NotFound) => Ok(KeyState::Absent),
            Err(e) => Err(e.into()),
        }
    }

    /// Ready a bucket+key for a durable write under the overlay (§7). Serving is never gated: a
    /// `Restoring` bucket first has K materialized from the remote into the cache — under K's lock so
    /// it doesn't race the write's own bracket — leaving a correct tombstone for conditional
    /// evaluation. An absent bucket is `NoSuchBucket`.
    pub(super) async fn prepare_write(&self, bucket: &str, key: &str) -> S3Result<()> {
        match self.readiness(bucket).await? {
            Readiness::Absent => Err(Error::NoSuchBucket.into()),
            Readiness::Ready => Ok(()),
            Readiness::Restoring => {
                let _guard = self.tier.locks.lock(key).await;
                // The background restore provisions the projections and rebuilds the namespace, but
                // a write can beat it here — ensure the cache buckets exist (idempotent) so K's
                // materialization lands, then settle K to the remote's current state.
                crate::bucket_ctl::ensure_cache_bucket(self.data(), bucket).await?;
                crate::bucket_ctl::ensure_cache_bucket(self.meta(), bucket).await?;
                self.tier.repair_locked(bucket, key).await?;
                Ok(())
            }
        }
    }

    /// Project a remote LIST page into client-visible entries while the cache restores (§7): each
    /// object's plaintext facts come from its authenticated tail trailer, fanned out bounded. An
    /// object with no hypha trailer (foreign / never written through hypha) is dropped. Common
    /// prefixes are pure client keyspace and pass through. The caller forwards the remote's own
    /// pagination, so a restore-time LIST paginates exactly as the steady-state one does.
    pub(super) async fn project_remote_page(
        &self,
        bucket: &str,
        objs: Vec<aws_sdk_s3::types::Object>,
        raw_prefixes: Vec<aws_sdk_s3::types::CommonPrefix>,
    ) -> S3Result<(Vec<Object>, Vec<CommonPrefix>)> {
        let keys: Vec<String> = objs.into_iter().filter_map(|o| o.key).collect();
        let mut entries: Vec<Object> = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(REMOTE_LIST_FANOUT) {
            let batch =
                futures::future::try_join_all(chunk.iter().map(|k| self.remote_list_entry(bucket, k)))
                    .await?;
            entries.extend(batch.into_iter().flatten());
        }
        let common_prefixes = raw_prefixes
            .into_iter()
            .map(|cp| CommonPrefix { prefix: cp.prefix })
            .collect();
        Ok((entries, common_prefixes))
    }

    /// One remote LIST entry's client-visible facts, off its tail trailer. `None` if the object
    /// carries no hypha trailer.
    async fn remote_list_entry(&self, bucket: &str, key: &str) -> Result<Option<Object>, Error> {
        Ok(self.tier.read_tail(bucket, key).await?.map(|tail| Object {
            key: Some(key.to_string()),
            size: Some(tail.footer.plen as i64),
            e_tag: Some(ETag::Strong(tail.footer.client_etag())),
            last_modified: Some(ts_ms(tail.footer.mtime_ms.max(0))),
            ..Default::default()
        }))
    }
}
