//! The restore overlay: one interface every data op routes through so a bucket mid-restore is served
//! from the remote instead of its half-rebuilt cache (§7 *Buckets*).
//!
//! A bucket's [`Readiness`] — resolved at startup and published by the bucket-control actor, so
//! reading it costs one atomic load — selects the source every op resolves against:
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

use hypha_core::config::Mode;
use hypha_core::error::Error;
use hypha_core::meta;

use super::get::facts_from_tombstone;
use super::{ts_ms, Hypha};
use crate::bucket_ctl::Readiness;
use crate::tier::RemoteFacts;

/// Bounded fan-out for the per-key trailer reads a remote-served LIST page needs (§7).
const REMOTE_LIST_FANOUT: usize = 16;

/// Which write semantics an op runs under (§4/§7) — deliberately a property of the *bucket*, not of
/// the deployment.
pub(super) enum WriteMode {
    /// The remote is the commit point: bracket the write, upload inline, ack once durable.
    ///
    /// A cached deployment runs this too, for the whole of a bucket's namespace restore. The
    /// restore's premise is that the cache holds nothing authoritative — that is what lets the
    /// remote be the read source of truth, and what lets the restore be purely additive — and a
    /// cached write would falsify it the moment it acked, leaving committed state in a namespace
    /// every reader is being told to ignore. Running durable for the window is what makes the
    /// premise true rather than merely hoped for; the cost is remote latency on writes into a
    /// bucket that is already rebuilding.
    Durable,
    /// The cache write is the commit: ack on it, and owe the remote a pending marker (§7).
    Cached,
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
    /// An absent *bucket* is `NoSuchBucket`; an absent *key* is [`KeyState::Absent`].
    pub(super) async fn resolve_key(&self, bucket: &str, key: &str) -> S3Result<KeyState> {
        match self.buckets.readiness(bucket) {
            Readiness::Absent => Err(Error::NoSuchBucket.into()),
            Readiness::Ready => self.resolve_key_cache(bucket, key).await,
            Readiness::Restoring => self.resolve_key_remote(bucket, key).await,
        }
    }

    /// The tombstone classifier every cache-authoritative read shares (§7).
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

    /// The read source of truth while the cache restores. Facts come off the object's tail trailer;
    /// user metadata is empty (the trailer carries none).
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

    /// The semantics `bucket`'s writes run under right now (§7). Not simply the deployment's
    /// configured mode — see [`WriteMode::Durable`]. An absent bucket is `NoSuchBucket`.
    pub(super) async fn write_mode(&self, bucket: &str) -> S3Result<WriteMode> {
        match self.buckets.readiness(bucket) {
            Readiness::Absent => Err(Error::NoSuchBucket.into()),
            Readiness::Ready if self.mode == Mode::Cached => Ok(WriteMode::Cached),
            _ => Ok(WriteMode::Durable),
        }
    }

    /// Ready a bucket+key for a write under the overlay, and report the semantics it must run under
    /// (§7). Serving is never gated: a `Restoring` bucket first has K materialized from the remote
    /// into the cache — under K's lock so it doesn't race the write's own bracket — leaving a correct
    /// entry for conditional evaluation.
    pub(super) async fn prepare_write(&self, bucket: &str, key: &str) -> S3Result<WriteMode> {
        match self.buckets.readiness(bucket) {
            Readiness::Absent => Err(Error::NoSuchBucket.into()),
            Readiness::Ready if self.mode == Mode::Cached => Ok(WriteMode::Cached),
            Readiness::Ready => Ok(WriteMode::Durable),
            Readiness::Restoring => {
                let _guard = self.tier.locks.lock(key).await;
                // The background restore provisions the projections and rebuilds the namespace, but
                // a write can beat it here — have the actor provision on demand so K's
                // materialization lands. Coalesced there, so a burst of writes into a lost-volume
                // bucket costs one round, not one per request.
                self.buckets.provision(bucket).await?;
                self.tier.materialize_absent_locked(bucket, key).await?;
                Ok(WriteMode::Durable)
            }
        }
    }

    /// Validate a bucket exists, kicking its restore if unreconciled — the overlay hook for ops
    /// that route around the cache entirely (the multipart part path, §7) and so have no key
    /// state to materialize.
    pub(super) async fn check_bucket(&self, bucket: &str) -> S3Result<()> {
        match self.buckets.readiness(bucket) {
            Readiness::Absent => Err(Error::NoSuchBucket.into()),
            _ => Ok(()),
        }
    }

    /// Project a remote LIST page into client-visible entries while the cache restores (§7): each
    /// object's plaintext facts come from its authenticated tail trailer, fanned out bounded. Common
    /// prefixes are pure client keyspace and pass through. The caller forwards the remote's own
    /// pagination, so a restore-time LIST paginates exactly as the steady-state one does.
    pub(super) async fn project_remote_page(
        &self,
        bucket: &str,
        objs: Vec<aws_sdk_s3::types::Object>,
        raw_prefixes: Vec<aws_sdk_s3::types::CommonPrefix>,
    ) -> S3Result<(Vec<Object>, Vec<CommonPrefix>)> {
        let keys: Vec<String> = objs
            .into_iter()
            .filter_map(|o| o.key)
            .filter(|k| !meta::is_reserved_remote_key(k))
            .collect();
        let mut entries: Vec<Object> = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(REMOTE_LIST_FANOUT) {
            let batch = futures::future::try_join_all(
                chunk.iter().map(|k| self.remote_list_entry(bucket, k)),
            )
            .await?;
            entries.extend(batch);
        }
        let common_prefixes = raw_prefixes
            .into_iter()
            .map(|cp| CommonPrefix { prefix: cp.prefix })
            .collect();
        Ok((entries, common_prefixes))
    }

    /// One remote LIST entry's client-visible facts, off its tail trailer. A trailer that does not
    /// authenticate is fatal ([`hypha_core::fatal`]) — hypha is the only writer of these buckets, so
    /// the object cannot be dismissed as foreign junk.
    async fn remote_list_entry(&self, bucket: &str, key: &str) -> Result<Object, Error> {
        let Some(tail) = self.tier.read_tail(bucket, key).await? else {
            self.tier.halt.foreign_object(bucket, key).await
        };
        Ok(Object {
            key: Some(key.to_string()),
            size: Some(tail.footer.plen as i64),
            e_tag: Some(ETag::Strong(tail.footer.client_etag())),
            last_modified: Some(ts_ms(tail.footer.mtime_ms.max(0))),
            ..Default::default()
        })
    }
}
