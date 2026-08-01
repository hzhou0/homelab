//! Routes operations around a half-restored cache namespace.
//!
//! Reads resolve remotely while restoring. Writes materialize the destination's remote state first
//! and use durable semantics so they never commit into a namespace readers are ignoring.

use std::collections::HashMap;

use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use s3s::dto::*;
use s3s::S3Result;

use hypha_core::config::Mode;
use hypha_core::error::Error;
use hypha_core::meta;

use super::get::facts_from_tombstone;
use super::{ts_ms, Hypha};
use crate::bucket::{Admission, Readout, Refusal, WriteGuard};
use crate::gc::Plaintext;
use crate::tier::RemoteFacts;

/// Bounded fan-out for the per-key trailer reads a remote-served LIST page needs (§7).
const REMOTE_LIST_FANOUT: usize = 16;

/// The gate's two refusals, in the one place they become status codes. `Absent` is definitive;
/// `Closed` is a `DeleteBucket` that has not decided, so the client must be told to retry rather
/// than handed a `NoSuchBucket` it would be entitled to cache.
pub(super) fn refuse(refusal: Refusal) -> s3s::S3Error {
    match refusal {
        Refusal::Absent => Error::NoSuchBucket.into(),
        Refusal::Closed => Error::OperationAborted.into(),
    }
}

/// Write semantics are a bucket property because cached deployments use durable writes during
/// restore.
pub(super) enum WriteMode {
    Durable,
    Cached,
}

pub(super) enum KeyState {
    Absent,
    Remote {
        facts: RemoteFacts,
        md: HashMap<String, String>,
    },
    CacheBody {
        head: Box<HeadObjectOutput>,
        md: HashMap<String, String>,
    },
}

impl Hypha {
    /// An absent *bucket* is `NoSuchBucket`; an absent *key* is [`KeyState::Absent`].
    ///
    /// The ticket a `Restoring` bucket hands out is held for the whole remote answer: a cached-mode
    /// write admitted while it is out defers to durable semantics, so the remote cannot answer this
    /// read with data the write has already superseded in the cache.
    pub(super) async fn resolve_key(&self, bucket: &str, key: &str) -> S3Result<KeyState> {
        let state = match self.buckets.read_ticket(bucket).map_err(refuse)? {
            Readout::Cache => self.resolve_key_cache(bucket, key).await?,
            Readout::Remote(_ticket) => self.resolve_key_remote(bucket, key).await?,
        };
        // The read half of §8's recency feed, here rather than at each caller because *this* is what
        // "an op that resolves a single key" means — GET, HEAD, GetObjectAttributes, and both copy
        // sources reach the ring by construction, and a future single-key read cannot forget to.
        // LIST resolves pages elsewhere and never lands here, which is exactly the exclusion §8
        // wants. An absent key has no body to protect.
        // Which artifact holds the plaintext decides what the touch protects: a composite's lives in
        // K's shadow, and K itself holds a tombstone no eviction would take.
        match &state {
            KeyState::Absent => {}
            KeyState::Remote { facts, .. } => {
                self.gc.touch(bucket, key, Plaintext::of(&facts.cetag))
            }
            KeyState::CacheBody { .. } => self.gc.touch(bucket, key, Plaintext::AtKey),
        }
        Ok(state)
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
    /// user metadata is empty (the trailer carries none) except for the content type, which the
    /// remote object carries natively and so survives the cache loss this pass is recovering from.
    async fn resolve_key_remote(&self, bucket: &str, key: &str) -> S3Result<KeyState> {
        match self.remote().head(bucket, key).await {
            Ok(h) => {
                let mut md = HashMap::new();
                if let Some(ct) = h.content_type() {
                    md.insert(meta::CTYPE.to_string(), meta::encode_content_type(ct));
                }
                let facts = self.tier.remote_facts(bucket, key, &h).await?;
                Ok(KeyState::Remote { facts, md })
            }
            Err(Error::NotFound) => Ok(KeyState::Absent),
            Err(e) => Err(e.into()),
        }
    }

    /// The semantics `bucket`'s writes run under right now (§7). Not simply the deployment's
    /// configured mode — see [`WriteMode::Durable`].
    pub(super) fn write_mode(&self, bucket: &str) -> S3Result<(WriteGuard, WriteMode)> {
        let (gate, admission) = self.enter_write(bucket)?;
        Ok((gate, self.write_mode_for(admission)))
    }

    /// Ready a bucket+key for a write under the overlay, and report the semantics it must run under
    /// (§7). Serving is never gated: a durably-admitted write first has K materialized from the
    /// remote into the cache — under K's lock so it doesn't race the write's own bracket — leaving a
    /// correct entry for conditional evaluation.
    pub(super) async fn prepare_write(
        &self,
        bucket: &str,
        key: &str,
    ) -> S3Result<(WriteGuard, WriteMode)> {
        let (gate, admission) = self.enter_write(bucket)?;
        if admission == Admission::Durable {
            let _guard = self.tier.locks.lock(key).await;
            // The background restore provisions the projections and rebuilds the namespace, but a
            // write can beat it here — have the actor provision on demand so K's materialization
            // lands. Coalesced there, so a burst of writes into a lost-volume bucket costs one
            // round, not one per request. Safe to create projections a delete might be draining
            // only because the gate above is held: the drain cannot have started while this write
            // is inside it.
            self.buckets.provision(bucket).await?;
            // Additive, so the other durable admission — a `Ready` bucket with a straggler's ticket
            // out — costs at most one remote probe for a key the cache correctly has no entry for.
            self.tier.materialize_absent_locked(bucket, key).await?;
        }
        Ok((gate, self.write_mode_for(admission)))
    }

    /// Validate a bucket exists — the overlay hook for ops that route around the cache entirely (the
    /// multipart part path, §7) and so have no key state to materialize.
    pub(super) fn check_bucket(&self, bucket: &str) -> S3Result<WriteGuard> {
        Ok(self.enter_write(bucket)?.0)
    }

    /// The write's claim on the bucket existing, held for the whole op (§7). The `Admission` **is**
    /// the classification, taken in the CAS that admitted the write, so nothing can move between the
    /// two. A bucket whose gate is closed — a `DeleteBucket` between its emptiness check and its
    /// commit — answers `OperationAborted`, not `NoSuchBucket`: its fate is undecided, and a
    /// permanent `NoSuchBucket` would be wrong if the delete then fails.
    fn enter_write(&self, bucket: &str) -> S3Result<(WriteGuard, Admission)> {
        self.buckets.enter_write(bucket).map_err(refuse)
    }

    /// Cache-first only where the deployment asks for it *and* the gate says the namespace can take
    /// it; a durable deployment never has a cache-first commit to offer.
    fn write_mode_for(&self, admission: Admission) -> WriteMode {
        match admission {
            Admission::CachedEligible if self.mode == Mode::Cached => WriteMode::Cached,
            _ => WriteMode::Durable,
        }
    }

    /// The bucket-metadata probes, which resolve no key: they need the gate's verdict and nothing
    /// else, so they drop the ticket immediately. The state map is the existence authority — it is
    /// resolved in full at startup and maintained by Create/Delete since, and it is the only source
    /// that tells a bucket that is gone from one whose delete has not decided yet.
    pub(super) fn require_bucket(&self, bucket: &str) -> S3Result<()> {
        self.buckets.read_ticket(bucket).map_err(refuse)?;
        Ok(())
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
