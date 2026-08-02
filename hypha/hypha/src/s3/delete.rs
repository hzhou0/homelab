//! Single and batch deletion using the same per-key commit bracket.
//!
//! Only the remote leg batches; cache projection and error handling remain per key.

use std::collections::HashMap;

use futures::StreamExt as _;
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::backend::BatchDeleteError;
use hypha_core::error::Error;
use hypha_core::meta;

use super::overlay::WriteMode;
use super::Hypha;

const MAX_BATCH_KEYS: usize = 1000;

/// How many of a batch's per-key cache legs run at once. The keys are independent, but a 1000-key
/// batch must not open 1000 simultaneous cache requests.
const BATCH_FANOUT: usize = 32;

impl Hypha {
    pub(super) async fn op_delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;

        let (_gate, mode) = self.prepare_write(&bucket, &key).await?;
        if let WriteMode::Cached = mode {
            // Admission gate (§7) — see `op_put_object_cached`.
            if !self.tier.pressure.admit() {
                return Err(Error::SlowDown.into());
            }
            self.commit_cached_delete(&bucket, &key).await?;
            return Ok(S3Response::new(DeleteObjectOutput::default()));
        }

        let _guard = self.write_lock(&bucket, &key).await;

        self.repair_leftover_mark_locked(&bucket, &key).await?;

        // Mark → commit → settle. Crash before the remote delete: the object survives and repair
        // restores its projection. Crash after: 404 everywhere, repair removes the entry.
        self.tier.mark_transit_locked(&bucket, &key).await?;
        match self.remote().delete(&bucket, &key).await {
            Ok(()) | Err(Error::NotFound) => {}
            Err(e) => {
                // Failed or indeterminate commit — settle K to what the remote actually holds.
                if let Err(re) = self.tier.repair_locked(&bucket, &key).await {
                    tracing::warn!(key = %key, error = %re, "repair after failed commit did not settle; leftover mark repaired on next access");
                }
                return Err(e.into());
            }
        }
        self.tier.settle_absent_locked(&bucket, &key).await?;

        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    /// A fan-out of ≤ 1000 independent single-key deletes, **never a raw backend batch over client
    /// state**. Deleting an absent key is a success; `VersionId` is ignored (versioning is exempt).
    pub(super) async fn op_delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = req.input.bucket;
        let quiet = req.input.delete.quiet.unwrap_or(false);
        let requested: Vec<String> = req
            .input
            .delete
            .objects
            .into_iter()
            .map(|obj| obj.key)
            .collect();

        if requested.is_empty() || requested.len() > MAX_BATCH_KEYS {
            return Err(s3_error!(
                MalformedXML,
                "DeleteObjects takes between 1 and 1000 objects"
            ));
        }
        let (_gate, mode) = self.write_mode(&bucket)?;
        if let WriteMode::Cached = mode {
            return self
                .op_delete_objects_cached(bucket, quiet, requested)
                .await;
        }

        // Per-key failures land here; a key absent from the map at the end succeeded.
        let mut failed: HashMap<String, BatchDeleteError> = HashMap::new();

        let keys = self.admitted_keys(&bucket, &requested, &mut failed).await?;

        // Sequentially, in sorted order — the deadlock-freedom argument is the acquisition order.
        let mut guards = Vec::with_capacity(keys.len());
        for key in &keys {
            guards.push(self.write_lock(&bucket, key).await);
        }

        // Mark each key, dropping any that couldn't be masked: an unmasked key must stay out of
        // the commit, or a crash could leave it deleted on the remote but live in the cache.
        let marked = self.mark_batch(&bucket, keys, &mut failed).await;

        // Commit: one native call, but each key's slice of it is still its own atomic commit.
        // A whole-call failure leaves every mark standing — the indeterminate outcome the repair
        // rule resolves — and fails the request outright.
        let remote_failed = self
            .remote()
            .delete_objects_reporting(&bucket, &marked)
            .await?;
        let settle: Vec<String> = {
            let refused: std::collections::HashSet<&str> =
                remote_failed.iter().map(|e| e.key.as_str()).collect();
            let settle = marked
                .iter()
                .filter(|k| !refused.contains(k.as_str()))
                .cloned()
                .collect();
            // A refused key keeps its mark, so readers resolve it from the remote — where it still
            // is — and the next access repairs it.
            failed.extend(remote_failed.into_iter().map(|e| (e.key.clone(), e)));
            settle
        };

        // Settle each confirmed delete. A settle failure is not a client failure: the commit
        // already happened, so a reader following the leftover mark to the remote gets the 404
        // this delete promised. Only the stale cache entry survives, until repair sweeps it.
        let bucket = bucket.as_str();
        futures::stream::iter(settle.iter())
            .for_each_concurrent(BATCH_FANOUT, |key| async move {
                if let Err(e) = self.tier.settle_absent_locked(bucket, key).await {
                    tracing::warn!(key = %key, error = %e, "settle after committed delete failed; leftover mark repaired on next access");
                }
            })
            .await;

        drop(guards);

        Ok(delete_objects_reply(quiet, requested, &failed))
    }

    /// Cached-mode DeleteObjects (§7): the remote isn't touched here, so there is nothing to batch —
    /// it is a per-key fan-out of the cached single delete (its own write lock, removal + marker),
    /// with reconcile propagating the remote deletes. Same S3 contract as durable: deleting an
    /// absent key succeeds, `VersionId` is ignored.
    async fn op_delete_objects_cached(
        &self,
        bucket: String,
        quiet: bool,
        requested: Vec<String>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        // Admission gate (§7), once per request rather than per key — see `op_put_object_cached`.
        if !self.tier.pressure.admit() {
            return Err(Error::SlowDown.into());
        }
        let mut failed: HashMap<String, BatchDeleteError> = HashMap::new();

        let keys = self.admitted_keys(&bucket, &requested, &mut failed).await?;

        let bucket = bucket.as_str();
        let outcomes = futures::stream::iter(keys)
            .map(|key| async move {
                let r = self.commit_cached_delete(bucket, &key).await;
                (key, r)
            })
            .buffer_unordered(BATCH_FANOUT)
            .collect::<Vec<_>>()
            .await;
        for (key, outcome) in outcomes {
            if let Err(e) = outcome {
                failed.insert(
                    key.clone(),
                    batch_error(&key, "InternalError", &e.to_string()),
                );
            }
        }

        Ok(delete_objects_reply(quiet, requested, &failed))
    }

    /// Cached-mode delete of one key (§7), under its write lock: remove K — the commit, which makes
    /// GET/HEAD/LIST agree immediately — then hand the DELETE marker to the same queue as PUT. A
    /// crash before the marker lands leaves the clean marker absent; R2 recovers the remote-only key
    /// as an interrupted delete.
    async fn commit_cached_delete(&self, bucket: &str, key: &str) -> Result<(), Error> {
        let _guard = self.write_lock(bucket, key).await;

        if let Err(e) = self.data().delete(bucket, key).await {
            // Indeterminate, not a rollback: the cache may have removed K and lost the response,
            // leaving the key client-absent with no DELETE marker behind it — and so a remote object
            // nothing would ever propagate the delete to. Withdrawing the bucket's accounting (§6) is
            // what puts R2 on the remote-only sighting next run, instead of a clean marker telling it
            // there is nothing to look for.
            self.buckets.unaccount(bucket);
            return Err(e);
        }
        self.markers.owe(bucket, key, meta::delete_marker_body());
        // A deleted K can never name a shadow's generation again, so any shadow it had is orphaned (§8).
        self.orphans.owe(bucket, key);
        Ok(())
    }

    /// Mark every key of a batch, returning those that carry a mark and so belong in the commit.
    /// Distinct keys, so the marks run concurrently; each holds its own lock already.
    async fn mark_batch(
        &self,
        bucket: &str,
        keys: Vec<String>,
        failed: &mut HashMap<String, BatchDeleteError>,
    ) -> Vec<String> {
        let outcomes = futures::stream::iter(keys)
            .map(|key| async move {
                let marked = async {
                    self.repair_leftover_mark_locked(bucket, &key).await?;
                    self.tier.mark_transit_locked(bucket, &key).await
                }
                .await;
                (key, marked)
            })
            .buffer_unordered(BATCH_FANOUT)
            .collect::<Vec<_>>()
            .await;

        let mut marked = Vec::with_capacity(outcomes.len());
        for (key, outcome) in outcomes {
            match outcome {
                Ok(()) => marked.push(key),
                Err(e) => {
                    failed.insert(
                        key.clone(),
                        batch_error(&key, "InternalError", &e.to_string()),
                    );
                }
            }
        }
        // The commit body follows the marks, so it stays sorted.
        marked.sort_unstable();
        marked
    }

    /// A leftover mark is repaired before an op takes its own (§7) — the bracket must start from a
    /// settled projection, or a stale mark could hide an object this delete should 404. Caller
    /// holds K's write lock.
    async fn repair_leftover_mark_locked(&self, bucket: &str, key: &str) -> Result<(), Error> {
        match self.data().head(bucket, key).await {
            Ok(head) => {
                if head.metadata.as_ref().and_then(meta::tomb_kind) == Some(meta::TombKind::Transit)
                {
                    self.tier.repair_locked(bucket, key).await?;
                }
                Ok(())
            }
            Err(Error::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// The batch's admission: validate every key and materialize each one's remote state before any
    /// is marked, so the batch runs against correct entries. The per-key gate is dropped at once —
    /// the batch's own, taken by the caller, covers the whole op (§7).
    async fn admitted_keys(
        &self,
        bucket: &str,
        requested: &[String],
        failed: &mut HashMap<String, BatchDeleteError>,
    ) -> S3Result<Vec<String>> {
        let keys = valid_sorted_keys(requested, failed);
        for key in &keys {
            let (_, _) = self.prepare_write(bucket, key).await?;
        }
        Ok(keys)
    }
}

fn batch_error(key: &str, code: &str, message: &str) -> BatchDeleteError {
    BatchDeleteError {
        key: key.to_string(),
        code: code.to_string(),
        message: message.to_string(),
    }
}

/// The batch's deduped, sorted, admission-validated keys. Sorted so overlapping batches acquire
/// their shared keys in the same order (two batch deletes can't deadlock) and deduped so a key
/// repeated within one request never waits on the lock it already holds; an invalid key fails its
/// own entry and the rest still commit. The reply is still built per *requested* entry.
fn valid_sorted_keys(
    requested: &[String],
    failed: &mut HashMap<String, BatchDeleteError>,
) -> Vec<String> {
    let mut keys: Vec<String> = requested.to_vec();
    keys.sort_unstable();
    keys.dedup();
    keys.retain(|key| match meta::validate_client_key(key) {
        Ok(()) => true,
        Err(why) => {
            failed.insert(key.clone(), batch_error(key, "InvalidArgument", why));
            false
        }
    });
    keys
}

/// The per-requested-entry reply: a key in `failed` reports its error, any other reports deleted —
/// unless `quiet`, which suppresses success entries.
fn delete_objects_reply(
    quiet: bool,
    requested: Vec<String>,
    failed: &HashMap<String, BatchDeleteError>,
) -> S3Response<DeleteObjectsOutput> {
    let mut deleted = Vec::new();
    let mut errors = Vec::new();
    for key in requested {
        match failed.get(&key) {
            Some(e) => errors.push(s3s::dto::Error {
                code: Some(e.code.clone()),
                message: Some(e.message.clone()),
                key: Some(key),
                version_id: None,
            }),
            None if !quiet => deleted.push(DeletedObject {
                key: Some(key),
                ..Default::default()
            }),
            None => {}
        }
    }
    S3Response::new(DeleteObjectsOutput {
        deleted: (!deleted.is_empty()).then_some(deleted),
        errors: (!errors.is_empty()).then_some(errors),
        ..Default::default()
    })
}
