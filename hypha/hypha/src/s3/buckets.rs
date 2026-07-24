//! Bucket ops. Like multipart (§7), these route by the **remote as source of truth** and are
//! **always durable** — synchronous to the remote regardless of mode, no cache/marker machinery.
//! Existence and listing are answered from the remote; the cache bucket exists only so object-side
//! state (bodies, tombstones, twins, mpu records) has somewhere to live, so it is created/deleted
//! alongside but is never the authority.
//!
//! Create writes the cache projection first, then the remote — the remote create is the durable
//! commit; a crash before it leaves a harmless orphan cache bucket (the bucket simply doesn't
//! exist yet per the remote). Delete is the mirror: remote first (the durable commit that makes the
//! bucket cease to exist), then the cache — a crash between leaves a retryable cache orphan, never a
//! remote bucket the client believes is gone. The client's bucket passes through, mapped under each
//! backend's own prefix (backend.rs).

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;

use super::{ts_ms, Hypha};

impl Hypha {
    pub(super) async fn op_create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = &req.input.bucket;
        // The configured prefix is charged against S3's 63-byte cap; reject over-long names up
        // front rather than as an opaque backend error (§7 *Buckets*).
        meta::validate_bucket_name(bucket, self.max_bucket_prefix_len)
            .map_err(|e| s3_error!(InvalidBucketName, "{e}"))?;
        // Both cache projections first, then the remote — the remote create is the durable commit;
        // a crash before it leaves harmless orphan cache buckets. Idempotent, so retry repairs a
        // partial create.
        self.data().create_bucket(bucket).await?;
        self.meta().create_bucket(bucket).await?;
        self.remote().create_bucket(bucket).await?;
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    pub(super) async fn op_delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket = &req.input.bucket;
        // Remote first — the durable commit that makes the bucket cease to exist, and the emptiness
        // gate: the remote holds every committed object, so a non-empty client bucket fails here.
        self.remote().delete_bucket(bucket).await?;
        // Then both cache buckets. Emptiness is judged on the remote above, not on `<meta>`:
        // leftover twins/markers are hypha's own state, so drain them rather than let them block
        // the delete (§7 *Buckets*). `<data>` holds only bare-K tombstones after the client emptied
        // the bucket; drain defends against crash-leftover marks all the same.
        self.drain_and_delete_bucket(self.data(), bucket).await?;
        self.drain_and_delete_bucket(self.meta(), bucket).await?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    /// Empty a cache bucket, then delete it. Keys are deleted one at a time: the `<meta>` bucket's
    /// twins and mpu records carry the `0x01` control byte, which the batch `DeleteObjects` XML body
    /// cannot represent (§6). Buckets are rare control-plane events, so the per-key cost is fine.
    async fn drain_and_delete_bucket(
        &self,
        backend: &hypha_core::Backend,
        bucket: &str,
    ) -> Result<(), Error> {
        loop {
            let page = backend.list(bucket, None, None, None, None, None).await?;
            let keys: Vec<String> = page
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|o| o.key)
                .collect();
            if keys.is_empty() {
                break;
            }
            let deletes = keys.iter().map(|k| backend.delete(bucket, k));
            futures::future::try_join_all(deletes).await?;
            if page.is_truncated != Some(true) {
                break;
            }
        }
        backend.delete_bucket(bucket).await
    }

    pub(super) async fn op_head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.remote().head_bucket(&req.input.bucket).await?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    pub(super) async fn op_list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets: Vec<Bucket> = self
            .remote()
            .list_buckets()
            .await?
            .into_iter()
            .map(|(name, created)| Bucket {
                name: Some(name),
                creation_date: created.map(ts_ms),
                ..Default::default()
            })
            .collect();
        let resp = ListBucketsOutput {
            buckets: Some(buckets),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// **GetBucketVersioning** (§7): a benign stub — an empty versioning configuration, no backend
    /// call. hypha buckets never carry versioning, but `aws s3 sync` / boto / `mc` probe this up
    /// front and a `501` aborts them where "not enabled" passes; enabling it (`PutBucketVersioning`)
    /// stays exempt/rejected.
    pub(super) async fn op_get_bucket_versioning(
        &self,
        _req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let resp = GetBucketVersioningOutput {
            status: None,
            mfa_delete: Some(MFADeleteStatus::from_static(MFADeleteStatus::DISABLED)),
        };
        Ok(S3Response::new(resp))
    }

    pub(super) async fn op_get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        // Confirm existence against the source of truth, then report its backend region.
        self.remote().head_bucket(&req.input.bucket).await?;
        let resp = GetBucketLocationOutput {
            location_constraint: Some(BucketLocationConstraint::from(
                self.remote().region().to_string(),
            )),
        };
        Ok(S3Response::new(resp))
    }
}
