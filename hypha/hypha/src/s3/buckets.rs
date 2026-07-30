//! Bucket ops. Like multipart (§7), these route by the **remote as source of truth** and are
//! **always durable** — synchronous to the remote regardless of mode, no cache/marker machinery.
//! Existence and listing are answered from the remote; the cache bucket exists only so object-side
//! state (bodies, tombstones, twins, mpu records) has somewhere to live, so it is created/deleted
//! alongside but is never the authority.
//!
//! Lifecycle (Create/Delete) is owned by the bucket-control actor ([`crate::bucket`]), the sole
//! writer of the cache substrate: these ops validate, then hand off and await the remote's own
//! result. The remote create/delete is the commit; the cache projections are provisioned/drained
//! around it. The client's bucket passes through, mapped under each backend's own prefix
//! (backend.rs).

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::meta;

use super::{ts_ms, Hypha};

impl Hypha {
    pub(super) async fn op_create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = &req.input.bucket;
        // Reject over-long names up front rather than as an opaque backend error (§7 *Buckets* —
        // bucket-name budget).
        meta::validate_bucket_name(bucket, self.max_bucket_prefix_len)
            .map_err(|e| s3_error!(InvalidBucketName, "{e}"))?;
        self.buckets.create(bucket).await?;
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    pub(super) async fn op_delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.buckets.delete(&req.input.bucket).await?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
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

    /// **GetBucketVersioning** (§7): a benign stub, not a backend call — hypha buckets never carry
    /// versioning, but common S3 clients probe this up front and a `501` aborts them where "not
    /// enabled" passes.
    pub(super) async fn op_get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        self.remote().head_bucket(&req.input.bucket).await?;
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
        self.remote().head_bucket(&req.input.bucket).await?;
        let resp = GetBucketLocationOutput {
            location_constraint: Some(BucketLocationConstraint::from(
                self.remote().region().to_string(),
            )),
        };
        Ok(S3Response::new(resp))
    }
}
