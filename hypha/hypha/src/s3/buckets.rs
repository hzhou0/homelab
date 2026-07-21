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
use s3s::{S3Request, S3Response, S3Result};

use super::{ts_ms, Hypha};

impl Hypha {
    pub(super) async fn op_create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = &req.input.bucket;
        self.cache().create_bucket(bucket).await?;
        self.remote().create_bucket(bucket).await?;
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    pub(super) async fn op_delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket = &req.input.bucket;
        self.remote().delete_bucket(bucket).await?;
        self.cache().delete_bucket(bucket).await?;
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
