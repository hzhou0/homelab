//! Bucket operations. The remote is the commit for Create/Delete; the state map is what every
//! other op reads, since only it distinguishes a bucket that is gone from one whose delete has not
//! decided yet ([`Hypha::require_bucket`]).

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
        self.require_bucket(&req.input.bucket)?;
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
        self.require_bucket(&req.input.bucket)?;
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
        self.require_bucket(&req.input.bucket)?;
        let resp = GetBucketLocationOutput {
            location_constraint: Some(BucketLocationConstraint::from(
                self.remote().region().to_string(),
            )),
        };
        Ok(S3Response::new(resp))
    }
}
