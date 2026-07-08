//! Bucket ops. Buckets map one-to-one client ⇄ cache ⇄ remote (§7). Create is write-through
//! (cache then remote); delete mirrors it in reverse (remote first, so a crash between leaves a
//! retryable still-visible bucket, never a remote orphan). Rare control-plane events — no marker
//! machinery.

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use super::Hypha;

impl Hypha {
    pub(super) async fn op_create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.cache().create_bucket().await?;
        self.remote().create_bucket().await?;
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    pub(super) async fn op_delete_bucket(
        &self,
        _req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.remote().delete_bucket().await?;
        self.cache().delete_bucket().await?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }
}
