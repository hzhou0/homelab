//! Multipart (Phase 3) — per-part age files, composite part table, 4 GiB admission cap.
//! Stubs for now; Phase 2 lands the single-object path first.

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use super::Hypha;

impl Hypha {
    pub(super) async fn op_create_multipart_upload(
        &self,
        _req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        Err(s3_error!(NotImplemented, "multipart pending"))
    }

    pub(super) async fn op_upload_part(
        &self,
        _req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        Err(s3_error!(NotImplemented, "multipart pending"))
    }

    pub(super) async fn op_complete_multipart_upload(
        &self,
        _req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        Err(s3_error!(NotImplemented, "multipart pending"))
    }

    pub(super) async fn op_abort_multipart_upload(
        &self,
        _req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        Err(s3_error!(NotImplemented, "multipart pending"))
    }
}
