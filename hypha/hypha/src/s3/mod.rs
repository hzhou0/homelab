//! The `s3s::S3` trait implementation, split by op group (§3). Each submodule adds an
//! `impl Hypha` block; this module owns the struct and the trait wiring that dispatches to them.
//!
//! Phase 2 is the **durable** surface: writes go through the cache but ack only after the remote
//! is durable. Phase 4 adds cached mode (ack after cache write, async background upload).

mod buckets;
mod delete;
mod get;
mod list_head;
mod multipart;
mod put;

use std::collections::HashMap;
use std::sync::Arc;

use hypha_format::{Envelope, TrailerKey};
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::config::Mode;
use hypha_core::meta;
use hypha_core::Backend;

use crate::keylocks::KeyLocks;
use crate::tier::Reconciler;

#[derive(Clone)]
pub struct Hypha {
    /// Shared tiering machinery: cache + remote backends, the age envelope, and the per-key lock
    /// table. Every data-path op reaches the backends through here.
    pub tier: Reconciler,
    pub mode: Mode,
    /// Contiguous encrypt/decrypt above this offloads to `spawn_blocking` (§5). Unwired until
    /// an inline (non-offloaded) codec path exists — today every codec bridge offloads.
    #[allow(dead_code)]
    pub offload_threshold: usize,
}

impl Hypha {
    pub fn new(
        remote: Backend,
        cache: Backend,
        env: Envelope,
        trailer_key: TrailerKey,
        mode: Mode,
        offload_threshold: usize,
    ) -> Self {
        Self {
            tier: Reconciler {
                cache,
                remote,
                env: Arc::new(env),
                trailer_key,
                locks: KeyLocks::default(),
            },
            mode,
            offload_threshold,
        }
    }

    pub(crate) fn cache(&self) -> &Backend {
        &self.tier.cache
    }
    pub(crate) fn remote(&self) -> &Backend {
        &self.tier.remote
    }
    pub(crate) fn env(&self) -> Arc<Envelope> {
        self.tier.env.clone()
    }
}

/// Plaintext cap for any single encrypted upload leg — a PutObject body or one part (§7): the
/// framed form (age envelope + footer) must never push past the remote's 5 GiB PUT/part cap.
pub(crate) const MAX_INLINE_PLAINTEXT: u64 = 4 * 1024 * 1024 * 1024;

/// Unix-ms mtime (twin / tombstone metadata, §6) → an S3 `LastModified`.
pub(crate) fn ts_ms(ms: i64) -> Timestamp {
    let t = std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms.max(0) as u64);
    Timestamp::from(t)
}

/// Storage classes implying `RestoreObject`, which hypha's single physical tier cannot honour —
/// accepting one would promise a retrieval workflow that never arrives (§7).
const ARCHIVE_CLASSES: &[&str] = &[
    StorageClass::GLACIER,
    StorageClass::DEEP_ARCHIVE,
    StorageClass::GLACIER_IR,
    StorageClass::SNOW,
    StorageClass::OUTPOSTS,
];

/// Validate a requested `x-amz-storage-class` and resolve it to the label hypha will echo (§7).
/// One physical tier, so every non-archive class is accepted as-is and simply replayed on read.
pub(crate) fn resolve_storage_class(requested: Option<&StorageClass>) -> S3Result<String> {
    let Some(sc) = requested else {
        return Ok(meta::STANDARD.to_string());
    };
    if ARCHIVE_CLASSES.contains(&sc.as_str()) {
        return Err(s3_error!(
            InvalidStorageClass,
            "hypha has one storage tier; the archive classes are not supported"
        ));
    }
    Ok(sc.as_str().to_string())
}

/// The cache-side user-metadata a write carries alongside its facts (§7): the client's
/// `x-amz-meta-*` under hypha's namespace, plus the echoed storage class.
pub(crate) fn write_metadata(
    client: Option<&Metadata>,
    storage_class: &str,
) -> HashMap<String, String> {
    let mut md: HashMap<String, String> = client
        .map(|m| meta::encode_user_metadata(m).collect())
        .unwrap_or_default();
    md.insert(meta::SCLASS.to_string(), storage_class.to_string());
    md
}

/// The raw MD5 a client's `Content-MD5` header declares (base64 of the 16 digest bytes).
pub(crate) fn parse_content_md5(header: &str) -> S3Result<[u8; 16]> {
    let raw = base64_simd::STANDARD
        .decode_to_vec(header.as_bytes())
        .map_err(|_| s3_error!(InvalidDigest, "Content-MD5 is not valid base64"))?;
    <[u8; 16]>::try_from(raw.as_slice())
        .map_err(|_| s3_error!(InvalidDigest, "Content-MD5 must decode to 16 bytes"))
}

#[async_trait::async_trait]
impl s3s::S3 for Hypha {
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.op_abort_multipart_upload(req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.op_complete_multipart_upload(req).await
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.op_create_bucket(req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.op_create_multipart_upload(req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.op_delete_bucket(req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.op_delete_object(req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.op_get_bucket_location(req).await
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.op_get_object(req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.op_head_bucket(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.op_head_object(req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        self.op_list_buckets(req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.op_list_multipart_uploads(req).await
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.op_list_objects_v2(req).await
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        self.op_list_parts(req).await
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        self.op_put_object(req).await
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.op_upload_part(req).await
    }
}
