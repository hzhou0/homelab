//! Thin `ObjectStore` wrapper over an `aws-sdk-s3` client. The cache and the remote are two
//! independently-configured instances of this same type (§2); Phase 2 uses only the remote.
//!
//! The wrapper owns two concerns the op handlers should not repeat: the per-deployment key
//! `prefix` (architecture § *Two modes*) and the SDK-error → `hypha_core::Error`
//! mapping. Everything else — encryption, ETag math, DTO translation — stays in the handlers so
//! this layer is a mechanical passthrough.

use std::collections::HashMap;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::Client;

use crate::config::S3Endpoint;
use crate::error::{Error, Result};

#[derive(Clone)]
pub struct Backend {
    client: Client,
    bucket: String,
    prefix: String,
}

impl Backend {
    pub fn connect(cfg: &S3Endpoint) -> Self {
        let creds = Credentials::new(&cfg.access_key, &cfg.secret_key, None, None, "hypha");
        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(cfg.region.clone()))
            .endpoint_url(&cfg.endpoint)
            .credentials_provider(creds)
            // SeaweedFS/MinIO are path-style; virtual-host addressing needs per-bucket DNS.
            .force_path_style(true)
            .build();
        Self {
            client: Client::from_conf(conf),
            bucket: cfg.bucket.clone(),
            prefix: cfg.prefix.clone(),
        }
    }

    /// Full backend key for a client-visible key: the deployment prefix keeps deployments that
    /// share one remote in disjoint keyspaces.
    pub fn k(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Strip the deployment prefix off a full backend key for the client-visible form.
    /// Falls back to the full key if it somehow lacks the prefix.
    pub fn strip<'a>(&self, full: &'a str) -> &'a str {
        full.strip_prefix(&self.prefix).unwrap_or(full)
    }

    pub async fn get(&self, key: &str, range: Option<String>) -> Result<GetObjectOutput> {
        self.client
            .get_object()
            .bucket(&self.bucket)
            .key(self.k(key))
            .set_range(range)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    pub async fn head(&self, key: &str) -> Result<HeadObjectOutput> {
        self.client
            .head_object()
            .bucket(&self.bucket)
            .key(self.k(key))
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    /// PUT a body already in its final on-remote form (ciphertext, for hypha's objects).
    /// `content_length` must be `Some` for a non-seekable `ByteStream` — S3 needs it up front.
    #[allow(clippy::too_many_arguments)]
    pub async fn put(
        &self,
        key: &str,
        body: ByteStream,
        content_length: Option<i64>,
        metadata: HashMap<String, String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
    ) -> Result<PutObjectOutput> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.k(key))
            .body(body)
            .set_content_length(content_length)
            .set_metadata(Some(metadata))
            .set_if_match(if_match)
            .set_if_none_match(if_none_match)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    /// PUT a small in-memory body (tombstone sentinel, zero-byte twin) with optional conditions.
    /// Returns the object's new cache ETag (unquoted).
    pub async fn put_small(
        &self,
        key: &str,
        bytes: Vec<u8>,
        metadata: HashMap<String, String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
    ) -> Result<String> {
        let len = bytes.len() as i64;
        let out = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(self.k(key))
            .body(ByteStream::from(bytes))
            .content_length(len)
            .set_metadata(Some(metadata))
            .set_if_match(if_match)
            .set_if_none_match(if_none_match)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(out.e_tag().unwrap_or_default().trim_matches('"').to_string())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.k(key))
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn list(
        &self,
        prefix: Option<String>,
        delimiter: Option<String>,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: Option<i32>,
    ) -> Result<ListObjectsV2Output> {
        // Fold the deployment prefix into the client-supplied one so listings stay scoped.
        let scoped_prefix = Some(self.k(prefix.as_deref().unwrap_or("")));
        self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_prefix(scoped_prefix)
            .set_delimiter(delimiter)
            .set_continuation_token(continuation_token)
            .set_start_after(start_after.map(|s| self.k(&s)))
            .set_max_keys(max_keys)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    pub async fn create_bucket(&self) -> Result<()> {
        self.client
            .create_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    pub async fn delete_bucket(&self) -> Result<()> {
        self.client
            .delete_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    // ── Multipart-to-remote primitives (Phase 3) ────────────────────────────────────────────
    // hypha maps a client multipart upload onto a remote multipart upload at the composite key;
    // each part it uploads is an independent age file (§6), concatenated by the remote's own
    // CompleteMultipartUpload.

    pub async fn create_multipart(
        &self,
        key: &str,
        metadata: HashMap<String, String>,
    ) -> Result<CreateMultipartUploadOutput> {
        self.client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(self.k(key))
            .set_metadata(Some(metadata))
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    pub async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: ByteStream,
        content_length: Option<i64>,
    ) -> Result<UploadPartOutput> {
        self.client
            .upload_part()
            .bucket(&self.bucket)
            .key(self.k(key))
            .upload_id(upload_id)
            .part_number(part_number)
            .body(body)
            .set_content_length(content_length)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    pub async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: CompletedMultipartUpload,
    ) -> Result<CompleteMultipartUploadOutput> {
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(self.k(key))
            .upload_id(upload_id)
            .multipart_upload(parts)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    pub async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(self.k(key))
            .upload_id(upload_id)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }
}
