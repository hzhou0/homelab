//! Thin `ObjectStore` wrapper over an `aws-sdk-s3` client. The cache and the remote are two
//! independently-configured instances of this same type (§2); Phase 2 uses only the remote.
//!
//! Buckets map one-to-one client ⇄ cache ⇄ remote (§7): the client's bucket rides every call, and
//! the wrapper prepends the deployment's **bucket prefix** so deployments sharing one remote
//! account land in disjoint bucket namespaces (and strips it back off on `ListBuckets`). The other
//! cross-cutting concern is the SDK-error → `hypha_core::Error` mapping. Everything else —
//! encryption, ETag math, DTO translation — stays in the handlers so this layer is a mechanical
//! passthrough.

use std::collections::HashMap;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::list_multipart_uploads::ListMultipartUploadsOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, Delete, EncodingType, ObjectIdentifier};
use aws_sdk_s3::Client;
use percent_encoding::percent_decode_str;

use crate::config::S3Endpoint;
use crate::error::{Error, Result};

#[derive(Clone)]
pub struct Backend {
    client: Client,
    region: String,
    bucket_prefix: String,
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
            region: cfg.region.clone(),
            bucket_prefix: cfg.bucket_prefix.clone(),
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The backend's SigV4 signing region (a dummy for SeaweedFS); surfaced for `GetBucketLocation`.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Backend bucket name for a client bucket: the deployment's bucket prefix keeps deployments
    /// sharing one remote account in disjoint bucket namespaces.
    fn bkt(&self, bucket: &str) -> String {
        format!("{}{}", self.bucket_prefix, bucket)
    }

    /// The client-visible name for a backend bucket, or `None` if it isn't under this deployment's
    /// prefix — a sibling deployment's bucket on a shared account, not ours to list.
    fn strip_bkt<'a>(&self, full: &'a str) -> Option<&'a str> {
        full.strip_prefix(&self.bucket_prefix)
    }

    pub async fn get(
        &self,
        bucket: &str,
        key: &str,
        range: Option<String>,
    ) -> Result<GetObjectOutput> {
        self.client
            .get_object()
            .bucket(self.bkt(bucket))
            .key(key)
            .set_range(range)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    pub async fn head(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput> {
        self.client
            .head_object()
            .bucket(self.bkt(bucket))
            .key(key)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    /// PUT a body already in its final on-remote form (ciphertext, for hypha's objects).
    /// `content_length` must be `Some` for a non-seekable `ByteStream` — S3 needs it up front.
    #[allow(clippy::too_many_arguments)]
    pub async fn put(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        content_length: Option<i64>,
        metadata: HashMap<String, String>,
        if_match: Option<String>,
        if_none_match: Option<String>,
    ) -> Result<PutObjectOutput> {
        self.client
            .put_object()
            .bucket(self.bkt(bucket))
            .key(key)
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
    #[allow(clippy::too_many_arguments)]
    pub async fn put_small(
        &self,
        bucket: &str,
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
            .bucket(self.bkt(bucket))
            .key(key)
            .body(ByteStream::from(bytes))
            .content_length(len)
            .set_metadata(Some(metadata))
            .set_if_match(if_match)
            .set_if_none_match(if_none_match)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(out
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string())
    }

    pub async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(self.bkt(bucket))
            .key(key)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    /// Batch-delete up to 1000 keys in one round trip (S3 `DeleteObjects`). Used to reclaim a
    /// key's twins and an upload's per-part records without a request per object. `quiet` so the
    /// response omits per-key success entries; a partial failure surfaces via `from_sdk`.
    pub async fn delete_objects(&self, bucket: &str, keys: &[String]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let build_err = |e: aws_sdk_s3::error::BuildError| {
            Error::Backend(format!("building DeleteObjects request: {e}"))
        };
        let objects = keys
            .iter()
            .map(|k| ObjectIdentifier::builder().key(k.as_str()).build())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(build_err)?;
        let delete = Delete::builder()
            .set_objects(Some(objects))
            .quiet(true)
            .build()
            .map_err(build_err)?;
        self.client
            .delete_objects()
            .bucket(self.bkt(bucket))
            .delete(delete)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn list(
        &self,
        bucket: &str,
        prefix: Option<String>,
        delimiter: Option<String>,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: Option<i32>,
    ) -> Result<ListObjectsV2Output> {
        // `encoding-type=url` so keys carrying bytes XML can't represent — the twin separator
        // `0x01`, and any control byte a client used — survive the LIST response (§6). Keys come
        // back percent-encoded; decode them before returning so callers see raw bytes.
        let mut out = self
            .client
            .list_objects_v2()
            .bucket(self.bkt(bucket))
            .set_prefix(prefix)
            .set_delimiter(delimiter)
            .set_continuation_token(continuation_token)
            .set_start_after(start_after)
            .set_max_keys(max_keys)
            .encoding_type(EncodingType::Url)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        for obj in out.contents.iter_mut().flatten() {
            obj.key = obj.key.take().map(|k| url_decode(&k));
        }
        for cp in out.common_prefixes.iter_mut().flatten() {
            cp.prefix = cp.prefix.take().map(|p| url_decode(&p));
        }
        Ok(out)
    }

    // ── Bucket ops ──────────────────────────────────────────────────────────────────────────

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.client
            .create_bucket()
            .bucket(self.bkt(bucket))
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        self.client
            .delete_bucket()
            .bucket(self.bkt(bucket))
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    pub async fn head_bucket(&self, bucket: &str) -> Result<()> {
        self.client
            .head_bucket()
            .bucket(self.bkt(bucket))
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    /// This deployment's buckets, as `(client_name, creation_ms)` — the backend's `ListBuckets`
    /// filtered to those under our prefix, with the prefix stripped so clients see their own names.
    pub async fn list_buckets(&self) -> Result<Vec<(String, Option<i64>)>> {
        let out = self
            .client
            .list_buckets()
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(out
            .buckets()
            .iter()
            .filter_map(|b| {
                let name = self.strip_bkt(b.name()?)?;
                Some((
                    name.to_string(),
                    b.creation_date().and_then(|d| d.to_millis().ok()),
                ))
            })
            .collect())
    }

    // ── Multipart-to-remote primitives (Phase 3) ────────────────────────────────────────────
    // hypha maps a client multipart upload onto a remote multipart upload at the composite key;
    // each part it uploads is an independent age file (§6), concatenated by the remote's own
    // CompleteMultipartUpload.

    pub async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        metadata: HashMap<String, String>,
    ) -> Result<CreateMultipartUploadOutput> {
        self.client
            .create_multipart_upload()
            .bucket(self.bkt(bucket))
            .key(key)
            .set_metadata(Some(metadata))
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: ByteStream,
        content_length: Option<i64>,
    ) -> Result<UploadPartOutput> {
        self.client
            .upload_part()
            .bucket(self.bkt(bucket))
            .key(key)
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
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: CompletedMultipartUpload,
    ) -> Result<CompleteMultipartUploadOutput> {
        self.client
            .complete_multipart_upload()
            .bucket(self.bkt(bucket))
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(parts)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    /// Every part currently held by an in-progress native upload, as `(part_number, etag, size)` —
    /// the remote's own last-write-wins-resolved view. Complete uses it to pick the winning parts
    /// and their ciphertext sizes (§7), so a re-uploaded part's stale hypha record never wins.
    /// Paginated; ETags are unquoted.
    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<(i32, String, u64)>> {
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let page = self
                .client
                .list_parts()
                .bucket(self.bkt(bucket))
                .key(key)
                .upload_id(upload_id)
                .max_parts(1000)
                .set_part_number_marker(marker)
                .send()
                .await
                .map_err(Error::from_sdk)?;
            for p in page.parts() {
                if let (Some(n), Some(sz)) = (p.part_number(), p.size()) {
                    let etag = p.e_tag().unwrap_or_default().trim_matches('"').to_string();
                    out.push((n, etag, sz.max(0) as u64));
                }
            }
            if page.is_truncated() != Some(true) {
                break;
            }
            marker = page.next_part_number_marker().map(str::to_string);
            if marker.is_none() {
                break;
            }
        }
        Ok(out)
    }

    /// One page of the remote's in-progress uploads — what the client-facing
    /// `ListMultipartUploads` proxies (§7). hypha creates each native upload *at the client key*
    /// and hands the client the remote's own upload id, so a page needs no translation; the
    /// backend's `(key, upload_id)` ordering and markers are what make the op's pagination correct.
    ///
    /// `prefix` and `delimiter` forward like the rest: S3 specifies both here (prefix filters keys,
    /// delimiter groups them into `CommonPrefixes`), so a compliant backend answers them natively.
    /// **MinIO is a known exception** — it returns matches only when the prefix equals a key
    /// exactly, closed "working as intended" (minio/minio#20989, #11686) — so a prefixed listing
    /// against MinIO comes back empty. That is the backend's deviation, not something hypha
    /// emulates around.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: Option<String>,
        delimiter: Option<String>,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        max_uploads: Option<i32>,
    ) -> Result<ListMultipartUploadsOutput> {
        // `encoding-type=url` for the same reason LIST uses it: a client key may carry control
        // bytes the response XML cannot represent. Keys come back percent-encoded; decode them so
        // callers see raw bytes.
        let mut out = self
            .client
            .list_multipart_uploads()
            .bucket(self.bkt(bucket))
            .set_prefix(prefix)
            .set_delimiter(delimiter)
            .set_key_marker(key_marker)
            .set_upload_id_marker(upload_id_marker)
            .set_max_uploads(max_uploads)
            .encoding_type(EncodingType::Url)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        for u in out.uploads.iter_mut().flatten() {
            u.key = u.key.take().map(|k| url_decode(&k));
        }
        for cp in out.common_prefixes.iter_mut().flatten() {
            cp.prefix = cp.prefix.take().map(|p| url_decode(&p));
        }
        // The marker is a key too, and echoes back into the next request.
        out.next_key_marker = out.next_key_marker.take().map(|m| url_decode(&m));
        Ok(out)
    }

    pub async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(self.bkt(bucket))
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }
}

/// Reverse `encoding-type=url` on a LIST-returned key. Keys are UTF-8; a stray non-UTF-8 sequence
/// (which hypha never writes) degrades lossily rather than erroring a whole page.
fn url_decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}
