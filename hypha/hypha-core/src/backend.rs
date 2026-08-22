//! Prefixed AWS S3 backend client.

use std::collections::HashMap;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::copy_object::CopyObjectOutput;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::list_multipart_uploads::ListMultipartUploadsOutput;
use aws_sdk_s3::operation::list_objects::ListObjectsOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::operation::upload_part_copy::UploadPartCopyOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, Delete, EncodingType, MetadataDirective, ObjectIdentifier,
};
use aws_sdk_s3::Client;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

use crate::config::S3Endpoint;
use crate::error::{Error, Result};
use hypha_format::ChecksumAlgorithm;

#[derive(Clone, Debug)]
pub struct PutChecksum {
    pub algorithm: ChecksumAlgorithm,
    pub value: Option<String>,
}

#[derive(Default)]
pub struct PutOptions {
    pub content_length: Option<i64>,
    pub metadata: HashMap<String, String>,
    pub content_md5: Option<String>,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub content_type: Option<String>,
    pub checksum: Option<PutChecksum>,
}

pub struct CopyRequest<'a> {
    pub destination_bucket: &'a str,
    pub destination_key: &'a str,
    pub source_bucket: &'a str,
    pub source_key: &'a str,
    pub source_if_match: String,
    pub metadata: HashMap<String, String>,
    pub content_type: Option<String>,
}

pub struct UploadPartRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub part_number: i32,
    pub body: ByteStream,
    pub content_length: Option<i64>,
}

pub struct UploadPartCopyRequest<'a> {
    pub destination: UploadPartDestination<'a>,
    pub source_bucket: &'a str,
    pub source_key: &'a str,
    pub source_range: Option<String>,
    pub source_if_match: Option<String>,
}

pub struct UploadPartDestination<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub part_number: i32,
}

/// One entry of a `ListParts` page.
///
/// A struct rather than a `(number, etag, size)` tuple because **the part number is not a key**: S3
/// replaces a re-uploaded part, but SeaweedFS keeps every upload of one and lists them all, so a
/// caller has to say which entry of a number it means. `last_modified` is the only ordering signal
/// the protocol carries, and it is second-granular — enough to separate a deliberate re-upload,
/// never enough to order a race.
#[derive(Clone, Debug)]
pub struct RemotePart {
    pub number: i32,
    /// The backend's own ETag for this upload of the part — its last-write-wins token everywhere in
    /// hypha, and what `CompleteMultipartUpload` selects the entry by.
    pub etag: String,
    pub size: u64,
    pub last_modified_ms: i64,
}

#[derive(Clone, Debug)]
pub struct BatchDeleteError {
    pub key: String,
    pub code: String,
    pub message: String,
}

#[derive(Clone)]
pub struct Backend {
    client: Client,
    region: String,
    bucket_prefix: String,
}

impl Backend {
    pub fn connect(cfg: &S3Endpoint, bucket_prefix: String) -> Self {
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
            bucket_prefix,
        }
    }

    pub fn with_prefix(&self, bucket_prefix: String) -> Self {
        Self {
            client: self.client.clone(),
            region: self.region.clone(),
            bucket_prefix,
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The backend's SigV4 signing region (a dummy for SeaweedFS); surfaced for `GetBucketLocation`.
    pub fn region(&self) -> &str {
        &self.region
    }

    fn backend_bucket(&self, bucket: &str) -> String {
        format!("{}{}", self.bucket_prefix, bucket)
    }

    /// `None` if the bucket isn't under this deployment's prefix — a sibling deployment's bucket on
    /// a shared account, not ours to list.
    fn client_bucket<'a>(&self, full: &'a str) -> Option<&'a str> {
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
            .bucket(self.backend_bucket(bucket))
            .key(key)
            .set_range(range)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    /// Read exactly the generation identified by `if_match`. Used when a live cache body becomes
    /// the source of a durable copy: the HEAD that selected the live branch and the GET that opens
    /// its stream must not silently resolve to different bodies.
    pub async fn get_if_match(
        &self,
        bucket: &str,
        key: &str,
        if_match: String,
    ) -> Result<GetObjectOutput> {
        self.get_range_if_match(bucket, key, None, if_match).await
    }

    pub async fn get_range_if_match(
        &self,
        bucket: &str,
        key: &str,
        range: Option<String>,
        if_match: String,
    ) -> Result<GetObjectOutput> {
        self.client
            .get_object()
            .bucket(self.backend_bucket(bucket))
            .key(key)
            .set_range(range)
            .if_match(if_match)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    pub async fn head(&self, bucket: &str, key: &str) -> Result<HeadObjectOutput> {
        self.client
            .head_object()
            .bucket(self.backend_bucket(bucket))
            .key(key)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    /// PUT a body already in its final on-remote form (ciphertext, for hypha's objects).
    /// `content_length` must be `Some` for a non-seekable `ByteStream` — S3 needs it up front.
    pub async fn put(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        options: PutOptions,
    ) -> Result<PutObjectOutput> {
        let mut request = self
            .client
            .put_object()
            .bucket(self.backend_bucket(bucket))
            .key(key)
            .body(body)
            .set_content_length(options.content_length)
            .set_metadata(Some(options.metadata))
            .set_content_type(options.content_type)
            // Client `Content-MD5` forwarded to the cache (cached-mode PUT) so the backend
            // validates the plaintext and returns `BadDigest` atomically — nothing lands on a bad
            // digest, and any prior body stays intact. `None` for hypha's own writes (ciphertext,
            // whose integrity is the trailer's job).
            .set_content_md5(options.content_md5)
            .set_if_match(options.if_match)
            .set_if_none_match(options.if_none_match);
        if let Some(checksum) = options.checksum {
            let algorithm = match checksum.algorithm {
                ChecksumAlgorithm::Crc32 => aws_sdk_s3::types::ChecksumAlgorithm::Crc32,
                ChecksumAlgorithm::Crc32c => aws_sdk_s3::types::ChecksumAlgorithm::Crc32C,
                ChecksumAlgorithm::Crc64Nvme => aws_sdk_s3::types::ChecksumAlgorithm::Crc64Nvme,
                ChecksumAlgorithm::Sha1 => aws_sdk_s3::types::ChecksumAlgorithm::Sha1,
                ChecksumAlgorithm::Sha256 => aws_sdk_s3::types::ChecksumAlgorithm::Sha256,
            };
            request = request.checksum_algorithm(algorithm.clone());
            request = match algorithm {
                aws_sdk_s3::types::ChecksumAlgorithm::Crc32 => {
                    request.set_checksum_crc32(checksum.value)
                }
                aws_sdk_s3::types::ChecksumAlgorithm::Crc32C => {
                    request.set_checksum_crc32_c(checksum.value)
                }
                aws_sdk_s3::types::ChecksumAlgorithm::Crc64Nvme => {
                    request.set_checksum_crc64_nvme(checksum.value)
                }
                aws_sdk_s3::types::ChecksumAlgorithm::Sha1 => {
                    request.set_checksum_sha1(checksum.value)
                }
                aws_sdk_s3::types::ChecksumAlgorithm::Sha256 => {
                    request.set_checksum_sha256(checksum.value)
                }
                _ => unreachable!("hypha only constructs supported checksum algorithms"),
            };
        }
        request.send().await.map_err(Error::from_sdk)
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
            .bucket(self.backend_bucket(bucket))
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

    /// Atomic plaintext cache copy, generation-bound to the source HEAD Hypha resolved. Metadata is
    /// always replaced because the cache carrier contains Hypha's namespaced projection as well as
    /// the client's values; forwarding the backend source metadata implicitly would bypass the
    /// `COPY`/`REPLACE` decision already made by the S3 handler.
    pub async fn copy(&self, request: CopyRequest<'_>) -> Result<CopyObjectOutput> {
        let copy_source = format!(
            "{}/{}",
            self.backend_bucket(request.source_bucket),
            encode_copy_source_key(request.source_key)
        );
        self.client
            .copy_object()
            .bucket(self.backend_bucket(request.destination_bucket))
            .key(request.destination_key)
            .copy_source(copy_source)
            .copy_source_if_match(request.source_if_match)
            .metadata_directive(MetadataDirective::Replace)
            .set_metadata(Some(request.metadata))
            .set_content_type(request.content_type)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    pub async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(self.backend_bucket(bucket))
            .key(key)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    /// Cache-side conditional DELETE — remove `key` only if its current ETag matches `if_match`
    /// (quoted). Marker completion and shadow reclamation depend on this CAS.
    pub async fn delete_if_match(&self, bucket: &str, key: &str, if_match: String) -> Result<()> {
        self.client
            .delete_object()
            .bucket(self.backend_bucket(bucket))
            .key(key)
            .if_match(if_match)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    /// Batch-delete up to 1000 keys in one round trip (S3 `DeleteObjects`), reporting the keys the
    /// remote refused. `quiet` suppresses only the per-key *success* entries — the failures come
    /// back either way — so the caller reads "absent from the returned list" as deleted.
    pub async fn delete_objects_reporting(
        &self,
        bucket: &str,
        keys: &[String],
    ) -> Result<Vec<BatchDeleteError>> {
        if keys.is_empty() {
            return Ok(Vec::new());
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
        let out = self
            .client
            .delete_objects()
            .bucket(self.backend_bucket(bucket))
            .delete(delete)
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(out
            .errors
            .unwrap_or_default()
            .into_iter()
            .map(|e| BatchDeleteError {
                key: e.key.unwrap_or_default(),
                code: e.code.unwrap_or_else(|| "InternalError".to_string()),
                message: e.message.unwrap_or_default(),
            })
            .collect())
    }

    /// Batch-delete where any per-key failure is the caller's failure: hypha's own sweeps (a key's
    /// twins, an upload's part records) have no per-key contract to honour, so a partial result is
    /// simply an error.
    pub async fn delete_objects(&self, bucket: &str, keys: &[String]) -> Result<()> {
        let failed = self.delete_objects_reporting(bucket, keys).await?;
        match failed.first() {
            None => Ok(()),
            Some(first) => Err(Error::Backend(format!(
                "batch delete: {} of {} keys failed, first {:?}: {} {}",
                failed.len(),
                keys.len(),
                first.key,
                first.code,
                first.message
            ))),
        }
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
        // `0x01`, and any control byte a client used — survive the LIST response . Keys come
        // back percent-encoded; decode them before returning so callers see raw bytes.
        let mut out = self
            .client
            .list_objects_v2()
            .bucket(self.backend_bucket(bucket))
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

    /// The v1 listing, kept native rather than emulated on top of [`Self::list`]: `NextMarker` is a
    /// *key position* under the backend's own delimiter/rollup rules, and reconstructing one from a
    /// v2 page would mean guessing where a common-prefix group ends. A v1 `marker` is a plain key, so
    /// it forwards to either backend safely — the same property the v2 path buys with its own
    /// key-anchored cursor instead of the backend's opaque continuation token.
    pub async fn list_v1(
        &self,
        bucket: &str,
        prefix: Option<String>,
        delimiter: Option<String>,
        marker: Option<String>,
        max_keys: Option<i32>,
    ) -> Result<ListObjectsOutput> {
        let mut out = self
            .client
            .list_objects()
            .bucket(self.backend_bucket(bucket))
            .set_prefix(prefix)
            .set_delimiter(delimiter)
            .set_marker(marker)
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
        // Unlike v2's opaque continuation token, `NextMarker` is a key and comes back encoded too.
        out.next_marker = out.next_marker.take().map(|m| url_decode(&m));
        Ok(out)
    }

    // ── Bucket ops ──────────────────────────────────────────────────────────────────────────

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.client
            .create_bucket()
            .bucket(self.backend_bucket(bucket))
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        self.client
            .delete_bucket()
            .bucket(self.backend_bucket(bucket))
            .send()
            .await
            .map_err(Error::from_sdk)?;
        Ok(())
    }

    pub async fn head_bucket(&self, bucket: &str) -> Result<()> {
        self.client
            .head_bucket()
            .bucket(self.backend_bucket(bucket))
            .send()
            .await
            // A missing bucket HEADs as a bodyless 404 (`NotFound`); the callers that branch on
            // bucket existence want `NoSuchBucket`, not the key-level `NotFound`.
            .map_err(|e| match Error::from_sdk(e) {
                Error::NotFound => Error::NoSuchBucket,
                e => e,
            })?;
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
                let name = self.client_bucket(b.name()?)?;
                Some((
                    name.to_string(),
                    b.creation_date().and_then(|d| d.to_millis().ok()),
                ))
            })
            .collect())
    }

    // ── Multipart-to-remote primitives: each part an independent age file  ───────────

    pub async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        metadata: HashMap<String, String>,
        content_type: Option<String>,
    ) -> Result<CreateMultipartUploadOutput> {
        self.client
            .create_multipart_upload()
            .bucket(self.backend_bucket(bucket))
            .key(key)
            .set_metadata(Some(metadata))
            .set_content_type(content_type)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    pub async fn upload_part(&self, request: UploadPartRequest<'_>) -> Result<UploadPartOutput> {
        self.client
            .upload_part()
            .bucket(self.backend_bucket(request.bucket))
            .key(request.key)
            .upload_id(request.upload_id)
            .part_number(request.part_number)
            .body(request.body)
            .set_content_length(request.content_length)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    /// Server-side `UploadPartCopy` : copy a byte range of a source object straight into a part
    /// of an in-progress native upload, remote→remote, no bytes through hypha. `src_range` is over
    /// the **source object's** bytes (`bytes=a-b`), used to exclude the source's tail trailer.
    ///
    /// The SDK sends `x-amz-copy-source` verbatim, so the key must arrive already URL-encoded; the
    /// bucket prefix is applied to the source bucket, as it is to every other backend bucket ref.
    pub async fn upload_part_copy(
        &self,
        request: UploadPartCopyRequest<'_>,
    ) -> Result<UploadPartCopyOutput> {
        let copy_source = format!(
            "{}/{}",
            self.backend_bucket(request.source_bucket),
            encode_copy_source_key(request.source_key)
        );
        self.client
            .upload_part_copy()
            .bucket(self.backend_bucket(request.destination.bucket))
            .key(request.destination.key)
            .upload_id(request.destination.upload_id)
            .part_number(request.destination.part_number)
            .copy_source(copy_source)
            .set_copy_source_range(request.source_range)
            .set_copy_source_if_match(request.source_if_match)
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
            .bucket(self.backend_bucket(bucket))
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(parts)
            .send()
            .await
            .map_err(Error::from_sdk)
    }

    /// Every part currently held by an in-progress native upload, as `(part_number, etag, size)` —
    /// the remote's own last-write-wins-resolved view. Complete uses it to pick the winning parts
    /// and their ciphertext sizes , so a re-uploaded part's stale hypha record never wins.
    /// Paginated; ETags are unquoted.
    pub async fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<RemotePart>> {
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let page = self
                .client
                .list_parts()
                .bucket(self.backend_bucket(bucket))
                .key(key)
                .upload_id(upload_id)
                .max_parts(1000)
                .set_part_number_marker(marker)
                .send()
                .await
                .map_err(Error::from_sdk)?;
            for p in page.parts() {
                if let (Some(n), Some(sz)) = (p.part_number(), p.size()) {
                    out.push(RemotePart {
                        number: n,
                        etag: p.e_tag().unwrap_or_default().trim_matches('"').to_string(),
                        size: sz.max(0) as u64,
                        last_modified_ms: p
                            .last_modified()
                            .and_then(|t| t.to_millis().ok())
                            .unwrap_or_default(),
                    });
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
    /// `ListMultipartUploads` proxies . hypha creates each native upload *at the client key*
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
            .bucket(self.backend_bucket(bucket))
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
            .bucket(self.backend_bucket(bucket))
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
///
/// **Form encoding, not path encoding**: the encoding S3 names here is
/// `x-www-form-urlencoded`, so a space arrives as `+` and a literal `+` as `%2B` — hence the
/// substitution before the percent decode, and hence that it is unambiguous. Percent-decoding alone
/// hands back a key that addresses nothing: it is reported to the client under a name it cannot GET,
/// and every internal operation keyed off the listing (the reconcile sweep above all) is aimed at an
/// object that does not exist.
fn url_decode(s: &str) -> String {
    percent_decode_str(&s.replace('+', " "))
        .decode_utf8_lossy()
        .into_owned()
}

/// RFC 3986 unreserved bytes; everything else is percent-encoded per path segment (control bytes a
/// client key may carry included), then segments rejoin on `/` so it stays a key path separator.
const KEY_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// URL-encode a source key for the `x-amz-copy-source` header (the SDK sends it verbatim).
fn encode_copy_source_key(key: &str) -> String {
    key.split('/')
        .map(|seg| utf8_percent_encode(seg, KEY_SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}
