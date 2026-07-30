//! The `s3s::S3` trait implementation, split by op group (§3). Each submodule adds an
//! `impl Hypha` block; this module owns the struct and the trait wiring that dispatches to them.
//!
//! Phase 2 is the **durable** surface: writes go through the cache but ack only after the remote
//! is durable. Phase 4 adds cached mode (ack after cache write, async background upload).

mod buckets;
mod copy;
mod delete;
mod get;
mod list_head;
mod multipart;
mod overlay;
mod put;

use std::collections::HashMap;
use std::sync::Arc;

use hypha_format::Envelope;
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::config::Mode;
use hypha_core::meta;
use hypha_core::Backend;

use crate::background::Background;
use crate::bucket::BucketCtl;
use crate::gc::orphans::Orphans;
use crate::gc::Gc;
use crate::keylocks::KeyGuard;
use crate::markers::Markers;
use crate::tier::Tiering;

#[derive(Clone)]
pub struct Hypha {
    /// Shared tiering machinery: cache + remote backends, the age envelope, and the per-key lock
    /// table. Every data-path op reaches the backends through here.
    pub tier: Tiering,
    /// The bucket-control actor — sole writer of the cache substrate (§7 *Buckets*). Bucket
    /// lifecycle and repair route here; object reads/writes never do, beyond the 503 repair kick.
    pub(crate) buckets: BucketCtl,
    /// The background-transition actor (§8) — rehydrate. Client writes reach it only through
    /// [`Hypha::write_lock`], which cancels K's transition before queuing.
    pub(crate) background: Background,
    /// Pending-marker obligations (§7). A cached write acks on its body write and raises the marker
    /// here; the ack never depends on the marker landing.
    pub(crate) markers: Markers,
    /// The GC actor (§8). Every op that resolves or lands a single key touches it — reads *and*
    /// writes, since a write is the strongest statement of interest a key gets. LIST and DELETE
    /// deliberately do not: a full listing would mark the whole keyspace hot, and a delete leaves no
    /// body to protect.
    pub(crate) gc: Gc,
    /// Shadow-orphan obligations (§8). Every cached write that could have superseded a composite hands
    /// its key over; the actor is what decides whether there was a shadow to reclaim.
    pub(crate) orphans: Orphans,
    pub mode: Mode,
    /// Longest configured bucket prefix, charged against S3's 63-byte cap so the client-visible
    /// bucket-name limit is `63 − this` (§7 *Buckets*). Checked at CreateBucket.
    pub max_bucket_prefix_len: usize,
    /// Contiguous encrypt/decrypt above this offloads to `spawn_blocking` (§5). Unwired until
    /// an inline (non-offloaded) codec path exists — today every codec bridge offloads.
    #[allow(dead_code)]
    pub offload_threshold: usize,
}

impl Hypha {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tier: Tiering,
        buckets: BucketCtl,
        markers: Markers,
        gc: Gc,
        orphans: Orphans,
        background: Background,
        mode: Mode,
        offload_threshold: usize,
        max_bucket_prefix_len: usize,
    ) -> Self {
        Self {
            tier,
            buckets,
            background,
            markers,
            gc,
            orphans,
            mode,
            max_bucket_prefix_len,
            offload_threshold,
        }
    }

    /// Take K's **write** lock for a client write (§4), first telling any background transition on K
    /// to stop (§8). Every client write-lock acquisition goes through here rather than
    /// `tier.locks.lock` directly: a rehydrate holds the lock across a whole-object fetch, so
    /// without the cancel a conditional PUT, DELETE, or CompleteMultipartUpload on a hot key would
    /// park behind a multi-minute transfer. The cancel is a map lookup and needs no reply — see
    /// [`background`] for why the lock handoff is a sufficient rendezvous.
    pub(crate) async fn write_lock(&self, bucket: &str, key: &str) -> KeyGuard {
        self.background.cancel(bucket, key);
        self.tier.locks.lock(key).await
    }

    pub(crate) fn data(&self) -> &Backend {
        &self.tier.data
    }
    pub(crate) fn meta(&self) -> &Backend {
        &self.tier.meta
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

/// The ETag a server-side `UploadPartCopy` returned, unquoted. Required — an absent one could
/// never match this part at complete (§6).
pub(crate) fn copied_part_retag(
    out: &aws_sdk_s3::operation::upload_part_copy::UploadPartCopyOutput,
) -> Result<String, hypha_core::error::Error> {
    Ok(out
        .copy_part_result()
        .and_then(|r| r.e_tag())
        .ok_or_else(|| hypha_core::error::Error::Backend("part copy returned no ETag".into()))?
        .trim_matches('"')
        .to_string())
}

/// The trait surface, as one table: every method is the same shape — open the request's span,
/// delegate to the op module, report the call (§10) — so writing them out longhand would put
/// twenty-two copies of that shape between a reader and the one line that differs.
///
/// The trailing bracket names which of the span's request-side fields this op *has*, which is why
/// they are declared here rather than recorded by each handler: a new op cannot be added without an
/// entry, and an entry cannot be written without answering the question. `bytes` and `cache_hit`
/// are not knowable from the request, so they are left empty for the handler to fill in
/// ([`record_bytes`], [`record_cache_hit`]).
///
/// The latency recorded for `GetObject` — and the moment its span closes — is the *response*, not
/// the last byte: the body is a stream the handler returns before it is read. That is the number
/// worth alerting on anyway; everything hypha decides has happened by then, and what follows is the
/// client's bandwidth.
macro_rules! client_ops {
    ($($op:literal $method:ident($input:ident) -> $output:ident => $handler:ident [$($field:ident)*];)*) => {
        #[async_trait::async_trait]
        impl s3s::S3 for Hypha {
            $(
                async fn $method(&self, req: S3Request<$input>) -> S3Result<S3Response<$output>> {
                    let span = tracing::info_span!(
                        "s3",
                        op = $op,
                        bucket = tracing::field::Empty,
                        key = tracing::field::Empty,
                        bytes = tracing::field::Empty,
                        cache_hit = tracing::field::Empty,
                    );
                    $( span.record(stringify!($field), req.input.$field.as_str()); )*
                    let started = std::time::Instant::now();
                    let out = tracing::Instrument::instrument(self.$handler(req), span).await;
                    crate::metrics::s3_request($op, out.is_err(), started.elapsed());
                    out
                }
            )*
        }
    };
}

/// The payload this request moved, once the handler knows it (§10). For a read that is what the
/// response declares rather than what the client eventually pulls, since the span closes on the
/// response.
pub(crate) fn record_bytes(bytes: u64) {
    tracing::Span::current().record("bytes", bytes);
}

/// Whether the cache held the plaintext this read resolved to. Recorded where the read *becomes* a
/// hit or a miss, which is deeper than any single branch of the dispatch.
pub(crate) fn record_cache_hit(hit: bool) {
    tracing::Span::current().record("cache_hit", hit);
}

client_ops! {
    "AbortMultipartUpload" abort_multipart_upload(AbortMultipartUploadInput) -> AbortMultipartUploadOutput => op_abort_multipart_upload [bucket key];
    "CompleteMultipartUpload" complete_multipart_upload(CompleteMultipartUploadInput) -> CompleteMultipartUploadOutput => op_complete_multipart_upload [bucket key];
    "CopyObject" copy_object(CopyObjectInput) -> CopyObjectOutput => op_copy_object [bucket key];
    "CreateBucket" create_bucket(CreateBucketInput) -> CreateBucketOutput => op_create_bucket [bucket];
    "CreateMultipartUpload" create_multipart_upload(CreateMultipartUploadInput) -> CreateMultipartUploadOutput => op_create_multipart_upload [bucket key];
    "DeleteBucket" delete_bucket(DeleteBucketInput) -> DeleteBucketOutput => op_delete_bucket [bucket];
    "DeleteObject" delete_object(DeleteObjectInput) -> DeleteObjectOutput => op_delete_object [bucket key];
    "DeleteObjects" delete_objects(DeleteObjectsInput) -> DeleteObjectsOutput => op_delete_objects [bucket];
    "GetBucketLocation" get_bucket_location(GetBucketLocationInput) -> GetBucketLocationOutput => op_get_bucket_location [bucket];
    "GetBucketVersioning" get_bucket_versioning(GetBucketVersioningInput) -> GetBucketVersioningOutput => op_get_bucket_versioning [bucket];
    "GetObject" get_object(GetObjectInput) -> GetObjectOutput => op_get_object [bucket key];
    "GetObjectAttributes" get_object_attributes(GetObjectAttributesInput) -> GetObjectAttributesOutput => op_get_object_attributes [bucket key];
    "HeadBucket" head_bucket(HeadBucketInput) -> HeadBucketOutput => op_head_bucket [bucket];
    "HeadObject" head_object(HeadObjectInput) -> HeadObjectOutput => op_head_object [bucket key];
    "ListBuckets" list_buckets(ListBucketsInput) -> ListBucketsOutput => op_list_buckets [];
    "ListMultipartUploads" list_multipart_uploads(ListMultipartUploadsInput) -> ListMultipartUploadsOutput => op_list_multipart_uploads [bucket];
    "ListObjects" list_objects(ListObjectsInput) -> ListObjectsOutput => op_list_objects [bucket];
    "ListObjectsV2" list_objects_v2(ListObjectsV2Input) -> ListObjectsV2Output => op_list_objects_v2 [bucket];
    "ListParts" list_parts(ListPartsInput) -> ListPartsOutput => op_list_parts [bucket key];
    "PutObject" put_object(PutObjectInput) -> PutObjectOutput => op_put_object [bucket key];
    "UploadPart" upload_part(UploadPartInput) -> UploadPartOutput => op_upload_part [bucket key];
    "UploadPartCopy" upload_part_copy(UploadPartCopyInput) -> UploadPartCopyOutput => op_upload_part_copy [bucket key];
}
