//! Representation-aware CopyObject.
//!
//! Live plaintext uses an atomic cache copy. Remote-resident ciphertext is key-independent, so
//! large bodies use multipart server-side copy with a new destination-bound trailer; small bodies
//! are re-encrypted because a non-final copy part cannot be below the multipart minimum.

use std::collections::HashMap;
use std::ops::Range;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use futures::StreamExt as _;
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::backend::{
    CopyRequest, PutOptions, UploadPartCopyRequest, UploadPartDestination, UploadPartRequest,
};
use hypha_core::error::Error;
use hypha_core::meta;
use hypha_format::{encode_trailer, ChecksumKind, Footer, StoredChecksum};

use super::multipart::{parse_copy_source, MIN_REMOTE_PART};
use super::overlay::{KeyState, WriteMode};
use super::put::evaluate_precondition;
use super::{checksum, copied_part_retag, resolve_storage_class, ts_ms, write_metadata, Hypha};
use crate::bucket::Readout;
use crate::codec::{self, SingleTrailer};
use crate::gc::Plaintext;
use crate::tier::{self, RemoteFacts};

/// Ciphertext bytes per server-side copy part. A framed body can exceed one part, so
/// `[0, body_ct_len)` is split into chunks this size, balanced so every copy part clears the 5 MiB
/// minimum — each is non-final (the trailer is the sole final part), so none may fall below it.
/// `MAX_COPY_PLAINTEXT` holds the split to two parts, so the 10 000-part ceiling never binds.
const COPY_PART_CT: u64 = super::REMOTE_UPLOAD_LIMIT;
/// S3's `CopyObject` ceiling; a larger source is the client's to copy part by part.
const MAX_COPY_PLAINTEXT: u64 = 5 * 1024 * 1024 * 1024;

pub(super) struct ResolvedCopySource {
    pub facts: RemoteFacts,
    pub md: HashMap<String, String>,
    pub live: bool,
    pub generation: String,
    _ticket: Readout,
}

pub(super) struct CopySourceConditions<'a> {
    pub if_match: Option<&'a ETagCondition>,
    pub if_none_match: Option<&'a ETagCondition>,
    pub if_modified_since: Option<&'a Timestamp>,
    pub if_unmodified_since: Option<&'a Timestamp>,
}

/// CopyObject and UploadPartCopy carry the same four preconditions under the same names, in
/// separately generated input types.
macro_rules! copy_source_conditions {
    ($($input:ty),+ $(,)?) => {$(
        impl<'a> From<&'a $input> for CopySourceConditions<'a> {
            fn from(input: &'a $input) -> Self {
                CopySourceConditions {
                    if_match: input.copy_source_if_match.as_ref(),
                    if_none_match: input.copy_source_if_none_match.as_ref(),
                    if_modified_since: input.copy_source_if_modified_since.as_ref(),
                    if_unmodified_since: input.copy_source_if_unmodified_since.as_ref(),
                }
            }
        }
    )+};
}

copy_source_conditions!(CopyObjectInput, UploadPartCopyInput);

#[derive(Clone, Copy)]
struct ObjectRef<'a> {
    bucket: &'a str,
    key: &'a str,
}

struct CopyCommit<'a> {
    source: ObjectRef<'a>,
    destination: ObjectRef<'a>,
    resolved: &'a ResolvedCopySource,
    checksum: Option<&'a StoredChecksum>,
    metadata: HashMap<String, String>,
}

struct RemoteCopyCommit<'a> {
    source: ObjectRef<'a>,
    destination: ObjectRef<'a>,
    source_generation: &'a str,
    source_etag: &'a str,
    plaintext_len: u64,
    body_ciphertext_len: u64,
    trailer: Vec<u8>,
    content_type: Option<String>,
}

struct CopyOutcome {
    etag: String,
    mtime_ms: i64,
}

impl Hypha {
    pub(super) async fn op_copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;

        let (src_bucket, src_key) = parse_copy_source(&input.copy_source)?;
        meta::validate_client_key(&src_key).map_err(|e| Error::Invalid(e.to_string()))?;

        let storage_class = resolve_storage_class(input.storage_class.as_ref())?;

        // Overlay : the destination bucket must exist; a restoring one has K_dst materialized
        // from the remote first, so this copy's bracket then overwrites a correct tombstone.
        let (_gate, write_mode) = self.prepare_write(&bucket, &key).await?;

        // Shared with UploadPartCopy .
        let source = self
            .resolve_copy_source(&src_bucket, &src_key, (&input).into())
            .await?;
        let facts = &source.facts;
        let src_md = &source.md;
        let source_live = source.live;
        if facts.plen > MAX_COPY_PLAINTEXT {
            return Err(s3_error!(
                EntityTooLarge,
                "CopyObject source exceeds the 5 GiB limit"
            ));
        }
        let requested_checksum = input
            .checksum_algorithm
            .as_ref()
            .map(checksum::parse_algorithm)
            .transpose()?;
        let destination_checksum = match requested_checksum {
            None => facts.checksum.clone(),
            Some(algorithm)
                if facts
                    .checksum
                    .as_ref()
                    .is_some_and(|value| value.algorithm == algorithm) =>
            {
                facts.checksum.clone()
            }
            Some(algorithm) => Some(
                self.hash_copy_source(
                    &source,
                    ObjectRef {
                        bucket: &src_bucket,
                        key: &src_key,
                    },
                    algorithm,
                )
                .await?,
            ),
        };

        let replace = input
            .metadata_directive
            .as_ref()
            .is_some_and(|d| d.as_str() == MetadataDirective::REPLACE);
        let mut dst_passthrough = if replace {
            write_metadata(
                input.metadata.as_ref(),
                &storage_class,
                input.content_type.as_deref(),
            )
        } else {
            write_metadata(
                Some(&meta::decode_user_metadata(src_md)),
                &storage_class,
                meta::content_type(src_md).as_deref(),
            )
        };
        if let Some(value) = &destination_checksum {
            dst_passthrough.insert(meta::CHECKSUM.to_string(), meta::encode_checksum(value));
        }

        // Representation, not deployment mode, selects the transport. A live source is a
        // single-part plaintext cache body, so a ready cached destination can use one atomic
        // cache-side copy and owe the normal PUT marker. Tombstoned sources — composites included —
        // are already remote-resident and take the durable ciphertext-copy path below.
        //
        // A restoring destination is the exception: its cache namespace is deliberately ignored,
        // so even a live source must commit remotely. Stream that source snapshot through the
        // single-part durable path rather than acknowledging a body no reader would see.
        if source_live {
            let commit = CopyCommit {
                source: ObjectRef {
                    bucket: &src_bucket,
                    key: &src_key,
                },
                destination: ObjectRef {
                    bucket: &bucket,
                    key: &key,
                },
                resolved: &source,
                checksum: destination_checksum.as_ref(),
                metadata: dst_passthrough,
            };
            let outcome = match write_mode {
                WriteMode::Cached => {
                    // Admission gate  — see `op_put_object_cached`.
                    if !self.tier.pressure.admit() {
                        return Err(Error::SlowDown.into());
                    }
                    self.commit_cached_copy(commit).await?
                }
                WriteMode::Durable => self.commit_live_source_durable_copy(commit).await?,
            };
            return Ok(copy_response(outcome, destination_checksum.as_ref()));
        }

        // One bounded tail GET of the source's remote trailer (MAC-verified at K_src) fixes the
        // body/trailer boundary and, for a composite, the offset table — both body-relative, so they
        // carry over to K_dst unchanged. Foreign/unverifiable halts the deployment, as on any read
        // (`crate::halt`).
        let Some(tail) = self
            .tier
            .read_tail_at(&src_bucket, &src_key, Some(&source.generation))
            .await?
        else {
            self.tier.halt.foreign_object(&src_bucket, &src_key).await
        };
        let body_ct_len = tail.body_ct_len;

        // Whole bracket under K_dst's write lock . No destination precondition to resolve, and
        // this copy's mark → commit → settle overwrites K_dst wholesale, so any leftover mark on it
        // is simply superseded — no separate repair needed.
        let _guard = self.write_lock(&bucket, &key).await;

        // The fresh trailer: the source footer with mtime re-minted, re-MAC'd over K_dst; the table
        // (empty for single-part) carries over. Built once — both commit paths preserve body_ct_len.
        let mtime_ms = tier::now_ms();
        let footer = Footer {
            mtime_ms,
            checksum: destination_checksum.clone(),
            ..tail.footer.clone()
        };
        let table: Vec<u64> = tail.windows.iter().map(|w| w.end).collect();
        let trailer = encode_trailer(&self.tier.trailer_key, &key, body_ct_len, &footer, &table);

        self.tier.mark_transit_locked(&bucket, &key).await?;
        let dst_ct = meta::content_type(&dst_passthrough);
        let commit = self
            .commit_remote_copy(RemoteCopyCommit {
                source: ObjectRef {
                    bucket: &src_bucket,
                    key: &src_key,
                },
                destination: ObjectRef {
                    bucket: &bucket,
                    key: &key,
                },
                source_generation: &source.generation,
                source_etag: &facts.cetag,
                plaintext_len: facts.plen,
                body_ciphertext_len: body_ct_len,
                trailer,
                content_type: dst_ct,
            })
            .await;
        if let Err(e) = commit {
            // Settle K_dst to whatever the remote actually holds — the same repair as a crashed PUT.
            if let Err(re) = self.tier.repair_locked(&bucket, &key).await {
                tracing::warn!(key = %key, error = %re, "repair after failed copy did not settle; leftover mark repaired on next access");
            }
            return Err(e.into());
        }

        self.tier
            .settle_evict_locked(
                &bucket,
                &key,
                facts.plen,
                &facts.cetag,
                mtime_ms,
                dst_passthrough,
            )
            .await?;

        // The destination only; the source fed the ring when it resolved . A large copy commits
        // through native multipart, so the destination can be either shape.
        self.gc.touch(&bucket, &key, Plaintext::of(&facts.cetag));
        self.orphans.owe(&bucket, &key);
        Ok(copy_response(
            CopyOutcome {
                etag: facts.cetag.clone(),
                mtime_ms,
            },
            destination_checksum.as_ref(),
        ))
    }

    /// A live cached source copied into a ready cached destination. The backend copy is the commit,
    /// exactly like cached PUT's plaintext write; its native ETag names the marker obligation.
    ///
    /// `copy_source_if_match` is not the client's condition repeated. It binds the backend operation
    /// to the physical generation whose facts were evaluated above, closing HEAD → copy without
    /// making cached unconditional PUTs take the process write lock.
    async fn commit_cached_copy(&self, commit: CopyCommit<'_>) -> S3Result<CopyOutcome> {
        let facts = &commit.resolved.facts;
        let content_type = meta::content_type(&commit.metadata);
        let copied = match self
            .data()
            .copy(CopyRequest {
                destination_bucket: commit.destination.bucket,
                destination_key: commit.destination.key,
                source_bucket: commit.source.bucket,
                source_key: commit.source.key,
                source_if_match: commit.resolved.generation.clone(),
                metadata: commit.metadata,
                content_type,
            })
            .await
        {
            Ok(out) => out,
            // These answers prove the destination did not commit. Everything else is indeterminate:
            // the cache may have copied K and lost its response before Hypha could queue the marker.
            Err(e @ (Error::PreconditionFailed | Error::NotFound | Error::NoSuchBucket)) => {
                return Err(e.into())
            }
            Err(e) => {
                self.buckets.unaccount(commit.destination.bucket);
                return Err(e.into());
            }
        };

        let result = match copied.copy_object_result() {
            Some(result) => result,
            None => {
                self.buckets.unaccount(commit.destination.bucket);
                return Err(Error::Backend("cache copy returned no result".into()).into());
            }
        };
        let etag = match result
            .e_tag()
            .map(|value| value.trim_matches('"').to_string())
        {
            Some(etag) if !etag.is_empty() => etag,
            _ => {
                // The copy committed but cannot be named by a marker. R2 must derive it next run.
                self.buckets.unaccount(commit.destination.bucket);
                return Err(Error::Backend("cache copy returned no ETag".into()).into());
            }
        };
        if etag != facts.cetag {
            // A normal cache body is a single-part plaintext object, so its native ETag is stable
            // across copy. Do not publish inconsistent facts if a backend implements otherwise.
            self.buckets.unaccount(commit.destination.bucket);
            return Err(Error::Backend(format!(
                "cache copy changed plaintext ETag from {} to {etag}",
                facts.cetag
            ))
            .into());
        }
        let mtime_ms = tier::now_ms();

        self.markers.owe(
            commit.destination.bucket,
            commit.destination.key,
            etag.clone(),
        );
        self.gc.touch(
            commit.destination.bucket,
            commit.destination.key,
            Plaintext::AtKey,
        );
        self.orphans
            .owe(commit.destination.bucket, commit.destination.key);
        super::record_bytes(facts.plen);
        Ok(CopyOutcome { etag, mtime_ms })
    }

    /// A live source copied into a destination that is currently running durable semantics (a
    /// restore window in a cached deployment). Open a generation-bound cache GET before marking the
    /// destination, then use the ordinary single-part remote commit. Live cache bodies are always
    /// single-part and capped below the remote PUT limit.
    async fn commit_live_source_durable_copy(
        &self,
        commit: CopyCommit<'_>,
    ) -> S3Result<CopyOutcome> {
        let facts = &commit.resolved.facts;
        let source = self
            .data()
            .get_if_match(
                commit.source.bucket,
                commit.source.key,
                commit.resolved.generation.clone(),
            )
            .await?;
        let source_len = source.content_length().unwrap_or(0).max(0) as u64;
        if source_len != facts.plen {
            return Err(Error::PreconditionFailed.into());
        }

        let raw_md5 = hex::decode(&facts.cetag)
            .ok()
            .and_then(|bytes| <[u8; 16]>::try_from(bytes.as_slice()).ok())
            .ok_or_else(|| {
                Error::Backend(format!(
                    "live cache body has non-single-part ETag {}",
                    facts.cetag
                ))
            })?;
        let body = codec::bytestream_to_blob(source.body);

        let _guard = self
            .write_lock(commit.destination.bucket, commit.destination.key)
            .await;
        let mtime_ms = tier::now_ms();
        self.tier
            .mark_transit_locked(commit.destination.bucket, commit.destination.key)
            .await?;

        let trailer = SingleTrailer {
            trailer_key: self.tier.trailer_key.clone(),
            object_key: commit.destination.key.to_string(),
            mtime_ms,
            checksum: commit.checksum.cloned(),
        };
        let (framed_len, encrypted, etag_rx) = match codec::encrypt_blob_with_etag(
            self.env(),
            body,
            facts.plen,
            codec::EncryptOptions {
                trailer: Some(trailer),
                expected_md5: Some(raw_md5),
                ..Default::default()
            },
        )
        .await
        {
            Ok(value) => value,
            Err(e) => {
                if let Err(repair) = self
                    .tier
                    .repair_locked(commit.destination.bucket, commit.destination.key)
                    .await
                {
                    tracing::warn!(key = commit.destination.key, error = %repair, "repair after failed live-source copy did not settle");
                }
                return Err(Error::Io(e).into());
            }
        };

        let content_type = meta::content_type(&commit.metadata);
        if let Err(e) = self
            .remote()
            .put(
                commit.destination.bucket,
                commit.destination.key,
                encrypted,
                PutOptions {
                    content_length: Some(framed_len as i64),
                    content_type,
                    ..Default::default()
                },
            )
            .await
        {
            if let Err(repair) = self
                .tier
                .repair_locked(commit.destination.bucket, commit.destination.key)
                .await
            {
                tracing::warn!(key = commit.destination.key, error = %repair, "repair after failed live-source copy commit did not settle");
            }
            return Err(e.into());
        }

        let etag = match etag_rx.await {
            Ok(Ok(digests)) => digests.etag,
            other => {
                if let Err(repair) = self
                    .tier
                    .repair_locked(commit.destination.bucket, commit.destination.key)
                    .await
                {
                    tracing::warn!(key = commit.destination.key, error = %repair, "repair after indeterminate live-source copy did not settle");
                }
                return Err(match other {
                    Ok(Err(_)) => {
                        Error::Backend("cache source changed while it was copied".into()).into()
                    }
                    Err(_) => {
                        Error::Backend("copy MD5 task dropped before completing".into()).into()
                    }
                    Ok(Ok(_)) => unreachable!("successful digest handled by the outer match"),
                });
            }
        };

        self.tier
            .settle_evict_locked(
                commit.destination.bucket,
                commit.destination.key,
                facts.plen,
                &etag,
                mtime_ms,
                commit.metadata,
            )
            .await?;
        self.gc.touch(
            commit.destination.bucket,
            commit.destination.key,
            Plaintext::AtKey,
        );
        self.orphans
            .owe(commit.destination.bucket, commit.destination.key);
        super::record_bytes(facts.plen);
        Ok(CopyOutcome { etag, mtime_ms })
    }

    async fn commit_remote_copy(&self, commit: RemoteCopyCommit<'_>) -> Result<(), Error> {
        if commit.body_ciphertext_len < MIN_REMOTE_PART {
            let plaintext = self
                .tier
                .decrypt_remote_body_at(
                    commit.source.bucket,
                    commit.source.key,
                    commit.source_etag,
                    None,
                    commit.source_generation,
                )
                .await?;
            let (body_len, encrypted, _digests) = codec::encrypt_blob_with_etag(
                self.env(),
                plaintext,
                commit.plaintext_len,
                codec::EncryptOptions::default(),
            )
            .await
            .map_err(Error::Io)?;
            let framed_len = body_len + commit.trailer.len() as u64;
            return self
                .remote()
                .put(
                    commit.destination.bucket,
                    commit.destination.key,
                    codec::append_bytes(encrypted, commit.trailer),
                    PutOptions {
                        content_length: Some(framed_len as i64),
                        content_type: commit.content_type,
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ());
        }

        // The native upload writes no client-addressable record, so this shared create lock is its
        // only shield from the orphan sweep until completion or the best-effort abort below.
        let _create_guard = self
            .tier
            .mpu_create_locks
            .read(commit.destination.bucket, commit.destination.key)
            .await;
        let created = self
            .remote()
            .create_multipart(
                commit.destination.bucket,
                commit.destination.key,
                HashMap::new(),
                commit.content_type,
            )
            .await?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| Error::Backend("remote returned no upload id".into()))?
            .to_string();

        let result = async {
            let ranges = copy_part_ranges(commit.body_ciphertext_len);
            let mut parts = Vec::with_capacity(ranges.len() + 1);
            for (index, range) in ranges.iter().enumerate() {
                let part_number = index as i32 + 1;
                let copied = self
                    .remote()
                    .upload_part_copy(UploadPartCopyRequest {
                        destination: UploadPartDestination {
                            bucket: commit.destination.bucket,
                            key: commit.destination.key,
                            upload_id: &upload_id,
                            part_number,
                        },
                        source_bucket: commit.source.bucket,
                        source_key: commit.source.key,
                        source_range: Some(format!("bytes={}-{}", range.start, range.end - 1)),
                        source_if_match: Some(commit.source_generation.to_string()),
                    })
                    .await?;
                parts.push(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(copied_part_retag(&copied)?)
                        .build(),
                );
            }

            let trailer_part = ranges.len() as i32 + 1;
            let trailer_len = commit.trailer.len() as i64;
            let uploaded = self
                .remote()
                .upload_part(UploadPartRequest {
                    bucket: commit.destination.bucket,
                    key: commit.destination.key,
                    upload_id: &upload_id,
                    part_number: trailer_part,
                    body: ByteStream::from(commit.trailer),
                    content_length: Some(trailer_len),
                })
                .await?;
            let trailer_etag = uploaded
                .e_tag()
                .ok_or_else(|| Error::Backend("trailer part upload returned no ETag".into()))?;
            parts.push(
                CompletedPart::builder()
                    .part_number(trailer_part)
                    .e_tag(trailer_etag)
                    .build(),
            );
            self.remote()
                .complete_multipart(
                    commit.destination.bucket,
                    commit.destination.key,
                    &upload_id,
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            if let Err(error) = self
                .remote()
                .abort_multipart(
                    commit.destination.bucket,
                    commit.destination.key,
                    &upload_id,
                )
                .await
            {
                tracing::warn!(key = commit.destination.key, %error, "aborting the copy's dangling native upload failed; the sweep reclaims it");
            }
        }
        result
    }

    /// Resolve a copy source's facts, cache-side user metadata, and residency through the restore
    /// overlay — exactly as a read would — then evaluate its preconditions against that state. The
    /// shared head of CopyObject and UploadPartCopy : a live cache body reports natively,
    /// anything else resolves remote-side, and an absent source (including one mid-restore) is 404.
    pub(super) async fn resolve_copy_source(
        &self,
        src_bucket: &str,
        src_key: &str,
        conditions: CopySourceConditions<'_>,
    ) -> S3Result<ResolvedCopySource> {
        let (state, ticket) = self.resolve_key_with_ticket(src_bucket, src_key).await?;
        let (facts, md, live, generation) = match state {
            KeyState::Absent => return Err(s3_error!(NoSuchKey, "copy source does not exist")),
            KeyState::Remote { facts, md } => {
                let head = self.remote().head(src_bucket, src_key).await?;
                let generation = head
                    .e_tag()
                    .ok_or_else(|| Error::Backend("copy source has no physical ETag".into()))?
                    .to_string();
                let Some(tail) = self
                    .tier
                    .read_tail_at(src_bucket, src_key, Some(&generation))
                    .await?
                else {
                    self.tier.halt.foreign_object(src_bucket, src_key).await
                };
                if tail.footer.plen != facts.plen
                    || tail.footer.client_etag() != facts.cetag
                    || tail.footer.mtime_ms != facts.mtime_ms
                    || tail.footer.checksum != facts.checksum
                {
                    return Err(Error::OperationAborted.into());
                }
                (facts, md, false, generation)
            }
            KeyState::CacheBody { head, md } => {
                let facts = RemoteFacts::from_cache_head(&head);
                let generation = format!("\"{}\"", facts.cetag);
                (facts, md, true, generation)
            }
        };
        evaluate_precondition(
            conditions.if_match,
            conditions.if_none_match,
            Some(&facts.cetag),
        )?;
        evaluate_copy_source_time(
            conditions.if_modified_since,
            conditions.if_unmodified_since,
            facts.mtime_ms,
        )?;
        Ok(ResolvedCopySource {
            facts,
            md,
            live,
            generation,
            _ticket: ticket,
        })
    }

    async fn hash_copy_source(
        &self,
        source: &ResolvedCopySource,
        location: ObjectRef<'_>,
        algorithm: hypha_format::ChecksumAlgorithm,
    ) -> S3Result<StoredChecksum> {
        let mut body = if source.live {
            let out = self
                .data()
                .get_range_if_match(
                    location.bucket,
                    location.key,
                    None,
                    source.generation.clone(),
                )
                .await?;
            codec::bytestream_to_blob(out.body)
        } else {
            self.tier
                .decrypt_remote_body_at(
                    location.bucket,
                    location.key,
                    &source.facts.cetag,
                    None,
                    &source.generation,
                )
                .await?
        };
        let mut hasher = checksum::Hasher::new(algorithm);
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::Backend(format!("reading copy source: {e}")))?;
            hasher.update(&chunk);
        }
        Ok(hasher.finalize(ChecksumKind::FullObject))
    }
}

fn copy_response(
    outcome: CopyOutcome,
    checksum_value: Option<&StoredChecksum>,
) -> S3Response<CopyObjectOutput> {
    let mut result = CopyObjectResult {
        e_tag: Some(ETag::Strong(outcome.etag.clone())),
        last_modified: Some(ts_ms(outcome.mtime_ms)),
        ..Default::default()
    };
    if let Some(value) = checksum_value {
        let value = checksum::dto(value, super::get::checksum_count(&outcome.etag));
        result.checksum_crc32 = value.checksum_crc32;
        result.checksum_crc32c = value.checksum_crc32c;
        result.checksum_crc64nvme = value.checksum_crc64nvme;
        result.checksum_sha1 = value.checksum_sha1;
        result.checksum_sha256 = value.checksum_sha256;
        result.checksum_type = value.checksum_type;
    }
    S3Response::new(CopyObjectOutput {
        copy_object_result: Some(result),
        ..Default::default()
    })
}

/// Split `[0, total)` into server-side copy-part ranges, each in `[5 MiB, COPY_PART_CT]`. The caller
/// gates on `total ≥ MIN_REMOTE_PART`, so a single-range result already clears the minimum; when the
/// body needs several parts they are balanced (`total/n` each, `± 1`), which for `n ≥ 2` and
/// `total > COPY_PART_CT` puts every part above `COPY_PART_CT/2` — far past the 5 MiB floor every
/// non-final part must clear.
fn copy_part_ranges(total: u64) -> Vec<Range<u64>> {
    if total <= COPY_PART_CT {
        // A single copy part covering the whole body; the caller's `total ≥ 5 MiB` gate keeps it
        // above the part minimum. (`single_range_in_vec_init` misreads this as a `vec![n; len]` slip.)
        #[allow(clippy::single_range_in_vec_init)]
        return vec![0..total];
    }
    let n = total.div_ceil(COPY_PART_CT);
    let base = total / n;
    let rem = total % n;
    let mut ranges = Vec::with_capacity(n as usize);
    let mut start = 0u64;
    for i in 0..n {
        let len = base + if i < rem { 1 } else { 0 };
        ranges.push(start..start + len);
        start += len;
    }
    ranges
}

/// The two time-based copy-source conditions , compared at the second granularity a client sees
/// `LastModified` at. `if_modified_since` fails when the source has *not* changed since; `if_unmodified_since`
/// fails when it *has* — both surface as `412 PreconditionFailed`.
pub(super) fn evaluate_copy_source_time(
    if_modified_since: Option<&Timestamp>,
    if_unmodified_since: Option<&Timestamp>,
    src_mtime_ms: i64,
) -> Result<(), Error> {
    // Truncate to whole seconds so a sub-second mtime doesn't read as "modified" against a
    // second-granular HTTP-date condition.
    let src = ts_ms((src_mtime_ms / 1000) * 1000);
    if let Some(since) = if_modified_since {
        if src <= *since {
            return Err(Error::PreconditionFailed);
        }
    }
    if let Some(since) = if_unmodified_since {
        if src > *since {
            return Err(Error::PreconditionFailed);
        }
    }
    Ok(())
}
