//! Representation-aware CopyObject.
//!
//! Live plaintext uses an atomic cache copy. Remote-resident ciphertext is key-independent, so
//! large bodies use multipart server-side copy with a new destination-bound trailer; small bodies
//! are re-encrypted because a non-final copy part cannot be below the multipart minimum.

use std::collections::HashMap;
use std::ops::Range;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;
use hypha_format::{encode_trailer, Footer};

use super::multipart::{parse_copy_source, MIN_REMOTE_PART};
use super::overlay::{KeyState, WriteMode};
use super::put::evaluate_precondition;
use super::{copied_part_retag, resolve_storage_class, ts_ms, write_metadata, Hypha};
use crate::codec::{self, SingleTrailer};
use crate::gc::Plaintext;
use crate::tier::{self, RemoteFacts};

/// Ciphertext bytes per server-side copy part: the backend's 5 GiB part cap. A composite body can
/// exceed one part, so `[0, body_ct_len)` is split into chunks this size, balanced so every copy
/// part clears the 5 MiB minimum — each copy part is non-final (the trailer is the sole final part),
/// so none may fall below it. Even a maximal object (10 000 × 4 GiB parts ≈ 40 TiB) needs well under
/// 9 000 copy parts, leaving room for the trailer part under S3's 10 000-part ceiling.
const COPY_PART_CT: u64 = 5 * 1024 * 1024 * 1024;

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
        let (facts, src_md, source_live) = self
            .resolve_copy_source(
                &src_bucket,
                &src_key,
                input.copy_source_if_match.as_ref(),
                input.copy_source_if_none_match.as_ref(),
                input.copy_source_if_modified_since.as_ref(),
                input.copy_source_if_unmodified_since.as_ref(),
            )
            .await?;

        let replace = input
            .metadata_directive
            .as_ref()
            .is_some_and(|d| d.as_str() == MetadataDirective::REPLACE);
        let dst_passthrough = if replace {
            write_metadata(
                input.metadata.as_ref(),
                &storage_class,
                input.content_type.as_deref(),
            )
        } else {
            write_metadata(
                Some(&meta::decode_user_metadata(&src_md)),
                &storage_class,
                meta::content_type(&src_md).as_deref(),
            )
        };

        // Representation, not deployment mode, selects the transport. A live source is a
        // single-part plaintext cache body, so a ready cached destination can use one atomic
        // cache-side copy and owe the normal PUT marker. Tombstoned sources — composites included —
        // are already remote-resident and take the durable ciphertext-copy path below.
        //
        // A restoring destination is the exception: its cache namespace is deliberately ignored,
        // so even a live source must commit remotely. Stream that source snapshot through the
        // single-part durable path rather than acknowledging a body no reader would see.
        if source_live {
            return match write_mode {
                WriteMode::Cached => {
                    // Admission gate  — see `op_put_object_cached`.
                    if !self.tier.pressure.admit() {
                        return Err(Error::SlowDown.into());
                    }
                    self.commit_cached_copy(
                        &bucket,
                        &key,
                        &src_bucket,
                        &src_key,
                        &facts,
                        dst_passthrough,
                    )
                    .await
                }
                WriteMode::Durable => {
                    self.commit_live_source_durable_copy(
                        &bucket,
                        &key,
                        &src_bucket,
                        &src_key,
                        &facts,
                        dst_passthrough,
                    )
                    .await
                }
            };
        }

        // One bounded tail GET of the source's remote trailer (MAC-verified at K_src) fixes the
        // body/trailer boundary and, for a composite, the offset table — both body-relative, so they
        // carry over to K_dst unchanged. Foreign/unverifiable halts the deployment, as on any read
        // (`crate::halt`).
        let Some(tail) = self.tier.read_tail(&src_bucket, &src_key).await? else {
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
            ..tail.footer.clone()
        };
        let table: Vec<u64> = tail.windows.iter().map(|w| w.end).collect();
        let trailer = encode_trailer(&self.tier.trailer_key, &key, body_ct_len, &footer, &table);

        self.tier.mark_transit_locked(&bucket, &key).await?;
        let dst_ct = meta::content_type(&dst_passthrough);
        let commit = if body_ct_len >= MIN_REMOTE_PART {
            self.commit_copy_multipart(
                &bucket,
                &key,
                &src_bucket,
                &src_key,
                body_ct_len,
                &trailer,
                dst_ct,
            )
            .await
        } else {
            self.commit_copy_reencrypt(
                &bucket,
                &key,
                &src_bucket,
                &src_key,
                &facts,
                trailer,
                dst_ct,
            )
            .await
        };
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
        let resp = CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(facts.cetag.clone())),
                last_modified: Some(ts_ms(mtime_ms)),
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// A live cached source copied into a ready cached destination. The backend copy is the commit,
    /// exactly like cached PUT's plaintext write; its native ETag names the marker obligation.
    ///
    /// `copy_source_if_match` is not the client's condition repeated. It binds the backend operation
    /// to the physical generation whose facts were evaluated above, closing HEAD → copy without
    /// making cached unconditional PUTs take the process write lock.
    #[allow(clippy::too_many_arguments)]
    async fn commit_cached_copy(
        &self,
        bucket: &str,
        key: &str,
        src_bucket: &str,
        src_key: &str,
        facts: &RemoteFacts,
        dst_passthrough: HashMap<String, String>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let content_type = meta::content_type(&dst_passthrough);
        let copied = match self
            .data()
            .copy(
                bucket,
                key,
                src_bucket,
                src_key,
                format!("\"{}\"", facts.cetag),
                dst_passthrough,
                content_type,
            )
            .await
        {
            Ok(out) => out,
            // These answers prove the destination did not commit. Everything else is indeterminate:
            // the cache may have copied K and lost its response before Hypha could queue the marker.
            Err(e @ (Error::PreconditionFailed | Error::NotFound | Error::NoSuchBucket)) => {
                return Err(e.into())
            }
            Err(e) => {
                self.buckets.unaccount(bucket);
                return Err(e.into());
            }
        };

        let result = match copied.copy_object_result() {
            Some(result) => result,
            None => {
                self.buckets.unaccount(bucket);
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
                self.buckets.unaccount(bucket);
                return Err(Error::Backend("cache copy returned no ETag".into()).into());
            }
        };
        if etag != facts.cetag {
            // A normal cache body is a single-part plaintext object, so its native ETag is stable
            // across copy. Do not publish inconsistent facts if a backend implements otherwise.
            self.buckets.unaccount(bucket);
            return Err(Error::Backend(format!(
                "cache copy changed plaintext ETag from {} to {etag}",
                facts.cetag
            ))
            .into());
        }
        let mtime_ms = result
            .last_modified()
            .and_then(|value| value.to_millis().ok())
            .unwrap_or_else(tier::now_ms);

        self.markers.owe(bucket, key, etag.clone());
        self.gc.touch(bucket, key, Plaintext::AtKey);
        self.orphans.owe(bucket, key);
        super::record_bytes(facts.plen);

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(etag)),
                last_modified: Some(ts_ms(mtime_ms)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    /// A live source copied into a destination that is currently running durable semantics (a
    /// restore window in a cached deployment). Open a generation-bound cache GET before marking the
    /// destination, then use the ordinary single-part remote commit. Live cache bodies are always
    /// single-part and capped below the remote PUT limit.
    #[allow(clippy::too_many_arguments)]
    async fn commit_live_source_durable_copy(
        &self,
        bucket: &str,
        key: &str,
        src_bucket: &str,
        src_key: &str,
        facts: &RemoteFacts,
        dst_passthrough: HashMap<String, String>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let source = self
            .data()
            .get_if_match(src_bucket, src_key, format!("\"{}\"", facts.cetag))
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

        let _guard = self.write_lock(bucket, key).await;
        let mtime_ms = tier::now_ms();
        self.tier.mark_transit_locked(bucket, key).await?;

        let trailer = SingleTrailer {
            trailer_key: self.tier.trailer_key.clone(),
            object_key: key.to_string(),
            mtime_ms,
        };
        let (framed_len, encrypted, etag_rx) = match codec::encrypt_blob_with_etag(
            self.env(),
            body,
            facts.plen,
            Some(trailer),
            Some(raw_md5),
        )
        .await
        {
            Ok(value) => value,
            Err(e) => {
                if let Err(repair) = self.tier.repair_locked(bucket, key).await {
                    tracing::warn!(key, error = %repair, "repair after failed live-source copy did not settle");
                }
                return Err(Error::Io(e).into());
            }
        };

        let content_type = meta::content_type(&dst_passthrough);
        if let Err(e) = self
            .remote()
            .put(
                bucket,
                key,
                encrypted,
                Some(framed_len as i64),
                HashMap::new(),
                None,
                None,
                None,
                content_type,
            )
            .await
        {
            if let Err(repair) = self.tier.repair_locked(bucket, key).await {
                tracing::warn!(key, error = %repair, "repair after failed live-source copy commit did not settle");
            }
            return Err(e.into());
        }

        let etag = match etag_rx.await {
            Ok(Ok(etag)) => etag,
            other => {
                if let Err(repair) = self.tier.repair_locked(bucket, key).await {
                    tracing::warn!(key, error = %repair, "repair after indeterminate live-source copy did not settle");
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
            .settle_evict_locked(bucket, key, facts.plen, &etag, mtime_ms, dst_passthrough)
            .await?;
        self.gc.touch(bucket, key, Plaintext::AtKey);
        self.orphans.owe(bucket, key);
        super::record_bytes(facts.plen);

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(etag)),
                last_modified: Some(ts_ms(mtime_ms)),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    /// Large-body commit . Owns the native upload, so a failure aborts it best-effort — a
    /// leftover is a sweepable orphan regardless.
    #[allow(clippy::too_many_arguments)]
    async fn commit_copy_multipart(
        &self,
        bucket: &str,
        key: &str,
        src_bucket: &str,
        src_key: &str,
        body_ct_len: u64,
        trailer: &[u8],
        content_type: Option<String>,
    ) -> Result<(), Error> {
        let created = self
            .remote()
            .create_multipart(bucket, key, HashMap::new(), content_type)
            .await?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| Error::Backend("remote returned no upload id".into()))?
            .to_string();

        let result = self
            .copy_body_parts(
                bucket,
                key,
                src_bucket,
                src_key,
                &upload_id,
                body_ct_len,
                trailer,
            )
            .await;
        if result.is_err() {
            if let Err(ae) = self.remote().abort_multipart(bucket, key, &upload_id).await {
                tracing::warn!(key = %key, error = %ae, "aborting the copy's dangling native upload failed; the sweep reclaims it");
            }
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn copy_body_parts(
        &self,
        bucket: &str,
        key: &str,
        src_bucket: &str,
        src_key: &str,
        upload_id: &str,
        body_ct_len: u64,
        trailer: &[u8],
    ) -> Result<(), Error> {
        let ranges = copy_part_ranges(body_ct_len);
        let mut parts: Vec<CompletedPart> = Vec::with_capacity(ranges.len() + 1);
        for (i, r) in ranges.iter().enumerate() {
            let pn = (i + 1) as i32;
            let out = self
                .remote()
                .upload_part_copy(
                    bucket,
                    key,
                    upload_id,
                    pn,
                    src_bucket,
                    src_key,
                    Some(format!("bytes={}-{}", r.start, r.end - 1)),
                )
                .await?;
            let retag = copied_part_retag(&out)?;
            parts.push(
                CompletedPart::builder()
                    .part_number(pn)
                    .e_tag(retag)
                    .build(),
            );
        }

        // The fresh trailer as the sole final part, always above every copied body part — so the
        // small-final-part fold multipart needs never arises here .
        let trailer_pn = ranges.len() as i32 + 1;
        let tout = self
            .remote()
            .upload_part(
                bucket,
                key,
                upload_id,
                trailer_pn,
                ByteStream::from(trailer.to_vec()),
                Some(trailer.len() as i64),
            )
            .await?;
        let tetag = tout
            .e_tag()
            .ok_or_else(|| Error::Backend("trailer part upload returned no ETag".into()))?;
        parts.push(
            CompletedPart::builder()
                .part_number(trailer_pn)
                .e_tag(tetag)
                .build(),
        );

        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.remote()
            .complete_multipart(bucket, key, upload_id, completed)
            .await?;
        Ok(())
    }

    /// Small-body commit : the source is one age file, so decrypt it whole and re-encrypt as one
    /// age file (age's framed length is fixed by the plaintext length, so body_ct_len — and thus the
    /// prebuilt trailer's table — is unchanged), with the fresh trailer appended inline in one PUT.
    #[allow(clippy::too_many_arguments)]
    async fn commit_copy_reencrypt(
        &self,
        bucket: &str,
        key: &str,
        src_bucket: &str,
        src_key: &str,
        facts: &RemoteFacts,
        trailer: Vec<u8>,
        content_type: Option<String>,
    ) -> Result<(), Error> {
        let plaintext = self
            .tier
            .decrypt_remote_body(src_bucket, src_key, &facts.cetag, None)
            .await?;
        let (body_len, enc, _etag_rx) =
            codec::encrypt_blob_with_etag(self.env(), plaintext, facts.plen, None, None)
                .await
                .map_err(Error::Io)?;
        let framed_len = body_len + trailer.len() as u64;
        let framed = codec::append_bytes(enc, trailer);
        self.remote()
            .put(
                bucket,
                key,
                framed,
                Some(framed_len as i64),
                HashMap::new(),
                None,
                None,
                None,
                content_type,
            )
            .await?;
        Ok(())
    }

    /// Resolve a copy source's facts, cache-side user metadata, and residency through the restore
    /// overlay — exactly as a read would — then evaluate its preconditions against that state. The
    /// shared head of CopyObject and UploadPartCopy : a live cache body reports natively,
    /// anything else resolves remote-side, and an absent source (including one mid-restore) is 404.
    pub(super) async fn resolve_copy_source(
        &self,
        src_bucket: &str,
        src_key: &str,
        if_match: Option<&ETagCondition>,
        if_none_match: Option<&ETagCondition>,
        if_modified_since: Option<&Timestamp>,
        if_unmodified_since: Option<&Timestamp>,
    ) -> S3Result<(RemoteFacts, HashMap<String, String>, bool)> {
        let (facts, md, live) = match self.resolve_key(src_bucket, src_key).await? {
            KeyState::Absent => return Err(s3_error!(NoSuchKey, "copy source does not exist")),
            KeyState::Remote { facts, md } => (facts, md, false),
            KeyState::CacheBody { head, md } => (RemoteFacts::from_cache_head(&head), md, true),
        };
        evaluate_precondition(if_match, if_none_match, Some(&facts.cetag))?;
        evaluate_copy_source_time(if_modified_since, if_unmodified_since, facts.mtime_ms)?;
        Ok((facts, md, live))
    }
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
