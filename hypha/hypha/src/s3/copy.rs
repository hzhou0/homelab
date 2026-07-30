//! CopyObject (§7). The age body ciphertext is **key-independent** (per-file keys, §6) and reused
//! verbatim across keys; only the trailer, MAC-bound to the object key, is re-minted for the
//! destination. Durable copy is PUT's mark → commit → settle bracket with the body sourced from the
//! remote instead of the client:
//!
//! - **Large body** (source body ciphertext ≥ the 5 MiB part minimum): native multipart at `K_dst`
//!   — `UploadPartCopy` over `[0, body_ct_len)` (trailer excluded), then a fresh `K_dst`-bound
//!   trailer as the sole final part, then complete.
//! - **Small body**: a copy part can't stand alone as non-final and can't absorb the trailer, so
//!   re-encrypt — source GET → decrypt → one `PutObject` at `K_dst` with the trailer inline.
//!
//! Preconditions evaluate against the **source's** current client ETag / mtime
//! (`x-amz-copy-source-if-*`) only: s3s 0.14.1's `CopyObjectInput` predates the destination
//! `If-[None-]Match` fields (§2), so there is nothing to evaluate there yet.

use std::collections::HashMap;
use std::ops::Range;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::config::Mode;
use hypha_core::error::Error;
use hypha_core::meta;
use hypha_format::{encode_trailer, Footer};

use super::multipart::{parse_copy_source, MIN_REMOTE_PART};
use super::overlay::KeyState;
use super::put::evaluate_precondition;
use super::{copied_part_retag, resolve_storage_class, ts_ms, write_metadata, Hypha};
use crate::codec;
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

        if self.mode != Mode::Durable {
            // Cached-mode copy (cache→cache server-side, reconcile mints the trailer) lands in Phase 4.
            return Err(s3_error!(NotImplemented, "cached-mode CopyObject pending"));
        }

        let (src_bucket, src_key) = parse_copy_source(&input.copy_source)?;
        meta::validate_client_key(&src_key).map_err(|e| Error::Invalid(e.to_string()))?;

        let storage_class = resolve_storage_class(input.storage_class.as_ref())?;

        // Overlay (§7): the destination bucket must exist; a restoring one has K_dst materialized
        // from the remote first, so this copy's bracket then overwrites a correct tombstone.
        self.prepare_write(&bucket, &key).await?;

        // Resolve the source's facts + cache-side user metadata through the restore overlay, exactly
        // as a read would: a live cache body reports natively, anything else resolves remote-side.
        let (facts, src_md) = match self.resolve_key(&src_bucket, &src_key).await? {
            KeyState::Absent => return Err(s3_error!(NoSuchKey, "copy source does not exist")),
            KeyState::Remote { facts, md } => (facts, md),
            KeyState::CacheBody { head, md } => (RemoteFacts::from_cache_head(&head), md),
        };

        // Copy-source preconditions against the source's current state (§7). ETag conditions reuse
        // the PUT evaluator (the source exists, so its ETag is the `current`); the two time
        // conditions compare at the second granularity a client sees LastModified.
        evaluate_precondition(
            input.copy_source_if_match.as_ref(),
            input.copy_source_if_none_match.as_ref(),
            Some(&facts.cetag),
        )?;
        evaluate_copy_source_time(
            input.copy_source_if_modified_since.as_ref(),
            input.copy_source_if_unmodified_since.as_ref(),
            facts.mtime_ms,
        )?;

        let replace = input
            .metadata_directive
            .as_ref()
            .is_some_and(|d| d.as_str() == MetadataDirective::REPLACE);
        let dst_passthrough = if replace {
            write_metadata(input.metadata.as_ref(), &storage_class)
        } else {
            write_metadata(Some(&meta::decode_user_metadata(&src_md)), &storage_class)
        };

        // One bounded tail GET of the source's remote trailer (MAC-verified at K_src) fixes the
        // body/trailer boundary and, for a composite, the offset table — both body-relative, so they
        // carry over to K_dst unchanged. Foreign/unverifiable halts the deployment, as on any read
        // (§6, `crate::halt`).
        let Some(tail) = self.tier.read_tail(&src_bucket, &src_key).await? else {
            self.tier.halt.foreign_object(&src_bucket, &src_key).await
        };
        let body_ct_len = tail.body_ct_len;

        // Whole bracket under K_dst's write lock (§7). No destination precondition to resolve, and
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
        let commit = if body_ct_len >= MIN_REMOTE_PART {
            self.commit_copy_multipart(&bucket, &key, &src_bucket, &src_key, body_ct_len, &trailer)
                .await
        } else {
            self.commit_copy_reencrypt(&bucket, &key, &src_bucket, &src_key, &facts, trailer)
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

        // The destination only; the source fed the ring when it resolved (§8). A large copy commits
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

    /// Large-body commit (§7). Owns the native upload, so a failure aborts it best-effort — a
    /// leftover is a sweepable orphan regardless.
    async fn commit_copy_multipart(
        &self,
        bucket: &str,
        key: &str,
        src_bucket: &str,
        src_key: &str,
        body_ct_len: u64,
        trailer: &[u8],
    ) -> Result<(), Error> {
        let created = self
            .remote()
            .create_multipart(bucket, key, HashMap::new())
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
        // small-final-part fold multipart needs never arises here (§7).
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

    /// Small-body commit (§7): the source is one age file, so decrypt it whole and re-encrypt as one
    /// age file (age's framed length is fixed by the plaintext length, so body_ct_len — and thus the
    /// prebuilt trailer's table — is unchanged), with the fresh trailer appended inline in one PUT.
    async fn commit_copy_reencrypt(
        &self,
        bucket: &str,
        key: &str,
        src_bucket: &str,
        src_key: &str,
        facts: &RemoteFacts,
        trailer: Vec<u8>,
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
            )
            .await?;
        Ok(())
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

/// The two time-based copy-source conditions (§7), compared at the second granularity a client sees
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
