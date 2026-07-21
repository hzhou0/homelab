//! Multipart (§7) — one path, both modes. Parts route **around the cache** onto the remote's own
//! native multipart upload at K: each part is an independent, pure age file (fresh file key ⇒
//! parallel and re-uploaded parts need no coordination), and the remote concatenates them at
//! complete. The object's facts **and its parts table** travel as its one terminating trailer
//! part (part number above every client part — hence the 9999 client cap), uploaded just before
//! the native complete so the commit lands body and facts in one atomic op. The table is the
//! per-part cumulative ciphertext end-offsets, so reads recover every part boundary from the
//! trailer alone — no remote part-index calls (§6).
//!
//! hypha's own state per upload is minimal and cache-resident under the reserved prefix:
//! per-part `{pmd5, plen, retag}` facts, needed at complete because an in-progress upload's
//! parts aren't readable. Its loss with the cache volume merely fails the eventual complete —
//! never-acked, the client retries.

use std::collections::HashMap;

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;
use hypha_format::offset::{plaintext_len_from, HLEN};
use hypha_format::{encode_trailer, Footer, FooterKind};

use super::{Hypha, MAX_INLINE_PLAINTEXT};
use crate::codec;
use crate::tier;

impl Hypha {
    pub(super) async fn op_create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;

        let created = self
            .remote()
            .create_multipart(&bucket, &key, HashMap::new())
            .await?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| Error::Backend("remote returned no upload id".into()))?
            .to_string();

        // The upload's own record: client key as the body (keys may carry bytes an ASCII
        // metadata header can't).
        self.cache()
            .put_small(
                &bucket,
                &meta::mpu_upload_key(&upload_id),
                key.clone().into_bytes(),
                HashMap::new(),
                None,
                None,
            )
            .await?;

        let resp = CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    pub(super) async fn op_upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;
        let part_number = input.part_number;
        // 9999, not S3's 10000: the number above every client part is reserved for the
        // terminating footer part the complete appends (§7).
        if !(1..=9_999).contains(&part_number) {
            return Err(s3_error!(
                InvalidPart,
                "part number must be between 1 and 9999 (10000 is reserved by hypha)"
            ));
        }
        let plen = input
            .content_length
            .filter(|&n| n >= 0)
            .ok_or_else(|| Error::Invalid("UploadPart requires Content-Length".into()))?
            as u64;
        if plen > MAX_INLINE_PLAINTEXT {
            return Err(s3_error!(
                EntityTooLarge,
                "parts are capped at {MAX_INLINE_PLAINTEXT} bytes"
            ));
        }
        let body = input
            .body
            .ok_or_else(|| Error::Invalid("UploadPart requires a body".into()))?;

        // Fail fast if the upload is unknown to us — the eventual complete needs these records.
        match self
            .cache()
            .head(&bucket, &meta::mpu_upload_key(&input.upload_id))
            .await
        {
            Ok(_) => {}
            Err(Error::NotFound) => return Err(s3_error!(NoSuchUpload, "unknown upload id")),
            Err(e) => return Err(e.into()),
        }

        // Encrypt the part as its own pure age file, computing its plaintext MD5 inline in the
        // same pass (§7); the ciphertext streams to the remote as the native part.
        let (ct_len, enc, etag_rx) = codec::encrypt_blob_with_etag(self.env(), body, plen, None)
            .await
            .map_err(Error::Io)?;
        let out = self
            .remote()
            .upload_part(
                &bucket,
                &key,
                &input.upload_id,
                part_number,
                enc,
                Some(ct_len as i64),
            )
            .await?;
        // The remote accepted the part, so it must echo the ETag that identifies it — an empty
        // `retag` would silently fail to match this part at complete (§6).
        let retag = out
            .e_tag()
            .ok_or_else(|| Error::Backend("part upload returned no ETag".into()))?
            .trim_matches('"')
            .to_string();
        let pmd5 = etag_rx
            .await
            .map_err(|_| Error::Backend("MD5 task dropped before completing".into()))?;

        // Persist the part's facts in the record KEY (§6): `pmd5` (the plaintext MD5, unknowable to
        // the remote) plus `retag` (its last-write-wins token). A re-upload writes a new key; the
        // stale one is resolved away at complete by the remote's `ListParts`. Survives process
        // restarts across a multi-hour upload; `plen` isn't stored — it's `plaintext_len_from` the
        // remote's part size at complete.
        self.cache()
            .put_small(
                &bucket,
                &meta::mpu_part_key(&input.upload_id, part_number, &retag, &pmd5),
                Vec::new(),
                HashMap::new(),
                None,
                None,
            )
            .await?;

        let resp = UploadPartOutput {
            e_tag: Some(ETag::Strong(pmd5)),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    pub(super) async fn op_complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;
        let upload_id = input.upload_id.clone();

        let requested = input
            .multipart_upload
            .and_then(|m| m.parts)
            .unwrap_or_default();
        if requested.is_empty() {
            return Err(s3_error!(
                InvalidRequest,
                "complete requires at least one part"
            ));
        }
        if !requested
            .windows(2)
            .all(|w| w[0].part_number < w[1].part_number)
        {
            return Err(s3_error!(
                InvalidPartOrder,
                "parts must be listed in ascending part-number order"
            ));
        }

        // The whole bracket runs under K's write lock (§7).
        let _guard = self.tier.locks.lock(&key).await;

        match self
            .cache()
            .head(&bucket, &meta::mpu_upload_key(&upload_id))
            .await
        {
            Ok(_) => {}
            Err(Error::NotFound) => return Err(s3_error!(NoSuchUpload, "unknown upload id")),
            Err(e) => return Err(e.into()),
        }

        // 1. Recover per-part facts and geometry, then compose the client ETag, total plaintext
        //    length, and offset table. Two reads, no per-part HEAD (§6/§7):
        //    · one LIST of the upload's records → `(part, retag) → pmd5` (facts live in the keys);
        //    · one `ListParts` of the remote upload → the winning `(part → retag, size)`, with
        //      last-write-wins on re-uploaded parts already resolved by the remote.
        //    Matching a winner's `retag` to its record yields the surviving upload's `pmd5`; stale
        //    re-upload records simply never match and are swept at settle.
        let pmd5_by_part = self.load_part_pmd5s(&bucket, &upload_id).await?;
        let winners: HashMap<i32, (String, u64)> = self
            .remote()
            .list_parts(&bucket, &key, &upload_id)
            .await?
            .into_iter()
            .map(|(n, retag, size)| (n, (retag, size)))
            .collect();

        let mut pmd5s = Vec::with_capacity(requested.len());
        let mut remote_parts = Vec::with_capacity(requested.len());
        let mut total_plen: u64 = 0;
        // Parts table (§6): cumulative ciphertext end-offset after each part, taken from the
        // remote's own part sizes — the exact bytes the native complete will concatenate.
        let mut table = Vec::with_capacity(requested.len());
        let mut ct_acc: u64 = 0;
        for cp in &requested {
            let n = cp
                .part_number
                .ok_or_else(|| s3_error!(InvalidPart, "part entry missing part number"))?;
            let (retag, size) = winners
                .get(&n)
                .ok_or_else(|| s3_error!(InvalidPart, "no such uploaded part"))?;
            let pmd5 = pmd5_by_part
                .get(&(n, retag.clone()))
                .cloned()
                .ok_or_else(|| Error::Backend(format!("no local pmd5 for winning part {n}")))?;
            // S3 verifies the caller's part ETags against what the uploads returned — for hypha
            // those are the plaintext part MD5s.
            if let Some(e) = &cp.e_tag {
                if e.value().trim_matches('"') != pmd5 {
                    return Err(s3_error!(InvalidPart, "part etag mismatch"));
                }
            }
            let plen = plaintext_len_from(*size, HLEN).ok_or_else(|| {
                Error::Backend(format!("part {n} size {size} inconsistent with HLEN"))
            })?;
            total_plen += plen;
            ct_acc += *size;
            table.push(ct_acc);
            pmd5s.push(pmd5);
            remote_parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(n)
                    .e_tag(retag)
                    .build(),
            );
        }
        let md5 = meta::composite_md5(&pmd5s)
            .ok_or_else(|| Error::Backend("empty part md5 set".into()))?;
        let cetag = meta::composite_etag(&pmd5s)
            .ok_or_else(|| Error::Backend("empty part md5 set".into()))?;
        let mtime_ms = tier::now_ms();

        // 2. Upload the terminating **trailer part** (§6) — the object's one facts + parts-table
        //    carrier, its part number above every client part (the 9999 cap guarantees room) — so
        //    the native complete below commits body and facts in one atomic op. A crash from here
        //    on leaves only the dangling native upload, swept like any abandoned one.
        let footer = Footer {
            kind: FooterKind::Composite,
            count: requested.len() as u32,
            plen: total_plen,
            mtime_ms,
            md5,
        };
        let trailer = encode_trailer(&self.tier.trailer_key, &key, ct_acc, &footer, &table);
        let trailer_pn = requested.last().and_then(|p| p.part_number).unwrap_or(0) + 1;
        let fout = self
            .remote()
            .upload_part(
                &bucket,
                &key,
                &upload_id,
                trailer_pn,
                aws_sdk_s3::primitives::ByteStream::from(trailer.clone()),
                Some(trailer.len() as i64),
            )
            .await?;
        // The remote just accepted this part, so it must echo its ETag; an empty one would silently
        // build a mismatched CompletedPart and fail (or corrupt) the native complete.
        let trailer_etag = fout
            .e_tag()
            .ok_or_else(|| Error::Backend("trailer part upload returned no ETag".into()))?;
        remote_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(trailer_pn)
                .e_tag(trailer_etag)
                .build(),
        );

        // 3. Mark → 4. commit (the native complete concatenates the parts at K).
        self.tier.mark_transit_locked(&bucket, &key).await?;
        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(remote_parts))
            .build();
        if let Err(e) = self
            .remote()
            .complete_multipart(&bucket, &key, &upload_id, completed)
            .await
        {
            // Failed or indeterminate commit: settle K to whatever the remote holds (§7) and
            // leave the native upload as a sweepable orphan.
            if let Err(re) = self.tier.repair_locked(&bucket, &key).await {
                tracing::warn!(key = %key, error = %re, "repair after failed commit did not settle; leftover mark repaired on next access");
            }
            return Err(e.into());
        }

        // 5. Settle: project the tombstone + twin, drop the mpu state.
        self.tier
            .settle_evict_locked(&bucket, &key, total_plen, &cetag, mtime_ms)
            .await?;
        self.drop_mpu_state(&bucket, &upload_id).await?;

        let resp = CompleteMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(key),
            e_tag: Some(ETag::Strong(cetag)),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    pub(super) async fn op_abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let input = req.input;
        match self
            .remote()
            .abort_multipart(&input.bucket, &input.key, &input.upload_id)
            .await
        {
            // Already gone remotely: still drop our records — abort is idempotent.
            Ok(()) | Err(Error::NotFound) => {}
            Err(e) => return Err(e.into()),
        }
        self.drop_mpu_state(&input.bucket, &input.upload_id).await?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    /// One LIST of an upload's part records → `(part_number, retag) → pmd5` (facts in the keys,
    /// §6). Both surviving and stale (re-uploaded-over) records appear; complete matches by the
    /// remote's winning `retag`, so the stale ones never resolve. The upload's own `/u` record and
    /// any malformed key don't parse and are skipped.
    async fn load_part_pmd5s(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> Result<HashMap<(i32, String), String>, Error> {
        let prefix = meta::mpu_prefix(upload_id);
        let mut out = HashMap::new();
        let mut token: Option<String> = None;
        loop {
            let page = self
                .cache()
                .list(
                    bucket,
                    Some(prefix.clone()),
                    None,
                    token.clone(),
                    None,
                    None,
                )
                .await?;
            for obj in page.contents.unwrap_or_default() {
                if let Some(full) = obj.key {
                    if let Some((n, retag, pmd5)) = meta::parse_mpu_part(&full) {
                        out.insert((n, retag.to_string()), pmd5.to_string());
                    }
                }
            }
            if page.is_truncated != Some(true) {
                return Ok(out);
            }
            token = page.next_continuation_token;
            if token.is_none() {
                return Ok(out);
            }
        }
    }

    /// Drop everything recorded for one upload (the `mpu/<id>/` range); complete/abort both end
    /// here. The §8 sweep reclaims records of uploads abandoned without either.
    async fn drop_mpu_state(&self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        let prefix = meta::mpu_prefix(upload_id);
        loop {
            let page = self
                .cache()
                .list(bucket, Some(prefix.clone()), None, None, None, None)
                .await?;
            let objs = page.contents.unwrap_or_default();
            if objs.is_empty() {
                return Ok(());
            }
            // One LIST page is ≤1000 keys — exactly one batch DeleteObjects.
            let keys: Vec<String> = objs
                .iter()
                .filter_map(|o| o.key())
                .map(str::to_string)
                .collect();
            self.cache().delete_objects(bucket, &keys).await?;
            if page.is_truncated != Some(true) {
                return Ok(());
            }
        }
    }
}
