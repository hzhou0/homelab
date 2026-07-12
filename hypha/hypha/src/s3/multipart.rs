//! Multipart (§7) — one path, both modes. Parts route **around the cache** onto the remote's own
//! native multipart upload at K: each part is an independent, pure age file (fresh file key ⇒
//! parallel and re-uploaded parts need no coordination), the remote concatenates them at
//! complete, and reads derive part boundaries from the remote's part index — no stored part
//! table (§6). The object's facts travel as its **one terminating footer part** (48 bytes,
//! part number above every client part — hence the 9999 client cap), uploaded just before the
//! native complete so the commit lands body and facts in one atomic op; no side records, no
//! tags.
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
use hypha_format::{Footer, FooterKind, FOOTER_LEN};

use super::{Hypha, MAX_INLINE_PLAINTEXT};
use crate::codec;
use crate::tier;

impl Hypha {
    pub(super) async fn op_create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;

        let created = self.remote().create_multipart(&key, HashMap::new()).await?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| Error::Backend("remote returned no upload id".into()))?
            .to_string();

        // The upload's own record: client key as the body (keys may carry bytes an ASCII
        // metadata header can't).
        self.cache()
            .put_small(
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
            .head(&meta::mpu_upload_key(&input.upload_id))
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
                &key,
                &input.upload_id,
                part_number,
                enc,
                Some(ct_len as i64),
            )
            .await?;
        let retag = out
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let pmd5 = etag_rx
            .await
            .map_err(|_| Error::Backend("MD5 task dropped before completing".into()))?;

        // Persist the part's facts; last write wins on re-upload, mirroring the remote's own
        // part semantics. Survives process restarts across a multi-hour upload (§6).
        let mut pmd = HashMap::new();
        pmd.insert(meta::PMD5.to_string(), pmd5.clone());
        pmd.insert(meta::PLEN.to_string(), plen.to_string());
        pmd.insert(meta::RETAG.to_string(), retag);
        self.cache()
            .put_small(
                &meta::mpu_part_key(&input.upload_id, part_number),
                Vec::new(),
                pmd,
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

        match self.cache().head(&meta::mpu_upload_key(&upload_id)).await {
            Ok(_) => {}
            Err(Error::NotFound) => return Err(s3_error!(NoSuchUpload, "unknown upload id")),
            Err(e) => return Err(e.into()),
        }

        // 1. Load the per-part facts for exactly the client's part list; compose the client ETag
        //    and total plaintext length.
        let mut pmd5s = Vec::with_capacity(requested.len());
        let mut remote_parts = Vec::with_capacity(requested.len());
        let mut total_plen: u64 = 0;
        for cp in &requested {
            let n = cp
                .part_number
                .ok_or_else(|| s3_error!(InvalidPart, "part entry missing part number"))?;
            let rec = match self.cache().head(&meta::mpu_part_key(&upload_id, n)).await {
                Ok(h) => h,
                Err(Error::NotFound) => {
                    return Err(s3_error!(InvalidPart, "no such uploaded part"))
                }
                Err(e) => return Err(e.into()),
            };
            let md = rec.metadata().cloned().unwrap_or_default();
            let (pmd5, plen, retag) = md
                .get(meta::PMD5)
                .zip(md.get(meta::PLEN).and_then(|s| s.parse::<u64>().ok()))
                .zip(md.get(meta::RETAG))
                .map(|((a, b), c)| (a.clone(), b, c.clone()))
                .ok_or_else(|| Error::Backend("part record missing facts".into()))?;
            // S3 verifies the caller's part ETags against what the uploads returned — for hypha
            // those are the plaintext part MD5s.
            if let Some(e) = &cp.e_tag {
                if e.value().trim_matches('"') != pmd5 {
                    return Err(s3_error!(InvalidPart, "part etag mismatch"));
                }
            }
            total_plen += plen;
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

        // 2. Upload the terminating **footer part** (§6) — the object's one facts carrier, its
        //    part number above every client part (the 9999 cap guarantees room) — so the native
        //    complete below commits body and facts in one atomic op. A crash from here on leaves
        //    only the dangling native upload, swept like any abandoned one.
        let footer = Footer {
            kind: FooterKind::Composite,
            count: requested.len() as u32,
            plen: total_plen,
            mtime_ms,
            md5,
        };
        let footer_pn = requested.last().and_then(|p| p.part_number).unwrap_or(0) + 1;
        let fout = self
            .remote()
            .upload_part(
                &key,
                &upload_id,
                footer_pn,
                aws_sdk_s3::primitives::ByteStream::from(footer.encode().to_vec()),
                Some(FOOTER_LEN as i64),
            )
            .await?;
        remote_parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(footer_pn)
                .e_tag(fout.e_tag().unwrap_or_default().to_string())
                .build(),
        );

        // 3. Mark → 4. commit (the native complete concatenates the parts at K).
        self.tier.mark_transit_locked(&key).await?;
        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
            .set_parts(Some(remote_parts))
            .build();
        if let Err(e) = self
            .remote()
            .complete_multipart(&key, &upload_id, completed)
            .await
        {
            // Failed or indeterminate commit: settle K to whatever the remote holds (§7) and
            // leave the native upload as a sweepable orphan.
            let _ = self.tier.repair_locked(&key).await;
            return Err(e.into());
        }

        // 5. Settle: project the tombstone + twin, drop the mpu state.
        self.tier
            .settle_evict_locked(&key, total_plen, &cetag, mtime_ms)
            .await?;
        self.drop_mpu_state(&upload_id).await?;

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
            .abort_multipart(&input.key, &input.upload_id)
            .await
        {
            // Already gone remotely: still drop our records — abort is idempotent.
            Ok(()) | Err(Error::NotFound) => {}
            Err(e) => return Err(e.into()),
        }
        self.drop_mpu_state(&input.upload_id).await?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    /// Drop everything recorded for one upload (the `mpu/<id>/` range); complete/abort both end
    /// here. The §8 sweep reclaims records of uploads abandoned without either.
    async fn drop_mpu_state(&self, upload_id: &str) -> Result<(), Error> {
        let prefix = meta::mpu_prefix(upload_id);
        loop {
            let page = self
                .cache()
                .list(Some(prefix.clone()), None, None, None, None)
                .await?;
            let objs = page.contents.unwrap_or_default();
            if objs.is_empty() {
                return Ok(());
            }
            for obj in &objs {
                if let Some(full) = obj.key() {
                    let k = self.cache().strip(full).to_string();
                    self.cache().delete(&k).await?;
                }
            }
            if page.is_truncated != Some(true) {
                return Ok(());
            }
        }
    }
}
