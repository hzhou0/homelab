//! Multipart (§7) — one path, both modes. Parts route **around the cache** onto the remote's own
//! native multipart upload at K: each part is an independent, pure age file (fresh file key ⇒
//! parallel and re-uploaded parts need no coordination), and the remote concatenates them at
//! complete. The object's facts **and its parts table** travel as a terminating trailer that is the
//! object's final bytes — normally its own part above every client part, but folded into the last
//! client part whenever nothing can follow that part: it is below the backend's 5 MiB minimum (only
//! the final part is exempt, and a separate trailer part would steal that exemption), or it is part
//! 10000 (no number is left). Clients keep S3's full 1–10000 range; the trailer never costs one.
//! Either way it lands in the same native complete, so the commit lands body and facts in one op.
//! The table is the per-part cumulative ciphertext end-offsets, so reads recover every part boundary
//! from the trailer alone — no remote part-index calls (§6).
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

use super::{resolve_storage_class, ts_ms, write_metadata, Hypha, MAX_INLINE_PLAINTEXT};
use crate::codec;
use crate::tier;

/// A fresh token naming one part's retained ciphertext ([`meta::mpu_stash_key`]). Minted before the
/// part streams, since the remote's `retag` — which disambiguates re-uploads everywhere else —
/// doesn't exist until the upload returns. 128 random bits, base64url unpadded: its alphabet is
/// `A–Z a–z 0–9 - _`, so no `;` and no control byte, and it can't disturb the `;`-delimited record
/// key it rides on. It must be unpredictable rather than merely distinct: two concurrent
/// re-uploads of one part colliding here would let the fold take the losing generation's bytes.
fn mint_nonce() -> String {
    base64_simd::URL_SAFE_NO_PAD.encode_to_string(rand::random::<[u8; 16]>())
}

/// S3/MinIO reject any multipart part below 5 MiB except the upload's final part. hypha's trailer
/// normally occupies that final-part slot, so a client's last data part this small must instead
/// *carry* the trailer (the fold in `op_complete_multipart_upload`, §7); `op_upload_part` retains
/// such a part's ciphertext up front so complete can re-upload it as `part ‖ trailer`.
const MIN_REMOTE_PART: u64 = 5 * 1024 * 1024;

impl Hypha {
    pub(super) async fn op_create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;
        let storage_class = resolve_storage_class(input.storage_class.as_ref())?;

        let created = self
            .remote()
            .create_multipart(&bucket, &key, HashMap::new())
            .await?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| Error::Backend("remote returned no upload id".into()))?
            .to_string();

        // The upload's own record: client key as the body (keys may carry bytes an ASCII
        // metadata header can't), and — in its metadata — the pass-through carrier this upload
        // will settle with, parked here because complete is where it reaches the tombstone (§7).
        self.cache()
            .put_small(
                &bucket,
                &meta::mpu_upload_key(&upload_id),
                key.clone().into_bytes(),
                write_metadata(input.metadata.as_ref(), &storage_class),
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
        if !(1..=meta::MAX_CLIENT_PART).contains(&part_number) {
            return Err(s3_error!(
                InvalidPart,
                "part number must be between 1 and 10000"
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
        let (ct_len, enc, etag_rx) =
            codec::encrypt_blob_with_etag(self.env(), body, plen, None, None)
                .await
                .map_err(Error::Io)?;

        // Retain the ciphertext if this part admits no successor (§7) — one predicate, so the
        // decision here and complete's fold decision cannot drift apart. When it fires the encrypted
        // stream is split and driven into the remote and the cache in one pass: no buffering, and
        // no size distinction, so a 4 KiB part and a 4 GiB one take the same path. The retained copy
        // is keyed by a nonce minted now, because that write starts before the remote has returned
        // the `retag` that names this generation everywhere else.
        let stash_nonce = meta::admits_no_successor(part_number, ct_len, MIN_REMOTE_PART)
            .then(mint_nonce)
            .unwrap_or_default();

        let out = if stash_nonce.is_empty() {
            self.remote()
                .upload_part(
                    &bucket,
                    &key,
                    &input.upload_id,
                    part_number,
                    enc,
                    Some(ct_len as i64),
                )
                .await?
        } else {
            let stash_key = meta::mpu_stash_key(&input.upload_id, part_number, &stash_nonce);
            let (to_remote, to_cache) = codec::tee(enc);
            let (out, _) = tokio::try_join!(
                self.remote().upload_part(
                    &bucket,
                    &key,
                    &input.upload_id,
                    part_number,
                    to_remote,
                    Some(ct_len as i64),
                ),
                self.cache().put(
                    &bucket,
                    &stash_key,
                    to_cache,
                    Some(ct_len as i64),
                    HashMap::new(),
                    None,
                    None,
                ),
            )?;
            out
        };
        // The remote accepted the part, so it must echo the ETag that identifies it — an empty
        // `retag` would silently fail to match this part at complete (§6).
        let retag = out
            .e_tag()
            .ok_or_else(|| Error::Backend("part upload returned no ETag".into()))?
            .trim_matches('"')
            .to_string();
        // `UploadPart` passes no expected digest, so the mismatch arm is unreachable here.
        let pmd5 = etag_rx
            .await
            .map_err(|_| Error::Backend("MD5 task dropped before completing".into()))?
            .map_err(|_| {
                Error::Backend("unexpected digest mismatch on an unchecked part".into())
            })?;

        // Persist the part's facts in the record KEY (§6): `pmd5` (the plaintext MD5, unknowable to
        // the remote), `retag` (its last-write-wins token), and the nonce naming any retained
        // ciphertext. A re-upload writes a new key; the stale one is resolved away at complete by
        // the remote's `ListParts`, which is also what points the fold at the right retained copy.
        // Survives process restarts across a multi-hour upload; `plen` isn't stored — it's
        // `plaintext_len_from` the remote's part size at complete.
        self.cache()
            .put_small(
                &bucket,
                &meta::mpu_part_key(
                    &input.upload_id,
                    meta::MpuPart {
                        part_number,
                        retag: &retag,
                        pmd5: &pmd5,
                        stash_nonce: &stash_nonce,
                    },
                ),
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

        // The upload record also carries the pass-through metadata + storage class recorded at
        // create (§7); settle stamps them onto the tombstone below.
        let carrier = match self
            .cache()
            .head(&bucket, &meta::mpu_upload_key(&upload_id))
            .await
        {
            Ok(h) => h.metadata.clone().unwrap_or_default(),
            Err(Error::NotFound) => return Err(s3_error!(NoSuchUpload, "unknown upload id")),
            Err(e) => return Err(e.into()),
        };

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
            let (pmd5, _) = pmd5_by_part
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

        // 2. Build the terminating trailer (§6) — the object's one facts + parts-table carrier —
        //    and place it as the object's final bytes so the native complete below commits body and
        //    facts in one atomic op. A crash from here on leaves only the dangling native upload,
        //    swept like any abandoned one.
        let footer = Footer {
            kind: FooterKind::Composite,
            count: requested.len() as u32,
            plen: total_plen,
            mtime_ms,
            md5,
        };
        let trailer = encode_trailer(&self.tier.trailer_key, &key, ct_acc, &footer, &table);

        // Placement turns on one question: can a part follow the highest client part? If it can,
        // the trailer rides its own part at highest + 1. If it cannot — the part is under the 5 MiB
        // minimum (so any backend would reject it as non-final), or it is part 10000 (so there is
        // no number left) — the trailer folds into it, re-uploaded as `part ‖ trailer` so it stays
        // final. The same `admits_no_successor` predicate decided at upload time that this part's
        // ciphertext had to be retained, which is what makes the fold possible at all: an
        // in-progress part cannot be read back. K is byte-identical either way (same
        // concatenation), so reads are unaffected (§7).
        let last_n = requested.last().and_then(|p| p.part_number).unwrap_or(0);
        let (last_retag, last_size) = winners
            .get(&last_n)
            .cloned()
            .unwrap_or_else(|| (String::new(), 0));
        if meta::admits_no_successor(last_n, last_size, MIN_REMOTE_PART) {
            // The winning generation's own retained copy: its record carries the nonce naming it,
            // so a re-uploaded part folds exactly what `ListParts` picked (§6).
            let (_, nonce) = pmd5_by_part
                .get(&(last_n, last_retag.clone()))
                .cloned()
                .unwrap_or_default();
            let stash_key = meta::mpu_stash_key(&upload_id, last_n, &nonce);
            let stashed = match self.cache().get(&bucket, &stash_key, None).await {
                Ok(o) => o,
                Err(Error::NotFound) => {
                    return Err(Error::Backend(format!(
                        "final part {last_n} ciphertext not retained; cannot fold trailer"
                    ))
                    .into())
                }
                Err(e) => return Err(e.into()),
            };
            // Streamed, not buffered: part 10000 may be gigabytes.
            let stash_len = stashed.content_length().unwrap_or(0).max(0) as u64;
            let folded_len = stash_len + trailer.len() as u64;
            let folded = codec::append_bytes(stashed.body, trailer.clone());
            let fout = self
                .remote()
                .upload_part(
                    &bucket,
                    &key,
                    &upload_id,
                    last_n,
                    folded,
                    Some(folded_len as i64),
                )
                .await?;
            let fold_etag = fout.e_tag().ok_or_else(|| {
                Error::Backend("folded final part upload returned no ETag".into())
            })?;
            // The last client part now carries the trailer; point its CompletedPart at the re-upload.
            *remote_parts
                .last_mut()
                .expect("requested is non-empty, so remote_parts is too") =
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(last_n)
                    .e_tag(fold_etag)
                    .build();
        } else {
            let trailer_pn = last_n + 1;
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
            // The remote just accepted this part, so it must echo its ETag; an empty one would
            // silently build a mismatched CompletedPart and fail (or corrupt) the native complete.
            let trailer_etag = fout
                .e_tag()
                .ok_or_else(|| Error::Backend("trailer part upload returned no ETag".into()))?;
            remote_parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(trailer_pn)
                    .e_tag(trailer_etag)
                    .build(),
            );
        }

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
            .settle_evict_locked(&bucket, &key, total_plen, &cetag, mtime_ms, carrier)
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

    /// **ListMultipartUploads** (§7): a straight proxy of the remote's own. hypha creates each
    /// native upload at the client key and returns the remote's upload id verbatim, so a remote
    /// page already carries `{Key, UploadId, Initiated}` with nothing to translate — and its
    /// `(key, upload_id)` ordering is what makes `key-marker`/`upload-id-marker` correct, which no
    /// cache-side record could offer (they are keyed by upload id alone).
    ///
    /// Remote-as-truth resolves both crash windows by construction: an upload whose remote create
    /// landed but whose cache record didn't is real and abortable, so it lists; a cache record whose
    /// remote upload was aborted or expired externally does not.
    pub(super) async fn op_list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let input = req.input;
        let raw = self
            .remote()
            .list_multipart_uploads(
                &input.bucket,
                input.prefix.clone(),
                input.delimiter.clone(),
                input.key_marker.clone(),
                input.upload_id_marker.clone(),
                input.max_uploads,
            )
            .await?;

        let uploads = raw
            .uploads
            .unwrap_or_default()
            .into_iter()
            .map(|u| MultipartUpload {
                key: u.key,
                upload_id: u.upload_id,
                initiated: u.initiated.and_then(|t| t.to_millis().ok()).map(ts_ms),
                // The class the client asked for at create lives in the cache record, and reporting
                // it would cost a fetch per upload — the cosmetic corner LIST already accepts (§7).
                storage_class: Some(StorageClass::from(meta::STANDARD.to_string())),
                ..Default::default()
            })
            .collect();

        let resp = ListMultipartUploadsOutput {
            bucket: Some(input.bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            key_marker: input.key_marker,
            upload_id_marker: input.upload_id_marker,
            max_uploads: input.max_uploads,
            // The backend's key-position cursor, forwarded verbatim.
            is_truncated: raw.is_truncated,
            next_key_marker: raw.next_key_marker,
            next_upload_id_marker: raw.next_upload_id_marker,
            common_prefixes: Some(
                raw.common_prefixes
                    .unwrap_or_default()
                    .into_iter()
                    .map(|cp| CommonPrefix { prefix: cp.prefix })
                    .collect(),
            ),
            uploads: Some(uploads),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// **ListParts** (§7): the remote's `ListParts` is authoritative for the winning part set and
    /// its ciphertext sizes; each winner's `retag` matches the mpu record holding that part's
    /// plaintext MD5 — the ETag the client saw at upload, and the one datum the remote cannot
    /// reproduce. Sizes convert back to plaintext through the closed form over the constant `HLEN`,
    /// and the reserved trailer part (above every client part) is filtered out.
    pub(super) async fn op_list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;

        match self
            .cache()
            .head(&bucket, &meta::mpu_upload_key(&input.upload_id))
            .await
        {
            Ok(_) => {}
            Err(Error::NotFound) => return Err(s3_error!(NoSuchUpload, "unknown upload id")),
            Err(e) => return Err(e.into()),
        }

        let pmd5_by_part = self.load_part_pmd5s(&bucket, &input.upload_id).await?;
        let mut parts: Vec<Part> = Vec::new();
        for (n, retag, size) in self
            .remote()
            .list_parts(&bucket, &key, &input.upload_id)
            .await?
        {
            // The trailer's own part, when it has one, sits above every client part.
            if n > meta::MAX_CLIENT_PART {
                continue;
            }
            // A winning part with no record lost its cache state; its plaintext ETag is gone and
            // cannot be re-derived from ciphertext, so there is nothing truthful to report for it.
            let Some((pmd5, _)) = pmd5_by_part.get(&(n, retag)) else {
                continue;
            };
            parts.push(Part {
                part_number: Some(n),
                e_tag: Some(ETag::Strong(pmd5.clone())),
                size: plaintext_len_from(size, HLEN).map(|p| p as i64),
                ..Default::default()
            });
        }
        parts.sort_by_key(|p| p.part_number);

        // Parts cap at 10000, so the winning set is already in hand and small — paginate over it
        // here rather than threading the remote's cursor through a set hypha has to re-filter.
        let after: i32 = input.part_number_marker.unwrap_or(0);
        let max = input.max_parts.unwrap_or(1000).max(0) as usize;
        let mut page: Vec<Part> = parts
            .into_iter()
            .filter(|p| p.part_number.unwrap_or(0) > after)
            .collect();
        let is_truncated = page.len() > max;
        page.truncate(max);

        let resp = ListPartsOutput {
            bucket: Some(input.bucket),
            key: Some(key),
            upload_id: Some(input.upload_id),
            max_parts: input.max_parts,
            part_number_marker: input.part_number_marker,
            next_part_number_marker: is_truncated
                .then(|| page.last().and_then(|p| p.part_number))
                .flatten(),
            is_truncated: Some(is_truncated),
            storage_class: Some(StorageClass::from(meta::STANDARD.to_string())),
            parts: Some(page),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// One LIST of an upload's part records → `(part_number, retag) → (pmd5, stash_nonce)` (facts
    /// in the keys, §6). Both surviving and stale (re-uploaded-over) records appear; complete
    /// matches by the remote's winning `retag`, so the stale ones never resolve — which is also
    /// what points a fold at the retained ciphertext of exactly the winning generation. The
    /// upload's own `/u` record, the `c` retained-ciphertext objects, and any malformed key don't
    /// parse and are skipped.
    async fn load_part_pmd5s(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> Result<HashMap<(i32, String), (String, String)>, Error> {
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
                    if let Some(p) = meta::parse_mpu_part(&full) {
                        out.insert(
                            (p.part_number, p.retag.to_string()),
                            (p.pmd5.to_string(), p.stash_nonce.to_string()),
                        );
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
