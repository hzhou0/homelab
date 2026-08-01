//! Durable multipart shared by both modes.
//!
//! Independently encrypted parts go directly to the remote. Completion atomically adds the facts
//! trailer, folding it into a terminal client part when S3 would reject a separate small part.
//! Fold intent makes retries distinguish an already-replaced terminal part.

use std::collections::HashMap;

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;
use hypha_format::offset::{ciphertext_len, plaintext_len_from, HLEN};
use hypha_format::{encode_trailer, Footer, FooterKind};

use super::{
    copied_part_retag, parse_content_md5, resolve_storage_class, ts_ms, write_metadata, Hypha,
    MAX_INLINE_PLAINTEXT,
};
use crate::codec;
use crate::gc::Plaintext;
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

fn mpu_part_lock_key(bucket: &str, upload_id: &str, part_number: i32) -> String {
    format!("mpu\0{bucket}\0{upload_id}\0{part_number}")
}

/// S3/MinIO reject any multipart part below 5 MiB except the upload's final part. hypha's trailer
/// normally occupies that final-part slot, so a client's last data part this small must instead
/// *carry* the trailer (the fold in `op_complete_multipart_upload`, §7); `op_upload_part` retains
/// such a part's ciphertext up front so complete can re-upload it as `part ‖ trailer`.
pub(super) const MIN_REMOTE_PART: u64 = 5 * 1024 * 1024;

const FOLD_PART_NUMBER: &str = "part";
const FOLD_RETAG: &str = "retag";
const FOLD_PMD5: &str = "pmd5";
const FOLD_STASH_NONCE: &str = "nonce";
const FOLD_CIPHERTEXT_LEN: &str = "ctlen";

#[derive(Clone, Debug)]
struct FoldIntent {
    part_number: i32,
    retag: String,
    pmd5: String,
    stash_nonce: String,
    ciphertext_len: u64,
}

#[derive(Clone, Debug)]
struct ResolvedPart {
    number: i32,
    retag: String,
    size: u64,
    stash_nonce: String,
}

impl Hypha {
    pub(super) async fn op_create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;
        let _gate = self.check_bucket(&bucket)?;
        let storage_class = resolve_storage_class(input.storage_class.as_ref())?;

        let created = self
            .remote()
            .create_multipart(&bucket, &key, HashMap::new(), input.content_type.clone())
            .await?;
        let upload_id = created
            .upload_id()
            .ok_or_else(|| Error::Backend("remote returned no upload id".into()))?
            .to_string();

        // The upload's own record: client key as the body (keys may carry bytes an ASCII
        // metadata header can't), and — in its metadata — the pass-through carrier this upload
        // will settle with, parked here because complete is where it reaches the tombstone (§7).
        self.meta()
            .put_small(
                &bucket,
                &meta::mpu_upload_key(&upload_id),
                key.clone().into_bytes(),
                write_metadata(
                    input.metadata.as_ref(),
                    &storage_class,
                    input.content_type.as_deref(),
                ),
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
        let _gate = self.check_bucket(&bucket)?;
        let part_number = input.part_number;
        validate_part_number(part_number)?;
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
        let expect_md5 = input
            .content_md5
            .as_deref()
            .map(parse_content_md5)
            .transpose()?;
        let body = input
            .body
            .ok_or_else(|| Error::Invalid("UploadPart requires a body".into()))?;

        self.require_upload(&bucket, &input.upload_id).await?;
        let lock_key = mpu_part_lock_key(&bucket, &input.upload_id, part_number);
        let _part_guard = self.tier.mpu_part_locks.lock(&lock_key).await;

        // Past the byte source, a part is a part: encrypt as a pure age file, stream to the remote,
        // record its facts. The copy path (`op_upload_part_copy`) shares this tail (§7).
        let pmd5 = self
            .stream_part(
                &bucket,
                &key,
                &input.upload_id,
                part_number,
                plen,
                body,
                expect_md5,
            )
            .await?;
        self.clear_fold_intent_for_part(&bucket, &input.upload_id, part_number)
            .await?;

        let resp = UploadPartOutput {
            e_tag: Some(ETag::Strong(pmd5)),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// The shared tail of `UploadPart` and the re-encrypt leg of `UploadPartCopy` (§7): one plaintext
    /// part body becomes its own pure age file on the remote's native upload. Returns the part's
    /// plaintext MD5, computed inline as the body streams.
    #[allow(clippy::too_many_arguments)]
    async fn stream_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        plen: u64,
        body: StreamingBlob,
        expect_md5: Option<[u8; 16]>,
    ) -> S3Result<String> {
        let (ct_len, enc, mut etag_rx) =
            codec::encrypt_blob_with_etag(self.env(), body, plen, None, expect_md5)
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

        let uploaded = if stash_nonce.is_empty() {
            self.remote()
                .upload_part(
                    bucket,
                    key,
                    upload_id,
                    part_number,
                    enc,
                    Some(ct_len as i64),
                )
                .await
        } else {
            let stash_key = meta::mpu_stash_key(upload_id, part_number, &stash_nonce);
            let (to_remote, to_cache) = codec::tee(enc);
            tokio::try_join!(
                self.remote().upload_part(
                    bucket,
                    key,
                    upload_id,
                    part_number,
                    to_remote,
                    Some(ct_len as i64),
                ),
                self.meta().put(
                    bucket,
                    &stash_key,
                    to_cache,
                    Some(ct_len as i64),
                    HashMap::new(),
                    None,
                    None,
                    None,
                    None,
                ),
            )
            .map(|(out, _)| out)
        };
        let out = match uploaded {
            Ok(out) => out,
            Err(e) => {
                return Err(match etag_rx.try_recv() {
                    Ok(Err(_)) => {
                        s3_error!(BadDigest, "Content-MD5 does not match the request body")
                    }
                    _ => e.into(),
                });
            }
        };
        // The remote accepted the part, so it must echo the ETag that identifies it — an empty
        // `retag` would silently fail to match this part at complete (§6).
        let retag = out
            .e_tag()
            .ok_or_else(|| Error::Backend("part upload returned no ETag".into()))?
            .trim_matches('"')
            .to_string();
        let pmd5 = etag_rx
            .await
            .map_err(|_| Error::Backend("MD5 task dropped before completing".into()))?
            .map_err(|_| s3_error!(BadDigest, "Content-MD5 does not match the request body"))?;

        self.record_part(bucket, upload_id, part_number, &retag, &pmd5, &stash_nonce)
            .await?;
        Ok(pmd5)
    }

    /// Fail with `NoSuchUpload` unless the upload's record is in `<meta>` — the eventual complete
    /// needs these records.
    async fn require_upload(&self, bucket: &str, upload_id: &str) -> S3Result<()> {
        match self
            .meta()
            .head(bucket, &meta::mpu_upload_key(upload_id))
            .await
        {
            Ok(_) => Ok(()),
            Err(Error::NotFound) => Err(s3_error!(NoSuchUpload, "unknown upload id")),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist a part's facts in the record KEY (§6): `pmd5` (the plaintext MD5, unknowable to the
    /// remote), `retag` (its last-write-wins token), and the nonce naming any retained ciphertext. A
    /// re-upload writes a new key; the stale one is resolved away at complete by the remote's
    /// `ListParts`, which is also what points the fold at the right retained copy. Survives process
    /// restarts across a multi-hour upload; `plen` isn't stored — it's `plaintext_len_from` the
    /// remote's part size at complete.
    async fn record_part(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: i32,
        retag: &str,
        pmd5: &str,
        stash_nonce: &str,
    ) -> Result<(), Error> {
        self.meta()
            .put_small(
                bucket,
                &meta::mpu_part_key(
                    upload_id,
                    meta::MpuPart {
                        part_number,
                        retag,
                        pmd5,
                        stash_nonce,
                    },
                ),
                Vec::new(),
                HashMap::new(),
                None,
                None,
            )
            .await?;
        Ok(())
    }

    async fn fold_intent(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> Result<Option<FoldIntent>, Error> {
        let head = match self
            .meta()
            .head(bucket, &meta::mpu_fold_key(upload_id))
            .await
        {
            Ok(head) => head,
            Err(Error::NotFound) => return Ok(None),
            Err(e) => return Err(e),
        };
        let md = head.metadata.unwrap_or_default();
        let malformed = || Error::Backend("multipart fold intent is malformed".into());
        Ok(Some(FoldIntent {
            part_number: md
                .get(FOLD_PART_NUMBER)
                .ok_or_else(malformed)?
                .parse()
                .map_err(|_| malformed())?,
            retag: md.get(FOLD_RETAG).ok_or_else(malformed)?.clone(),
            pmd5: md.get(FOLD_PMD5).ok_or_else(malformed)?.clone(),
            stash_nonce: md.get(FOLD_STASH_NONCE).ok_or_else(malformed)?.clone(),
            ciphertext_len: md
                .get(FOLD_CIPHERTEXT_LEN)
                .ok_or_else(malformed)?
                .parse()
                .map_err(|_| malformed())?,
        }))
    }

    async fn persist_fold_intent(
        &self,
        bucket: &str,
        upload_id: &str,
        intent: &FoldIntent,
    ) -> Result<(), Error> {
        let md = HashMap::from([
            (FOLD_PART_NUMBER.to_string(), intent.part_number.to_string()),
            (FOLD_RETAG.to_string(), intent.retag.clone()),
            (FOLD_PMD5.to_string(), intent.pmd5.clone()),
            (FOLD_STASH_NONCE.to_string(), intent.stash_nonce.clone()),
            (
                FOLD_CIPHERTEXT_LEN.to_string(),
                intent.ciphertext_len.to_string(),
            ),
        ]);
        self.meta()
            .put_small(
                bucket,
                &meta::mpu_fold_key(upload_id),
                Vec::new(),
                md,
                None,
                None,
            )
            .await?;
        Ok(())
    }

    async fn clear_fold_intent_for_part(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: i32,
    ) -> Result<(), Error> {
        if self
            .fold_intent(bucket, upload_id)
            .await?
            .is_some_and(|intent| intent.part_number == part_number)
        {
            match self
                .meta()
                .delete(bucket, &meta::mpu_fold_key(upload_id))
                .await
            {
                Ok(()) | Err(Error::NotFound) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn clear_fold_intent(&self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        match self
            .meta()
            .delete(bucket, &meta::mpu_fold_key(upload_id))
            .await
        {
            Ok(()) | Err(Error::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// The ciphertext retained for a final part (§7), size-verified against the recorded value — the
    /// guard that a fold/unfold re-upload concatenates from exactly the bytes the ETag was computed
    /// over. Absence means the upload never reached the retain path, so there is nothing to recover.
    async fn retained_part_body(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: i32,
        stash_nonce: &str,
        expected_len: u64,
        why: &str,
    ) -> Result<aws_sdk_s3::primitives::ByteStream, Error> {
        let stash_key = meta::mpu_stash_key(upload_id, part_number, stash_nonce);
        let retained = match self.meta().get(bucket, &stash_key, None).await {
            Ok(o) => o,
            Err(Error::NotFound) => {
                return Err(Error::Backend(format!(
                    "final part {part_number} ciphertext not retained; {why}"
                )))
            }
            Err(e) => return Err(e),
        };
        let len = retained.content_length().unwrap_or(0).max(0) as u64;
        if len != expected_len {
            return Err(Error::Backend(format!(
                "retained final part {part_number} has size {len}, expected {expected_len}"
            )));
        }
        Ok(retained.body)
    }

    /// **UploadPartCopy** (§7): the copy-source is an alternate byte source for a part. Fast path —
    /// a whole, unranged, single-part, remote-resident source is one age file, so its body copies
    /// **server-side** with `pmd5` = the source cetag and no bytes through hypha; everything else
    /// (composite sources, ranged copies, parts complete's fold would need) re-encrypts via
    /// `stream_part`.
    pub(super) async fn op_upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;
        let _gate = self.check_bucket(&bucket)?;

        let part_number = input.part_number;
        validate_part_number(part_number)?;

        let (src_bucket, src_key) = parse_copy_source(&input.copy_source)?;
        meta::validate_client_key(&src_key).map_err(|e| Error::Invalid(e.to_string()))?;

        self.require_upload(&bucket, &input.upload_id).await?;
        let lock_key = mpu_part_lock_key(&bucket, &input.upload_id, part_number);
        let _part_guard = self.tier.mpu_part_locks.lock(&lock_key).await;

        let (facts, _md, live) = self
            .resolve_copy_source(
                &src_bucket,
                &src_key,
                input.copy_source_if_match.as_ref(),
                input.copy_source_if_none_match.as_ref(),
                input.copy_source_if_modified_since.as_ref(),
                input.copy_source_if_unmodified_since.as_ref(),
            )
            .await?;

        // The copy range over the source's PLAINTEXT: the whole object, or `copy-source-range`.
        let pt = match input.copy_source_range.as_deref() {
            None => 0..facts.plen,
            Some(r) => parse_copy_source_range(r, facts.plen)?,
        };
        let part_plen = pt.end - pt.start;
        if part_plen > MAX_INLINE_PLAINTEXT {
            return Err(s3_error!(
                EntityTooLarge,
                "parts are capped at {MAX_INLINE_PLAINTEXT} bytes"
            ));
        }
        let whole = pt == (0..facts.plen);
        let composite = meta::is_composite_etag(&facts.cetag);

        // Fast path: a whole, unranged, single-part, remote-resident source is one age file — copy
        // its body range server-side (trailer excluded), `pmd5` = source cetag (a single-part cetag
        // *is* its plaintext MD5). Declined when the part admits no successor: complete's fold needs
        // the ciphertext retained, which only the re-encrypt tee produces — a server-side copy never
        // routes the bytes through hypha, so there is nothing to stash.
        let body_ct_len = ciphertext_len(part_plen, HLEN);
        let pmd5 = if !live
            && !composite
            && whole
            && !meta::admits_no_successor(part_number, body_ct_len, MIN_REMOTE_PART)
        {
            let out = self
                .remote()
                .upload_part_copy(
                    &bucket,
                    &key,
                    &input.upload_id,
                    part_number,
                    &src_bucket,
                    &src_key,
                    Some(format!("bytes=0-{}", body_ct_len - 1)),
                )
                .await?;
            let retag = copied_part_retag(&out)?;
            let pmd5 = facts.cetag.clone();
            self.record_part(&bucket, &input.upload_id, part_number, &retag, &pmd5, "")
                .await?;
            pmd5
        } else {
            // Re-encrypt path: obtain the source plaintext (whole or ranged), then it's a part
            // like any other.
            let plaintext = if live {
                let range = (!whole).then(|| format!("bytes={}-{}", pt.start, pt.end - 1));
                let out = self.data().get(&src_bucket, &src_key, range).await?;
                codec::bytestream_to_blob(out.body)
            } else {
                let sub = (!whole).then(|| pt.clone());
                self.tier
                    .decrypt_remote_body(&src_bucket, &src_key, &facts.cetag, sub)
                    .await?
            };
            self.stream_part(
                &bucket,
                &key,
                &input.upload_id,
                part_number,
                part_plen,
                plaintext,
                None,
            )
            .await?
        };
        self.clear_fold_intent_for_part(&bucket, &input.upload_id, part_number)
            .await?;

        let resp = UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: Some(ETag::Strong(pmd5)),
                last_modified: Some(ts_ms(facts.mtime_ms)),
                ..Default::default()
            }),
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
        let (_gate, _) = self.prepare_write(&bucket, &key).await?;
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
        let last_requested_n = requested
            .last()
            .and_then(|part| part.part_number)
            .ok_or_else(|| s3_error!(InvalidPart, "part entry missing part number"))?;

        // The whole bracket runs under K's write lock (§7).
        let _guard = self.write_lock(&bucket, &key).await;
        // Only the final part can be folded. Same-part UploadPart calls share this lock, so the
        // intent below can never be mistaken for a later client re-upload; other parts stay parallel.
        let part_lock_key = mpu_part_lock_key(&bucket, &upload_id, last_requested_n);
        let _part_guard = self.tier.mpu_part_locks.lock(&part_lock_key).await;

        // The upload record also carries the pass-through metadata + storage class recorded at
        // create (§7); settle stamps them onto the tombstone below.
        let carrier = match self
            .meta()
            .head(&bucket, &meta::mpu_upload_key(&upload_id))
            .await
        {
            Ok(h) => h.metadata.unwrap_or_default(),
            Err(Error::NotFound) => return Err(s3_error!(NoSuchUpload, "unknown upload id")),
            Err(e) => return Err(e.into()),
        };

        // 1. Recover per-part facts and geometry, then compose the client ETag, total plaintext
        //    length, and offset table. Two reads, no per-part HEAD (§6/§7):
        //    · one LIST of the upload's records → `(part, retag) → pmd5` (facts live in the keys);
        //    · one `ListParts` of the remote upload → each live `(part, retag)`'s ciphertext size.
        //
        //    The client's part ETag selects its cache record; intersecting that record with the
        //    remote's live generation rejects a superseded re-upload without trusting stale facts.
        let mut pmd5_by_part = self.load_part_pmd5s(&bucket, &upload_id).await?;
        let fold_intent = self.fold_intent(&bucket, &upload_id).await?;
        let mut live: HashMap<(i32, String), u64> = self
            .remote()
            .list_parts(&bucket, &key, &upload_id)
            .await?
            .into_iter()
            .map(|p| ((p.number, p.etag), p.size))
            .collect();

        // Normalize any interrupted fold before resolving this request. The retry need not repeat
        // the same part list: once the retained pure part is live again, ordinary part selection
        // applies and a new fold intent can safely replace the old one.
        if let Some(intent) = fold_intent {
            let record_matches = pmd5_by_part
                .get(&(intent.part_number, intent.retag.clone()))
                .is_some_and(|(pmd5, nonce)| pmd5 == &intent.pmd5 && nonce == &intent.stash_nonce);
            if !record_matches {
                return Err(Error::Backend(
                    "multipart fold intent has no matching part record".into(),
                )
                .into());
            }
            let replacement_is_live = pmd5_by_part.iter().any(|((part_number, retag), _)| {
                *part_number == intent.part_number
                    && retag != &intent.retag
                    && live.contains_key(&(*part_number, retag.clone()))
            });
            let pure_is_live = replacement_is_live
                || pmd5_by_part
                    .iter()
                    .any(|((part_number, retag), (pmd5, nonce))| {
                        *part_number == intent.part_number
                            && pmd5 == &intent.pmd5
                            && nonce == &intent.stash_nonce
                            && live.contains_key(&(*part_number, retag.clone()))
                    });
            if !pure_is_live {
                let original = self
                    .retained_part_body(
                        &bucket,
                        &upload_id,
                        intent.part_number,
                        &intent.stash_nonce,
                        intent.ciphertext_len,
                        "cannot unfold",
                    )
                    .await?;
                let out = self
                    .remote()
                    .upload_part(
                        &bucket,
                        &key,
                        &upload_id,
                        intent.part_number,
                        original,
                        Some(intent.ciphertext_len as i64),
                    )
                    .await?;
                let retag = out
                    .e_tag()
                    .ok_or_else(|| {
                        Error::Backend("unfolded final part upload returned no ETag".into())
                    })?
                    .trim_matches('"')
                    .to_string();
                self.record_part(
                    &bucket,
                    &upload_id,
                    intent.part_number,
                    &retag,
                    &intent.pmd5,
                    &intent.stash_nonce,
                )
                .await?;
                pmd5_by_part.insert(
                    (intent.part_number, retag.clone()),
                    (intent.pmd5, intent.stash_nonce),
                );
                live.insert((intent.part_number, retag), intent.ciphertext_len);
            }
            self.clear_fold_intent(&bucket, &upload_id).await?;
        }

        let mut pmd5s = Vec::with_capacity(requested.len());
        let mut remote_parts = Vec::with_capacity(requested.len());
        let mut resolved = Vec::with_capacity(requested.len());
        let mut total_plen: u64 = 0;
        // Parts table (§6): cumulative ciphertext end-offset after each part, taken from the
        // remote's own part sizes — the exact bytes the native complete will concatenate.
        let mut table = Vec::with_capacity(requested.len());
        let mut ct_acc: u64 = 0;
        for cp in &requested {
            let n = cp
                .part_number
                .ok_or_else(|| s3_error!(InvalidPart, "part entry missing part number"))?;
            let pmd5 = cp
                .e_tag
                .as_ref()
                .ok_or_else(|| s3_error!(InvalidPart, "part entry missing etag"))?
                .value()
                .trim_matches('"')
                .to_string();
            let current =
                pmd5_by_part
                    .iter()
                    .find_map(|((record_n, retag), (record_pmd5, stash_nonce))| {
                        (*record_n == n && record_pmd5 == &pmd5)
                            .then(|| {
                                live.get(&(n, retag.clone())).map(|size| ResolvedPart {
                                    number: n,
                                    retag: retag.clone(),
                                    size: *size,
                                    stash_nonce: stash_nonce.clone(),
                                })
                            })
                            .flatten()
                    });
            let part = current.ok_or_else(|| s3_error!(InvalidPart, "no such uploaded part"))?;
            let plen = plaintext_len_from(part.size, HLEN).ok_or_else(|| {
                Error::Backend(format!(
                    "part {n} size {} inconsistent with HLEN",
                    part.size
                ))
            })?;
            total_plen += plen;
            ct_acc += part.size;
            table.push(ct_acc);
            pmd5s.push(pmd5);
            remote_parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(n)
                    .e_tag(&part.retag)
                    .build(),
            );
            resolved.push(part);
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
        let last = resolved
            .last()
            .cloned()
            .expect("requested is non-empty, so resolved is too");
        if meta::admits_no_successor(last.number, last.size, MIN_REMOTE_PART) {
            // The generation the caller named, not whichever the listing offered: its record carries
            // the nonce naming the ciphertext retained for it, so the fold re-uploads exactly the
            // bytes the composite ETag was just computed over (§6).
            if last.stash_nonce.is_empty() {
                return Err(Error::Backend(format!(
                    "final part {} ciphertext not retained; cannot fold trailer",
                    last.number
                ))
                .into());
            }
            let desired_intent = FoldIntent {
                part_number: last.number,
                retag: last.retag.clone(),
                pmd5: pmd5s
                    .last()
                    .cloned()
                    .expect("requested is non-empty, so pmd5s is too"),
                stash_nonce: last.stash_nonce.clone(),
                ciphertext_len: last.size,
            };
            self.persist_fold_intent(&bucket, &upload_id, &desired_intent)
                .await?;
            let stashed = self
                .retained_part_body(
                    &bucket,
                    &upload_id,
                    last.number,
                    &last.stash_nonce,
                    last.size,
                    "cannot fold the trailer",
                )
                .await?;
            let folded_len = last.size + trailer.len() as u64;
            // Streamed, not buffered: part 10000 may be gigabytes.
            let folded = codec::append_bytes(stashed, trailer.clone());
            let fout = self
                .remote()
                .upload_part(
                    &bucket,
                    &key,
                    &upload_id,
                    last.number,
                    folded,
                    Some(folded_len as i64),
                )
                .await?;
            let fold_etag = fout.e_tag().ok_or_else(|| {
                Error::Backend("folded final part upload returned no ETag".into())
            })?;
            *remote_parts
                .last_mut()
                .expect("requested is non-empty, so remote_parts is too") =
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(last.number)
                    .e_tag(fold_etag)
                    .build();
        } else {
            let trailer_pn = last.number + 1;
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
        self.tier
            .settle_evict_locked(&bucket, &key, total_plen, &cetag, mtime_ms, carrier)
            .await?;

        // A completed composite is remote-resident with only a tombstone at K, so what a future
        // eviction could take — and therefore what this write is a statement of interest in — is the
        // shadow the first read will land (§8).
        self.gc.touch(&bucket, &key, Plaintext::of(&cetag));
        // A new composite at K supersedes the previous generation's shadow, which lands at the same
        // shadow key but under the old generation's ETag and so is unreachable (§8).
        self.orphans.owe(&bucket, &key);
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
        let _gate = self.check_bucket(&input.bucket)?;
        match self
            .remote()
            .abort_multipart(&input.bucket, &input.key, &input.upload_id)
            .await
        {
            // Already gone remotely: abort is idempotent, and the records go the same way either
            // way — the remote no longer running this upload is the whole of what the sweep needs.
            Ok(()) | Err(Error::NotFound) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    /// **ListMultipartUploads** (§7): a straight proxy of the remote's own — hypha creates each
    /// native upload at the client key and returns the remote's id verbatim, so the page needs no
    /// translation and the remote's own `(key, upload_id)` ordering makes
    /// `key-marker`/`upload-id-marker` correct, which no cache-side record could offer (they are
    /// keyed by upload id alone). Remote-as-truth resolves both crash windows by construction (§7).
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

        let uploads: Vec<MultipartUpload> = raw
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

    /// **ListParts** (§7): the remote's `ListParts` supplies the live part set and its ciphertext
    /// sizes; each entry's `retag` matches the mpu record holding that part's plaintext MD5 — the
    /// ETag the client saw at upload, and the one datum the remote cannot reproduce. Sizes convert
    /// back to plaintext through the closed form over the constant `HLEN`, and the reserved trailer
    /// part (above every client part) is filtered out.
    pub(super) async fn op_list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;
        self.require_upload(&bucket, &input.upload_id).await?;

        let pmd5_by_part = self.load_part_pmd5s(&bucket, &input.upload_id).await?;
        let mut parts: Vec<Part> = self
            .remote()
            .list_parts(&bucket, &key, &input.upload_id)
            .await?
            .into_iter()
            .filter(|p| p.number <= meta::MAX_CLIENT_PART)
            .filter_map(|p| {
                pmd5_by_part.get(&(p.number, p.etag)).map(|(pmd5, _)| Part {
                    part_number: Some(p.number),
                    e_tag: Some(ETag::Strong(pmd5.clone())),
                    size: plaintext_len_from(p.size, HLEN).map(|n| n as i64),
                    ..Default::default()
                })
            })
            .collect();
        parts.sort_by_key(|p| p.part_number);

        // Parts cap at 10000, so the set is already in hand and small.
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
                .meta()
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
}

/// Resolve an `x-amz-copy-source` to a `(bucket, key)` this deployment can serve (§7). hypha has no
/// versioning and no access-point/outpost addressing, so those forms are rejected rather than
/// silently mishandled.
pub(super) fn parse_copy_source(cs: &CopySource) -> S3Result<(String, String)> {
    match cs {
        CopySource::Bucket {
            bucket,
            key,
            version_id,
        } => {
            if version_id.is_some() {
                return Err(s3_error!(
                    NotImplemented,
                    "hypha does not support versioned copy sources"
                ));
            }
            Ok((bucket.to_string(), key.to_string()))
        }
        _ => Err(s3_error!(
            NotImplemented,
            "access point and outpost copy sources are not supported"
        )),
    }
}

/// Parse a `copy-source-range` header (`bytes=first-last`, both bounds required) into a half-open
/// plaintext range, validated against the source length.
fn parse_copy_source_range(raw: &str, plen: u64) -> S3Result<std::ops::Range<u64>> {
    let malformed = || {
        s3_error!(
            InvalidArgument,
            "copy-source-range must be of the form bytes=first-last"
        )
    };
    let (a, b) = raw
        .strip_prefix("bytes=")
        .and_then(|spec| spec.split_once('-'))
        .ok_or_else(malformed)?;
    let first: u64 = a.trim().parse().map_err(|_| malformed())?;
    let last: u64 = b.trim().parse().map_err(|_| malformed())?;
    if first > last || last >= plen {
        return Err(s3_error!(
            InvalidArgument,
            "copy-source-range is out of the source object's bounds"
        ));
    }
    Ok(first..last + 1)
}

fn validate_part_number(part_number: i32) -> S3Result<()> {
    if !(1..=meta::MAX_CLIENT_PART).contains(&part_number) {
        return Err(s3_error!(
            InvalidPart,
            "part number must be between 1 and 10000"
        ));
    }
    Ok(())
}
