//! Single-object durable and cached commits.
//!
//! Durable PUT frames plaintext-derived facts into the same remote object so body and facts commit
//! atomically. Cached PUT commits plaintext first and records the resulting remote obligation.

use aws_sdk_s3::primitives::ByteStream;
use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use std::collections::HashMap;

use hypha_core::error::Error;
use hypha_core::meta;

use super::overlay::WriteMode;
use super::{
    parse_content_md5, resolve_storage_class, write_metadata, Hypha, MAX_INLINE_PLAINTEXT,
};
use crate::codec::{self, SingleTrailer};
use crate::gc::Plaintext;
use crate::tier;

impl Hypha {
    pub(super) async fn op_put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;

        // Ahead of the mode split (§6): durable mode has no plaintext to spoof today, but a bucket
        // later switched to cached would rehydrate this plaintext to bare `K`, where it becomes the
        // classification — so the check must hold store-wide, not just for the mode that needs it now.
        let mut input = input;
        if input.content_length == Some(16) {
            if let Some(body) = input.body.take() {
                input.body = Some(reject_sentinel_body(body).await?);
            }
        }

        let (_gate, mode) = self.prepare_write(&bucket, &key).await?;
        if let WriteMode::Cached = mode {
            return self.op_put_object_cached(input, bucket, key).await;
        }

        let storage_class = resolve_storage_class(input.storage_class.as_ref())?;
        let expect_md5 = input
            .content_md5
            .as_deref()
            .map(parse_content_md5)
            .transpose()?;

        let (plen, body) = require_inline_body(&mut input)?;

        // One lock for the whole bracket: precondition → mark → commit → settle (§4).
        let _guard = self.write_lock(&bucket, &key).await;

        // Resolve the key's *current* client-visible ETag for the conditional-write check (§4),
        // then evaluate. Repairs a leftover transition mark first — the marking writer held this
        // lock, so a mark seen here is always a crash leftover.
        let current_etag = self.resolve_current_client_etag(&bucket, &key).await?;
        evaluate_precondition(
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            current_etag.as_deref(),
        )?;

        // Mark → commit → settle. The commit is one streaming PutObject at K: ciphertext framed
        // with the facts footer (client MD5 computed inline, §6) — durable mode never writes
        // plaintext to the cache. On failure or indeterminacy, settle K to whichever way the
        // remote actually landed — the same repair that handles a crash here (§7).
        let mtime_ms = tier::now_ms();
        self.tier.mark_transit_locked(&bucket, &key).await?;
        let trailer = SingleTrailer {
            trailer_key: self.tier.trailer_key.clone(),
            object_key: key.clone(),
            mtime_ms,
        };
        let (framed_len, enc, mut etag_rx) = match codec::encrypt_blob_with_etag(
            self.env(),
            body,
            plen,
            Some(trailer),
            expect_md5,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                if let Err(re) = self.tier.repair_locked(&bucket, &key).await {
                    tracing::warn!(key = %key, error = %re, "repair after failed commit did not settle; leftover mark repaired on next access");
                }
                return Err(Error::Io(e).into());
            }
        };
        if let Err(e) = self
            .remote()
            .put(
                &bucket,
                &key,
                enc,
                Some(framed_len as i64),
                HashMap::new(),
                None,
                None,
                None,
                input.content_type.clone(),
            )
            .await
        {
            if let Err(re) = self.tier.repair_locked(&bucket, &key).await {
                tracing::warn!(key = %key, error = %re, "repair after failed commit did not settle; leftover mark repaired on next access");
            }
            // A Content-MD5 mismatch cuts the ciphertext stream short, so it reaches us as a
            // backend fault; the digest channel is what tells the two apart (§7).
            return Err(match etag_rx.try_recv() {
                Ok(Err(_)) => s3_error!(BadDigest, "Content-MD5 does not match the request body"),
                _ => e.into(),
            });
        }
        // The PUT consumed the whole framed body, footer included — the etag is ready. Its loss
        // means the encrypt task died mid-commit; repair settles K from the remote either way.
        let etag = match etag_rx.await {
            Ok(Ok(e)) => e,
            other => {
                if let Err(re) = self.tier.repair_locked(&bucket, &key).await {
                    tracing::warn!(key = %key, error = %re, "repair after failed commit did not settle; leftover mark repaired on next access");
                }
                return Err(match other {
                    // A short body the remote nonetheless accepted: reject it, don't project it.
                    Ok(Err(_)) => {
                        s3_error!(BadDigest, "Content-MD5 does not match the request body")
                    }
                    _ => Error::Backend("MD5 task dropped before completing".into()).into(),
                });
            }
        };
        self.tier
            .settle_evict_locked(
                &bucket,
                &key,
                plen,
                &etag,
                mtime_ms,
                write_metadata(
                    input.metadata.as_ref(),
                    &storage_class,
                    input.content_type.as_deref(),
                ),
            )
            .await?;

        super::record_bytes(plen);
        // The write half of §8's recency feed. A write is the strongest statement of interest a key
        // gets, and a read-only ring would have write-hot/read-cold keys evict first — reclaiming
        // bytes the next PUT immediately takes back. Always `AtKey`: a PUT is single-part, so its
        // plaintext lives at K whether it stays cached or is rehydrated back there later.
        self.gc.touch(&bucket, &key, Plaintext::AtKey);
        let resp = PutObjectOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// Caller holds K's write lock, so a transition mark seen here is always a crash leftover and is
    /// repaired from the remote before the ETag is read off it (§4).
    pub(super) async fn resolve_current_client_etag(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<String>, Error> {
        match self.data().head(bucket, key).await {
            Ok(head) => {
                let md = head.metadata.as_ref();
                Ok(match md.and_then(meta::tomb_kind) {
                    Some(meta::TombKind::Transit) => {
                        self.tier.repair_locked(bucket, key).await?.map(|f| f.cetag)
                    }
                    Some(meta::TombKind::Evict) => md.and_then(|m| m.get(meta::CETAG)).cloned(),
                    None => head
                        .e_tag
                        .as_deref()
                        .map(|e| e.trim_matches('"').to_string()),
                })
            }
            Err(Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Cached-mode PUT (§7) — reached only for a bucket whose namespace is ready, since a restoring
    /// one runs the durable bracket above. Ack on the cache body write, with the bare-`K` pending marker handed to
    /// the marker queue behind it; the reconcile sweep uploads to the remote asynchronously. A
    /// conditional PUT holds K's write lock across resolve → evaluate → commit; an unconditional one
    /// takes **no** lock — it races on the cache (S3 last-writer-wins) and is fenced against eviction
    /// by §8's remote-generation confirm, not the lock. The cache computes `MD5(plaintext)` natively
    /// as the ETag, and validates a forwarded `Content-MD5` (⇒ `BadDigest`) before storing.
    async fn op_put_object_cached(
        &self,
        mut input: PutObjectInput,
        bucket: String,
        key: String,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let storage_class = resolve_storage_class(input.storage_class.as_ref())?;
        // Validate the digest shape up front (bad base64/length ⇒ InvalidDigest), then forward the
        // raw header to the cache, which validates it against the body atomically.
        if let Some(h) = input.content_md5.as_deref() {
            parse_content_md5(h)?;
        }
        let content_md5 = input.content_md5.clone();
        let (plen, body) = require_inline_body(&mut input)?;
        let md = write_metadata(
            input.metadata.as_ref(),
            &storage_class,
            input.content_type.as_deref(),
        );

        let conditional = input.if_match.is_some() || input.if_none_match.is_some();
        let etag = if conditional {
            // The lock covers resolve → evaluate → commit → marker (§4), the linearization point.
            let _guard = self.write_lock(&bucket, &key).await;
            let current = self.resolve_current_client_etag(&bucket, &key).await?;
            evaluate_precondition(
                input.if_match.as_ref(),
                input.if_none_match.as_ref(),
                current.as_deref(),
            )?;
            self.commit_cached(&bucket, &key, body, plen, md, content_md5)
                .await?
        } else {
            self.commit_cached(&bucket, &key, body, plen, md, content_md5)
                .await?
        };

        super::record_bytes(plen);
        self.gc.touch(&bucket, &key, Plaintext::AtKey);
        // This write replaced whatever K held; if that was a rehydrated composite, its shadow is now
        // unreachable (§8). Unconditional here because the unconditional branch above never read K and
        // so cannot know — the actor resolves it.
        self.orphans.owe(&bucket, &key);
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        }))
    }

    /// Land a cached-mode PUT: plaintext body to `<data>` at K (native ETag), which **is** the
    /// commit — the caller acks on it. The pending marker at bare `K` is then handed to the marker
    /// queue (§7, [`crate::markers`]) rather than written here: the ack cannot depend on it, because
    /// a marker failure has no honest error to return once the body is live and client-visible.
    /// Returns the client ETag.
    async fn commit_cached(
        &self,
        bucket: &str,
        key: &str,
        body: StreamingBlob,
        plen: u64,
        md: HashMap<String, String>,
        content_md5: Option<String>,
    ) -> S3Result<String> {
        let body = codec::blob_to_bytestream(body);

        let out = self
            .data()
            .put(
                bucket,
                key,
                body,
                Some(plen as i64),
                md,
                content_md5,
                None,
                None,
                None,
            )
            .await
            .map_err(|e| match e {
                // The cache validated the digest against the body and refused it, so nothing landed
                // — a clean client error, and the one failure here that is *not* indeterminate.
                Error::BadDigest => {
                    s3_error!(BadDigest, "Content-MD5 does not match the request body")
                }
                other => {
                    // Everything else may have landed and lost its response, leaving K live with no
                    // marker queued behind it. This run can no longer claim the bucket's pending set
                    // is a complete account of itself, so it withdraws the claim (§6) — no clean
                    // marker, and the next run's R2 rebuilds the set from both namespaces.
                    self.buckets.unaccount(bucket);
                    other.into()
                }
            })?;
        let etag = out
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();

        self.markers.owe(bucket, key, etag.clone());
        Ok(etag)
    }
}

/// The single-part body both PUT branches commit, taken out of `input`. Durable and cached mode
/// validate it identically: a `Content-Length` is mandatory and bounds the request to what an inline
/// commit can carry.
fn require_inline_body(input: &mut PutObjectInput) -> S3Result<(u64, StreamingBlob)> {
    let plen = input
        .content_length
        .filter(|&n| n >= 0)
        .ok_or_else(|| Error::Invalid("PutObject requires Content-Length".into()))?
        as u64;
    if plen > MAX_INLINE_PLAINTEXT {
        return Err(s3_error!(
            EntityTooLarge,
            "PutObject bodies over 4 GiB must use multipart upload"
        ));
    }
    let body = input
        .body
        .take()
        .ok_or_else(|| Error::Invalid("PutObject requires a body".into()))?;
    Ok((plen, body))
}

/// Reject a body equal to one of hypha's reserved 16-byte tombstone sentinels (§6), handing the
/// buffered bytes back as the body otherwise. Only a 16-byte body can collide, so the caller gates
/// on that and nothing larger is ever buffered.
async fn reject_sentinel_body(body: StreamingBlob) -> S3Result<StreamingBlob> {
    let bytes = codec::blob_to_bytestream(body)
        .collect()
        .await
        .map_err(|e| Error::Backend(format!("reading PutObject body: {e}")))?
        .into_bytes();
    if meta::is_reserved_sentinel(bytes.as_ref()) {
        return Err(s3_error!(
            InvalidRequest,
            "body collides with a reserved hypha sentinel value"
        ));
    }
    Ok(codec::bytestream_to_blob(ByteStream::from(bytes.to_vec())))
}

/// `current_etag` is the client-visible ETag of whatever is at K now; `None` ⇒ K is client-visibly
/// absent, which no condition can match (§4).
pub(super) fn evaluate_precondition(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    current_etag: Option<&str>,
) -> Result<(), Error> {
    let satisfied = |cond: &ETagCondition| match cond {
        ETagCondition::Any => current_etag.is_some(),
        ETagCondition::ETag(e) => current_etag
            .map(|c| c.trim_matches('"') == e.value().trim_matches('"'))
            .unwrap_or(false),
    };
    if let Some(cond) = if_match {
        if !satisfied(cond) {
            return Err(Error::PreconditionFailed);
        }
    }
    if let Some(cond) = if_none_match {
        if satisfied(cond) {
            return Err(Error::PreconditionFailed);
        }
    }
    Ok(())
}
