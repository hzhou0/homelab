//! Single-object PUT. Durable mode is the §7 transition bracket — precondition → mark →
//! commit → settle, all under K's write lock — with the remote op as the commit point.
//!
//! The commit must land the body *and* its facts at K atomically (a committed object without
//! facts would be indistinguishable from a foreign write and unrecoverable without decrypting).
//! `cetag = MD5(plaintext)` is only known after the body has streamed, so the facts travel as
//! the **footer behind the ciphertext** (§6) — computed inline and framed into the same single
//! streaming `PutObject`. K is marked for the transfer's duration; readers of K meanwhile
//! resolve from the remote, which atomically holds the old object until the PUT completes.

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use std::collections::HashMap;

use hypha_core::config::Mode;
use hypha_core::error::Error;
use hypha_core::meta;

use super::{Hypha, MAX_INLINE_PLAINTEXT};
use crate::codec;
use crate::tier;

impl Hypha {
    pub(super) async fn op_put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        let key = input.key.clone();
        meta::validate_client_key(&key).map_err(|e| Error::Invalid(e.to_string()))?;

        if self.mode != Mode::Durable {
            // Cached (async write-through) path lands in Phase 4.
            return Err(s3_error!(NotImplemented, "cached-mode PutObject pending"));
        }

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
            .ok_or_else(|| Error::Invalid("PutObject requires a body".into()))?;

        // One lock for the whole bracket: precondition → mark → commit → settle (§4).
        let _guard = self.tier.locks.lock(&key).await;

        // Resolve the key's *current* client-visible ETag for the conditional-write check: a live
        // body reports it natively, a tombstone carries it in metadata, an absent key has none,
        // and a leftover transition mark is repaired first (§7 — the marking writer held this
        // lock, so a mark seen here is always a crash leftover).
        let current_etag = match self.cache().head(&key).await {
            Ok(head) => {
                let md = head.metadata.clone().unwrap_or_default();
                match meta::tomb_kind(&md) {
                    Some(meta::TombKind::Transit) => {
                        self.tier.repair_locked(&key).await?.map(|f| f.cetag)
                    }
                    Some(meta::TombKind::Evict) => md.get(meta::CETAG).cloned(),
                    Some(meta::TombKind::Delete) => None,
                    None => head
                        .e_tag
                        .as_deref()
                        .map(|e| e.trim_matches('"').to_string()),
                }
            }
            Err(Error::NotFound) => None,
            Err(e) => return Err(e.into()),
        };
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
        self.tier.mark_transit_locked(&key).await?;
        let (framed_len, enc, etag_rx) =
            match codec::encrypt_blob_with_etag(self.env(), body, plen, Some(mtime_ms)).await {
                Ok(v) => v,
                Err(e) => {
                    let _ = self.tier.repair_locked(&key).await;
                    return Err(Error::Io(e).into());
                }
            };
        if let Err(e) = self
            .remote()
            .put(&key, enc, Some(framed_len as i64), HashMap::new(), None, None)
            .await
        {
            let _ = self.tier.repair_locked(&key).await;
            return Err(e.into());
        }
        // The PUT consumed the whole framed body, footer included — the etag is ready. Its loss
        // means the encrypt task died mid-commit; repair settles K from the remote either way.
        let etag = match etag_rx.await {
            Ok(e) => e,
            Err(_) => {
                let _ = self.tier.repair_locked(&key).await;
                return Err(Error::Backend("MD5 task dropped before completing".into()).into());
            }
        };
        self.tier
            .settle_evict_locked(&key, plen, &etag, mtime_ms)
            .await?;

        let resp = PutObjectOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }
}

/// Decide whether a conditional PUT may proceed against the key's current state (§4).
///
/// `current_etag` is the client-visible ETag of whatever is at K now (`None` ⇒ K is
/// client-visibly absent). `if_match` / `if_none_match` are s3s's parsed condition:
/// `ETagCondition::Any` is the `*` wildcard, `ETagCondition::ETag(e)` a specific tag (compare
/// against `current_etag` via `e.value()`). Return `Err(Error::PreconditionFailed)` to reject.
fn evaluate_precondition(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    current_etag: Option<&str>,
) -> Result<(), Error> {
    if let Some(cond) = if_match {
        let exists = match cond {
            ETagCondition::Any => current_etag.is_some(),
            ETagCondition::ETag(e) => current_etag
                .map(|c| c.trim_matches('"') == e.value().trim_matches('"'))
                .unwrap_or(false),
        };
        if !exists {
            return Err(Error::PreconditionFailed);
        }
    }
    if let Some(cond) = if_none_match {
        let exists = match cond {
            ETagCondition::Any => current_etag.is_some(),
            ETagCondition::ETag(e) => current_etag
                .map(|c| c.trim_matches('"') == e.value().trim_matches('"'))
                .unwrap_or(false),
        };
        if exists {
            return Err(Error::PreconditionFailed);
        }
    }
    Ok(())
}
