//! Single-object PUT. Routes **through** the cache: the client's plaintext is written to the
//! cache (which computes the S3 ETag natively), then — in `sync` mode — encrypted and uploaded to
//! the remote and tombstoned inline before the ack (§4/§7, unified tiering design). The whole
//! finalize holds the key lock so concurrent same-key PUTs can't interleave or reorder.

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use std::collections::HashMap;

use hypha_core::config::Mode;
use hypha_core::error::Error;
use hypha_core::meta;

use super::Hypha;
use crate::codec;

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
        let body = input
            .body
            .ok_or_else(|| Error::Invalid("PutObject requires a body".into()))?;

        // One lock for the whole sequence: precondition → cache write → upload → tombstone.
        let _guard = self.tier.locks.lock(&key).await;

        // Resolve the key's *current* client-visible ETag for the conditional-write check: a live
        // body reports it natively; a tombstone carries it in metadata; an absent key has none.
        let current_etag = match self.cache().head(&key).await {
            Ok(head) => current_client_etag(&head.metadata, head.e_tag.as_deref()),
            Err(Error::NotFound) => None,
            Err(e) => return Err(e.into()),
        };
        evaluate_precondition(
            input.if_match.as_ref(),
            input.if_none_match.as_ref(),
            current_etag.as_deref(),
        )?;

        // Encrypt the request body directly — durable mode never writes plaintext to cache. The
        // client ETag (MD5 of the plaintext) is computed inline alongside the encryption.
        let (ct_len, enc, etag_rx) = codec::encrypt_blob_with_etag(self.env(), body, plen)
            .await
            .map_err(Error::Io)?;
        let mut remote_md = HashMap::new();
        remote_md.insert(meta::PLEN.to_string(), plen.to_string());
        self.remote()
            .put(&key, enc, Some(ct_len as i64), remote_md, None, None)
            .await?;
        // Remote PUT consumed the body; the MD5 task has finished — etag is ready.
        let etag = etag_rx
            .await
            .map_err(|_| Error::Backend("MD5 task dropped before completing".into()))?;

        self.tier.tombstone_with_facts_locked(&key, plen, &etag).await?;

        let resp = PutObjectOutput {
            e_tag: client_etag_from_raw(&etag),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }
}

/// The current object's client-visible ETag, whether it is a live body (native cache ETag) or a
/// tombstone (client ETag in metadata) (§4). `None` ⇒ absent.
fn current_client_etag(
    metadata: &Option<HashMap<String, String>>,
    native_etag: Option<&str>,
) -> Option<String> {
    if let Some(md) = metadata {
        match md.get(meta::TOMB).map(String::as_str) {
            Some(meta::TOMB_EVICT) => return md.get(meta::CETAG).cloned(),
            // Delete-tombstone is client-visibly absent.
            Some(meta::TOMB_DELETE) => return None,
            _ => {}
        }
    }
    native_etag.map(|e| e.trim_matches('"').to_string())
}

fn client_etag_from_raw(raw: &str) -> Option<ETag> {
    Some(ETag::Strong(raw.trim_matches('"').to_string()))
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
