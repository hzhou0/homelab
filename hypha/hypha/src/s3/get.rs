//! Cache-first GET with authenticated remote fallback and range-aware composite decryption.

use std::ops::Range as ByteRange;

use s3s::dto::*;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use hypha_core::config::Mode;
use hypha_core::error::Error;
use hypha_core::meta;

use super::overlay::KeyState;
use super::{ts_ms, Hypha};
use crate::background;
use crate::codec;
use crate::tier::{shadow_is_generation, RemoteFacts};

impl Hypha {
    pub(super) async fn op_get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        if meta::validate_client_key(&key).is_err() {
            return Err(Error::NotFound.into());
        }

        match self.resolve_key(&bucket, &key).await? {
            KeyState::Absent => Err(Error::NotFound.into()),
            KeyState::Remote { facts, md } => {
                // Cached mode promotes an eviction-tombstoned read back into the cache : probe the
                // shadow for a composite, else serve from the remote and kick an async rehydrate. A
                // transition mark or a restoring bucket resolves from the remote with no rehydrate.
                if self.mode == Mode::Cached && meta::tomb_kind(&md) == Some(meta::TombKind::Evict)
                {
                    self.serve_evicted_cached(&bucket, &key, &input, &facts, &md)
                        .await
                } else {
                    self.serve_remote(&bucket, &key, &input, &facts, &md).await
                }
            }
            KeyState::CacheBody { head, md } => {
                let plen = head.content_length.unwrap_or_default().max(0) as u64;
                self.serve_cache_body(&bucket, &key, &input, plen, &md)
                    .await
            }
        }
    }

    /// Serve an eviction-tombstoned key in cached mode, rehydrating . A composite is probed in the
    /// shadow body first and served on a verified hit; on a miss — or for a single-part object — the
    /// read is served from the remote and a rehydrate is queued on the background actor
    /// ([`background`]) to land the plaintext (single-part into K, composite into the shadow)
    /// so the next read is a cache hit. Submitting is a map insert plus a `try_send` and never
    /// blocks: the read owes the client bytes, not a warm cache.
    async fn serve_evicted_cached(
        &self,
        bucket: &str,
        key: &str,
        input: &GetObjectInput,
        facts: &RemoteFacts,
        md: &std::collections::HashMap<String, String>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        if meta::is_composite_etag(&facts.cetag) {
            if let Some(resp) = self.try_serve_shadow(bucket, key, input, facts, md).await? {
                return Ok(resp);
            }
        }
        self.background.submit(background::Transition::Rehydrate {
            bucket: bucket.to_string(),
            key: key.to_string(),
            cetag: facts.cetag.clone(),
            plen: facts.plen,
        });
        self.serve_remote(bucket, key, input, facts, md).await
    }

    /// Serve a rehydrated composite from its shadow body, or `None` if there is no shadow of this
    /// generation. The shadow holds the full plaintext, so a range maps straight through; the
    /// client-visible facts (composite ETag, plen, mtime, pass-through metadata) come from K's
    /// tombstone, not the shadow's native fields.
    async fn try_serve_shadow(
        &self,
        bucket: &str,
        key: &str,
        input: &GetObjectInput,
        facts: &RemoteFacts,
        md: &std::collections::HashMap<String, String>,
    ) -> S3Result<Option<S3Response<GetObjectOutput>>> {
        if let Some(range) = &input.range {
            plaintext_range(range, facts.plen)?;
        }
        let shadow = meta::shadow_key(key);
        let out = match self
            .meta()
            .get(bucket, &shadow, input.range.as_ref().map(range_header))
            .await
        {
            Ok(o) => o,
            Err(Error::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // The shadow key is the whole digest of K, so a hit is K's by construction — only the
        // generation is in question. A superseded shadow (K overwritten by a newer composite) misses
        // here and falls through to the remote, which re-rehydrates; without the cetag gate it would
        // serve the old bytes under the new ETag/length.
        if !shadow_is_generation(out.metadata(), &facts.cetag) {
            return Ok(None);
        }
        resolved(true, facts.plen);

        let ranged = input.range.is_some();
        let resp = GetObjectOutput {
            content_length: if ranged {
                out.content_length
            } else {
                Some(facts.plen as i64)
            },
            content_range: out.content_range,
            e_tag: Some(ETag::Strong(facts.cetag.clone())),
            last_modified: Some(ts_ms(facts.mtime_ms)),
            body: Some(codec::bytestream_to_blob(out.body)),
            metadata: Some(meta::decode_user_metadata(md)),
            storage_class: Some(StorageClass::from(meta::storage_class(md))),
            content_type: meta::content_type(md),
            accept_ranges: Some("bytes".to_string()),
            ..Default::default()
        };
        Ok(Some(if ranged {
            S3Response::with_status(resp, hyper::StatusCode::PARTIAL_CONTENT)
        } else {
            S3Response::new(resp)
        }))
    }

    /// Resolve a transition-marked K from the remote : repair it if its lock is free (crash
    /// leftover), else read through to the remote's current state. `None` ⇒ K is absent there.
    pub(super) async fn resolve_transit(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<RemoteFacts>, Error> {
        if let Some(_guard) = self.tier.write_locks.try_lock(bucket, key) {
            return self.tier.repair_locked(bucket, key).await;
        }
        match self.remote().head(bucket, key).await {
            Ok(h) => Ok(Some(self.tier.remote_facts(bucket, key, &h).await?)),
            Err(Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn serve_cache_body(
        &self,
        bucket: &str,
        key: &str,
        input: &GetObjectInput,
        plen: u64,
        md: &std::collections::HashMap<String, String>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        if let Some(range) = &input.range {
            plaintext_range(range, plen)?;
        }
        let out = self
            .data()
            .get(bucket, key, input.range.as_ref().map(range_header))
            .await?;
        resolved(true, out.content_length.unwrap_or_default().max(0) as u64);
        let status = if input.range.is_some() {
            Some(hyper::StatusCode::PARTIAL_CONTENT)
        } else {
            None
        };
        let resp = GetObjectOutput {
            content_length: out.content_length,
            content_range: out.content_range,
            e_tag: out
                .e_tag
                .map(|e| ETag::Strong(e.trim_matches('"').to_string())),
            last_modified: out
                .last_modified
                .and_then(|t| t.to_millis().ok())
                .map(ts_ms),
            body: Some(codec::bytestream_to_blob(out.body)),
            metadata: Some(meta::decode_user_metadata(md)),
            storage_class: Some(StorageClass::from(meta::storage_class(md))),
            content_type: meta::content_type(md),
            accept_ranges: Some("bytes".to_string()),
            ..Default::default()
        };
        Ok(S3Response {
            output: resp,
            status,
            headers: Default::default(),
            extensions: Default::default(),
        })
    }

    /// Serve a remote-only object (tombstoned or mid-bracket) by decrypting from the remote ;
    /// durable mode never repopulates the cache here.
    async fn serve_remote(
        &self,
        bucket: &str,
        key: &str,
        input: &GetObjectInput,
        facts: &RemoteFacts,
        md: &std::collections::HashMap<String, String>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        // The miss is counted here rather than at the classification above, because this is where a
        // read *becomes* one: a tombstoned key whose shadow answered never reaches this path.
        resolved(false, facts.plen);
        let plen = facts.plen;
        let etag = Some(ETag::Strong(facts.cetag.clone()));
        let last_modified = Some(ts_ms(facts.mtime_ms));
        let metadata = Some(meta::decode_user_metadata(md));
        let storage_class = Some(StorageClass::from(meta::storage_class(md)));
        let content_type = meta::content_type(md);
        let pt = match &input.range {
            None => None,
            Some(range) => Some(plaintext_range(range, plen)?),
        };

        let body = self
            .tier
            .decrypt_remote_body(bucket, key, &facts.cetag, pt.clone())
            .await?;

        let resp = match pt {
            None => GetObjectOutput {
                body: Some(body),
                content_length: Some(plen as i64),
                e_tag: etag,
                last_modified,
                metadata,
                storage_class,
                content_type: content_type.clone(),
                accept_ranges: Some("bytes".to_string()),
                ..Default::default()
            },
            Some(pt) => GetObjectOutput {
                body: Some(body),
                content_length: Some((pt.end - pt.start) as i64),
                content_range: Some(format!("bytes {}-{}/{}", pt.start, pt.end - 1, plen)),
                e_tag: etag,
                last_modified,
                metadata,
                storage_class,
                content_type,
                accept_ranges: Some("bytes".to_string()),
                ..Default::default()
            },
        };
        if resp.content_range.is_some() {
            Ok(S3Response::with_status(
                resp,
                hyper::StatusCode::PARTIAL_CONTENT,
            ))
        } else {
            Ok(S3Response::new(resp))
        }
    }

    /// **GetObjectAttributes** : a read projection over the same key-state dispatch as HEAD.
    /// `ObjectParts` for a composite comes straight off the trailer's offset table (one bounded
    /// MAC-verified tail GET, no remote part index). `Checksum` is deferred .
    pub(super) async fn op_get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let key = input.key.clone();
        if meta::validate_client_key(&key).is_err() {
            return Err(Error::NotFound.into());
        }

        let (facts, md) = match self.resolve_key(&bucket, &key).await? {
            KeyState::Absent => return Err(Error::NotFound.into()),
            KeyState::Remote { facts, md } => (facts, md),
            KeyState::CacheBody { head, md } => (RemoteFacts::from_cache_head(&head), md),
        };
        let storage_class = meta::storage_class(&md);

        let want = |name: &str| input.object_attributes.iter().any(|a| a.as_str() == name);

        // Sizes are the per-part *plaintext* lengths from the trailer's table ; the parts
        // paginate like ListParts.
        let object_parts =
            if want(ObjectAttributes::OBJECT_PARTS) && meta::is_composite_etag(&facts.cetag) {
                let Some(tail) = self.tier.read_tail(&bucket, &key).await? else {
                    self.tier.halt.foreign_object(&bucket, &key).await
                };
                Some(build_object_parts(
                    &tail.plens,
                    input.part_number_marker,
                    input.max_parts,
                ))
            } else {
                None
            };

        let resp = GetObjectAttributesOutput {
            // Quoted here though AWS sends this one unquoted: s3s 0.14.1 quotes every ETag DTO value
            // uniformly, an upstream bug (Nugine/s3s#629, fixed for 0.15.0). Harmless — every S3
            // client trims quotes — drop this note on the s3s bump.
            e_tag: want(ObjectAttributes::ETAG).then(|| ETag::Strong(facts.cetag.clone())),
            object_size: want(ObjectAttributes::OBJECT_SIZE).then_some(facts.plen as i64),
            storage_class: want(ObjectAttributes::STORAGE_CLASS)
                .then(|| StorageClass::from(storage_class)),
            object_parts,
            last_modified: Some(ts_ms(facts.mtime_ms)),
            // No versioning, so never a delete marker; Checksum deferred .
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }
}

/// Paginated like `ListParts`.
fn build_object_parts(
    plens: &[u64],
    part_number_marker: Option<i32>,
    max_parts: Option<i32>,
) -> GetObjectAttributesParts {
    let after = part_number_marker.unwrap_or(0);
    let max = max_parts.unwrap_or(1000).max(0) as usize;
    let mut parts: Vec<ObjectPart> = plens
        .iter()
        .enumerate()
        .map(|(i, &plen)| (i as i32 + 1, plen))
        .filter(|(n, _)| *n > after)
        .map(|(n, plen)| ObjectPart {
            part_number: Some(n),
            size: Some(plen as i64),
            ..Default::default()
        })
        .collect();
    let is_truncated = parts.len() > max;
    parts.truncate(max);
    GetObjectAttributesParts {
        total_parts_count: Some(plens.len() as i32),
        part_number_marker: Some(after),
        max_parts: Some(max as i32),
        is_truncated: Some(is_truncated),
        next_part_number_marker: is_truncated
            .then(|| parts.last().and_then(|p| p.part_number))
            .flatten(),
        parts: Some(parts),
    }
}

/// Where a read resolved, and how much it will return — reported to both surfaces at once,
/// because the two are the same statement and a call site that made only one of them would drift.
fn resolved(cache_hit: bool, bytes: u64) {
    crate::metrics::cache_read(cache_hit);
    super::record_cache_hit(cache_hit);
    super::record_bytes(bytes);
}

/// Plaintext facts off an eviction tombstone's own metadata  — the authoritative copy.
pub(super) fn facts_from_tombstone(
    key: &str,
    md: &std::collections::HashMap<String, String>,
) -> Result<RemoteFacts, Error> {
    let plen = md
        .get(meta::PLEN)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Backend(format!("tombstone for {key:?} missing plen")))?;
    let cetag = md
        .get(meta::CETAG)
        .cloned()
        .ok_or_else(|| Error::Backend(format!("tombstone for {key:?} missing cetag")))?;
    // hypha writes MTIME on every eviction tombstone , so — like plen/cetag above — a missing
    // or unparseable value is a corrupt tombstone, not a defaultable optional.
    let mtime_ms = md
        .get(meta::MTIME)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Backend(format!("tombstone for {key:?} missing mtime")))?;
    Ok(RemoteFacts {
        plen,
        cetag,
        mtime_ms,
    })
}

fn range_header(range: &Range) -> String {
    match *range {
        Range::Int {
            first,
            last: Some(last),
        } => format!("bytes={first}-{last}"),
        Range::Int { first, .. } => format!("bytes={first}-"),
        Range::Suffix { length } => format!("bytes=-{length}"),
    }
}

fn plaintext_range(range: &Range, plen: u64) -> S3Result<ByteRange<u64>> {
    if plen == 0 {
        return Err(s3_error!(
            InvalidRange,
            "range cannot select an empty object"
        ));
    }
    range
        .check(plen)
        .map_err(|_| s3_error!(InvalidRange, "requested range is not satisfiable"))
}
