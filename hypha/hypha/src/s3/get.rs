//! GET, cache-first, dispatched on K's classification (§7):
//!
//! - **Live body** — plaintext served straight from the cache (ranges forwarded).
//! - **Eviction tombstone** — facts from its metadata; body decrypted from the remote. Durable
//!   mode never repopulates the cache (the body would immediately be tombstoned again).
//! - **Delete tombstone** — client-visible 404.
//! - **Transition mark** — remote-as-truth: always a crash leftover *or* a commit in flight, so
//!   facts and bytes both come from the remote (no hybrid reads). If K's lock is free the mark is
//!   a leftover and is repaired opportunistically; a held lock means the writer is alive and the
//!   read serves the remote's current state without queuing.
//!
//! A composite (client ETag `hash-N`) is a concatenation of pure per-part age files followed by
//! the terminating trailer part. Its part boundaries and per-part plaintext lengths come from the
//! **parts table in the object's own trailer** (§6), recovered in the one speculative tail read
//! that also yields the facts — no remote part-index calls, no per-part header probes. A whole-
//! object read then decrypts every part from a single `[0, body_ct_len)` GET; a range read fetches
//! only the parts it touches.

use std::ops::Range as ByteRange;

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;
use hypha_format::SINGLE_TRAILER_LEN;

use super::{ts_ms, Hypha};
use crate::codec::{self, PartSegment};
use crate::tier::RemoteFacts;

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

        let head = self.cache().head(&bucket, &key).await?;
        let md = head.metadata.clone().unwrap_or_default();

        match meta::tomb_kind(&md) {
            Some(meta::TombKind::Delete) => Err(Error::NotFound.into()),
            Some(meta::TombKind::Evict) => {
                let facts = facts_from_tombstone(&key, &md)?;
                self.serve_remote(&bucket, &key, &input, &facts, &md).await
            }
            Some(meta::TombKind::Transit) => match self.resolve_transit(&bucket, &key).await? {
                None => Err(Error::NotFound.into()),
                Some(facts) => self.serve_remote(&bucket, &key, &input, &facts, &md).await,
            },
            None => self.serve_cache_body(&bucket, &key, &input, &md).await,
        }
    }

    /// Resolve a transition-marked K from the remote (§7): repair it if its lock is free (crash
    /// leftover), else read through to the remote's current state. `None` ⇒ K is absent there.
    pub(super) async fn resolve_transit(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<RemoteFacts>, Error> {
        if let Some(_guard) = self.tier.locks.try_lock(key) {
            return self.tier.repair_locked(bucket, key).await;
        }
        match self.remote().head(bucket, key).await {
            Ok(h) => Ok(Some(self.tier.remote_facts(bucket, key, &h).await?)),
            Err(Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Live plaintext body in the cache.
    async fn serve_cache_body(
        &self,
        bucket: &str,
        key: &str,
        input: &GetObjectInput,
        md: &std::collections::HashMap<String, String>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let out = self
            .cache()
            .get(bucket, key, input.range.as_ref().map(range_header))
            .await?;
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

    /// Serve a remote-only object (tombstoned or mid-bracket) by decrypting from the remote (§6).
    /// No cache repopulation — durable mode never restores.
    async fn serve_remote(
        &self,
        bucket: &str,
        key: &str,
        input: &GetObjectInput,
        facts: &RemoteFacts,
        md: &std::collections::HashMap<String, String>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let plen = facts.plen;
        let etag = Some(ETag::Strong(facts.cetag.clone()));
        let last_modified = Some(ts_ms(facts.mtime_ms));
        let metadata = Some(meta::decode_user_metadata(md));
        let storage_class = Some(StorageClass::from(meta::storage_class(md)));
        let pt = match &input.range {
            None => None,
            Some(range) => Some(plaintext_range(range, plen)?),
        };

        let body = if meta::is_composite_etag(&facts.cetag) {
            // The trailer's parts table (recovered in one tail read) gives every part's ciphertext
            // window and plaintext length — no remote part-index calls.
            let tail = self.tier.read_tail(bucket, key).await?.ok_or_else(|| {
                Error::Backend(format!("composite {key:?} carries no hypha trailer"))
            })?;
            match &pt {
                // Whole object: one GET of the concatenated parts, decrypted part-by-part in-stream.
                None => {
                    let out = self
                        .remote()
                        .get(
                            bucket,
                            key,
                            Some(format!("bytes=0-{}", tail.body_ct_len - 1)),
                        )
                        .await?;
                    let part_lens = tail.windows.iter().map(|w| w.end - w.start).collect();
                    codec::decrypt_composite_full(self.env(), out.body, part_lens)
                }
                // Range: fetch only the parts it touches.
                Some(pt) => {
                    let segments = composite_segments(&tail.windows, &tail.plens, pt);
                    codec::decrypt_composite(
                        self.env(),
                        self.remote().clone(),
                        bucket.to_string(),
                        key.to_string(),
                        segments,
                    )
                }
            }
        } else {
            match &pt {
                None => {
                    let out = self.remote().get(bucket, key, None).await?;
                    let ct_len = envelope_len(key, out.content_length)?;
                    codec::decrypt_full(self.env(), out.body, ct_len)
                }
                Some(pt) => {
                    let rhead = self.remote().head(bucket, key).await?;
                    let ct_len = envelope_len(key, rhead.content_length)?;
                    codec::decrypt_range(
                        self.env(),
                        self.remote().clone(),
                        bucket.to_string(),
                        key.to_string(),
                        ct_len,
                        pt.clone(),
                    )
                }
            }
        };

        let resp = match pt {
            None => GetObjectOutput {
                body: Some(body),
                content_length: Some(plen as i64),
                e_tag: etag,
                last_modified,
                metadata,
                storage_class,
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
}

/// Resolve a plaintext range against a composite's parts (§7): with per-part windows and plaintext
/// lengths already in hand (from the trailer's parts table), clip the parts that cover `pt`.
fn composite_segments(
    windows: &[ByteRange<u64>],
    plens: &[u64],
    pt: &ByteRange<u64>,
) -> Vec<PartSegment> {
    let mut segs = Vec::new();
    let mut acc = 0u64;
    for (w, &p) in windows.iter().zip(plens) {
        if acc >= pt.end {
            break;
        }
        segs.extend(clip(w, acc, p, pt));
        acc += p;
    }
    segs
}

/// The age-envelope length of a single-part remote object: its Content-Length minus the tail
/// trailer, which must never reach the decryptor (§6).
fn envelope_len(key: &str, content_length: Option<i64>) -> Result<u64, Error> {
    let framed = content_length
        .filter(|&n| n >= 0)
        .ok_or_else(|| Error::Backend("remote response missing content-length".into()))?
        as u64;
    framed
        .checked_sub(SINGLE_TRAILER_LEN as u64)
        .ok_or_else(|| Error::Backend(format!("remote object {key:?} shorter than a trailer")))
}

/// The segment (if any) part `w` contributes to plaintext range `pt`, given the part's plaintext
/// starts at `start_pt` and holds `part_plen` bytes.
fn clip(
    w: &ByteRange<u64>,
    start_pt: u64,
    part_plen: u64,
    pt: &ByteRange<u64>,
) -> Option<PartSegment> {
    let lo = pt.start.max(start_pt);
    let hi = pt.end.min(start_pt + part_plen);
    if lo >= hi {
        return None;
    }
    if lo == start_pt && hi == start_pt + part_plen {
        Some(PartSegment::Whole(w.clone()))
    } else {
        Some(PartSegment::Partial {
            ct: w.clone(),
            pt: (lo - start_pt)..(hi - start_pt),
        })
    }
}

/// Plaintext facts off an eviction tombstone's own metadata (§6) — the authoritative copy.
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
    // hypha writes MTIME on every eviction tombstone (§6), so — like plen/cetag above — a missing
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

/// Reconstruct the HTTP Range header value the client sent, to forward to the cache.
fn range_header(range: &Range) -> String {
    match *range {
        Range::Int {
            first,
            last: Some(last),
        } => format!("bytes={first}-{last}"),
        Range::Int { first, last: None } => format!("bytes={first}-"),
        Range::Suffix { length } => format!("bytes=-{length}"),
    }
}

/// Resolve an HTTP Range against the plaintext length to a half-open `[start, end)`.
fn plaintext_range(range: &Range, plen: u64) -> Result<ByteRange<u64>, Error> {
    match *range {
        Range::Int { first, last } => {
            if first >= plen {
                return Err(Error::Invalid("range start beyond object length".into()));
            }
            let end = last.map(|l| l.saturating_add(1).min(plen)).unwrap_or(plen);
            Ok(first..end)
        }
        Range::Suffix { length } => Ok(plen.saturating_sub(length)..plen),
    }
}
