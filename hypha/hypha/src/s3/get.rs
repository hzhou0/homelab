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
//! the terminating footer part; its part boundaries are **derived at read time** from the
//! remote's own part index (`GetObjectAttributes` ObjectParts, falling back to
//! `HEAD partNumber=n`), and per-part plaintext lengths fall out of the closed form once one
//! header-prefix read supplies `hlen` (§6) — hypha stores no part table.

use std::ops::Range as ByteRange;

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;
use hypha_format::offset::{parse_header_len, plaintext_len_from};
use hypha_format::FOOTER_LEN;

use super::{ts_ms, Hypha};
use crate::codec::{self, PartSegment};
use crate::tier::RemoteFacts;

impl Hypha {
    pub(super) async fn op_get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let key = input.key.clone();
        if meta::validate_client_key(&key).is_err() {
            return Err(Error::NotFound.into());
        }

        let head = self.cache().head(&key).await?;
        let md = head.metadata.clone().unwrap_or_default();

        match meta::tomb_kind(&md) {
            Some(meta::TombKind::Delete) => Err(Error::NotFound.into()),
            Some(meta::TombKind::Evict) => {
                let facts = facts_from_tombstone(&key, &md)?;
                self.serve_remote(&key, &input, &facts).await
            }
            Some(meta::TombKind::Transit) => match self.resolve_transit(&key).await? {
                None => Err(Error::NotFound.into()),
                Some(facts) => self.serve_remote(&key, &input, &facts).await,
            },
            None => self.serve_cache_body(&key, &input).await,
        }
    }

    /// Resolve a transition-marked K from the remote (§7): repair it if its lock is free (crash
    /// leftover), else read through to the remote's current state. `None` ⇒ K is absent there.
    pub(super) async fn resolve_transit(&self, key: &str) -> Result<Option<RemoteFacts>, Error> {
        if let Some(_guard) = self.tier.locks.try_lock(key) {
            return self.tier.repair_locked(key).await;
        }
        match self.remote().head(key).await {
            Ok(h) => Ok(Some(self.tier.remote_facts(key, &h).await?)),
            Err(Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Live plaintext body in the cache.
    async fn serve_cache_body(
        &self,
        key: &str,
        input: &GetObjectInput,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let out = self
            .cache()
            .get(key, input.range.as_ref().map(range_header))
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
        key: &str,
        input: &GetObjectInput,
        facts: &RemoteFacts,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let plen = facts.plen;
        let etag = Some(ETag::Strong(facts.cetag.clone()));
        let last_modified = Some(ts_ms(facts.mtime_ms));
        let pt = match &input.range {
            None => None,
            Some(range) => Some(plaintext_range(range, plen)?),
        };

        let body = if meta::is_composite_etag(&facts.cetag) {
            let windows = self.part_windows(key).await?;
            let segments = match &pt {
                None => windows.into_iter().map(PartSegment::Whole).collect(),
                Some(pt) => self.composite_segments(key, &windows, plen, pt).await?,
            };
            codec::decrypt_composite(self.env(), self.remote().clone(), key.to_string(), segments)
        } else {
            match &pt {
                None => {
                    let out = self.remote().get(key, None).await?;
                    let ct_len = envelope_len(key, out.content_length)?;
                    codec::decrypt_full(self.env(), out.body, ct_len)
                }
                Some(pt) => {
                    let rhead = self.remote().head(key).await?;
                    let ct_len = envelope_len(key, rhead.content_length)?;
                    codec::decrypt_range(
                        self.env(),
                        self.remote().clone(),
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
                accept_ranges: Some("bytes".to_string()),
                ..Default::default()
            },
            Some(pt) => GetObjectOutput {
                body: Some(body),
                content_length: Some((pt.end - pt.start) as i64),
                content_range: Some(format!("bytes {}-{}/{}", pt.start, pt.end - 1, plen)),
                e_tag: etag,
                last_modified,
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

    /// Absolute ciphertext windows of a committed composite's **age-file parts** (the trailing
    /// 48-byte footer part is stripped — it is facts, not ciphertext), from the remote's own
    /// part index. `GetObjectAttributes` yields the whole index in one call; remotes that omit
    /// `Part` elements (§9) degrade to one `HEAD partNumber=n` per part. Derived per read —
    /// hypha stores no part table (§6).
    async fn part_windows(&self, key: &str) -> Result<Vec<ByteRange<u64>>, Error> {
        let mut sizes: Vec<(i32, u64)> = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let out = match self.remote().object_parts(key, marker.clone(), 1000).await {
                Ok(out) => out,
                // The object being gone is real; anything else (NotImplemented, access shape)
                // means the remote lacks the op — degrade to the HEAD walk below.
                Err(Error::NotFound) => return Err(Error::NotFound),
                Err(e) => {
                    tracing::debug!(key, error = %e, "GetObjectAttributes unavailable; using HEAD partNumber walk");
                    sizes.clear();
                    break;
                }
            };
            let Some(op) = out.object_parts() else { break };
            for p in op.parts() {
                if let (Some(n), Some(sz)) = (p.part_number(), p.size()) {
                    sizes.push((n, sz.max(0) as u64));
                }
            }
            if op.is_truncated() != Some(true) {
                break;
            }
            marker = op.next_part_number_marker().map(str::to_string);
            if marker.is_none() {
                break;
            }
        }

        if sizes.is_empty() {
            // Part-index-less remote: walk `HEAD partNumber=n`, count from the first response.
            let first = self.remote().head_part(key, 1).await?;
            let count = first.parts_count().unwrap_or(1).max(1);
            sizes.push((1, first.content_length().unwrap_or(0).max(0) as u64));
            for n in 2..=count {
                let h = self.remote().head_part(key, n).await?;
                sizes.push((n, h.content_length().unwrap_or(0).max(0) as u64));
            }
        }

        sizes.sort_unstable_by_key(|(n, _)| *n);
        // The last part is the terminating footer (§6) — always present on a hypha composite.
        match sizes.pop() {
            Some((_, sz)) if sz == FOOTER_LEN && !sizes.is_empty() => {}
            _ => {
                return Err(Error::Backend(format!(
                    "composite {key:?} lacks a terminating footer part"
                )))
            }
        }
        let mut windows = Vec::with_capacity(sizes.len());
        let mut off = 0u64;
        for (_, sz) in sizes {
            windows.push(off..off + sz);
            off += sz;
        }
        Ok(windows)
    }

    /// Per-part plaintext lengths, in closed form (§6): the scrypt header length is shared
    /// across a composite's parts (deterministic today — `streaming_ctlen.rs` guards it), so
    /// one header-prefix read of part 1 gives `hlen` and every part's `plen` falls out of its
    /// ciphertext length, validated by tiling to the stamped total. If age ever varies headers
    /// again the tiling check fails and the walk degrades to one header parse per part.
    async fn part_plens(
        &self,
        key: &str,
        windows: &[ByteRange<u64>],
        total_plen: u64,
    ) -> Result<Vec<u64>, Error> {
        let hlen = self.part_hlen(key, &windows[0]).await?;
        let shared: Option<Vec<u64>> = windows
            .iter()
            .map(|w| plaintext_len_from(len(w), hlen))
            .collect();
        if let Some(plens) = shared {
            if plens.iter().sum::<u64>() == total_plen {
                return Ok(plens);
            }
        }

        let mut plens = Vec::with_capacity(windows.len());
        for w in windows {
            let hlen = self.part_hlen(key, w).await?;
            plens.push(plaintext_len_from(len(w), hlen).ok_or_else(|| {
                Error::Backend(format!("part of {key:?} inconsistent with its header"))
            })?);
        }
        if plens.iter().sum::<u64>() != total_plen {
            return Err(Error::Backend(format!(
                "parts of {key:?} do not tile to the stamped plaintext length"
            )));
        }
        Ok(plens)
    }

    /// One part's header length, parsed from a small prefix read (`--- <mac>` ends the header
    /// unambiguously).
    async fn part_hlen(&self, key: &str, w: &ByteRange<u64>) -> Result<u64, Error> {
        let end = (w.start + HEADER_PROBE).min(w.end);
        let out = self
            .remote()
            .get(key, Some(format!("bytes={}-{}", w.start, end - 1)))
            .await?;
        let prefix = out
            .body
            .collect()
            .await
            .map_err(|e| Error::Backend(format!("part header read: {e}")))?
            .into_bytes();
        parse_header_len(&prefix)
            .ok_or_else(|| Error::Backend("part header MAC line not in probe window".into()))
    }

    /// Resolve a plaintext range against a composite's parts (§7): derive every part's `plen`
    /// (closed form, one small read), then clip the parts covering `pt`.
    async fn composite_segments(
        &self,
        key: &str,
        windows: &[ByteRange<u64>],
        total_plen: u64,
        pt: &ByteRange<u64>,
    ) -> Result<Vec<PartSegment>, Error> {
        let plens = self.part_plens(key, windows, total_plen).await?;
        let mut segs = Vec::new();
        let mut acc = 0u64;
        for (w, &p) in windows.iter().zip(&plens) {
            if acc >= pt.end {
                break;
            }
            segs.extend(clip(w, acc, p, pt));
            acc += p;
        }
        Ok(segs)
    }
}

/// Bytes fetched to find a part's header end. Scrypt headers are ~120 B; the probe leaves
/// generous room for future stanza growth while staying one small ranged GET.
const HEADER_PROBE: u64 = 8 * 1024;

fn len(r: &ByteRange<u64>) -> u64 {
    r.end - r.start
}

/// The age-envelope length of a single-part remote object: its Content-Length minus the inline
/// tail footer, which must never reach the decryptor (§6).
fn envelope_len(key: &str, content_length: Option<i64>) -> Result<u64, Error> {
    let framed = content_length
        .filter(|&n| n >= 0)
        .ok_or_else(|| Error::Backend("remote response missing content-length".into()))?
        as u64;
    framed
        .checked_sub(FOOTER_LEN)
        .ok_or_else(|| Error::Backend(format!("remote object {key:?} shorter than a footer")))
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
    let mtime_ms = md
        .get(meta::MTIME)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
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
