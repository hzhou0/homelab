//! Plaintext HEAD and LIST projections.
//!
//! LIST merge-joins data entries with facts twins and forwards one backend page. It does not
//! backfill short pages because doing so would weaken key-position pagination under mutation.

use std::collections::HashMap;

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;

use super::get::facts_from_tombstone;
use super::overlay::KeyState;
use super::{ts_ms, Hypha};
use crate::bucket::Readout;

use super::overlay::refuse;

struct PageView {
    entries: Vec<Object>,
    common_prefixes: Vec<CommonPrefix>,
}

/// Version byte for the hypha-owned v2 LIST cursor.
const LIST_TOKEN_VERSION: u8 = 1;

/// One hop of a v2 LIST cursor. The resume position is a plain key, not the backend's opaque token,
/// so the page can resume on whichever backend serves the next one — the `Restoring` → `Ready` flip
/// is a non-event. The stream's prefix and delimiter ride along so a continued request that omits
/// them still paginates the stream it started.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ListToken {
    prefix: Option<String>,
    delimiter: Option<String>,
    anchor: String,
}

impl ListToken {
    fn new(prefix: Option<String>, delimiter: Option<String>, anchor: String) -> Self {
        Self {
            prefix,
            delimiter,
            anchor,
        }
    }

    fn encode(&self) -> String {
        let mut buf = vec![LIST_TOKEN_VERSION];
        push_token_field(&mut buf, self.prefix.as_deref().map(str::as_bytes));
        push_token_field(&mut buf, self.delimiter.as_deref().map(str::as_bytes));
        push_token_field(&mut buf, Some(self.anchor.as_bytes()));
        base64_simd::URL_SAFE_NO_PAD.encode_to_string(&buf)
    }

    /// `None` for anything hypha could not have minted — a foreign backend's token, corruption, a
    /// non-UTF-8 field — which the caller turns into a retryable client error.
    fn decode(token: &str) -> Option<Self> {
        let raw = base64_simd::URL_SAFE_NO_PAD.decode_to_vec(token).ok()?;
        if raw.first() != Some(&LIST_TOKEN_VERSION) {
            return None;
        }
        let mut rest = &raw[1..];
        let prefix = match take_token_field(&mut rest)? {
            None => None,
            Some(bytes) => Some(String::from_utf8(bytes.to_vec()).ok()?),
        };
        let delimiter = match take_token_field(&mut rest)? {
            None => None,
            Some(bytes) => Some(String::from_utf8(bytes.to_vec()).ok()?),
        };
        let anchor = String::from_utf8(take_token_field(&mut rest)??.to_vec()).ok()?;
        if !rest.is_empty() {
            return None;
        }
        Some(Self {
            prefix,
            delimiter,
            anchor,
        })
    }

    /// Fold a continuation request's own stream parameters over the token's: the request's win when
    /// it sends them, the token's carry the stream forward when it omits them. A contradiction is an
    /// error — the token would silently resume a different LIST.
    fn resolve(
        self,
        prefix: &Option<String>,
        delimiter: &Option<String>,
    ) -> Result<(Option<String>, Option<String>, String), Error> {
        if let (Some(p), Some(tp)) = (prefix.as_ref(), self.prefix.as_ref()) {
            if p != tp {
                return Err(Error::Invalid(
                    "continuation token was minted for a different prefix".into(),
                ));
            }
        }
        if let (Some(d), Some(td)) = (delimiter.as_ref(), self.delimiter.as_ref()) {
            if d != td {
                return Err(Error::Invalid(
                    "continuation token was minted for a different delimiter".into(),
                ));
            }
        }
        Ok((
            prefix.clone().or(self.prefix),
            delimiter.clone().or(self.delimiter),
            self.anchor,
        ))
    }
}

fn push_token_field(buf: &mut Vec<u8>, field: Option<&[u8]>) {
    match field {
        Some(bytes) => {
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        // `u16::MAX` cannot collide with a real length: keys and prefixes cap at 1024 bytes.
        None => buf.extend_from_slice(&u16::MAX.to_le_bytes()),
    }
}

fn take_token_field<'a>(rest: &mut &'a [u8]) -> Option<Option<&'a [u8]>> {
    if rest.len() < 2 {
        return None;
    }
    let len = u16::from_le_bytes([rest[0], rest[1]]);
    *rest = &rest[2..];
    if len == u16::MAX {
        return Some(None);
    }
    let field = rest.get(..len as usize)?;
    *rest = &rest[len as usize..];
    Some(Some(field))
}

impl Hypha {
    pub(super) async fn op_head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        if meta::validate_client_key(&key).is_err() {
            return Err(Error::NotFound.into());
        }
        let (content_length, e_tag, last_modified, checksum_value, md) =
            match self.resolve_key(&bucket, &key).await? {
                KeyState::Absent => return Err(Error::NotFound.into()),
                KeyState::Remote { facts, md } => (
                    Some(facts.plen as i64),
                    Some(ETag::Strong(facts.cetag.clone())),
                    Some(ts_ms(facts.mtime_ms)),
                    facts
                        .checksum
                        .map(|value| (value, super::get::checksum_count(&facts.cetag))),
                    md,
                ),
                KeyState::CacheBody { head, md } => {
                    let facts = crate::tier::RemoteFacts::from_cache_head(&head);
                    (
                        head.content_length,
                        Some(ETag::Strong(facts.cetag)),
                        head.last_modified
                            .and_then(|t| t.to_millis().ok())
                            .map(ts_ms),
                        facts.checksum.map(|value| (value, 1)),
                        md,
                    )
                }
            };
        super::record_bytes(content_length.unwrap_or_default().max(0) as u64);

        let mut resp = HeadObjectOutput {
            content_length,
            e_tag,
            last_modified,
            // The pass-through carrier the facts above share . A remote-resolved key (mid-bracket
            // or mid-restore) carries neither, so both fall back to their defaults.
            metadata: Some(meta::decode_user_metadata(&md)),
            storage_class: Some(StorageClass::from(meta::storage_class(&md))),
            content_type: meta::content_type(&md),
            accept_ranges: Some("bytes".to_string()),
            ..Default::default()
        };
        if req
            .input
            .checksum_mode
            .as_ref()
            .is_some_and(|mode| mode.as_str() == ChecksumMode::ENABLED)
        {
            if let Some((value, count)) = checksum_value {
                let value = super::checksum::dto(&value, count);
                resp.checksum_crc32 = value.checksum_crc32;
                resp.checksum_crc32c = value.checksum_crc32c;
                resp.checksum_crc64nvme = value.checksum_crc64nvme;
                resp.checksum_sha1 = value.checksum_sha1;
                resp.checksum_sha256 = value.checksum_sha256;
                resp.checksum_type = value.checksum_type;
            }
        }
        Ok(S3Response::new(resp))
    }

    pub(super) async fn op_list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        // The ticket is held until the page is projected, so a cached-mode write admitted while it
        // is out commits remote-first.
        let ticket = self.buckets.read_ticket(&bucket).map_err(refuse)?;
        let restoring = matches!(ticket, Readout::Remote(_));
        let source = if restoring {
            self.remote()
        } else {
            self.data()
        };

        // The resume position is hypha's own key-anchored cursor, never the backend's opaque token:
        // a page minted while the bucket was `Restoring` (served from the remote) must be able to
        // resume on the cache once it is `Ready`, and only a key means the same thing to both. The
        // token carries the stream's prefix and delimiter, so a continued request that omits them
        // still paginates the stream it started; a request that contradicts them is a fresh LIST,
        // not a continuation.
        let (eff_prefix, eff_delim, start_after) = match &input.continuation_token {
            Some(token) => {
                let tok = ListToken::decode(token).ok_or_else(|| {
                    Error::Invalid("continuation token is not one of hypha's own".into())
                })?;
                let (prefix, delimiter, anchor) = tok.resolve(&input.prefix, &input.delimiter)?;
                (prefix, delimiter, Some(anchor))
            }
            None => (
                input.prefix.clone(),
                input.delimiter.clone(),
                input.start_after.clone(),
            ),
        };

        let raw = source
            .list(
                &bucket,
                eff_prefix.clone(),
                eff_delim.clone(),
                None,
                start_after,
                input.max_keys,
            )
            .await?;
        let max_keys = raw.max_keys;
        let is_truncated = raw.is_truncated;
        let objs = raw.contents.unwrap_or_default();
        let prefixes = raw.common_prefixes.unwrap_or_default();

        // The next resume position, present only while truncated: the greater of the page's last raw
        // key and its last common prefix — with a delimiter the two interleave, and resuming from the
        // last *content* key would re-roll a group already emitted (the rule v1 applies to
        // `NextMarker`). Raw, not projected: the anchor must advance past entries the projection
        // drops, or a page with no client-visible entry could re-read itself forever.
        let next_continuation_token = if is_truncated == Some(true) {
            objs.iter()
                .next_back()
                .and_then(|o| o.key())
                .map(str::to_string)
                .into_iter()
                .chain(
                    prefixes
                        .iter()
                        .next_back()
                        .and_then(|cp| cp.prefix())
                        .map(str::to_string),
                )
                .max()
                .map(|a| ListToken::new(eff_prefix.clone(), eff_delim.clone(), a).encode())
        } else {
            None
        };

        let PageView {
            entries,
            common_prefixes,
        } = self
            .page_view(
                &bucket,
                restoring,
                eff_prefix.as_deref(),
                eff_delim.as_deref(),
                objs,
                prefixes,
            )
            .await?;

        // KeyCount counts keys and common prefixes alike (S3). It is ≤ MaxKeys but may be strictly
        // less when an internal transition entry is omitted.
        let key_count = (entries.len() + common_prefixes.len()) as i32;
        let resp = ListObjectsV2Output {
            name: Some(bucket),
            prefix: eff_prefix,
            delimiter: eff_delim,
            key_count: Some(key_count),
            max_keys,
            // The backend's truncation flag, forwarded verbatim: a short page still paginates
            // correctly, and a client follows the cursor until IsTruncated is false.
            is_truncated,
            continuation_token: input.continuation_token,
            next_continuation_token,
            common_prefixes: Some(common_prefixes),
            contents: Some(entries),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// LIST v1. The classifier and the key-anchored pagination discipline are v2's verbatim (s3s does
    /// not translate v1→v2, so it is its own method); only the pagination shell differs — `marker`
    /// in, `NextMarker` out. Short pages are as valid under v1 as under v2.
    pub(super) async fn op_list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        // The ticket is held until the page is projected, so a cached-mode write admitted while it
        // is out commits remote-first.
        let ticket = self.buckets.read_ticket(&bucket).map_err(refuse)?;
        let restoring = matches!(ticket, Readout::Remote(_));
        let source = if restoring {
            self.remote()
        } else {
            self.data()
        };
        let raw = source
            .list_v1(
                &bucket,
                input.prefix.clone(),
                input.delimiter.clone(),
                input.marker.clone(),
                input.max_keys,
            )
            .await?;

        let max_keys = raw.max_keys;
        let is_truncated = raw.is_truncated;
        let objs = raw.contents.unwrap_or_default();
        let prefixes = raw.common_prefixes.unwrap_or_default();

        // v1's `NextMarker` is a *key*, not v2's opaque token, so hypha computes the resume
        // position: the greater of the page's last raw key and its last common prefix (with a
        // delimiter the two interleave, and resuming from the last *content* key would re-roll a
        // group already emitted). Both sources hold nothing but client objects at client keys ,
        // so the last raw key is always an XML-safe, strictly-increasing client key.
        let next_marker = objs
            .iter()
            .next_back()
            .and_then(|o| o.key())
            .map(str::to_string)
            .into_iter()
            .chain(
                prefixes
                    .iter()
                    .next_back()
                    .and_then(|cp| cp.prefix())
                    .map(str::to_string),
            )
            .max()
            .filter(|_| is_truncated == Some(true));

        let PageView {
            entries,
            common_prefixes,
        } = self
            .page_view(
                &bucket,
                restoring,
                input.prefix.as_deref(),
                input.delimiter.as_deref(),
                objs,
                prefixes,
            )
            .await?;

        let resp = ListObjectsOutput {
            name: Some(bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            marker: input.marker,
            next_marker,
            max_keys,
            is_truncated,
            common_prefixes: Some(common_prefixes),
            contents: Some(entries),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    async fn page_view(
        &self,
        bucket: &str,
        restoring: bool,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        objs: Vec<aws_sdk_s3::types::Object>,
        prefixes: Vec<aws_sdk_s3::types::CommonPrefix>,
    ) -> S3Result<PageView> {
        if restoring {
            let (entries, common_prefixes) =
                self.project_remote_page(bucket, objs, prefixes).await?;
            Ok(PageView {
                entries,
                common_prefixes,
            })
        } else {
            self.project_page(bucket, prefix, delimiter, objs, prefixes)
                .await
        }
    }

    /// One `<data>` page → the client-visible entries and common prefixes it projects (the
    /// classifier), pairing eviction tombstones with their `<meta>` twins by a merge join. Shared by
    /// both LIST versions, which differ only in their pagination shell. `<data><b>` holds only
    /// client objects, so there is nothing hypha-internal to filter out here.
    async fn project_page(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        objs: Vec<aws_sdk_s3::types::Object>,
        raw_prefixes: Vec<aws_sdk_s3::types::CommonPrefix>,
    ) -> S3Result<PageView> {
        // Classify each entry first, collecting the eviction tombstones that need a twin. The twin
        // cursor is then fetched once, bounded to the span those keys occupy .
        enum Kind {
            Live,
            Evict,
            Transit,
        }
        let mut classified: Vec<(String, i64, String, Option<Timestamp>, Kind)> = Vec::new();
        for o in &objs {
            // Every S3 object has a key; a keyless LIST entry is a broken backend response.
            let key = o
                .key()
                .ok_or_else(|| Error::Backend("LIST returned an entry with no key".into()))?;
            let size = o.size().unwrap_or_default();
            let etag = o.e_tag().unwrap_or_default().trim_matches('"').to_string();
            let lm = o
                .last_modified()
                .and_then(|t| t.to_millis().ok())
                .map(ts_ms);
            let kind = match meta::classify_entry(size, &etag) {
                None => Kind::Live,
                Some(meta::TombKind::Evict) => Kind::Evict,
                Some(meta::TombKind::Transit) => Kind::Transit,
            };
            classified.push((key.to_string(), size, etag, lm, kind));
        }

        // The twin cursor, over exactly the key span the eviction tombstones on this page occupy.
        let evict_keys: Vec<&str> = classified
            .iter()
            .filter(|(_, _, _, _, k)| matches!(k, Kind::Evict))
            .map(|(key, ..)| key.as_str())
            .collect();
        let twins = match (evict_keys.first(), evict_keys.last()) {
            (Some(lo), Some(hi)) => self.fetch_twins(bucket, prefix, delimiter, lo, hi).await?,
            _ => HashMap::new(),
        };

        let mut entries: Vec<Object> = Vec::new();
        for (key, size, etag, lm, kind) in classified {
            match kind {
                Kind::Live => entries.push(Object {
                    key: Some(key),
                    size: Some(size),
                    e_tag: Some(ETag::Strong(etag)),
                    last_modified: lm,
                    ..Default::default()
                }),
                Kind::Evict => match twins.get(&key) {
                    // Paired by base-key equality : a twin against an eviction tombstone is
                    // valid by construction.
                    Some(f) => entries.push(Object {
                        key: Some(key),
                        size: Some(f.plen as i64),
                        e_tag: Some(ETag::Strong(f.client_etag.clone())),
                        last_modified: Some(ts_ms(f.mtime_ms)),
                        ..Default::default()
                    }),
                    // No twin: a crash window, a page straddle, or a key over the twin threshold.
                    // The tombstone's own metadata is authoritative — one per-key HEAD .
                    None => {
                        if let Some(o) = self.head_facts(bucket, &key).await? {
                            entries.push(o);
                        }
                    }
                },
                // Mid-bracket: the one classification that leaves the cache — remote HEAD .
                Kind::Transit => match self.remote().head(bucket, &key).await {
                    Ok(h) => {
                        let f = self.tier.remote_facts(bucket, &key, &h).await?;
                        entries.push(Object {
                            key: Some(key),
                            size: Some(f.plen as i64),
                            e_tag: Some(ETag::Strong(f.cetag)),
                            last_modified: Some(ts_ms(f.mtime_ms)),
                            ..Default::default()
                        });
                    }
                    Err(Error::NotFound) => {}
                    Err(e) => return Err(e.into()),
                },
            }
        }

        let common_prefixes: Vec<CommonPrefix> = raw_prefixes
            .into_iter()
            .map(|cp| CommonPrefix { prefix: cp.prefix })
            .collect();

        Ok(PageView {
            entries,
            common_prefixes,
        })
    }

    /// The twin cursor : `<meta>` range B over `[lo, hi]`, keyed back to base keys. Prefix
    /// `0x01 ‖ <client prefix>` and the mirrored delimiter make its shape track the client cursor's
    /// — a twin whose base rolls up under the delimiter rolls up identically (the facts alphabet
    /// excludes `/`), so only individual twins (those matching individual client entries) come
    /// back as content. `start_after` past `0x01 ‖ lo` skips range A (mpu/shadow, `0x01 0x01 …`).
    async fn fetch_twins(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        lo: &str,
        hi: &str,
    ) -> S3Result<HashMap<String, meta::Facts>> {
        let c = meta::CTRL as char;
        let twin_prefix = format!("{c}{}", prefix.unwrap_or(""));
        let start_after = format!("{c}{lo}");
        let mut map = HashMap::new();
        let mut token: Option<String> = None;
        loop {
            let first = token.is_none();
            let page = self
                .meta()
                .list(
                    bucket,
                    Some(twin_prefix.clone()),
                    delimiter.map(str::to_string),
                    token.clone(),
                    first.then(|| start_after.clone()),
                    None,
                )
                .await?;
            let mut past_hi = false;
            for obj in page.contents.unwrap_or_default() {
                let Some(k) = obj.key else { continue };
                // Range-A records never appear (start_after skips them); a stray non-twin is
                // ignored. Twins past `hi` end the scan — the cursor is sorted.
                if let Some((base, facts)) = meta::parse_twin(&k) {
                    if base > hi {
                        past_hi = true;
                        break;
                    }
                    map.insert(base.to_string(), facts);
                }
            }
            if past_hi || page.is_truncated != Some(true) {
                break;
            }
            token = page.next_continuation_token;
            if token.is_none() {
                break;
            }
        }
        Ok(map)
    }

    /// HEAD-fallback facts for an eviction tombstone missing its twin . `None` if the key
    /// moved on (deleted / absent) since the LIST page was cut.
    async fn head_facts(&self, bucket: &str, key: &str) -> S3Result<Option<Object>> {
        match self.data().head(bucket, key).await {
            Ok(head) => {
                // No metadata ⇒ no tombstone; the empty-map classifier would say the same.
                let Some(md) = head.metadata.as_ref() else {
                    return Ok(None);
                };
                match meta::tomb_kind(md) {
                    Some(meta::TombKind::Evict) => {
                        let f = facts_from_tombstone(key, md)?;
                        Ok(Some(Object {
                            key: Some(key.to_string()),
                            size: Some(f.plen as i64),
                            e_tag: Some(ETag::Strong(f.cetag)),
                            last_modified: Some(ts_ms(f.mtime_ms)),
                            ..Default::default()
                        }))
                    }
                    _ => Ok(None),
                }
            }
            Err(Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
