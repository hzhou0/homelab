//! HEAD and LIST, both cache-served and reporting **plaintext** facts (§7). HEAD reads them off
//! the `<data>` object (native for a live body, metadata for a tombstone; a transition mark
//! resolves from the remote). LIST is a **merge join** of two cursors (§6/§7): the client cursor
//! over `<data><b>` (client bodies + tombstones, so its keys and pagination are clean), and the
//! twin cursor over `<meta><b>`'s range B, `prefix = 0x01 ‖ <client prefix>`, delimiter mirrored.
//! Each client entry is classified from its (size, ETag) sentinel pair (§6); an eviction tombstone
//! takes its facts from the twin matched **by base-key equality**, with a per-key `<data>` HEAD
//! fallback when the twin is missing (crash window, page straddle, or a key over the §6 twin
//! threshold). Delete-tombstones are dropped; transition marks resolve from the remote.
//!
//! LIST is a **single page**, forwarded pagination — the client cursor drives it. Delete-tombstones
//! are dropped, so a page can yield fewer than `MaxKeys` client entries (a short page, valid S3 as
//! long as `IsTruncated` and the resume position are honest); but with the twins moved out of the
//! client keyspace, pages are short only where keys were *deleted*, not for every evicted key.
//! hypha forwards the `<data>` cursor's own continuation token / truncation flag (v2) or last raw
//! key (v1's `NextMarker`) and deliberately does **not** backfill to fill a page: coalescing pages
//! would require reusing a backend cursor across requests or resuming by a client-entry count, and
//! both weaken S3's key-position guarantee under concurrent mutation.

use std::collections::HashMap;

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;

use super::get::facts_from_tombstone;
use super::{ts_ms, Hypha};

/// The client-visible projection of one raw cache page — what both LIST versions put in `Contents`
/// and `CommonPrefixes`. Pagination is not in here: the versions resume differently.
struct PageView {
    entries: Vec<Object>,
    common_prefixes: Vec<CommonPrefix>,
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
        let head = self.data().head(&bucket, &key).await?;
        let md = head.metadata.clone().unwrap_or_default();

        let (content_length, e_tag, last_modified) = match meta::tomb_kind(&md) {
            Some(meta::TombKind::Delete) => return Err(Error::NotFound.into()),
            Some(meta::TombKind::Evict) => {
                let f = facts_from_tombstone(&key, &md)?;
                (
                    Some(f.plen as i64),
                    Some(ETag::Strong(f.cetag)),
                    Some(ts_ms(f.mtime_ms)),
                )
            }
            Some(meta::TombKind::Transit) => match self.resolve_transit(&bucket, &key).await? {
                None => return Err(Error::NotFound.into()),
                Some(f) => (
                    Some(f.plen as i64),
                    Some(ETag::Strong(f.cetag)),
                    Some(ts_ms(f.mtime_ms)),
                ),
            },
            None => (
                head.content_length,
                head.e_tag
                    .as_ref()
                    .map(|e| ETag::Strong(e.trim_matches('"').to_string())),
                head.last_modified
                    .and_then(|t| t.to_millis().ok())
                    .map(ts_ms),
            ),
        };

        let resp = HeadObjectOutput {
            content_length,
            e_tag,
            last_modified,
            // The pass-through carrier the facts above share (§7). A transition mark resolves its
            // facts from the remote, which carries neither — so both fall back to their defaults.
            metadata: Some(meta::decode_user_metadata(&md)),
            storage_class: Some(StorageClass::from(meta::storage_class(&md))),
            accept_ranges: Some("bytes".to_string()),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    pub(super) async fn op_list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let raw = self
            .data()
            .list(
                &bucket,
                input.prefix.clone(),
                input.delimiter.clone(),
                input.continuation_token.clone(),
                input.start_after.clone(),
                input.max_keys,
            )
            .await?;

        let page = self
            .project_page(
                &bucket,
                input.prefix.as_deref(),
                input.delimiter.as_deref(),
                raw.contents.unwrap_or_default(),
                raw.common_prefixes.unwrap_or_default(),
            )
            .await?;

        // KeyCount counts keys and common prefixes alike (S3). It is ≤ MaxKeys but may be strictly
        // less: dropped delete-tombstones leave a short — but honestly truncated — page.
        let key_count = (page.entries.len() + page.common_prefixes.len()) as i32;
        let resp = ListObjectsV2Output {
            name: Some(bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            key_count: Some(key_count),
            max_keys: raw.max_keys,
            // The backend's key-position token and flag, forwarded verbatim: a short page still
            // paginates correctly, and a client follows the token until IsTruncated is false.
            is_truncated: raw.is_truncated,
            continuation_token: input.continuation_token,
            next_continuation_token: raw.next_continuation_token,
            common_prefixes: Some(page.common_prefixes),
            contents: Some(page.entries),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// LIST v1. The classifier and the forwarded-pagination discipline are v2's verbatim (s3s does
    /// not translate v1→v2, so it is its own method); only the pagination shell differs —
    /// `marker` in, `NextMarker` out. Short pages are as valid under v1 as under v2.
    pub(super) async fn op_list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let input = req.input;
        let bucket = input.bucket.clone();
        let raw = self
            .data()
            .list_v1(
                &bucket,
                input.prefix.clone(),
                input.delimiter.clone(),
                input.marker.clone(),
                input.max_keys,
            )
            .await?;

        let is_truncated = raw.is_truncated;
        // v1's `NextMarker` is a *key*, not v2's opaque token, so hypha computes the resume
        // position: the key position the `<data>` page actually reached — the greater of its last
        // raw key and its last common prefix (with a delimiter the two interleave, and resuming
        // from the last *content* key would re-roll a group already emitted). This is expressible
        // only because `<data><b>` holds nothing but client objects (§6): the last raw key is
        // always an XML-safe, strictly-increasing client key — never a twin (which would carry an
        // illegal `0x01`), and never an empty marker that loops on a page of filtered records.
        let next_marker = raw
            .contents
            .iter()
            .flatten()
            .next_back()
            .and_then(|o| o.key())
            .map(str::to_string)
            .into_iter()
            .chain(
                raw.common_prefixes
                    .iter()
                    .flatten()
                    .next_back()
                    .and_then(|cp| cp.prefix())
                    .map(str::to_string),
            )
            .max()
            .filter(|_| is_truncated == Some(true));

        let page = self
            .project_page(
                &bucket,
                input.prefix.as_deref(),
                input.delimiter.as_deref(),
                raw.contents.unwrap_or_default(),
                raw.common_prefixes.unwrap_or_default(),
            )
            .await?;

        let resp = ListObjectsOutput {
            name: Some(bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            marker: input.marker,
            next_marker,
            max_keys: raw.max_keys,
            is_truncated,
            common_prefixes: Some(page.common_prefixes),
            contents: Some(page.entries),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// One `<data>` page → the client-visible entries and common prefixes it projects (§7's
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
        // cursor is then fetched once, bounded to the span those keys occupy (§7).
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
                Some(meta::TombKind::Delete) => continue, // client-visibly absent
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
                    // Paired by base-key equality (§7): a twin against an eviction tombstone is
                    // valid by construction.
                    Some(f) => entries.push(Object {
                        key: Some(key),
                        size: Some(f.plen as i64),
                        e_tag: Some(ETag::Strong(f.client_etag.clone())),
                        last_modified: Some(ts_ms(f.mtime_ms)),
                        ..Default::default()
                    }),
                    // No twin: a crash window, a page straddle, or a key over the §6 twin threshold.
                    // The tombstone's own metadata is authoritative — one per-key HEAD (§6).
                    None => {
                        if let Some(o) = self.head_facts(bucket, &key).await? {
                            entries.push(o);
                        }
                    }
                },
                // Mid-bracket: the one classification that leaves the cache — remote HEAD (§7).
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

        // `<data>` common prefixes are pure client keyspace — no hypha-internal groups to filter.
        let common_prefixes: Vec<CommonPrefix> = raw_prefixes
            .into_iter()
            .map(|cp| CommonPrefix { prefix: cp.prefix })
            .collect();

        Ok(PageView {
            entries,
            common_prefixes,
        })
    }

    /// The twin cursor (§6/§7): `<meta>` range B over `[lo, hi]`, keyed back to base keys. Prefix
    /// `0x01 ‖ <client prefix>` and the mirrored delimiter make its shape track the client cursor's
    /// — a twin whose base rolls up under the delimiter rolls up identically (the facts alphabet
    /// excludes `/`, §6), so only individual twins (those matching individual client entries) come
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

    /// HEAD-fallback facts for an eviction tombstone missing its twin (§6). `None` if the key
    /// moved on (deleted / absent) since the LIST page was cut.
    async fn head_facts(&self, bucket: &str, key: &str) -> S3Result<Option<Object>> {
        match self.data().head(bucket, key).await {
            Ok(head) => {
                let md = head.metadata.clone().unwrap_or_default();
                match meta::tomb_kind(&md) {
                    Some(meta::TombKind::Evict) => {
                        let f = facts_from_tombstone(key, &md)?;
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
