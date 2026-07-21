//! HEAD and LIST, both cache-served and reporting **plaintext** facts (§7). HEAD reads them off
//! the object (native for a live body, metadata for a tombstone; a transition mark resolves from
//! the remote). LIST classifies each entry from its (size, ETag) sentinel pair (§6) and pairs it
//! with its adjacent facts twin in one pass — the twin applies **iff the entry classifies as an
//! eviction tombstone**; per-key HEADs happen only for the rare missing-twin fallback and for
//! transition marks (the one classification that leaves the cache).

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;

use super::get::facts_from_tombstone;
use super::{ts_ms, Hypha};

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
        let head = self.cache().head(&bucket, &key).await?;
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
            .cache()
            .list(
                &bucket,
                input.prefix.clone(),
                input.delimiter.clone(),
                input.continuation_token.clone(),
                input.start_after.clone(),
                input.max_keys,
            )
            .await?;

        let objs = raw.contents.unwrap_or_default();

        // Walk the raw page, filtering the reserved keyspace and pairing each base key with the
        // twin that sorts immediately after it.
        let mut entries: Vec<Object> = Vec::new();
        let mut i = 0;
        while i < objs.len() {
            // Every S3 object has a key; a keyless LIST entry is a broken backend response.
            let key = objs[i]
                .key()
                .ok_or_else(|| Error::Backend("LIST returned an entry with no key".into()))?;
            // Reserved-prefix records and orphan twins are hypha-internal, never listed.
            if meta::is_reserved_key(key) || meta::parse_twin(key).is_some() {
                i += 1;
                continue;
            }
            // The twin, if the next entry is this key's twin.
            let twin = objs.get(i + 1).and_then(|o| {
                let k = o.key().unwrap_or_default();
                meta::parse_twin(k)
                    .filter(|(base, _)| *base == key)
                    .map(|(_, f)| f)
            });
            let consumed_twin = twin.is_some();

            let size = objs[i].size().unwrap_or_default();
            let etag = objs[i].e_tag().unwrap_or_default().trim_matches('"');

            match meta::classify_entry(size, etag) {
                // Live plaintext body: native facts; any adjacent twin is stale — ignored.
                None => entries.push(Object {
                    key: Some(key.to_string()),
                    size: Some(size),
                    e_tag: Some(ETag::Strong(etag.to_string())),
                    last_modified: objs[i]
                        .last_modified()
                        .and_then(|t| t.to_millis().ok())
                        .map(ts_ms),
                    ..Default::default()
                }),
                Some(meta::TombKind::Delete) => {} // client-visibly absent
                Some(meta::TombKind::Evict) => match twin {
                    // The classification gate (§6): a twin next to an eviction tombstone is
                    // valid by construction.
                    Some(f) => entries.push(Object {
                        key: Some(key.to_string()),
                        size: Some(f.plen as i64),
                        e_tag: Some(ETag::Strong(f.client_etag)),
                        last_modified: Some(ts_ms(f.mtime_ms)),
                        ..Default::default()
                    }),
                    // Missing/unparseable twin: the tombstone's metadata is authoritative (§6).
                    None => {
                        if let Some(o) = self.head_facts(&bucket, key).await? {
                            entries.push(o);
                        }
                    }
                },
                // Mid-bracket: the one classification that leaves the cache — remote HEAD (§7).
                Some(meta::TombKind::Transit) => match self.remote().head(&bucket, key).await {
                    Ok(h) => {
                        let f = self.tier.remote_facts(&bucket, key, &h).await?;
                        entries.push(Object {
                            key: Some(key.to_string()),
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
            i += if consumed_twin { 2 } else { 1 };
        }

        let common_prefixes = raw.common_prefixes.map(|cps| {
            cps.into_iter()
                .map(|cp| CommonPrefix { prefix: cp.prefix })
                .filter(|cp| !cp.prefix.as_deref().is_some_and(meta::is_reserved_key))
                .collect()
        });

        let resp = ListObjectsV2Output {
            name: Some(bucket),
            prefix: input.prefix,
            delimiter: input.delimiter,
            key_count: Some(entries.len() as i32),
            max_keys: raw.max_keys,
            is_truncated: raw.is_truncated,
            continuation_token: input.continuation_token,
            next_continuation_token: raw.next_continuation_token,
            common_prefixes,
            contents: Some(entries),
            ..Default::default()
        };
        Ok(S3Response::new(resp))
    }

    /// HEAD-fallback facts for an eviction tombstone missing its twin (§6). `None` if the key
    /// moved on (deleted / absent) since the LIST page was cut.
    async fn head_facts(&self, bucket: &str, key: &str) -> S3Result<Option<Object>> {
        match self.cache().head(bucket, key).await {
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
