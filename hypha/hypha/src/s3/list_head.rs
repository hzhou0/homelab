//! HEAD and LIST, both cache-served and reporting **plaintext** facts (§7). HEAD reads them off
//! the object (native for a live body, metadata for a tombstone). LIST classifies each entry from
//! its (size, ETag) sentinel pair and pairs it with its adjacent facts twin in one pass — no
//! per-key HEAD except the rare unbound-twin fallback.

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;

use super::get::{client_etag, require_plen};
use super::{meta_get, Hypha};

impl Hypha {
    pub(super) async fn op_head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let head = self.cache().head(&req.input.key).await?;
        let md = &head.metadata;

        let (content_length, e_tag) = if md.as_ref().map(meta::is_tombstone).unwrap_or(false) {
            if meta_get(md, meta::TOMB) == Some(meta::TOMB_DELETE) {
                return Err(Error::NotFound.into());
            }
            (Some(require_plen(md)? as i64), client_etag(md))
        } else {
            (
                head.content_length,
                head.e_tag.as_ref().map(|e| ETag::Strong(e.trim_matches('"').to_string())),
            )
        };

        let resp = HeadObjectOutput {
            content_length,
            e_tag,
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
        let raw = self
            .cache()
            .list(
                input.prefix.clone(),
                input.delimiter.clone(),
                input.continuation_token.clone(),
                input.start_after.clone(),
                input.max_keys,
            )
            .await?;

        let evict_etag = meta::evict_sentinel_etag();
        let delete_etag = meta::delete_sentinel_etag();
        let objs = raw.contents.unwrap_or_default();

        // Walk the raw page, stripping the deployment prefix on the fly and pairing each base key
        // with the twin that sorts immediately after it — allocating only the fields emitted.
        let mut entries: Vec<Object> = Vec::new();
        let mut i = 0;
        while i < objs.len() {
            let key = self.cache().strip(objs[i].key().unwrap_or_default());
            // A twin with no preceding base it belongs to: orphan, skip (swept elsewhere).
            if meta::parse_twin(key).is_some() {
                i += 1;
                continue;
            }
            // The twin, if the next entry is this key's twin.
            let twin = objs.get(i + 1).and_then(|o| {
                let k = self.cache().strip(o.key().unwrap_or_default());
                meta::parse_twin(k).filter(|(base, _)| *base == key).map(|(_, f)| f)
            });
            let consumed_twin = twin.is_some();

            let size = objs[i].size().unwrap_or_default();
            let etag = objs[i].e_tag().unwrap_or_default().trim_matches('"');
            let is_evict = size == 16 && etag == evict_etag;
            let is_delete = size == 16 && etag == delete_etag;

            if is_delete {
                // Client-visibly absent — omit.
            } else if is_evict {
                match twin.filter(|f| f.bound_etag == evict_etag) {
                    Some(f) => entries.push(Object {
                        key: Some(key.to_string()),
                        size: Some(f.plen as i64),
                        e_tag: Some(ETag::Strong(f.client_etag)),
                        ..Default::default()
                    }),
                    // Unbound / missing twin: fall back to the object's own metadata (§6).
                    None => {
                        if let Some(o) = self.head_facts(key).await? {
                            entries.push(o);
                        }
                    }
                }
            } else {
                // Live plaintext body: cache size/ETag already describe the plaintext.
                entries.push(Object {
                    key: Some(key.to_string()),
                    size: Some(size),
                    e_tag: Some(ETag::Strong(etag.to_string())),
                    ..Default::default()
                });
            }
            i += if consumed_twin { 2 } else { 1 };
        }

        let common_prefixes = raw.common_prefixes.map(|cps| {
            cps.into_iter()
                .map(|cp| CommonPrefix {
                    prefix: cp.prefix.map(|p| self.cache().strip(&p).to_string()),
                })
                .collect()
        });

        let resp = ListObjectsV2Output {
            name: Some(self.cache().bucket().to_string()),
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

    /// HEAD-fallback facts for an unbound-twin tombstone (§6). `None` if it turned out absent.
    async fn head_facts(&self, key: &str) -> S3Result<Option<Object>> {
        match self.cache().head(key).await {
            Ok(head) => {
                let md = &head.metadata;
                if meta_get(md, meta::TOMB) == Some(meta::TOMB_DELETE) {
                    return Ok(None);
                }
                Ok(Some(Object {
                    key: Some(key.to_string()),
                    size: Some(require_plen(md)? as i64),
                    e_tag: client_etag(md),
                    ..Default::default()
                }))
            }
            Err(Error::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
