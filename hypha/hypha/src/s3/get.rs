//! GET, cache-first. A live cache body is plaintext — served straight through (ranges forwarded
//! to the cache). A tombstone means the body is remote-only: fetch and decrypt from the remote,
//! and — in `durable` mode — do **not** repopulate the cache (never-restore, as the body would
//! immediately be tombstoned again). A delete-tombstone is a client-visible 404.

use std::ops::Range as ByteRange;

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use hypha_core::error::Error;
use hypha_core::meta;

use super::{meta_get, Hypha};
use crate::codec;

impl Hypha {
    pub(super) async fn op_get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let key = input.key.clone();

        let head = self.cache().head(&key).await?;
        let md = &head.metadata;

        if md.as_ref().map(meta::is_tombstone).unwrap_or(false) {
            if meta_get(md, meta::TOMB) == Some(meta::TOMB_DELETE) {
                return Err(Error::NotFound.into());
            }
            return self.get_from_remote(&key, &input, md).await;
        }

        // Live plaintext body in the cache.
        let out = self
            .cache()
            .get(&key, input.range.as_ref().map(range_header))
            .await?;
        let status = if input.range.is_some() {
            Some(hyper::StatusCode::PARTIAL_CONTENT)
        } else {
            None
        };
        let resp = GetObjectOutput {
            content_length: out.content_length,
            content_range: out.content_range,
            e_tag: out.e_tag.map(|e| ETag::Strong(e.trim_matches('"').to_string())),
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

    /// Serve a tombstoned (remote-only) object by decrypting from the remote (§6). No cache
    /// repopulation — durable mode never restores.
    async fn get_from_remote(
        &self,
        key: &str,
        input: &GetObjectInput,
        md: &Option<Metadata>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let plen = require_plen(md)?;
        let etag = client_etag(md);
        match &input.range {
            None => {
                let out = self.remote().get(key, None).await?;
                let body = codec::decrypt_full(self.env(), out.body);
                let resp = GetObjectOutput {
                    body: Some(body),
                    content_length: Some(plen as i64),
                    e_tag: etag,
                    accept_ranges: Some("bytes".to_string()),
                    ..Default::default()
                };
                Ok(S3Response::new(resp))
            }
            Some(range) => {
                let rhead = self.remote().head(key).await?;
                let ct_len = rhead
                    .content_length
                    .ok_or_else(|| Error::Backend("remote HEAD missing content-length".into()))?
                    as u64;
                let pt = plaintext_range(range, plen)?;
                let content_range = format!("bytes {}-{}/{}", pt.start, pt.end - 1, plen);
                let len = pt.end - pt.start;
                let body =
                    codec::decrypt_range(self.env(), self.remote().clone(), key.to_string(), ct_len, pt);
                let resp = GetObjectOutput {
                    body: Some(body),
                    content_length: Some(len as i64),
                    content_range: Some(content_range),
                    e_tag: etag,
                    accept_ranges: Some("bytes".to_string()),
                    ..Default::default()
                };
                Ok(S3Response::with_status(resp, hyper::StatusCode::PARTIAL_CONTENT))
            }
        }
    }
}

/// Plaintext length carried on a tombstone (or remote object); absence means the object wasn't
/// written through hypha.
pub(crate) fn require_plen(m: &Option<Metadata>) -> Result<u64, Error> {
    meta_get(m, meta::PLEN)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Error::Backend("object missing hypha plaintext-length metadata".into()))
}

/// Client-visible ETag from a tombstone's / remote object's metadata (§9). Strong validator; s3s
/// renders the quotes.
pub(crate) fn client_etag(m: &Option<Metadata>) -> Option<ETag> {
    meta_get(m, meta::CETAG).map(|e| ETag::Strong(e.to_string()))
}

/// Reconstruct the HTTP Range header value the client sent, to forward to the cache.
fn range_header(range: &Range) -> String {
    match *range {
        Range::Int { first, last: Some(last) } => format!("bytes={first}-{last}"),
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
