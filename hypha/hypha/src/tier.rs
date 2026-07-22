//! The shared tiering machinery: the §7 transition-bracket primitives (mark / settle / repair),
//! encrypt-and-upload of a cache body, and tombstoning once ciphertext is durable on the remote.
//! All of it serializes on the per-key lock ([`KeyLocks`]); the durable path calls these inline
//! while holding the key lock, and the cached path's background reconcile and GC will call the
//! same primitives (Phases 4–5).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use hypha_format::{decode_tail, Tail, TrailerKey};
use hypha_format::{Envelope, MAX_TAIL_LEN};

use hypha_core::error::{Error, Result};
use hypha_core::{meta, Backend};

use crate::codec::{self, SingleTrailer};
use crate::keylocks::KeyLocks;

#[derive(Clone)]
pub struct Reconciler {
    pub cache: Backend,
    pub remote: Backend,
    pub env: Arc<Envelope>,
    /// Keys the tail trailer's authentication tag (§6); derived once from the master passphrase.
    pub trailer_key: TrailerKey,
    pub locks: KeyLocks,
}

/// The plaintext facts of a committed remote object, read off its tail footer (§6).
#[derive(Clone, Debug)]
pub(crate) struct RemoteFacts {
    pub plen: u64,
    pub cetag: String,
    pub mtime_ms: i64,
}

impl Reconciler {
    // ── The transition bracket (§7) ─────────────────────────────────────────────────────────

    /// **Mark**: overwrite K's cache entry with the transition tombstone. Readers resolve K from
    /// the remote until settle. Caller holds K's write lock — a mark is only ever *observed* by
    /// lock-free readers mid-bracket or by anyone after a crash.
    pub(crate) async fn mark_transit_locked(&self, bucket: &str, key: &str) -> Result<()> {
        let mut md = HashMap::new();
        md.insert(meta::TOMB.to_string(), meta::TOMB_TRANSIT.to_string());
        self.cache
            .put_small(bucket, key, meta::TRANSIT_SENTINEL.to_vec(), md, None, None)
            .await?;
        Ok(())
    }

    /// **Settle** after a commit that left K present on the remote: fresh twin, then the
    /// eviction tombstone carrying the full facts (kind, cetag, plen, original mtime) in its
    /// user-metadata — the authoritative copy; the twin is its LIST projection (§6).
    pub(crate) async fn settle_evict_locked(
        &self,
        bucket: &str,
        key: &str,
        plen: u64,
        cetag: &str,
        mtime_ms: i64,
    ) -> Result<()> {
        let facts = meta::Facts {
            client_etag: cetag.to_string(),
            plen,
            mtime_ms,
        };
        self.refresh_twin(bucket, key, &facts).await?;

        let mut md = HashMap::new();
        md.insert(meta::TOMB.to_string(), meta::TOMB_EVICT.to_string());
        md.insert(meta::PLEN.to_string(), plen.to_string());
        md.insert(meta::CETAG.to_string(), cetag.to_string());
        md.insert(meta::MTIME.to_string(), mtime_ms.to_string());
        self.cache
            .put_small(bucket, key, meta::EVICT_SENTINEL.to_vec(), md, None, None)
            .await?;
        Ok(())
    }

    /// **Settle** after a commit that removed K from the remote: absent is the authoritative 404.
    pub(crate) async fn settle_absent_locked(&self, bucket: &str, key: &str) -> Result<()> {
        self.delete_twins(bucket, key).await?;
        self.cache.delete(bucket, key).await?;
        Ok(())
    }

    /// **Repair rule** (§7): settle K to whatever the remote actually holds. Idempotent; needs no
    /// knowledge of what the dead (or failed) writer was doing. Caller holds K's write lock.
    /// Returns the facts K settled to, `None` if it settled absent.
    pub(crate) async fn repair_locked(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<RemoteFacts>> {
        let head = match self.remote.head(bucket, key).await {
            Ok(h) => h,
            Err(Error::NotFound) => {
                self.settle_absent_locked(bucket, key).await?;
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let facts = self.remote_facts(bucket, key, &head).await?;
        self.settle_evict_locked(bucket, key, facts.plen, &facts.cetag, facts.mtime_ms)
            .await?;
        Ok(Some(facts))
    }

    /// Resolve a remote object's plaintext facts from its tail trailer (§6): **one speculative tail
    /// read**, single-part and composite alike — the trailer carries the complete facts either way,
    /// and its kind/count distinguish the two. Mid-bracket reads, repair, and the restore sweep all
    /// resolve through here. The HEAD supplies the mtime fallback only.
    pub(crate) async fn remote_facts(
        &self,
        bucket: &str,
        key: &str,
        head: &HeadObjectOutput,
    ) -> Result<RemoteFacts> {
        let remote_mtime = head
            .last_modified()
            .map(|t| t.to_millis().unwrap_or_default())
            .unwrap_or_else(now_ms);

        let tail = self
            .read_tail(bucket, key)
            .await?
            .ok_or_else(|| bad_facts(key))?;
        let f = &tail.footer;
        Ok(RemoteFacts {
            plen: f.plen,
            cetag: f.client_etag(),
            mtime_ms: if f.mtime_ms > 0 {
                f.mtime_ms
            } else {
                remote_mtime
            },
        })
    }

    /// One speculative suffix GET of the trailing [`MAX_TAIL_LEN`] bytes, then authenticate and
    /// parse the trailer (§6): this captures `table ‖ facts ‖ tag ‖ version` for any object in a
    /// single round trip, so composite reads recover their parts table without a second fetch. The
    /// object's total length — needed to place the body/trailer boundary — comes from the suffix
    /// GET's own `Content-Range` (`bytes X-Y/TOTAL`), else the whole object was returned and its
    /// byte count is the length. `None` ⇒ the bytes don't authenticate as a hypha trailer: the
    /// object was never written through hypha, or is foreign/tampered.
    pub(crate) async fn read_tail(&self, bucket: &str, key: &str) -> Result<Option<Tail>> {
        let out = self
            .remote
            .get(bucket, key, Some(format!("bytes=-{MAX_TAIL_LEN}")))
            .await?;
        let total = out.content_range().and_then(parse_content_range_total);
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| Error::Backend(format!("tail read: {e}")))?
            .into_bytes();
        let object_len = total.unwrap_or(bytes.len() as u64);
        Ok(decode_tail(&self.trailer_key, key, object_len, &bytes))
    }

    // ── Upload / GC primitives ──────────────────────────────────────────────────────────────

    /// Encrypt the current cache body at `key` and PUT it to the remote at `key`, the plaintext
    /// facts framed in as the footer (§6). `plen`, `cetag`, and `mtime` are read from the *same*
    /// GET response that streams the body, so the framed facts can never disagree with the
    /// uploaded bytes. Assumes the caller holds `key`'s lock.
    ///
    /// Phase 4 note: the cached-mode reconciler serializes same-key passes on a dedicated per-key
    /// *upload* lock (a second `KeyLocks` instance), not the write lock — held across the whole
    /// upload + marker CAS. Unserialized same-key uploads can finish out of order and leave the
    /// remote stale with an empty pending set (IMPLEMENTATION §7); the separate instance keeps a
    /// conditional PUT from ever queuing behind a multi-second transfer.
    #[allow(dead_code)] // phase 4: the cached-mode reconcile sweep
    pub(crate) async fn upload_locked(&self, bucket: &str, key: &str) -> Result<()> {
        let out = self.cache.get(bucket, key, None).await?;
        let plen = out.content_length().unwrap_or(0).max(0) as u64;
        // Single-part client ETag == the cache's own MD5 (composites route around this path, §7);
        // the trailer recomputes it from the streamed body, so validating the shape here suffices.
        let cetag = out.e_tag().unwrap_or_default().trim_matches('"');
        if hex::decode(cetag).map(|b| b.len()) != Ok(16) {
            return Err(Error::Backend(format!(
                "cache ETag for {key:?} is not an MD5"
            )));
        }
        let mtime_ms = out
            .last_modified()
            .map(|t| t.to_millis().unwrap_or_default())
            .unwrap_or_else(now_ms);
        let body = out.body;

        let trailer = SingleTrailer {
            trailer_key: self.trailer_key.clone(),
            object_key: key.to_string(),
            mtime_ms,
        };
        let (framed_len, enc) = codec::encrypt_stream(self.env.clone(), body, plen, trailer)
            .await
            .map_err(Error::Io)?;
        self.remote
            .put(
                bucket,
                key,
                enc,
                Some(framed_len as i64),
                HashMap::new(),
                None,
                None,
            )
            .await?;
        Ok(())
    }

    /// Replace the cache body at `key` with an eviction tombstone (the phase-5 GC transition).
    /// Facts are read from the cache body itself (one HEAD) rather than trusted from the caller;
    /// twin-before-tombstone (§8) refreshes the facts twin, then the body is overwritten
    /// conditional on its current ETag so a concurrent writer aborts us. Assumes the caller holds
    /// `key`'s lock.
    ///
    /// `remote_confirmed`: the caller already knows the remote copy is present. Pass `false`
    /// from the cached-mode GC, which must gate tombstoning on a successful remote HEAD (§7).
    #[allow(dead_code)] // phase 5: the GC scavenger's eviction transition
    pub(crate) async fn tombstone_locked(
        &self,
        bucket: &str,
        key: &str,
        remote_confirmed: bool,
    ) -> Result<()> {
        let head = self.cache.head(bucket, key).await?;
        let body_etag = head
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let plen = head.content_length().unwrap_or(0).max(0) as u64;
        // Eviction must not move the key's client-visible LastModified (§6).
        let mtime_ms = head
            .last_modified()
            .map(|t| t.to_millis().unwrap_or_default())
            .unwrap_or_else(now_ms);
        if !remote_confirmed {
            // Durability-gates-GC (§7): never tombstone a body whose ciphertext isn't on the remote.
            self.remote.head(bucket, key).await?;
        }

        let facts = meta::Facts {
            client_etag: body_etag.clone(),
            plen,
            mtime_ms,
        };
        self.refresh_twin(bucket, key, &facts).await?;

        let mut md = HashMap::new();
        md.insert(meta::TOMB.to_string(), meta::TOMB_EVICT.to_string());
        md.insert(meta::PLEN.to_string(), plen.to_string());
        md.insert(meta::CETAG.to_string(), body_etag.clone());
        md.insert(meta::MTIME.to_string(), mtime_ms.to_string());
        self.cache
            .put_small(
                bucket,
                key,
                meta::EVICT_SENTINEL.to_vec(),
                md,
                Some(quote(&body_etag)),
                None,
            )
            .await?;
        Ok(())
    }

    /// Delete any stale twins of `key`, then write the fresh zero-byte twin. A crash between
    /// leaves only a twin next to a non-evict entry — ignored by the classification gate (§6)
    /// and swept later.
    async fn refresh_twin(&self, bucket: &str, key: &str, facts: &meta::Facts) -> Result<()> {
        self.delete_twins(bucket, key).await?;
        self.cache
            .put_small(
                bucket,
                &facts.twin_key(key),
                Vec::new(),
                HashMap::new(),
                None,
                None,
            )
            .await?;
        Ok(())
    }

    /// Delete every twin of `key` (the `key ‖ 0x01 …` suffix range). Twins carry the `0x01`
    /// separator, which XML 1.0 cannot represent at all — so they must go through single-object
    /// `DeleteObject` (key in the percent-encoded URL path), never the batch `DeleteObjects` whose
    /// XML body would be rejected as malformed. There is ≤ 1 twin per key in steady state (refresh
    /// deletes the stale one before writing the new); the rare multi-twin cleanup fires the
    /// per-key deletes concurrently.
    pub(crate) async fn delete_twins(&self, bucket: &str, key: &str) -> Result<()> {
        let sep = format!("{}{}", key, meta::TWIN_SEP as char);
        let existing = self
            .cache
            .list(bucket, Some(sep), None, None, None, None)
            .await?;
        let deletes = existing
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|obj| obj.key)
            .map(|twin| async move { self.cache.delete(bucket, &twin).await });
        futures::future::try_join_all(deletes).await?;
        Ok(())
    }
}

fn bad_facts(key: &str) -> Error {
    Error::Backend(format!("remote object {key:?} carries no hypha facts"))
}

/// Total object length from a `Content-Range: bytes <start>-<end>/<total>` header (the response to
/// a suffix-range GET). `None` if the header is malformed or the size is unknown (`*`).
fn parse_content_range_total(cr: &str) -> Option<u64> {
    cr.rsplit_once('/')?.1.trim().parse().ok()
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// S3 ETags are quoted on the wire; conditions must match that form.
pub(crate) fn quote(etag: &str) -> String {
    format!("\"{}\"", etag.trim_matches('"'))
}
