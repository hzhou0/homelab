//! The shared tiering machinery: encrypt-and-upload a cache body to the remote, and tombstone a
//! cache body once its ciphertext is durable there. Both operations serialize on the per-key lock
//! ([`KeyLocks`]) so same-key writes never overlap or reorder. The durable path calls these inline
//! while holding the key lock; the cached path's background reconcile and GC will call the same
//! primitives (Phases 4–5).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use hypha_format::Envelope;

use hypha_core::error::{Error, Result};
use hypha_core::{meta, Backend};

use crate::codec;
use crate::keylocks::KeyLocks;

#[derive(Clone)]
pub struct Reconciler {
    pub cache: Backend,
    pub remote: Backend,
    pub env: Arc<Envelope>,
    pub locks: KeyLocks,
}

impl Reconciler {
    /// Encrypt the current cache body at `key` and PUT it to the remote at `key`, stamping the
    /// plaintext facts the §10 restore sweep needs. `plen` and `cetag` are read from the *same*
    /// GET response that streams the body, so the ciphertext length can never disagree with the
    /// bytes uploaded. Assumes the caller holds `key`'s lock.
    ///
    /// Phase 4 note: the cached-mode reconciler must NOT hold the lock for this step — only for
    /// the tombstone. Remote PUTs are unconditional and idempotent; lock contention during a large
    /// upload would block same-key PUTs for seconds. Split: upload without lock → acquire lock →
    /// call `tombstone_locked`.
    pub(crate) async fn upload_locked(&self, key: &str) -> Result<()> {
        let out = self.cache.get(key, None).await?;
        let plen = out.content_length().unwrap_or(0).max(0) as u64;
        // Single-part client ETag == the cache's own MD5 (composites route around this path, §7).
        let cetag = out.e_tag().unwrap_or_default().trim_matches('"').to_string();
        let body = out.body;

        let (ct_len, enc) = codec::encrypt_stream(self.env.clone(), body, plen)
            .await
            .map_err(Error::Io)?;
        let mut md = HashMap::new();
        md.insert(meta::PLEN.to_string(), plen.to_string());
        md.insert(meta::CETAG.to_string(), cetag);
        self.remote
            .put(key, enc, Some(ct_len as i64), md, None, None)
            .await?;
        Ok(())
    }

    /// Replace the cache body at `key` with an eviction tombstone. Facts are read from the cache
    /// body itself (one HEAD) rather than trusted from the caller; twin-before-tombstone (§8)
    /// refreshes the facts twin, then the body is overwritten conditional on its current ETag so a
    /// concurrent writer aborts us. Assumes the caller holds `key`'s lock.
    ///
    /// `remote_confirmed`: the caller already knows the remote copy is present (e.g. durable-mode
    /// PUT, which just finished `upload_locked`). Pass `false` from the cached-mode reconciler,
    /// which must gate tombstoning on a successful remote HEAD (§7).
    pub(crate) async fn tombstone_locked(&self, key: &str, remote_confirmed: bool) -> Result<()> {
        let head = self.cache.head(key).await?;
        let body_etag = head.e_tag().unwrap_or_default().trim_matches('"').to_string();
        let plen = head.content_length().unwrap_or(0).max(0) as u64;
        if !remote_confirmed {
            // Durability-gates-GC (§7): never tombstone a body whose ciphertext isn't on the remote.
            self.remote.head(key).await?;
        }

        let facts = meta::Facts {
            kind: meta::TOMB_EVICT.to_string(),
            bound_etag: meta::evict_sentinel_etag(),
            plen,
            client_etag: body_etag.clone(),
            mtime_ms: now_ms(),
        };
        self.refresh_twin(key, &facts).await?;

        let mut md = HashMap::new();
        md.insert(meta::TOMB.to_string(), meta::TOMB_EVICT.to_string());
        md.insert(meta::PLEN.to_string(), plen.to_string());
        md.insert(meta::CETAG.to_string(), body_etag.clone());
        self.cache
            .put_small(key, meta::EVICT_SENTINEL.to_vec(), md, Some(quote(&body_etag)), None)
            .await?;
        Ok(())
    }

    /// Delete any stale twins of `key`, then write the fresh zero-byte twin. A crash between
    /// leaves only a twin next to a live body — unbound (its `bound_etag` ≠ the body's), so LIST
    /// falls back to HEAD and the sweep collects it (§9).
    async fn refresh_twin(&self, key: &str, facts: &meta::Facts) -> Result<()> {
        self.delete_twins(key).await?;
        self.cache
            .put_small(&facts.twin_key(key), Vec::new(), HashMap::new(), None, None)
            .await?;
        Ok(())
    }

    /// Write an eviction tombstone to the cache with caller-supplied facts, skipping the cache and
    /// remote HEADs that `tombstone_locked` performs. Used by the durable PUT path, which computed
    /// `plen`/`cetag` inline during encryption and confirmed remote durability via a successful PUT.
    /// The unconditional cache write is safe because the caller holds `key`'s lock. Assumes the
    /// caller holds `key`'s lock.
    pub(crate) async fn tombstone_with_facts_locked(
        &self,
        key: &str,
        plen: u64,
        cetag: &str,
    ) -> Result<()> {
        let facts = meta::Facts {
            kind: meta::TOMB_EVICT.to_string(),
            bound_etag: meta::evict_sentinel_etag(),
            plen,
            client_etag: cetag.to_string(),
            mtime_ms: now_ms(),
        };
        self.refresh_twin(key, &facts).await?;

        let mut md = HashMap::new();
        md.insert(meta::TOMB.to_string(), meta::TOMB_EVICT.to_string());
        md.insert(meta::PLEN.to_string(), plen.to_string());
        md.insert(meta::CETAG.to_string(), cetag.to_string());
        self.cache
            .put_small(key, meta::EVICT_SENTINEL.to_vec(), md, None, None)
            .await?;
        Ok(())
    }

    /// Delete every twin of `key` (the `key ‖ 0x01 …` suffix range).
    pub(crate) async fn delete_twins(&self, key: &str) -> Result<()> {
        let sep = format!("{}{}", key, meta::TWIN_SEP as char);
        let existing = self.cache.list(Some(sep), None, None, None, None).await?;
        for obj in existing.contents.unwrap_or_default() {
            if let Some(full) = obj.key {
                let client_key = self.cache.strip(&full).to_string();
                self.cache.delete(&client_key).await?;
            }
        }
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// S3 ETags are quoted on the wire; conditions must match that form.
fn quote(etag: &str) -> String {
    format!("\"{}\"", etag.trim_matches('"'))
}
