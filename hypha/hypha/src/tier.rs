//! The shared tiering machinery: the §7 transition-bracket primitives (mark / settle / repair),
//! encrypt-and-upload of a cache body, and tombstoning once ciphertext is durable on the remote.
//! All of it serializes on the per-key lock ([`KeyLocks`]); the durable path calls these inline
//! while holding the key lock, and the cached path's background reconcile and GC will call the
//! same primitives (Phases 4–5).

use std::collections::HashMap;
use std::ops::Range as ByteRange;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use hypha_format::{decode_tail, Tail, TrailerKey};
use hypha_format::{Envelope, MAX_TAIL_LEN, SINGLE_TRAILER_LEN};
use s3s::dto::StreamingBlob;

use hypha_core::error::{Error, Result};
use hypha_core::{meta, Backend};

use crate::codec::{self, PartSegment, SingleTrailer};
use crate::keylocks::KeyLocks;

#[derive(Clone)]
pub struct Reconciler {
    /// `<data>` cache bucket: client bodies and tombstones at bare `K` (§6).
    pub data: Backend,
    /// `<meta>` cache bucket: facts twins, pending markers, mpu records (§6).
    pub meta: Backend,
    pub remote: Backend,
    pub env: Arc<Envelope>,
    /// Keys the tail trailer's authentication tag (§6); derived once from the master passphrase.
    pub trailer_key: TrailerKey,
    /// The **write** lock table (§4): conditional writes, the durable finalize, GC tombstone
    /// transitions, and rehydrate all serialize on it.
    pub locks: KeyLocks,
    /// The **upload** lock table (§4) — a *second* instance, reconcile-only. Same-key reconcile
    /// passes serialize here so an unserialized older upload can't finish after a newer one and leave
    /// the remote stale, while a replication upload never blocks a client's conditional PUT (which
    /// takes `locks`, not this).
    pub upload_locks: KeyLocks,
}

/// What one reconcile upload attempt found at K (§7). The marker is cleared only on a real upload;
/// the other two arms leave it for the pass that owns that transition.
pub(crate) enum UploadOutcome {
    /// A live client body was encrypted and PUT to the remote.
    Uploaded,
    /// K is a tombstone — a cached delete raced in under the write lock. The delete branch (driven by
    /// the marker that delete rewrote) propagates it; this pass leaves the marker be.
    SkippedTombstone,
    /// K is gone from the cache entirely: the marker is an orphan to clear.
    Vanished,
}

/// The plaintext facts of a committed remote object, read off its tail footer (§6).
#[derive(Clone, Debug)]
pub(crate) struct RemoteFacts {
    pub plen: u64,
    pub cetag: String,
    pub mtime_ms: i64,
}

impl RemoteFacts {
    /// A live cache body's facts, off its native HEAD: the cache body is plaintext, so the native
    /// size/ETag/mtime already are the client-visible facts.
    pub(crate) fn from_cache_head(head: &HeadObjectOutput) -> Self {
        RemoteFacts {
            plen: head.content_length.unwrap_or(0).max(0) as u64,
            cetag: head
                .e_tag
                .as_deref()
                .unwrap_or_default()
                .trim_matches('"')
                .to_string(),
            mtime_ms: head
                .last_modified
                .and_then(|t| t.to_millis().ok())
                .unwrap_or_default(),
        }
    }
}

impl Reconciler {
    // ── The transition bracket (§7) ─────────────────────────────────────────────────────────

    /// **Mark**: overwrite K's cache entry with the transition tombstone. Readers resolve K from
    /// the remote until settle. Caller holds K's write lock — a mark is only ever *observed* by
    /// lock-free readers mid-bracket or by anyone after a crash.
    pub(crate) async fn mark_transit_locked(&self, bucket: &str, key: &str) -> Result<()> {
        let mut md = HashMap::new();
        md.insert(meta::TOMB.to_string(), meta::TOMB_TRANSIT.to_string());
        self.data
            .put_small(bucket, key, meta::TRANSIT_SENTINEL.to_vec(), md, None, None)
            .await?;
        Ok(())
    }

    /// **Settle** after a commit that left K present on the remote: fresh twin, then the
    /// eviction tombstone carrying the full facts (kind, cetag, plen, original mtime) in its
    /// user-metadata — the authoritative copy; the twin is its LIST projection (§6).
    ///
    /// `extra` is the pass-through carrier those facts share (§7): the client's namespaced
    /// `x-amz-meta-*` and the echoed storage class. It is cache-only — the remote's trailer holds
    /// facts and nothing else — so a rebuild from the remote settles it empty, which is what
    /// [`Self::repair_locked`] does.
    pub(crate) async fn settle_evict_locked(
        &self,
        bucket: &str,
        key: &str,
        plen: u64,
        cetag: &str,
        mtime_ms: i64,
        extra: HashMap<String, String>,
    ) -> Result<()> {
        let facts = meta::Facts {
            client_etag: cetag.to_string(),
            plen,
            mtime_ms,
        };
        self.refresh_twin(bucket, key, &facts).await?;

        let mut md = extra;
        md.insert(meta::TOMB.to_string(), meta::TOMB_EVICT.to_string());
        md.insert(meta::PLEN.to_string(), plen.to_string());
        md.insert(meta::CETAG.to_string(), cetag.to_string());
        md.insert(meta::MTIME.to_string(), mtime_ms.to_string());
        self.data
            .put_small(bucket, key, meta::EVICT_SENTINEL.to_vec(), md, None, None)
            .await?;
        Ok(())
    }

    /// **Settle** after a commit that removed K from the remote: absent is the authoritative 404.
    pub(crate) async fn settle_absent_locked(&self, bucket: &str, key: &str) -> Result<()> {
        self.delete_twins(bucket, key).await?;
        self.data.delete(bucket, key).await?;
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
        self.settle_evict_locked(
            bucket,
            key,
            facts.plen,
            &facts.cetag,
            facts.mtime_ms,
            HashMap::new(),
        )
        .await?;
        Ok(Some(facts))
    }

    /// Rebuild a bucket's cache namespace from the remote — the §7 restore sweep, per bucket. LIST
    /// the remote and materialize each object's eviction tombstone + twin from its authenticated
    /// tail trailer ([`Self::repair_locked`]), then write the sync marker so the bucket flips
    /// cache-authoritative. Idempotent (repair only re-settles what a key already resolves to), so a
    /// crash mid-sweep resumes by re-running — the marker, written last, is the only "done" signal.
    /// Assumes the cache buckets already exist.
    pub(crate) async fn restore_bucket(&self, bucket: &str) -> Result<()> {
        let mut token = None;
        loop {
            let page = self
                .remote
                .list(bucket, None, None, token, None, Some(1000))
                .await?;
            let keys: Vec<String> = page
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|o| o.key)
                .collect();
            for key in keys {
                let _guard = self.locks.lock(&key).await;
                self.repair_locked(bucket, &key).await?;
            }
            match page.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        self.meta
            .put_small(
                bucket,
                &meta::sync_marker_key(),
                Vec::new(),
                HashMap::new(),
                None,
                None,
            )
            .await?;
        Ok(())
    }

    /// Resolve a remote object's plaintext facts from its tail trailer (§6): **one speculative tail
    /// read**, single-part and composite alike — the trailer carries the complete facts either way,
    /// and its kind/count distinguish the two. Mid-bracket reads, repair, and the restore sweep all
    /// resolve through here. The HEAD supplies the mtime fallback only. A trailer that does not
    /// authenticate breaks the sole-writer assumption and is fatal ([`hypha_core::fatal`]).
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

        let Some(tail) = self.read_tail(bucket, key).await? else {
            hypha_core::fatal::foreign_object(bucket, key)
        };
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
    /// The reconciler serializes same-key passes on the dedicated per-key **upload** lock
    /// ([`Self::upload_locks`]), not the write lock — held across the whole upload + marker CAS.
    /// Unserialized same-key uploads can finish out of order and leave the remote stale with an empty
    /// pending set (§7); the separate instance keeps a conditional PUT from ever queuing behind a
    /// multi-second transfer.
    ///
    /// The cache GET yields `plen`, ETag, and body from **one** response, so the framed facts can't
    /// disagree with the uploaded bytes. Reconcile takes only the upload lock, so a cached delete
    /// (which takes the *write* lock) can overwrite K with a tombstone concurrently — hence the
    /// classify guard: a sentinel body is never uploaded as if it were a client object.
    pub(crate) async fn upload_locked(&self, bucket: &str, key: &str) -> Result<UploadOutcome> {
        let out = match self.data.get(bucket, key, None).await {
            Ok(o) => o,
            Err(Error::NotFound) => return Ok(UploadOutcome::Vanished),
            Err(e) => return Err(e),
        };
        let plen = out.content_length().unwrap_or(0).max(0) as u64;
        let cetag = out
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        // A tombstone sentinel raced in (a concurrent cached delete under the write lock): body and
        // ETag are the compiled sentinel, classifiable with no metadata read (§6). Don't upload it. A
        // *client* body can't spoof this: cached PUT rejects any body equal to a sentinel at write
        // time (`meta::is_reserved_sentinel`), so a sentinel here is always hypha's own tombstone.
        if meta::classify_entry(plen as i64, &cetag).is_some() {
            return Ok(UploadOutcome::SkippedTombstone);
        }
        // Single-part client ETag == the cache's own MD5 (composites route around this path, §7);
        // the trailer recomputes it from the streamed body, so validating the shape here suffices.
        if hex::decode(&cetag).map(|b| b.len()) != Ok(16) {
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
                None,
            )
            .await?;
        Ok(UploadOutcome::Uploaded)
    }

    /// **Delete branch** of the reconcile sweep (§7), under K's upload lock. A cached delete left a
    /// delete-tombstone + a rewritten marker; propagate it: remote `DeleteObject` (the commit),
    /// clear the delete-tombstone (conditional on the delete sentinel — a concurrent create moved the
    /// ETag, so its 412 leaves the new body), then clear the marker (conditional on `M_etag`). The
    /// remote delete must be serialized against same-key uploads or a stale in-flight upload could
    /// land bytes *after* the delete and resurrect K at the next restore sweep — which the shared
    /// upload lock ensures.
    pub(crate) async fn propagate_delete_locked(
        &self,
        bucket: &str,
        key: &str,
        m_etag: &str,
    ) -> Result<()> {
        match self.remote.delete(bucket, key).await {
            Ok(()) | Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        match self
            .data
            .delete_if_match(bucket, key, quote(&meta::delete_sentinel_etag()))
            .await
        {
            Ok(()) | Err(Error::PreconditionFailed) | Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        self.clear_marker_cas(bucket, key, m_etag).await
    }

    /// Clear the pending marker at bare `K`, conditional on its `M_etag` (§7). A PUT that landed a
    /// newer body mid-pass rewrote the marker, so the CAS 412s and the next pass uploads that
    /// version — the remote is transiently one version behind, never stale with an empty pending set.
    /// A 404 (already cleared) is equally fine.
    pub(crate) async fn clear_marker_cas(
        &self,
        bucket: &str,
        key: &str,
        m_etag: &str,
    ) -> Result<()> {
        match self
            .meta
            .delete_if_match(bucket, meta::pending_marker_key(key), quote(m_etag))
            .await
        {
            Ok(()) | Err(Error::PreconditionFailed) | Err(Error::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// **Rehydrate** a single-part body back to a live cache entry at K (§8): the decrypted plaintext
    /// (streamed by the caller from the remote) overwrites the eviction tombstone, conditional on the
    /// evict sentinel so a concurrent write/rehydrate aborts us. The sentinel ETag is constant across
    /// generations, so the CAS alone can't see an evict→rehydrate→re-evict ABA — the caller re-reads
    /// the tombstone's `cetag` under the lock for that ([`crate::background`]). `md` is the
    /// tombstone's client pass-through ([`meta::passthrough_metadata`]).
    ///
    /// The caller drops K's twin ([`Self::delete_twins`]) once this returns, making K's facts native
    /// again. Deliberately *not* done here: this PUT is what drives the remote fetch, so it is the
    /// long, cancellable half of a rehydrate (§8), while the twin drop must run to completion — a
    /// cancel between the two would leave a live body beside a stale twin (benign, ignored by the
    /// LIST gate per §6, but debris nothing else reclaims until phase 5).
    pub(crate) async fn land_rehydrated_single_locked(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        plen: u64,
        md: HashMap<String, String>,
    ) -> Result<()> {
        self.data
            .put(
                bucket,
                key,
                body,
                Some(plen as i64),
                md,
                None,
                Some(quote(&meta::evict_sentinel_etag())),
                None,
            )
            .await?;
        Ok(())
    }

    /// Whether K's shadow body already holds the current composite generation (its stored digest and
    /// ETag both match) — a HEAD, no body fetch. Lets a rehydrate skip re-downloading a composite an
    /// earlier read already landed (§8).
    pub(crate) async fn shadow_is_current(
        &self,
        bucket: &str,
        key: &str,
        cetag: &str,
    ) -> Result<bool> {
        match self.meta.head(bucket, &meta::shadow_key(key)).await {
            Ok(h) => Ok(shadow_matches(h.metadata(), key, cetag)),
            Err(Error::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Decrypt a remote-resident object into a plaintext stream — the whole body, or a plaintext
    /// sub-range `pt` (§7). Single-part goes through `decrypt_full`/`decrypt_range`; a composite
    /// recovers its parts table from one tail read and decrypts part-by-part. Shared by GET's remote
    /// path, UploadPartCopy's re-encrypt source read, and the background rehydrate (§8).
    pub(crate) async fn decrypt_remote_body(
        &self,
        bucket: &str,
        key: &str,
        cetag: &str,
        pt: Option<ByteRange<u64>>,
    ) -> Result<StreamingBlob> {
        if meta::is_composite_etag(cetag) {
            // The trailer's parts table (recovered in one tail read) gives every part's ciphertext
            // window and plaintext length — no remote part-index calls.
            let Some(tail) = self.read_tail(bucket, key).await? else {
                hypha_core::fatal::foreign_object(bucket, key)
            };
            Ok(match &pt {
                // Whole object: one GET of the concatenated parts, decrypted part-by-part in-stream.
                None => {
                    let out = self
                        .remote
                        .get(
                            bucket,
                            key,
                            Some(format!("bytes=0-{}", tail.body_ct_len - 1)),
                        )
                        .await?;
                    let part_lens = tail.windows.iter().map(|w| w.end - w.start).collect();
                    codec::decrypt_composite_full(self.env.clone(), out.body, part_lens)
                }
                // Range: fetch only the parts it touches.
                Some(pt) => {
                    let segments = composite_segments(&tail.windows, &tail.plens, pt);
                    codec::decrypt_composite(
                        self.env.clone(),
                        self.remote.clone(),
                        bucket.to_string(),
                        key.to_string(),
                        segments,
                    )
                }
            })
        } else {
            Ok(match &pt {
                None => {
                    let out = self.remote.get(bucket, key, None).await?;
                    let ct_len = envelope_len(key, out.content_length)?;
                    codec::decrypt_full(self.env.clone(), out.body, ct_len)
                }
                Some(pt) => {
                    let rhead = self.remote.head(bucket, key).await?;
                    let ct_len = envelope_len(key, rhead.content_length)?;
                    codec::decrypt_range(
                        self.env.clone(),
                        self.remote.clone(),
                        bucket.to_string(),
                        key.to_string(),
                        ct_len,
                        pt.clone(),
                    )
                }
            })
        }
    }

    /// **Rehydrate** a composite into its shadow body (§8): a rehydrated composite's plaintext lives
    /// at `sha256(K)`-keyed [`meta::shadow_key`] (a point lookup, so K's length can't constrain it),
    /// with the full-width key digest in metadata for the read-time collision check. K's tombstone
    /// and twin stay untouched, so composite rehydration is invisible to LIST/HEAD and rewrites no
    /// twin. Caller holds K's write lock.
    ///
    /// The shadow key is deterministic in K, so a later composite at K overwrites the *same* shadow —
    /// but a same-K key digest doesn't distinguish generations. `cetag` (the rehydrated composite's
    /// client ETag) rides the metadata and is checked against the current tombstone on read, so a
    /// shadow left over from a superseded generation misses and re-rehydrates rather than serving
    /// stale bytes under the new ETag.
    pub(crate) async fn land_shadow_locked(
        &self,
        bucket: &str,
        key: &str,
        body: ByteStream,
        plen: u64,
        cetag: &str,
    ) -> Result<()> {
        let mut md = HashMap::new();
        md.insert(
            meta::SHADOW_KEY_DIGEST.to_string(),
            meta::shadow_key_digest(key),
        );
        md.insert(meta::CETAG.to_string(), cetag.to_string());
        self.meta
            .put(
                bucket,
                &meta::shadow_key(key),
                body,
                Some(plen as i64),
                md,
                None,
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
        let head = self.data.head(bucket, key).await?;
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
        self.data
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

    /// Delete any stale twins of `key` in `<meta>`, then write the fresh zero-byte twin. A key over
    /// the §6 twin threshold gets **no** twin (`twin_key` is `None`) and resolves through LIST's
    /// HEAD fallback instead — the tombstone metadata is authoritative either way. A crash between
    /// leaves only a twin whose base key is a live/absent entry — ignored by the LIST gate (§6),
    /// swept later.
    async fn refresh_twin(&self, bucket: &str, key: &str, facts: &meta::Facts) -> Result<()> {
        self.delete_twins(bucket, key).await?;
        if let Some(twin_key) = facts.twin_key(key) {
            self.meta
                .put_small(bucket, &twin_key, Vec::new(), HashMap::new(), None, None)
                .await?;
        }
        Ok(())
    }

    /// Delete `key`'s twin from `<meta>` (range B, `0x01 ‖ key ‖ 0x01 ‖ …`). Twin keys carry the
    /// `0x01` control byte, which XML 1.0 cannot represent — so they must go through single-object
    /// `DeleteObject` (key in the percent-encoded URL path), never the batch `DeleteObjects` whose
    /// XML body would be rejected as malformed. There is ≤ 1 twin per key in steady state (refresh
    /// deletes the stale one before writing the new); the rare multi-twin cleanup fires the per-key
    /// deletes concurrently.
    pub(crate) async fn delete_twins(&self, bucket: &str, key: &str) -> Result<()> {
        let c = meta::CTRL as char;
        let prefix = format!("{c}{key}{c}");
        let existing = self
            .meta
            .list(bucket, Some(prefix), None, None, None, None)
            .await?;
        let deletes = existing
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|obj| obj.key)
            .map(|twin| async move { self.meta.delete(bucket, &twin).await });
        futures::future::try_join_all(deletes).await?;
        Ok(())
    }
}

/// Total object length from a `Content-Range: bytes <start>-<end>/<total>` header (the response to
/// a suffix-range GET). `None` if the header is malformed or the size is unknown (`*`).
fn parse_content_range_total(cr: &str) -> Option<u64> {
    cr.rsplit_once('/')?.1.trim().parse().ok()
}

/// Whether a shadow body's metadata identifies it as K's *current* composite generation (§6/§8):
/// the full key digest must match (the shadow key is only a 160-bit prefix) **and** the stored client
/// ETag must equal `cetag` (the live tombstone's), so a shadow left over from a superseded generation
/// is treated as a miss.
pub(crate) fn shadow_matches(
    metadata: Option<&HashMap<String, String>>,
    key: &str,
    cetag: &str,
) -> bool {
    let Some(md) = metadata else { return false };
    md.get(meta::SHADOW_KEY_DIGEST) == Some(&meta::shadow_key_digest(key))
        && md.get(meta::CETAG).map(String::as_str) == Some(cetag)
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

/// The age-envelope length of a single-part remote object: its Content-Length minus the tail
/// trailer, which must never reach the decryptor (§6).
fn envelope_len(key: &str, content_length: Option<i64>) -> Result<u64> {
    let framed = content_length
        .filter(|&n| n >= 0)
        .ok_or_else(|| Error::Backend("remote response missing content-length".into()))?
        as u64;
    framed
        .checked_sub(SINGLE_TRAILER_LEN as u64)
        .ok_or_else(|| Error::Backend(format!("remote object {key:?} shorter than a trailer")))
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
