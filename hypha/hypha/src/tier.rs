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

/// One LIST page while walking a namespace.
const SCAN_PAGE: i32 = 1000;

/// The framed size a single-part remote object would have for a `plen`-byte plaintext (§6). A
/// markerless live body is always single-part — a composite is tombstoned at K with its plaintext in
/// the shadow — so this is exact where [`Reconciler::classify_cache_entry`] applies it.
fn single_part_framed_len(plen: u64) -> Option<u64> {
    hypha_format::offset::ciphertext_len(plen, hypha_format::offset::HLEN)
        .checked_add(SINGLE_TRAILER_LEN as u64)
}

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
    /// passes mutually exclude here so an unserialized older upload can't finish after a newer one
    /// and leave the remote stale, while a replication upload never blocks a client's conditional PUT
    /// (which takes `locks`, not this). Held via `try_lock`: a pass that finds K busy coalesces onto
    /// the in-flight upload rather than queuing, so this table never accumulates waiters.
    pub upload_locks: KeyLocks,
    /// Cached mode (§4). Decides who wins when a surviving cache entry and the remote disagree: in
    /// cached mode the cache write *is* the commit, so a generation the remote lacks is an acked
    /// write still owed to it; in durable mode the remote is the commit, so the same divergence is
    /// a stale projection. See [`Self::classify_cache_entry`].
    pub cached: bool,
}

/// What a surviving cache entry at K means once reconciled against the remote (§7) — the shared
/// verdict driving [`Reconciler::reconcile_bucket`], which both restores an untrusted namespace and
/// rebuilds a bucket's pending set (§7) — one traversal, because both jobs turn on this one
/// question.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CacheVerdict {
    /// The cache holds a committed write the remote lacks — an acked PUT, or a delete it has not
    /// honoured. **Cache wins**: the entry stands as written, and owes a pending marker.
    Pending,
    /// Cache and remote already agree. Leave K exactly as it is.
    Agrees,
    /// The cache entry is unresolved, or contradicted by the remote. **Remote wins**: settle K by
    /// the repair rule ([`Reconciler::repair_locked`]).
    Stale,
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

    /// Reconcile a bucket's cache namespace with the remote — the §7 restore sweep, per bucket.
    /// Idempotent, so a crash mid-pass resumes by re-running. Assumes the cache buckets already
    /// exist; the caller writes the sync marker once this returns, which is the only "done" signal.
    ///
    /// This is **also** the recovery scan (§7): rebuilding the pending-marker index and settling the
    /// namespace are the same traversal over the same two listings, and both hinge on the same
    /// question — which side of a divergence at K is the committed one. Splitting them was a bug
    /// factory: two snapshots of the same state, taken at different moments and acted on
    /// independently, could disagree, and a scan calling K pending where restore called it stale
    /// would raise a marker over a key restore had just overwritten (an inert marker no sweep ever
    /// clears). One pass over one map cannot. So the pass does both jobs at once — `Pending` keys
    /// get their marker, everything else gets settled — and the two callers differ only in what
    /// made them ask (see [`crate::bucket_ctl`]).
    ///
    /// **Bidirectional**, because restore is not only a rebuild-from-nothing. A lost cache volume
    /// is the easy case — the cache walk finds no entries and every remote key materializes. But
    /// restore also runs on a bucket whose cache *survived* and whose marker did not: a crash
    /// mid-sweep, a crash before a clean marker, a `reset_cache` that failed partway. There the
    /// cache holds committed state the remote has not got yet, and in cached mode the cache write
    /// **is** the ack — so a remote-wins sweep would settle an acked PUT down to the older remote
    /// generation and resurrect an acked DELETE as a live object. Both are silent losses of a write
    /// hypha already told a client had succeeded, which no later pass can detect: the sweep's
    /// classify sees a well-formed eviction tombstone and the pending marker it can no longer
    /// explain is cleared as an orphan.
    ///
    /// So the pass walks **both** namespaces and settles every key of their union by
    /// [`Self::classify_cache_entry`]. Cached-mode writes that win keep their body or tombstone
    /// untouched and have their pending marker re-raised, so the reconcile sweep pushes them out
    /// once the bucket is ready — this pass hands them back to the normal pending path rather than
    /// completing them itself. Returns how many markers it raised.
    ///
    /// Both listings are resident, bounded by one bucket's key count.
    pub(crate) async fn reconcile_bucket(&self, bucket: &str) -> Result<usize> {
        // Framed sizes, so the merge settles most keys without a per-key remote HEAD: a remote
        // object's framed length is the closed form over its plaintext length, so an overwrite that
        // changed that length is caught from the listing alone (§6).
        let mut remote = HashMap::new();
        self.walk(&self.remote, bucket, |k, size, _| {
            remote.insert(k, size);
        })
        .await?;

        let mut cache = HashMap::new();
        self.walk(&self.data, bucket, |k, size, etag| {
            cache.insert(k, (size, etag));
        })
        .await?;

        let keys: Vec<String> = cache
            .keys()
            .cloned()
            .chain(remote.keys().filter(|k| !cache.contains_key(*k)).cloned())
            .collect();

        let mut raised = 0;
        for key in keys {
            let remote_framed = remote.get(&key).copied();
            let _guard = self.locks.lock(&key).await;

            let mut entry = cache.get(&key).cloned();
            let mut verdict = self
                .verdict_for(bucket, &key, &entry, remote_framed)
                .await?;

            // Both walks are snapshots, and serving is never gated during a restore (§7) — a client
            // PUT or DELETE can have acked at K in the interval, and in cached mode that ack *is*
            // the commit. Settling K on a stale listing would destroy it exactly as a remote-wins
            // sweep would. So before the one arm that overwrites, re-read K now that the lock pins
            // it, and re-decide. Only that arm pays the HEAD: `Pending` re-raises a marker (last
            // writer wins) and `Agrees` writes nothing, so neither can lose a racing write.
            if self.cached && verdict == CacheVerdict::Stale {
                entry = self.cache_entry(bucket, &key).await?;
                verdict = self
                    .verdict_for(bucket, &key, &entry, remote_framed)
                    .await?;
            }

            match verdict {
                // The entry stands as written; the marker hands it to the reconcile sweep once the
                // bucket is ready, rather than this pass completing the write itself. Only a
                // classified entry is ever `Pending`, so the `None` arm is unreachable.
                CacheVerdict::Pending => {
                    if let Some((_, etag)) = &entry {
                        self.raise_marker(bucket, &key, etag).await?;
                        raised += 1;
                    }
                }
                CacheVerdict::Agrees => {}
                CacheVerdict::Stale => {
                    self.repair_locked(bucket, &key).await?;
                }
            }
        }

        Ok(raised)
    }

    /// [`Self::classify_cache_entry`] over a cache entry that may not exist: no entry means K is
    /// remote-only, which the repair rule materializes.
    async fn verdict_for(
        &self,
        bucket: &str,
        key: &str,
        entry: &Option<(u64, String)>,
        remote_framed: Option<u64>,
    ) -> Result<CacheVerdict> {
        match entry {
            Some((size, etag)) => {
                self.classify_cache_entry(bucket, key, *size, etag, remote_framed)
                    .await
            }
            None => Ok(CacheVerdict::Stale),
        }
    }

    /// K's `<data>` entry as `(size, ETag)`, or `None` if the cache does not hold one.
    async fn cache_entry(&self, bucket: &str, key: &str) -> Result<Option<(u64, String)>> {
        match self.data.head(bucket, key).await {
            Ok(h) => Ok(Some((
                h.content_length().unwrap_or(0).max(0) as u64,
                h.e_tag().unwrap_or_default().trim_matches('"').to_string(),
            ))),
            Err(Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Reconcile one surviving cache entry against the remote (§7). `size`/`etag` are K's `<data>`
    /// entry as listed, `remote_framed` the framed size of the remote object at K if it holds one.
    /// What makes a cached write pending is a live body whose generation the remote lacks, or a
    /// delete-tombstone the remote has not honoured — derivable from cache and remote alone, which
    /// is exactly why a lost pending marker costs durability rather than the write (§7).
    ///
    /// Triage keeps this to the two listings in the common case: a key the remote lacks diverges
    /// outright, and for one it holds, a single-part object's framed size is the closed form over
    /// the cache body's plaintext length — so any overwrite that changed that length is caught with
    /// no extra request. Only a same-length overwrite is ambiguous, and only it pays a tail read.
    pub(crate) async fn classify_cache_entry(
        &self,
        bucket: &str,
        key: &str,
        size: u64,
        etag: &str,
        remote_framed: Option<u64>,
    ) -> Result<CacheVerdict> {
        // Who owns a generation the other side lacks. In cached mode the cache write is the commit,
        // so it is an acked write still owed to the remote; in durable mode the remote is, so the
        // cache entry is a stale projection to re-settle.
        let diverged = if self.cached {
            CacheVerdict::Pending
        } else {
            CacheVerdict::Stale
        };

        Ok(match meta::classify_entry(size as i64, etag) {
            // Unpropagated by definition: the sweep's delete branch clears the tombstone and the
            // marker together, so a surviving tombstone is owed one either way. That also frees a
            // tombstone stranded by a crash between the remote delete and the clear, which nothing
            // else revisits. A durable delete commits on the remote first, so there the same
            // tombstone is a half-finished bracket for the repair rule.
            Some(meta::TombKind::Delete) => diverged,
            // Already settled from the remote once, and its metadata is the *only* copy of the
            // client's `x-amz-meta-*` and storage class — the trailer holds facts and nothing else
            // (§7). So while the remote still backs it, re-deriving it would only erase those;
            // re-derive exactly when the remote no longer does.
            Some(meta::TombKind::Evict) if remote_framed.is_some() => CacheVerdict::Agrees,
            Some(meta::TombKind::Evict) => CacheVerdict::Stale,
            // A mark is precisely what the repair rule exists to resolve, in either mode.
            Some(meta::TombKind::Transit) => CacheVerdict::Stale,
            None => match remote_framed {
                None => diverged,
                Some(framed) => match single_part_framed_len(size) {
                    Some(expect) if expect != framed => diverged,
                    // Same framed length: only the trailer can tell the generations apart.
                    _ => {
                        if self.remote_generation_matches(bucket, key, etag).await? {
                            CacheVerdict::Agrees
                        } else {
                            diverged
                        }
                    }
                },
            },
        })
    }

    /// Does the remote's trailer carry the cache body's client ETag?
    ///
    /// A trailer that does not authenticate is fatal here as at every other site that reads one
    /// ([`hypha_core::fatal`]): hypha is the sole writer of these buckets, so the object is not
    /// stray junk to be tidied away — either something else writes here or this process holds the
    /// wrong trailer key. Reading it as a plain "no" would be worse than a wrong answer, because
    /// this answer *authorizes an overwrite*: the marker it owes sends the sweep to upload the
    /// cache body over the object hypha just failed to identify.
    pub(crate) async fn remote_generation_matches(
        &self,
        bucket: &str,
        key: &str,
        etag: &str,
    ) -> Result<bool> {
        let Some(tail) = self.read_tail(bucket, key).await? else {
            hypha_core::fatal::foreign_object(bucket, key)
        };
        Ok(tail.footer.client_etag() == etag)
    }

    /// Raise K's pending marker at `payload` (the cache body's ETag). Last writer wins — only the
    /// marker's own ETag matters to the sweep, which CASes on it — so re-raising one is harmless.
    pub(crate) async fn raise_marker(&self, bucket: &str, key: &str, payload: &str) -> Result<()> {
        self.meta
            .put_small(
                bucket,
                meta::pending_marker_key(key),
                payload.as_bytes().to_vec(),
                HashMap::new(),
                None,
                None,
            )
            .await
            .map(|_| ())
    }

    /// `(key, size, ETag)` for every entry of one flat namespace, paged to exhaustion.
    pub(crate) async fn walk(
        &self,
        backend: &Backend,
        bucket: &str,
        mut each: impl FnMut(String, u64, String),
    ) -> Result<()> {
        let mut token = None;
        loop {
            let page = backend
                .list(bucket, None, None, token.take(), None, Some(SCAN_PAGE))
                .await?;
            for o in page.contents.unwrap_or_default() {
                let Some(k) = o.key else { continue };
                let etag = o.e_tag.unwrap_or_default().trim_matches('"').to_string();
                each(k, o.size.unwrap_or(0).max(0) as u64, etag);
            }
            match page.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
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
    /// The reconciler excludes same-key passes on the dedicated per-key **upload** lock
    /// ([`Self::upload_locks`]), not the write lock — held across the whole upload + marker CAS.
    /// Unserialized same-key uploads can finish out of order and leave the remote stale with an empty
    /// pending set (§7); the separate instance keeps a conditional PUT from ever queuing behind a
    /// multi-second transfer. A pass that finds the lock held drops its attempt rather than waiting
    /// (see [`crate::replication`]) — this body read is the coalescing point, since it picks up
    /// whatever generation is current when the lock is won.
    ///
    /// Reconcile takes only the upload lock, so a cached delete
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
