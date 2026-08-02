//! Shared transition-bracket, remote-upload, and tombstone machinery.

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
use crate::halt::Halt;
use crate::keylocks::KeyLocks;

/// The framed size a single-part remote object would have for a `plen`-byte plaintext. A
/// markerless live body is always single-part — a composite is tombstoned at K with its plaintext in
/// the shadow — so this is exact where the pending-set rebuild applies it
/// ([`crate::bucket`]).
pub(crate) fn single_part_framed_len(plen: u64) -> Option<u64> {
    hypha_format::offset::ciphertext_len(plen, hypha_format::offset::HLEN)
        .checked_add(SINGLE_TRAILER_LEN as u64)
}

/// The [`Tiering::create_locks`] key for a `CreateMultipartUpload`: `(bucket, key)`, the one
/// identity the op knows before its remote create — the upload id does not exist yet, so it cannot
/// key the lock.
pub(crate) fn create_lock_key(bucket: &str, key: &str) -> String {
    format!("create\0{bucket}\0{key}")
}

#[derive(Clone)]
pub struct Tiering {
    pub data: Backend,
    pub meta: Backend,
    pub remote: Backend,
    pub env: Arc<Envelope>,
    pub trailer_key: TrailerKey,
    /// The **write** lock table: conditional writes, the durable finalize, GC tombstone
    /// transitions, and rehydrate all serialize on it.
    pub locks: KeyLocks,
    /// The **upload** lock table — a *second* instance, reconcile-only. Same-key reconcile
    /// passes mutually exclude here so an unserialized older upload can't finish after a newer one
    /// and leave the remote stale, while a replication upload never blocks a client's conditional PUT
    /// (which takes `locks`, not this). Held via `try_lock`: a pass that finds K busy coalesces onto
    /// the in-flight upload rather than queuing, so this table never accumulates waiters.
    pub upload_locks: KeyLocks,
    pub mpu_part_locks: KeyLocks,
    /// The **create** lock table: per-`(bucket, key)`, held from before the remote
    /// `CreateMultipartUpload` until the cache `u`-record is written. That ordering is what lets the
    /// orphan sweep ([`crate::gc::debris`]) distinguish a create still in flight (lock held, record
    /// not yet) from a leak (lock free, record absent) — and it must be held *before* the remote
    /// create, because the upload becomes listable the instant that returns.
    pub create_locks: KeyLocks,
    /// Cached mode. Decides who wins when a surviving cache entry and the remote disagree: in
    /// cached mode the cache write *is* the commit, so a generation the remote lacks is an acked
    /// write still owed to it — the pending set the clean marker accounts for. Durable mode
    /// has no pending set at all.
    pub cached: bool,
    pub(crate) halt: Halt,
    /// The cached-write backpressure gate. Inert in durable mode, which has no pending set.
    pub(crate) pressure: Arc<crate::pressure::Pressure>,
}

/// What one reconcile upload attempt found at K. Only a real upload completes a PUT marker.
pub(crate) enum UploadOutcome {
    /// A live client body was encrypted and PUT to the remote.
    Uploaded,
    /// K is a tombstone owned by another state transition. The kind decides what becomes of the
    /// marker, so it is carried out: a transition tombstone's bracket will raise its own, while an
    /// eviction tombstone certifies the remote already holds K and so discharges this one.
    SkippedTombstone(meta::TombKind),
    /// K was deleted after this marker was raised, so the generation it names can no longer be
    /// uploaded from anywhere — the obligation is undischargeable, not pending.
    Vanished,
}

#[derive(Clone, Debug)]
pub(crate) struct RemoteFacts {
    pub plen: u64,
    pub cetag: String,
    pub mtime_ms: i64,
}

impl RemoteFacts {
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

impl Tiering {
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
    /// user-metadata — the authoritative copy; the twin is its LIST projection.
    ///
    /// `client_passthrough` (the client's namespaced `x-amz-meta-*` and echoed storage class) is
    /// cache-only — the remote's trailer holds facts and nothing else — so a rebuild from the remote
    /// settles it empty.
    pub(crate) async fn settle_evict_locked(
        &self,
        bucket: &str,
        key: &str,
        plen: u64,
        cetag: &str,
        mtime_ms: i64,
        client_passthrough: HashMap<String, String>,
    ) -> Result<()> {
        let facts = meta::Facts {
            client_etag: cetag.to_string(),
            plen,
            mtime_ms,
        };
        self.refresh_twin(bucket, key, &facts).await?;

        let mut md = client_passthrough;
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

    /// **Repair rule**: settle K to whatever the remote actually holds. Idempotent; needs no
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
        // The client pass-through is cache-resident and this settle is rebuilding the cache, so it
        // comes back empty — except the content type, which rides the remote object natively.
        let mut passthrough = HashMap::new();
        if let Some(ct) = head.content_type() {
            passthrough.insert(meta::CTYPE.to_string(), meta::encode_content_type(ct));
        }
        let facts = self.remote_facts(bucket, key, &head).await?;
        self.settle_evict_locked(
            bucket,
            key,
            facts.plen,
            &facts.cetag,
            facts.mtime_ms,
            passthrough,
        )
        .await?;
        Ok(Some(facts))
    }

    /// Materialize K's cache entry from the remote **only if the cache has none** — the single
    /// mutation a namespace restore performs, applied per key by the sweep and on demand by a
    /// write that beats the sweep to K.
    ///
    /// Additive by construction, and that is the whole point. An entry already at K during a restore
    /// is one of two things: a tombstone this restore (or an earlier, crashed run of it) settled from
    /// the remote, or the settle of a write committed during the window — writes run with durable
    /// semantics for the whole restore, so a committed one has already recorded itself here. Both are
    /// current. Overwriting either from the remote would at best erase the client pass-through the
    /// tombstone is the only copy of, and at worst roll a committed write back to a superseded
    /// generation.
    ///
    /// Caller holds K's write lock, which is what makes the absence check and the settle one step.
    pub(crate) async fn materialize_absent_locked(&self, bucket: &str, key: &str) -> Result<()> {
        match self.data.head(bucket, key).await {
            Ok(_) => Ok(()),
            Err(Error::NotFound) | Err(Error::NoSuchBucket) => {
                self.repair_locked(bucket, key).await?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Does the remote's trailer carry the cache body's client ETag?
    ///
    /// A trailer that does not authenticate halts the deployment here as at every other site that
    /// reads one ([`crate::halt`]): hypha is the sole writer of these buckets, so the object is not
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
            self.halt.foreign_object(bucket, key).await
        };
        Ok(tail.footer.client_etag() == etag)
    }

    /// Last writer wins — for the operation's **own** writer, which knows what it just did. A
    /// speculative raiser must use [`Self::raise_marker_if_absent`] instead.
    ///
    /// The body encodes an upload or DELETE operation; the marker object's own ETag is its branch
    /// discriminator and completion CAS.
    ///
    /// Create-only first, so the key is counted once toward backpressure: an overwrite is a
    /// replacement, not a new pending obligation.
    pub(crate) async fn raise_marker(
        &self,
        bucket: &str,
        key: &str,
        marker_body: &str,
    ) -> Result<()> {
        if self.raise_marker_if_absent(bucket, key, marker_body).await? {
            return Ok(());
        }
        self.meta
            .put_small(
                bucket,
                meta::pending_marker_key(key),
                marker_body.as_bytes().to_vec(),
                HashMap::new(),
                None,
                None,
            )
            .await
            .map(|_| ())
    }

    /// Fill an **absent** marker, for the raisers that reconstruct an obligation from an observation
    /// rather than from having performed the operation (eviction's durability gate, R2). `Ok(false)`
    /// ⇒ a marker was already there.
    ///
    /// Create-only because the body now carries the *operation*: these raisers only ever infer an
    /// upload, so overwriting an existing marker could retype a client's acked DELETE into a PUT,
    /// and the sweep would then find K absent, decline, and leave the remote object standing forever
    /// — a delete that never propagates and resurrects on the next restore. Whatever marker is
    /// already there was written by the operation that owns it, which is by definition the newer
    /// judgement.
    ///
    /// Counts the created marker toward backpressure; a `false` is an existing marker the key is
    /// already counted for.
    pub(crate) async fn raise_marker_if_absent(
        &self,
        bucket: &str,
        key: &str,
        marker_body: &str,
    ) -> Result<bool> {
        match self
            .meta
            .put_small(
                bucket,
                meta::pending_marker_key(key),
                marker_body.as_bytes().to_vec(),
                HashMap::new(),
                None,
                Some("*".to_string()),
            )
            .await
        {
            Ok(_) => {
                self.pressure.raised();
                Ok(true)
            }
            Err(Error::PreconditionFailed) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Resolve a remote object's plaintext facts from its tail trailer: **one speculative tail
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
            self.halt.foreign_object(bucket, key).await
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
    /// parse the trailer: this captures `table ‖ facts ‖ tag ‖ version` for any object in a
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
    /// facts framed in as the footer. `plen`, `cetag`, and `mtime` are read from the *same*
    /// GET response that streams the body, so the framed facts can never disagree with the
    /// uploaded bytes. Assumes the caller holds `key`'s lock.
    ///
    /// The reconciler excludes same-key passes on the dedicated per-key **upload** lock
    /// ([`Self::upload_locks`]), not the write lock — held across the whole upload + marker CAS.
    /// Unserialized same-key uploads can finish out of order and leave the remote stale with an empty
    /// pending set; the separate instance keeps a conditional PUT from ever queuing behind a
    /// multi-second transfer. A pass that finds the lock held drops its attempt rather than waiting
    /// (see [`crate::replication`]) — this body read is the coalescing point, since it picks up
    /// whatever generation is current when the lock is won.
    ///
    /// Reconcile takes only the upload lock, so a transition may replace K concurrently; the
    /// classify guard keeps its internal sentinel from being uploaded as a client body.
    pub(crate) async fn upload_locked(&self, bucket: &str, key: &str) -> Result<UploadOutcome> {
        let out = match self.data.get(bucket, key, None).await {
            Ok(o) => o,
            Err(Error::NotFound) => return Ok(UploadOutcome::Vanished),
            Err(e) => return Err(e),
        };
        let plen = out.content_length().unwrap_or(0).max(0) as u64;
        let content_type = out.metadata.as_ref().and_then(meta::content_type);
        let cetag = out
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        // An internal tombstone raced in. A client body cannot spoof this classification because
        // cached PUT reserves the sentinel bodies.
        if let Some(kind) = meta::classify_entry(plen as i64, &cetag) {
            return Ok(UploadOutcome::SkippedTombstone(kind));
        }
        // Single-part client ETag == the cache's own MD5 (composites route around this path);
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
                content_type,
            )
            .await?;
        Ok(UploadOutcome::Uploaded)
    }

    /// **DELETE branch** of the reconcile sweep. Confirm the listed marker is still current and
    /// that K is still absent, then delete the remote object. A newer marker survives the completion
    /// CAS for the operation that superseded this one.
    ///
    /// **This branch takes K's write lock**, unlike the upload branch beside it. The reason the
    /// upload branch does not is a transfer it must not queue conditional writes behind; a
    /// delete is a small round trip, so it pays nothing for the lock — and it needs it, because it
    /// is the only reconcile action that *destroys* a generation. The upload branch racing a newer
    /// write is self-correcting (the newer write's own marker is still standing, so it is uploaded
    /// next pass); a delete racing one is not.
    ///
    /// The caller already holds K's upload lock. Taking both lock domains excludes every remote
    /// writer: reconcile uploads take the upload lock, while durable writes and multipart completion
    /// take the write lock. Hypha exclusively owns the remote, so no backend CAS is needed here.
    ///
    /// And racing one is possible without any of the usual interleaving: **multipart is always
    /// durable**, so a `CompleteMultipartUpload` commits to the remote and settles K *without
    /// raising a marker of its own*. It is therefore the one write path that does not supersede a
    /// marker already standing at K — a cached DELETE's marker, still in flight, would otherwise be
    /// discharged against the composite the client was just told was committed. Hence the K check
    /// under the lock: a marker whose key exists again is superseded, whatever wrote it.
    pub(crate) async fn propagate_delete_locked(
        &self,
        bucket: &str,
        key: &str,
        m_etag: &str,
    ) -> Result<()> {
        let _guard = self.locks.lock(key).await;
        match self.data.head(bucket, key).await {
            // The genuine case: K is absent, so this marker is the record of the delete that made it
            // so, and the remote still has to be told.
            Err(Error::NotFound) => {}
            Err(e) => return Err(e),
            Ok(head) => {
                let md = head.metadata.as_ref();
                return match md.and_then(meta::tomb_kind) {
                    // A bracket owns K and will settle it; whatever it commits decides the remote's
                    // contents, so this obligation is neither dischargeable nor stranded yet. Same
                    // reasoning as the upload branch's transit arm.
                    Some(meta::TombKind::Transit) => Ok(()),
                    // A live body or an eviction tombstone: K exists again, so the delete this marker
                    // records has been superseded by whatever wrote it. Clearing under the CAS is
                    // what makes that safe — a write that raised its own marker moved the ETag, so
                    // only the markerless case (a multipart complete) is cleared here.
                    _ => self.clear_marker_cas(bucket, key, m_etag).await,
                };
            }
        }
        let current_marker = match self.meta.head(bucket, meta::pending_marker_key(key)).await {
            Ok(head) => head
                .e_tag()
                .unwrap_or_default()
                .trim_matches('"')
                .to_string(),
            Err(Error::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        };
        if current_marker != m_etag {
            return Ok(());
        }

        match self.remote.delete(bucket, key).await {
            Ok(()) | Err(Error::NotFound) => {}
            Err(e) => return Err(e),
        }
        self.clear_marker_cas(bucket, key, m_etag).await
    }

    /// Clear the pending marker at bare `K`, conditional on its `M_etag`. A newer operation
    /// rewrites the marker, so a 412 leaves that obligation for the next pass. A 404 is equally fine.
    /// Counts the removed marker against backpressure only when the CAS actually removed it.
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
            Ok(()) => {
                self.pressure.cleared();
                Ok(())
            }
            Err(Error::PreconditionFailed) | Err(Error::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// **Rehydrate** a single-part body back to a live cache entry at K: the decrypted plaintext
    /// (streamed by the caller from the remote) overwrites the eviction tombstone, conditional on the
    /// evict sentinel so a concurrent write/rehydrate aborts us. The sentinel ETag is constant across
    /// generations, so the CAS alone can't see an evict→rehydrate→re-evict ABA — the caller re-reads
    /// the tombstone's `cetag` under the lock for that ([`crate::background`]). `md` is the
    /// tombstone's client pass-through ([`meta::passthrough_metadata`]).
    ///
    /// The caller drops K's twin ([`Self::delete_twins`]) once this returns, making K's facts native
    /// again. Deliberately *not* done here: this PUT is what drives the remote fetch, so it is the
    /// long, cancellable half of a rehydrate, while the twin drop must run to completion — a
    /// cancel between the two would leave a live body beside a stale twin (benign, ignored by the
    /// LIST gate, but debris only an orphan-twin sweep reclaims).
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
                None,
            )
            .await?;
        Ok(())
    }

    /// Whether K's shadow body already holds the current composite generation — a HEAD, no body
    /// fetch. Lets a rehydrate skip re-downloading a composite an earlier read already landed.
    pub(crate) async fn shadow_is_current(
        &self,
        bucket: &str,
        key: &str,
        cetag: &str,
    ) -> Result<bool> {
        match self.meta.head(bucket, &meta::shadow_key(key)).await {
            Ok(h) => Ok(shadow_is_generation(h.metadata(), cetag)),
            Err(Error::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Decrypt a remote-resident object into a plaintext stream — the whole body, or a plaintext
    /// sub-range `pt`. Single-part goes through `decrypt_full`/`decrypt_range`; a composite
    /// recovers its parts table from one tail read and decrypts part-by-part. Shared by GET's remote
    /// path, UploadPartCopy's re-encrypt source read, and the background rehydrate.
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
                self.halt.foreign_object(bucket, key).await
            };
            Ok(match &pt {
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
                    codec::decrypt_full(self.env.clone(), out.body, ct_len).await?
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

    /// **Rehydrate** a composite into its shadow body: a rehydrated composite's plaintext lives
    /// at `sha256(K)`-keyed [`meta::shadow_key`] (a point lookup, so K's length can't constrain it).
    /// K's tombstone and twin stay untouched, so composite rehydration is invisible to LIST/HEAD and
    /// rewrites no twin. Caller holds K's write lock.
    ///
    /// The shadow key is deterministic in K, so a later composite at K overwrites the *same* shadow —
    /// which means the key alone cannot distinguish generations. `cetag` (the rehydrated composite's
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
        md.insert(meta::CETAG.to_string(), cetag.to_string());
        // The back-pointer the digest key cannot provide: without it a shadow whose K was deleted or
        // overwritten is unreachable *and* unidentifiable, so nothing could ever judge it.
        md.insert(
            meta::SHADOW_CLIENT_KEY.to_string(),
            meta::encode_shadow_client_key(key),
        );
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
                None,
            )
            .await?;
        Ok(())
    }

    /// Replace the cache body at `key` with an eviction tombstone (the phase-5 GC transition).
    /// Facts are read from the cache body itself (one HEAD) rather than trusted from the caller;
    /// twin-before-tombstone refreshes the facts twin, then the body is overwritten
    /// conditional on its current ETag so a concurrent writer aborts us. Assumes the caller holds
    /// `key`'s lock.
    ///
    /// `expected_etag` is the generation the caller judged — the version token `If-Match`
    /// conditions on. The durability gate is the caller's: this writes the tombstone unconditionally
    /// once the CAS holds, so whoever calls it must already have confirmed that the remote holds
    /// *this* generation ([`crate::gc`]). A bare presence check is not that confirmation — the remote
    /// holding an older generation would have the tombstone stamped with the cache body's facts, and
    /// reads would return the old plaintext under the new ETag and length.
    ///
    /// The fresh HEAD is compared against `expected_etag` before anything is written: the CAS below
    /// would refuse a superseded generation anyway, but the twin is written first, and a twin built
    /// from a *different* generation's facts is debris that outlives the failed attempt.
    pub(crate) async fn tombstone_locked(
        &self,
        bucket: &str,
        key: &str,
        expected_etag: &str,
    ) -> Result<()> {
        let head = self.data.head(bucket, key).await?;
        let body_etag = head
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        if body_etag != expected_etag {
            return Err(Error::PreconditionFailed);
        }
        let plen = head.content_length().unwrap_or(0).max(0) as u64;
        // Eviction must not move the key's client-visible LastModified.
        let mtime_ms = head
            .last_modified()
            .map(|t| t.to_millis().unwrap_or_default())
            .unwrap_or_else(now_ms);

        let facts = meta::Facts {
            client_etag: body_etag.clone(),
            plen,
            mtime_ms,
        };
        self.refresh_twin(bucket, key, &facts).await?;

        // The tombstone is where a `x-amz-meta-*` pass-through and the storage class live while the
        // body is remote-only — the remote object carries neither (the trailer holds facts, not
        // client metadata), so anything dropped here is unrecoverable.
        let mut md = meta::passthrough_metadata(&head.metadata.unwrap_or_default());
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
    /// A key over the twin threshold gets **no** twin (`twin_key` is `None`) and resolves through LIST's
    /// HEAD fallback instead — the tombstone metadata is authoritative either way. A crash between
    /// leaves only a twin whose base key is a live/absent entry — ignored by the LIST gate,
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

/// `bytes <start>-<end>/<total>` → `total`; `None` for a malformed header or an unknown (`*`) size.
fn parse_content_range_total(content_range: &str) -> Option<u64> {
    content_range.rsplit_once('/')?.1.trim().parse().ok()
}

/// Whether a shadow body is K's *current* composite generation. Only the generation is in
/// question — the shadow key is the whole digest of K, so a shadow found under it is K's — and a
/// shadow left over from a superseded generation is treated as a miss.
pub(crate) fn shadow_is_generation(
    metadata: Option<&HashMap<String, String>>,
    cetag: &str,
) -> bool {
    metadata
        .and_then(|md| md.get(meta::CETAG))
        .map(String::as_str)
        == Some(cetag)
}

/// Resolve a plaintext range against a composite's parts: with per-part windows and plaintext
/// lengths already in hand (from the trailer's parts table), clip the parts that cover `pt`.
fn composite_segments(
    windows: &[ByteRange<u64>],
    plens: &[u64],
    pt: &ByteRange<u64>,
) -> Vec<PartSegment> {
    let mut segs = Vec::new();
    let mut part_pt_start = 0u64;
    for (window, &part_plen) in windows.iter().zip(plens) {
        if part_pt_start >= pt.end {
            break;
        }
        segs.extend(clip_part_to_range(window, part_pt_start, part_plen, pt));
        part_pt_start += part_plen;
    }
    segs
}

fn clip_part_to_range(
    part_ct: &ByteRange<u64>,
    part_pt_start: u64,
    part_plen: u64,
    want: &ByteRange<u64>,
) -> Option<PartSegment> {
    let part_pt_end = part_pt_start + part_plen;
    let lo = want.start.max(part_pt_start);
    let hi = want.end.min(part_pt_end);
    if lo >= hi {
        return None;
    }
    if lo == part_pt_start && hi == part_pt_end {
        Some(PartSegment::Whole(part_ct.clone()))
    } else {
        Some(PartSegment::Partial {
            ct: part_ct.clone(),
            pt: (lo - part_pt_start)..(hi - part_pt_start),
        })
    }
}

/// The age-envelope length of a single-part remote object: its Content-Length minus the tail
/// trailer, which must never reach the decryptor.
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
