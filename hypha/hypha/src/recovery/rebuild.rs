//! **R2 — the pending-set rebuild** (§7): re-derive a bucket's pending markers when its clean marker
//! is absent, i.e. when a run ended without proving it had indexed every acked write.
//!
//! The namespace here is **authoritative** — that is the whole difference from a restore — so this
//! pass writes nothing but markers. It never materializes a key from the remote (on a ready bucket,
//! cache-absent *is* the client's 404, and rebuilding one would resurrect a deleted object) and
//! never settles a cache entry from a listing (which is what would roll an acked write back).
//!
//! **No key locks.** Raising a marker is last-writer-wins and the sweep clears one by CAS-ing on the
//! marker's own ETag, so a marker raised beside a concurrent upload costs at most one redundant
//! upload and never a lost write. The only lock this pass takes is on the two paths about to declare
//! an invariant violation, where a stale snapshot must not be allowed to halt a healthy deployment.
//!
//! Cached mode only: durable writes commit on the remote, so there is no pending set (invariant I4).
//!
//! §6 is what licenses re-deriving the set at all — *"what makes a cached write pending is a state
//! of the world, not a record hypha keeps"* — and the marker's only job is to make that set
//! enumerable in `O(pending)` rather than `O(keyspace)`. This pass is the `O(keyspace)` fallback for
//! when the index is known incomplete, which is the one time it is worth paying.

use std::cmp::Ordering as KeyOrder;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::TryStreamExt as _;

use hypha_core::error::{Error, Result};
use hypha_core::{meta, Backend};

use crate::halt::{Invariant, Violation};
use crate::tier::Tiering;

/// Keys per backend LIST page.
const PAGE: i32 = 1000;

/// Bounds resident memory and how far the walk runs ahead of the work it has dispatched.
const BATCH: usize = 256;

/// Most keys resolve from the two listings alone; this bounds the minority that pay a trailer read
/// or a marker write.
const CONCURRENCY: usize = 16;

/// Returns how many markers it raised.
pub(crate) async fn rebuild_pending(tier: &Tiering, bucket: &str) -> Result<usize> {
    if !tier.cached {
        // Invariant I4. Not a data fault, but it means the recovery classification is wrong, and a
        // wrong classification is not something to keep serving through.
        tier.halt
            .raise(Violation {
                invariant: Invariant::PendingRebuildInDurableMode,
                bucket: bucket.to_string(),
                key: None,
                detail: "a pending-set rebuild was dispatched for a durable-mode deployment, \
                         which commits on the remote and has no pending set"
                    .to_string(),
            })
            .await
    }

    let raised = AtomicUsize::new(0);
    let mut cache = Cursor::new(&tier.data, bucket);
    let mut remote = Cursor::new(&tier.remote, bucket);

    loop {
        let mut batch = Vec::with_capacity(BATCH);
        while batch.len() < BATCH {
            let Some(sighting) = step(&mut cache, &mut remote).await? else {
                break;
            };
            batch.push(sighting);
        }
        if batch.is_empty() {
            return Ok(raised.into_inner());
        }
        // Short-circuits on the first error, which is what an invariant violation needs: the pass
        // must stop, not finish its batch against data it has just declared untrustworthy.
        futures::stream::iter(batch.into_iter().map(Ok))
            .try_for_each_concurrent(CONCURRENCY, |sighting| async {
                if owes_marker(tier, bucket, sighting).await? {
                    raised.fetch_add(1, Ordering::Relaxed);
                }
                Ok::<_, Error>(())
            })
            .await?;
    }
}

/// The tombstone kind decides this on its own except in the two cases that turn on what the remote
/// holds: a live body (is it *this* generation?) and an eviction (is it still backed?).
async fn owes_marker(tier: &Tiering, bucket: &str, sighting: Sighting) -> Result<bool> {
    let (key, size, etag, remote_framed) = match sighting {
        // Invariant I2: every site that removes a `<data>` entry does so only once the remote
        // object is already gone, so a remote-only key cannot arise.
        Sighting::RemoteOnly { key } => {
            confirm_remote_only(tier, bucket, &key).await?;
            return Ok(false);
        }
        Sighting::Cached {
            key,
            size,
            etag,
            remote_framed,
        } => (key, size, etag, remote_framed),
    };

    match meta::classify_entry(size as i64, &etag) {
        // An acked delete the remote has not honoured, or one it has — with the crash landing
        // between the remote delete and clearing the tombstone and marker together. Both want the
        // marker back, and the sweep clears the pair.
        Some(meta::TombKind::Delete) => {
            tier.raise_marker(bucket, &key, &etag).await?;
            Ok(true)
        }
        // Settled, so nothing is owed — unless the remote no longer backs it, which is I3.
        Some(meta::TombKind::Evict) => {
            if remote_framed.is_none() {
                confirm_remote_lost_object(tier, bucket, &key).await?;
            }
            Ok(false)
        }
        // A bracket that died before its commit. The repair rule owns it, on the next access;
        // settling it here would be this pass mutating `<data>`.
        Some(meta::TombKind::Transit) => Ok(false),
        None => match remote_framed {
            Some(framed) if same_generation(tier, bucket, &key, size, &etag, framed).await? => {
                Ok(false)
            }
            _ => {
                tier.raise_marker(bucket, &key, &etag).await?;
                Ok(true)
            }
        },
    }
}

/// §6's closed form over plaintext length answers this from the two listings for any overwrite that
/// changed the length; only a same-length overwrite pays a trailer read.
async fn same_generation(
    tier: &Tiering,
    bucket: &str,
    key: &str,
    plen: u64,
    etag: &str,
    remote_framed: u64,
) -> Result<bool> {
    match crate::tier::single_part_framed_len(plen) {
        Some(expect) if expect != remote_framed => Ok(false),
        _ => tier.remote_generation_matches(bucket, key, etag).await,
    }
}

/// Invariant **I2**, confirmed against fresh reads under K's lock.
///
/// The two cursors are snapshots taken at different moments, and the benign interleaving is
/// ordinary: the cache listing runs, a client writes K, the reconcile sweep uploads it, and the
/// remote listing then sees a key the cache listing could not. Halting on that would take a healthy
/// deployment down. Under the lock no write can move K, so a fresh pair of HEADs settles it — and
/// **both** sides must be re-read, since the delete that removes the cache entry removes the remote
/// object first.
async fn confirm_remote_only(tier: &Tiering, bucket: &str, key: &str) -> Result<()> {
    let _guard = tier.locks.lock(key).await;
    if cache_has_entry(tier, bucket, key).await? || !remote_has_object(tier, bucket, key).await? {
        return Ok(());
    }
    tier.halt
        .raise(Violation {
            invariant: Invariant::RemoteOnlyKey,
            bucket: bucket.to_string(),
            key: Some(key.to_string()),
            detail: "the remote holds an object this ready bucket's cache has no entry for; \
                     cache-absent is the authoritative 404, so the two disagree about whether \
                     the object exists"
                .to_string(),
        })
        .await
}

/// Invariant **I3**, confirmed under K's lock — same snapshot skew as [`confirm_remote_only`], in
/// the other direction: the remote listing can predate the upload that settled this tombstone.
async fn confirm_remote_lost_object(tier: &Tiering, bucket: &str, key: &str) -> Result<()> {
    let _guard = tier.locks.lock(key).await;
    if remote_has_object(tier, bucket, key).await? {
        return Ok(());
    }
    // The tombstone may itself have been superseded since the listing.
    let head = match tier.data.head(bucket, key).await {
        Ok(h) => h,
        Err(Error::NotFound) | Err(Error::NoSuchBucket) => return Ok(()),
        Err(e) => return Err(e),
    };
    if meta::tomb_kind(&head.metadata.unwrap_or_default()) != Some(meta::TombKind::Evict) {
        return Ok(());
    }
    tier.halt
        .raise(Violation {
            invariant: Invariant::RemoteLostObject,
            bucket: bucket.to_string(),
            key: Some(key.to_string()),
            detail: "an eviction tombstone's remote object is missing; the remote lost bytes \
                     hypha reported as committed, and this tombstone is the only record they \
                     existed"
                .to_string(),
        })
        .await
}

async fn cache_has_entry(tier: &Tiering, bucket: &str, key: &str) -> Result<bool> {
    match tier.data.head(bucket, key).await {
        Ok(_) => Ok(true),
        Err(Error::NotFound) | Err(Error::NoSuchBucket) => Ok(false),
        Err(e) => Err(e),
    }
}

async fn remote_has_object(tier: &Tiering, bucket: &str, key: &str) -> Result<bool> {
    match tier.remote.head(bucket, key).await {
        Ok(_) => Ok(true),
        Err(Error::NotFound) | Err(Error::NoSuchBucket) => Ok(false),
        Err(e) => Err(e),
    }
}

// ── the two-cursor walk ───────────────────────────────────────────────────────────────────────

/// What the two listings say about one key.
enum Sighting {
    /// The cache holds an entry — a body or a tombstone. `remote_framed` is the framed length of the
    /// remote object, when the remote holds one.
    Cached {
        key: String,
        size: u64,
        etag: String,
        remote_framed: Option<u64>,
    },
    /// Only the remote holds the key.
    RemoteOnly { key: String },
}

/// Advance whichever cursor is behind, emitting the next key of the two namespaces' union.
///
/// **Streaming, not buffered.** Both backends return keys in lexicographic order within a bucket, so
/// the walk holds one page per side however large the keyspace is. Reading both namespaces into maps
/// first — which is what this replaces — made a recovery's memory proportional to the bucket, on the
/// path that runs when a deployment is already in trouble.
async fn step(cache: &mut Cursor<'_>, remote: &mut Cursor<'_>) -> Result<Option<Sighting>> {
    cache.fill().await?;
    remote.fill().await?;
    let order = match (cache.front(), remote.front()) {
        (None, None) => return Ok(None),
        (Some(_), None) => KeyOrder::Less,
        (None, Some(_)) => KeyOrder::Greater,
        (Some(c), Some(r)) => c.cmp(r),
    };
    Ok(Some(if order == KeyOrder::Greater {
        Sighting::RemoteOnly {
            key: remote.pop().key,
        }
    } else {
        let entry = cache.pop();
        Sighting::Cached {
            key: entry.key,
            size: entry.size,
            etag: entry.etag,
            remote_framed: (order == KeyOrder::Equal).then(|| remote.pop().size),
        }
    }))
}

struct Entry {
    key: String,
    size: u64,
    etag: String,
}

struct Cursor<'a> {
    backend: &'a Backend,
    bucket: &'a str,
    token: Option<String>,
    buf: VecDeque<Entry>,
    exhausted: bool,
}

impl<'a> Cursor<'a> {
    fn new(backend: &'a Backend, bucket: &'a str) -> Self {
        Cursor {
            backend,
            bucket,
            token: None,
            buf: VecDeque::new(),
            exhausted: false,
        }
    }

    /// Valid only after [`Cursor::fill`] — a cursor with a page left to fetch reads as empty.
    fn front(&self) -> Option<&str> {
        self.buf.front().map(|e| e.key.as_str())
    }

    fn pop(&mut self) -> Entry {
        self.buf.pop_front().expect("stepped past a filled cursor")
    }

    async fn fill(&mut self) -> Result<()> {
        while self.buf.is_empty() && !self.exhausted {
            let page = self
                .backend
                .list(self.bucket, None, None, self.token.take(), None, Some(PAGE))
                .await?;
            for o in page.contents.unwrap_or_default() {
                let Some(key) = o.key else { continue };
                // A no-op on `<data>`, whose keys cannot carry the control byte — so one cursor
                // type rather than two, or one with a flag.
                if meta::is_reserved_remote_key(&key) {
                    continue;
                }
                self.buf.push_back(Entry {
                    key,
                    size: o.size.unwrap_or(0).max(0) as u64,
                    etag: o.e_tag.unwrap_or_default().trim_matches('"').to_string(),
                });
            }
            match page.next_continuation_token {
                Some(t) => self.token = Some(t),
                None => self.exhausted = true,
            }
        }
        Ok(())
    }
}
