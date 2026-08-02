//! Re-derives a cached bucket's pending set from an authoritative cache namespace.
//!
//! It writes markers only—materializing remote keys could resurrect deletes. Every inference is
//! made from a locked re-read so a stale listing cannot retype a concurrent client operation.

use std::cmp::Ordering as KeyOrder;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::TryStreamExt as _;

use hypha_core::error::{Error, Result};
use hypha_core::{meta, Backend};

use crate::halt::{Invariant, Violation};
use crate::tier::Tiering;

const PAGE_KEYS: i32 = 1000;

/// Bounds resident memory and how far the walk runs ahead of the work it has dispatched.
const BATCH: usize = 256;

/// Most keys resolve from the two listings alone; this bounds the minority that pay a trailer read
/// or a marker write.
const CONCURRENCY: usize = 16;

/// Returns how many markers it raised.
pub(super) async fn pending_set(tier: &Tiering, bucket: &str) -> Result<usize> {
    if !tier.cached {
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

/// Rebuild the operation marker implied by the cache/remote comparison.
async fn owes_marker(tier: &Tiering, bucket: &str, sighting: Sighting) -> Result<bool> {
    let (key, size, etag, remote_framed) = match sighting {
        Sighting::RemoteOnly { key } => {
            return recover_remote_only(tier, bucket, &key).await;
        }
        Sighting::Cached {
            key,
            size,
            etag,
            remote_framed,
        } => (key, size, etag, remote_framed),
    };

    match meta::classify_entry(size as i64, &etag) {
        // Settled, so nothing is owed — unless the remote no longer backs it, which is I2.
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
            _ => raise_upload_marker(tier, bucket, &key, &etag).await,
        },
    }
}

/// The closed form over plaintext length answers this from the two listings for any overwrite that
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

/// Re-read K under its lock before inferring an upload from it. The cursor page is a snapshot, and a
/// marker now records an *operation*: a PUT marker raised over the DELETE marker of a delete that
/// committed since would retype it, and the sweep — finding K absent — would then decline, leaving
/// the remote object standing and the marker unclearable. A generation that moved since the sighting
/// owes its own marker, so there is nothing left here to infer.
async fn raise_upload_marker(tier: &Tiering, bucket: &str, key: &str, etag: &str) -> Result<bool> {
    let _guard = tier.locks.lock(key).await;
    let head = match tier.data.head(bucket, key).await {
        Ok(h) => h,
        Err(Error::NotFound) | Err(Error::NoSuchBucket) => return Ok(false),
        Err(e) => return Err(e),
    };
    if head.e_tag().unwrap_or_default().trim_matches('"') != etag {
        return Ok(false);
    }
    tier.raise_marker_if_absent(bucket, key, etag).await
}

/// A remote-only cursor sighting is either snapshot skew or an interrupted cached delete. R2 may
/// trust cache absence because total volume loss is dispatched to R1 by the missing sync marker.
async fn recover_remote_only(tier: &Tiering, bucket: &str, key: &str) -> Result<bool> {
    let _guard = tier.locks.lock(key).await;
    if cache_has_entry(tier, bucket, key).await? || !remote_has_object(tier, bucket, key).await? {
        return Ok(false);
    }
    let delete = meta::delete_marker_body();
    tier.raise_marker(bucket, key, &delete).await?;
    Ok(true)
}

/// Invariant **I2**, confirmed under K's lock: the remote listing can predate the upload that
/// settled this tombstone, so the violation needs a fresh read.
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
    /// The cache entry is a body or a tombstone; `size` and `etag` are what tell them apart.
    Cached {
        key: String,
        size: u64,
        etag: String,
        remote_framed: Option<u64>,
    },
    /// A committed cached delete whose marker had not landed before a crash.
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
                .list(
                    self.bucket,
                    None,
                    None,
                    self.token.take(),
                    None,
                    Some(PAGE_KEYS),
                )
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
