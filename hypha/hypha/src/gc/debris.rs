//! Reclaims upload records exhaustively and collects marks and twins opportunistically from
//! existing probes, avoiding dedicated listings for harmless debris.

use std::collections::HashMap;

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::tier::Tiering;

const PAGE_KEYS: i32 = 1000;

/// Reclaim the record ranges of uploads the remote is no longer running, and abort the remote
/// uploads whose cache record is gone — one streaming pass over the live list.
///
/// Every upload's records share the `0x01 0x01 m ‖ <upload-id> ‖ 0x01` range, so the whole set
/// is one prefix scan and needs no side index — which is what lets complete and abort skip the
/// delete entirely and hand it here, off the client path.
///
/// **The remote decides, and it is asked second.** An upload id the remote is still running has
/// records this pass must not touch. Listing the cache first and the remote second is what makes
/// that safe: `CreateMultipartUpload` initiates the remote upload *before* writing the cache record
/// ([`crate::s3`]), so any range this pass observed was written after its upload existed remotely —
/// and therefore before the in-progress snapshot taken below. An upload absent from that snapshot
/// had already completed or aborted. Asking in the other order would let an upload created between
/// the two calls look abandoned, and this pass would delete a live upload's parts.
///
/// The reverse direction is the same list walked forward: a live upload whose record the scan did
/// not see is a leak — a `CreateMultipartUpload` whose record write failed, or whose records the
/// cache lost. A successfully-created upload always gets its record written before the create acks,
/// so a record-less upload is one no client can address ([`crate::s3`]'s `require_upload` refuses on
/// it) — aborting is safe and its parts go with it. **The create lock makes that safe:** it is held
/// from before the remote create until the record lands, so `try_lock` yields to the in-flight
/// window and the record is re-checked once the lock is won, when the exclusion is conclusive.
pub(super) async fn sweep_uploads(tier: &Tiering, bucket: &str) -> Result<Swept> {
    let mut ranges = cache_ranges(tier, bucket).await?;
    let mut reclaimed = Swept::default();

    let mut key_marker = None;
    let mut upload_id_marker = None;
    loop {
        let page = tier
            .remote
            .list_multipart_uploads(bucket, None, None, key_marker, upload_id_marker, None)
            .await?;
        for upload in page.uploads.unwrap_or_default() {
            let (Some(key), Some(upload_id)) = (upload.key, upload.upload_id) else {
                continue;
            };
            // A record in the snapshot shields the upload from both halves: its records survive,
            // and it is not an orphan. The scan predates the stream, so the create-lock handshake
            // below still guards the window where a record lands mid-pass.
            if ranges.remove(&upload_id).is_some() {
                continue;
            }
            let Some(_guard) = tier
                .create_locks
                .try_lock(&crate::tier::create_lock_key(bucket, &key))
            else {
                continue;
            };
            // The scan may predate the create's record write; only a fresh head settles it.
            if has_record(tier, bucket, &upload_id).await? {
                continue;
            }
            // A 404 on the abort is the same outcome — whatever ended the upload is gone now too —
            // so both count as the orphan resolved.
            match tier.remote.abort_multipart(bucket, &key, &upload_id).await {
                Ok(()) | Err(Error::NotFound) => reclaimed.orphaned += 1,
                Err(e) => tracing::debug!(
                    bucket,
                    key,
                    upload_id,
                    error = %e,
                    "orphaned upload not aborted"
                ),
            }
        }
        if page.is_truncated != Some(true) {
            break;
        }
        key_marker = page.next_key_marker;
        upload_id_marker = page.next_upload_id_marker;
        // A truncated page that carries no markers would loop forever re-reading the first one.
        if key_marker.is_none() && upload_id_marker.is_none() {
            break;
        }
    }

    // Ranges the live list never named belong to completed or aborted uploads.
    for (_upload_id, records) in ranges {
        // Record keys carry the `0x01` control byte, which the batch `DeleteObjects` XML body
        // cannot represent — single-object deletes only, as with twins.
        let deletes = records.iter().map(|(key, _)| tier.meta.delete(bucket, key));
        futures::future::try_join_all(deletes).await?;
        reclaimed.uploads += 1;
        reclaimed.bytes += records.iter().map(|(_, bytes)| bytes).sum::<u64>();
    }
    Ok(reclaimed)
}

/// Repair transition marks the eviction probe walked past.
///
/// **The lock is the whole test.** Every bracket holds K's write lock from mark to settle, so a mark
/// under a *free* lock is one whose writer is gone — exactly the inference [`crate::s3`]'s reader
/// makes. A mark under a held lock belongs to a live bracket about to settle it, and is left alone.
pub(super) async fn repair_marks(tier: &Tiering, bucket: &str, marked: Vec<String>) -> usize {
    let mut repaired = 0;
    for key in marked {
        let Some(_guard) = tier.locks.try_lock(&key) else {
            continue;
        };
        match tier.repair_locked(bucket, &key).await {
            Ok(_) => repaired += 1,
            // Idempotent and re-derived from a fresh listing next pass, so one key's failure is not
            // worth abandoning the rest for.
            Err(e) => tracing::debug!(bucket, key, error = %e, "transition mark not repaired"),
        }
    }
    repaired
}

/// Reclaim the twins the `<meta>` probe walked past that no longer project any K.
///
/// Every path that moves K refreshes or deletes its twin, but each of those is two writes, and a
/// crash between them leaves the twin behind: beside a live body (eviction, cut between the twin
/// write and the tombstone CAS), beside nothing at all (a settle-absent cut between the twin delete
/// and K's), or beside a tombstone of a *newer* generation (a rehydrate-then-evict cycle).
/// Classification already ignores all three, so this is a cost sweep, not a correctness one —
/// but an unreclaimed twin dilutes every LIST page that covers it, which is paid by clients.
///
/// **Judged under K's lock**, taken with `try_lock` and yielded to whoever holds it: the twin and K
/// are only ever consistent between two writes of one holder, so an unlocked test would see the gap
/// eviction opens between its twin write and its tombstone CAS.
pub(super) async fn reclaim_twins(tier: &Tiering, bucket: &str, found: Vec<String>) -> usize {
    let mut reclaimed = 0;
    for twin in found {
        // Re-derived rather than carried: the base key is a slice of the twin's own, and parsing is
        // what established this was a twin in the first place.
        let Some((base, _)) = meta::parse_twin(&twin) else {
            continue;
        };
        let Some(_guard) = tier.locks.try_lock(base) else {
            continue;
        };
        // "Cannot judge" is not "orphan": a HEAD that failed says nothing about what K holds, and
        // the next probe re-derives the whole judgement anyway.
        let orphan = match projects_a_key(tier, bucket, base, &twin).await {
            Ok(projects) => !projects,
            Err(e) => {
                tracing::debug!(bucket, key = base, error = %e, "twin's key could not be read");
                false
            }
        };
        if !orphan {
            continue;
        }
        match tier.meta.delete(bucket, &twin).await {
            Ok(()) => reclaimed += 1,
            Err(e) => tracing::debug!(bucket, twin, error = %e, "orphan twin not reclaimed"),
        }
    }
    reclaimed
}

/// Whether `twin_key` is the twin K would be given right now. Derived rather than compared field by
/// field: the twin key *is* the facts, so the tombstone's own copy — the authoritative one —
/// re-deriving to the same key is the entire test, and it covers the stale-generation and
/// several-twins cases without either being a case.
async fn projects_a_key(tier: &Tiering, bucket: &str, base: &str, twin_key: &str) -> Result<bool> {
    let md = match tier.data.head(bucket, base).await {
        Ok(head) => head.metadata().cloned().unwrap_or_default(),
        Err(Error::NotFound) => return Ok(false),
        Err(e) => return Err(e),
    };
    match meta::tomb_kind(&md) {
        Some(meta::TombKind::Evict) => Ok(current_twin(&md, base).as_deref() == Some(twin_key)),
        // A mark is mid-bracket: its settle may land a tombstone carrying exactly these facts, and
        // whatever it lands refreshes the twin anyway. Left to `sweep_stale_marks` either way.
        Some(meta::TombKind::Transit) => Ok(true),
        None => Ok(false),
    }
}

fn current_twin(md: &HashMap<String, String>, base: &str) -> Option<String> {
    meta::Facts {
        client_etag: md.get(meta::CETAG)?.clone(),
        plen: md.get(meta::PLEN)?.parse().ok()?,
        mtime_ms: md.get(meta::MTIME)?.parse().ok()?,
    }
    .twin_key(base)
}

/// What one bucket's sweeps returned. The bytes are what a pressured pass counts against its target
/// (rung 0) — mostly stashed part ciphertext, since every other item here is a marker-sized
/// object whose value is the round trips it stops costing rather than the space it frees.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Swept {
    pub(crate) uploads: usize,
    pub(crate) twins: usize,
    pub(crate) marks: usize,
    /// Remote multipart uploads aborted because their cache record was gone.
    pub(crate) orphaned: usize,
    pub(crate) bytes: u64,
}

impl Swept {
    pub(super) fn any(&self) -> bool {
        self.uploads > 0 || self.twins > 0 || self.marks > 0 || self.orphaned > 0
    }
}

impl std::ops::AddAssign for Swept {
    fn add_assign(&mut self, other: Self) {
        self.uploads += other.uploads;
        self.twins += other.twins;
        self.marks += other.marks;
        self.orphaned += other.orphaned;
        self.bytes += other.bytes;
    }
}

/// Every mpu record in `<meta>` with its size, grouped by the upload it belongs to.
async fn cache_ranges(tier: &Tiering, bucket: &str) -> Result<HashMap<String, Vec<(String, u64)>>> {
    let prefix = meta::mpu_scan_prefix();
    let mut ranges: HashMap<String, Vec<(String, u64)>> = HashMap::new();
    let mut token = None;
    loop {
        let page = tier
            .meta
            .list(
                bucket,
                Some(prefix.clone()),
                None,
                token.take(),
                None,
                Some(PAGE_KEYS),
            )
            .await?;
        for obj in page.contents.unwrap_or_default() {
            let Some(key) = obj.key else { continue };
            // A malformed key under this prefix has no upload to judge it against, so leaving it is
            // the only safe reading — it is not evidence of an abandoned upload.
            if let Some(id) = meta::parse_mpu_upload_id(&key) {
                let bytes = obj.size.unwrap_or(0).max(0) as u64;
                ranges
                    .entry(id.to_string())
                    .or_default()
                    .push((key.clone(), bytes));
            }
        }
        match page.next_continuation_token {
            Some(t) => token = Some(t),
            None => return Ok(ranges),
        }
    }
}

/// Whether the cache still has a record for `upload_id`. The create lock can win only against a
/// create that has already released it — i.e. one whose record write finished — so a head that finds
/// no record there is conclusive proof of a leak.
async fn has_record(tier: &Tiering, bucket: &str, upload_id: &str) -> Result<bool> {
    match tier
        .meta
        .head(bucket, &meta::mpu_upload_key(upload_id))
        .await
    {
        Ok(_) => Ok(true),
        Err(Error::NotFound) | Err(Error::NoSuchBucket) => Ok(false),
        Err(e) => Err(e),
    }
}
