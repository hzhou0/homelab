//! The eviction gates (§8) — the only place in hypha that removes a body a client can still ask for.
//!
//! **Write-awareness is a property of the remote, not of process memory.** The hazard is one step:
//! tombstoning a body the remote does not hold *in that generation*. An in-flight-PUT counter used to
//! guard it, but the window no longer belongs to a single request (§7 — the marker write outlives the
//! ack), and a counter never covered a marker owed by a process that has since died. So the guard is
//! entirely cache-and-remote observable, and one check subsumes three hazards: a markerless
//! just-written body, a marker lost to a crash, and the corruption a bare *presence* check would
//! allow — where the remote holds an older generation, the tombstone is stamped with the cache body's
//! facts, and reads then return the old plaintext under the new ETag and length.
//!
//! The three gates layer marker → remote generation → conditional CAS, which is what makes every
//! interleaving auto-healing rather than lossy: a writer landing anywhere between them has moved the
//! ETag, so the CAS fails and eviction simply retries next pass.

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use super::scan::{Artifact, Candidate};
use crate::tier::{quote, single_part_framed_len, Tiering};

/// Reclaim the candidate, or decline to. `Ok(0)` is a decline — a gate refused, or a writer moved the
/// key — and is not a failure: every reason to decline is either transient or self-healing, and the
/// next pass re-derives the whole judgement from a fresh listing.
pub(super) async fn evict(tier: &Tiering, candidate: &Candidate) -> Result<u64> {
    match &candidate.artifact {
        Artifact::Body(key) => evict_body(tier, candidate, key).await,
        Artifact::Shadow(shadow) => drop_shadow(tier, candidate, shadow).await,
    }
}

async fn evict_body(tier: &Tiering, candidate: &Candidate, key: &str) -> Result<u64> {
    let (bucket, etag) = (&candidate.bucket, &candidate.etag);

    // Gate 1 — a pending marker means the remote is owed this generation. A cheap local
    // short-circuit that spares the remote round trip below, not the correctness gate itself.
    match tier.meta.head(bucket, meta::pending_marker_key(key)).await {
        Ok(_) => return Ok(0),
        Err(Error::NotFound) | Err(Error::NoSuchBucket) => {}
        Err(e) => return Err(e),
    }

    // Gate 2 — durability. A skip here **raises the key's pending marker**: this check has just
    // established the one thing a marker records, gate 1 established there is none, and no other path
    // can see a body that is cache-only because a write forgot to owe one. It costs no extra round
    // trip and it lands on cold keys — precisely the ones no future write would have re-owed (§8).
    if !remote_holds_generation(tier, candidate, key).await? {
        tier.raise_marker(bucket, key, etag).await?;
        tracing::debug!(
            bucket = %bucket,
            key,
            "eviction candidate is not durable; marker raised"
        );
        return Ok(0);
    }

    // Gate 3 — the CAS, under K's lock. Twin before tombstone, so a sentinel always has its twin; a
    // crash between leaves a twin next to a live body, which classification ignores (§6) and a later
    // sweep reclaims.
    let _guard = tier.locks.lock(key).await;
    match tier.tombstone_locked(bucket, key, etag).await {
        Ok(()) => Ok(candidate.bytes),
        // A client wrote, deleted, or completed K while we were judging it. Its eviction was the one
        // that should not have run.
        Err(Error::PreconditionFailed) | Err(Error::NotFound) => Ok(0),
        Err(e) => Err(e),
    }
}

/// Reclaim a rehydrated composite's shadow body (§6/§8) — one conditional delete, and none of the
/// three gates above.
///
/// **No durability gate, because there is nothing to gate.** A shadow only ever exists because a
/// rehydrate fetched that composite from the remote and decrypted it, and `land_shadow_locked` leaves
/// K's tombstone and twin untouched throughout — so the remote demonstrably holds the object, K still
/// points at it, and dropping the shadow costs at most a re-fetch on the next read. That is also why
/// this needs no lock: nothing here can be half-applied, and there is no K to lock on anyway, since a
/// shadow key is the digest of K.
///
/// **Conditional on the observed ETag**, though, because a rehydrate may have landed a *newer*
/// generation between the probe and now. Deleting that would still be safe, but it would throw away a
/// transfer that just completed for a client who wanted it.
async fn drop_shadow(tier: &Tiering, candidate: &Candidate, shadow: &str) -> Result<u64> {
    match tier
        .meta
        .delete_if_match(&candidate.bucket, shadow, quote(&candidate.etag))
        .await
    {
        Ok(()) => Ok(candidate.bytes),
        Err(Error::PreconditionFailed) | Err(Error::NotFound) => Ok(0),
        Err(e) => Err(e),
    }
}

/// Whether the remote holds *this* candidate's generation.
///
/// A single-part object's framed size is a closed-form function of its plaintext length, so a HEAD
/// settles the common case: any other generation of a different length is refused on one round trip.
/// Only a same-plaintext-length candidate is ambiguous, and only it pays the trailer's `cetag` — the
/// same triage the pending-set rebuild runs (§7). A composite has no such closed form (its remote
/// form is per-part age files), so it goes straight to the trailer.
async fn remote_holds_generation(tier: &Tiering, candidate: &Candidate, key: &str) -> Result<bool> {
    let (bucket, etag) = (&candidate.bucket, &candidate.etag);

    if !meta::is_composite_etag(etag) {
        let framed = match tier.remote.head(bucket, key).await {
            Ok(head) => head.content_length().unwrap_or(0).max(0) as u64,
            Err(Error::NotFound) | Err(Error::NoSuchBucket) => return Ok(false),
            Err(e) => return Err(e),
        };
        if single_part_framed_len(candidate.bytes) != Some(framed) {
            return Ok(false);
        }
    }
    match tier.remote_generation_matches(bucket, key, etag).await {
        Err(Error::NotFound) | Err(Error::NoSuchBucket) => Ok(false),
        other => other,
    }
}
