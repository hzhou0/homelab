//! One `AtomicU64` per bucket: the lifecycle word and both in-flight counts in a single CAS, so a
//! decision is *made* by the transition, never by a second load that could move underneath it.
//!
//! ```text
//! [ ready:1 | closed:1 | epoch:30 | rcount:16 | wcount:16 ]
//! ```
//!
//! `epoch` moves on every write admission so a write that starts and finishes inside a delete's
//! emptiness listing still defeats the close (zero-count ABA). `rcount` is masked out of the close
//! because deletes never wait on readers. Counts saturate and refuse rather than carry into a
//! neighbour, which would corrupt the close's evidence.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const READY: u64 = 1 << 63;
const CLOSED: u64 = 1 << 62;
/// Wrapping is fine: only *change* is ever asked of it, and a wrap needs 2³⁰ admissions in one listing.
const EPOCH_MASK: u64 = 0x3fff_ffff_0000_0000;
const EPOCH_ONE: u64 = 1 << 32;
const RCOUNT_MASK: u64 = 0x0000_0000_ffff_0000;
const RCOUNT_ONE: u64 = 1 << 16;
const WCOUNT_MASK: u64 = 0x0000_0000_0000_ffff;

/// The whole of a bucket's visible state. `Absent` is not a bit pattern: it is the map having no
/// entry at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketStatus {
    /// Deleted, or never known. Definitive.
    Absent,
    /// A `DeleteBucket` is between its emptiness check and its commit; its fate is undecided.
    Deleting,
    Restoring,
    Ready,
}

/// Where a read must go for its answer, decided atomically with its ticket.
pub enum Readout {
    /// No ticket — `Ready` never reverts.
    Cache,
    /// Held for the whole remote answer, so a cached-mode write admitted meanwhile cannot commit
    /// cache-first into it.
    Remote(ReadGuard),
}

/// What a write may do, decided by the CAS that admitted it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    /// `Ready` with no restoring read in flight.
    CachedEligible,
    /// `Restoring`, or `Ready` with a straggler's ticket out — cache-first there would let that
    /// read answer stale. Never blocked, just forced remote-first for this one op.
    Durable,
}

/// Why the gate refused an op — one answer shared by reads and writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// No gate, so no bucket. Definitive.
    Absent,
    /// A delete mid-flight: retryable, since the delete may still fail. (Unreachably, saturation
    /// refuses the same way.)
    Closed,
}

fn rcount(word: u64) -> u64 {
    word & RCOUNT_MASK
}

fn wcount(word: u64) -> u64 {
    word & WCOUNT_MASK
}

// ── the transition table ────
//
// One row per transition; [`Gate::advance`] re-runs a row on a lost race, so the CAS *is* the
// classification. A row that returns `None` refuses, and the word it refused on is the answer.

/// `wcount + 1`, with the epoch moved so a write that finishes before the close CAS still defeats it
/// (ABA). Saturation refuses rather than carry into the epoch.
fn admit_write(word: u64) -> Option<u64> {
    (word & CLOSED == 0 && wcount(word) != WCOUNT_MASK).then(|| {
        (word & !(EPOCH_MASK | WCOUNT_MASK))
            | (word.wrapping_add(EPOCH_ONE) & EPOCH_MASK)
            | (wcount(word) + 1)
    })
}

/// `rcount + 1`, no epoch move — one would fail closes on readers, the very thing the mask is for.
fn take_ticket(word: u64) -> Option<u64> {
    (word & (READY | CLOSED) == 0 && rcount(word) != RCOUNT_MASK).then_some(word + RCOUNT_ONE)
}

/// The flip (`Restoring` → `Ready`). Refuses `closed` — a flip must not contradict a delete's
/// emptiness listing — and tolerates whatever the counters hold.
fn set_ready(word: u64) -> Option<u64> {
    (word & (READY | CLOSED) == 0).then_some(word | READY)
}

/// The delete's close, valid only while nothing the listing depended on changed. `rcount` is masked
/// — deletes never wait on readers — but a flip in the window fails it: the listing read the remote
/// namespace, a `Ready` bucket's is the cache's.
fn set_closed(observed: u64) -> impl Fn(u64) -> Option<u64> {
    move |word| (word & !RCOUNT_MASK == observed & !RCOUNT_MASK).then_some(word | CLOSED)
}

pub(super) struct Gate {
    state: AtomicU64,
}

impl Gate {
    pub(super) fn new(status: BucketStatus) -> Self {
        Gate {
            state: AtomicU64::new(if status == BucketStatus::Ready {
                READY
            } else {
                0
            }),
        }
    }

    /// Run one row of the table. `Ok`/`Err` carry the pre-image / refusing word, so the caller's
    /// answer is the CAS itself, never a fresh load.
    ///
    /// `AcqRel` on success: an op admitted ahead of a delete sees what it published; a delete's later
    /// load happens-after the writes it is allowed to miss.
    fn advance(&self, row: impl Fn(u64) -> Option<u64>) -> Result<u64, u64> {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            let next = row(current).ok_or(current)?;
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(current),
                Err(actual) => current = actual,
            }
        }
    }

    pub(super) fn status(&self) -> BucketStatus {
        let word = self.state.load(Ordering::Acquire);
        if word & CLOSED != 0 {
            BucketStatus::Deleting
        } else if word & READY != 0 {
            BucketStatus::Ready
        } else {
            BucketStatus::Restoring
        }
    }

    /// Admit a write, classifying it in the same CAS. The guard is the write's whole claim on the
    /// bucket existing — hold it until the write has committed and raised whatever it owes.
    pub(super) fn enter_write(self: &Arc<Self>) -> Result<(WriteGuard, Admission), Refusal> {
        let observed = self.advance(admit_write).map_err(|_| Refusal::Closed)?;
        Ok((WriteGuard { gate: self.clone() }, admission(observed)))
    }

    /// Classify a read, taking a ticket if the answer must come from the remote. The refusal needs
    /// no re-check — the word it refused on already says which.
    pub(super) fn read_ticket(self: &Arc<Self>) -> Result<Readout, Refusal> {
        match self.advance(take_ticket) {
            Ok(_) => Ok(Readout::Remote(ReadGuard { gate: self.clone() })),
            // `closed` outranks `ready`: a cache-authoritative bucket being deleted must not serve.
            Err(word) if word & CLOSED == 0 && word & READY != 0 => Ok(Readout::Cache),
            Err(_) => Err(Refusal::Closed),
        }
    }

    /// End a restore. In-place and one-way, so a delete reading the counts across the flip cannot
    /// see zero.
    pub(super) fn flip(&self) {
        let _ = self.advance(set_ready);
    }

    /// Step 1 of a delete: `Some` only if nothing is closing or writing. Records the word
    /// [`Quiescent::close`] must still find. Nothing is mutated.
    pub(super) fn quiescent(self: &Arc<Self>) -> Option<Quiescent> {
        let observed = self.state.load(Ordering::Acquire);
        (observed & CLOSED == 0 && wcount(observed) == 0).then(|| Quiescent {
            gate: self.clone(),
            observed,
        })
    }
}

/// Read off the admission's pre-image, so it is atomic with it. `ready` never reverts, so any
/// restoring read still to answer is already counted.
fn admission(observed: u64) -> Admission {
    if observed & READY != 0 && rcount(observed) == 0 {
        Admission::CachedEligible
    } else {
        Admission::Durable
    }
}

pub(super) struct Quiescent {
    gate: Arc<Gate>,
    observed: u64,
}

impl Quiescent {
    /// Step 3: close iff nothing the listing depended on changed. `None` means a write or flip
    /// landed mid-listing — the delete must refuse, again without touching anything.
    pub(super) fn close(self) -> Option<ClosedGate> {
        self.gate.advance(set_closed(self.observed)).ok()?;
        Some(ClosedGate {
            gate: self.gate,
            committed: false,
        })
    }
}

/// Held across the delete's commit; reopens on drop unless [`Self::commit`] took it, so a failed
/// commit returns the bucket to service.
#[must_use = "dropping this reopens the gate"]
pub(super) struct ClosedGate {
    gate: Arc<Gate>,
    committed: bool,
}

impl ClosedGate {
    pub(super) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for ClosedGate {
    fn drop(&mut self) {
        if !self.committed {
            self.gate.state.fetch_and(!CLOSED, Ordering::Release);
        }
    }
}

/// Release on drop, so a delete's later load happens-after this write's commit.
#[must_use = "dropping the guard releases the write's claim on the bucket"]
pub struct WriteGuard {
    gate: Arc<Gate>,
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        self.gate.state.fetch_sub(1, Ordering::Release);
    }
}

/// One restoring read, held until its remote answer is computed.
#[must_use = "dropping the ticket lets cached writes commit cache-first again"]
pub struct ReadGuard {
    gate: Arc<Gate>,
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.gate.state.fetch_sub(RCOUNT_ONE, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A bucket mid-restore.
    fn restoring() -> Arc<Gate> {
        Arc::new(Gate::new(BucketStatus::Restoring))
    }

    /// A bucket serving from its cache.
    fn ready() -> Arc<Gate> {
        Arc::new(Gate::new(BucketStatus::Ready))
    }

    fn admit(gate: &Arc<Gate>) -> (WriteGuard, Admission) {
        gate.enter_write().expect("an open gate admits")
    }

    #[test]
    fn a_write_in_flight_refuses_the_delete_without_touching_the_gate() {
        let gate = ready();
        let held = admit(&gate);

        assert!(gate.quiescent().is_none(), "a live write is visible");
        drop(held);

        // The refused observation cost the bucket nothing: writes never stopped being admitted.
        assert!(gate.enter_write().is_ok(), "the gate was never closed");
        assert!(gate.quiescent().is_some(), "and it is quiescent again");
    }

    #[test]
    fn a_write_admitted_during_the_listing_defeats_the_close() {
        let gate = ready();
        let seen = gate.quiescent().expect("an idle bucket is quiescent");
        let racer = admit(&gate);
        assert!(
            seen.close().is_none(),
            "the listing is stale; the delete must refuse"
        );
        drop(racer);
        assert!(
            gate.enter_write().is_ok(),
            "a defeated close must leave the gate open"
        );
    }

    // The ABA case, and the whole reason for the epoch: only a moved epoch distinguishes this word
    // once the racer is gone.
    #[test]
    fn a_write_that_starts_and_finishes_during_the_listing_defeats_the_close() {
        let gate = ready();
        let seen = gate.quiescent().expect("an idle bucket is quiescent");
        drop(admit(&gate));
        assert!(
            seen.close().is_none(),
            "a write that came and went still invalidates the listing"
        );
    }

    // The mask, from the other side: deletes never wait on readers.
    #[test]
    fn readers_never_defeat_the_close() {
        let gate = restoring();
        let seen = gate.quiescent().expect("an idle bucket is quiescent");
        let Ok(Readout::Remote(held)) = gate.read_ticket() else {
            panic!("a restoring bucket hands out a ticket");
        };
        drop(gate.read_ticket());
        assert!(
            seen.close().is_some(),
            "a delete must not wait on readers, however they overlap it"
        );
        drop(held);
    }

    #[test]
    fn a_closed_gate_refuses_every_op_and_further_deletes() {
        let gate = ready();
        let seen = gate.quiescent().expect("an idle bucket is quiescent");
        seen.close().expect("an idle gate closes").commit();

        assert!(
            matches!(gate.enter_write(), Err(Refusal::Closed)),
            "a closed gate admits no write"
        );
        assert_eq!(
            gate.read_ticket().err(),
            Some(Refusal::Closed),
            "and no read"
        );
        assert_eq!(gate.status(), BucketStatus::Deleting);
        assert!(gate.quiescent().is_none(), "and is not quiescent");
    }

    #[test]
    fn an_uncommitted_close_reopens() {
        let gate = ready();
        let seen = gate.quiescent().expect("an idle bucket is quiescent");
        drop(seen.close().expect("an idle gate closes"));
        assert!(
            gate.enter_write().is_ok(),
            "a delete that failed to commit must return the bucket to service"
        );
        assert_eq!(gate.status(), BucketStatus::Ready);
    }

    #[test]
    fn the_flip_is_one_way_and_refuses_a_closing_bucket() {
        let gate = restoring();
        assert_eq!(gate.status(), BucketStatus::Restoring);
        gate.flip();
        gate.flip();
        assert_eq!(gate.status(), BucketStatus::Ready, "the flip never reverts");

        let closing = restoring();
        let seen = closing.quiescent().expect("an idle bucket is quiescent");
        let closed = seen.close().expect("an idle gate closes");
        closing.flip();
        assert_eq!(
            closing.status(),
            BucketStatus::Deleting,
            "a flip must not contradict the delete's emptiness listing"
        );
        drop(closed);
    }

    // The straggler's other half: the CAS failure itself redirects the read to the cache.
    #[test]
    fn a_restoring_read_is_redirected_to_the_cache_once_the_flip_lands() {
        let gate = restoring();
        gate.flip();
        assert!(matches!(gate.read_ticket(), Ok(Readout::Cache)));
    }

    #[test]
    fn a_reader_in_flight_defers_the_cached_write_to_durable() {
        let gate = restoring();
        let Ok(Readout::Remote(ticket)) = gate.read_ticket() else {
            panic!("a restoring bucket hands out a ticket");
        };
        assert_eq!(
            admit(&gate).1,
            Admission::Durable,
            "a write admitted before the flip commits remotely"
        );

        gate.flip();
        assert_eq!(
            admit(&gate).1,
            Admission::Durable,
            "the straggler's ticket forces the stronger semantics"
        );

        drop(ticket);
        assert_eq!(
            admit(&gate).1,
            Admission::CachedEligible,
            "and cache-first returns the moment it drops"
        );
    }

    // The flip must be in-place: a fresh gate would hide the counts a delete reads across it.
    #[test]
    fn the_flip_keeps_the_writes_already_counted() {
        let gate = restoring();
        let held = admit(&gate);
        gate.flip();
        assert!(
            gate.quiescent().is_none(),
            "the flip must not lose the writes in flight"
        );
        drop(held);
        assert!(gate.quiescent().is_some());
    }

    // The only forbidden outcome is a close winning over a live write. Closers keep trying until
    // writers are done, so the run must contain successful closes — a gate refusing every one would
    // pass the safety assertion while proving nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_close_never_wins_against_a_live_write() {
        const WRITERS: usize = 6;
        let gate = ready();
        let inside = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let retired = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..WRITERS {
            let gate = gate.clone();
            let inside = inside.clone();
            let retired = retired.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..2_000 {
                    if let Ok(guard) = gate.enter_write() {
                        inside.fetch_add(1, Ordering::AcqRel);
                        tokio::task::yield_now().await;
                        inside.fetch_sub(1, Ordering::AcqRel);
                        drop(guard);
                    }
                    // Leaves quiescent windows for a closer to find.
                    tokio::task::yield_now().await;
                }
                retired.fetch_add(1, Ordering::AcqRel);
            }));
        }
        for _ in 0..3 {
            let gate = gate.clone();
            let inside = inside.clone();
            let closes = closes.clone();
            let retired = retired.clone();
            tasks.push(tokio::spawn(async move {
                while retired.load(Ordering::Acquire) < WRITERS {
                    let Some(seen) = gate.quiescent() else {
                        tokio::task::yield_now().await;
                        continue;
                    };
                    // Stands in for the emptiness listing: the window the close CAS must cover.
                    tokio::task::yield_now().await;
                    let Some(closed) = seen.close() else {
                        continue;
                    };
                    assert_eq!(
                        inside.load(Ordering::Acquire),
                        0,
                        "a close succeeded with a write inside the gate"
                    );
                    closes.fetch_add(1, Ordering::AcqRel);
                    // Uncommitted, so the bucket returns to service and the writers keep running.
                    drop(closed);
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert!(
            closes.load(Ordering::Acquire) > 0,
            "the test proves nothing if no close ever succeeded"
        );
    }
}
