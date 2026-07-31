//! Per-bucket write gate: what makes a `DeleteBucket`'s emptiness check a fact about the *future*
//! rather than about the instant the listing ran.
//!
//! A readiness check is a load, not a hold. A write that read `Ready` and then spent a body upload on
//! the wire carries a verdict that stopped being true, and no amount of re-checking closes that — the
//! commit is a network round trip, so there is always a gap between the last check and the wire. So
//! the delete has to establish two things *together*: the namespace is empty, and nothing is in a
//! position to add to it.
//!
//! Both live in one `AtomicU64`:
//!
//! ```text
//! [ closed:1 | epoch:31 | count:32 ]
//! ```
//!
//! `count` is writes admitted and unfinished; `epoch` counts admissions ever. A writer's admission is
//! one CAS that fails if `closed`, so it always moves both. The delete then runs straight-line, with
//! no waiting anywhere:
//!
//! 1. load the word — a non-zero `count` means a write is in flight, so **refuse and touch nothing**;
//! 2. list the namespace;
//! 3. CAS that exact word to `closed`.
//!
//! The CAS is the whole design. It succeeds only if no writer was admitted between the load and it,
//! which is precisely the interval the listing spans — so the three writers that could invalidate the
//! listing are each excluded by a different step: one that finished before the load is *in* the
//! listing, one still running at the load fails step 1, and one admitted during the listing moves the
//! epoch and fails step 3. The epoch is what makes it ABA-safe: a write that is admitted and
//! completes inside the window returns `count` to zero, and without it the CAS could not tell that
//! word from the one it read.
//!
//! What this buys is that **the gate closes only past the point of no return**. Nothing is disturbed
//! on any path that ends in a refusal, so no client is ever told a live bucket is absent — the reason
//! this is a CAS and not a barrier writes queue on, and not a flag published ahead of the listing.
//! Both of those refuse writes speculatively, and a delete that is then refused itself has spent
//! `NoSuchBucket` on a bucket that still exists.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

/// Top bit: a delete has taken the bucket, and no further write is admitted.
const CLOSED: u64 = 1 << 63;
/// Bits 32..62. Wrapping within its own field is deliberate — only *change* is ever asked of it, and
/// a wrap would need 2³¹ admissions inside one listing.
const EPOCH_MASK: u64 = 0x7fff_ffff_0000_0000;
const EPOCH_ONE: u64 = 1 << 32;
/// Bits 0..31: in-flight writes, so bounded by concurrent requests rather than by anything
/// cumulative. Saturating it would take 4·10⁹ of them at once.
const COUNT_MASK: u64 = 0x0000_0000_ffff_ffff;

fn count(state: u64) -> u64 {
    state & COUNT_MASK
}

/// The word one admission later: `count` up by one, `epoch` moved so the delete's CAS can see that it
/// happened even if this write also finishes before the CAS runs.
fn admitted(state: u64) -> u64 {
    (state & CLOSED) | (state.wrapping_add(EPOCH_ONE) & EPOCH_MASK) | (count(state) + 1)
}

#[derive(Default)]
struct Gate {
    state: AtomicU64,
}

impl Gate {
    fn enter(self: &Arc<Self>) -> Option<WriteGuard> {
        let mut current = self.state.load(Ordering::Relaxed);
        loop {
            // Refusing on a saturated count would answer `NoSuchBucket` for a bucket that is merely
            // busy, which is wrong — but letting the increment carry into the epoch would corrupt the
            // delete's evidence, which is worse, and 2³² concurrent in-flight writes is not a state
            // this process can reach.
            if current & CLOSED != 0 || count(current) == COUNT_MASK {
                return None;
            }
            // Acquire on success: whatever a delete published before closing is visible to a writer
            // that gets in ahead of it.
            match self.state.compare_exchange_weak(
                current,
                admitted(current),
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(WriteGuard { gate: self.clone() }),
                Err(actual) => current = actual,
            }
        }
    }

    /// Release so a delete's later load happens-after this write's commit — the load is what has to
    /// see a namespace this write has finished changing.
    fn leave(&self) {
        self.state.fetch_sub(1, Ordering::Release);
    }
}

/// The gates, keyed by client bucket, published as one immutable map — the same shape and the same
/// lifecycle as [`super::BucketCtl`]'s phase map, because it *is* the same lifecycle: a gate is
/// created with its bucket and dropped with it, and **an absent entry means the bucket is not
/// there**, exactly as an absent phase does.
///
/// Modelling the two separately is what would create questions that have no good answer — whether a
/// reader may install a gate it finds missing, what a deleted bucket's gate should be left saying.
/// Here the actor is the sole writer and readers never mutate, so a read is one atomic load and
/// nothing else. Copy-on-write costs a clone of a map holding tens of entries, paid once per bucket
/// lifecycle event; a sharded concurrent map would be paying for churn this table does not have.
type Gates = Arc<ArcSwap<HashMap<String, Arc<Gate>>>>;

#[derive(Clone, Default)]
pub struct WriteGates {
    table: Gates,
}

impl WriteGates {
    /// Admit a write, or report that the bucket is gone — deleted, or never known. The guard must be
    /// held until the write has committed **and** raised whatever it owes: it is the write's whole
    /// claim on the bucket existing, and a delete that observes it gone treats the namespace as
    /// settled.
    pub fn enter(&self, bucket: &str) -> Option<WriteGuard> {
        self.gate(bucket)?.enter()
    }

    /// Step 1 of a delete: `Some` if no write is in flight. Records the exact word it saw, which
    /// [`Quiescent::close`] must still find there. Nothing is mutated, so a `None` costs the bucket
    /// nothing at all.
    pub(super) fn quiescent(&self, bucket: &str) -> Option<Quiescent> {
        let gate = self.gate(bucket)?;
        let state = gate.state.load(Ordering::Acquire);
        (state & CLOSED == 0 && count(state) == 0).then_some(Quiescent { gate, state })
    }

    /// Give `bucket` an open gate if it has none. Called wherever the actor publishes a bucket's
    /// phase, so every bucket this process serves has one before it can be written to.
    ///
    /// Insert-*if-absent*, never replace: a bucket moving `Restoring` → `Ready` must keep the gate
    /// its in-flight writes are counted in, and a fresh one there would leave a delete reading zero
    /// while writes are still landing.
    pub(super) fn install(&self, bucket: &str) {
        if self.table.load().contains_key(bucket) {
            return;
        }
        self.table.rcu(|current| {
            let mut next = HashMap::clone(current);
            next.entry(bucket.to_string()).or_default();
            Arc::new(next)
        });
    }

    /// Drop a deleted bucket's gate. Safe only because an absent entry refuses rather than installs:
    /// a reader that could create one would walk in behind the drain.
    pub(super) fn retire(&self, bucket: &str) {
        self.table.rcu(|current| {
            let mut next = HashMap::clone(current);
            next.remove(bucket);
            Arc::new(next)
        });
    }

    fn gate(&self, bucket: &str) -> Option<Arc<Gate>> {
        self.table.load().get(bucket).cloned()
    }
}

/// A bucket observed with no write inside it, and the proof — the word that said so.
pub(super) struct Quiescent {
    gate: Arc<Gate>,
    state: u64,
}

impl Quiescent {
    /// Step 3: close the gate iff nothing has been admitted since the observation. `None` means a
    /// write arrived while the caller was listing, so the listing is stale and the delete must
    /// refuse — again without having touched anything.
    pub(super) fn close(self) -> Option<ClosedGate> {
        self.gate
            .state
            .compare_exchange(
                self.state,
                self.state | CLOSED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .ok()
            .map(|_| ClosedGate {
                gate: self.gate,
                committed: false,
            })
    }
}

/// A closed gate, held across the delete's commit. Reopens on drop unless [`Self::commit`] takes it,
/// so a commit that fails — or a panic between the two — returns the bucket to service rather than
/// leaving a live bucket that refuses every write for the life of the process.
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

/// One admitted write. Holds its gate by `Arc`, so a gate discarded by a concurrent create cannot
/// strand the count it is still carrying.
#[must_use = "dropping the guard releases the write's claim on the bucket"]
pub struct WriteGuard {
    gate: Arc<Gate>,
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        self.gate.leave();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A gate table with `b` published, as the actor publishes a bucket it serves.
    fn published() -> WriteGates {
        let gates = WriteGates::default();
        gates.install("b");
        gates
    }

    #[test]
    fn a_write_in_flight_refuses_the_delete_without_touching_the_gate() {
        let gates = published();
        let held = gates.enter("b").expect("open gate admits");

        assert!(gates.quiescent("b").is_none(), "a live write is visible");
        drop(held);

        // The refused observation cost the bucket nothing: writes never stopped being admitted.
        assert!(gates.enter("b").is_some(), "the gate was never closed");
        assert!(gates.quiescent("b").is_some(), "and it is quiescent again");
    }

    #[test]
    fn a_write_admitted_during_the_listing_defeats_the_close() {
        let gates = published();
        let seen = gates.quiescent("b").expect("an idle bucket is quiescent");
        let racer = gates.enter("b").expect("open gate admits");
        assert!(
            seen.close().is_none(),
            "the listing is stale; the delete must refuse"
        );
        drop(racer);
        assert!(
            gates.enter("b").is_some(),
            "a defeated close must leave the gate open"
        );
    }

    // The ABA case, and the whole reason for the epoch: the racer is gone by the time the CAS runs,
    // so `count` is back to zero and only a moved epoch distinguishes this word from the one
    // observed.
    #[test]
    fn a_write_that_starts_and_finishes_during_the_listing_defeats_the_close() {
        let gates = published();
        let seen = gates.quiescent("b").expect("an idle bucket is quiescent");
        drop(gates.enter("b").expect("open gate admits"));
        assert!(
            seen.close().is_none(),
            "a write that came and went still invalidates the listing"
        );
    }

    #[test]
    fn a_closed_gate_refuses_writes_and_further_deletes() {
        let gates = published();
        let seen = gates.quiescent("b").expect("an idle bucket is quiescent");
        seen.close().expect("an idle gate closes").commit();

        assert!(gates.enter("b").is_none(), "a closed gate admits nothing");
        assert!(gates.quiescent("b").is_none(), "and is not quiescent");
    }

    #[test]
    fn an_uncommitted_close_reopens() {
        let gates = published();
        let seen = gates.quiescent("b").expect("an idle bucket is quiescent");
        drop(seen.close().expect("an idle gate closes"));
        assert!(
            gates.enter("b").is_some(),
            "a delete that failed to commit must return the bucket to service"
        );
    }

    // An unknown bucket has no gate, and a reader may not create one — that is what makes retiring
    // a deleted bucket's gate safe, and what a recreated name relies on to start open.
    #[test]
    fn a_retired_gate_refuses_until_the_bucket_is_published_again() {
        let gates = published();
        let seen = gates.quiescent("b").expect("an idle bucket is quiescent");
        seen.close().expect("an idle gate closes").commit();
        gates.retire("b");

        assert!(
            gates.enter("b").is_none(),
            "an unknown bucket admits nothing"
        );
        assert!(gates.quiescent("b").is_none(), "and cannot be deleted");

        gates.install("b");
        assert!(
            gates.enter("b").is_some(),
            "a recreated bucket starts with an open gate"
        );
    }

    // `Restoring` → `Ready` republishes the phase, and the gate must survive it holding the writes
    // it has already counted.
    #[test]
    fn install_never_replaces_a_gate_that_is_counting_writes() {
        let gates = published();
        let held = gates.enter("b").expect("open gate admits");
        gates.install("b");
        assert!(
            gates.quiescent("b").is_none(),
            "a republished phase must not lose the writes in flight"
        );
        drop(held);
        assert!(gates.quiescent("b").is_some());
    }

    // Under real contention the only outcome that must never occur is a close that succeeds while a
    // write is inside — that is the one the emptiness listing would be wrong about. Closers keep
    // trying until the writers are done, so the run is guaranteed to contain successful closes: a
    // gate that simply refused every one would pass the safety assertion while proving nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_close_never_wins_against_a_live_write() {
        const WRITERS: usize = 6;
        let gates = published();
        let inside = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let retired = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..WRITERS {
            let gates = gates.clone();
            let inside = inside.clone();
            let retired = retired.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..2_000 {
                    if let Some(guard) = gates.enter("b") {
                        inside.fetch_add(1, Ordering::AcqRel);
                        tokio::task::yield_now().await;
                        inside.fetch_sub(1, Ordering::AcqRel);
                        drop(guard);
                    }
                    // Leaves quiescent windows for a closer to find; without them the writers simply
                    // hand the gate to each other and no close ever gets a look.
                    tokio::task::yield_now().await;
                }
                retired.fetch_add(1, Ordering::AcqRel);
            }));
        }
        for _ in 0..3 {
            let gates = gates.clone();
            let inside = inside.clone();
            let closes = closes.clone();
            let retired = retired.clone();
            tasks.push(tokio::spawn(async move {
                while retired.load(Ordering::Acquire) < WRITERS {
                    let Some(seen) = gates.quiescent("b") else {
                        tokio::task::yield_now().await;
                        continue;
                    };
                    // Stands in for the emptiness listing: the window the CAS has to cover.
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
