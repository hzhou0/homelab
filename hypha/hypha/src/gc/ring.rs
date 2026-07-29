//! The recency sketch (§8) — the only thing GC knows about what clients want.
//!
//! Plain state, owned by [`super::GcActor`] and reached only from its task: no lock, no interior
//! mutability, and nothing here is `Send`-shared. What keeps it off the request path is the actor
//! boundary in [`super`], not anything in this file.
//!
//! Recency is a **Bloom-ring sketch**: one filter per *fill window*, retained `depth` deep behind
//! the current one. A probe answers with the newest slice holding the key, which is a quantized
//! last-access age — `depth + 1` buckets from the current window down to [`Age::Miss`], colder than
//! anything the ring remembers.
//!
//! **Denominated in distinct keys, never in time.** A slice rotates when its distinct-key fill
//! reaches the design point, so recency is relative to *competing traffic*: an idle cache holds its
//! working set indefinitely, and nothing ages out except by displacement. Rotating on fill also
//! bounds each slice's false-positive rate by construction — no read rate can silently saturate a
//! slice into reporting every key as recent, which is the protect-everything failure this mechanism
//! exists to avoid. Fill stays exact because `insert` reports whether the key was already present,
//! so a hot key touched a thousand times advances it once.
//!
//! **One ring for the deployment**, keyed by fully qualified `<bucket>/<key>` and persisted to GC's
//! own bucket rather than per-bucket `<meta>`. Recency is competition-relative, so the competition
//! has to be the whole deployment's traffic: a per-bucket ring would let a quiet bucket keep its
//! working set warm indefinitely while a busy one aged its own out, and the two would then be
//! compared against a single eviction threshold as though the numbers meant the same thing.
//!
//! **Advisory, and only advisory.** A ring that is lost, cold, or stale (first boot, a failover
//! without a persisted ring, a parameter change that invalidates the slices on disk) collapses every
//! key into one bucket, and eviction ordering degrades to LastModified for a churnier cycle. It
//! never degrades to incorrectness: the correctness gates that decide whether a key *may* be evicted
//! are elsewhere and absolute (§8).

use std::collections::VecDeque;

use fastbloom::BloomFilter;

use hypha_core::config::Recency;

/// The hash seed, compiled in — **load-bearing for persistence**. `fastbloom`'s default hasher
/// derives its SipHash key from process entropy (from `rand`, or from foldhash's `RandomState` with
/// that feature off, which is why disabling it is not by itself enough), so a slice written by one
/// process would answer noise when another read it back. A fixed seed is what makes a persisted
/// slice mean the same thing in the process that reloads it.
const HASH_SEED: u128 = 0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c835;

/// Quantized last-access age: the newest slice holding the key. Ordered **coldest-greatest**, which
/// is what lets eviction take "everything at or above the threshold" as a comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[allow(dead_code)] // the eviction scan is its only reader (phase 5d)
pub(super) enum Age {
    /// Slices back from the current window; `Window(0)` is the current one.
    Window(u16),
    /// Colder than everything the ring remembers.
    Miss,
}

/// Slice geometry, derived once (see [`Recency`]) so every slice in the ring — and every slice read
/// back off disk — agrees on the bit count and hash count that make its bits mean anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Geometry {
    bits: usize,
    hashes: u32,
    fill_target: usize,
    depth: usize,
}

struct Slice {
    filter: BloomFilter,
    /// Keys the filter had not already seen — the exact fill, not the bit population.
    distinct: usize,
}

impl Slice {
    fn empty(cfg: &Recency) -> Self {
        Slice {
            filter: BloomFilter::with_false_pos(cfg.false_positive_rate)
                .seed(&HASH_SEED)
                .expected_items(cfg.fill_target.max(1)),
            distinct: 0,
        }
    }
}

/// Fully qualified, because the ring spans buckets and a key is only unique within one. Bucket names
/// cannot contain `/`, so the join is unambiguous.
pub(super) fn qualified(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}

/// A rotated-out slice, encoded, on its way to GC's bucket.
pub(super) struct RetiredSlice {
    pub seq: u64,
    pub body: Vec<u8>,
}

pub(super) struct RecencyRing {
    cfg: Recency,
    geometry: Geometry,
    current: Slice,
    /// Newest first, so a probe's index *is* the age.
    retired: VecDeque<Slice>,
    next_seq: u64,
}

impl RecencyRing {
    pub(super) fn new(cfg: &Recency) -> Self {
        let current = Slice::empty(cfg);
        let geometry = Geometry {
            bits: current.filter.num_bits(),
            hashes: current.filter.num_hashes(),
            fill_target: cfg.fill_target.max(1),
            // A ring one slice deep still answers current-vs-miss, the distinction eviction most
            // depends on; zero would make every probe a miss.
            depth: cfg.depth.max(1),
        };
        RecencyRing {
            cfg: *cfg,
            geometry,
            current,
            retired: VecDeque::new(),
            next_seq: 0,
        }
    }

    /// Record interest in a key, yielding a slice to persist when the touch filled the window.
    pub(super) fn record(&mut self, qualified: String) -> Option<RetiredSlice> {
        if !self.current.filter.insert(&qualified) {
            self.current.distinct += 1;
        }
        (self.current.distinct >= self.geometry.fill_target).then(|| self.rotate())
    }

    #[allow(dead_code)] // the eviction scan is its only caller (phase 5d)
    pub(super) fn probe(&self, qualified: &str) -> Age {
        if self.current.filter.contains(qualified) {
            return Age::Window(0);
        }
        self.retired
            .iter()
            .position(|s| s.filter.contains(qualified))
            .map(|i| Age::Window(i as u16 + 1))
            .unwrap_or(Age::Miss)
    }

    /// Install slices read back from GC's bucket, newest first. They land *behind* whatever this
    /// process has already collected, which is what makes the age ordering right regardless of when
    /// the read lands.
    pub(super) fn install(&mut self, slices: Vec<(u64, Vec<u8>)>) {
        for (seq, body) in slices {
            self.next_seq = self.next_seq.max(seq + 1);
            match decode(&body, &self.geometry) {
                Some(slice) => self.retired.push_back(slice),
                // A slice written under a different geometry cannot be read under this one, and
                // guessing would be worse than starting cold (§8: advisory, never incorrect).
                None => tracing::info!(seq, "recency slice ignored; geometry changed"),
            }
        }
        self.retired.truncate(self.geometry.depth);
    }

    pub(super) fn depth(&self) -> usize {
        self.geometry.depth
    }

    /// Retire the current slice and start an empty one, returning what should be persisted.
    fn rotate(&mut self) -> RetiredSlice {
        let seq = self.next_seq;
        self.next_seq += 1;
        let slice = std::mem::replace(&mut self.current, Slice::empty(&self.cfg));
        let body = encode(&slice, &self.geometry);
        self.retired.push_front(slice);
        self.retired.truncate(self.geometry.depth);
        RetiredSlice { seq, body }
    }
}

/// `version ‖ hashes ‖ bits ‖ distinct ‖ words`. The geometry is carried, not assumed, so a reload
/// under changed parameters can recognize a slice it cannot use instead of misreading its bits.
/// Bump the version for any change to hashing or layout — the seed and the `Hash` impl are as much
/// part of a slice's meaning as its bit count is.
const SLICE_VERSION: u8 = 1;
const SLICE_HEADER_LEN: usize = 1 + 4 + 8 + 8;

fn encode(slice: &Slice, geometry: &Geometry) -> Vec<u8> {
    let words = slice.filter.as_slice();
    let mut out = Vec::with_capacity(SLICE_HEADER_LEN + words.len() * 8);
    out.push(SLICE_VERSION);
    out.extend_from_slice(&geometry.hashes.to_le_bytes());
    out.extend_from_slice(&(geometry.bits as u64).to_le_bytes());
    out.extend_from_slice(&(slice.distinct as u64).to_le_bytes());
    for word in words {
        out.extend_from_slice(&word.to_le_bytes());
    }
    out
}

fn decode(body: &[u8], geometry: &Geometry) -> Option<Slice> {
    if body.len() < SLICE_HEADER_LEN || body[0] != SLICE_VERSION {
        return None;
    }
    let hashes = u32::from_le_bytes(body[1..5].try_into().ok()?);
    let bits = u64::from_le_bytes(body[5..13].try_into().ok()?) as usize;
    let distinct = u64::from_le_bytes(body[13..21].try_into().ok()?) as usize;
    if hashes != geometry.hashes || bits != geometry.bits {
        return None;
    }
    let words: Vec<u64> = body[SLICE_HEADER_LEN..]
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact(8) yields 8 bytes")))
        .collect();
    if words.len() * 64 != bits {
        return None;
    }
    Some(Slice {
        filter: BloomFilter::from_vec(words).seed(&HASH_SEED).hashes(hashes),
        distinct,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_ring(fill_target: usize, depth: usize) -> RecencyRing {
        RecencyRing::new(&Recency {
            fill_target,
            depth,
            false_positive_rate: 0.01,
        })
    }

    fn touch(ring: &mut RecencyRing, bucket: &str, key: &str) -> Option<RetiredSlice> {
        ring.record(qualified(bucket, key))
    }

    fn age(ring: &RecencyRing, bucket: &str, key: &str) -> Age {
        ring.probe(&qualified(bucket, key))
    }

    #[test]
    fn duplicate_touches_do_not_advance_fill() {
        let mut ring = fresh_ring(8, 2);
        for _ in 0..100 {
            assert!(
                touch(&mut ring, "b", "hot").is_none(),
                "one key touched 100 times must not rotate a slice sized for 8"
            );
        }
        assert_eq!(age(&ring, "b", "hot"), Age::Window(0));
    }

    #[test]
    fn fill_rotates_and_ages_by_slice() {
        let mut ring = fresh_ring(4, 3);
        for i in 0..3 {
            assert!(touch(&mut ring, "b", &format!("k{i}")).is_none());
        }
        assert!(
            touch(&mut ring, "b", "k3").is_some(),
            "reaching the fill target rotates a slice"
        );

        assert_eq!(age(&ring, "b", "k0"), Age::Window(1));
        touch(&mut ring, "b", "fresh");
        assert_eq!(age(&ring, "b", "fresh"), Age::Window(0));
        assert_eq!(age(&ring, "b", "never-touched"), Age::Miss);
    }

    #[test]
    fn keys_fall_off_the_end_by_displacement() {
        let mut ring = fresh_ring(2, 1);
        touch(&mut ring, "b", "old0");
        touch(&mut ring, "b", "old1");
        for i in 0..4 {
            touch(&mut ring, "b", &format!("new{i}"));
        }
        assert_eq!(age(&ring, "b", "old0"), Age::Miss);
    }

    /// The ring is global, so the same key in two buckets is two entries — and one bucket's traffic
    /// ages the other's out, which is what makes a single eviction threshold comparable across them.
    #[test]
    fn keys_are_qualified_by_bucket() {
        let mut ring = fresh_ring(64, 2);
        touch(&mut ring, "a", "k");
        assert_eq!(age(&ring, "a", "k"), Age::Window(0));
        assert_eq!(age(&ring, "b", "k"), Age::Miss);
    }

    #[test]
    fn ages_order_coldest_greatest() {
        assert!(Age::Window(0) < Age::Window(1));
        assert!(Age::Window(u16::MAX) < Age::Miss);
    }

    /// The property the fixed seed exists for: a slice retired by one ring must mean the same thing
    /// to a different one, which is every restart and every failover.
    #[test]
    fn retired_slice_round_trips_into_a_separate_ring() {
        let mut ring = fresh_ring(4, 3);
        let retired = (0..4)
            .find_map(|i| touch(&mut ring, "b", &format!("k{i}")))
            .expect("slice retired");

        let mut reloaded = fresh_ring(4, 3);
        reloaded.install(vec![(retired.seq, retired.body)]);
        assert_eq!(age(&reloaded, "b", "k0"), Age::Window(1));
        assert_eq!(age(&reloaded, "b", "absent"), Age::Miss);
    }

    #[test]
    fn slice_from_another_geometry_is_ignored_not_misread() {
        let mut ring = fresh_ring(4, 3);
        let retired = (0..4)
            .find_map(|i| touch(&mut ring, "b", &format!("k{i}")))
            .expect("slice retired");

        let mut wider = fresh_ring(4096, 3);
        wider.install(vec![(retired.seq, retired.body)]);
        assert_eq!(age(&wider, "b", "k0"), Age::Miss);
    }

    #[test]
    fn restored_slices_do_not_reuse_a_sequence() {
        let mut ring = fresh_ring(2, 4);
        ring.install(vec![(41, Vec::new())]);
        touch(&mut ring, "b", "k0");
        let retired = touch(&mut ring, "b", "k1").expect("slice retired");
        assert_eq!(retired.seq, 42);
    }

    #[test]
    fn false_positive_rate_holds_at_the_fill_target() {
        let mut ring = fresh_ring(1000, 1);
        for i in 0..1000 {
            touch(&mut ring, "b", &format!("present{i}"));
        }
        let positives = (0..10_000)
            .filter(|i| age(&ring, "b", &format!("absent{i}")) != Age::Miss)
            .count();
        assert!(positives < 300, "{positives} false positives in 10k probes");
    }
}
