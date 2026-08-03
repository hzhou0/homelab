//! Cached-write admission gate driven by the pending-marker set .
//!
//! One global counter of pending markers, seeded by a full census once at startup and maintained
//! exactly since: a marker counts once when raised (create-only, so a last-writer-wins overwrite
//! never double-counts), once when the sweep's CAS actually removes it, and wholesale when
//! `DeleteBucket` drains a `<meta>` projection. The gate checks the counter against the configured
//! size and age thresholds and refuses a write the moment it is over — no waiting: the pacing that
//! a wait would provide is the SDK's retry backoff on `503 SlowDown` .

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use hypha_core::config::Backpressure;

#[derive(Debug)]
pub(crate) struct Pressure {
    enabled: bool,
    max_pending: usize,
    max_age_ms: u64,
    pending: AtomicUsize,
    oldest_age_ms: AtomicU64,
}

impl Pressure {
    pub(crate) fn new(cfg: &Backpressure) -> Self {
        Pressure {
            enabled: cfg.max_pending > 0 || cfg.max_age_ms > 0,
            max_pending: cfg.max_pending,
            max_age_ms: cfg.max_age_ms,
            pending: AtomicUsize::new(0),
            oldest_age_ms: AtomicU64::new(0),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    /// A cached write raised a marker for a key that had none.
    pub(crate) fn raised(&self) {
        if !self.enabled {
            return;
        }
        self.pending.fetch_add(1, Ordering::Relaxed);
    }

    /// The sweep's CAS actually removed a marker.
    pub(crate) fn cleared(&self) {
        if !self.enabled {
            return;
        }
        self.pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            })
            .ok();
    }

    /// `DeleteBucket` (or a namespace reset) drained a `<meta>` projection whose markers never ran
    /// through [`Self::cleared`].
    pub(crate) fn drained(&self, n: usize) {
        if !self.enabled || n == 0 {
            return;
        }
        self.pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(n))
            })
            .ok();
    }

    /// `Lifecycle::startup` seeds the counter and oldest age from a full census before the listener
    /// opens; the age is then re-published every pass because it has no atomic source — it is sampled
    /// where the sweep already enumerates the whole set .
    pub(crate) fn publish(&self, pending: usize, oldest_age_ms: u64) {
        if !self.enabled {
            return;
        }
        self.pending.store(pending, Ordering::Relaxed);
        self.oldest_age_ms.store(oldest_age_ms, Ordering::Relaxed);
    }

    /// Each drain pass re-publishes the oldest pending marker's age, from the set it enumerated.
    /// The count is deliberately left alone: it is tracked exactly by the raise/clear accounting,
    /// and re-storing the pass's pre-drain census here would resurrect markers the pass just cleared.
    pub(crate) fn publish_age(&self, oldest_age_ms: u64) {
        if !self.enabled {
            return;
        }
        self.oldest_age_ms.store(oldest_age_ms, Ordering::Relaxed);
    }

    fn pressured(&self) -> bool {
        (self.max_pending > 0 && self.pending.load(Ordering::Relaxed) > self.max_pending)
            || (self.max_age_ms > 0 && self.oldest_age_ms.load(Ordering::Relaxed) > self.max_age_ms)
    }

    /// Whether the op may proceed: `false` means the pending set is over a threshold, and the op
    /// returns `503 SlowDown` right now — nothing waits, so a gated write can never hold the shutdown
    /// drain.
    pub(crate) fn admit(&self) -> bool {
        if !self.enabled {
            return true;
        }
        if self.pressured() {
            crate::metrics::backpressure_throttled();
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(max_pending: usize, max_age_ms: u64) -> std::sync::Arc<Pressure> {
        std::sync::Arc::new(Pressure::new(&Backpressure {
            max_pending,
            max_age_ms,
        }))
    }

    #[test]
    fn a_disabled_gate_admits_everything() {
        let p = gate(0, 0);
        assert!(!p.enabled());
        assert!(p.admit());
        p.raised();
        p.raised();
        assert!(p.admit(), "a disabled gate admits under any count");
    }

    #[test]
    fn count_gate_admits_below_the_threshold() {
        let p = gate(2, 0);
        p.raised();
        p.raised();
        assert!(p.admit(), "at the threshold is not pressured");
    }

    #[test]
    fn a_count_over_the_threshold_is_refused_immediately() {
        let p = gate(1, 0);
        p.raised();
        p.raised();
        assert!(
            !p.admit(),
            "an over-threshold set must refuse the next write at once"
        );
    }

    #[test]
    fn an_age_over_the_threshold_is_refused_immediately() {
        let p = gate(0, 100);
        p.publish_age(1_000);
        assert!(!p.admit());
    }

    #[test]
    fn a_clear_that_crosses_the_threshold_reopens_the_gate() {
        let p = gate(1, 0);
        p.raised();
        p.raised();
        assert!(!p.admit());
        p.cleared();
        assert!(p.admit());
    }

    #[test]
    fn a_wholesale_drain_reopens_the_gate() {
        let p = gate(1, 0);
        p.raised();
        p.raised();
        p.raised();
        assert!(!p.admit());
        p.drained(2);
        assert!(p.admit());
    }
}
