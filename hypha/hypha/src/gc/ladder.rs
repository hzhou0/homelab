//! §8's escalation ladder — the control law that decides how hard GC pushes back on pressure.
//!
//! Pressure has four answers and the order they are spent in *is* the design. How **often** the
//! scavenger passes, how **wide** each pass runs, and only then how **warm** a key it will take.
//! Rungs 1 and 2 spend nothing but work — round trips, bandwidth, CPU the deployment already has,
//! all of it given back the moment pressure drops. Rung 3 spends the quality of the decision: a
//! warmer key is likelier to be wanted back, and that bill is paid later, by a client, as
//! rehydration latency and a re-upload. So exhaust what costs work before spending what costs the
//! client. (Rung 0 — debris and dead-byte compaction — needs no state here: every pass does it.)
//!
//! **One position, not three knobs.** The rungs are laid out once as a single ordered list of
//! settings, and the ladder is an index into it. Climbing is `+1`, and descending is `-1` — which is
//! LIFO *by construction*, so the expensive rung can never outlive the evidence that justified it
//! without the descent having to be reasoned about separately.
//!
//! Movement is capped at one step per completed pass, a pass being one round of probes across the
//! sampled buckets — the unit of evidence a sampling scan can offer. That cap is what damps the
//! control: nothing moves faster than the scan can observe what the previous setting yielded.
//! Deliberately not a proportional map from pressure onto a rung, which would pick an aggressive
//! threshold on a spike even when the keyspace is full of misses that would have met the target on
//! their own.

use std::time::Duration;

use hypha_core::config;

use super::ring::Age;

/// A configured cadence, floored at one tick: a zero period is what `min_interval_ms = 0` means to an
/// operator ("as fast as it can go"), and it is also what `tokio::time::interval` panics on.
fn interval(ms: u64) -> Duration {
    Duration::from_millis(ms.max(1))
}

/// What one rung leaves the scavenger configured to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Setting {
    pub(super) interval: Duration,
    pub(super) concurrency: usize,
    /// Candidates at or above this age may be evicted (ages order coldest-greatest). [`Age::Miss`] —
    /// the keys the ring affirmatively vouches nothing has touched — is where it starts.
    pub(super) threshold: Age,
}

/// Which of §8's rungs the current setting represents, for the §10 metric an operator reads to tell
/// whether GC is coping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Rung {
    /// Base: debris and compaction only, nothing escalated.
    Unpressured,
    Interval,
    Concurrency,
    Threshold,
}

pub(super) struct Ladder {
    /// Index 0 is the unpressured base; the interval steps come first, then concurrency, then the
    /// threshold moving one bucket younger at a time.
    rungs: Vec<Setting>,
    /// Where the threshold steps begin — the boundary the in-flight jump stops short of, since the
    /// threshold's cost is not the deployment's to take back.
    reversible_end: usize,
    interval_end: usize,
    at: usize,
}

impl Ladder {
    pub(super) fn new(cfg: &config::Gc, depth: usize) -> Self {
        let base = Setting {
            interval: interval(cfg.interval_ms),
            concurrency: cfg.concurrency.max(1),
            threshold: Age::Miss,
        };
        let mut rungs = vec![base];

        // Halving rather than a fixed step: the useful range spans orders of magnitude (a 5-minute
        // base to a 1-second floor), so a linear walk would spend most of its rungs where the change
        // no longer matters.
        let floor = interval(cfg.min_interval_ms).min(base.interval);
        let mut interval = base.interval;
        while interval > floor {
            interval = (interval / 2).max(floor);
            rungs.push(Setting { interval, ..base });
        }
        let interval_end = rungs.len();

        let ceiling = cfg.max_concurrency.max(base.concurrency);
        let mut concurrency = base.concurrency;
        let widest = *rungs.last().expect("the base rung is always present");
        while concurrency < ceiling {
            concurrency = (concurrency * 2).min(ceiling);
            rungs.push(Setting {
                concurrency,
                ..widest
            });
        }
        let reversible_end = rungs.len();

        // Down to the current window inclusive: at the top the ring contributes nothing and ordering
        // is LastModified alone, which is the honest description of a cache too small for its working
        // set. Refusing to evict there is the worse failure, so the ladder keeps going.
        let widest = *rungs.last().expect("the base rung is always present");
        for window in (0..=depth as u16).rev() {
            rungs.push(Setting {
                threshold: Age::Window(window),
                ..widest
            });
        }

        Ladder {
            rungs,
            reversible_end,
            interval_end,
            at: 0,
        }
    }

    pub(super) fn current(&self) -> Setting {
        self.rungs[self.at]
    }

    pub(super) fn rung(&self) -> Rung {
        match self.at {
            0 => Rung::Unpressured,
            at if at < self.interval_end => Rung::Interval,
            at if at < self.reversible_end => Rung::Concurrency,
            _ => Rung::Threshold,
        }
    }

    /// A pass completed with its target unmet.
    pub(super) fn escalate(&mut self) {
        self.at = (self.at + 1).min(self.rungs.len() - 1);
    }

    /// A pass completed with its target met: give back the most recently taken rung, so the threshold
    /// moves *older* before any cheap rung is surrendered and tracks sustained pressure rather than
    /// the worst moment the process ever saw. Without this the mechanism is a ratchet — one burst
    /// leaves it evicting warm keys forever, which is protect-nothing, the mirror of the
    /// protect-everything failure the ring's fill-driven rotation exists to prevent.
    pub(super) fn relax(&mut self) {
        self.at = self.at.saturating_sub(1);
    }

    /// Usage below the low-water mark: with no pressure at all, nothing justifies evicting a key the
    /// ring still vouches for.
    pub(super) fn reset(&mut self) {
        self.at = 0;
    }

    /// Usage climbing *while a pass is in flight*. A cache filling faster than a pass completes never
    /// reaches rung 1 by the normal route, because the evidence never arrives — so the two rungs that
    /// cost only work jump straight to their bounds. Safe precisely because they are given back the
    /// moment a pass meets its target; the threshold never jumps.
    pub(super) fn escalate_reversible(&mut self) {
        self.at = self.at.max(self.reversible_end - 1);
    }

    /// Interval at its floor, concurrency at its ceiling, threshold at its youngest bucket. The cache
    /// is undersized for its working set and the choice is thrashing or running out of space — the one
    /// GC condition an operator must act on, hence §10's warn.
    pub(super) fn clamped(&self) -> bool {
        self.at == self.rungs.len() - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> config::Gc {
        config::Gc {
            interval_ms: 8_000,
            min_interval_ms: 1_000,
            concurrency: 2,
            max_concurrency: 8,
            ..config::Gc::default()
        }
    }

    fn ladder() -> Ladder {
        Ladder::new(&cfg(), 2)
    }

    /// The whole ordering claim of §8 in one assertion: every cheap rung is spent before the first
    /// expensive one, and the threshold only ever moves once both bounds are reached.
    #[test]
    fn escalation_spends_work_before_it_spends_the_client() {
        let mut ladder = ladder();
        let mut seen = vec![(ladder.rung(), ladder.current())];
        while !ladder.clamped() {
            ladder.escalate();
            seen.push((ladder.rung(), ladder.current()));
        }

        let rungs: Vec<Rung> = seen.iter().map(|(rung, _)| *rung).collect();
        assert_eq!(
            rungs,
            vec![
                Rung::Unpressured,
                Rung::Interval,
                Rung::Interval,
                Rung::Interval,
                Rung::Concurrency,
                Rung::Concurrency,
                Rung::Threshold,
                Rung::Threshold,
                Rung::Threshold,
            ]
        );
        let settings: Vec<Setting> = seen.iter().map(|(_, s)| *s).collect();
        assert!(
            settings
                .iter()
                .take_while(|s| s.threshold == Age::Miss)
                .count()
                == 6,
            "the threshold must hold at miss until both cheap bounds are reached"
        );
        assert_eq!(
            settings.last().copied(),
            Some(Setting {
                interval: Duration::from_millis(1_000),
                concurrency: 8,
                threshold: Age::Window(0),
            })
        );
    }

    /// The floor an operator writes as "no delay at all" — and the one value `tokio::time::interval`
    /// refuses.
    #[test]
    fn a_zero_floor_still_yields_a_tickable_interval() {
        let ladder = Ladder::new(
            &config::Gc {
                interval_ms: 4,
                min_interval_ms: 0,
                ..cfg()
            },
            1,
        );
        let mut ladder = ladder;
        while ladder.rung() == Rung::Unpressured || ladder.rung() == Rung::Interval {
            assert!(!ladder.current().interval.is_zero());
            ladder.escalate();
        }
    }

    #[test]
    fn intervals_halve_to_the_floor_and_stop_there() {
        let mut ladder = ladder();
        let mut intervals = vec![ladder.current().interval];
        while ladder.rung() == Rung::Unpressured || ladder.rung() == Rung::Interval {
            ladder.escalate();
            intervals.push(ladder.current().interval);
        }
        assert_eq!(
            intervals,
            vec![
                Duration::from_millis(8_000),
                Duration::from_millis(4_000),
                Duration::from_millis(2_000),
                Duration::from_millis(1_000),
                Duration::from_millis(1_000),
            ]
        );
    }

    #[test]
    fn relax_surrenders_the_most_recent_rung_first() {
        let mut ladder = ladder();
        while !ladder.clamped() {
            ladder.escalate();
        }
        assert_eq!(ladder.current().threshold, Age::Window(0));

        ladder.relax();
        assert_eq!(
            ladder.current().threshold,
            Age::Window(1),
            "the threshold moves older before a cheap rung is given back"
        );
        assert_eq!(ladder.current().concurrency, 8);
        assert_eq!(ladder.current().interval, Duration::from_millis(1_000));
    }

    #[test]
    fn reset_returns_to_the_configured_base() {
        let mut ladder = ladder();
        for _ in 0..20 {
            ladder.escalate();
        }
        ladder.reset();
        assert_eq!(ladder.rung(), Rung::Unpressured);
        assert_eq!(
            ladder.current(),
            Setting {
                interval: Duration::from_millis(8_000),
                concurrency: 2,
                threshold: Age::Miss,
            }
        );
    }

    #[test]
    fn the_in_flight_jump_stops_short_of_the_threshold() {
        let mut ladder = ladder();
        ladder.escalate_reversible();
        assert_eq!(ladder.rung(), Rung::Concurrency);
        assert_eq!(
            ladder.current(),
            Setting {
                interval: Duration::from_millis(1_000),
                concurrency: 8,
                threshold: Age::Miss,
            },
            "both cheap rungs at their bounds, the threshold untouched"
        );
        assert!(!ladder.clamped());
    }

    /// A jump must never *undo* a threshold the ladder has already paid for.
    #[test]
    fn the_in_flight_jump_never_walks_the_threshold_back() {
        let mut ladder = ladder();
        while !ladder.clamped() {
            ladder.escalate();
        }
        ladder.escalate_reversible();
        assert!(ladder.clamped());
    }

    /// A deployment that pins the base to the bounds has no cheap rungs to spend — the ladder must
    /// still be a valid one-step-at-a-time climb rather than an empty or panicking list.
    #[test]
    fn a_ladder_with_no_reversible_rungs_still_climbs() {
        let mut ladder = Ladder::new(
            &config::Gc {
                interval_ms: 1_000,
                min_interval_ms: 1_000,
                concurrency: 4,
                max_concurrency: 4,
                ..config::Gc::default()
            },
            1,
        );
        assert_eq!(ladder.rung(), Rung::Unpressured);
        ladder.escalate();
        assert_eq!(ladder.rung(), Rung::Threshold);
        assert_eq!(ladder.current().threshold, Age::Window(1));
        ladder.escalate_reversible();
        assert_eq!(
            ladder.current().threshold,
            Age::Window(1),
            "a jump with nothing cheap left to take must not disturb the threshold"
        );
    }
}
