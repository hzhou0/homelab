//! The GC actor (§8) — the active replica's standing duty to reclaim what the client path
//! deliberately left behind, and the sole owner of everything GC remembers.
//!
//! Several paths ack a client before their cleanup is done, on purpose: complete and abort leave an
//! upload's whole record range in place rather than pay a large multi-object delete on the client
//! path (§6, *Multipart upload state*), and every crash-window sequence in §6/§7 is ordered to leave
//! *debris* rather than a hybrid state — a twin beside a live body, a transition mark nobody came
//! back for. Nothing there is a durability obligation; every item is something a reader already
//! ignores, which is what makes the sweep's cadence a cost question rather than a correctness one.
//! Eviction is the other half, and the opposite: it takes bodies a client can still ask for, so it
//! answers to a byte target and to the gates in [`evict`].
//!
//! **One actor, one owner.** GC's state — the recency ring, the per-bucket cold yields, the pressure
//! rung — is read and written from this task and nowhere else. The rest of the crate holds a [`Gc`],
//! whose whole API is [`Gc::touch`]: a request states interest in a key and returns, with no lock to
//! contend on and no I/O to wait for. The filter bits, the rotation and the encode of the retired
//! slice all happen here, so a request never carries the rotation its own touch happened to trigger —
//! which, at the design fill target, means copying out a megabyte-scale filter.
//!
//! **The loop itself never awaits I/O.** Every pass is dispatched as its own task, and the actor only
//! picks the moment, refuses to start a second one, and answers the questions a running pass asks of
//! the state it owns. Doing the pass inline would stall the touch queue for the length of a
//! listing-heavy round of probes and shed exactly the traffic GC most wants to remember.
//!
//! Both modes sweep debris. Durable mode evicts nothing — it holds no bodies to evict — so it never
//! probes, and its passes are rung 0 alone.
//!
//! Only `Ready` buckets are swept. A bucket mid-restore has no cache namespace worth reading (§7),
//! and its recovery is additive, so debris there is either already gone with the volume or about to
//! be judged against a namespace that is still being rebuilt. Eviction narrows that further to
//! buckets this run **accounts for**: before a bucket's pending-set rebuild completes, the pending set
//! on disk is known incomplete, and a scavenger reading it as exhaustive is the one way an acked write
//! is lost. [`evict`]'s generation check independently refuses those bodies, so this is the second of
//! two locks on the same door rather than the only one.

mod debris;
mod evict;
mod ladder;
pub(crate) mod orphans;
mod ring;
mod scan;
mod store;
mod usage;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use hypha_core::config;
use hypha_core::{meta, Backend};

use crate::bucket::BucketCtl;
use crate::tier::Tiering;
use ladder::{Ladder, Setting};
use ring::{Age, RecencyRing};
use scan::{Candidate, ProbeYield, Yields};
use store::GcStore;
use usage::{Usage, UsageSource};

/// Touches waiting to be recorded. Generous, because shedding one is a small but real loss of
/// fidelity and the loop never awaits I/O — it can only fall behind by being starved of CPU, which a
/// deep queue rides out.
const TOUCH_QUEUE_DEPTH: usize = 1024;

/// Batched off the queue for the same reason the marker actor batches: a touch is on every op's
/// path, so one wake-up should take whatever a burst deposited.
const TOUCH_BATCH: usize = 256;

enum GcMsg {
    Touch(String),
    /// A running pass asking for its candidates' ages. The ring is the actor's alone, so a pass asks
    /// rather than holding a reference to it — a round trip through this queue, which the actor
    /// answers from memory without awaiting anything.
    Ages {
        qualified: Vec<String>,
        reply: oneshot::Sender<Vec<Age>>,
    },
    Finished(PassReport),
}

/// The rest of the crate's view of GC. Cheap to clone, holds no GC state, and offers nothing but a
/// touch — every decision GC makes is its own.
#[derive(Clone)]
pub(crate) struct Gc {
    tx: mpsc::Sender<GcMsg>,
}

/// The actor runs until the last [`Gc`] drops, i.e. with the service — the same handle-drop
/// shutdown [`crate::bucket`] and [`crate::markers`] use, with no separate liveness plumbing.
pub(crate) fn spawn(
    tier: Tiering,
    buckets: BucketCtl,
    backend: Backend,
    bucket: String,
    cfg: &config::Gc,
) -> Gc {
    let (tx, rx) = mpsc::channel(TOUCH_QUEUE_DEPTH);
    let ring = RecencyRing::new(&cfg.recency);
    let source = cfg.usage.as_ref().map(usage::connect);
    if source.is_none() && tier.cached {
        tracing::warn!(
            "no gc.usage source: the cache will fill, since GC cannot measure pressure and \
             will never evict"
        );
    }
    tokio::spawn(
        GcActor {
            ladder: Ladder::new(cfg, ring.depth()),
            yields: Yields::new(cfg.yield_floor),
            store: GcStore::new(backend, bucket),
            probe_pages: cfg.probe_pages.max(1),
            opportunistic_evictions: cfg.opportunistic_evictions,
            low_water: cfg.low_water,
            high_water: cfg.high_water,
            source,
            tier,
            buckets,
            ring,
            rx,
            replies: tx.clone(),
            pass: None,
        }
        .run(),
    );
    Gc { tx }
}

/// Where a key's plaintext lives, and therefore what a touch is *about* — the one thing a caller has
/// to tell GC, because it is the one thing GC cannot work out for itself.
///
/// A shadow body is keyed by the digest of K (§6), so K is not recoverable from it: a probe of the
/// shadow range holds a digest and nothing else, and a Bloom filter has no enumerable contents to
/// search backwards through. The ring therefore has to have been fed the shadow's own key, which only
/// the request that resolved through it knows to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Plaintext {
    /// At bare K: a live cache body, or where a single-part rehydrate will land it.
    AtKey,
    /// In K's shadow body (§6). K then holds a composite's tombstone, which is never an eviction
    /// candidate — so recording K would protect nothing that can be taken.
    InShadow,
}

impl Plaintext {
    /// Which artifact a client ETag implies. Composite ⇒ the shadow, since a composite's remote form
    /// is per-part age files and its cached plaintext has nowhere to be but the shadow.
    pub(crate) fn of(cetag: &str) -> Self {
        if meta::is_composite_etag(cetag) {
            Plaintext::InShadow
        } else {
            Plaintext::AtKey
        }
    }
}

impl Gc {
    /// Record interest in a key (§8). Every op that resolves or lands a single key calls this — the
    /// write path included, because a write is the strongest available statement of interest in a
    /// key and a read-only ring gets write-hot/read-cold keys exactly backwards.
    ///
    /// Infallible and non-blocking by construction: the ring is advisory, so there is no outcome a
    /// caller could act on, and a shed touch costs at most one key's ordering in one eviction cycle.
    ///
    /// Recording the *shadow's* key for a composite costs no extra fill — it replaces K's touch
    /// rather than adding to it — and it is exact even for a shadow that does not exist yet: the read
    /// that raises a rehydrate is precisely the interest the shadow it creates should inherit.
    pub(crate) fn touch(&self, bucket: &str, key: &str, holds: Plaintext) {
        let artifact = match holds {
            Plaintext::AtKey => ring::qualified(bucket, key),
            Plaintext::InShadow => ring::qualified(bucket, &meta::shadow_key(key)),
        };
        if self.tx.try_send(GcMsg::Touch(artifact)).is_err() {
            tracing::debug!("recency touch dropped; GC is behind");
        }
    }
}

struct GcActor {
    tier: Tiering,
    buckets: BucketCtl,
    store: GcStore,
    ring: RecencyRing,
    yields: Yields,
    ladder: Ladder,
    probe_pages: usize,
    opportunistic_evictions: usize,
    low_water: f64,
    high_water: f64,
    source: Option<Arc<dyn UsageSource>>,
    rx: mpsc::Receiver<GcMsg>,
    /// Handed to each pass so it can ask the actor for ages and report its own completion. Held as a
    /// sender rather than reconstructed per pass because it must *not* keep the actor alive: it is a
    /// clone of the same handle the rest of the crate holds, so the queue closes when the service
    /// drops regardless of how many passes have run.
    replies: mpsc::Sender<GcMsg>,
    /// At most one pass in flight: passes are idempotent, but overlapping them buys nothing and
    /// doubles the listing load exactly when the cache is already struggling.
    pass: Option<JoinHandle<()>>,
}

impl GcActor {
    async fn run(mut self) {
        self.restore().await;

        let mut cadence = self.ladder.current().interval;
        let mut ticks = interval(cadence);
        let mut batch = Vec::with_capacity(TOUCH_BATCH);
        loop {
            tokio::select! {
                n = self.rx.recv_many(&mut batch, TOUCH_BATCH) => {
                    if n == 0 {
                        // Every reclaim a pass makes is idempotent and will be re-found by the next
                        // process, so there is nothing to finish on the way out.
                        if let Some(pass) = self.pass.take() {
                            pass.abort();
                        }
                        return;
                    }
                    for msg in batch.drain(..) {
                        match msg {
                            GcMsg::Touch(qualified) => self.record(qualified),
                            GcMsg::Ages { qualified, reply } => {
                                let ages = qualified.iter().map(|q| self.ring.probe(q)).collect();
                                let _ = reply.send(ages);
                            }
                            GcMsg::Finished(report) => self.settle(report),
                        }
                    }
                }
                _ = ticks.tick() => self.dispatch(),
            }
            // The ladder moves the cadence, so the timer is rebuilt whenever a rung changed it —
            // never on the same tick that fired, so a shortened interval takes effect from now.
            let wanted = self.ladder.current().interval;
            if wanted != cadence {
                cadence = wanted;
                ticks = interval(cadence);
            }
        }
    }

    /// Read the persisted slices back, before the first touch is recorded — so the slices installed
    /// are unambiguously older than anything this run goes on to touch, whatever else has started.
    /// A ring that cannot be read starts cold, which it is designed to survive.
    async fn restore(&mut self) {
        if let Err(e) = self.store.ensure().await {
            tracing::warn!(error = %e, "GC bucket unavailable; recency will not survive this run");
            return;
        }
        match self.store.load(self.ring.depth()).await {
            Ok(slices) if slices.is_empty() => {}
            Ok(slices) => {
                tracing::info!(slices = slices.len(), "recency ring restored");
                self.ring.install(slices);
            }
            Err(e) => tracing::warn!(error = %e, "recency ring not restored; starting cold"),
        }
    }

    fn record(&mut self, qualified: String) {
        let Some(retired) = self.ring.record(qualified) else {
            return;
        };
        // Off the loop, for the same reason a pass is: the actor must stay ready to record the
        // touches arriving behind this one. The slice is already in the ring, so a persist that
        // fails costs only the *next* process one colder cycle.
        let (store, depth) = (self.store.clone(), self.ring.depth());
        tokio::spawn(async move {
            if let Err(e) = store.persist(&retired, depth).await {
                tracing::warn!(seq = retired.seq, error = %e, "recency slice not persisted");
            }
        });
    }

    fn dispatch(&mut self) {
        if self.pass.as_ref().is_some_and(|p| !p.is_finished()) {
            tracing::debug!("scavenger pass still running; skipping this tick");
            return;
        }
        let swept = self.buckets.ready();
        self.yields.retain(&swept);
        // Durable mode holds no bodies to evict, so it never probes. Cached mode narrows to the
        // buckets whose pending set this run accounts for.
        let evictable: Vec<String> = if self.tier.cached {
            let accounted = self.buckets.accounted();
            swept
                .iter()
                .filter(|b| accounted.contains(b))
                .cloned()
                .collect()
        } else {
            Vec::new()
        };

        // One probe per evictable bucket is the pass's round: enough to keep the yields learning, and
        // it is the *interval* the ladder shortens when this proves too slow, not the round.
        let probes = self.yields.sample(&evictable, evictable.len());
        let plan = Plan {
            setting: self.ladder.current(),
            swept,
            probes,
            probe_pages: self.probe_pages,
            opportunistic_evictions: self.opportunistic_evictions,
            low_water: self.low_water,
            high_water: self.high_water,
        };
        let pass = Pass {
            tier: self.tier.clone(),
            source: self.source.clone(),
            actor: self.replies.clone(),
            plan,
        };
        self.pass = Some(tokio::spawn(pass.run()));
    }

    /// Fold a finished pass back into the state it was judged against — the yields it taught, and the
    /// one rung the ladder may move on the evidence of exactly one pass (§8).
    fn settle(&mut self, report: PassReport) {
        for observed in &report.yields {
            self.yields.observe(observed);
        }
        let Some(usage) = report.usage else {
            // No usage source, or the sample failed: the ladder has no evidence either way, and
            // moving it on a guess is what the one-rung-per-pass cap exists to prevent.
            return;
        };

        if usage.after.ratio() < self.low_water {
            self.ladder.reset();
            return;
        }
        if report.target > 0 {
            if report.reclaimed >= report.target {
                self.ladder.relax();
            } else {
                self.ladder.escalate();
            }
        }
        // §8's one exception: a cache filling faster than a pass completes never reaches rung 1 by the
        // normal route, because the evidence never arrives. The two rungs that cost only work jump to
        // their bounds; the threshold never does.
        if usage.after.used > usage.before.used {
            self.ladder.escalate_reversible();
        }
        if self.ladder.clamped() && report.reclaimed < report.target {
            tracing::warn!(
                used = usage.after.used,
                capacity = usage.after.capacity,
                target = report.target,
                reclaimed = report.reclaimed,
                "cache is undersized for its working set: GC is at the top of its ladder with the \
                 byte target still unmet, so it is now evicting the working set itself"
            );
        }
        tracing::debug!(
            rung = ?self.ladder.rung(),
            used = usage.after.used,
            capacity = usage.after.capacity,
            target = report.target,
            reclaimed = report.reclaimed,
            "scavenger pass settled"
        );
    }
}

/// Missed ticks are *delayed*, not caught up on: a pass that overran its interval has already done
/// the work the skipped ticks would have asked for, and firing them back to back would only stack
/// listings on a backend that is evidently already slow.
fn interval(period: Duration) -> tokio::time::Interval {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticks
}

/// Everything a pass needs from the state it does not own, decided before it is dispatched.
struct Plan {
    setting: Setting,
    /// `Ready` buckets — where debris is swept.
    swept: Vec<String>,
    /// One bucket per probe, already drawn against the cold yields. Empty in durable mode.
    probes: Vec<String>,
    probe_pages: usize,
    opportunistic_evictions: usize,
    low_water: f64,
    high_water: f64,
}

struct Pass {
    tier: Tiering,
    source: Option<Arc<dyn UsageSource>>,
    actor: mpsc::Sender<GcMsg>,
    plan: Plan,
}

/// What the pass observed, on its way back to the actor.
struct PassReport {
    reclaimed: u64,
    target: u64,
    yields: Vec<ProbeYield>,
    /// `None` when there is no usage source or its sample failed — the ladder then stays put.
    usage: Option<UsageBracket>,
}

/// Usage either side of the pass. The *after* sample is what the water marks are read against, and
/// the pair is what tells the ladder whether the cache filled faster than the pass drained it.
struct UsageBracket {
    before: Usage,
    after: Usage,
}

impl Pass {
    async fn run(self) {
        let report = self.execute().await;
        // A closed queue is shutdown; the next process re-derives everything this pass learned.
        let _ = self.actor.send(GcMsg::Finished(report)).await;
    }

    async fn execute(&self) -> PassReport {
        let before = self.sample().await;
        let target = before
            .filter(|u| u.ratio() >= self.plan.high_water)
            .map(|u| u.excess_over(self.plan.low_water))
            .unwrap_or(0);

        // Rung 0, always first: debris and dead bytes are reclaim at zero rehydration risk — nobody
        // was ever going to read an abandoned upload's parts — so a target met from them alone evicts
        // nothing at all.
        let mut reclaimed = self.sweep_debris().await;
        let mut yields = Vec::new();
        if target > 0 {
            self.compact().await;
            // Probing is itself listing-heavy, so an unpressured pass doesn't do it: with no target
            // there is nothing a candidate could be selected *for*.
            if reclaimed < target {
                let (candidates, observed) = self.probe().await;
                yields = observed;
                reclaimed += self.evict(candidates, target - reclaimed).await;
            }
        }

        let after = self.sample().await;
        PassReport {
            reclaimed,
            target,
            yields,
            usage: before
                .zip(after)
                .map(|(before, after)| UsageBracket { before, after }),
        }
    }

    async fn sample(&self) -> Option<Usage> {
        let source = self.source.as_ref()?;
        match source.sample().await {
            Ok(usage) => Some(usage),
            Err(e) => {
                tracing::warn!(error = %e, "cache usage unavailable; GC will not evict this pass");
                None
            }
        }
    }

    async fn compact(&self) {
        if let Some(source) = self.source.as_ref() {
            if let Err(e) = source.compact().await {
                tracing::warn!(error = %e, "dead-byte compaction failed");
            }
        }
    }

    async fn sweep_debris(&self) -> u64 {
        let mut reclaimed = debris::Reclaimed::default();
        for chunk in self.plan.swept.chunks(self.plan.setting.concurrency.max(1)) {
            let sweeps = chunk
                .iter()
                .map(|bucket| debris::sweep_mpu_ranges(&self.tier, bucket));
            match futures::future::try_join_all(sweeps).await {
                Ok(swept) => swept.into_iter().for_each(|one| reclaimed += one),
                // A sweep reclaims nothing it cannot see; the next pass starts over from a fresh
                // listing, so a failure costs one interval of unreclaimed bytes.
                Err(e) => {
                    tracing::warn!(error = %e, "debris sweep failed; retrying next interval");
                    break;
                }
            }
        }
        if reclaimed.uploads > 0 {
            tracing::info!(
                uploads = reclaimed.uploads,
                bytes = reclaimed.bytes,
                "reclaimed mpu record ranges"
            );
        }
        reclaimed.bytes
    }

    /// Run the pass's probes and return the candidates in the order eviction should consider them:
    /// coldest age first, LastModified ascending within an age (§8).
    ///
    /// Each probe covers **both** places a bucket keeps plaintext — `<data>`'s client bodies and
    /// `<meta>`'s shadow bodies — rather than making shadows a separately weighted namespace. One
    /// bucket's shadows are only the composites something has read back, so the range is usually small
    /// or empty and the extra listing is close to free; giving it its own yield would mean two
    /// feedback loops to reason about for a range that mostly does not exist.
    async fn probe(&self) -> (Vec<(Candidate, Age)>, Vec<ProbeYield>) {
        let mut candidates = Vec::new();
        let mut yields = Vec::new();
        for chunk in self
            .plan
            .probes
            .chunks(self.plan.setting.concurrency.max(1))
        {
            let probes = chunk.iter().map(|bucket| async move {
                let pages = self.plan.probe_pages;
                (
                    scan::probe_bodies(&self.tier, bucket, pages).await,
                    scan::probe_shadows(&self.tier, bucket, pages).await,
                )
            });
            // One bucket's listing failing says nothing about the others', so the round continues on
            // whatever it did find rather than abandoning the pass.
            for (bucket, found) in chunk.iter().zip(futures::future::join_all(probes).await) {
                let (bodies, shadows) = found;
                for found in [bodies, shadows] {
                    match found {
                        Ok(found) => {
                            yields.push(found.yielded(bucket));
                            candidates.extend(found.candidates);
                        }
                        Err(e) => tracing::debug!(bucket, error = %e, "probe failed"),
                    }
                }
            }
        }

        let Some(ages) = self.ages(&candidates).await else {
            return (Vec::new(), yields);
        };
        let mut aged: Vec<(Candidate, Age)> = candidates.into_iter().zip(ages).collect();
        aged.sort_by(|(left, left_age), (right, right_age)| {
            right_age
                .cmp(left_age)
                .then(left.mtime_ms.cmp(&right.mtime_ms))
        });
        (aged, yields)
    }

    async fn ages(&self, candidates: &[Candidate]) -> Option<Vec<Age>> {
        if candidates.is_empty() {
            return Some(Vec::new());
        }
        let qualified = candidates.iter().map(Candidate::qualified).collect();
        let (reply, answer) = oneshot::channel();
        self.actor
            .send(GcMsg::Ages { qualified, reply })
            .await
            .ok()?;
        // No answer means the actor is gone. Declining to evict is the only safe reading: without
        // ages every candidate would look like a miss, which is *evict everything*.
        answer.await.ok()
    }

    /// Evict coldest-first until `target` bytes are reclaimed, then keep taking **misses** up to the
    /// opportunistic bound (§8): over-evicting an affirmatively cold key is nearly free in rehydration
    /// risk, yet each eviction still costs a remote HEAD, a twin write, and a CAS, hence the bound.
    async fn evict(&self, candidates: Vec<(Candidate, Age)>, target: u64) -> u64 {
        let (tier, threshold) = (&self.tier, self.plan.setting.threshold);
        let eligible: Vec<(Candidate, Age)> = candidates
            .into_iter()
            .filter(|(_, age)| *age >= threshold)
            .collect();

        let mut reclaimed = 0;
        let mut opportunistic = self.plan.opportunistic_evictions;
        // Chunked rather than a streaming `buffer_unordered`: stopping mid-stream would drop
        // half-applied evictions, and the twin is written before the tombstone — abandoning between
        // the two leaves a twin beside a live body for a later sweep to find. Chunking also means the
        // stop condition reads *actual* reclaimed bytes rather than the candidates' advertised sizes,
        // so a run of declined gates doesn't silently end the pass short of its target.
        for chunk in eligible.chunks(self.plan.setting.concurrency.max(1)) {
            let batch: Vec<&Candidate> = chunk
                .iter()
                .filter_map(|(candidate, age)| {
                    if reclaimed < target {
                        return Some(candidate);
                    }
                    // Target met. Candidates are coldest-first, so the misses still ahead are the only
                    // ones cheap enough to keep taking, and only up to the bound.
                    (*age == Age::Miss && opportunistic > 0).then(|| {
                        opportunistic -= 1;
                        candidate
                    })
                })
                .collect();
            if batch.is_empty() {
                break;
            }
            let attempts = batch.into_iter().map(|c| evict::evict(tier, c));
            for outcome in futures::future::join_all(attempts).await {
                match outcome {
                    Ok(bytes) => reclaimed += bytes,
                    Err(e) => tracing::debug!(error = %e, "eviction failed"),
                }
            }
        }
        if reclaimed > 0 {
            tracing::info!(bytes = reclaimed, threshold = ?threshold, "evicted cold bodies");
        }
        reclaimed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole of what a caller has to get right, and it follows from the ETag alone.
    #[test]
    fn a_composite_etag_puts_the_plaintext_in_the_shadow() {
        assert_eq!(Plaintext::of(&"ab".repeat(16)), Plaintext::AtKey);
        assert_eq!(
            Plaintext::of(&format!("{}-7", "ab".repeat(16))),
            Plaintext::InShadow
        );
    }
}
