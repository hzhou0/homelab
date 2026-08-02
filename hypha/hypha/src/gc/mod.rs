//! GC owns recency state, debris sweeps, and pressure-driven eviction.
//!
//! Touches are queued so request paths never rotate or persist Bloom filters. Passes run in separate
//! tasks so listing I/O cannot stall that queue. Debris is always safe to reclaim; eviction is
//! limited to ready buckets whose pending set is accounted and independently verifies the remote
//! generation before removing plaintext.

mod debris;
mod evict;
mod ladder;
pub(crate) mod orphans;
mod ring;
mod scan;
mod store;
mod usage;

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

use hypha_core::config;
use hypha_core::{meta, Backend};

use crate::bucket::BucketCtl;
use crate::tier::Tiering;
pub(crate) use debris::Swept;
use ladder::{Ladder, Setting};
use ring::{Age, RecencyRing};
use scan::{Candidate, Prefixes, ProbeYield, Yields};
use store::GcStore;
use usage::{Usage, UsageSource};

/// Touches waiting to be recorded. Generous, because shedding one is a small but real loss of
/// fidelity and the loop never awaits I/O — it can only fall behind by being starved of CPU, which a
/// deep queue rides out.
const TOUCH_QUEUE_DEPTH: usize = 1024;

/// Batched off the queue for the same reason the marker actor batches: a touch is on every op's
/// path, so one wake-up should take whatever a burst deposited.
const TOUCH_BATCH: usize = 256;

#[derive(Clone)]
pub(crate) struct Gc {
    tx: mpsc::Sender<String>,
}

/// The actor runs until the last [`Gc`] drops, i.e. with the service: every sender is a handle held
/// outside GC, so closure is a signal the actor can observe and it needs no shutdown token of its own.
/// The returned task is what the drain joins — see [`GcActor::run`] for what it finishes on the way
/// out.
pub(crate) fn spawn(
    tier: Tiering,
    buckets: BucketCtl,
    backend: Backend,
    bucket: String,
    cfg: &config::Gc,
) -> (Gc, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(TOUCH_QUEUE_DEPTH);
    let ring = RecencyRing::new(&cfg.recency);
    let depth = ring.depth();
    let source = cfg.usage.as_ref().map(usage::connect);
    if source.is_none() && tier.cached {
        tracing::warn!(
            "no gc.usage source: the cache will fill, since GC cannot measure pressure and \
             will never evict"
        );
    }
    let task = tokio::spawn(
        GcActor {
            ladder: Ladder::new(cfg, depth),
            yields: Yields::new(cfg.yield_floor),
            prefixes: Prefixes::new(),
            store: GcStore::new(backend, bucket),
            depth,
            probe_pages: cfg.probe_pages.max(1),
            opportunistic_evictions: cfg.opportunistic_evictions,
            low_water: cfg.low_water,
            high_water: cfg.high_water,
            source,
            tier,
            buckets,
            ring: Arc::new(Mutex::new(ring)),
            rx,
            persists: JoinSet::new(),
        }
        .run(),
    );
    (Gc { tx }, task)
}

/// Where a key's plaintext lives, and therefore what a touch is *about* — the one thing a caller has
/// to tell GC, because it is the one thing GC cannot work out for itself.
///
/// A shadow body is keyed by the digest of K, so K is not recoverable from it: a probe of the
/// shadow range holds a digest and nothing else, and a Bloom filter has no enumerable contents to
/// search backwards through. The ring therefore has to have been fed the shadow's own key, which only
/// the request that resolved through it knows to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Plaintext {
    /// At bare K: a live cache body, or where a single-part rehydrate will land it.
    AtKey,
    /// In K's shadow body. K then holds a composite's tombstone, which is never an eviction
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
    /// Record interest in a key. Every op that resolves or lands a single key calls this — the
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
        if self.tx.try_send(artifact).is_err() {
            tracing::debug!("recency touch dropped; GC is behind");
        }
    }
}

struct GcActor {
    tier: Tiering,
    buckets: BucketCtl,
    store: GcStore,
    /// Shared with the running pass, which probes it directly. The critical sections are a handful of
    /// filter lookups with no I/O and no await inside them, so contention with the touch loop is
    /// bounded by the ring's own arithmetic.
    ring: Arc<Mutex<RecencyRing>>,
    /// Fixed at construction, so the persist path doesn't take the ring's lock to learn how many
    /// slices to keep.
    depth: usize,
    yields: Yields,
    /// Where within a bucket its probes land — learned from the same probes that teach the yields.
    prefixes: Prefixes,
    ladder: Ladder,
    probe_pages: usize,
    opportunistic_evictions: usize,
    low_water: f64,
    high_water: f64,
    source: Option<Arc<dyn UsageSource>>,
    rx: mpsc::Receiver<String>,
    /// Rotated slices on their way to GC's bucket. Tracked rather than detached so the drain can wait
    /// for them: a slice is only worth writing if the *next* process gets to read it.
    persists: JoinSet<()>,
}

impl GcActor {
    /// The loop, and what it owes on the way out. Both are exits worth being precise about:
    ///
    /// A touch queue with no senders left means the service is gone, so the pass in flight is *awaited*
    /// rather than abandoned — its evictions each hold a key's write lock across a twin write and a CAS,
    /// and one cut between the two leaves a twin beside a live body for a later sweep to find. The
    /// retired slices still being persisted are joined for the same reason, less severely: a slice lost
    /// mid-PUT costs the next process a colder ring.
    async fn run(mut self) {
        self.restore().await;

        let mut cadence = self.ladder.current().interval;
        let mut ticks = interval(cadence);
        let mut batch = Vec::with_capacity(TOUCH_BATCH);
        // At most one pass in flight: passes are idempotent, but overlapping them buys nothing and
        // doubles the listing load exactly when the cache is already struggling.
        let mut pass: Option<JoinHandle<PassReport>> = None;
        loop {
            tokio::select! {
                n = self.rx.recv_many(&mut batch, TOUCH_BATCH) => {
                    if n == 0 {
                        self.finish(pass).await;
                        return;
                    }
                    for qualified in batch.drain(..) {
                        self.record(qualified);
                    }
                }
                report = finished(&mut pass) => {
                    if let Some(report) = report {
                        self.settle(report);
                    }
                }
                // Reaped as they land so the set does not grow across a long run; the persist itself
                // logs its own failure.
                Some(_) = self.persists.join_next(), if !self.persists.is_empty() => {}
                _ = ticks.tick() => match pass {
                    // A handle still here is a pass whose report has not been folded in yet;
                    // replacing it would discard that report.
                    Some(_) => tracing::debug!("scavenger pass still running; skipping this tick"),
                    None => pass = Some(self.dispatch()),
                },
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
        match self.store.load(self.depth).await {
            Ok(slices) if slices.is_empty() => {}
            Ok(slices) => {
                tracing::info!(slices = slices.len(), "recency ring restored");
                self.ring().install(slices);
            }
            Err(e) => tracing::warn!(error = %e, "recency ring not restored; starting cold"),
        }
    }

    /// Panic-free by construction: nothing under this lock can panic, so a poisoned lock is
    /// unreachable and treating it as fatal is honest about that.
    fn ring(&self) -> std::sync::MutexGuard<'_, RecencyRing> {
        self.ring.lock().expect("recency ring lock poisoned")
    }

    fn record(&mut self, qualified: String) {
        let Some(retired) = self.ring().record(qualified) else {
            return;
        };
        // Off the loop, for the same reason a pass is: the actor must stay ready to record the
        // touches arriving behind this one. The slice is already in the ring, so a persist that
        // fails costs only the *next* process one colder cycle.
        let (store, depth) = (self.store.clone(), self.depth);
        self.persists.spawn(async move {
            if let Err(e) = store.persist(&retired, depth).await {
                tracing::warn!(seq = retired.seq, error = %e, "recency slice not persisted");
            }
        });
    }

    /// Let the work already in hand land. The report is still settled even though nothing outlives the
    /// process to read the ladder or the yields: settling is what logs the bytes the pass reclaimed,
    /// and a pass that did its work unrecorded is indistinguishable from one that was cut off.
    async fn finish(&mut self, pass: Option<JoinHandle<PassReport>>) {
        if let Some(pass) = pass {
            match pass.await {
                Ok(report) => self.settle(report),
                Err(e) => tracing::warn!(error = %e, "scavenger pass did not finish"),
            }
        }
        while let Some(persisted) = self.persists.join_next().await {
            if let Err(e) = persisted {
                tracing::warn!(error = %e, "recency slice persist did not finish");
            }
        }
    }

    fn dispatch(&mut self) -> JoinHandle<PassReport> {
        let swept = self.buckets.ready();
        self.yields.retain(&swept);
        self.prefixes.retain(&swept);
        // Every `Ready` bucket is probed, in both modes: the walk is what finds this pass's debris,
        // and debris accrues wherever the client path acked before its cleanup was done. What mode
        // and accounting decide is only whether a *body* found there may be taken — durable mode
        // holds none, and a bucket whose pending set this run has not rebuilt is one whose cache
        // cannot yet be read as exhaustive.
        let evictable: HashSet<Arc<str>> = if self.tier.cached {
            let accounted = self.buckets.accounted();
            swept
                .iter()
                .filter(|b| accounted.contains(b))
                .map(|b| Arc::from(b.as_str()))
                .collect()
        } else {
            HashSet::new()
        };

        // One probe per bucket is the pass's round: enough to keep the yields learning, and it is the
        // *interval* the ladder shortens when this proves too slow, not the round.
        let probes: Vec<Probe> = self
            .yields
            .sample(&swept, swept.len())
            .into_iter()
            .map(|bucket| Probe {
                position: self.prefixes.position(&bucket),
                bucket: Arc::from(bucket),
            })
            .collect();
        let plan = Plan {
            setting: self.ladder.current(),
            swept,
            evictable,
            probes,
            probe_pages: self.probe_pages,
            opportunistic_evictions: self.opportunistic_evictions,
            low_water: self.low_water,
            high_water: self.high_water,
        };
        let pass = Pass {
            tier: self.tier.clone(),
            source: self.source.clone(),
            ring: self.ring.clone(),
            plan,
        };
        tokio::spawn(pass.execute())
    }

    /// Fold a finished pass back into the state it was judged against — the yields it taught, and the
    /// one rung the ladder may move on the evidence of exactly one pass.
    fn settle(&mut self, report: PassReport) {
        let elapsed = report.elapsed;
        self.fold(report);
        // After the fold, so the gauge names the rung now engaged rather than the one that has just
        // been judged — a pass can be minutes long, and the whole point of the metric is what GC is
        // about to do next.
        crate::metrics::gc_pass(self.ladder.rung().number(), elapsed);
    }

    fn fold(&mut self, report: PassReport) {
        for observed in &report.yields {
            self.yields.observe(observed);
            self.prefixes.observe(&observed.bucket, &observed.prefixes);
        }
        if let Some(usage) = &report.usage {
            crate::metrics::cache_usage(
                usage.after.used,
                usage.after.capacity,
                self.low_water,
                self.high_water,
            );
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
        // one exception: a cache filling faster than a pass completes never reaches rung 1 by the
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

/// The running pass's report, clearing the handle as it takes it. Pends forever when no pass is
/// running, so it can sit in the loop's `select!` unconditionally. A pass that panicked or was
/// aborted leaves nothing to settle: the ladder and the yields simply keep the state they were
/// judged against, and the next tick starts a pass that re-derives its plan from scratch.
async fn finished(pass: &mut Option<JoinHandle<PassReport>>) -> Option<PassReport> {
    let Some(handle) = pass.as_mut() else {
        return std::future::pending().await;
    };
    let outcome = handle.await;
    *pass = None;
    match outcome {
        Ok(report) => Some(report),
        Err(e) => {
            tracing::warn!(error = %e, "scavenger pass did not finish");
            None
        }
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
    /// Buckets an eviction may take a body from. Empty in durable mode; a subset of `swept` in
    /// cached, since eviction waits for a bucket's pending-set rebuild.
    evictable: HashSet<Arc<str>>,
    /// One entry per probe, already drawn against the cold yields and the learned prefixes.
    probes: Vec<Probe>,
    probe_pages: usize,
    opportunistic_evictions: usize,
    low_water: f64,
    high_water: f64,
}

/// One bucket to probe and where in it to start. Drawn on the actor, which owns both learners, so the
/// pass carries the decision rather than the state behind it.
struct Probe {
    /// Shared with every candidate the probe returns, so a bucket name is allocated once per probe
    /// rather than once per key found — the same reason [`crate::keylocks`] shares its keys.
    bucket: Arc<str>,
    /// The `<data>` position only: a shadow probe draws its own, over digests.
    position: String,
}

struct Pass {
    tier: Tiering,
    source: Option<Arc<dyn UsageSource>>,
    /// Read directly, not asked for: a pass is GC's own work, and a probe is a filter lookup that
    /// would cost more to queue than to take the lock for.
    ring: Arc<Mutex<RecencyRing>>,
    plan: Plan,
}

/// What the pass observed, on its way back to the actor.
struct PassReport {
    reclaimed: u64,
    target: u64,
    elapsed: Duration,
    yields: Vec<ProbeYield>,
    /// `None` when there is no usage source or its sample failed — the ladder then stays put.
    usage: Option<UsageBracket>,
}

/// What one round of probes found: eviction candidates in the order they should be considered, what
/// the probes taught the actor's learned state, and the transition marks they passed on the way.
struct Probed {
    /// Evictable-bucket candidates only, coldest first. A probe of a bucket eviction may not touch
    /// still teaches the yields and still reclaims debris — it just has nothing to offer here.
    candidates: Vec<(Candidate, Age)>,
    yields: Vec<ProbeYield>,
    /// Debris the probes reclaimed on their way past, as counts: it is dealt with in the future that
    /// found it, so nothing about it travels.
    marks: usize,
    twins: usize,
}

/// One bucket's two walks. The debris is already reclaimed by the time this is returned — the probe
/// holds the bucket and the tier, and nothing above needs to see a key it will never act on.
#[derive(Default)]
struct BucketProbe {
    yields: Vec<ProbeYield>,
    candidates: Vec<Candidate>,
    marks: usize,
    twins: usize,
}

/// Usage either side of the pass. The *after* sample is what the water marks are read against, and
/// the pair is what tells the ladder whether the cache filled faster than the pass drained it.
struct UsageBracket {
    before: Usage,
    after: Usage,
}

impl Pass {
    async fn execute(self) -> PassReport {
        let started = std::time::Instant::now();
        let before = self.sample().await;
        let target = before
            .filter(|u| u.ratio() >= self.plan.high_water)
            .map(|u| u.excess_over(self.plan.low_water))
            .unwrap_or(0);

        // Rung 0, always first: debris and dead bytes are reclaim at zero rehydration risk — nobody
        // was ever going to read an abandoned upload's parts — so a target met from them alone evicts
        // nothing at all.
        let mut swept = self.sweep_uploads().await;
        if target > 0 {
            self.compact().await;
        }

        // The probes run in **both modes and under no pressure**, because the walk is not only the
        // eviction scan: two of the three debris classes are found by classifying entries it reads
        // regardless, and a durable deployment — which never evicts and so would never probe on
        // eviction's account alone — accrues both of them. It costs what the separate debris sweeps
        // it replaced cost, and the same pages serve eviction the moment there is pressure.
        let probed = self.probe().await;
        swept.marks += probed.marks;
        swept.twins += probed.twins;

        let mut reclaimed = swept.bytes;
        if reclaimed < target {
            reclaimed += self.evict(probed.candidates, target - reclaimed).await;
        }
        let yields = probed.yields;
        if swept.any() {
            tracing::info!(
                uploads = swept.uploads,
                twins = swept.twins,
                marks = swept.marks,
                orphaned = swept.orphaned,
                bytes = swept.bytes,
                "swept debris"
            );
        }
        crate::metrics::gc_debris_swept(&swept);

        let after = self.sample().await;
        PassReport {
            reclaimed,
            target,
            elapsed: started.elapsed(),
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

    /// The one debris class with a listing of its own: an upload's record range is named by a single
    /// prefix, so it is swept exhaustively rather than sampled. The same listing is what the orphan
    /// half needs — a leak from a failed create is reclaimed within one interval rather than
    /// surviving to the next restart — so the two are one pass.
    async fn sweep_uploads(&self) -> Swept {
        let mut reclaimed = Swept::default();
        for chunk in self.plan.swept.chunks(self.plan.setting.concurrency.max(1)) {
            let sweeps = chunk
                .iter()
                .map(|bucket| debris::sweep_uploads(&self.tier, bucket));
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
        reclaimed
    }

    /// Run the pass's probes and return the candidates in the order eviction should consider them:
    /// coldest age first, LastModified ascending within an age.
    ///
    /// Two walks per bucket, one per namespace, and each returns everything its pages contained:
    /// `<data>` gives client bodies and transition marks, `<meta>` gives shadow bodies and twins.
    /// Shadows are not a separately weighted namespace — one bucket's shadows are only the composites
    /// something read back, so that range is usually near-empty and a yield of its own would mean two
    /// feedback loops to reason about for something that mostly does not exist.
    async fn probe(&self) -> Probed {
        let mut found = BucketProbe::default();
        for chunk in self
            .plan
            .probes
            .chunks(self.plan.setting.concurrency.max(1))
        {
            let probes = chunk.iter().map(|probe| self.probe_bucket(probe));
            for one in futures::future::join_all(probes).await {
                found.yields.extend(one.yields);
                found.candidates.extend(one.candidates);
                found.marks += one.marks;
                found.twins += one.twins;
            }
        }

        let mut aged: Vec<(Candidate, Age)> = {
            let ring = self.ring.lock().expect("recency ring lock poisoned");
            found
                .candidates
                .into_iter()
                .filter(|candidate| self.plan.evictable.contains(&candidate.bucket))
                .map(|candidate| {
                    let age = ring.probe(&candidate.qualified());
                    (candidate, age)
                })
                .collect()
        };
        aged.sort_by(|(left, left_age), (right, right_age)| {
            right_age
                .cmp(left_age)
                .then(left.mtime_ms.cmp(&right.mtime_ms))
        });
        Probed {
            candidates: aged,
            yields: found.yields,
            marks: found.marks,
            twins: found.twins,
        }
    }

    /// Walk one bucket's two namespaces and reclaim what the walks turn up that eviction will not.
    ///
    /// One walk's failure says nothing about the other's, or about any other bucket's, so each is
    /// logged and the round continues on whatever it did find.
    async fn probe_bucket(&self, probe: &Probe) -> BucketProbe {
        let (pages, bucket) = (self.plan.probe_pages, &probe.bucket);
        let mut found = BucketProbe::default();

        match scan::probe_bodies(&self.tier, bucket, probe.position.clone(), pages).await {
            Ok((yielded, candidates, marked)) => {
                found.yields.push(yielded);
                found.candidates = candidates;
                found.marks = debris::repair_marks(&self.tier, bucket, marked).await;
            }
            Err(e) => tracing::debug!(bucket = %bucket, error = %e, "body probe failed"),
        }
        match scan::probe_meta(&self.tier, bucket, pages).await {
            Ok((yielded, candidates, twins)) => {
                found.yields.push(yielded);
                found.candidates.extend(candidates);
                found.twins = debris::reclaim_twins(&self.tier, bucket, twins).await;
            }
            Err(e) => tracing::debug!(bucket = %bucket, error = %e, "meta probe failed"),
        }
        found
    }

    /// Evict coldest-first until `target` bytes are reclaimed, then keep taking **misses** up to the
    /// opportunistic bound: over-evicting an affirmatively cold key is nearly free in rehydration
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
        crate::metrics::gc_evicted(reclaimed);
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
