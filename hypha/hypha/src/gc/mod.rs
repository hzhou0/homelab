//! The GC actor (§8) — the active replica's standing duty to reclaim what the client path
//! deliberately left behind, and the sole owner of everything GC remembers.
//!
//! Several paths ack a client before their cleanup is done, on purpose: complete and abort leave an
//! upload's whole record range in place rather than pay a large multi-object delete on the client
//! path (§6, *Multipart upload state*), and every crash-window sequence in §6/§7 is ordered to leave
//! *debris* rather than a hybrid state — a twin beside a live body, a transition mark nobody came
//! back for. Nothing here is a durability obligation; every item is something a reader already
//! ignores, which is what makes the sweep's cadence a cost question rather than a correctness one.
//!
//! **One actor, one owner.** GC's state — the recency ring today, the per-bucket cold yields and the
//! pressure rung that phase 5c adds — is read and written from this task and nowhere else. The rest
//! of the crate holds a [`Gc`], whose whole API is [`Gc::touch`]: a request states interest in a key
//! and returns, with no lock to contend on and no I/O to wait for. The filter bits, the rotation and
//! the encode of the retired slice all happen here, so a request never carries the rotation its own
//! touch happened to trigger — which, at the design fill target, means copying out a megabyte-scale
//! filter.
//!
//! **The loop itself never awaits I/O.** A sweep is dispatched as its own task; the actor only picks
//! the moment and refuses to start a second one. Doing the sweep inline would stall the touch queue
//! for the length of a listing-heavy pass and shed exactly the traffic GC most wants to remember.
//!
//! Both modes sweep. Durable mode evicts nothing — it holds no bodies to evict — but it produces
//! every class of debris that cached mode does.
//!
//! Only `Ready` buckets are swept. A bucket mid-restore has no cache namespace worth reading (§7),
//! and its recovery is additive, so debris there is either already gone with the volume or about to
//! be judged against a namespace that is still being rebuilt.
//!
//! The interval and concurrency here are §8's **unpressured base**, held fixed: the escalation
//! ladder that moves them arrives with the usage source that can tell whether there is any pressure
//! to respond to. Debris reclaim is the ladder's rung 0 either way — it returns bytes at no
//! rehydration risk, so it is what a pressured pass will spend before it evicts anything.

mod debris;
mod ring;
mod store;

use std::time::Duration;

use futures::TryStreamExt as _;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use hypha_core::config;
use hypha_core::error::Result;
use hypha_core::Backend;

use crate::bucket::BucketCtl;
use crate::tier::Tiering;
use ring::RecencyRing;
use store::GcStore;

/// Touches waiting to be recorded. Generous, because shedding one is a small but real loss of
/// fidelity and the loop never awaits I/O — it can only fall behind by being starved of CPU, which a
/// deep queue rides out.
const TOUCH_QUEUE_DEPTH: usize = 1024;

/// Batched off the queue for the same reason the marker actor batches: a touch is on every op's
/// path, so one wake-up should take whatever a burst deposited.
const TOUCH_BATCH: usize = 256;

enum GcMsg {
    Touch(String),
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
    tokio::spawn(
        GcActor {
            tier,
            buckets,
            store: GcStore::new(backend, bucket),
            ring: RecencyRing::new(&cfg.recency),
            interval: Duration::from_millis(cfg.interval_ms),
            concurrency: cfg.concurrency.max(1),
            rx,
            pass: None,
        }
        .run(),
    );
    Gc { tx }
}

impl Gc {
    /// Record interest in a key (§8). Every op that resolves or lands a single key calls this — the
    /// write path included, because a write is the strongest available statement of interest in a
    /// key and a read-only ring gets write-hot/read-cold keys exactly backwards.
    ///
    /// Infallible and non-blocking by construction: the ring is advisory, so there is no outcome a
    /// caller could act on, and a shed touch costs at most one key's ordering in one eviction cycle.
    pub(crate) fn touch(&self, bucket: &str, key: &str) {
        if self
            .tx
            .try_send(GcMsg::Touch(ring::qualified(bucket, key)))
            .is_err()
        {
            tracing::debug!("recency touch dropped; GC is behind");
        }
    }
}

struct GcActor {
    tier: Tiering,
    buckets: BucketCtl,
    store: GcStore,
    ring: RecencyRing,
    interval: Duration,
    concurrency: usize,
    rx: mpsc::Receiver<GcMsg>,
    /// At most one sweep in flight: passes are idempotent, but overlapping them buys nothing and
    /// doubles the listing load exactly when the cache is already struggling.
    pass: Option<JoinHandle<()>>,
}

impl GcActor {
    async fn run(mut self) {
        self.restore().await;
        let mut ticks = tokio::time::interval(self.interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick resolves immediately; a fresh process has nothing to sweep yet.
        ticks.tick().await;

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
                        }
                    }
                }
                _ = ticks.tick() => self.dispatch_pass(),
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
        // Off the loop, for the same reason the sweep is: the actor must stay ready to record the
        // touches arriving behind this one. The slice is already in the ring, so a persist that
        // fails costs only the *next* process one colder cycle.
        let (store, depth) = (self.store.clone(), self.ring.depth());
        tokio::spawn(async move {
            if let Err(e) = store.persist(&retired, depth).await {
                tracing::warn!(seq = retired.seq, error = %e, "recency slice not persisted");
            }
        });
    }

    fn dispatch_pass(&mut self) {
        if self.pass.as_ref().is_some_and(|p| !p.is_finished()) {
            tracing::debug!("scavenger pass still running; skipping this tick");
            return;
        }
        let (tier, buckets, concurrency) =
            (self.tier.clone(), self.buckets.clone(), self.concurrency);
        self.pass = Some(tokio::spawn(async move {
            if let Err(e) = pass(&tier, &buckets, concurrency).await {
                // A pass reclaims nothing it cannot see; the next one starts over from a fresh
                // listing, so a failed pass costs one interval of unreclaimed bytes.
                tracing::warn!(error = %e, "scavenger pass failed; retrying next interval");
            }
        }));
    }
}

async fn pass(tier: &Tiering, buckets: &BucketCtl, concurrency: usize) -> Result<()> {
    futures::stream::iter(buckets.ready().into_iter().map(Ok))
        .try_for_each_concurrent(concurrency, |bucket| async move {
            let reclaimed = debris::sweep_mpu_ranges(tier, &bucket).await?;
            if reclaimed > 0 {
                tracing::info!(bucket, uploads = reclaimed, "reclaimed mpu record ranges");
            }
            Ok(())
        })
        .await
}
