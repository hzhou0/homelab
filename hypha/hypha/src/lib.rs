//! Library entrypoint shared by the `hypha` binary and the integration tests: build the s3s
//! service from a validated [`Config`] and serve it with graceful connection draining. The binary
//! ([`main`](../main.rs)) is a thin wrapper that loads config, wires signal-driven shutdown, and
//! calls [`serve`]; the tests build the same service in-process and drive it with a real S3 client.
//!
//! # Shutdown
//!
//! Three phases, each with its own budget, run in an order that follows what the work *owes* rather
//! than where it lives:
//!
//! 1. **The API closes.** The accept loop stops and hyper drains the live connections, after which
//!    every handler has returned and no new one can start — step 1 of §7's quiescence proof.
//! 2. **Obligations are settled** ([`Lifecycle::seal`]): the startup shadow sweeps are joined, then
//!    the two sealed actors write what the run has earned. This precedes the rest deliberately — the
//!    clean markers are the run's durability-relevant output and must not queue behind a bucket
//!    recovery that could run for minutes.
//! 3. **Every remaining actor quiesces** ([`Lifecycle::quiesce`]): the shutdown token is cancelled,
//!    the last handles are dropped so each queue closes, and each actor is *joined* — it finishes the
//!    messages it already holds and returns on its own. Nothing is aborted while it still has work,
//!    which is the point: a task killed mid-call leaves debris that is recoverable but real (a twin
//!    beside a live body, a half-drained bucket).
//!
//! A phase that overruns its budget logs and aborts what is left, since the alternative is
//! overrunning the pod's `terminationGracePeriod` and being killed mid-call anyway (§9).

mod admin;
mod auth;
mod background;
mod bucket;
mod codec;
mod gc;
mod halt;
mod keylocks;
mod markers;
pub mod metrics;
mod replication;
mod s3;
mod tier;
mod volume_watch;

use std::error::Error;
use std::future::Future;
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::service::{S3Service, S3ServiceBuilder};
use tokio::net::TcpListener;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use hypha_core::config::{Mode, DATA_ROLE, META_ROLE, REMOTE_ROLE};
use hypha_core::{Backend, Config};

pub use admin::{serve as serve_admin, Health};
pub use halt::EXIT_INVARIANT_VIOLATION;
pub use s3::Hypha;

pub type BoxError = Box<dyn Error + Send + Sync>;

/// Build the s3s `S3Service` — hypha's client auth over the `Hypha` app — from a validated config,
/// alongside the [`Lifecycle`] [`serve`] needs to bracket it: the startup marker clear and the
/// drain-time quiescence proof (§7). The age envelope and trailer key both derive from
/// `master_passphrase` (§6). The persistent halt check precedes every actor spawn, so a halted
/// deployment does no background work on its way back out.
pub async fn build_service(config: &Config) -> Result<(S3Service, Lifecycle), BoxError> {
    let env = hypha_format::Envelope::new(&config.master_passphrase)
        .map_err(|e| format!("parsing master passphrase: {e}"))?;
    // Trailer authentication key: same master passphrase, distinct KDF domain (§6).
    let trailer_key = hypha_format::TrailerKey::derive(&config.master_passphrase);

    let remote = Backend::connect(&config.remote, config.role_prefix(REMOTE_ROLE));
    halt::exit_if_marked(&remote).await?;
    // The cache carries three roles on one endpoint (§6, §8): <data> holds client bodies +
    // tombstones, <meta> hypha's twins/markers/mpu records, and GC's own bucket the recency ring.
    let data = Backend::connect(&config.cache, config.role_prefix(DATA_ROLE));
    let meta = data.with_prefix(config.role_prefix(META_ROLE));
    // A whole bucket name, not a prefix — nothing is appended to it — so the backend it goes
    // through must not prepend one either.
    let gc_backend = data.with_prefix(String::new());

    let halt = halt::Halt::new(remote.clone());
    let serve_halt = halt.clone();

    let tier = tier::Tiering {
        data,
        meta,
        remote,
        env: std::sync::Arc::new(env),
        trailer_key,
        locks: keylocks::KeyLocks::default(),
        upload_locks: keylocks::KeyLocks::default(),
        cached: config.mode == Mode::Cached,
        halt,
    };

    // Cancelled at the start of the actor-quiescence phase. It exists because two of the actors
    // cannot learn of shutdown from their queue closing: the bucket-control actor holds a sender of
    // its own so a recovery can re-queue itself, and the interval loops are asleep between passes
    // rather than parked on a channel. It is a *stop taking new work* signal, never an abandon — the
    // work already in hand is always finished (see the module docs).
    let shutdown = CancellationToken::new();

    // Ordered: the marker machinery reads its per-bucket accounting off the bucket-control actor's
    // published state, so the actor exists first (the dependency runs one way — `bucket::ctl` knows
    // nothing of `markers`).
    let (buckets, bucket_actor) = bucket::spawn(tier.clone(), shutdown.clone());
    let (startup_tier, startup_buckets) = (tier.clone(), buckets.clone());

    // The repair queue's only strong sender goes to `Lifecycle`, so dropping that at drain is what
    // closes the channel — the proof that every obligation has finished (§7).
    let (markers, run_seal, marker_actor) = markers::spawn(
        tier.clone(),
        buckets.clone(),
        Duration::from_millis(config.reconcile.interval_ms),
        config.reconcile.concurrency,
    );

    // Orphaned shadow bodies (§8), cached mode's third obligation of the marker shape: a write that
    // supersedes a composite leaves its rehydrated plaintext unreachable, and — unlike an evictable
    // body — nothing ever touches it again for the recency ring to rank. Same queue/seal/marker
    // structure as `markers`, and for the same reason: the enqueue sits after the commit.
    let (orphans, orphan_seal, orphan_actor) = gc::orphans::spawn(
        tier.clone(),
        buckets.clone(),
        Duration::from_millis(config.reconcile.interval_ms),
    );

    // The GC actor (§8), both modes: debris accumulates wherever the client path acked before its
    // cleanup was done, and recency is fed by every op that resolves or lands a key. It owns its own
    // cadence and stops when the last handle — the one `Hypha` holds — drops.
    let (gc, gc_actor) = gc::spawn(
        tier.clone(),
        buckets.clone(),
        gc_backend,
        config.gc_bucket(),
        &config.gc,
    );

    // Spawned here rather than inside `Hypha` so the drain has a handle to join: the transitions it
    // runs hold K's write lock across a fetch, and one killed between landing a body and deleting its
    // twin leaves exactly the hybrid state §8 orders every path to avoid.
    let (background, background_actor) =
        background::spawn(tier.clone(), config.background, shutdown.clone());

    let app = Hypha::new(
        tier,
        buckets,
        markers.clone(),
        gc,
        orphans,
        background,
        config.mode,
        config.serving.offload_threshold,
        config.max_bucket_prefix_len(),
    );

    // The one failure a running process still has to watch for (§7): its cache volume vanishing
    // underneath it, which would have a ready bucket answering 404 for objects that exist.
    let volume_watch = tokio::spawn(
        volume_watch::VolumeWatch::new(
            app.tier.clone(),
            app.buckets.clone(),
            Duration::from_millis(config.volume_watch_interval_ms),
        )
        .run(shutdown.clone()),
    );

    // The cached-mode reconcile sweep (§7): a background duty that trails cache writes onto the
    // remote. Durable mode has no pending set, so no sweep.
    let replication = (config.mode == Mode::Cached).then(|| {
        let replication = replication::ReplicationTask::new(
            app.tier.clone(),
            app.buckets.clone(),
            Duration::from_millis(config.reconcile.interval_ms),
            config.reconcile.concurrency,
        );
        tokio::spawn(replication.run(shutdown.clone()))
    });

    let mut b = S3ServiceBuilder::new(app);
    b.set_auth(auth::SingleKeyAuth::new(
        config.auth.access_key.clone(),
        config.auth.secret_key.clone(),
    ));
    let mut actors = vec![
        ("bucket-control", bucket_actor),
        ("gc", gc_actor),
        ("background-transition", background_actor),
        ("volume-watch", volume_watch),
    ];
    actors.extend(replication.map(|task| ("reconcile", task)));

    let lifecycle = Lifecycle {
        health: Health::new(startup_tier.remote.clone()),
        tier: startup_tier,
        buckets: startup_buckets,
        halt: serve_halt,
        shutdown,
        seal: Some(run_seal),
        marker_actor: tokio::spawn(marker_actor.run()),
        orphan_seal: Some(orphan_seal),
        orphan_actor: tokio::spawn(orphan_actor.run()),
        sweeps: JoinSet::new(),
        actors,
    };
    Ok((b.build(), lifecycle))
}

/// The two ends of a run that the object path itself cannot own (§7): the startup clear that makes
/// every bucket dirty on disk before a write can land, and the drain that proves quiescence before
/// writing any clean marker back.
pub struct Lifecycle {
    /// Handed to the admin listener before this is moved into [`serve`] — the probes have to be
    /// answering *before* startup finishes, since "not ready yet" is precisely what they report.
    health: admin::Health,
    tier: tier::Tiering,
    buckets: bucket::BucketCtl,
    halt: halt::Halt,
    /// Cancelled once the obligations are settled, so no actor takes new work after that point.
    shutdown: CancellationToken,
    /// The repair queue's only strong sender outside the marker tasks. Sending its one message is
    /// the seal; *dropping* it only closes the channel, which an abort does too.
    seal: Option<markers::RunSeal>,
    marker_actor: JoinHandle<()>,
    /// The same pair for the shadow-orphan queue (§8), held separately so a shadow reclaim that never
    /// lands cannot withhold a *pending-set* clean marker and send the next run into a full rebuild.
    orphan_seal: Option<gc::orphans::OrphanSeal>,
    orphan_actor: JoinHandle<()>,
    /// The startup shadow sweeps (§8). Joined before the orphan seal, since a sweep only earns its
    /// bucket's accounting by finishing — after the seal has read the accounting it counts for
    /// nothing, and the sweep is one prefix listing.
    sweeps: JoinSet<()>,
    /// Every actor with no obligation to settle, named for the log line it gets if it overruns.
    actors: Vec<(&'static str, JoinHandle<()>)>,
}

impl Lifecycle {
    pub fn health(&self) -> admin::Health {
        self.health.clone()
    }

    pub async fn startup(&mut self) -> Result<(), BoxError> {
        self.sweeps = bucket::resolve_all(&self.tier, &self.buckets).await?;
        self.health.started();
        Ok(())
    }

    /// Seal the run: [`markers::MarkerActor`] then makes its final attempt and writes the clean
    /// markers. Awaited *before* the active claim is released (§7) — a passive that promotes first
    /// could take writes into a bucket this run is about to vouch for.
    ///
    /// Called only when the connection drain completed. A drain that timed out cannot bound the work
    /// a clean marker would be vouching for, so the run ends with none.
    async fn seal(&mut self) {
        let settled = {
            let halt = self.halt.clone();
            let halt_signal = halt.shutdown_signalled();
            let settle = tokio::time::timeout(OBLIGATION_DRAIN, self.settle());
            tokio::pin!(halt_signal, settle);
            tokio::select! {
                biased;
                () = halt_signal.as_mut() => halt_until_exit().await,
                settled = settle.as_mut() => settled,
            }
        };
        if settled.is_err() {
            tracing::warn!("obligation drain overran its budget; clean markers withheld");
            self.sweeps.abort_all();
            self.marker_actor.abort();
            self.orphan_actor.abort();
        }
    }

    async fn settle(&mut self) {
        while let Some(swept) = self.sweeps.join_next().await {
            if let Err(e) = swept {
                tracing::warn!(error = %e, "startup shadow sweep did not finish");
            }
        }
        if let Some(run_seal) = self.seal.take() {
            run_seal.seal();
        }
        if let Some(orphan_seal) = self.orphan_seal.take() {
            orphan_seal.seal();
        }
        if let Err(e) = (&mut self.marker_actor).await {
            tracing::warn!(error = %e, "marker actor did not finish; clean markers withheld");
        }
        // Awaited after the markers: this one only ever leaks bytes, so it must not delay the
        // obligation that decides whether the next run rescans for acked writes.
        if let Err(e) = (&mut self.orphan_actor).await {
            tracing::warn!(error = %e, "shadow actor did not finish; shadow-clean markers withheld");
        }
    }

    /// Stop the remaining actors and join them. Each finishes the messages it is already holding, so
    /// this waits on work rather than cutting it off — the whole reason the drain has a budget at all.
    ///
    /// Dropping the handles is half the signal: an actor whose queue has no senders left knows there
    /// is nothing more to receive. The token is the other half, for the two that could not tell.
    async fn quiesce(mut self) {
        self.shutdown.cancel();
        // The last handles outside the actors themselves. Until these go, a queue still has a sender
        // and an actor still has reason to wait on it.
        drop(self.buckets);
        drop(self.tier);

        let joined = {
            let halt = self.halt.clone();
            let halt_signal = halt.shutdown_signalled();
            let actors = tokio::time::timeout(ACTOR_QUIESCE, async {
                for (name, actor) in &mut self.actors {
                    if let Err(e) = actor.await {
                        tracing::warn!(actor = name, error = %e, "actor did not finish");
                    }
                }
            });
            tokio::pin!(halt_signal, actors);
            tokio::select! {
                biased;
                () = halt_signal.as_mut() => halt_until_exit().await,
                joined = actors.as_mut() => joined,
            }
        };

        // Reached only when an actor still had work in hand, where the choice is no longer between
        // finishing and not — it is between this and being SIGKILLed with the same work outstanding
        // and no log line to say so.
        if joined.is_err() {
            let outstanding: Vec<&str> = self
                .actors
                .iter()
                .filter(|(_, actor)| !actor.is_finished())
                .map(|(name, _)| *name)
                .collect();
            tracing::warn!(
                ?outstanding,
                "actor quiescence overran its budget; abandoning what is left"
            );
            for (_, actor) in &self.actors {
                actor.abort();
            }
        }
    }
}

async fn halt_until_exit() -> ! {
    tracing::error!("halted on an invariant violation; recording the halt");
    std::future::pending().await
}

/// Serve `service` on `listener`, accepting connections until `shutdown` resolves, then drain
/// in-flight connections (bounded to 15 s). TLS is terminated at the cluster gateway, so this is
/// plain HTTP.
pub async fn serve<F>(
    listener: TcpListener,
    service: S3Service,
    lifecycle: Lifecycle,
    shutdown: F,
) -> Result<(), BoxError>
where
    F: Future<Output = ()>,
{
    let mut lifecycle = lifecycle;
    lifecycle.startup().await?;

    let http = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);
    let halt = lifecycle.halt.clone();
    let mut connections = JoinSet::new();
    let mut accept_backoff = ACCEPT_RETRY_MIN;

    let halted = 'accept: loop {
        tokio::select! {
            biased;
            () = halt.shutdown_signalled() => {
                lifecycle.health.stopping();
                break true;
            }
            () = shutdown.as_mut() => {
                tracing::info!("shutdown signalled: draining");
                lifecycle.health.stopping();
                break false;
            }
            Some(finished) = connections.join_next(), if !connections.is_empty() => {
                if let Err(e) = finished {
                    tracing::warn!(error = %e, "connection task did not finish");
                }
            }
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(connection) => {
                        accept_backoff = ACCEPT_RETRY_MIN;
                        connection
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            retry_ms = accept_backoff.as_millis(),
                            "accept failed; retrying"
                        );
                        tokio::select! {
                            biased;
                            () = halt.shutdown_signalled() => {
                                lifecycle.health.stopping();
                                break 'accept true;
                            }
                            () = shutdown.as_mut() => {
                                tracing::info!("shutdown signalled: draining");
                                lifecycle.health.stopping();
                                break 'accept false;
                            }
                            () = tokio::time::sleep(accept_backoff) => {}
                        }
                        accept_backoff = (accept_backoff * 2).min(ACCEPT_RETRY_MAX);
                        continue;
                    }
                };

                // Disable Nagle: streamed-body responses (GET) write headers then body chunks, and
                // with Nagle on the second small segment waits for the client's delayed ACK — a
                // ~40 ms stall on every read (writes/HEAD have single-segment responses and don't
                // hit it). Latency over throughput is the right trade for a request/response S3
                // surface, but failing to set the optimization is not a reason to reject a client.
                if let Err(e) = stream.set_nodelay(true) {
                    tracing::warn!(%peer, error = %e, "TCP_NODELAY could not be enabled");
                }
                let conn = http.serve_connection(TokioIo::new(stream), service.clone());
                let conn = graceful.watch(conn.into_owned());
                connections.spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!(%peer, error = %e, "connection ended");
                    }
                });
            }
        }
    };
    drop(listener);

    // Signal the live connections, but neither await the drain nor return: a handler that raised the
    // violation remains in flight until the recorder exits the process, so the drain could only time
    // out — and returning would end the process *successfully*, losing the record (`crate::halt`).
    if halted {
        tokio::spawn(graceful.shutdown());
        halt_until_exit().await
    }

    // Step 1 of the quiescence proof (§7): when this resolves, every handler has returned and no new
    // one can start, so every marker obligation that will ever exist has been raised. On timeout the
    // seal is skipped entirely — a connection may still commit a write, and the claim a clean marker
    // makes is about work we can no longer bound.
    let mut connection_drain = tokio::spawn(graceful.shutdown());
    let drained = tokio::select! {
        biased;
        () = halt.shutdown_signalled() => halt_until_exit().await,
        result = &mut connection_drain => match result {
            Ok(()) => {
                tracing::info!("connections drained");
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "connection drain task did not finish");
                false
            }
        },
        () = tokio::time::sleep(CONNECTION_DRAIN) => {
            tracing::warn!("drain timeout; clean markers withheld and the next run scans");
            false
        }
    };
    if drained {
        while let Some(finished) = connections.join_next().await {
            if let Err(e) = finished {
                tracing::warn!(error = %e, "connection task did not finish");
            }
        }
    } else {
        connections.shutdown().await;
        let _ = connection_drain.await;
    }

    // The API's own copy of every actor handle. Dropped before the actors are joined, or each of them
    // would still be holding a queue that the service could in principle write to again.
    drop(service);

    if drained {
        lifecycle.seal().await;
    }
    lifecycle.quiesce().await;
    Ok(())
}

/// Per-phase shutdown budgets (§9). Deliberately not configurable: they are a property of the pod's
/// `terminationGracePeriod`, which has to be at least their sum plus the `preStop` delay, so the two
/// numbers only mean anything together. Overrunning any of them is safe — it costs the next run a
/// recovery scan, and leaves debris every sweep already handles — but never silent.
const CONNECTION_DRAIN: Duration = Duration::from_secs(15);
const OBLIGATION_DRAIN: Duration = Duration::from_secs(10);
const ACTOR_QUIESCE: Duration = Duration::from_secs(10);
const ACCEPT_RETRY_MIN: Duration = Duration::from_millis(10);
const ACCEPT_RETRY_MAX: Duration = Duration::from_secs(1);
