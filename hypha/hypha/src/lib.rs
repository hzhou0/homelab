//! Library entrypoint shared by the `hypha` binary and the integration tests: build the s3s
//! service from a validated [`Config`] and serve it with graceful connection draining. The binary
//! ([`main`](../main.rs)) is a thin wrapper that loads config, wires signal-driven shutdown, and
//! calls [`serve`]; the tests build the same service in-process and drive it with a real S3 client.

mod auth;
mod background;
mod bucket_ctl;
mod codec;
mod keylocks;
mod markers;
mod replication;
mod s3;
mod tier;

use std::error::Error;
use std::future::Future;
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::service::{S3Service, S3ServiceBuilder};
use tokio::net::TcpListener;

use hypha_core::config::Mode;
use hypha_core::{Backend, Config};

pub use s3::Hypha;

pub type BoxError = Box<dyn Error + Send + Sync>;

/// Build the s3s `S3Service` — hypha's client auth over the `Hypha` app — from a validated config,
/// alongside the [`Lifecycle`] [`serve`] needs to bracket it: the startup marker clear and the
/// drain-time quiescence proof (§7). The age envelope and trailer key both derive from
/// `master_passphrase` (§6).
pub fn build_service(config: &Config) -> Result<(S3Service, Lifecycle), BoxError> {
    let env = hypha_format::Envelope::new(&config.master_passphrase)
        .map_err(|e| format!("parsing master passphrase: {e}"))?;
    // Trailer authentication key: same master passphrase, distinct KDF domain (§6).
    let trailer_key = hypha_format::TrailerKey::derive(&config.master_passphrase);

    let remote = Backend::connect(&config.remote);
    // The cache is two buckets on one endpoint (§6): <data> holds client bodies + tombstones,
    // <meta> holds hypha's twins, markers, and mpu records.
    let data = Backend::connect(&config.cache);
    let meta = data.with_prefix(config.cache_meta_prefix.clone());

    let tier = tier::Reconciler {
        data,
        meta,
        remote,
        env: std::sync::Arc::new(env),
        trailer_key,
        locks: keylocks::KeyLocks::default(),
        upload_locks: keylocks::KeyLocks::default(),
        cached: config.mode == Mode::Cached,
    };

    // The repair queue's only strong sender goes to `Lifecycle`, so dropping that at drain is what
    // closes the channel — the proof that every obligation has finished (§7).
    let (markers, queue, worker) = markers::spawn(
        tier.clone(),
        Duration::from_millis(config.reconcile.interval_ms),
        config.reconcile.concurrency,
    );

    let app = Hypha::new(
        tier,
        markers.clone(),
        config.mode,
        config.serving.offload_threshold,
        config.max_bucket_prefix_len(),
        config.background,
    );

    // The cached-mode reconcile sweep (§7): a background duty that trails cache writes onto the
    // remote. It holds only a `Weak` to the app's liveness sentinel, so it stops when the service
    // drops — no explicit shutdown wiring. Durable mode has no pending set, so no sweep.
    if config.mode == Mode::Cached {
        let sweep = replication::Reconcile::new(
            app.tier.clone(),
            Duration::from_millis(config.reconcile.interval_ms),
            config.reconcile.concurrency,
        );
        tokio::spawn(sweep.run(app.liveness()));
    }

    let buckets = app.buckets.clone();
    let mut b = S3ServiceBuilder::new(app);
    b.set_auth(auth::SingleKeyAuth::new(
        config.auth.access_key.clone(),
        config.auth.secret_key.clone(),
    ));
    let lifecycle = Lifecycle {
        cached: config.mode == Mode::Cached,
        buckets,
        markers,
        queue: Some(queue),
        worker: tokio::spawn(worker.run()),
    };
    Ok((b.build(), lifecycle))
}

/// The two ends of a run that the object path itself cannot own (§7): the startup clear that makes
/// every bucket dirty on disk before a write can land, and the drain that proves quiescence before
/// writing any clean marker back.
pub struct Lifecycle {
    cached: bool,
    markers: markers::Markers,
    /// Startup owes a reconcile pass for every bucket whose clean marker was absent, and the actor
    /// is what runs it — so a pass restore already wants for the same bucket is one pass, not two.
    buckets: bucket_ctl::BucketCtl,
    /// The repair queue's only strong sender outside the obligation tasks. Dropping it is the seal.
    queue: Option<markers::Queue>,
    worker: tokio::task::JoinHandle<()>,
}

impl Lifecycle {
    /// Clear every bucket's clean marker before serving. Fails startup rather than serving around a
    /// marker that will not delete — that marker would skip next run's reconcile pass, by which time
    /// real orphans exist. Durable mode owes no markers, so there is nothing to clear.
    pub async fn startup(&self) -> Result<(), BoxError> {
        if self.cached {
            self.markers.startup(&self.buckets).await?;
        }
        Ok(())
    }

    /// Seal the run: drop the queue so the channel closes once the last obligation finishes, then
    /// let the worker make its final attempt and write the clean markers. Awaited *before* the
    /// active claim is released (§7) — a passive that promotes first could take writes into a bucket
    /// this run is about to vouch for.
    pub async fn drain(mut self) {
        if let Some(queue) = self.queue.take() {
            queue.seal();
        }
        if let Err(e) = self.worker.await {
            tracing::warn!(error = %e, "marker worker did not finish; clean markers withheld");
        }
    }
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
    lifecycle.startup().await?;

    let http = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        let (stream, _peer) = tokio::select! {
            res = listener.accept() => match res {
                Ok(c) => c,
                Err(e) => { tracing::error!(error = %e, "accept failed"); continue; }
            },
            () = shutdown.as_mut() => { tracing::info!("shutdown signalled: draining"); break; }
        };

        // Disable Nagle: streamed-body responses (GET) write headers then body chunks, and with
        // Nagle on the second small segment waits for the client's delayed ACK — a ~40 ms stall on
        // every read (writes/HEAD have single-segment responses and don't hit it). Latency over
        // throughput is the right trade for a request/response S3 surface.
        stream.set_nodelay(true)?;
        let conn = http.serve_connection(TokioIo::new(stream), service.clone());
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(error = %e, "connection ended");
            }
        });
    }

    // Step 1 of the quiescence proof (§7): when this resolves, every handler has returned and no new
    // one can start, so every marker obligation that will ever exist has been raised. On timeout we
    // skip the seal entirely — a connection may still commit a write, and the claim a clean marker
    // makes is about work we can no longer bound.
    tokio::select! {
        () = graceful.shutdown() => {
            tracing::info!("connections drained");
            lifecycle.drain().await;
        }
        () = tokio::time::sleep(DRAIN_TIMEOUT) => {
            tracing::warn!("drain timeout; clean markers withheld and the next run scans");
        }
    }
    Ok(())
}

/// Budget for the connection drain, sized to fit inside the pod's `terminationGracePeriod` (§9).
/// Overrunning it is safe — it costs the next run a recovery scan — but never silent.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(15);
