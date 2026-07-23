//! Library entrypoint shared by the `hypha` binary and the integration tests: build the s3s
//! service from a validated [`Config`] and serve it with graceful connection draining. The binary
//! ([`main`](../main.rs)) is a thin wrapper that loads config, wires signal-driven shutdown, and
//! calls [`serve`]; the tests build the same service in-process and drive it with a real S3 client.

mod auth;
mod codec;
mod keylocks;
mod s3;
mod tier;

use std::error::Error;
use std::future::Future;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::service::{S3Service, S3ServiceBuilder};
use tokio::net::TcpListener;

use hypha_core::{Backend, Config};

pub use s3::Hypha;

pub type BoxError = Box<dyn Error + Send + Sync>;

/// Build the s3s `S3Service` — hypha's client auth over the `Hypha` app — from a validated config.
/// The age envelope and trailer key both derive from `master_passphrase` (§6).
pub fn build_service(config: &Config) -> Result<S3Service, BoxError> {
    let env = hypha_format::Envelope::new(&config.master_passphrase)
        .map_err(|e| format!("parsing master passphrase: {e}"))?;
    // Trailer authentication key: same master passphrase, distinct KDF domain (§6).
    let trailer_key = hypha_format::TrailerKey::derive(&config.master_passphrase);

    let remote = Backend::connect(&config.remote);
    let cache = Backend::connect(&config.cache);

    let app = Hypha::new(
        remote,
        cache,
        env,
        trailer_key,
        config.mode,
        config.serving.offload_threshold,
    );

    let mut b = S3ServiceBuilder::new(app);
    b.set_auth(auth::SingleKeyAuth::new(
        config.auth.access_key.clone(),
        config.auth.secret_key.clone(),
    ));
    Ok(b.build())
}

/// Serve `service` on `listener`, accepting connections until `shutdown` resolves, then drain
/// in-flight connections (bounded to 15 s). TLS is terminated at the cluster gateway, so this is
/// plain HTTP.
pub async fn serve<F>(
    listener: TcpListener,
    service: S3Service,
    shutdown: F,
) -> Result<(), BoxError>
where
    F: Future<Output = ()>,
{
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

    tokio::select! {
        () = graceful.shutdown() => tracing::info!("drained"),
        () = tokio::time::sleep(std::time::Duration::from_secs(15)) => tracing::warn!("drain timeout"),
    }
    Ok(())
}
