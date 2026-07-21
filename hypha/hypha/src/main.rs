//! hypha serving binary bootstrap: load config, build the age envelope + backends, wire the
//! `s3s` service with hypha's client auth, and serve over plain HTTP (TLS is terminated at the
//! cluster gateway). Signal handling drains in-flight connections on SIGTERM/Ctrl-C.

mod auth;
mod codec;
mod keylocks;
mod s3;
mod tier;

use std::error::Error;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;

use hypha_core::{Backend, Config};

type BoxError = Box<dyn Error + Send + Sync>;

/// Wrap an error with a bootstrap-context message (stands in for `anyhow::Context`).
fn ctx<E: std::fmt::Display>(msg: &str) -> impl FnOnce(E) -> BoxError + '_ {
    move |e| format!("{msg}: {e}").into()
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::load().map_err(ctx("loading config"))?;

    let env = hypha_format::Envelope::new(&config.master_passphrase)
        .map_err(ctx("parsing master passphrase"))?;
    // Trailer authentication key: derived from the same master passphrase, distinct domain (§6).
    let trailer_key = hypha_format::TrailerKey::derive(&config.master_passphrase);

    let remote = Backend::connect(&config.remote);
    let cache = Backend::connect(&config.cache);
    tracing::info!(mode = ?config.mode, "hypha starting");

    let app = s3::Hypha::new(
        remote,
        cache,
        env,
        trailer_key,
        config.mode,
        config.serving.offload_threshold,
    );

    let service = {
        let mut b = S3ServiceBuilder::new(app);
        b.set_auth(auth::SingleKeyAuth::new(
            config.auth.access_key.clone(),
            config.auth.secret_key.clone(),
        ));
        b.build()
    };

    let listener = TcpListener::bind(&config.serving.listen)
        .await
        .map_err(ctx(&format!("binding {}", config.serving.listen)))?;
    tracing::info!(addr = %config.serving.listen, "hypha listening");

    let http = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        let (stream, _peer) = tokio::select! {
            res = listener.accept() => match res {
                Ok(c) => c,
                Err(e) => { tracing::error!(error = %e, "accept failed"); continue; }
            },
            _ = ctrl_c.as_mut() => { tracing::info!("Ctrl-C: draining"); break; }
            _ = sigterm.recv() => { tracing::info!("SIGTERM: draining"); break; }
        };

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
