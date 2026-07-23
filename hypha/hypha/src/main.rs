//! hypha serving binary: load config, build the s3s service, and serve over plain HTTP (TLS is
//! terminated at the cluster gateway). Signal handling drains in-flight connections on
//! SIGTERM/Ctrl-C. The service construction and accept loop live in the library ([`hypha`]) so the
//! integration tests can build and drive the same service in-process.

use anyhow::Context as _;
use tokio::net::TcpListener;

use hypha::{build_service, serve};
use hypha_core::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::load().context("loading config")?;
    tracing::info!(mode = ?config.mode, "hypha starting");

    let service = build_service(&config).context("building service")?;

    let listener = TcpListener::bind(&config.serving.listen)
        .await
        .with_context(|| format!("binding {}", config.serving.listen))?;
    tracing::info!(addr = %config.serving.listen, "hypha listening");

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let ctrl_c = tokio::signal::ctrl_c();
    let shutdown = async move {
        tokio::select! {
            _ = ctrl_c => tracing::info!("Ctrl-C received"),
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
        }
    };

    serve(listener, service, shutdown).await.context("serving")
}
