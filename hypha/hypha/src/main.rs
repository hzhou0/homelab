//! Hypha process entry point.

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use hypha::{build_service, serve, BoxError};
use hypha_core::Config;

fn ctx<E: std::fmt::Display>(msg: &str) -> impl FnOnce(E) -> BoxError + '_ {
    move |e| format!("{msg}: {e}").into()
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .json()
        // One line per request, on the span's close : the fields are filled in over the
        // handler's life, so the open carries almost nothing worth reading and only the close
        // carries the latency.
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::load().map_err(ctx("loading config"))?;
    tracing::info!(mode = ?config.mode, "hypha starting");

    // Installed here rather than in the library: the recorder is process-wide, and the integration
    // harness runs many hyphas in one process .
    let metrics = hypha::metrics::install().map_err(ctx("installing the metrics recorder"))?;

    let (service, lifecycle, _) = build_service(&config).await?;

    let listener = TcpListener::bind(&config.serving.listen)
        .await
        .map_err(ctx(&format!("binding {}", config.serving.listen)))?;
    tracing::info!(addr = %config.serving.listen, "hypha listening");

    let admin_listener = TcpListener::bind(&config.serving.admin_listen)
        .await
        .map_err(ctx(&format!("binding {}", config.serving.admin_listen)))?;
    tracing::info!(addr = %config.serving.admin_listen, "admin endpoints listening");

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let ctrl_c = tokio::signal::ctrl_c();
    let shutdown = async move {
        tokio::select! {
            _ = ctrl_c => tracing::info!("Ctrl-C received"),
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
        }
    };

    // Outlives the S3 drain deliberately: the readiness probe reports "not ready" for the whole
    // shutdown, which is what takes this pod out of rotation before its connections drain.
    let admin_shutdown = CancellationToken::new();
    let admin = tokio::spawn(hypha::serve_admin(
        admin_listener,
        lifecycle.health(),
        metrics,
        admin_shutdown.clone().cancelled_owned(),
    ));

    let served = serve(listener, service, lifecycle, shutdown).await;
    admin_shutdown.cancel();
    let _ = admin.await;
    served
}
