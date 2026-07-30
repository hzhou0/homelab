//! The operational surface (§10): `/metrics`, `/healthz`, `/readyz`, on a listener of their own.
//!
//! Separate from the S3 port because the two answer to different clients. The S3 port is
//! authenticated and reached through the gateway; these are unauthenticated, scraped and probed
//! from inside the cluster, and must keep answering while the S3 port is refusing — a readiness
//! probe that fails only because the thing it reports on is unhealthy tells an operator nothing.
//!
//! **Bound by the binary, never by the library.** The integration harness builds many hyphas in one
//! process, and a fixed admin port would make that a port conflict; the metrics recorder is global
//! for the same reason (`crate::metrics`).
//!
//! Readiness is deliberately narrow. It reports the things that make a served answer *wrong* rather
//! than slow: a startup that has not finished, so a bucket's clean marker may not yet be cleared and
//! a restore not yet owed (§7); a drain that has begun, which takes the pod out of rotation before
//! its connections are cut rather than after; and a remote hypha cannot reach, which in either mode
//! is the authority behind every read it cannot serve from cache. Active/passive is **not** in it —
//! a passive that failed its probe could not be promoted into.

use std::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;

use hypha_core::Backend;

/// What the probes read. Cloned freely; the flag is the only state, since everything else worth
/// asking about is a live call to the thing being asked about.
#[derive(Clone)]
pub struct Health {
    serving: Arc<AtomicBool>,
    remote: Backend,
}

impl Health {
    pub(crate) fn new(remote: Backend) -> Self {
        Health {
            serving: Arc::new(AtomicBool::new(false)),
            remote,
        }
    }

    pub(crate) fn started(&self) {
        self.serving.store(true, Ordering::Release);
    }

    /// The drain has begun. Reported before the connections are cut, so traffic moves off this pod
    /// while it can still serve what it is holding.
    pub(crate) fn stopping(&self) {
        self.serving.store(false, Ordering::Release);
    }

    /// One call to the remote per probe, rather than a cached verdict: a reachability answer that
    /// can be stale is the one kind of answer readiness must not give.
    async fn ready(&self) -> bool {
        self.serving.load(Ordering::Acquire) && self.remote.list_buckets().await.is_ok()
    }
}

/// Serve the admin endpoints until `shutdown` resolves. In-flight probes are dropped rather than
/// drained — a scrape is repeated every interval and a probe every period, so there is nothing here
/// worth spending the pod's grace budget on.
pub async fn serve<F>(listener: TcpListener, health: Health, metrics: PrometheusHandle, shutdown: F)
where
    F: Future<Output = ()>,
{
    let http = std::sync::Arc::new(ConnBuilder::new(TokioExecutor::new()));
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _peer)) => stream,
                Err(e) => { tracing::warn!(error = %e, "admin accept failed"); continue; }
            },
            () = shutdown.as_mut() => return,
        };

        let (health, metrics, http) = (health.clone(), metrics.clone(), http.clone());
        tokio::spawn(async move {
            let serve = http.serve_connection(
                TokioIo::new(stream),
                hyper::service::service_fn(move |req| route(req, health.clone(), metrics.clone())),
            );
            if let Err(e) = serve.await {
                tracing::debug!(error = %e, "admin connection ended");
            }
        });
    }
}

async fn route(
    req: Request<hyper::body::Incoming>,
    health: Health,
    metrics: PrometheusHandle,
) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(match req.uri().path() {
        // Liveness is the process answering at all: every failure hypha can diagnose is either fatal
        // on its own (`crate::halt`) or something a restart would not fix, and both are worse served
        // by a restart loop than by the alert the other endpoints raise.
        "/healthz" => text(StatusCode::OK, "ok"),
        "/readyz" => match health.ready().await {
            true => text(StatusCode::OK, "ready"),
            false => text(StatusCode::SERVICE_UNAVAILABLE, "not ready"),
        },
        "/metrics" => text(StatusCode::OK, &metrics.render()),
        _ => text(StatusCode::NOT_FOUND, "not found"),
    })
}

fn text(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(body.to_owned())))
        .expect("a status and a fixed body always build a response")
}
