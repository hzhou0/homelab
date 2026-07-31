//! Operational endpoints against the real binary, which alone installs the metrics recorder.

mod common;

use common::*;

/// Readiness is the endpoint with a decision behind it (a started run, a reachable remote), so it is
/// the one that could plausibly answer wrong; liveness rides along because a probe pair that
/// disagrees about a healthy process is worth catching in the same breath.
#[tokio::test]
async fn a_serving_hypha_is_live_and_ready() {
    let h = Harness::durable_subprocess().await;

    assert_eq!(admin_get(&h, "/healthz").await.0, 200);
    assert_eq!(admin_get(&h, "/readyz").await.0, 200);
    assert_eq!(admin_get(&h, "/nope").await.0, 404);
}

/// The recorder is installed by the binary and the metric names are strings; nothing in the type
/// system connects a call site to what a dashboard queries, so the round trip is what pins them.
#[tokio::test]
async fn client_traffic_reaches_the_metrics_endpoint() {
    let h = Harness::durable_subprocess().await;
    let c = h.client();
    h.create_bucket("adminmetrics").await;
    put(&c, "adminmetrics", "k", b"hello").await;
    get_all(&c, "adminmetrics", "k").await;

    let (status, body) = admin_get(&h, "/metrics").await;
    assert_eq!(status, 200);
    for series in [
        "hypha_s3_requests_total{op=\"PutObject\",outcome=\"ok\"}",
        "hypha_s3_request_seconds",
        "hypha_cache_reads_total{result=\"miss\"}",
    ] {
        assert!(
            body.contains(series),
            "{series} is missing from the exposition:\n{body}"
        );
    }

    // The ladder gauge is written by the GC actor rather than by a request, so it also settles the
    // question of whether a background task's numbers reach the exposition at all.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !admin_get(&h, "/metrics")
        .await
        .1
        .contains("hypha_gc_ladder_rung ")
    {
        assert!(
            std::time::Instant::now() < deadline,
            "no scavenger pass reached the metrics endpoint"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
