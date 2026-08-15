//! Prometheus metric definitions and recording helpers.

use std::time::Duration;

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};

/// Latency buckets, in seconds. Wide rather than fine: the questions these answer are "is a read
/// still sub-100 ms" and "did uploads fall off a cliff", and a homelab's scrape budget is better
/// spent on more series than on more resolution in any one.
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

pub fn install() -> Result<PrometheusHandle, BuildError> {
    let handle = PrometheusBuilder::new()
        .set_buckets(LATENCY_BUCKETS)?
        .install_recorder()?;
    describe();
    Ok(handle)
}

fn describe() {
    describe_counter!(
        "hypha_s3_requests_total",
        "Client S3 requests by operation and outcome"
    );
    describe_histogram!(
        "hypha_s3_request_seconds",
        "Client S3 request latency by operation"
    );
    describe_counter!(
        "hypha_cache_reads_total",
        "Client reads by whether the cache held the plaintext; a miss is served from the remote"
    );
    describe_gauge!(
        "hypha_pending_markers",
        "Keys in the pending set at the end of the last reconcile pass "
    );
    describe_histogram!(
        "hypha_reconcile_pass_seconds",
        "Duration of one reconcile pass"
    );
    describe_gauge!(
        "hypha_markers_owed",
        "Markers the queue is still retrying. Flat zero in health: an owed marker is the cache \
         refusing small writes, and it is also the queue's only bound "
    );
    describe_gauge!(
        "hypha_buckets_dirty_at_drain",
        "Buckets that got no clean marker at the last drain, so the next run rescans them"
    );
    describe_histogram!(
        "hypha_remote_upload_seconds",
        "Latency of one pending key's upload to the remote"
    );
    describe_counter!(
        "hypha_remote_uploads_total",
        "Reconcile transitions to the remote by outcome; a failure is retried on the next pass"
    );
    describe_counter!(
        "hypha_backpressure_throttled",
        "Cached writes refused with 503 SlowDown after waiting out the reconcile backpressure \
         timeout"
    );
    describe_counter!(
        "hypha_gc_reclaimed_bytes_total",
        "Bytes GC reclaimed, by whether they cost a client anything (debris is free, eviction \
         is paid back as rehydration latency)"
    );
    describe_counter!(
        "hypha_gc_debris_total",
        "Debris items GC reclaimed, by class"
    );
    describe_counter!(
        "hypha_rehydrations_declined_total",
        "Reads whose plaintext was left on the remote because the body is too large a share of \
         the cache to be worth landing; each one is a permanent cache miss for that key"
    );
    describe_histogram!("hypha_gc_pass_seconds", "Duration of one scavenger pass");
    describe_gauge!(
        "hypha_gc_ladder_rung",
        "The rung of the escalation ladder currently engaged. Its top with the byte target still \
         unmet is the cache-undersized signal — the one GC condition an operator must act on"
    );
    describe_gauge!(
        "hypha_cache_used_bytes",
        "Physical cache bytes in use (the usage source)"
    );
    describe_gauge!("hypha_cache_capacity_bytes", "Physical cache capacity");
    describe_histogram!(
        "hypha_startup_seconds",
        "Time from process start to serving, split by whether the last drain left anything to \
         recover. The two paths are only comparable apart: a rebuild re-derives pending sets a \
         clean start inherits"
    );
    describe_gauge!(
        "hypha_cache_water_mark_bytes",
        "Where a pass starts evicting (high) and what it reclaims down to (low)"
    );
}

pub(crate) fn s3_request(op: &'static str, failed: bool, elapsed: Duration) {
    let outcome = if failed { "error" } else { "ok" };
    counter!("hypha_s3_requests_total", "op" => op, "outcome" => outcome).increment(1);
    histogram!("hypha_s3_request_seconds", "op" => op).record(elapsed.as_secs_f64());
}

pub(crate) fn startup(recovering: bool, elapsed: Duration) {
    let path = if recovering { "rebuild" } else { "clean" };
    histogram!("hypha_startup_seconds", "path" => path).record(elapsed.as_secs_f64());
}

pub(crate) fn cache_read(hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    counter!("hypha_cache_reads_total", "result" => result).increment(1);
}

pub(crate) fn reconcile_pass(pending: usize, elapsed: Duration) {
    gauge!("hypha_pending_markers").set(pending as f64);
    histogram!("hypha_reconcile_pass_seconds").record(elapsed.as_secs_f64());
}

pub(crate) fn remote_upload(failed: bool, elapsed: Duration) {
    let outcome = if failed { "error" } else { "ok" };
    counter!("hypha_remote_uploads_total", "outcome" => outcome).increment(1);
    histogram!("hypha_remote_upload_seconds").record(elapsed.as_secs_f64());
}

pub(crate) fn backpressure_throttled() {
    counter!("hypha_backpressure_throttled").increment(1);
}

pub(crate) fn markers_owed(owed: usize) {
    gauge!("hypha_markers_owed").set(owed as f64);
}

pub(crate) fn buckets_dirty_at_drain(dirty: usize) {
    gauge!("hypha_buckets_dirty_at_drain").set(dirty as f64);
}

pub(crate) fn gc_debris_swept(swept: &crate::gc::Swept) {
    counter!("hypha_gc_debris_total", "class" => "upload_record").increment(swept.uploads as u64);
    counter!("hypha_gc_debris_total", "class" => "orphan_twin").increment(swept.twins as u64);
    counter!("hypha_gc_debris_total", "class" => "transition_mark").increment(swept.marks as u64);
    counter!("hypha_gc_debris_total", "class" => "orphaned_upload")
        .increment(swept.orphaned as u64);
    counter!("hypha_gc_reclaimed_bytes_total", "source" => "debris").increment(swept.bytes);
}

pub(crate) fn gc_evicted(bytes: u64) {
    counter!("hypha_gc_reclaimed_bytes_total", "source" => "eviction").increment(bytes);
}

pub(crate) fn gc_pass(rung: usize, elapsed: Duration) {
    gauge!("hypha_gc_ladder_rung").set(rung as f64);
    histogram!("hypha_gc_pass_seconds").record(elapsed.as_secs_f64());
}

pub(crate) fn rehydration_declined() {
    counter!("hypha_rehydrations_declined_total").increment(1);
}

pub(crate) fn cache_usage(used: u64, capacity: u64, low_water: f64, high_water: f64) {
    gauge!("hypha_cache_used_bytes").set(used as f64);
    gauge!("hypha_cache_capacity_bytes").set(capacity as f64);
    gauge!("hypha_cache_water_mark_bytes", "mark" => "low").set(capacity as f64 * low_water);
    gauge!("hypha_cache_water_mark_bytes", "mark" => "high").set(capacity as f64 * high_water);
}
