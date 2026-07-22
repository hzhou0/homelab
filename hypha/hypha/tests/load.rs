//! Load / concurrency stress. `#[ignore]` by default — run explicitly, ideally in release:
//!
//! ```text
//! cargo test -p hypha --test load --release -- --ignored --nocapture
//! ```
//!
//! Three angles: sustained mixed throughput across many workers (zero errors, reported ops/sec);
//! a linearizability assertion (N racing `If-None-Match:*` creates on one key ⇒ exactly one wins,
//! the rest 412 — no double-create); and parallel multipart uploads that must all commit and read
//! back correctly. Owns its MinIO + hypha; cleans up on drop.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use common::*;
use futures::future::join_all;

const B: &str = "load";

/// Sustained mixed PUT/GET/DELETE across many concurrent workers on disjoint keyspaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load test; run with --ignored --release"]
async fn throughput_mixed() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;

    let workers = 16usize;
    let iters = 25usize;
    let body = Arc::new(pattern(16 * 1024));
    let ops = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let tasks = (0..workers).map(|w| {
        let client = h.client();
        let body = body.clone();
        let ops = ops.clone();
        tokio::spawn(async move {
            for i in 0..iters {
                let key = format!("w{w}/obj{i}");
                let etag = put(&client, B, &key, &body).await;
                assert_eq!(etag, md5_hex(&body));
                let got = get_all(&client, B, &key).await;
                assert_eq!(got.len(), body.len());
                client
                    .delete_object()
                    .bucket(B)
                    .key(&key)
                    .send()
                    .await
                    .expect("delete");
                ops.fetch_add(3, Ordering::Relaxed);
            }
        })
    });
    for t in join_all(tasks).await {
        t.expect("worker panicked");
    }

    let n = ops.load(Ordering::Relaxed);
    let secs = start.elapsed().as_secs_f64();
    eprintln!(
        "throughput_mixed: {n} ops across {workers} workers in {secs:.2}s = {:.0} ops/s",
        n as f64 / secs
    );
}

/// Linearizability: many racing `If-None-Match:*` creates on one key resolve to exactly one winner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load test; run with --ignored --release"]
async fn no_double_create_under_contention() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let key = "hotkey";
    let racers = 24usize;

    let tasks = (0..racers).map(|w| {
        let client = h.client();
        tokio::spawn(async move {
            let body = pattern_seeded(2048, w as u8);
            client
                .put_object()
                .bucket(B)
                .key(key)
                .body(bytes_body(&body))
                .content_length(body.len() as i64)
                .if_none_match("*")
                .send()
                .await
                .map_err(|e| sdk_err_code(&e))
        })
    });

    let mut wins = 0usize;
    for r in join_all(tasks).await {
        match r.expect("racer panicked") {
            Ok(_) => wins += 1,
            Err(code) => assert_eq!(
                code.as_deref(),
                Some("PreconditionFailed"),
                "losers must fail with 412, not {code:?}"
            ),
        }
    }
    assert_eq!(wins, 1, "exactly one create may win the race");
    // The surviving object reads back cleanly.
    let head = h
        .client()
        .head_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("head winner");
    assert_eq!(head.content_length(), Some(2048));
}

/// Many multipart uploads in parallel all commit and read back byte-exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load test; run with --ignored --release"]
async fn parallel_multipart() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let uploads = 6usize;

    let tasks = (0..uploads).map(|u| {
        let client = h.client();
        tokio::spawn(async move {
            let key = format!("mpu/{u}");
            let p1 = pattern_seeded(MIN_PART, u as u8);
            let p2 = pattern_seeded(512 * 1024, u as u8 ^ 0xaa);
            let up = create_mpu(&client, B, &key).await;
            let e1 = upload_part(&client, B, &key, &up, 1, &p1).await;
            let e2 = upload_part(&client, B, &key, &up, 2, &p2).await;
            let etag = complete_mpu(&client, B, &key, &up, &[(1, e1), (2, e2)]).await;
            assert_eq!(etag, expected_composite_etag(&[&p1, &p2]));
            let whole: Vec<u8> = [p1.as_slice(), p2.as_slice()].concat();
            assert_eq!(get_all(&client, B, &key).await, whole, "upload {u} body");
        })
    });
    for t in join_all(tasks).await {
        t.expect("upload panicked");
    }
}
