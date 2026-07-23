//! Load / concurrency stress. `#[ignore]` by default — run explicitly, ideally in release:
//!
//! ```text
//! cargo test -p hypha --test load --release -- --ignored --nocapture
//! ```
//!
//! Angles: sustained mixed throughput across many workers (zero errors, reported ops/sec);
//! a linearizability assertion (N racing `If-None-Match:*` creates on one key ⇒ exactly one wins,
//! the rest 412 — no double-create); parallel multipart uploads that must all commit and read back
//! correctly; and per-operation **latency** percentiles, both idle and under concurrent load. Owns
//! its MinIO + hypha; cleans up on drop.
//!
//! The reported numbers are indicative only — one MinIO backs both cache and remote on a temp dir,
//! so they measure hypha's coordination overhead against a shared local backend, not a production
//! split. Tests assert correctness and success, never a latency threshold (that would be flaky and
//! machine-dependent); the percentiles are printed for a human to read with `--nocapture`.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// Idle per-operation latency: with no competing traffic, time each op individually and report the
/// distribution. Captures the base cost of hypha's durable bracket (mark → encrypt → remote →
/// settle) per op type.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load test; run with --ignored --release"]
async fn latency_baseline() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let body = pattern(16 * 1024);

    // Warm the connection pool / auth so the first few requests don't skew the tail.
    for i in 0..5 {
        put(&client, B, &format!("warm/{i}"), &body).await;
    }

    let iters = 60;
    let mut puts = Vec::new();
    let mut gets = Vec::new();
    let mut heads = Vec::new();
    let mut ranges = Vec::new();
    let mut deletes = Vec::new();
    for i in 0..iters {
        let key = format!("lat/{i}");

        let t = Instant::now();
        put(&client, B, &key, &body).await;
        puts.push(t.elapsed());

        let t = Instant::now();
        let got = get_all(&client, B, &key).await;
        gets.push(t.elapsed());
        assert_eq!(got.len(), body.len());

        let t = Instant::now();
        client
            .head_object()
            .bucket(B)
            .key(&key)
            .send()
            .await
            .expect("head");
        heads.push(t.elapsed());

        let t = Instant::now();
        let r = get_range(&client, B, &key, 0, 4095).await;
        ranges.push(t.elapsed());
        assert_eq!(r.len(), 4096);

        let t = Instant::now();
        client
            .delete_object()
            .bucket(B)
            .key(&key)
            .send()
            .await
            .expect("delete");
        deletes.push(t.elapsed());
    }

    summarize("PUT 16KiB", puts);
    summarize("GET 16KiB", gets);
    summarize("HEAD", heads);
    summarize("GET range 4KiB", ranges);
    summarize("DELETE", deletes);
}

/// Read latency **under concurrent write load**: background workers churn PUT/DELETE on disjoint
/// keys while we time repeated GETs of one stable key. Surfaces tail latency the idle test hides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load test; run with --ignored --release"]
async fn latency_under_load() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let body = pattern(16 * 1024);
    put(&client, B, "hot", &body).await;

    let stop = Arc::new(AtomicBool::new(false));
    let writers = 8usize;
    let bg: Vec<_> = (0..writers)
        .map(|w| {
            let c = h.client();
            let b = body.clone();
            let s = stop.clone();
            tokio::spawn(async move {
                let mut i = 0u64;
                while !s.load(Ordering::Relaxed) {
                    let key = format!("bg{w}/{i}");
                    put(&c, B, &key, &b).await;
                    c.delete_object()
                        .bucket(B)
                        .key(&key)
                        .send()
                        .await
                        .expect("bg delete");
                    i += 1;
                }
            })
        })
        .collect();

    let mut gets = Vec::new();
    for _ in 0..100 {
        let t = Instant::now();
        let got = get_all(&client, B, "hot").await;
        gets.push(t.elapsed());
        assert_eq!(got, body, "hot object must read correctly under load");
    }

    stop.store(true, Ordering::Relaxed);
    for t in bg {
        t.await.expect("background writer panicked");
    }

    summarize(&format!("GET (hot) w/ {writers} writers"), gets);
}

/// Streaming validation across a 1000× size range. §5's design is that per-request memory (and so
/// **time-to-first-byte**) is bounded by the pipe capacity, *not* the object size: hypha decrypts
/// the remote object chunk-by-chunk into the response as it goes. So GET TTFB must stay roughly
/// flat while total transfer grows with size — a buffer-the-whole-object implementation would show
/// TTFB scaling with size (TTFB ≈ total). Asserts the architectural property; prints the curve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load test; run with --ignored --release"]
async fn latency_streaming_by_size() {
    use tokio::io::AsyncReadExt;

    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    // 64 KiB (one chunk) → 64 MiB (1024 chunks): a 1000× span.
    let sizes = [
        64 * 1024usize,
        1024 * 1024,
        8 * 1024 * 1024,
        64 * 1024 * 1024,
    ];
    put(&client, B, "warm", &pattern(64 * 1024)).await; // warm pool/auth

    let mut ttfbs: Vec<(usize, Duration)> = Vec::new();
    let mut largest: Option<(usize, Duration, Duration)> = None;
    for &size in &sizes {
        let key = format!("stream/{size}");
        let body = pattern(size);

        let t = Instant::now();
        put(&client, B, &key, &body).await;
        let put_total = t.elapsed();

        // GET, timing the first streamed body byte vs. the full drain.
        let t0 = Instant::now();
        let out = client
            .get_object()
            .bucket(B)
            .key(&key)
            .send()
            .await
            .expect("get");
        let mut reader = out.body.into_async_read();
        let mut first = [0u8; 1];
        reader.read_exact(&mut first).await.expect("first byte");
        let ttfb = t0.elapsed();
        assert_eq!(first[0], body[0], "first streamed byte is correct");

        let mut drained = 1usize;
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            let n = reader.read(&mut buf).await.expect("read chunk");
            if n == 0 {
                break;
            }
            drained += n;
        }
        let total = t0.elapsed();
        assert_eq!(drained, size, "streamed the whole object");

        eprintln!(
            "size={:>9}  PUT total={:>7.2}  GET ttfb={:>6.2}  GET total={:>7.2}  (ms)",
            size,
            ms(put_total),
            ms(ttfb),
            ms(total),
        );
        ttfbs.push((size, ttfb));
        largest = Some((size, ttfb, total));
    }

    // For the largest object, the first byte must arrive well before the object finishes — proof it
    // streams rather than buffering the whole plaintext before the first byte (which would make
    // ttfb ≈ total). 64 MiB of decrypt+transfer dwarfs the fixed first-chunk latency.
    let (size, ttfb, total) = largest.unwrap();
    assert!(
        ttfb * 3 < total,
        "streaming: {size}-byte GET ttfb {ttfb:?} should be « total {total:?}"
    );

    // TTFB must not scale with size: the 64 MiB first byte lands within a small, size-independent
    // margin of the 64 KiB first byte (a buffered impl would be ~1000× larger). Generous absolute
    // cushion so machine jitter on a few-ms baseline can't flake it.
    let base = ttfbs.first().unwrap().1;
    assert!(
        ttfb < base * 8 + Duration::from_millis(30),
        "streaming: TTFB grew with size — {size}B ttfb {ttfb:?} vs smallest {base:?}"
    );
}

/// Sort a latency sample and print min / p50 / p90 / p99 / max in milliseconds (nearest-rank).
fn summarize(label: &str, mut samples: Vec<Duration>) {
    assert!(!samples.is_empty(), "no latency samples for {label}");
    samples.sort_unstable();
    let n = samples.len();
    // Nearest-rank: the ceil(p·n)-th value (1-indexed), clamped into range.
    let q = |p: f64| ms(samples[(((p * n as f64).ceil() as usize).max(1) - 1).min(n - 1)]);
    eprintln!(
        "{label:<28} n={n:<4} min={:6.2}  p50={:6.2}  p90={:6.2}  p99={:6.2}  max={:6.2}  (ms)",
        ms(samples[0]),
        q(0.50),
        q(0.90),
        q(0.99),
        ms(samples[n - 1]),
    );
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
