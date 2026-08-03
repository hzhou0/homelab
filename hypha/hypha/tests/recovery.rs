//! Namespace restore, pending-set rebuild, and their invariant violations.
//!
//! The write-mode gate is the load-bearing piece: a cached deployment runs **durable** semantics for
//! the whole of a bucket's restore, which is what makes "the cache holds nothing authoritative"
//! true rather than merely assumed, and what makes the restore safe to be purely additive.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::*;
use futures::StreamExt as _;
use hypha_core::meta;

const B: &str = "recov";

async fn wait_until<F, Fut>(ms: u64, what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + Duration::from_millis(ms);
    loop {
        if cond().await {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out after {ms}ms waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn raw_exists(h: &Harness, bucket: &str, key: &str) -> bool {
    h.raw_for_bucket(bucket)
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .is_ok()
}

async fn remote_present(h: &Harness, key: &str) -> bool {
    raw_exists(h, &h.remote_bucket(B), key).await
}

async fn marker_present(h: &Harness, key: &str) -> bool {
    raw_exists(h, &h.meta_bucket(B), key).await
}

async fn sync_marker_present(h: &Harness) -> bool {
    raw_exists(h, &h.meta_bucket(B), &meta::sync_marker_key()).await
}

/// Classify K's `<data>` entry: `None` ⇒ a live plaintext body, `Some(kind)` ⇒ a tombstone .
/// Panics if K has no entry at all — the callers here all know it does.
async fn data_class(h: &Harness, key: &str) -> Option<meta::TombKind> {
    let head = h
        .raw()
        .head_object()
        .bucket(h.cache_bucket(B))
        .key(key)
        .send()
        .await
        .expect("data head");
    meta::classify_entry(
        head.content_length().unwrap_or(0),
        head.e_tag().unwrap_or_default().trim_matches('"'),
    )
}

/// Every `<data>` object with its (size, ETag) — the shape a pass that must not touch client state
/// has to leave byte-identical.
async fn data_namespace(h: &Harness) -> HashMap<String, (i64, String)> {
    let mut out = HashMap::new();
    for key in raw_list(&h.raw(), &h.cache_bucket(B), None).await {
        let head = h
            .raw()
            .head_object()
            .bucket(h.cache_bucket(B))
            .key(&key)
            .send()
            .await
            .expect("data head");
        out.insert(
            key,
            (
                head.content_length().unwrap_or(0),
                head.e_tag()
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_string(),
            ),
        );
    }
    out
}

/// Drop the bucket's sync marker, so the next access classifies the namespace as untrusted and owes
/// a restore. Models the marker dying with a lost cache volume.
async fn drop_sync_marker(h: &Harness) {
    h.raw()
        .delete_object()
        .bucket(h.meta_bucket(B))
        .key(meta::sync_marker_key())
        .send()
        .await
        .expect("drop sync marker");
}

/// Destroy both cache projections, buckets and all — a cache volume loss.
///
/// The buckets go too, not just their objects: an absent projection and a projection with no sync
/// marker are the same evidence to the survey, and the restore provisions what it finds missing.
async fn destroy_cache(h: &Harness) {
    let raw = h.raw();
    for bucket in [h.cache_bucket(B), h.meta_bucket(B)] {
        for key in raw_list(&raw, &bucket, None).await {
            raw.delete_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .expect("wipe cache object");
        }
        raw.delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .expect("drop cache bucket");
    }
}

/// Seed `B` with enough remote objects that its restore cannot finish before the test's next
/// request, then take the cache volume out from under it and restart.
///
/// Startup resolves every bucket and queues the restore itself, so the window is no longer opened by
/// the first request to arrive — it is already open, and closing. Each seeded key costs the restore a
/// trailer read and two small writes; the tests below all assert something only a write *inside* the
/// window produces, so a window that closed early fails them loudly rather than passing vacuously.
async fn open_restore_window(h: &mut Harness) {
    let c = h.client();
    // Bounded: the suites run in parallel, and 400 requests in flight at once starves the others.
    futures::stream::iter((0..WINDOW_KEYS).map(|i| {
        let c = c.clone();
        let body = pattern(64);
        async move { put(&c, B, &format!("seed-{i:04}"), &body).await }
    }))
    .buffer_unordered(16)
    .collect::<Vec<_>>()
    .await;
    // Counted, not sampled at the lexicographically last key: a failed `reconcile_key` is retried on
    // a *later* pass, so the last key can land while a scattered few are still owed and the tests
    // below then list a namespace short of what they seeded.
    //
    // The budget covers the whole sweep (~40 s for `WINDOW_KEYS` in a debug build), not the tail of
    // it — a cache that acks promptly leaves all of the sweep inside this wait, where a slower one
    // overlaps most of it with the writes above.
    wait_until(90_000, "the whole seed reaches the remote", || async {
        raw_list(&h.raw_remote(), &h.remote_bucket(B), Some("seed-"))
            .await
            .len()
            == WINDOW_KEYS
    })
    .await;

    // Stopped first: taking the volume from a *live* ready bucket is the one thing the watchdog
    // halts on (I6), and it is not what these tests are about.
    h.stop_hypha().await;
    destroy_cache(h).await;
    h.start_hypha().await;
}

/// Sized so the restore takes seconds against a local MinIO, not milliseconds.
const WINDOW_KEYS: usize = 400;

// ── the write-mode gate ───────────────────────────────────────────────────────────────────────

/// A cached deployment runs **durable** semantics for the whole of a bucket's restore .
///
/// This is what makes the restore's premise true rather than hoped-for. A cached write would ack off
/// the cache and leave committed state in a namespace every reader is being told to ignore and the
/// restore is about to declare authoritative; a durable one commits on the remote first and settles
/// its own tombstone, which is exactly what the restore would have materialized.
#[tokio::test]
async fn writes_during_a_restore_commit_to_the_remote() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;

    destroy_cache(&h).await;
    h.restart_hypha().await;
    let c = h.client();

    // The PUT is the first request to touch the bucket, so it is the one that classifies the
    // namespace as untrusted and kicks the restore — and it runs inside the window it opened.
    let body = pattern(4096);
    put(&c, B, "k", &body).await;

    // Durable semantics: the remote holds it the instant the client is acked — no reconcile pass
    // has run, and none is owed.
    assert!(
        remote_present(&h, "k").await,
        "a write taken during a restore must be durable at ack, not owed to the reconcile sweep"
    );
    assert_eq!(
        data_class(&h, "k").await,
        Some(meta::TombKind::Evict),
        "a durable write settles an eviction tombstone; a live plaintext body here would be an \
         acked write inside an untrusted namespace"
    );
    assert!(
        !marker_present(&h, "k").await,
        "durable writes owe no pending marker"
    );
    assert_eq!(get_all(&c, B, "k").await, body);
}

/// A read committed to the remote must not be answered stale by a cached write landing after the
/// flip. The ticket — taken with the read's `Restoring` classification and held until the remote
/// answer is computed — is what forces that write to commit remote-first.
#[tokio::test]
async fn a_remote_read_across_the_flip_is_never_answered_stale() {
    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;

    let superseded = pattern(4096);
    put(&h.client(), B, "k", &superseded).await;
    wait_until(30_000, "the seed to reach the remote", || async {
        remote_present(&h, "k").await
    })
    .await;

    h.stop_hypha().await;
    destroy_cache(&h).await;
    // Park the restore on its opening `<data>` listing so the bucket stays `Restoring` until
    // released.
    let restoring = h
        .cache_faults()
        .pause_next_prefix(hyper::Method::GET, format!("/{}", h.cache_bucket(B)));
    let mut reading = h
        .remote_faults()
        .pause_next_prefix(hyper::Method::HEAD, format!("/{}/k", h.remote_bucket(B)));
    h.start_hypha().await;

    let reader = h.client();
    let read = tokio::spawn(async move { get_all(&reader, B, "k").await });
    // Reaching the remote proves the read took the ticket — a `Ready` bucket would have served from
    // the cache.
    tokio::time::timeout(Duration::from_secs(15), reading.reached())
        .await
        .expect("the read must resolve against the remote while the bucket restores");

    restoring.release();
    wait_until(30_000, "the namespace restore to complete", || async {
        sync_marker_present(&h).await
    })
    .await;

    // Past the flip, so cache-first is the norm — the ticket still out defers it to durable instead.
    let fresh = pattern(8192);
    put(&h.client(), B, "k", &fresh).await;

    reading.release();
    assert_eq!(
        read.await.expect("read task"),
        fresh,
        "a read that outlived the flip must not answer with the generation the write replaced"
    );
}

/// The regression test for the acked-write loss: with the cache untrusted, a *second* write to the
/// same key must not destroy the first.
///
/// The old path settled K from the remote before every write, which for a key the remote did not yet
/// hold deleted the acked cache body outright, failed the precondition, and left a pending marker
/// the sweep then reaped as an orphan — an acked write gone with nothing left to show it existed.
#[tokio::test]
async fn a_write_during_a_restore_survives_a_later_conditional_write() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;

    open_restore_window(&mut h).await;
    let c = h.client();

    let first = pattern(2048);
    let etag = put(&c, B, "k", &first).await;

    // A conditional write on the generation the client was just given. Under the old behaviour the
    // preceding settle had already deleted it, so this 412'd *and* took the object with it.
    let second = pattern_seeded(2048, 9);
    c.put_object()
        .bucket(B)
        .key("k")
        .if_match(&etag)
        .body(bytes_body(&second))
        .send()
        .await
        .expect("If-Match on the generation hypha just acked must succeed");

    assert_eq!(get_all(&c, B, "k").await, second);
    assert!(remote_present(&h, "k").await);
}

// ── R1: the namespace restore is additive ─────────────────────────────────────────────────────

/// The restore materializes only keys the cache has no entry for, so a delete taken during the
/// window is not resurrected by the pass that runs after it.
#[tokio::test]
async fn restore_does_not_resurrect_a_delete_taken_during_the_window() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();

    put(&c, B, "kept", &pattern(512)).await;
    put(&c, B, "gone", &pattern(512)).await;
    wait_until(10_000, "both keys reach the remote", || async {
        remote_present(&h, "kept").await && remote_present(&h, "gone").await
    })
    .await;

    // The cache volume dies; the remote objects do not. The delete below runs inside the window.
    open_restore_window(&mut h).await;
    let c = h.client();

    assert!(
        !sync_marker_present(&h).await,
        "the delete must run inside the restore window, not after it"
    );
    c.delete_object()
        .bucket(B)
        .key("gone")
        .send()
        .await
        .expect("delete during the restore window");

    wait_until(15_000, "the restore completes", || async {
        sync_marker_present(&h).await
    })
    .await;

    assert!(
        c.head_object().bucket(B).key("gone").send().await.is_err(),
        "the restore must not resurrect a key deleted during its own window"
    );
    assert!(c.head_object().bucket(B).key("kept").send().await.is_ok());
}

/// An entry the cache already holds is left exactly as it is — including the client pass-through the
/// eviction tombstone's metadata is the only surviving copy of (the remote's trailer carries
/// facts and nothing else). Settling every key from the remote, as the old path did, silently erased
/// `x-amz-meta-*` and the storage class on every key a restore touched.
#[tokio::test]
async fn restore_preserves_client_metadata_on_entries_it_finds() {
    let mut h = Harness::durable().await;
    h.create_bucket(B).await;
    let c = h.client();

    c.put_object()
        .bucket(B)
        .key("k")
        .metadata("colour", "octarine")
        .storage_class(aws_sdk_s3::types::StorageClass::StandardIa)
        .body(bytes_body(&pattern(256)))
        .send()
        .await
        .expect("put with metadata");

    // Trust is gone, the namespace is not — the restore finds an entry at every key.
    drop_sync_marker(&h).await;
    h.restart_hypha().await;
    let c = h.client();

    // Any access re-classifies and kicks the pass.
    let _ = c.head_object().bucket(B).key("k").send().await;
    wait_until(15_000, "the restore completes", || async {
        sync_marker_present(&h).await
    })
    .await;

    let head = c
        .head_object()
        .bucket(B)
        .key("k")
        .send()
        .await
        .expect("head after restore");
    assert_eq!(
        head.metadata()
            .and_then(|m| m.get("colour"))
            .map(String::as_str),
        Some("octarine"),
        "an additive restore must not re-derive an entry it found, erasing its pass-through"
    );
    assert_eq!(
        head.storage_class().map(|s| s.as_str()),
        Some("STANDARD_IA")
    );
}

/// A cache with no projections *at all* — a fresh volume pointed at a remote that has been served
/// before — is the same recovery as one whose sync marker died. The survey has no marker to read
/// either way, so the pass it picks is the same one; the restore just creates the projections it
/// finds missing before rebuilding into them.
///
/// Both modes, because the survey probes a durable deployment with the sync-marker HEAD alone: a
/// backend that answered a missing *bucket* with anything but the key-level 404 would classify it as
/// an unreadable cache rather than an untrusted one, and durable mode has no second probe to catch it.
async fn brand_new_cache_restores_every_remote_bucket(mode: hypha_core::config::Mode) {
    let other = "recov-second";
    let mut h = Harness::with_mode(mode).await;
    h.create_bucket(B).await;
    h.create_bucket(other).await;
    let c = h.client();

    let body = pattern(4096);
    put(&c, B, "alpha", &body).await;
    put(&c, other, "beta", &body).await;
    wait_until(15_000, "both keys reach the remote", || async {
        remote_present(&h, "alpha").await && raw_exists(&h, &h.remote_bucket(other), "beta").await
    })
    .await;

    h.stop_hypha().await;
    for bucket in [
        h.cache_bucket(B),
        h.meta_bucket(B),
        h.cache_bucket(other),
        h.meta_bucket(other),
        h.gc_bucket(),
    ] {
        drop_backend_bucket(&h, &bucket).await;
    }
    h.start_hypha().await;
    h.await_ready().await;
    let c = h.client();

    assert_eq!(get_all(&c, B, "alpha").await, body);
    assert_eq!(get_all(&c, other, "beta").await, body);

    wait_until(30_000, "both namespaces to be restored", || async {
        sync_marker_present(&h).await
            && raw_exists(&h, &h.meta_bucket(other), &meta::sync_marker_key()).await
    })
    .await;

    assert_eq!(h.bucket_status(B), hypha::BucketStatus::Ready);
    assert_eq!(h.bucket_status(other), hypha::BucketStatus::Ready);
    assert!(
        data_class(&h, "alpha").await.is_some(),
        "the restore must leave a tombstone at every remote key"
    );
    assert!(
        h.raw()
            .head_bucket()
            .bucket(h.gc_bucket())
            .send()
            .await
            .is_ok(),
        "the recency ring's bucket is the GC actor's to recreate"
    );
}

#[tokio::test]
async fn a_brand_new_cache_restores_every_remote_bucket_cached() {
    brand_new_cache_restores_every_remote_bucket(hypha_core::config::Mode::Cached).await;
}

#[tokio::test]
async fn a_brand_new_cache_restores_every_remote_bucket_durable() {
    brand_new_cache_restores_every_remote_bucket(hypha_core::config::Mode::Durable).await;
}

// ── R2: the pending-set rebuild touches markers and nothing else ──────────────────────────────

/// The rebuild's premise is that the namespace is authoritative, so it may re-derive the *index*
/// and nothing else. Asserted structurally: `<data>` is byte-identical across the pass, and exactly
/// the keys that owe the remote something gain markers.
#[tokio::test]
async fn pending_rebuild_writes_markers_and_never_touches_data() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();

    // Settled: uploaded and no longer pending.
    put(&c, B, "settled", &pattern(512)).await;
    wait_until(10_000, "the settled key reaches the remote", || async {
        remote_present(&h, "settled").await
    })
    .await;

    // Owed: acked on the cache, with its marker deleted out of band so only a rebuild can find it.
    // This is precisely the state a crash between the body write and the marker write leaves.
    put(&c, B, "owed", &pattern(1024)).await;
    h.raw()
        .delete_object()
        .bucket(h.meta_bucket(B))
        .key("owed")
        .send()
        .await
        .expect("drop the pending marker");
    let before = data_namespace(&h).await;

    // No clean marker (an ungraceful stop), sync marker intact ⇒ the pending rebuild, not a restore.
    h.kill_hypha().await;
    h.start_hypha().await;

    wait_until(
        15_000,
        "the rebuild re-raises the missing marker",
        || async { marker_present(&h, "owed").await || remote_present(&h, "owed").await },
    )
    .await;
    wait_until(
        15_000,
        "the re-raised marker drains to the remote",
        || async { remote_present(&h, "owed").await },
    )
    .await;

    let after = data_namespace(&h).await;
    assert_eq!(
        before, after,
        "the pending rebuild may write markers only — `<data>` must come through untouched"
    );
}

// ── invariants ────────────────────────────────────────────────────────────────────────────────

/// Poll for the halt marker, which is recorded on the **remote** so it outlives the cache .
async fn halt_marker_present(h: &Harness) -> bool {
    raw_exists(h, &h.remote_bucket(B), &meta::halt_marker_key()).await
}

/// Assert the process ended on an invariant violation, recorded it, and that every run after it
/// ends the same way without re-deriving anything — the crashloop an operator alerts on.
async fn assert_halted(h: &mut Harness) {
    let status = h.child().wait_exit(Duration::from_secs(30)).await;
    assert_eq!(
        status.code(),
        Some(hypha::EXIT_INVARIANT_VIOLATION),
        "an invariant violation must end the process"
    );
    assert!(
        halt_marker_present(h).await,
        "the violation must be recorded on the remote before the process exits, or the next run \
         would serve the same data"
    );
    h.start_hypha_expecting_exit();
    let status = h.child().wait_exit(Duration::from_secs(30)).await;
    assert_eq!(
        status.code(),
        Some(hypha::EXIT_INVARIANT_VIOLATION),
        "a run that finds a halt marker must exit before serving anything"
    );
}

/// A crash can lose the asynchronous marker after a cached delete has committed locally. With the
/// sync marker intact, R2 trusts that absence and re-indexes the remote-only key as a pending delete.
#[tokio::test]
async fn remote_only_key_on_a_ready_bucket_rebuilds_the_delete() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;
    let key = "markerless-delete";
    put(&h.client(), B, key, &pattern(64)).await;
    wait_until(
        15_000,
        "the original generation reaches the remote",
        || async { remote_present(&h, key).await && !marker_present(&h, key).await },
    )
    .await;

    // The delete commit landed but its marker did not: the exact state left by a crash between the
    // local DELETE and the queue handoff.
    h.raw()
        .delete_object()
        .bucket(h.cache_bucket(B))
        .key(key)
        .send()
        .await
        .expect("commit markerless cache delete");

    h.kill_hypha().await;
    h.start_hypha().await;
    wait_until(
        15_000,
        "R2 re-indexes and propagates the delete",
        || async { !remote_present(&h, key).await && !marker_present(&h, key).await },
    )
    .await;
    let err = h
        .client()
        .get_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect_err("the recovered delete remains authoritative");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("NoSuchKey"));
}

/// **I2** — an eviction tombstone whose remote object is gone.
///
/// The remote lost bytes hypha reported as committed, and the tombstone is the only surviving record
/// that they existed. The tombstone here is minted the ordinary way: by a durable-semantics write
/// taken during a restore window.
#[tokio::test]
async fn eviction_tombstone_without_its_remote_object_halts() {
    let mut h = Harness::cached_subprocess().await;
    h.create_bucket(B).await;

    // Force the restore window, then write through it — the write commits on the remote and settles
    // an eviction tombstone in the cache.
    open_restore_window(&mut h).await;
    put(&h.client(), B, "k", &pattern(256)).await;
    assert_eq!(data_class(&h, "k").await, Some(meta::TombKind::Evict));
    wait_until(15_000, "the restore completes", || async {
        sync_marker_present(&h).await
    })
    .await;

    // The remote loses the object the tombstone stands for.
    h.raw_remote()
        .delete_object()
        .bucket(h.remote_bucket(B))
        .key("k")
        .send()
        .await
        .expect("drop the remote object");

    h.kill_hypha().await;
    h.start_hypha_expecting_exit();
    assert_halted(&mut h).await;
}

/// **I1**, and the classification rule in one: with *both* markers absent the bucket is classified
/// as a restore, and a restore may not find a live plaintext body.
///
/// A cached write acked into a namespace that is not authoritative is the exact state the write-mode
/// gate exists to prevent, so one here means the gate leaked. Note what the rebuild would have done
/// instead — quietly raised a marker — which is why "both markers absent ⇒ restore" is a
/// classification and not a preference.
#[tokio::test]
async fn a_cached_body_in_an_untrusted_namespace_halts() {
    let mut h = Harness::cached_subprocess().await;
    h.create_bucket(B).await;

    // A live, acked, not-yet-uploaded cache body — the ordinary cached-mode steady state.
    put(&h.client(), B, "k", &pattern(256)).await;
    assert_eq!(data_class(&h, "k").await, None, "a live plaintext body");

    // Now take namespace trust away without taking the body with it, and stop ungracefully so the
    // clean marker is absent too. Both markers gone ⇒ restore ⇒ the body is inadmissible.
    h.kill_hypha().await;
    drop_sync_marker(&h).await;
    h.start_hypha_expecting_exit();

    // Any access re-classifies the bucket and dispatches the pass.
    let c = h.client();
    for _ in 0..40 {
        if c.head_object().bucket(B).key("k").send().await.is_err() && halt_marker_present(&h).await
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_halted(&mut h).await;
}

/// **I6** — the cache volume disappears under a *running* process.
///
/// Startup resolves a bucket's readiness once and holds it for the run, which is sound only while
/// the volume stays. A `Ready` bucket whose cache is gone answers 404 for objects that exist, and
/// says nothing about it — cache-absent *is* the authoritative 404 there. The watchdog is the only
/// thing still asking, and its answer is a halt: the run cannot re-derive what it already served.
#[tokio::test]
async fn a_sync_marker_vanishing_mid_run_halts() {
    let mut h = Harness::cached_subprocess().await;
    h.create_bucket(B).await;
    put(&h.client(), B, "k", &pattern(256)).await;

    // The volume goes, and nothing in the request path would ever notice: reads of `k` still hit a
    // live cache body, and reads of anything else were 404 before and after.
    drop_sync_marker(&h).await;

    assert_halted(&mut h).await;
}

/// **I6 from the other side** — the marker queue, rather than the watchdog.
///
/// A marker whose bucket is gone is *dropped*, deliberately: it can never land, and retrying it would
/// withhold every other bucket's clean marker for the rest of the run . That drop is gated on the
/// state map agreeing the bucket is gone, and this is the case where it does not: the `<meta>`
/// projection vanished under a live bucket, so dropping the marker would silently shorten a pending set
/// this run still vouches for. The watchdog would eventually see the same loss, so its interval is
/// pinned out of reach here — the point is that the queue does not wait for it.
///
/// **MinIO only**, because the signal is the backend's: SeaweedFS answers a PUT into a bucket that
/// does not exist by creating it (tests/backend.rs), so the marker simply lands and there is nothing
/// for the queue to notice. The loss itself is still caught there — by the watchdog, on its own
/// interval, which `a_sync_marker_vanishing_mid_run_halts` covers on both backends. What is
/// backend-dependent is only how fast, and this is the fast path.
#[tokio::test]
async fn a_marker_owed_to_a_live_bucket_whose_projection_vanished_halts() {
    if external_cache_backend().is_some() {
        return;
    }
    let mut h = Harness::builder(hypha_core::config::Mode::Cached)
        .subprocess()
        .with_faults()
        // Long enough that only the marker path can raise the violation; the watchdog's own detection
        // is `a_sync_marker_vanishing_mid_run_halts`.
        .tune(|c| c.volume_watch_interval_ms = 600_000)
        .start()
        .await;
    h.create_bucket(B).await;

    // Hold the marker write at the proxy: the body is committed and acked, and the obligation is in
    // flight, which is the only window in which the projection can vanish underneath one.
    let mut marker = h
        .cache_faults()
        .pause_next(hyper::Method::PUT, format!("/{}/k", h.meta_bucket(B)));
    put(&h.client(), B, "k", &pattern(256)).await;
    tokio::time::timeout(Duration::from_secs(10), marker.reached())
        .await
        .expect("the marker write was never attempted");

    // The volume goes, not the bucket: nothing has deleted it, so the state map still calls it live.
    drop_backend_bucket(&h, &h.meta_bucket(B)).await;
    marker.release();

    assert_halted(&mut h).await;
    let recorded = get_all(
        &h.raw_remote(),
        &h.remote_bucket(B),
        &meta::halt_marker_key(),
    )
    .await;
    let recorded =
        String::from_utf8(recorded).expect("the halt marker is plain text for an operator");
    assert!(
        recorded.contains("invariant: cache-volume-lost"),
        "the recorded violation must name the invariant: {recorded}"
    );
    assert!(
        recorded.contains("an owed marker's <meta> projection is gone"),
        "and it must be the marker queue's own detection, not the watchdog's: {recorded}"
    );
}

/// **I6 (`CacheVolumeLost`) from a third side** — `<data>`, rather than the sync marker or the
/// marker queue.
///
/// A partial loss that leaves `<meta>` standing keeps the sync marker with it, so the survey calls
/// the bucket synced and dispatches R2 rather than a restore. R2 is entitled to read cache absence
/// as the client's own deletes, so provisioning the missing projection back — which is what every
/// other phase's absence legitimately wants — would hand it an empty namespace and it would raise a
/// delete marker for every key the remote still holds. The loss must halt at the provisioning step,
/// before the pass that would launder it into client intent.
#[tokio::test]
async fn a_data_projection_vanishing_under_a_synced_bucket_halts() {
    let mut h = Harness::cached_subprocess().await;
    h.create_bucket(B).await;
    let c = h.client();
    for i in 0..5 {
        put(&c, B, &format!("k{i}"), &pattern(256)).await;
    }
    // Every one of them: the sweep orders nothing across keys, so the last put is not the last
    // uploaded, and a key still owed at the kill would be legitimately absent from the remote below.
    wait_until(15_000, "the keys reach the remote", || async {
        for i in 0..5 {
            if !remote_present(&h, &format!("k{i}")).await {
                return false;
            }
        }
        true
    })
    .await;

    // Ungraceful, so no clean marker: the next run owes R2. `<meta>` — and the sync marker in it —
    // survives, which is what makes this a *synced* bucket missing a projection.
    h.kill_hypha().await;
    drop_backend_bucket(&h, &h.cache_bucket(B)).await;
    h.start_hypha_expecting_exit();

    assert_halted(&mut h).await;
    let recorded = get_all(
        &h.raw_remote(),
        &h.remote_bucket(B),
        &meta::halt_marker_key(),
    )
    .await;
    let recorded =
        String::from_utf8(recorded).expect("the halt marker is plain text for an operator");
    assert!(
        recorded.contains("invariant: cache-volume-lost"),
        "the recorded violation must name the invariant: {recorded}"
    );
    assert!(
        recorded.contains("the <data> projection of a ready bucket is absent"),
        "and it must name the projection that went: {recorded}"
    );

    for i in 0..5 {
        assert!(
            remote_present(&h, &format!("k{i}")).await,
            "halting is only worth anything if it beats the deletes: k{i} is gone from the remote"
        );
    }
}

/// hypha's own keys live in the remote bucket alongside client objects (the halt marker), and
/// every remote key a restore-time LIST emits goes to a trailer read. An unfiltered one would be
/// reported as a foreign object — hypha halting on its own bookkeeping.
#[tokio::test]
async fn reserved_remote_keys_are_invisible_and_harmless() {
    let mut h = Harness::durable().await;
    h.create_bucket(B).await;
    let c = h.client();
    put(&c, B, "visible", &pattern(64)).await;
    wait_until(10_000, "the client key reaches the remote", || async {
        remote_present(&h, "visible").await
    })
    .await;

    // A reserved-prefix key that is not the halt marker: the filter must be the control byte, not a
    // name match, or the next reserved key added would slip through.
    let reserved = format!("{c}{c}z", c = meta::CTRL as char);
    h.raw_remote()
        .put_object()
        .bucket(h.remote_bucket(B))
        .key(&reserved)
        .body(bytes_body(b"hypha bookkeeping, no trailer"))
        .send()
        .await
        .expect("plant a reserved remote key");

    // Serve the bucket from the remote, where the reserved key is. Durable mode keeps only
    // tombstones in `<data>`, so dropping trust alone is a legitimate restore here.
    drop_sync_marker(&h).await;
    h.restart_hypha().await;
    let c = h.client();

    let listed = c
        .list_objects_v2()
        .bucket(B)
        .send()
        .await
        .expect("LIST while restoring must not choke on hypha's own keys");
    let keys: Vec<&str> = listed.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, ["visible"], "reserved keys must never reach a client");
}

/// **The phase-5 LIST token boundary :** a page read while the bucket is `Restoring` is served
/// from the remote and carries hypha's own cursor — a plain key position, never the remote's opaque
/// continuation token — so the very next page, served from the cache after the flip to `Ready`,
/// resumes without gaps or duplicates and sees the writes that landed after the flip.
#[tokio::test]
async fn list_pagination_spans_the_restore_flip() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;

    open_restore_window(&mut h).await;
    let c = h.client();

    // The in-process harness returns before startup's `resolve_all` publishes the gate, so wait for
    // it — via the authoritative state, not a backend artifact — and assert the restore window is
    // still open when the first page is read, or the test proves nothing about the boundary.
    wait_until(30_000, "the restore window to open", || async {
        h.bucket_status(B) == hypha::BucketStatus::Restoring
    })
    .await;
    assert_eq!(
        h.bucket_status(B),
        hypha::BucketStatus::Restoring,
        "the first page must be read inside the restore window"
    );

    // Page 1, served from the remote.
    let page1 = c
        .list_objects_v2()
        .bucket(B)
        .max_keys(50)
        .send()
        .await
        .expect("first list page");
    assert_eq!(
        page1.is_truncated(),
        Some(true),
        "a 400-key namespace cannot fit one page of 50"
    );
    let mut all: Vec<String> = page1
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect();
    let mut token = page1.next_continuation_token().map(str::to_string);

    // The flip: the restore completes and the cache becomes authoritative. Waited on via the gate.
    wait_until(60_000, "the restore to flip the bucket Ready", || async {
        h.bucket_status(B) == hypha::BucketStatus::Ready
    })
    .await;

    // A cached-mode write after the flip; it sorts last, so the remaining pages must surface it.
    put(&c, B, "zz-append", &pattern(32)).await;

    // The continuation, now served from the cache.
    for _ in 0..(WINDOW_KEYS + 8) {
        let mut req = c.list_objects_v2().bucket(B).max_keys(50);
        if let Some(t) = &token {
            req = req.continuation_token(t.clone());
        }
        let page = req.send().await.expect("continuation list page");
        all.extend(
            page.contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string)),
        );
        match page.next_continuation_token() {
            Some(t) if page.is_truncated() == Some(true) => token = Some(t.to_string()),
            _ => {
                token = None;
                break;
            }
        }
    }
    assert!(token.is_none(), "pagination must terminate");

    let mut expected: Vec<String> = (0..WINDOW_KEYS).map(|i| format!("seed-{i:04}")).collect();
    expected.push("zz-append".to_string());
    assert_eq!(
        all, expected,
        "a remote page then cache pages must cover every key exactly once, in order"
    );
}
