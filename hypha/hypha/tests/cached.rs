//! Phase-4 exit: cached mode over real MinIO (§7/§8). Writes ack after the cache write plus a
//! bare-`K` pending marker; the reconcile sweep trails them onto the remote. Covers the marker +
//! reconcile upload, cached delete propagation (mask-then-propagate), conditional-write
//! linearization on the cache, `Content-MD5` validation, the marker scan staying `O(pending)`
//! (evicted keys untouched), and rehydrate on a tombstoned read — single-part back into K and a
//! composite into the shadow body.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use common::*;
use hypha_core::meta;

const B: &str = "cached";

// ── small polling / inspection helpers ────────────────────────────────────────────────────────

/// Poll `cond` every 50 ms until it holds or `ms` elapses (then panic with `what`).
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

/// Does a raw object exist at `key` in `bucket` (bypassing hypha)?
async fn raw_exists(h: &Harness, bucket: &str, key: &str) -> bool {
    h.raw()
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .is_ok()
}

/// The pending marker for `key` lives at bare `K` in `<meta>` (§6).
async fn marker_present(h: &Harness, client_bucket: &str, key: &str) -> bool {
    raw_exists(h, &h.meta_bucket(client_bucket), key).await
}

async fn remote_present(h: &Harness, client_bucket: &str, key: &str) -> bool {
    raw_exists(h, &h.remote_bucket(client_bucket), key).await
}

/// Classify the `<data>` object at `key`: `None` ⇒ live body, `Some(kind)` ⇒ a tombstone (§6).
async fn data_class(h: &Harness, client_bucket: &str, key: &str) -> Option<meta::TombKind> {
    let head = h
        .raw()
        .head_object()
        .bucket(h.cache_bucket(client_bucket))
        .key(key)
        .send()
        .await
        .expect("data head");
    let size = head.content_length().unwrap_or(0);
    let etag = head
        .e_tag()
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();
    meta::classify_entry(size, &etag)
}

// ── tests ─────────────────────────────────────────────────────────────────────────────────────

/// A cached PUT acks after the cache write, serves from the cache immediately, and the reconcile
/// sweep uploads it to the remote and clears the marker — the body staying live in the cache.
#[tokio::test]
async fn cached_put_serves_from_cache_and_reconciles() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(40_000);

    let etag = put(&c, B, "obj", &body).await;
    assert_eq!(etag, md5_hex(&body), "cached ETag is MD5(plaintext)");

    // Served from the live cache body before any reconcile could run.
    assert_eq!(get_all(&c, B, "obj").await, body);
    assert!(marker_present(&h, B, "obj").await, "marker written on ack");
    assert!(
        data_class(&h, B, "obj").await.is_none(),
        "cache holds a live body"
    );
    assert!(!remote_present(&h, B, "obj").await, "remote trails the ack");

    wait_until(6000, "reconcile uploads and clears the marker", || async {
        remote_present(&h, B, "obj").await && !marker_present(&h, B, "obj").await
    })
    .await;
    assert!(
        data_class(&h, B, "obj").await.is_none(),
        "body stays live after reconcile"
    );
    assert_eq!(
        get_all(&c, B, "obj").await,
        body,
        "still readable post-reconcile"
    );
}

/// A cached delete masks K immediately (404 to clients) and the reconcile sweep propagates it to the
/// remote, clearing both the tombstone and the marker.
#[tokio::test]
async fn cached_delete_masks_then_propagates() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(20_000);

    put(&c, B, "d", &body).await;
    wait_until(6000, "initial upload to remote", || async {
        remote_present(&h, B, "d").await && !marker_present(&h, B, "d").await
    })
    .await;

    c.delete_object()
        .bucket(B)
        .key("d")
        .send()
        .await
        .expect("delete");
    let got = c.get_object().bucket(B).key("d").send().await;
    assert_eq!(
        sdk_err_code(&got.unwrap_err()).as_deref(),
        Some("NoSuchKey")
    );
    assert_eq!(data_class(&h, B, "d").await, Some(meta::TombKind::Delete));
    assert!(
        marker_present(&h, B, "d").await,
        "delete leaves a pending marker"
    );

    wait_until(6000, "delete propagates to the remote", || async {
        !remote_present(&h, B, "d").await
            && !raw_exists(&h, &h.cache_bucket(B), "d").await
            && !marker_present(&h, B, "d").await
    })
    .await;
    let got = c.get_object().bucket(B).key("d").send().await;
    assert_eq!(
        sdk_err_code(&got.unwrap_err()).as_deref(),
        Some("NoSuchKey")
    );
}

/// Conditional writes linearize on the cache in cached mode (§4): `If-None-Match: *` creates only
/// when absent, `If-Match` requires the current ETag — hypha's own semantics, not the backend's.
#[tokio::test]
async fn cached_conditional_put_linearizes() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let v1 = pattern(1024);

    // Create-if-absent succeeds, then fails once the key exists.
    let e1 = c
        .put_object()
        .bucket(B)
        .key("c")
        .body(bytes_body(&v1))
        .content_length(v1.len() as i64)
        .if_none_match("*")
        .send()
        .await
        .expect("create-if-absent")
        .e_tag()
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();
    assert_eq!(e1, md5_hex(&v1));

    let dup = c
        .put_object()
        .bucket(B)
        .key("c")
        .body(bytes_body(&v1))
        .content_length(v1.len() as i64)
        .if_none_match("*")
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&dup.unwrap_err()).as_deref(),
        Some("PreconditionFailed"),
        "If-None-Match:* must reject an existing key"
    );

    // If-Match against the wrong ETag is rejected; against the right one it proceeds.
    let bad = c
        .put_object()
        .bucket(B)
        .key("c")
        .body(bytes_body(&v1))
        .content_length(v1.len() as i64)
        .if_match("\"00000000000000000000000000000000\"")
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&bad.unwrap_err()).as_deref(),
        Some("PreconditionFailed")
    );

    let v2 = pattern(2048);
    c.put_object()
        .bucket(B)
        .key("c")
        .body(bytes_body(&v2))
        .content_length(v2.len() as i64)
        .if_match(format!("\"{e1}\""))
        .send()
        .await
        .expect("If-Match on the current ETag");
    assert_eq!(get_all(&c, B, "c").await, v2);
}

/// A cached PUT with a wrong `Content-MD5` is rejected `BadDigest` by the cache and nothing lands.
#[tokio::test]
async fn cached_put_rejects_bad_content_md5() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(4096);

    let bad = c
        .put_object()
        .bucket(B)
        .key("m")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .content_md5(base64_md5(b"a different body"))
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&bad.unwrap_err()).as_deref(),
        Some("BadDigest")
    );

    let got = c.get_object().bucket(B).key("m").send().await;
    assert_eq!(
        sdk_err_code(&got.unwrap_err()).as_deref(),
        Some("NoSuchKey"),
        "a rejected PUT leaves the key absent"
    );
}

/// The reconcile marker scan is `O(pending)`, not `O(evicted)`: an eviction tombstone + twin with no
/// marker is left completely untouched by the sweep, while a pending key beside it is uploaded.
#[tokio::test]
async fn reconcile_scans_pending_only_not_evicted() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();

    // A live-body pending key.
    let live = pattern(8192);
    put(&c, B, "pending", &live).await;

    // An *evicted* key: put + reconcile it to the remote, then plant an eviction tombstone + twin
    // over it (as GC would) and confirm no marker remains. The sweep must never re-touch it.
    let cold = pattern(9000);
    put(&c, B, "evicted", &cold).await;
    wait_until(6000, "evicted key reaches the remote", || async {
        remote_present(&h, B, "evicted").await && !marker_present(&h, B, "evicted").await
    })
    .await;
    plant_eviction_tombstone(&h, "evicted", &cold).await;
    let twin = meta::Facts {
        client_etag: md5_hex(&cold),
        plen: cold.len() as u64,
        mtime_ms: 1,
    }
    .twin_key("evicted")
    .unwrap();

    wait_until(6000, "pending key uploaded and cleared", || async {
        remote_present(&h, B, "pending").await && !marker_present(&h, B, "pending").await
    })
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    // The evicted key's projections are exactly as planted — the sweep never enumerated them.
    assert_eq!(
        data_class(&h, B, "evicted").await,
        Some(meta::TombKind::Evict)
    );
    assert!(
        raw_exists(&h, &h.meta_bucket(B), &twin).await,
        "twin untouched"
    );
    assert!(
        !marker_present(&h, B, "evicted").await,
        "no marker for an evicted key"
    );
    assert_eq!(get_all(&c, B, "evicted").await, cold);
}

/// A tombstoned single-part read rehydrates back into K: served from the remote, then the plaintext
/// lands as a live cache body so the next read is a cache hit.
#[tokio::test]
async fn rehydrate_single_part_on_read() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(30_000);

    put(&c, B, "r", &body).await;
    wait_until(6000, "upload to remote", || async {
        remote_present(&h, B, "r").await && !marker_present(&h, B, "r").await
    })
    .await;
    plant_eviction_tombstone(&h, "r", &body).await;
    assert_eq!(data_class(&h, B, "r").await, Some(meta::TombKind::Evict));

    // The tombstoned GET serves from the remote and kicks the rehydrate.
    assert_eq!(get_all(&c, B, "r").await, body);

    wait_until(6000, "single-part rehydrate lands at K", || async {
        data_class(&h, B, "r").await.is_none()
    })
    .await;
    assert_eq!(get_all(&c, B, "r").await, body, "next read is a cache hit");
}

/// A completed multipart object is tombstoned in both modes; a cached read rehydrates the composite
/// into its shadow body, and the following read is served from that shadow.
#[tokio::test]
async fn rehydrate_composite_into_shadow() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "big/composite";
    let p1 = pattern_seeded(MIN_PART, 1);
    let p2 = pattern_seeded(MIN_PART, 2);
    let whole: Vec<u8> = p1.iter().chain(&p2).copied().collect();

    let up = create_mpu(&c, B, key).await;
    let e1 = upload_part(&c, B, key, &up, 1, &p1).await;
    let e2 = upload_part(&c, B, key, &up, 2, &p2).await;
    complete_mpu(&c, B, key, &up, &[(1, e1), (2, e2)]).await;

    // Complete tombstones K (both modes, §7); the composite lives on the remote.
    assert_eq!(data_class(&h, B, key).await, Some(meta::TombKind::Evict));
    let shadow = meta::shadow_key(key);

    // First read: served from the remote, rehydrate into the shadow kicked.
    assert_eq!(get_all(&c, B, key).await, whole);
    wait_until(6000, "composite rehydrate lands in the shadow", || async {
        raw_exists(&h, &h.meta_bucket(B), &shadow).await
    })
    .await;
    // K's tombstone and twin are untouched by composite rehydration.
    assert_eq!(data_class(&h, B, key).await, Some(meta::TombKind::Evict));
    // Second read is served from the shadow (still correct, whole + ranged).
    assert_eq!(get_all(&c, B, key).await, whole);
    assert_eq!(
        get_range(&c, B, key, 10, 10 + MIN_PART as u64).await,
        whole[10..=10 + MIN_PART]
    );
}

/// Linearizability on the cached write path (§4): many racing `If-None-Match:*` creates on one key
/// resolve to exactly one winner, the losers 412 — the conditional PUTs serialize on the write lock
/// even though unconditional cached PUTs take none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_conditional_creates_linearize_under_contention() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let key = "hotkey";

    let tasks = (0..24usize).map(|w| {
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
    for r in futures::future::join_all(tasks).await {
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

/// A client body colliding with a reserved 16-byte tombstone sentinel is rejected at the write path,
/// and any prior version at K survives — nothing lands (review finding 1).
#[tokio::test]
async fn cached_put_rejects_reserved_sentinel_body() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();

    let good = pattern(64);
    put(&c, B, "s", &good).await;

    for sentinel in [
        meta::DELETE_SENTINEL,
        meta::EVICT_SENTINEL,
        meta::TRANSIT_SENTINEL,
    ] {
        let bad = c
            .put_object()
            .bucket(B)
            .key("s")
            .body(bytes_body(&sentinel))
            .content_length(16)
            .send()
            .await;
        assert_eq!(
            sdk_err_code(&bad.unwrap_err()).as_deref(),
            Some("InvalidRequest"),
            "a body equal to a reserved sentinel must be rejected"
        );
    }
    // The prior version is untouched — the rejected writes never landed.
    assert_eq!(get_all(&c, B, "s").await, good);
}

/// After K is overwritten by a *new* composite, the stale shadow from the previous generation is not
/// served: the read returns the new bytes and re-rehydrates (review finding 2).
#[tokio::test]
async fn composite_overwrite_does_not_serve_stale_shadow() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "gen/composite";

    // Generation 1 → rehydrate into the shadow.
    let a = two_part_composite(&c, key, 11, 12).await;
    assert_eq!(get_all(&c, B, key).await, a);
    let shadow = meta::shadow_key(key);
    wait_until(6000, "gen-1 shadow lands", || async {
        raw_exists(&h, &h.meta_bucket(B), &shadow).await
    })
    .await;

    // Generation 2 at the same key — leaves the gen-1 shadow in place.
    let b = two_part_composite(&c, key, 21, 22).await;

    // The gen-1 shadow still carries the old cetag — a correct read must not hit it.
    assert_eq!(
        get_all(&c, B, key).await,
        b,
        "must serve gen-2, not the stale shadow"
    );
    // And it re-rehydrates to gen-2, so subsequent shadow-served reads are also correct.
    wait_until(6000, "gen-2 shadow served correctly", || async {
        get_all(&c, B, key).await == b
    })
    .await;
}

/// Concurrent reads of a freshly-completed (tombstoned) composite all return correct bytes; the
/// rehydrate coalesces on the write lock instead of each read re-downloading (review finding 3).
#[tokio::test]
async fn composite_concurrent_reads_are_correct() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "concurrent/composite";
    let whole = two_part_composite(&c, key, 31, 32).await;

    let tasks = (0..8).map(|_| {
        let cc = h.client();
        let want = whole.clone();
        let k = key.to_string();
        tokio::spawn(async move { assert_eq!(get_all(&cc, B, &k).await, want) })
    });
    for t in futures::future::join_all(tasks).await {
        t.expect("reader panicked");
    }
    wait_until(6000, "shadow converges", || async {
        raw_exists(&h, &h.meta_bucket(B), &meta::shadow_key(key)).await
    })
    .await;
    assert_eq!(get_all(&c, B, key).await, whole);
}

/// A client write to a key with a rehydrate in flight supersedes it: the write's bytes stand and the
/// evicted generation never resurfaces. The write cancels K's background transition (§8) instead of
/// queuing behind its fetch, and a cancelled — or merely late — rehydrate cannot land over the new
/// body anyway: its land CAS is conditional on the evict sentinel the write already replaced.
#[tokio::test]
async fn cached_write_supersedes_in_flight_rehydrate() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let old = pattern(400_000);
    let new = pattern_seeded(400_000, 77);

    put(&c, B, "sup", &old).await;
    wait_until(6000, "upload to remote", || async {
        remote_present(&h, B, "sup").await && !marker_present(&h, B, "sup").await
    })
    .await;
    plant_eviction_tombstone(&h, "sup", &old).await;

    assert_eq!(get_all(&c, B, "sup").await, old);
    put(&c, B, "sup", &new).await;
    assert_eq!(get_all(&c, B, "sup").await, new);

    // Give any surviving rehydrate every chance to land before re-checking.
    tokio::time::sleep(Duration::from_millis(750)).await;
    assert_eq!(
        get_all(&c, B, "sup").await,
        new,
        "a rehydrate of the evicted generation must never land over a newer write"
    );
    assert!(
        data_class(&h, B, "sup").await.is_none(),
        "K is the newly written live body"
    );
}

/// Upload and complete a two-part composite at `key`; returns its whole plaintext.
async fn two_part_composite(c: &aws_sdk_s3::Client, key: &str, seed1: u8, seed2: u8) -> Vec<u8> {
    let p1 = pattern_seeded(MIN_PART, seed1);
    let p2 = pattern_seeded(MIN_PART, seed2);
    let up = create_mpu(c, B, key).await;
    let e1 = upload_part(c, B, key, &up, 1, &p1).await;
    let e2 = upload_part(c, B, key, &up, 2, &p2).await;
    complete_mpu(c, B, key, &up, &[(1, e1), (2, e2)]).await;
    p1.iter().chain(&p2).copied().collect()
}

/// Plant an eviction tombstone over `key` (as GC would, §8): the remote must already hold the
/// ciphertext. Overwrites the `<data>` body with the evict sentinel + facts metadata, and writes the
/// facts twin — leaving the key resolvable from the remote and rehydratable on read.
async fn plant_eviction_tombstone(h: &Harness, key: &str, body: &[u8]) {
    let cetag = md5_hex(body);
    let mut md = HashMap::new();
    md.insert(meta::TOMB.to_string(), meta::TOMB_EVICT.to_string());
    md.insert(meta::PLEN.to_string(), body.len().to_string());
    md.insert(meta::CETAG.to_string(), cetag.clone());
    md.insert(meta::MTIME.to_string(), "1".to_string());
    md.insert(meta::SCLASS.to_string(), meta::STANDARD.to_string());
    h.raw()
        .put_object()
        .bucket(h.cache_bucket(B))
        .key(key)
        .body(ByteStream::from(meta::EVICT_SENTINEL.to_vec()))
        .set_metadata(Some(md))
        .send()
        .await
        .expect("plant eviction tombstone");
    let twin = meta::Facts {
        client_etag: cetag,
        plen: body.len() as u64,
        mtime_ms: 1,
    }
    .twin_key(key)
    .unwrap();
    h.raw()
        .put_object()
        .bucket(h.meta_bucket(B))
        .key(twin)
        .body(ByteStream::from(Vec::new()))
        .send()
        .await
        .expect("plant twin");
}

/// The per-bucket clean marker (§6): present iff a graceful drain vouched for that bucket.
async fn clean_marker_present(h: &Harness, client_bucket: &str) -> bool {
    raw_exists(h, &h.meta_bucket(client_bucket), &meta::clean_marker_key()).await
}

/// A graceful drain writes each accounted-for bucket's clean marker, and the next startup deletes
/// every one of them **before serving** — so the on-disk default is always "dirty" (§6/§7). Absence
/// is what buys a recovery scan, so a marker that outlived a startup would silently skip one.
#[tokio::test]
async fn clean_marker_is_written_on_drain_and_cleared_on_startup() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;
    put(&h.client(), B, "k", b"body").await;

    assert!(
        !clean_marker_present(&h, B).await,
        "no clean marker while the run is live"
    );

    h.stop_hypha().await;
    assert!(
        clean_marker_present(&h, B).await,
        "a graceful drain vouches for the bucket"
    );

    h.start_hypha().await;
    wait_until(5_000, "startup to clear the clean marker", || async {
        !clean_marker_present(&h, B).await
    })
    .await;
}

/// A kill leaves no clean marker, so the next run rebuilds the pending set from cache-vs-remote
/// state (§7). The orphan here is a live cache body with no marker whose generation the remote does
/// not hold — exactly what a crash between an acked write and its marker leaves behind, and what
/// nothing else would ever revisit: the reconcile sweep enumerates markers, so a markerless body is
/// invisible to it.
#[tokio::test]
async fn ungraceful_stop_rebuilds_missing_markers_on_the_next_run() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;

    // A normal write, carried all the way to the remote: the scan must leave it alone.
    put(&h.client(), B, "durable", b"already-uploaded").await;
    wait_until(5_000, "the reconcile sweep to upload and clear", || async {
        remote_present(&h, B, "durable").await && !marker_present(&h, B, "durable").await
    })
    .await;

    // The orphan: straight into `<data>` behind hypha's back, so it has no marker and never
    // reached the remote.
    raw_cache_put(&h, B, "orphan", b"never-indexed".to_vec(), HashMap::new()).await;
    assert!(!marker_present(&h, B, "orphan").await);

    h.kill_hypha().await;
    assert!(
        !clean_marker_present(&h, B).await,
        "a kill must never leave a bucket claiming to be clean"
    );

    h.start_hypha().await;
    wait_until(
        15_000,
        "the recovery scan to make the orphan durable",
        || async { remote_present(&h, B, "orphan").await },
    )
    .await;
    assert_eq!(
        get_all(&h.client(), B, "orphan").await,
        b"never-indexed",
        "the recovered body is the one the scan found"
    );
    assert!(
        !marker_present(&h, B, "durable").await,
        "a key the remote already holds in this generation is not re-marked"
    );
}
