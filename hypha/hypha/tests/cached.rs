//! Cached commits, reconciliation, rehydration, and shutdown accounting.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use common::*;
use hypha_core::config::Mode;
use hypha_core::meta;

const B: &str = "cached";

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
    h.raw_for_bucket(bucket)
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
    // Waited for rather than asserted outright: the marker is handed to the queue *after* the commit
    // (§7), precisely so a marker failure cannot turn an acked write into an error, so its presence
    // is never synchronous with the ack. On a fast backend it lands within the same millisecond,
    // which is what made an immediate assertion look sound.
    wait_until(5_000, "the queue to land the write's marker", || async {
        marker_present(&h, B, "obj").await
    })
    .await;
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

/// A marker backend failure cannot turn an already-committed cached PUT into a client error. The
/// marker actor retains the obligation and retries it until the remote catches up.
#[tokio::test]
async fn failed_marker_write_is_retried_after_the_put_ack() {
    let h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let key = "marker-fault";
    let body = pattern(32_000);
    let failed = h.cache_faults().fail_times(
        hyper::Method::PUT,
        format!("/{}/{key}", h.meta_bucket(B)),
        hyper::StatusCode::FORBIDDEN,
        8,
    );

    let etag = put(&h.client(), B, key, &body).await;
    assert_eq!(etag, md5_hex(&body), "the committed PUT must be acked");
    let intercepted = tokio::time::timeout(Duration::from_secs(5), failed)
        .await
        .expect("marker write was never attempted")
        .expect("fault proxy stopped before failing the marker");
    assert_eq!(intercepted.method, hyper::Method::PUT);
    assert_eq!(
        get_all(&h.client(), B, key).await,
        body,
        "the failed marker write must not roll back the cache commit"
    );

    wait_until(6_000, "marker retry to make the PUT durable", || async {
        remote_present(&h, B, key).await && !marker_present(&h, B, key).await
    })
    .await;
}

/// A reconcile already carrying one generation may finish after a newer cached PUT. Its marker
/// clear must lose the CAS, leaving the newer generation enumerable for the next pass.
#[tokio::test]
async fn overwrite_during_reconcile_preserves_the_newer_marker() {
    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let key = "overwrite-during-reconcile";
    let v1 = pattern_seeded(48_000, 1);
    let v2 = pattern_seeded(48_000, 2);
    let path = format!("/{}/{key}", h.remote_bucket(B));
    let faults = h.remote_faults();

    let mut first_upload = faults.pause_next(hyper::Method::PUT, &path);
    put(&h.client(), B, key, &v1).await;
    tokio::time::timeout(Duration::from_secs(5), first_upload.reached())
        .await
        .expect("the first reconcile upload was never attempted");
    let old_marker_etag = h
        .raw()
        .head_object()
        .bucket(h.meta_bucket(B))
        .key(key)
        .send()
        .await
        .expect("head first marker")
        .e_tag()
        .expect("first marker ETag")
        .to_string();

    put(&h.client(), B, key, &v2).await;
    let new_marker_etag = h
        .raw()
        .head_object()
        .bucket(h.meta_bucket(B))
        .key(key)
        .send()
        .await
        .expect("head replacement marker")
        .e_tag()
        .expect("replacement marker ETag")
        .to_string();
    assert_ne!(
        old_marker_etag, new_marker_etag,
        "the overwrite must replace the operation marker"
    );

    let mut old_clear = h.cache_faults().pause_next_then_fail(
        hyper::Method::DELETE,
        format!("/{}/{key}", h.meta_bucket(B)),
        hyper::StatusCode::PRECONDITION_FAILED,
    );
    let mut second_upload = faults.pause_next(hyper::Method::PUT, &path);
    first_upload.release();
    let clear = tokio::time::timeout(Duration::from_secs(5), old_clear.reached())
        .await
        .expect("the first generation never attempted to clear its marker");
    assert_eq!(
        clear
            .headers
            .get(hyper::header::IF_MATCH)
            .and_then(|v| v.to_str().ok()),
        Some(old_marker_etag.as_str()),
        "the first upload must clear only the marker generation it observed"
    );
    old_clear.release();
    tokio::time::timeout(Duration::from_secs(5), second_upload.reached())
        .await
        .expect("the replacement generation was never reconciled");
    assert!(
        marker_present(&h, B, key).await,
        "finishing the old upload must not clear the replacement marker"
    );
    let standing_marker_etag = h
        .raw()
        .head_object()
        .bucket(h.meta_bucket(B))
        .key(key)
        .send()
        .await
        .expect("head standing replacement marker")
        .e_tag()
        .expect("standing marker ETag")
        .to_string();
    assert_eq!(standing_marker_etag, new_marker_etag);
    second_upload.release();

    wait_until(
        6_000,
        "replacement generation to settle remotely",
        || async { remote_present(&h, B, key).await && !marker_present(&h, B, key).await },
    )
    .await;

    h.stop_hypha().await;
    drop_backend_bucket(&h, &h.cache_bucket(B)).await;
    drop_backend_bucket(&h, &h.meta_bucket(B)).await;
    h.start_hypha().await;
    assert_eq!(
        get_all(&h.client(), B, key).await,
        v2,
        "the remote must end on the replacement generation"
    );
}

/// Emptiness is a claim about the **client** namespace, so a cached bucket whose deletes have not
/// reached the remote yet still deletes: the cache is what the client can see, and the remote bodies
/// standing behind it are exactly as stale as the bucket now is. hypha drains them itself rather
/// than leaving the remote to refuse the delete (§7) — which is also what makes the gate independent
/// of whether the backend refuses one at all.
#[tokio::test]
async fn a_cached_bucket_deletes_before_its_deletes_have_propagated() {
    let mut h = Harness::builder(Mode::Cached)
        // Long enough that the DELETE marker cannot propagate on its own — the remote must still
        // hold the body when the bucket delete runs, or the test proves nothing.
        .tune(|c| c.reconcile.interval_ms = 600_000)
        .start()
        .await;
    h.create_bucket(B).await;
    let key = "pending-delete";
    put(&h.client(), B, key, b"body").await;
    // Written straight to the remote by the harness's own client so the pending state under test is
    // the *delete*, not an upload the paused sweep never made.
    h.raw_remote()
        .put_object()
        .bucket(h.remote_bucket(B))
        .key(key)
        .body(ByteStream::from_static(b"stale"))
        .send()
        .await
        .expect("plant a remote body the sweep has not caught up with");

    h.client()
        .delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("cached delete");
    h.client()
        .delete_bucket()
        .bucket(B)
        .send()
        .await
        .expect("a client-empty bucket deletes whatever the remote still holds");

    for gone in [h.remote_bucket(B), h.cache_bucket(B), h.meta_bucket(B)] {
        assert!(
            h.raw_for_bucket(&gone)
                .head_bucket()
                .bucket(&gone)
                .send()
                .await
                .is_err(),
            "{gone} outlived the delete"
        );
    }
    h.stop_hypha().await;
}

/// A cached delete removes K immediately, including from delimiter grouping, and the reconcile
/// sweep propagates that authoritative absence to the remote.
#[tokio::test]
async fn cached_delete_removes_then_propagates() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(20_000);
    let key = "gone/d";

    put(&c, B, key, &body).await;
    wait_until(6000, "initial upload to remote", || async {
        remote_present(&h, B, key).await && !marker_present(&h, B, key).await
    })
    .await;

    c.delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("delete");
    let got = c.get_object().bucket(B).key(key).send().await;
    assert_eq!(
        sdk_err_code(&got.unwrap_err()).as_deref(),
        Some("NoSuchKey")
    );
    assert!(
        !raw_exists(&h, &h.cache_bucket(B), key).await,
        "the committed cache state is absence, not a tombstone"
    );
    let listed = c
        .list_objects_v2()
        .bucket(B)
        .delimiter("/")
        .send()
        .await
        .expect("delimited list after delete");
    let listed_v1 = c
        .list_objects()
        .bucket(B)
        .delimiter("/")
        .send()
        .await
        .expect("delimited v1 list after delete");
    assert!(listed.contents().is_empty() && listed_v1.contents().is_empty());
    // Only where the backend has no directories to leave behind: SeaweedFS keeps the emptied prefix
    // and hypha forwards common prefixes verbatim. tests/backend.rs states that divergence, and why
    // nothing here works around it.
    if external_cache_backend().is_none() {
        assert!(
            listed.common_prefixes().is_empty(),
            "an absent subtree must not survive as a common prefix"
        );
        assert!(
            listed_v1.common_prefixes().is_empty(),
            "v1 must project the same absent subtree"
        );
    }

    wait_until(6000, "delete propagates to the remote", || async {
        !remote_present(&h, B, key).await && !marker_present(&h, B, key).await
    })
    .await;
    let got = c.get_object().bucket(B).key(key).send().await;
    assert_eq!(
        sdk_err_code(&got.unwrap_err()).as_deref(),
        Some("NoSuchKey")
    );
}

/// A cached delete cannot pass an older reconcile upload: both use K's upload lock, so the upload
/// finishes first and the following delete removes it. This is what makes an unconditional remote
/// delete safe against the one remote writer that deliberately does not take K's write lock.
#[tokio::test]
async fn cached_delete_waits_for_an_in_flight_reconcile_upload() {
    let h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let key = "delete-behind-upload";
    let faults = h.remote_faults();
    let path = format!("/{}/{key}", h.remote_bucket(B));
    let mut upload = faults.pause_next(hyper::Method::PUT, &path);
    put(&h.client(), B, key, &pattern(20_000)).await;
    tokio::time::timeout(Duration::from_secs(5), upload.reached())
        .await
        .expect("reconcile upload was never attempted");

    let delete = faults.pause_next(hyper::Method::DELETE, &path);
    let mut delete_reached = tokio::spawn(async move {
        let mut delete = delete;
        let request = delete.reached().await;
        (delete, request)
    });
    h.client()
        .delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("cached delete");
    wait_until(
        5_000,
        "the delete marker to replace the upload marker",
        || async { marker_present(&h, B, key).await },
    )
    .await;
    // The default fixture's MinIO cache ignores this CAS. Refuse the stale clear explicitly so this
    // test isolates the remote lock ordering; backend.rs tests the deployed cache's real refusal.
    let stale_clear = h.cache_faults().fail_next(
        hyper::Method::DELETE,
        format!("/{}/{key}", h.meta_bucket(B)),
        hyper::StatusCode::PRECONDITION_FAILED,
    );

    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut delete_reached)
            .await
            .is_err(),
        "the remote delete passed an upload still holding K's upload lock"
    );
    upload.release();
    tokio::time::timeout(Duration::from_secs(5), stale_clear)
        .await
        .expect("the stale upload did not try to clear the replacement marker")
        .expect("cache fault proxy stopped before the stale marker clear");

    let (delete, request) = tokio::time::timeout(Duration::from_secs(5), delete_reached)
        .await
        .expect("remote delete was never attempted after the upload finished")
        .expect("delete observer task panicked");
    assert!(
        !request.headers.contains_key(hyper::header::IF_MATCH),
        "the serialized remote delete must not require a backend CAS"
    );
    delete.release();

    wait_until(6_000, "serialized delete to settle", || async {
        !remote_present(&h, B, key).await && !marker_present(&h, B, key).await
    })
    .await;
}

/// Multipart completion is the remote writer that raises no pending marker. The delete branch's
/// write lock keeps it behind the remote delete; once the lock is released, the completion becomes
/// the newer operation and its committed composite survives.
#[tokio::test]
async fn multipart_completion_waits_for_an_in_flight_cached_delete() {
    let h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "complete-behind-delete";

    put(&c, B, key, &pattern_seeded(20_000, 1)).await;
    wait_until(6_000, "initial generation to settle remotely", || async {
        remote_present(&h, B, key).await && !marker_present(&h, B, key).await
    })
    .await;

    let body = pattern_seeded(30_000, 2);
    let upload_id = create_mpu(&c, B, key).await;
    let part = upload_part(&c, B, key, &upload_id, 1, &body).await;

    let path = format!("/{}/{key}", h.remote_bucket(B));
    let mut remote_delete = h.remote_faults().pause_next(hyper::Method::DELETE, &path);
    c.delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("cached delete");
    let request = tokio::time::timeout(Duration::from_secs(5), remote_delete.reached())
        .await
        .expect("remote delete was never attempted");
    assert!(!request.headers.contains_key(hyper::header::IF_MATCH));

    let complete_client = c.clone();
    let complete_upload_id = upload_id.clone();
    let mut completion = tokio::spawn(async move {
        complete_mpu(&complete_client, B, key, &complete_upload_id, &[(1, part)]).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut completion)
            .await
            .is_err(),
        "multipart completion passed a delete still holding K's write lock"
    );

    remote_delete.release();
    let completed = tokio::time::timeout(Duration::from_secs(5), completion)
        .await
        .expect("multipart completion stayed blocked after the delete")
        .expect("multipart completion task panicked");
    assert_eq!(completed, expected_composite_etag(&[&body]));
    assert_eq!(get_all(&c, B, key).await, body);
    assert!(remote_present(&h, B, key).await);
    assert!(!marker_present(&h, B, key).await);
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

/// A client body colliding with a reserved internal sentinel is rejected at the write path, and any
/// prior version at K survives — nothing lands (review finding 1).
#[tokio::test]
async fn cached_put_rejects_reserved_sentinel_body() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();

    let good = pattern(64);
    put(&c, B, "s", &good).await;

    for sentinel in [
        meta::DELETE_MARKER_SENTINEL,
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

/// The drain must wait for a handler that has not committed yet, then put its newly owed marker
/// ahead of the seal. Otherwise the same shutdown could leave a committed body beside a clean
/// marker that falsely vouches for an incomplete pending set.
#[tokio::test]
async fn drain_orders_a_concurrent_commit_before_the_clean_marker() {
    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let key = "commit-during-drain";
    let body = pattern(64_000);
    let data_bucket = h.cache_bucket(B);
    let meta_bucket = h.meta_bucket(B);
    let remote_bucket = h.remote_bucket(B);
    let raw = h.raw();
    let raw_remote = h.raw_remote();
    let faults = h.cache_faults();
    let mut body_write = faults.pause_next(hyper::Method::PUT, format!("/{data_bucket}/{key}"));
    let mut marker_write = faults.pause_next(hyper::Method::PUT, format!("/{meta_bucket}/{key}"));
    let client = h.client();
    let submitted = body.clone();
    let request = tokio::spawn(async move {
        client
            .put_object()
            .bucket(B)
            .key(key)
            .body(bytes_body(&submitted))
            .content_length(submitted.len() as i64)
            .send()
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), body_write.reached())
        .await
        .expect("the cache commit was never attempted");

    let stop = h.stop_hypha();
    tokio::pin!(stop);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut stop)
            .await
            .is_err(),
        "the drain finished while a request had not committed"
    );
    body_write.release();

    let marker_request = tokio::select! {
        biased;
        () = &mut stop => panic!("the drain sealed before the committed write's marker"),
        reached = tokio::time::timeout(Duration::from_secs(5), marker_write.reached()) => {
            reached.expect("the committed write never raised its marker")
        }
    };
    assert_eq!(marker_request.method, hyper::Method::PUT);
    assert!(
        raw.head_object()
            .bucket(&data_bucket)
            .key(key)
            .send()
            .await
            .is_ok(),
        "the body must already be committed while its marker is in flight"
    );
    assert!(
        raw.head_object()
            .bucket(&meta_bucket)
            .key(meta::clean_marker_key())
            .send()
            .await
            .is_err(),
        "the clean marker must remain absent until the marker obligation settles"
    );

    marker_write.release();
    stop.await;
    request
        .await
        .expect("PUT task panicked")
        .expect("the drained PUT must retain its acknowledgement");

    assert!(
        raw.head_object()
            .bucket(&meta_bucket)
            .key(meta::clean_marker_key())
            .send()
            .await
            .is_ok(),
        "the completed drain must vouch for the now-indexed write"
    );
    let marker_stands = raw
        .head_object()
        .bucket(&meta_bucket)
        .key(key)
        .send()
        .await
        .is_ok();
    let remote_settled = raw_remote
        .head_object()
        .bucket(&remote_bucket)
        .key(key)
        .send()
        .await
        .is_ok();
    assert!(
        marker_stands || remote_settled,
        "a clean drain must leave the write either pending and indexed or already remote"
    );
}

/// A seal proves that every obligation was handed to the actor, not that every backend write
/// succeeded. If the final marker attempt still fails, no clean marker may be written.
#[tokio::test]
async fn marker_still_owed_at_drain_withholds_the_clean_marker() {
    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let key = "owed-at-drain";
    let data_bucket = h.cache_bucket(B);
    let meta_bucket = h.meta_bucket(B);
    let raw = h.raw();
    let faults = h.cache_faults();
    let marker_path = format!("/{meta_bucket}/{key}");
    let mut first_attempt = faults.pause_next_then_fail(
        hyper::Method::PUT,
        &marker_path,
        hyper::StatusCode::PRECONDITION_FAILED,
    );
    let client = h.client();
    let request = tokio::spawn(async move {
        client
            .put_object()
            .bucket(B)
            .key(key)
            .body(bytes_body(b"committed"))
            .content_length(9)
            .send()
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), first_attempt.reached())
        .await
        .expect("the marker actor never received the obligation");
    let stop = h.stop_hypha();
    tokio::pin!(stop);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut stop)
            .await
            .is_err(),
        "the drain finished while the marker actor was blocked"
    );

    let _final_failures = faults.fail_times(
        hyper::Method::PUT,
        &marker_path,
        hyper::StatusCode::PRECONDITION_FAILED,
        2,
    );
    first_attempt.release();
    stop.await;
    request
        .await
        .expect("PUT task panicked")
        .expect("the marker failure must not retract the acknowledged PUT");

    assert!(
        raw.head_object()
            .bucket(&data_bucket)
            .key(key)
            .send()
            .await
            .is_ok(),
        "the client-visible commit must remain live"
    );
    assert!(
        raw.head_object()
            .bucket(&meta_bucket)
            .key(key)
            .send()
            .await
            .is_err(),
        "all marker attempts were rejected"
    );
    assert!(
        raw.head_object()
            .bucket(&meta_bucket)
            .key(meta::clean_marker_key())
            .send()
            .await
            .is_err(),
        "an owed marker must leave the run dirty"
    );
}

/// **A multipart complete must survive the delete it superseded.** Multipart is always durable (§7),
/// so a complete commits to the remote and settles K without raising a pending marker — which makes
/// it the one write path that does not supersede a marker already standing at K. If a cached DELETE's
/// marker has not been swept yet when the complete lands, the sweep is left holding an obligation for
/// a key that exists again, and the generation it would discharge it against is the one the client
/// was just told was committed.
///
/// Set up with the sweep effectively switched off, so the interleaving is a fact of the state rather
/// than a race the test has to win: the marker and the completed composite are made to coexist, and
/// only then is a sweep allowed to run.
#[tokio::test]
async fn a_multipart_complete_survives_the_delete_marker_it_superseded() {
    let mut h = Harness::builder(Mode::Cached)
        .tune(|c| c.reconcile.interval_ms = 600_000)
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "superseded-delete";

    // The generation the stale marker will name. Put on the remote directly: the sweep is off, so
    // nothing would upload it, and what this test needs is only that the delete branch finds
    // something there to bind to.
    put(&c, B, key, &pattern_seeded(20_000, 3)).await;
    h.raw_remote()
        .put_object()
        .bucket(h.remote_bucket(B))
        .key(key)
        .body(ByteStream::from_static(
            b"the generation the delete was for",
        ))
        .send()
        .await
        .expect("plant the remote generation");

    c.delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("cached delete");
    wait_until(6_000, "the delete's obligation to stand", || async {
        marker_present(&h, B, key).await
    })
    .await;

    // K is taken again by the one path that raises no marker of its own, so nothing supersedes the
    // marker the sweep is about to read.
    let body = pattern_seeded(30_000, 4);
    let up = create_mpu(&c, B, key).await;
    let part = upload_part(&c, B, key, &up, 1, &body).await;
    let composite = complete_mpu(&c, B, key, &up, &[(1, part)]).await;
    assert_eq!(composite, expected_composite_etag(&[&body]));
    assert!(
        marker_present(&h, B, key).await,
        "and the multipart complete must not have cleared it on its way past"
    );

    // Now let a sweep run against exactly that state.
    h.stop_hypha().await;
    h.config.reconcile.interval_ms = 100;
    h.start_hypha().await;

    wait_until(
        6_000,
        "the sweep to resolve the superseded marker",
        || async { !marker_present(&h, B, key).await },
    )
    .await;
    assert_eq!(
        get_all(&h.client(), B, key).await,
        body,
        "the completed multipart upload must survive the delete it superseded"
    );
    assert!(
        remote_present(&h, B, key).await,
        "and its remote generation with it"
    );
}

/// **A cached commit whose response was lost may have landed**, and the obligation that would have
/// followed it did not. A cached DELETE removes K and *then* queues its marker (§7 — the queue sits
/// after the commit so a marker failure cannot turn an acked write into an error); a cache that takes
/// the delete and loses the response returns an error from between those two steps, leaving the key
/// client-absent with nothing to propagate the delete to the remote.
///
/// The remedy is the one the design already has for "cannot vouch": the run withdraws the bucket's
/// accounting, so the drain writes no clean marker and the next run's R2 rebuilds the pending set
/// from both namespaces — where the remote-only key reads as exactly what it is, an interrupted
/// delete. Without that, the run would end clean over a hole and the orphan would never be found.
#[tokio::test]
async fn a_cached_delete_that_lost_its_response_leaves_the_bucket_unaccounted() {
    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "lost-response";

    put(&c, B, key, &pattern_seeded(8_000, 5)).await;
    wait_until(6_000, "the write to reach the remote", || async {
        remote_present(&h, B, key).await && !marker_present(&h, B, key).await
    })
    .await;

    // The cache takes the delete and the response never comes back — standing, because the SDK
    // retries and a one-shot loss is simply served from the retry.
    let faults = h.cache_faults();
    let lost = faults.fail_response_times(
        hyper::Method::DELETE,
        format!("/{}/{key}", h.cache_bucket(B)),
        hyper::StatusCode::INTERNAL_SERVER_ERROR,
        // A cut must *stand*: the SDK retries, and every retry of an already-committed delete
        // succeeds at the backend, so the loss has to outlast the whole retry budget.
        1_000,
    );
    let refused = c.delete_object().bucket(B).key(key).send().await;
    assert!(
        refused.is_err(),
        "the lost response must surface as an error"
    );
    tokio::time::timeout(Duration::from_secs(5), lost)
        .await
        .expect("the delete was never attempted")
        .expect("fault proxy stopped before losing the response");
    faults.clear();

    // It landed all the same, so the key is gone client-side while the remote still holds it.
    assert!(
        !raw_exists(&h, &h.cache_bucket(B), key).await,
        "the delete reached the cache despite the error"
    );
    assert!(
        remote_present(&h, B, key).await,
        "and the remote still has it"
    );

    // No clean marker: this run cannot account for a pending set it may be missing an entry from.
    h.stop_hypha().await;
    assert!(
        !clean_marker_present(&h, B).await,
        "a run that may have dropped an obligation must not vouch for its pending set"
    );

    // R2 then finds the remote-only key and re-indexes it as the interrupted delete it is, which the
    // sweep propagates.
    h.start_hypha().await;
    wait_until(10_000, "R2 and the sweep to finish the delete", || async {
        !remote_present(&h, B, key).await
    })
    .await;
    assert_eq!(
        sdk_err_code(
            &h.client()
                .get_object()
                .bucket(B)
                .key(key)
                .send()
                .await
                .unwrap_err()
        )
        .as_deref(),
        Some("NoSuchKey")
    );
}

/// A marker owed to a deleted bucket must be discarded, not retried: one permanently owed marker
/// withholds the clean marker of *every* bucket at drain (§6). The obligation is dropped on the
/// state map's verdict rather than on the backend's error, so it does not matter what the backend
/// makes of a write into a bucket that is gone.
///
/// Which is a real difference: a marker write already at the backend when the delete drains its
/// projection **re-creates that projection** on SeaweedFS (tests/backend.rs). That leftover is inert
/// — startup does not resolve a cache bucket with no remote bucket, and the reconcile sweep takes
/// its bucket set from the state map — so what this pins is the client-visible half: the delete
/// stands, and the unrelated bucket still ends clean.
#[tokio::test]
async fn marker_for_a_deleted_bucket_does_not_withhold_surviving_clean_markers() {
    const DELETED: &str = "deleted-marker";
    const SURVIVOR: &str = "surviving-marker";

    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(DELETED).await;
    h.create_bucket(SURVIVOR).await;
    let deleted_meta = h.meta_bucket(DELETED);
    let survivor_meta = h.meta_bucket(SURVIVOR);
    let raw = h.raw();
    let key = "pending";
    let mut marker_write = h
        .cache_faults()
        .pause_next(hyper::Method::PUT, format!("/{deleted_meta}/{key}"));

    put(&h.client(), DELETED, key, b"body").await;
    tokio::time::timeout(Duration::from_secs(5), marker_write.reached())
        .await
        .expect("the deleted bucket's marker was never attempted");
    // The bucket has to be emptied to be deletable (§7's emptiness gate), which does not settle the
    // marker: the actor is still blocked on the held write, so the obligation stays owed across the
    // delete — which is the state under test.
    h.client()
        .delete_object()
        .bucket(DELETED)
        .key(key)
        .send()
        .await
        .expect("empty the bucket");
    h.client()
        .delete_bucket()
        .bucket(DELETED)
        .send()
        .await
        .expect("delete bucket while its marker is in flight");
    marker_write.release();

    h.stop_hypha().await;
    assert!(
        h.raw_remote()
            .head_bucket()
            .bucket(h.remote_bucket(DELETED))
            .send()
            .await
            .is_err(),
        "the delete must stand — the remote bucket is the client-visible one"
    );
    if external_cache_backend().is_none() {
        assert!(
            raw.head_bucket()
                .bucket(&deleted_meta)
                .send()
                .await
                .is_err(),
            "a backend that refuses the held write leaves no projection behind either"
        );
    }
    assert!(
        raw.head_object()
            .bucket(&survivor_meta)
            .key(meta::clean_marker_key())
            .send()
            .await
            .is_ok(),
        "the unrelated accounted bucket must still end clean"
    );
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

/// The drain joins every actor rather than leaving the runtime to drop them mid-call, so it has to
/// *end* — an actor that cannot observe shutdown would sit out its whole budget instead. Traffic first,
/// so the ring has slices to persist, GC has a bucket to sweep and the reconcile sweep has markers to
/// clear: the paths whose in-flight work the drain waits on.
#[tokio::test]
async fn a_graceful_drain_joins_every_actor_well_inside_its_budget() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;
    for i in 0..8 {
        put(&h.client(), B, &format!("k{i}"), b"body").await;
    }

    let started = std::time::Instant::now();
    h.stop_hypha().await;
    let drain = started.elapsed();

    // Sub-second in practice; the bound is loose because the assertion is about an actor that never
    // returns, not about latency. Reaching even half of one phase budget means something was waited
    // out rather than joined.
    assert!(
        drain < Duration::from_secs(5),
        "drain took {drain:?}; an actor is not observing shutdown"
    );
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

/// The active dies with a sweep in flight. Nothing about the pending set lives in the process — it is
/// the marker range itself — so the next run resumes from the same LIST and drops nothing, whichever
/// key the dead sweep was in the middle of.
///
/// The pause is what makes "mid-sweep" a fact rather than a hope: the kill happens with one key's
/// upload demonstrably in flight and the rest of the set still enumerable.
#[tokio::test]
async fn killing_the_active_mid_sweep_loses_no_pending_key() {
    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let bodies: Vec<Vec<u8>> = (0..6u8)
        .map(|i| pattern_seeded(8_192 + i as usize, i))
        .collect();
    let keys: Vec<String> = (0..bodies.len()).map(|i| format!("sweep/{i}")).collect();

    let mut in_flight = h.remote_faults().pause_next(
        hyper::Method::PUT,
        format!("/{}/{}", h.remote_bucket(B), keys[0]),
    );
    for (key, body) in keys.iter().zip(&bodies) {
        put(&h.client(), B, key, body).await;
    }
    tokio::time::timeout(Duration::from_secs(10), in_flight.reached())
        .await
        .expect("no upload was in flight, so nothing was killed mid-sweep");

    h.kill_hypha().await;
    in_flight.release();
    h.start_hypha().await;

    for (key, body) in keys.iter().zip(&bodies) {
        wait_until(
            20_000,
            &format!("{key} to be reconciled by the new run"),
            || async { remote_present(&h, B, key).await && !marker_present(&h, B, key).await },
        )
        .await;
        assert_eq!(
            &get_all(&h.client(), B, key).await,
            body,
            "{key} came back as a different generation"
        );
    }
}

/// The bounded loss window (§7): a cache volume that goes takes exactly the keys the pending set names
/// and nothing else. That is the whole durability claim of cached mode, and the pending set is what
/// makes it a *bound* rather than a hope — so both sides are asserted from one wipe, a key the sweep
/// had already uploaded and a key it could not.
#[tokio::test]
async fn a_cache_volume_wipe_loses_exactly_the_pending_set() {
    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let durable = pattern_seeded(4_096, 1);
    let pending = pattern_seeded(4_096, 2);

    put(&h.client(), B, "reconciled", &durable).await;
    wait_until(10_000, "the first key to reach the remote", || async {
        remote_present(&h, B, "reconciled").await && !marker_present(&h, B, "reconciled").await
    })
    .await;

    // The second key acks in the cache and can never reach the remote, so it is exactly what the
    // pending marker is standing for when the volume goes.
    h.remote_faults().fail_prefix_times(
        hyper::Method::PUT,
        format!("/{}/unreconciled", h.remote_bucket(B)),
        hyper::StatusCode::SERVICE_UNAVAILABLE,
        10_000,
    );
    put(&h.client(), B, "unreconciled", &pending).await;
    assert!(marker_present(&h, B, "unreconciled").await);

    // Stopped gracefully, not killed, and the difference is the harness rather than the subject: an
    // in-process `kill` abandons the serving task but leaves the run's background actors on the
    // runtime, so the volume wipe below would be observed by the *previous* run's watchdog — which
    // correctly reads a ready bucket's vanished sync marker as I6 and takes the test process down
    // with it. The drain is irrelevant to what this asserts: the marker is owed to a remote that
    // still refuses it, so this run vouches for nothing either way.
    h.stop_hypha().await;
    h.remote_faults().clear();
    for bucket in [h.cache_bucket(B), h.meta_bucket(B)] {
        drop_backend_bucket(&h, &bucket).await;
    }
    h.start_hypha().await;
    let c = h.client();

    wait_until(20_000, "the namespace restore to complete", || async {
        raw_exists(&h, &h.meta_bucket(B), &meta::sync_marker_key()).await
    })
    .await;
    assert_eq!(
        get_all(&c, B, "reconciled").await,
        durable,
        "everything the sweep had uploaded survives the volume"
    );
    let lost = c
        .get_object()
        .bucket(B)
        .key("unreconciled")
        .send()
        .await
        .expect_err("a cache-only generation cannot survive its cache");
    assert_eq!(
        sdk_err_code(&lost).as_deref(),
        Some("NoSuchKey"),
        "and the loss is a clean absence, not a key that reads as something else"
    );
    assert!(
        !marker_present(&h, B, "unreconciled").await,
        "a restored namespace owes nothing: the marker range went with the volume"
    );
}

/// The clean marker is **positive evidence** and nothing else: a bucket whose pending-set rebuild
/// never completed ends the run dirty, however cleanly the run drains. And one bucket's doubt must not
/// spread — a bucket this run established itself still ends clean, or a single unrecoverable bucket
/// would send the next run into a full rebuild of every other one.
#[tokio::test]
async fn a_bucket_whose_rebuild_never_completed_ends_the_run_dirty() {
    const DOUBTED: &str = "rebuild-doubted";
    const FRESH: &str = "rebuild-fresh";

    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(DOUBTED).await;
    put(&h.client(), DOUBTED, "k", &pattern(1_024)).await;

    // A kill leaves the bucket dirty, so the next run owes it a rebuild…
    h.kill_hypha().await;
    // …which cannot finish: the rebuild is a two-cursor join, and this is the remote cursor. Object
    // reads are unaffected — the path here is the bucket's own, without a key.
    // The bucket-scoped LIST, and only it: path-style renders it with a trailing slash, so this
    // matches no object read. `refused` is checked below, since a path that drifted would make the
    // whole test pass vacuously.
    let refused = h.remote_faults().fail_times(
        hyper::Method::GET,
        format!("/{}/", h.remote_bucket(DOUBTED)),
        hyper::StatusCode::SERVICE_UNAVAILABLE,
        10_000,
    );
    h.start_hypha().await;
    h.await_ready().await;
    h.create_bucket(FRESH).await;
    put(&h.client(), FRESH, "k", &pattern(1_024)).await;

    tokio::time::timeout(Duration::from_secs(10), refused)
        .await
        .expect("the rebuild never listed the remote, so nothing was held back")
        .expect("fault proxy stopped before the rebuild's listing");

    h.stop_hypha().await;
    assert!(
        !clean_marker_present(&h, DOUBTED).await,
        "a bucket whose pending set this run could not account for must end dirty"
    );
    assert!(
        clean_marker_present(&h, FRESH).await,
        "a bucket this run created empty is accounted by construction, and one doubted bucket must \
         not withhold its marker"
    );
}

/// Bursty same-key overwrites: the sweep coalesces onto whatever generation is current when it wins
/// the upload lock, so the remote must converge on the **last acked** one — not on whichever upload
/// happened to finish last. An unserialized sweep would leave an older generation standing with an
/// empty pending set, which no later pass would ever revisit.
///
/// Which generation the remote holds is read off its framed length: every body here has a distinct
/// plaintext length, and the framed size is a closed form of it (§6), so the byte count names the
/// generation without decrypting anything.
///
/// The claim is **convergence**, so it is asserted as one: an empty pending set is not by itself the
/// end state to wait for. A marker is written after its write acks (§7), so between an ack and the
/// queue landing its marker there is a moment when the key is genuinely owed and genuinely unmarked,
/// and sampling a single instant can catch exactly that moment.
///
/// **`#[ignore]`d because the default fixture uses MinIO as the cache.** The sweep's cache-side
/// marker clear is a conditional delete, and MinIO ignores `If-Match` on `DeleteObject`
/// (`backend.rs`) — so a clear issued for the generation a pass listed also removes the marker a
/// newer write raised while that pass ran. The remote is then left holding an older generation with
/// an empty pending set, which nothing revisits: this test reproduces that in roughly one run in
/// three under load. The SeaweedFS cache enforces the precondition.
#[tokio::test]
#[ignore = "needs a cache that enforces If-Match on DeleteObject (SeaweedFS does, MinIO does not); see tests/backend.rs"]
async fn bursty_same_key_overwrites_converge_on_the_last_acked_generation() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "burst";

    let mut last = Vec::new();
    for i in 0..16u8 {
        let body = pattern_seeded(20_000 + i as usize * 97, i);
        put(&c, B, key, &body).await;
        last = body;
    }
    // Every body has a distinct plaintext length and the framed size is a closed form of it (§6), so
    // the remote object's byte count names the generation without decrypting anything.
    let framed =
        hypha_format::offset::ciphertext_len(last.len() as u64, hypha_format::offset::HLEN)
            + hypha_format::SINGLE_TRAILER_LEN as u64;

    wait_until(
        15_000,
        "the remote to converge on the last acked generation",
        || async {
            let settled = h
                .raw_remote()
                .head_object()
                .bucket(h.remote_bucket(B))
                .key(key)
                .send()
                .await
                .ok()
                .and_then(|head| head.content_length())
                .is_some_and(|len| len as u64 == framed);
            settled && !marker_present(&h, B, key).await
        },
    )
    .await;
    assert_eq!(get_all(&c, B, key).await, last);
}
