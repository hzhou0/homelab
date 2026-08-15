//! Durable and cached CopyObject behavior, races, and recovery.

mod common;

use common::*;
use hypha_core::config::Mode;

const B: &str = "cpy";

async fn copy(client: &aws_sdk_s3::Client, dst: &str, src_bucket: &str, src_key: &str) -> String {
    client
        .copy_object()
        .bucket(B)
        .key(dst)
        .copy_source(format!("{src_bucket}/{src_key}"))
        .send()
        .await
        .expect("copy_object")
        .copy_object_result()
        .and_then(|r| r.e_tag())
        .expect("copy result etag")
        .trim_matches('"')
        .to_string()
}

/// A small single-part source takes the re-encrypt path: bytes and content-derived ETag survive,
/// the destination is independently decryptable, and the source is untouched.
#[tokio::test]
async fn copy_small_single_part_reencrypts() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let src = pattern_seeded(4096, 1);
    let src_etag = put(&client, B, "src/small", &src).await;

    let dst_etag = copy(&client, "dst/small", B, "src/small").await;
    assert_eq!(
        dst_etag, src_etag,
        "copy preserves the content-derived ETag"
    );
    assert_eq!(get_all(&client, B, "dst/small").await, src, "copied bytes");
    assert_eq!(
        get_all(&client, B, "src/small").await,
        src,
        "source untouched"
    );

    // A ranged GET of the destination proves the fresh trailer frames a readable single-part object.
    assert_eq!(
        get_range(&client, B, "dst/small", 100, 200).await,
        src[100..=200]
    );
}

/// A single-part source at/above the 5 MiB part minimum takes the server-side `UploadPartCopy` path.
#[tokio::test]
async fn copy_large_single_part_server_side() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let src = pattern_seeded(MIN_PART + 4096, 2);
    let src_etag = put(&client, B, "src/large", &src).await;

    let dst_etag = copy(&client, "dst/large", B, "src/large").await;
    assert_eq!(dst_etag, src_etag);
    assert_eq!(
        get_all(&client, B, "dst/large").await,
        src,
        "whole copied body"
    );
    // Range straddling an age chunk boundary well inside the body.
    let a = 3 * 1024 * 1024;
    assert_eq!(
        get_range(&client, B, "dst/large", a, a + 500).await,
        src[a as usize..=(a + 500) as usize]
    );
}

/// A composite source copies through the server-side path with its offset table carried over
/// untouched: the destination reports the same composite ETag and reads back part-for-part.
#[tokio::test]
async fn copy_composite_source_preserves_geometry() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let p1 = pattern_seeded(MIN_PART, 1);
    let p2 = pattern_seeded(MIN_PART, 2);
    let p3 = pattern_seeded(2 * 1024 * 1024, 3);
    let whole: Vec<u8> = [p1.as_slice(), p2.as_slice(), p3.as_slice()].concat();
    let up = create_mpu(&client, B, "src/comp").await;
    let e1 = upload_part(&client, B, "src/comp", &up, 1, &p1).await;
    let e2 = upload_part(&client, B, "src/comp", &up, 2, &p2).await;
    let e3 = upload_part(&client, B, "src/comp", &up, 3, &p3).await;
    let src_etag = complete_mpu(&client, B, "src/comp", &up, &[(1, e1), (2, e2), (3, e3)]).await;
    assert!(src_etag.ends_with("-3"), "composite ETag: {src_etag}");

    let dst_etag = copy(&client, "dst/comp", B, "src/comp").await;
    assert_eq!(dst_etag, src_etag, "composite ETag carries over");
    assert_eq!(
        get_all(&client, B, "dst/comp").await,
        whole,
        "whole composite copy"
    );

    // Ranges across the carried-over part boundaries.
    for (a, b) in [(0u64, 10u64), (MIN_PART as u64 - 5, MIN_PART as u64 + 5)] {
        assert_eq!(
            get_range(&client, B, "dst/comp", a, b).await,
            whole[a as usize..=b as usize],
            "range {a}..={b}"
        );
    }

    // The destination's parts table matches the source's part geometry.
    let parts = client
        .get_object_attributes()
        .bucket(B)
        .key("dst/comp")
        .object_attributes(aws_sdk_s3::types::ObjectAttributes::ObjectParts)
        .send()
        .await
        .expect("get_object_attributes")
        .object_parts
        .expect("object parts");
    assert_eq!(parts.total_parts_count(), Some(3));
}

/// `metadata-directive: COPY` (the default) forwards source user metadata; `REPLACE` takes the
/// request's. Storage class is the request's either way.
#[tokio::test]
async fn copy_metadata_directive() {
    for mode in [Mode::Durable, Mode::Cached] {
        let h = Harness::with_mode(mode).await;
        h.create_bucket(B).await;
        let client = h.client();

        client
            .put_object()
            .bucket(B)
            .key("src/meta")
            .body(bytes_body(b"body"))
            .content_length(4)
            .metadata("colour", "green")
            .send()
            .await
            .expect("put with metadata");

        // COPY (default): the source metadata rides along.
        copy(&client, "dst/copied-meta", B, "src/meta").await;
        let copied = client
            .head_object()
            .bucket(B)
            .key("dst/copied-meta")
            .send()
            .await
            .expect("head");
        assert_eq!(
            copied
                .metadata()
                .and_then(|m| m.get("colour"))
                .map(String::as_str),
            Some("green"),
            "COPY metadata in {mode:?}"
        );

        // REPLACE: the request's metadata wins, the source's is dropped.
        client
            .copy_object()
            .bucket(B)
            .key("dst/replaced-meta")
            .copy_source(format!("{B}/src/meta"))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .metadata("colour", "blue")
            .send()
            .await
            .expect("copy REPLACE");
        let replaced = client
            .head_object()
            .bucket(B)
            .key("dst/replaced-meta")
            .send()
            .await
            .expect("head");
        assert_eq!(
            replaced
                .metadata()
                .and_then(|m| m.get("colour"))
                .map(String::as_str),
            Some("blue"),
            "REPLACE metadata in {mode:?}"
        );
    }
}

/// `x-amz-copy-source-if-match` / `if-none-match` gate on the source's current ETag.
#[tokio::test]
async fn copy_source_preconditions() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let src = pattern_seeded(2048, 7);
    let etag = put(&client, B, "src/cond", &src).await;

    // if-match on the wrong ETag → 412.
    let err = client
        .copy_object()
        .bucket(B)
        .key("dst/cond-a")
        .copy_source(format!("{B}/src/cond"))
        .copy_source_if_match("\"00000000000000000000000000000000\"")
        .send()
        .await
        .expect_err("stale if-match must fail");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("PreconditionFailed")
    );

    // if-none-match on the current ETag → 412.
    let err = client
        .copy_object()
        .bucket(B)
        .key("dst/cond-b")
        .copy_source(format!("{B}/src/cond"))
        .copy_source_if_none_match(format!("\"{etag}\""))
        .send()
        .await
        .expect_err("matching if-none-match must fail");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("PreconditionFailed")
    );

    // if-match on the current ETag → proceeds.
    client
        .copy_object()
        .bucket(B)
        .key("dst/cond-ok")
        .copy_source(format!("{B}/src/cond"))
        .copy_source_if_match(format!("\"{etag}\""))
        .send()
        .await
        .expect("matching if-match copies");
    assert_eq!(get_all(&client, B, "dst/cond-ok").await, src);
}

#[tokio::test]
async fn copy_destination_preconditions() {
    for mode in [Mode::Durable, Mode::Cached] {
        let h = Harness::with_mode(mode).await;
        h.create_bucket(B).await;
        let client = h.client();

        let src = pattern_seeded(2048, 71);
        let original = pattern_seeded(1024, 72);
        put(&client, B, "src/dst-cond", &src).await;
        let original_etag = put(&client, B, "dst/conditional", &original).await;

        let stale = client
            .copy_object()
            .bucket(B)
            .key("dst/conditional")
            .copy_source(format!("{B}/src/dst-cond"))
            .if_match("\"00000000000000000000000000000000\"")
            .send()
            .await
            .expect_err("stale destination If-Match must fail");
        assert_eq!(
            stale.into_service_error().meta().code(),
            Some("PreconditionFailed"),
            "{mode:?}"
        );

        let occupied = client
            .copy_object()
            .bucket(B)
            .key("dst/conditional")
            .copy_source(format!("{B}/src/dst-cond"))
            .if_none_match("*")
            .send()
            .await
            .expect_err("destination If-None-Match must reject an existing key");
        assert_eq!(
            occupied.into_service_error().meta().code(),
            Some("PreconditionFailed"),
            "{mode:?}"
        );
        assert_eq!(get_all(&client, B, "dst/conditional").await, original);

        client
            .copy_object()
            .bucket(B)
            .key("dst/conditional")
            .copy_source(format!("{B}/src/dst-cond"))
            .if_match(format!("\"{original_etag}\""))
            .send()
            .await
            .expect("matching destination If-Match copies");
        assert_eq!(get_all(&client, B, "dst/conditional").await, src);

        client
            .copy_object()
            .bucket(B)
            .key("dst/create")
            .copy_source(format!("{B}/src/dst-cond"))
            .if_none_match("*")
            .send()
            .await
            .expect("destination If-None-Match copies to an absent key");
        assert_eq!(get_all(&client, B, "dst/create").await, src);

        let absent = client
            .copy_object()
            .bucket(B)
            .key("dst/missing")
            .copy_source(format!("{B}/src/dst-cond"))
            .if_match(format!("\"{original_etag}\""))
            .send()
            .await
            .expect_err("destination If-Match must reject an absent key");
        assert_eq!(
            absent.into_service_error().meta().code(),
            Some("PreconditionFailed"),
            "{mode:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_conditional_copy_creates_linearize_under_contention() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let body = pattern_seeded(2048, 73);
    put(&h.client(), B, "src/contention", &body).await;

    let tasks = (0..24).map(|_| {
        let client = h.client();
        tokio::spawn(async move {
            client
                .copy_object()
                .bucket(B)
                .key("dst/contention")
                .copy_source(format!("{B}/src/contention"))
                .if_none_match("*")
                .send()
                .await
                .map_err(|error| sdk_err_code(&error))
        })
    });

    let mut wins = 0;
    for result in futures::future::join_all(tasks).await {
        match result.expect("racer panicked") {
            Ok(_) => wins += 1,
            Err(code) => assert_eq!(code.as_deref(), Some("PreconditionFailed")),
        }
    }
    assert_eq!(wins, 1);
    assert_eq!(get_all(&h.client(), B, "dst/contention").await, body);
}

/// A same-key `REPLACE` copy is an in-place metadata edit: the body is unchanged, the metadata swaps.
#[tokio::test]
async fn copy_in_place_metadata_edit() {
    for mode in [Mode::Durable, Mode::Cached] {
        let h = Harness::with_mode(mode).await;
        h.create_bucket(B).await;
        let client = h.client();

        let body = pattern_seeded(1024, 4);
        client
            .put_object()
            .bucket(B)
            .key("obj")
            .body(bytes_body(&body))
            .content_length(body.len() as i64)
            .metadata("v", "1")
            .send()
            .await
            .expect("put");

        client
            .copy_object()
            .bucket(B)
            .key("obj")
            .copy_source(format!("{B}/obj"))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .metadata("v", "2")
            .send()
            .await
            .expect("in-place copy");

        let head = client
            .head_object()
            .bucket(B)
            .key("obj")
            .send()
            .await
            .expect("head");
        assert_eq!(
            head.metadata().and_then(|m| m.get("v")).map(String::as_str),
            Some("2"),
            "metadata replaced in {mode:?}"
        );
        assert_eq!(
            get_all(&client, B, "obj").await,
            body,
            "body unchanged by the edit in {mode:?}"
        );
    }
}

/// A copy from a source that does not exist is a client-visible 404.
#[tokio::test]
async fn copy_missing_source() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let err = client
        .copy_object()
        .bucket(B)
        .key("dst/nope")
        .copy_source(format!("{B}/does/not/exist"))
        .send()
        .await
        .expect_err("copy from a missing source must fail");
    assert_eq!(err.into_service_error().meta().code(), Some("NoSuchKey"));
}

/// A live source in cached mode stays on the cache fast path: the cache copy is the commit, its
/// native plaintext ETag names the pending marker, and reconcile trails the destination remotely.
#[tokio::test]
async fn cached_live_source_copies_in_cache_and_raises_a_marker() {
    let h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let client = h.client();
    let source = pattern_seeded(64 * 1024, 21);
    let source_etag = put(&client, B, "src/live", &source).await;

    // Hold the reconcile upload so the post-copy representation can be inspected without racing a
    // fast local MinIO round trip.
    let mut upload = h.remote_faults().pause_next(
        hyper::Method::PUT,
        format!("/{}/dst/live", h.remote_bucket(B)),
    );

    let copied_etag = copy(&client, "dst/live", B, "src/live").await;
    assert_eq!(copied_etag, source_etag);
    assert_eq!(get_all(&client, B, "dst/live").await, source);
    assert_eq!(
        data_class(&h, B, "dst/live").await,
        None,
        "a live-source copy commits as a live cache body"
    );

    wait_until(5_000, "the cached copy's marker to land", || async {
        marker_present(&h, B, "dst/live").await
    })
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(5), upload.reached())
        .await
        .expect("reconcile never attempted the copied destination");
    upload.release();

    wait_until(5_000, "the copied destination to reconcile", || async {
        remote_present(&h, B, "dst/live").await && !marker_present(&h, B, "dst/live").await
    })
    .await;
}

/// A remote-resident source in a cached deployment keeps the durable ciphertext-copy path. A
/// composite is the decisive case: its plaintext may have a warm shadow, but K remains tombstoned,
/// so the destination preserves its composite ETag and never raises a pending marker.
#[tokio::test]
async fn cached_remote_source_uses_the_durable_copy_path() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let client = h.client();
    let p1 = pattern_seeded(MIN_PART, 31);
    let p2 = pattern_seeded(1024 * 1024, 32);
    let whole: Vec<u8> = [p1.as_slice(), p2.as_slice()].concat();

    let upload = create_mpu(&client, B, "src/composite").await;
    let e1 = upload_part(&client, B, "src/composite", &upload, 1, &p1).await;
    let e2 = upload_part(&client, B, "src/composite", &upload, 2, &p2).await;
    let source_etag = complete_mpu(&client, B, "src/composite", &upload, &[(1, e1), (2, e2)]).await;

    let copied_etag = copy(&client, "dst/composite", B, "src/composite").await;
    assert_eq!(copied_etag, source_etag);
    assert_eq!(
        data_class(&h, B, "dst/composite").await,
        Some(hypha_core::meta::TombKind::Evict),
        "remote-source copy settles an eviction tombstone"
    );
    assert!(
        !marker_present(&h, B, "dst/composite").await,
        "the synchronous remote commit owes no reconcile marker"
    );
    assert!(remote_present(&h, B, "dst/composite").await);
    assert_eq!(get_all(&client, B, "dst/composite").await, whole);
}

/// The source HEAD only chooses the cache-copy branch; the backend copy itself is bound to that
/// physical ETag. A concurrent unconditional PUT may win without taking Hypha's write lock, but it
/// must make this copy fail rather than copying new bytes under the old source facts.
#[tokio::test]
async fn cached_copy_is_bound_to_the_resolved_source_generation() {
    let h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let first = pattern_seeded(32 * 1024, 41);
    let second = pattern_seeded(32 * 1024, 42);
    let first_etag = put(&h.client(), B, "src/race", &first).await;

    let mut backend_copy = h.cache_faults().pause_next(
        hyper::Method::PUT,
        format!("/{}/dst/race", h.cache_bucket(B)),
    );
    let client = h.client();
    let request = tokio::spawn(async move {
        client
            .copy_object()
            .bucket(B)
            .key("dst/race")
            .copy_source(format!("{B}/src/race"))
            .send()
            .await
    });

    let captured = tokio::time::timeout(std::time::Duration::from_secs(5), backend_copy.reached())
        .await
        .expect("cache copy was never attempted");
    assert_eq!(
        captured
            .headers
            .get("x-amz-copy-source-if-match")
            .and_then(|value| value.to_str().ok()),
        Some(format!("\"{first_etag}\"").as_str()),
        "the cache operation must bind the generation selected by HEAD"
    );

    put(&h.client(), B, "src/race", &second).await;
    backend_copy.release();

    let error = request
        .await
        .expect("copy task panicked")
        .expect_err("a replaced source must fail the generation-bound copy");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("PreconditionFailed")
    );
    assert!(
        h.raw()
            .head_object()
            .bucket(h.cache_bucket(B))
            .key("dst/race")
            .send()
            .await
            .is_err(),
        "a failed source condition must leave the destination absent"
    );
}

/// A cache copy can commit and lose every response before Hypha raises its marker. As with cached
/// PUT/DELETE, the run must withdraw its clean accounting so R2 reconstructs the missing obligation
/// instead of permanently leaving the destination absent from the remote.
#[tokio::test]
async fn cached_copy_with_a_lost_response_is_rebuilt_next_run() {
    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let body = pattern_seeded(48 * 1024, 51);
    put(&h.client(), B, "src/lost-response", &body).await;

    let faults = h.cache_faults();
    let lost = faults.fail_response_times(
        hyper::Method::PUT,
        format!("/{}/dst/lost-response", h.cache_bucket(B)),
        hyper::StatusCode::INTERNAL_SERVER_ERROR,
        1_000,
    );
    let result = h
        .client()
        .copy_object()
        .bucket(B)
        .key("dst/lost-response")
        .copy_source(format!("{B}/src/lost-response"))
        .send()
        .await;
    assert!(result.is_err(), "the lost responses must reach the client");
    tokio::time::timeout(std::time::Duration::from_secs(5), lost)
        .await
        .expect("the cache copy was never attempted")
        .expect("fault proxy stopped before losing the response");
    faults.clear();

    assert_eq!(
        get_all(&h.client(), B, "dst/lost-response").await,
        body,
        "the cache commit landed despite the client error"
    );
    assert!(
        !marker_present(&h, B, "dst/lost-response").await,
        "the response was lost before the marker could be queued"
    );

    h.stop_hypha().await;
    assert!(
        !raw_exists(&h, &h.meta_bucket(B), &hypha_core::meta::clean_marker_key()).await,
        "an indeterminate copy must withhold the clean marker"
    );

    h.start_hypha().await;
    wait_until(
        10_000,
        "R2 to rebuild and reconcile the copied body",
        || async {
            remote_present(&h, B, "dst/lost-response").await
                && !marker_present(&h, B, "dst/lost-response").await
        },
    )
    .await;
}
