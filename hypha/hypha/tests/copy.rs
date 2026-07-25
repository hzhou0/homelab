//! Phase-3 exit: CopyObject (§7). A copy reuses the source body ciphertext verbatim (key-independent
//! per-file keys) and re-mints only the trailer, bound to the destination key. Covers the small-body
//! re-encrypt path and the large-body server-side `UploadPartCopy` path, single-part and composite
//! sources, `COPY`/`REPLACE` metadata directives, the copy-source preconditions, in-place metadata
//! edit, and the missing-source 404.

mod common;

use common::*;

const B: &str = "cpy";

/// Copy the destination ETag out of a `CopyObject`.
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
    let h = Harness::durable().await;
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
        Some("green")
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
        Some("blue")
    );
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

/// A same-key `REPLACE` copy is an in-place metadata edit: the body is unchanged, the metadata swaps.
#[tokio::test]
async fn copy_in_place_metadata_edit() {
    let h = Harness::durable().await;
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
        Some("2")
    );
    assert_eq!(
        get_all(&client, B, "obj").await,
        body,
        "body unchanged by the edit"
    );
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
