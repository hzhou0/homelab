//! Phase-3 exit: the multipart path end-to-end (§7). Parts route around the cache onto the
//! remote's native multipart upload as independent age files; complete lands a terminating trailer
//! part carrying the facts + parts table, and reads recover every part boundary from that trailer
//! alone. Covers out-of-order/parallel parts, re-upload last-write-wins resolution, composite ETag
//! correctness, single-stream + ranged composite GET (uniform and ragged parts), abort cleanup,
//! process restart mid-upload, the part-number cap, and trailer-based recovery after a mid-complete
//! crash mark.

mod common;

use std::collections::HashMap;

use common::*;
use hypha_core::meta;

const B: &str = "mpu";

/// Out-of-order parts, ragged sizes, composite ETag, and whole + ranged composite GET off the
/// trailer's offset table.
#[tokio::test]
async fn multipart_roundtrip_ranges_and_etag() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "big/object";

    // Ragged geometry: two full 5 MiB parts and a short tail — exercises non-uniform boundaries.
    let p1 = pattern_seeded(MIN_PART, 1);
    let p2 = pattern_seeded(MIN_PART, 2);
    let p3 = pattern_seeded(3 * 1024 * 1024, 3);
    let whole: Vec<u8> = [p1.as_slice(), p2.as_slice(), p3.as_slice()].concat();

    let up = create_mpu(&client, B, key).await;
    // Upload out of order (2, then 3, then 1); part order is only asserted at complete.
    let e2 = upload_part(&client, B, key, &up, 2, &p2).await;
    let e3 = upload_part(&client, B, key, &up, 3, &p3).await;
    let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
    assert_eq!(e1, md5_hex(&p1), "part ETag is the plaintext MD5");

    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2), (3, e3)]).await;
    assert_eq!(
        etag,
        expected_composite_etag(&[&p1, &p2, &p3]),
        "composite ETag must be md5(pmd5s)-N"
    );

    // HEAD reports the total plaintext length and the composite ETag.
    let head = client
        .head_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("head");
    assert_eq!(head.content_length(), Some(whole.len() as i64));

    // Whole-object composite GET.
    assert_eq!(
        get_all(&client, B, key).await,
        whole,
        "single-stream composite GET"
    );

    // Ranges: within part 1, straddling the 1↔2 boundary, straddling 2↔3, within part 3, suffix.
    let cases = [
        (0u64, 100u64),
        (MIN_PART as u64 - 10, MIN_PART as u64 + 10),
        (2 * MIN_PART as u64 - 5, 2 * MIN_PART as u64 + 5),
        (2 * MIN_PART as u64 + 1000, 2 * MIN_PART as u64 + 2000),
    ];
    for (a, b) in cases {
        assert_eq!(
            get_range(&client, B, key, a, b).await,
            whole[a as usize..=b as usize],
            "range {a}..={b} across composite parts"
        );
    }
    assert_eq!(
        get_suffix(&client, B, key, 4096).await,
        whole[whole.len() - 4096..]
    );
}

/// A re-uploaded part's stale record is resolved away at complete by the remote's `ListParts`; the
/// surviving object reflects the *last* upload, and no mpu records linger.
#[tokio::test]
async fn multipart_reupload_resolution() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "reupload";

    let p1 = pattern_seeded(MIN_PART, 10);
    let p2_old = pattern_seeded(MIN_PART, 20);
    let p2_new = pattern_seeded(MIN_PART, 21);

    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
    let _stale = upload_part(&client, B, key, &up, 2, &p2_old).await;
    let e2 = upload_part(&client, B, key, &up, 2, &p2_new).await; // supersedes the stale part

    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2)]).await;
    assert_eq!(
        etag,
        expected_composite_etag(&[&p1, &p2_new]),
        "winner is the re-upload"
    );

    let whole = get_all(&client, B, key).await;
    assert_eq!(
        &whole[MIN_PART..],
        p2_new.as_slice(),
        "part 2 must be the re-uploaded bytes"
    );

    // All per-upload records (including the superseded one) are dropped at complete.
    let residue = raw_list(&h.raw(), &h.cache_bucket(B), Some(meta::RESERVED_PREFIX)).await;
    assert!(
        residue.is_empty(),
        "mpu records must be swept at complete, found {residue:?}"
    );
}

/// Two concurrent uploads of the same part number: the remote keeps one, complete resolves to it,
/// and the object is coherent (part 2 equals one of the two candidates).
#[tokio::test]
async fn multipart_concurrent_same_part() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "concurrent";

    let p1 = pattern_seeded(MIN_PART, 30);
    let a = pattern_seeded(MIN_PART, 40);
    let b = pattern_seeded(MIN_PART, 41);

    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
    // Race two writers on part 2.
    let (ra, rb) = tokio::join!(
        upload_part(&client, B, key, &up, 2, &a),
        upload_part(&client, B, key, &up, 2, &b),
    );
    assert_ne!(
        ra, rb,
        "the two candidate parts have distinct plaintext MD5s"
    );

    // Let hypha resolve the winner via the remote's ListParts (omit the part-2 ETag).
    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, String::new())]).await;

    let whole = get_all(&client, B, key).await;
    let part2 = &whole[MIN_PART..];
    assert!(
        part2 == a.as_slice() || part2 == b.as_slice(),
        "part 2 must be exactly one of the concurrent uploads"
    );
    // Whichever won, the composite ETag reflects that part's plaintext MD5.
    let want = if part2 == a.as_slice() {
        expected_composite_etag(&[&p1, &a])
    } else {
        expected_composite_etag(&[&p1, &b])
    };
    assert_eq!(etag, want);
}

/// Concurrent uploads of a *small* final part (the fold path): the object's bytes must match the
/// remote's winning part and its composite ETag must agree with those same bytes — i.e. the fold
/// takes the remote's `ListParts` winner, not a divergent cache last-writer.
#[tokio::test]
async fn multipart_concurrent_small_final_part() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "concurrent-tail";

    let p1 = pattern_seeded(MIN_PART, 90); // full part
    let a = pattern_seeded(256 * 1024, 91); // small final-part candidates
    let b = pattern_seeded(256 * 1024, 92);

    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
    let (_ra, _rb) = tokio::join!(
        upload_part(&client, B, key, &up, 2, &a),
        upload_part(&client, B, key, &up, 2, &b),
    );

    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, String::new())]).await;
    let whole = get_all(&client, B, key).await;
    let tail = &whole[MIN_PART..];
    assert!(
        tail == a.as_slice() || tail == b.as_slice(),
        "final part is one candidate"
    );
    let want = if tail == a.as_slice() {
        expected_composite_etag(&[&p1, &a])
    } else {
        expected_composite_etag(&[&p1, &b])
    };
    assert_eq!(
        etag, want,
        "composite ETag must match the folded winner's bytes"
    );
}

/// Abort drops the upload: its records vanish and it can no longer be completed.
#[tokio::test]
async fn multipart_abort_cleanup() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "aborted";

    let up = create_mpu(&client, B, key).await;
    upload_part(&client, B, key, &up, 1, &pattern_seeded(MIN_PART, 50)).await;
    upload_part(&client, B, key, &up, 2, &pattern_seeded(MIN_PART, 51)).await;

    client
        .abort_multipart_upload()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .send()
        .await
        .expect("abort");

    let residue = raw_list(&h.raw(), &h.cache_bucket(B), Some(meta::RESERVED_PREFIX)).await;
    assert!(
        residue.is_empty(),
        "abort must sweep mpu records, found {residue:?}"
    );

    // Completing an aborted upload fails, and the object was never created.
    let done = complete_mpu_res(
        &client,
        B,
        key,
        &up,
        &[(1, String::new()), (2, String::new())],
    )
    .await;
    assert!(done.is_err(), "completing an aborted upload must fail");
    let get = client.get_object().bucket(B).key(key).send().await;
    assert_eq!(
        sdk_err_code(&get.unwrap_err()).as_deref(),
        Some("NoSuchKey")
    );
}

/// A process restart mid-upload: the upload's cache-resident records survive, so a fresh hypha
/// finishes the upload and the object is correct.
#[tokio::test]
async fn multipart_restart_mid_upload() {
    let mut h = Harness::durable().await;
    h.create_bucket(B).await;
    let key = "resumed";

    let p1 = pattern_seeded(MIN_PART, 60);
    let p2 = pattern_seeded(MIN_PART, 61);

    let (up, e1) = {
        let client = h.client();
        let up = create_mpu(&client, B, key).await;
        let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
        (up, e1)
    };

    h.restart_hypha().await;

    let client = h.client();
    let e2 = upload_part(&client, B, key, &up, 2, &p2).await;
    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2)]).await;
    assert_eq!(etag, expected_composite_etag(&[&p1, &p2]));

    let whole: Vec<u8> = [p1.as_slice(), p2.as_slice()].concat();
    assert_eq!(
        get_all(&client, B, key).await,
        whole,
        "object correct after restart mid-upload"
    );
}

/// A multipart upload whose only/last part is below the 5 MiB backend minimum: the trailer folds
/// into that part (it stays the final part), so complete succeeds where a separate trailer part
/// would have demoted it to an illegal sub-minimum non-final part. The common "small object over
/// the multipart API" case.
#[tokio::test]
async fn multipart_small_final_part_folds_trailer() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    // Single small part.
    let key = "tiny/single";
    let body = pattern_seeded(128 * 1024, 80);
    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part(&client, B, key, &up, 1, &body).await;
    let etag = complete_mpu(&client, B, key, &up, &[(1, e1)]).await;
    assert_eq!(etag, expected_composite_etag(&[&body]));
    assert_eq!(
        get_all(&client, B, key).await,
        body,
        "single small-part composite"
    );
    assert_eq!(get_range(&client, B, key, 10, 20).await, body[10..=20]);

    // Full 5 MiB part followed by a small tail — the tail (highest) folds the trailer.
    let key2 = "big/small-tail";
    let p1 = pattern_seeded(MIN_PART, 81);
    let p2 = pattern_seeded(64 * 1024, 82);
    let whole: Vec<u8> = [p1.as_slice(), p2.as_slice()].concat();
    let up2 = create_mpu(&client, B, key2).await;
    let a1 = upload_part(&client, B, key2, &up2, 1, &p1).await;
    let a2 = upload_part(&client, B, key2, &up2, 2, &p2).await;
    complete_mpu(&client, B, key2, &up2, &[(1, a1), (2, a2)]).await;
    assert_eq!(get_all(&client, B, key2).await, whole);
    // Straddle the boundary into the folded final part.
    assert_eq!(
        get_range(&client, B, key2, MIN_PART as u64 - 3, MIN_PART as u64 + 3).await,
        whole[MIN_PART - 3..=MIN_PART + 3]
    );
}

/// A part number above hypha's 9999 client cap (10000 is the reserved trailer part) is rejected.
#[tokio::test]
async fn multipart_part_number_cap() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "capped";

    let up = create_mpu(&client, B, key).await;
    let res = client
        .upload_part()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .part_number(10_000)
        .body(bytes_body(&pattern(1024)))
        .content_length(1024)
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&res.unwrap_err()).as_deref(),
        Some("InvalidPart")
    );
}

/// Trailer-based recovery: after a completed composite, plant the crash-window state a mid-complete
/// death leaves — a lone transition mark at the key — and assert a read reconstructs the facts and
/// the parts table from the terminating trailer part on the remote, with correct bytes and ETag,
/// then settles the cache back to a tombstone. (Full cache-wipe restore is the phase-5 sweep; the
/// mark-driven repair is the phase-3-testable core of it.)
#[tokio::test]
async fn multipart_restore_from_trailer() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "restore/me";

    let p1 = pattern_seeded(MIN_PART, 70);
    let p2 = pattern_seeded(2 * 1024 * 1024, 71);
    let whole: Vec<u8> = [p1.as_slice(), p2.as_slice()].concat();

    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
    let e2 = upload_part(&client, B, key, &up, 2, &p2).await;
    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2)]).await;

    // Plant the mid-complete crash residue: overwrite the cache key with the transition mark, so
    // the cache no longer carries the facts and the read must resolve them from the remote trailer.
    let md = HashMap::from([(meta::TOMB.to_string(), meta::TOMB_TRANSIT.to_string())]);
    raw_cache_put(&h, B, key, meta::TRANSIT_SENTINEL.to_vec(), md).await;

    // HEAD and GET both recover from the trailer alone.
    let head = client
        .head_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("head after mark");
    assert_eq!(head.content_length(), Some(whole.len() as i64));
    assert_eq!(head.e_tag().unwrap().trim_matches('"'), etag);
    assert_eq!(
        get_all(&client, B, key).await,
        whole,
        "composite recovered from trailer"
    );
    // A boundary-straddling range still resolves off the recovered parts table.
    assert_eq!(
        get_range(&client, B, key, MIN_PART as u64 - 4, MIN_PART as u64 + 4).await,
        whole[MIN_PART - 4..=MIN_PART + 4]
    );

    // The read repaired the cache: the key is back to an eviction tombstone (no lingering mark).
    let head2 = h
        .raw()
        .head_object()
        .bucket(h.cache_bucket(B))
        .key(key)
        .send()
        .await
        .expect("cache head after repair");
    let tomb = head2.metadata().and_then(|m| m.get(meta::TOMB));
    assert_eq!(
        tomb.map(String::as_str),
        Some(meta::TOMB_EVICT),
        "mark must settle to a tombstone"
    );
}

// ── helpers ──────────────────────────────────────────────────────────────────────────────────

/// Like [`complete_mpu`] but returns the `Result` so failure can be asserted.
async fn complete_mpu_res(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: &[(i32, String)],
) -> Result<
    (),
    aws_sdk_s3::error::SdkError<
        aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError,
    >,
> {
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    let completed: Vec<CompletedPart> = parts
        .iter()
        .map(|(n, etag)| {
            let mut b = CompletedPart::builder().part_number(*n);
            if !etag.is_empty() {
                b = b.e_tag(etag.clone());
            }
            b.build()
        })
        .collect();
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await
        .map(|_| ())
}
