//! Multipart completion, retry, recovery, and composite read behavior.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::*;
use hypha_core::meta;

const B: &str = "mpu";

async fn wait_for_mpu_cleanup(h: &Harness, upload_id: &str) {
    let prefix = meta::mpu_prefix(upload_id);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let residue = raw_list(&h.raw(), &h.meta_bucket(B), Some(&prefix)).await;
        if residue.is_empty() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "multipart records were not swept for {upload_id}: {residue:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

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

#[tokio::test]
async fn upload_part_validates_content_md5() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "part-md5";
    let good = pattern_seeded(4096, 8);
    let rejected = pattern_seeded(4096, 9);
    let up = create_mpu(&client, B, key).await;
    let good_etag = upload_part(&client, B, key, &up, 1, &good).await;

    let err = client
        .upload_part()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .part_number(1)
        .body(bytes_body(&rejected))
        .content_length(rejected.len() as i64)
        .content_md5(base64_md5(b"different bytes"))
        .send()
        .await
        .expect_err("a mismatched Content-MD5 must reject the part");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("BadDigest"));

    let listed = client
        .list_parts()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .send()
        .await
        .expect("list after rejected part");
    let parts = listed.parts();
    assert_eq!(parts.len(), 1, "the rejected part must not land");
    assert_eq!(
        parts[0].e_tag().map(|etag| etag.trim_matches('"')),
        Some(good_etag.as_str()),
        "a rejected re-upload must leave the previous generation intact"
    );
}

/// The remote part can commit before its cache-resident plaintext-MD5 record fails. That upload is
/// not falsely acknowledged or completable; a normal re-upload supplies a new winning part and
/// record, after which completion succeeds.
#[tokio::test]
async fn multipart_part_record_failure_requires_a_reupload() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "part-record-fault";
    let body = pattern_seeded(MIN_PART, 11);
    let up = create_mpu(&client, B, key).await;

    let faults = h.cache_faults();
    let failed = faults.fail_prefix_times(
        hyper::Method::PUT,
        format!("/{}/%01%01m", h.meta_bucket(B)),
        hyper::StatusCode::FORBIDDEN,
        8,
    );
    let upload = client
        .upload_part()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .part_number(1)
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .send()
        .await;
    let record = tokio::time::timeout(Duration::from_secs(5), failed)
        .await
        .expect("part record write was never attempted")
        .expect("fault proxy stopped before failing the part record");
    assert!(
        upload.is_err(),
        "the intercepted path was {}, but UploadPart succeeded",
        record.path
    );
    faults.clear();
    assert!(
        record.path.to_ascii_lowercase().contains("%01%01m"),
        "the injected write must be the MPU facts record: {}",
        record.path
    );

    // The ciphertext may reach the remote before the local facts record, but without both halves
    // hypha must not acknowledge or complete it.
    let remote_parts = h
        .raw_remote()
        .list_parts()
        .bucket(h.remote_bucket(B))
        .key(key)
        .upload_id(&up)
        .send()
        .await
        .expect("raw remote ListParts after record failure");
    assert!(
        !remote_parts.parts().is_empty(),
        "the backend part committed before its local record failed"
    );

    complete_mpu_res(&client, B, key, &up, &[(1, md5_hex(&body))])
        .await
        .expect_err("a remote part without its pmd5 record is not completable");

    let etag = upload_part(&client, B, key, &up, 1, &body).await;
    let complete = complete_mpu(&client, B, key, &up, &[(1, etag)]).await;
    assert_eq!(complete, expected_composite_etag(&[&body]));
    assert_eq!(get_all(&client, B, key).await, body);
}

#[tokio::test]
async fn complete_requires_every_part_etag() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "missing-complete-etag";
    let up = create_mpu(&client, B, key).await;
    upload_part(&client, B, key, &up, 1, &pattern_seeded(4096, 10)).await;

    let err = complete_mpu_res(&client, B, key, &up, &[(1, String::new())])
        .await
        .expect_err("a completed part without its ETag must be rejected");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("InvalidPart"));
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

    wait_for_mpu_cleanup(&h, &up).await;
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

    let e2 = listed_part_etag(&client, B, key, &up, 2).await;
    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2)]).await;

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

    let e2 = listed_part_etag(&client, B, key, &up, 2).await;
    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2)]).await;
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

/// Abort drops the upload: the remote stops running it, it can no longer be completed, and its
/// records are reclaimed — by the sweep, not on the abort itself. A maxed upload's range is 10 000
/// single-object deletes (its keys carry `0x01`, so no batch delete can represent them), and paying
/// that on the client's call to say "throw this away" is the cost the deferral exists to remove.
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

    wait_for_mpu_cleanup(&h, &up).await;

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
    for n in [0, 10_001] {
        let res = client
            .upload_part()
            .bucket(B)
            .key(key)
            .upload_id(&up)
            .part_number(n)
            .body(bytes_body(&pattern(1024)))
            .content_length(1024)
            .send()
            .await;
        assert_eq!(
            sdk_err_code(&res.unwrap_err()).as_deref(),
            Some("InvalidPart"),
            "part number {n} is outside S3's range"
        );
    }
}

/// Part 10000 is usable, and it is the case where no trailer part can follow: the trailer must fold
/// into it even though it is far above the 5 MiB minimum that drives the other fold. Uses a sparse
/// part set (1, 10000) so the upload stays cheap while still ending on the last legal number.
#[tokio::test]
async fn multipart_last_part_number_folds_trailer() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "parts/at-the-limit";

    // Both parts clear the 5 MiB minimum, so only the part number can force the fold.
    let p1 = pattern_seeded(MIN_PART, 80);
    let p2 = pattern_seeded(MIN_PART + 4096, 81);
    let whole: Vec<u8> = [p1.as_slice(), p2.as_slice()].concat();

    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
    let e2 = upload_part(&client, B, key, &up, 10_000, &p2).await;
    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (10_000, e2)]).await;

    assert_eq!(etag, expected_composite_etag(&[&p1, &p2]));
    assert_eq!(get_all(&client, B, key).await, whole);
    // The fold must not disturb the geometry the parts table describes.
    assert_eq!(
        get_range(&client, B, key, MIN_PART as u64 - 8, MIN_PART as u64 + 8).await,
        whole[MIN_PART - 8..MIN_PART + 9]
    );

    // The committed object carries exactly the client's parts — no trailer part above 10000.
    let raw = h
        .raw_remote()
        .head_object()
        .bucket(h.remote_bucket(B))
        .key(key)
        .part_number(1)
        .send()
        .await
        .expect("head part 1");
    assert_eq!(
        raw.parts_count(),
        Some(2),
        "the trailer rode part 10000, so the object has two parts"
    );

    // The retained ciphertext that made the fold possible is reclaimed with the rest of this
    // upload's records by the asynchronous debris sweep.
    wait_for_mpu_cleanup(&h, &up).await;
}

/// `ListParts` reports the *plaintext* view of an in-progress upload — client part numbers, the
/// plaintext MD5s hypha handed back at upload, and plaintext sizes — never the ciphertext geometry
/// the remote actually holds. Pagination is over that same view.
#[tokio::test]
async fn list_parts_reports_plaintext_view() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "listable/parts";

    let p1 = pattern_seeded(MIN_PART, 90);
    let p2 = pattern_seeded(MIN_PART + 1234, 91);
    let p3 = pattern_seeded(4096, 92);
    let up = create_mpu(&client, B, key).await;
    // Out of order, to prove the listing sorts by part number rather than arrival.
    upload_part(&client, B, key, &up, 3, &p3).await;
    upload_part(&client, B, key, &up, 1, &p1).await;
    upload_part(&client, B, key, &up, 2, &p2).await;

    let out = client
        .list_parts()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .send()
        .await
        .expect("list_parts");
    let parts = out.parts();
    assert_eq!(parts.len(), 3);
    for (i, (body, n)) in [(&p1, 1), (&p2, 2), (&p3, 3)].iter().enumerate() {
        assert_eq!(parts[i].part_number(), Some(*n));
        assert_eq!(
            parts[i].e_tag().unwrap_or_default().trim_matches('"'),
            md5_hex(body),
            "part {n} ETag is the plaintext MD5"
        );
        assert_eq!(
            parts[i].size(),
            Some(body.len() as i64),
            "part {n} size is the plaintext length, not the ciphertext's"
        );
    }

    // Pagination over the plaintext view.
    let page = client
        .list_parts()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .max_parts(2)
        .send()
        .await
        .expect("list_parts page 1");
    assert_eq!(page.parts().len(), 2);
    assert_eq!(page.is_truncated(), Some(true));
    assert_eq!(page.next_part_number_marker(), Some("2"));

    let rest = client
        .list_parts()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .part_number_marker("2")
        .send()
        .await
        .expect("list_parts page 2");
    assert_eq!(rest.parts().len(), 1);
    assert_eq!(rest.parts()[0].part_number(), Some(3));
    assert_eq!(rest.is_truncated(), Some(false));

    // An upload hypha doesn't know is not listable.
    let err = client
        .list_parts()
        .bucket(B)
        .key(key)
        .upload_id("no-such-upload")
        .send()
        .await
        .expect_err("unknown upload id");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("NoSuchUpload"));
}

/// `ListMultipartUploads` proxies the remote, which holds the client key and the client's own
/// upload id — so uploads appear under the keys the client used, in `(key, upload_id)` order, and
/// disappear on complete or abort.
#[tokio::test]
async fn list_multipart_uploads_tracks_in_progress() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    assert!(listed_uploads(&client, None).await.is_empty());

    let a = create_mpu(&client, B, "docs/a").await;
    let b = create_mpu(&client, B, "docs/b").await;
    let c = create_mpu(&client, B, "other/c").await;

    // Keys are the client's, ordered, and the ids round-trip as handed out at create.
    assert_eq!(
        listed_uploads(&client, None).await,
        vec![
            ("docs/a".to_string(), a.clone()),
            ("docs/b".to_string(), b.clone()),
            ("other/c".to_string(), c.clone()),
        ]
    );

    let body = pattern_seeded(MIN_PART, 93);
    let e = upload_part(&client, B, "docs/a", &a, 1, &body).await;
    complete_mpu(&client, B, "docs/a", &a, &[(1, e)]).await;
    client
        .abort_multipart_upload()
        .bucket(B)
        .key("docs/b")
        .upload_id(&b)
        .send()
        .await
        .expect("abort");

    assert_eq!(
        listed_uploads(&client, None).await,
        vec![("other/c".to_string(), c)],
        "completed and aborted uploads must drop out"
    );
}

/// `prefix` filters in-progress uploads by client key, per the S3 spec — hypha forwards it to the
/// remote, which is the only thing that can answer it.
///
/// **Ignored: the integration harness runs MinIO, which does not implement this.** MinIO returns
/// matches only when the prefix equals a key exactly, closed "working as intended"
/// (minio/minio#20989, #11686) — so this asserts hypha's contract against a compliant backend
/// rather than the harness's. Run it against one with
/// `cargo test --test multipart -- --ignored prefix`.
#[tokio::test]
#[ignore = "MinIO does not implement prefix on ListMultipartUploads (minio/minio#20989)"]
async fn list_multipart_uploads_prefix_filter() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let a = create_mpu(&client, B, "docs/a").await;
    let b = create_mpu(&client, B, "docs/b").await;
    create_mpu(&client, B, "other/c").await;

    assert_eq!(
        listed_uploads(&client, Some("docs/")).await,
        vec![("docs/a".to_string(), a), ("docs/b".to_string(), b)],
        "prefix filters on the client key"
    );
    assert!(listed_uploads(&client, Some("nothing/")).await.is_empty());
}

/// In-progress uploads as `(client key, upload id)`, in listing order.
async fn listed_uploads(
    client: &aws_sdk_s3::Client,
    prefix: Option<&str>,
) -> Vec<(String, String)> {
    let mut req = client.list_multipart_uploads().bucket(B);
    if let Some(p) = prefix {
        req = req.prefix(p);
    }
    req.send()
        .await
        .expect("list_multipart_uploads")
        .uploads()
        .iter()
        .map(|u| {
            (
                u.key().unwrap_or_default().to_string(),
                u.upload_id().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

/// Trailer-based repair: after a completed composite, plant the crash-window state a mid-complete
/// death leaves — a lone transition mark at the key — and assert a read reconstructs the facts and
/// parts table from the terminating trailer, then settles the cache back to a tombstone.
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

/// If the remote rejects the native complete, repair must expose the prior object and leave the
/// upload available for an explicit retry.
#[tokio::test]
async fn multipart_failed_complete_restores_the_previous_generation() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "complete/refused";
    let old = b"old generation";
    put(&client, B, key, old).await;

    let part = pattern_seeded(MIN_PART, 76);
    let up = create_mpu(&client, B, key).await;
    let etag = upload_part(&client, B, key, &up, 1, &part).await;
    let faults = h.remote_faults();
    let refused = faults.fail_times(
        hyper::Method::POST,
        format!("/{}/{key}", h.remote_bucket(B)),
        hyper::StatusCode::PRECONDITION_FAILED,
        8,
    );
    complete_mpu_res(&client, B, key, &up, &[(1, etag.clone())])
        .await
        .expect_err("the injected complete failure must reach the client");
    tokio::time::timeout(Duration::from_secs(5), refused)
        .await
        .expect("complete request never reached the remote")
        .expect("fault proxy stopped before refusing complete");
    faults.clear();

    assert_eq!(
        get_all(&client, B, key).await,
        old,
        "the refused complete must restore the previous object"
    );
    let cached = h
        .raw()
        .head_object()
        .bucket(h.cache_bucket(B))
        .key(key)
        .send()
        .await
        .expect("cache projection after refused complete");
    assert_eq!(
        cached
            .metadata()
            .and_then(|md| md.get(meta::TOMB))
            .map(String::as_str),
        Some(meta::TOMB_EVICT),
        "repair must leave no transition mark"
    );

    complete_mpu(&client, B, key, &up, &[(1, etag)]).await;
    assert_eq!(
        get_all(&client, B, key).await,
        part,
        "the uncommitted native upload must remain retryable"
    );
}

/// Folding replaces the client's final part on a compliant backend. If native completion is then
/// refused, the persisted intent must let a retry restore the retained pure part before folding it
/// again. Both reasons a part cannot take a trailer successor exercise the same recovery.
#[tokio::test]
async fn multipart_failed_folded_complete_is_retryable() {
    let mut h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;

    for (case, part_number) in [("small", 1), ("last-number", 10_000)] {
        let client = h.client();
        let key = format!("complete/folded-{case}");
        let old = format!("old generation for {case}");
        put(&client, B, &key, old.as_bytes()).await;

        let part = pattern_seeded(256 * 1024, part_number as u8);
        let up = create_mpu(&client, B, &key).await;
        let etag = upload_part(&client, B, &key, &up, part_number, &part).await;
        let faults = h.remote_faults();
        let refused = faults.fail_times(
            hyper::Method::POST,
            format!("/{}/{key}", h.remote_bucket(B)),
            hyper::StatusCode::PRECONDITION_FAILED,
            8,
        );
        complete_mpu_res(&client, B, &key, &up, &[(part_number, etag.clone())])
            .await
            .expect_err("the injected native completion failure must reach the client");
        tokio::time::timeout(Duration::from_secs(5), refused)
            .await
            .expect("complete request never reached the remote")
            .expect("fault proxy stopped before refusing complete");
        faults.clear();

        assert_eq!(
            get_all(&client, B, &key).await,
            old.as_bytes(),
            "the failed folded completion must preserve the previous object"
        );
        let rejected = pattern_seeded(256 * 1024, part_number as u8 ^ 0x80);
        let err = client
            .upload_part()
            .bucket(B)
            .key(&key)
            .upload_id(&up)
            .part_number(part_number)
            .body(bytes_body(&rejected))
            .content_length(rejected.len() as i64)
            .content_md5(base64_md5(b"different bytes"))
            .send()
            .await
            .expect_err("a rejected replacement must not supersede the fold intent");
        assert_eq!(sdk_err_code(&err).as_deref(), Some("BadDigest"));
        drop(client);
        h.restart_hypha().await;

        let client = h.client();
        complete_mpu(&client, B, &key, &up, &[(part_number, etag)]).await;
        assert_eq!(
            get_all(&client, B, &key).await,
            part,
            "the retry must unfold and refold the retained part"
        );
    }
}

/// The remote may commit CompleteMultipartUpload while its response is lost. The client receives an
/// error, but the failed-commit repair must discover the committed trailer and settle a coherent
/// cache projection before returning.
#[tokio::test]
async fn multipart_lost_complete_response_repairs_the_committed_object() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "complete/lost-response";
    put(&client, B, key, b"old generation").await;

    let p1 = pattern_seeded(MIN_PART, 72);
    let p2 = pattern_seeded(512 * 1024, 73);
    let whole: Vec<u8> = [p1.as_slice(), p2.as_slice()].concat();
    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
    let e2 = upload_part(&client, B, key, &up, 2, &p2).await;

    let lost = h.remote_faults().fail_next_response(
        hyper::Method::POST,
        format!("/{}/{key}", h.remote_bucket(B)),
        hyper::StatusCode::FORBIDDEN,
    );
    complete_mpu_res(&client, B, key, &up, &[(1, e1), (2, e2)])
        .await
        .expect_err("the injected response loss must reach the client as an error");
    let complete = tokio::time::timeout(Duration::from_secs(5), lost)
        .await
        .expect("complete request never reached the remote")
        .expect("fault proxy stopped before complete");
    assert!(
        complete.path.contains("uploadId="),
        "the intercepted POST must be CompleteMultipartUpload"
    );

    assert_eq!(
        get_all(&client, B, key).await,
        whole,
        "repair must expose the fully committed generation, never the old/new hybrid"
    );
    let head = client
        .head_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("head repaired composite");
    assert_eq!(
        head.e_tag().map(|e| e.trim_matches('"')),
        Some(expected_composite_etag(&[&p1, &p2]).as_str())
    );
    let cached = h
        .raw()
        .head_object()
        .bucket(h.cache_bucket(B))
        .key(key)
        .send()
        .await
        .expect("cache projection after repair");
    assert_eq!(
        cached
            .metadata()
            .and_then(|md| md.get(meta::TOMB))
            .map(String::as_str),
        Some(meta::TOMB_EVICT),
        "the failed-commit path must settle the transition mark"
    );
}

/// A total cache-volume loss discards every multipart record and projection. The completed remote
/// object remains self-describing: R1 rebuilds its tombstone from the terminating trailer alone.
#[tokio::test]
async fn multipart_cache_wipe_restores_facts_and_part_geometry() {
    let mut h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "restore/cache-wipe";

    let p1 = pattern_seeded(MIN_PART, 74);
    let p2 = pattern_seeded(768 * 1024, 75);
    let whole: Vec<u8> = [p1.as_slice(), p2.as_slice()].concat();
    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
    let e2 = upload_part(&client, B, key, &up, 2, &p2).await;
    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2)]).await;
    drop(client);

    h.stop_hypha().await;
    drop_backend_bucket(&h, &h.cache_bucket(B)).await;
    drop_backend_bucket(&h, &h.meta_bucket(B)).await;
    h.start_hypha().await;

    let client = h.client();
    let head = client
        .head_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("head after cache-volume restore");
    assert_eq!(head.content_length(), Some(whole.len() as i64));
    assert_eq!(
        head.e_tag().map(|e| e.trim_matches('"')),
        Some(etag.as_str())
    );
    assert_eq!(get_all(&client, B, key).await, whole);
    assert_eq!(
        get_range(&client, B, key, MIN_PART as u64 - 4, MIN_PART as u64 + 4).await,
        whole[MIN_PART - 4..=MIN_PART + 4],
        "the restored trailer table must retain the original part boundary"
    );

    let cached = h
        .raw()
        .head_object()
        .bucket(h.cache_bucket(B))
        .key(key)
        .send()
        .await
        .expect("restored cache projection");
    assert_eq!(
        cached
            .metadata()
            .and_then(|md| md.get(meta::TOMB))
            .map(String::as_str),
        Some(meta::TOMB_EVICT)
    );
}

/// GetObjectAttributes `ObjectParts` for a composite comes straight off the trailer's offset table
/// : total part count and per-part *plaintext* sizes, no remote part index — and it paginates.
#[tokio::test]
async fn get_object_attributes_composite_parts() {
    use aws_sdk_s3::types::ObjectAttributes;
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "composite/obj";

    // Ragged: two 5 MiB parts and a 3 MiB tail (< MIN_PART, so the trailer folds into part 3).
    let tail_len = 3 * 1024 * 1024;
    let p1 = pattern_seeded(MIN_PART, 1);
    let p2 = pattern_seeded(MIN_PART, 2);
    let p3 = pattern_seeded(tail_len, 3);
    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part(&client, B, key, &up, 1, &p1).await;
    let e2 = upload_part(&client, B, key, &up, 2, &p2).await;
    let e3 = upload_part(&client, B, key, &up, 3, &p3).await;
    complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2), (3, e3)]).await;

    let out = client
        .get_object_attributes()
        .bucket(B)
        .key(key)
        .object_attributes(ObjectAttributes::ObjectSize)
        .object_attributes(ObjectAttributes::ObjectParts)
        .send()
        .await
        .expect("get object attributes (composite)");

    assert_eq!(out.object_size(), Some((2 * MIN_PART + tail_len) as i64));
    let parts = out.object_parts().expect("composite reports ObjectParts");
    assert_eq!(parts.total_parts_count(), Some(3));
    let nums: Vec<i32> = parts
        .parts()
        .iter()
        .filter_map(|p| p.part_number())
        .collect();
    assert_eq!(nums, vec![1, 2, 3]);
    let sizes: Vec<i64> = parts.parts().iter().filter_map(|p| p.size()).collect();
    // Per-part *plaintext* sizes off the trailer table — the trailer folded into part 3 doesn't
    // inflate its reported size.
    assert_eq!(
        sizes,
        vec![MIN_PART as i64, MIN_PART as i64, tail_len as i64]
    );

    // Pagination truncates at max_parts.
    let page = client
        .get_object_attributes()
        .bucket(B)
        .key(key)
        .object_attributes(ObjectAttributes::ObjectParts)
        .max_parts(2)
        .send()
        .await
        .expect("get object attributes (paged)");
    let pp = page.object_parts().expect("ObjectParts");
    assert_eq!(pp.is_truncated(), Some(true));
    assert_eq!(pp.parts().len(), 2);
    assert_eq!(pp.total_parts_count(), Some(3));
}

/// UploadPartCopy fast path : a whole, single-part source copies **server-side** as a part,
/// with `pmd5` = the source's plaintext MD5 (its single-part cetag). Source is part 1 (≥ 5 MiB, so
/// it admits a successor and stays on the fast path); a small uploaded tail follows. The completed
/// object is `source ‖ tail`.
#[tokio::test]
async fn upload_part_copy_whole_single_part() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let src = pattern_seeded(MIN_PART, 9);
    put(&client, B, "src/obj", &src).await;

    let tail = pattern_seeded(4096, 8);
    let key = "dst/copied";
    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part_copy(&client, B, key, &up, 1, B, "src/obj", None).await;
    assert_eq!(
        e1,
        md5_hex(&src),
        "copied-part ETag is the source's plaintext MD5"
    );
    let e2 = upload_part(&client, B, key, &up, 2, &tail).await;

    let etag = complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2)]).await;
    assert_eq!(etag, expected_composite_etag(&[&src, &tail]));

    let whole: Vec<u8> = [src.as_slice(), tail.as_slice()].concat();
    assert_eq!(get_all(&client, B, key).await, whole);
}

/// A ranged UploadPartCopy re-encrypts (the fast path is whole-object only): copy a 5 MiB slice at
/// a non-zero offset out of a larger single-part source as part 1, then a small tail. The offset
/// catches a mis-sliced range.
#[tokio::test]
async fn upload_part_copy_range_reencrypts() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let src = pattern_seeded(MIN_PART + 4096, 5);
    put(&client, B, "src/ranged", &src).await;
    let slice = &src[100..100 + MIN_PART];

    let tail = pattern_seeded(2048, 6);
    let key = "dst/ranged";
    let up = create_mpu(&client, B, key).await;
    let range = format!("bytes=100-{}", 100 + MIN_PART - 1);
    let e1 = upload_part_copy(&client, B, key, &up, 1, B, "src/ranged", Some(&range)).await;
    assert_eq!(e1, md5_hex(slice), "re-encrypted slice MD5");
    let e2 = upload_part(&client, B, key, &up, 2, &tail).await;

    complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2)]).await;
    let whole: Vec<u8> = [slice, tail.as_slice()].concat();
    assert_eq!(get_all(&client, B, key).await, whole);
}

/// A composite source re-encrypts on copy — each source part is its own age file, so the whole is
/// not a single reusable age file. Build a 2-part composite, copy the whole of it as one part, add
/// a tail, and verify the bytes.
#[tokio::test]
async fn upload_part_copy_composite_source_reencrypts() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let sp1 = pattern_seeded(MIN_PART, 1);
    let sp2 = pattern_seeded(1024 * 1024, 2);
    let src_whole: Vec<u8> = [sp1.as_slice(), sp2.as_slice()].concat();
    let sup = create_mpu(&client, B, "src/comp").await;
    let s1 = upload_part(&client, B, "src/comp", &sup, 1, &sp1).await;
    let s2 = upload_part(&client, B, "src/comp", &sup, 2, &sp2).await;
    complete_mpu(&client, B, "src/comp", &sup, &[(1, s1), (2, s2)]).await;

    let tail = pattern_seeded(4096, 3);
    let key = "dst/from-comp";
    let up = create_mpu(&client, B, key).await;
    let e1 = upload_part_copy(&client, B, key, &up, 1, B, "src/comp", None).await;
    assert_eq!(
        e1,
        md5_hex(&src_whole),
        "re-encrypted copy MD5 is over the whole source plaintext"
    );
    let e2 = upload_part(&client, B, key, &up, 2, &tail).await;

    complete_mpu(&client, B, key, &up, &[(1, e1), (2, e2)]).await;
    let whole: Vec<u8> = [src_whole.as_slice(), tail.as_slice()].concat();
    assert_eq!(get_all(&client, B, key).await, whole);
}

/// A copy from a source that does not exist is a client-visible 404.
#[tokio::test]
async fn upload_part_copy_missing_source() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let key = "dst/nope";
    let up = create_mpu(&client, B, key).await;
    let err = client
        .upload_part_copy()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .part_number(1)
        .copy_source(format!("{B}/does/not/exist"))
        .send()
        .await
        .expect_err("copy from a missing source must fail");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("NoSuchKey"),
        "missing copy source is a 404"
    );
}

#[tokio::test]
async fn upload_part_copy_source_preconditions() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let src = pattern_seeded(4096, 97);
    let etag = put(&client, B, "copy-part/source-cond", &src).await;
    let key = "copy-part/dest-cond";
    let up = create_mpu(&client, B, key).await;

    let err = client
        .upload_part_copy()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .part_number(1)
        .copy_source(format!("{B}/copy-part/source-cond"))
        .copy_source_if_match("\"00000000000000000000000000000000\"")
        .send()
        .await
        .expect_err("a stale copy-source If-Match must fail");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("PreconditionFailed"));

    let err = client
        .upload_part_copy()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .part_number(1)
        .copy_source(format!("{B}/copy-part/source-cond"))
        .copy_source_if_none_match(format!("\"{etag}\""))
        .send()
        .await
        .expect_err("a matching copy-source If-None-Match must fail");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("PreconditionFailed"));

    client
        .upload_part_copy()
        .bucket(B)
        .key(key)
        .upload_id(&up)
        .part_number(1)
        .copy_source(format!("{B}/copy-part/source-cond"))
        .copy_source_if_match(format!("\"{etag}\""))
        .send()
        .await
        .expect("a matching copy-source If-Match must pass");
}

// ── helpers ──────────────────────────────────────────────────────────────────────────────────

async fn listed_part_etag(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
) -> String {
    client
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .expect("list winning parts")
        .parts()
        .iter()
        .find(|part| part.part_number() == Some(part_number))
        .and_then(|part| part.e_tag())
        .expect("winning part etag")
        .trim_matches('"')
        .to_string()
}

/// Copy (a range of) a source object into part `part_number` of `upload_id`; returns the part ETag.
#[allow(clippy::too_many_arguments)]
async fn upload_part_copy(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: i32,
    src_bucket: &str,
    src_key: &str,
    range: Option<&str>,
) -> String {
    let mut b = client
        .upload_part_copy()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .copy_source(format!("{src_bucket}/{src_key}"));
    if let Some(r) = range {
        b = b.copy_source_range(r);
    }
    b.send()
        .await
        .unwrap_or_else(|e| panic!("upload_part_copy {part_number}: {e}"))
        .copy_part_result()
        .and_then(|r| r.e_tag())
        .expect("copy part etag")
        .trim_matches('"')
        .to_string()
}

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
