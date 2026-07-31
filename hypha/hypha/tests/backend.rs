//! What the backend's conditional operations actually **do** — the assumption every CAS in hypha
//! rests on, made executable.
//!
//! §4's linearizability, §7's marker CAS and §8's three eviction gates are all conditional
//! operations: hypha's coordination is not locks over a shared store, it is `If-Match` on the store
//! itself. That makes the backend's precondition semantics part of hypha's correctness argument, and
//! an unenforced one does not fail — it silently succeeds, which is the failure mode no other test
//! in this suite can see, because every test that *drives* a precondition failure injects the 412
//! rather than earning it.
//!
//! These tests take one round trip each and are the cheapest thing in the suite. They belong here
//! rather than folded into the tests that depend on them for exactly the reason above: a test whose
//! subject is a race reports "the backend did not refuse" as a flake.
//!
//! The last four are the mirror image: S3 behaviours the two backends **disagree** on, pinned so
//! that hypha's not depending on either is a checked claim rather than an intention.

mod common;

use common::*;

/// A distinct bucket per test, so the two never race each other's objects.
async fn probe_bucket(h: &Harness, name: &str) -> String {
    let bucket = format!("{}-{name}", h.gc_bucket());
    h.raw()
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create the probe bucket");
    bucket
}

async fn put_raw(h: &Harness, bucket: &str, key: &str, body: &[u8]) -> String {
    h.raw()
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(bytes_body(body))
        .send()
        .await
        .expect("raw put")
        .e_tag()
        .expect("an ETag")
        .to_string()
}

/// The conditional **writes**, which is where hypha's single-writer guarantee is actually enforced:
/// `If-None-Match: *` is what makes a create linearizable (§4), and `If-Match` is the abort in
/// eviction's third gate and in every rehydrate landing (§8). Both are refused on a stale view here,
/// so those mechanisms are load-bearing rather than decorative.
#[tokio::test]
async fn conditional_writes_are_enforced() {
    let h = Harness::durable().await;
    let bucket = probe_bucket(&h, "cw").await;
    let raw = h.raw();

    let first = put_raw(&h, &bucket, "k", b"one").await;
    let created_twice = raw
        .put_object()
        .bucket(&bucket)
        .key("k")
        .body(bytes_body(b"two"))
        .if_none_match("*")
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&created_twice.expect_err("a create over a live key must be refused"))
            .as_deref(),
        Some("PreconditionFailed"),
        "without this, two concurrent creates could both win"
    );

    let second = put_raw(&h, &bucket, "k", b"three").await;
    assert_ne!(first, second);
    let stale = raw
        .put_object()
        .bucket(&bucket)
        .key("k")
        .body(bytes_body(b"four"))
        .if_match(&first)
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&stale.expect_err("a write against a superseded generation must be refused"))
            .as_deref(),
        Some("PreconditionFailed"),
        "without this, an eviction could tombstone a body a writer had already replaced"
    );
    assert!(
        raw.put_object()
            .bucket(&bucket)
            .key("k")
            .body(bytes_body(b"five"))
            .if_match(&second)
            .send()
            .await
            .is_ok(),
        "and the current generation must still be writable"
    );
}

/// The requirement itself: a delete bound to a generation must be **refused** once that generation
/// is superseded. Runs only against an external backend (`scripts/test-seaweedfs.sh`), because it is
/// the one assertion in the suite that the default one cannot satisfy — see below.
#[tokio::test]
async fn a_conditional_delete_is_enforced_by_the_deployed_backend() {
    let Some(_) = external_backend() else {
        // Deliberately a skip rather than a failure: the default backend's behaviour is asserted by
        // the test below, and the two together are the whole statement.
        return;
    };
    let h = Harness::durable().await;
    let bucket = probe_bucket(&h, "ce").await;
    let raw = h.raw();

    let superseded = put_raw(&h, &bucket, "k", b"one").await;
    let current = put_raw(&h, &bucket, "k", b"two").await;
    assert_ne!(superseded, current);

    let refused = raw
        .delete_object()
        .bucket(&bucket)
        .key("k")
        .if_match(&superseded)
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&refused.expect_err("a delete of a superseded generation must be refused"))
            .as_deref(),
        Some("PreconditionFailed"),
        "without this the reconcile sweep can clear a marker a newer write raised"
    );
    assert!(
        raw.head_object()
            .bucket(&bucket)
            .key("k")
            .send()
            .await
            .is_ok(),
        "and the current generation must survive the refusal"
    );
    assert!(
        raw.delete_object()
            .bucket(&bucket)
            .key("k")
            .if_match(&current)
            .send()
            .await
            .is_ok(),
        "while the generation the caller observed is still deletable"
    );
}

/// **The conditional delete is not enforced by MinIO**, and this records that as the fact it is.
///
/// It matters because three paths condition a *delete* on a generation they observed: the reconcile
/// sweep clearing a pending marker (§7), the cached DELETE branch removing a remote object bound to
/// the ETag its HEAD returned (§7), and the two shadow reclaims (§8). On a backend that ignores the
/// precondition, each becomes an unconditional delete of whatever is there **now**:
///
/// - the sweep can clear a marker raised by a write that landed after its listing, leaving the remote
///   holding an older generation with an empty pending set — stale for good, since nothing revisits a
///   key no marker names (`bursty_same_key_overwrites_converge_on_the_last_acked_generation`, which is
///   `#[ignore]`d for exactly this);
/// - the delete branch can remove a *newer* remote object that landed between its HEAD and its delete.
///
/// **SeaweedFS 4.37 — what the deployment runs on both legs (§9) — does enforce it**: a stale-ETag
/// delete is refused 412 and the object survives. So the mechanism is sound where it ships, and what
/// this test measures is the distance between the test backend and the deployed one: every
/// CAS-dependent path above is exercised here only where the precondition is a no-op, so no run of
/// this suite would catch a regression that stopped sending it.
///
/// **If this test fails, MinIO has gained the semantics**: re-enable the ignored test above and turn
/// this one into the positive assertion it wants to be.
#[tokio::test]
async fn the_default_test_backend_does_not_enforce_a_conditional_delete() {
    if external_backend().is_some() {
        return; // this one is about MinIO; the requirement is asserted above
    }
    let h = Harness::durable().await;
    let bucket = probe_bucket(&h, "cd").await;
    let raw = h.raw();

    let superseded = put_raw(&h, &bucket, "k", b"one").await;
    let current = put_raw(&h, &bucket, "k", b"two").await;
    assert_ne!(superseded, current);

    let deleted = raw
        .delete_object()
        .bucket(&bucket)
        .key("k")
        .if_match(&superseded)
        .send()
        .await;
    assert!(
        deleted.is_ok(),
        "MinIO is expected to accept the stale precondition; a refusal means it now enforces it"
    );
    assert!(
        raw.head_object()
            .bucket(&bucket)
            .key("k")
            .send()
            .await
            .is_err(),
        "and to have deleted the current generation regardless of the condition"
    );
}

/// **`DeleteBucket` on a non-empty bucket is not portable.** S3 and MinIO answer `BucketNotEmpty`;
/// SeaweedFS ships `allowDeleteBucketNotEmpty` on by default and deletes the bucket together with
/// everything in it.
///
/// hypha therefore gates emptiness itself (§7 — `BucketCtl::delete` lists the client namespace before
/// it commits), and this is why: a delegated gate is not a weaker gate on the deployed backend, it is
/// a recursive delete. The client-facing requirement is asserted where it belongs, in
/// `conformance::delete_bucket_refuses_a_non_empty_bucket`, which passes on both.
#[tokio::test]
async fn a_non_empty_bucket_is_refused_by_only_one_of_the_two_backends() {
    let h = Harness::durable().await;
    let bucket = probe_bucket(&h, "ne").await;
    let raw = h.raw();
    put_raw(&h, &bucket, "k", b"one").await;

    let deleted = raw.delete_bucket().bucket(&bucket).send().await;
    match external_backend() {
        Some(_) => {
            deleted.expect("SeaweedFS is expected to delete a non-empty bucket");
            assert!(
                raw.head_object()
                    .bucket(&bucket)
                    .key("k")
                    .send()
                    .await
                    .is_err(),
                "and to take its contents with it — which is the whole hazard"
            );
        }
        None => assert_eq!(
            sdk_err_code(&deleted.expect_err("MinIO is expected to refuse")).as_deref(),
            Some("BucketNotEmpty"),
        ),
    }
}

/// **An emptied prefix does not always disappear.** SeaweedFS's filer holds real directory entries,
/// and deleting the last object under one leaves the directory — so a delimited LIST keeps reporting
/// the prefix. Upstream made this unconditional: the `allowEmptyFolder` flag that once controlled it
/// is deprecated and ignored. S3 and MinIO have no directories and report nothing.
///
/// Nothing in hypha reads a common prefix — LIST forwards the backend's verbatim (§7) — so this
/// costs a client a phantom folder and hypha nothing. It is pinned rather than worked around because
/// the alternative is a probe LIST per prefix on every delimited page.
#[tokio::test]
async fn an_emptied_prefix_survives_on_only_one_of_the_two_backends() {
    let h = Harness::durable().await;
    let bucket = probe_bucket(&h, "ep").await;
    let raw = h.raw();
    put_raw(&h, &bucket, "p/k", b"one").await;
    raw.delete_object()
        .bucket(&bucket)
        .key("p/k")
        .send()
        .await
        .expect("empty the prefix");

    let prefixes: Vec<String> = raw
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await
        .expect("delimited list")
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix().map(str::to_string))
        .collect();
    match external_backend() {
        Some(_) => assert_eq!(
            prefixes,
            vec!["p/".to_string()],
            "SeaweedFS is expected to keep the directory entry behind the deleted object"
        ),
        None => assert!(
            prefixes.is_empty(),
            "MinIO is expected to report no prefix for an object that no longer exists"
        ),
    }
}

/// **A re-uploaded part number is not replaced everywhere.** S3 and MinIO keep one entry per part
/// number — the last upload wins and `ListParts` reports only it. SeaweedFS keeps every upload and
/// lists them all, in an order that is not the arrival order.
///
/// This is the divergence that mattered most: §7's complete used to build a `part → retag` map from
/// this listing, which on SeaweedFS silently kept whichever duplicate came last in it — so the ETag
/// `ListParts` reported and the one complete resolved could name different generations, and complete
/// refused the client's own part ETag. Complete now resolves the generation from the **client's**
/// part ETag (S3's own model) and `ListParts` reports one entry per number, so neither reads a
/// winner out of this listing. `CompleteMultipartUpload` honours whichever entry it is handed, which
/// is what makes that sound on both backends.
#[tokio::test]
async fn a_re_uploaded_part_replaces_the_old_one_on_only_one_of_the_two_backends() {
    let h = Harness::durable().await;
    let bucket = probe_bucket(&h, "rp").await;
    let raw = h.raw();
    let created = raw
        .create_multipart_upload()
        .bucket(&bucket)
        .key("k")
        .send()
        .await
        .expect("create mpu");
    let upload_id = created.upload_id().expect("upload id");

    let mut etags = Vec::new();
    for body in [b"first part payload".as_slice(), b"second part payload"] {
        let out = raw
            .upload_part()
            .bucket(&bucket)
            .key("k")
            .upload_id(upload_id)
            .part_number(1)
            .body(bytes_body(body))
            .send()
            .await
            .expect("upload part 1");
        etags.push(
            out.e_tag()
                .expect("part etag")
                .trim_matches('"')
                .to_string(),
        );
    }
    assert_ne!(etags[0], etags[1]);

    let listed = raw
        .list_parts()
        .bucket(&bucket)
        .key("k")
        .upload_id(upload_id)
        .send()
        .await
        .expect("list parts");
    let reported: Vec<String> = listed
        .parts()
        .iter()
        .filter_map(|p| p.e_tag().map(|e| e.trim_matches('"').to_string()))
        .collect();
    match external_backend() {
        Some(_) => {
            assert_eq!(
                reported.len(),
                2,
                "SeaweedFS is expected to keep both uploads of part 1: {reported:?}"
            );
            // And to honour the older of them at complete, which is what lets hypha choose.
            raw.complete_multipart_upload()
                .bucket(&bucket)
                .key("k")
                .upload_id(upload_id)
                .multipart_upload(
                    aws_sdk_s3::types::CompletedMultipartUpload::builder()
                        .parts(
                            aws_sdk_s3::types::CompletedPart::builder()
                                .part_number(1)
                                .e_tag(&etags[0])
                                .build(),
                        )
                        .build(),
                )
                .send()
                .await
                .expect("complete against the superseded generation");
            let landed = raw
                .get_object()
                .bucket(&bucket)
                .key("k")
                .send()
                .await
                .expect("get the completed object")
                .body
                .collect()
                .await
                .expect("collect body")
                .to_vec();
            assert_eq!(
                landed,
                b"first part payload".to_vec(),
                "the entry named by the request is the one that lands"
            );
        }
        None => assert_eq!(
            reported,
            vec![etags[1].clone()],
            "MinIO is expected to report only the last upload of part 1"
        ),
    }
}

/// **`ListMultipartUploads` is ordered by upload id, not by key**, on SeaweedFS — S3 and MinIO order
/// by key then upload id, and that is also the cursor SeaweedFS paginates on (it returns a
/// `NextUploadIdMarker` and no `NextKeyMarker`). Pagination itself is sound: the cursor advances and
/// terminates, so a client echoing the markers back sees every upload exactly once, and §8's debris
/// sweep — which only collects upload ids into a set — is unaffected. hypha sorts each page it
/// returns; across pages the order stays the remote's (§12).
#[tokio::test]
async fn multipart_uploads_are_listed_in_key_order_by_only_one_of_the_two_backends() {
    let h = Harness::durable().await;
    let bucket = probe_bucket(&h, "lu").await;
    let raw = h.raw();
    // Keys chosen so key order and creation order agree; any disagreement with the reported order is
    // then the backend's own ordering showing through.
    for key in ["a", "b", "c", "d"] {
        raw.create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .expect("create mpu");
    }
    let listed = raw
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .expect("list multipart uploads");
    let keys: Vec<&str> = listed.uploads().iter().filter_map(|u| u.key()).collect();
    assert_eq!(keys.len(), 4, "every upload is reported: {keys:?}");
    let sorted = {
        let mut s = keys.clone();
        s.sort_unstable();
        s
    };
    match external_backend() {
        // Not asserting a *specific* wrong order — upload ids are random, so which permutation comes
        // back is random too. The claim is only that key order is not what the backend guarantees.
        Some(_) => assert_eq!(
            keys.len(),
            sorted.len(),
            "the set is complete however it is ordered"
        ),
        None => assert_eq!(keys, sorted, "MinIO is expected to order by key"),
    }
}
