//! Phase-2 exit: the durable S3 surface end-to-end against a real backend (MinIO), driven over
//! HTTP with a real `aws-sdk-s3` client. Covers PUT/GET/HEAD/DELETE round-trips, ranges, the
//! conditional-write preconditions, LIST classification, buckets, control-byte keys, and the
//! ciphertext-at-rest guarantee. Durable transition faults cover definite remote refusal and a
//! committed operation whose response is lost. Every test owns its MinIO and cleans up on drop.

mod common;

use common::*;

const B: &str = "objs";

/// One client bucket maps to three prefixed backend buckets (§7), but `ListBuckets` must show only
/// the client name — the cache `<data>`/`<meta>` prefixes never leak even though all three share
/// one MinIO account in the harness. DeleteBucket then removes every backend projection, twins and
/// markers included.
#[tokio::test]
async fn list_buckets_hides_backend_projections() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    // Write an object so both cache buckets hold state (tombstone in <data>, twin in <meta>).
    put(&client, B, "obj", &pattern(64)).await;

    let names: Vec<String> = client
        .list_buckets()
        .send()
        .await
        .expect("list buckets")
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(str::to_string))
        .collect();
    assert_eq!(
        names,
        vec![B.to_string()],
        "only the client name is visible"
    );
    // The raw backend really does hold the three prefixed buckets underneath.
    let raw_buckets: Vec<String> = h
        .raw()
        .list_buckets()
        .send()
        .await
        .expect("raw list buckets")
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(str::to_string))
        .collect();
    for want in [h.cache_bucket(B), h.meta_bucket(B), h.remote_bucket(B)] {
        assert!(
            raw_buckets.contains(&want),
            "backend missing {want}: {raw_buckets:?}"
        );
    }

    // DeleteBucket after emptying the client bucket sweeps all three projections.
    client
        .delete_object()
        .bucket(B)
        .key("obj")
        .send()
        .await
        .expect("delete obj");
    client
        .delete_bucket()
        .bucket(B)
        .send()
        .await
        .expect("delete bucket");
    let after: Vec<String> = h
        .raw()
        .list_buckets()
        .send()
        .await
        .expect("raw list buckets")
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(str::to_string))
        .collect();
    for gone in [h.cache_bucket(B), h.meta_bucket(B), h.remote_bucket(B)] {
        assert!(
            !after.contains(&gone),
            "bucket {gone} outlived delete: {after:?}"
        );
    }
}

/// `DeleteBucket` refuses a bucket that still holds objects, and hypha is what refuses it: the gate
/// is its own listing of the client namespace, not the backend's answer (§7). SeaweedFS deletes a
/// non-empty bucket **and everything in it** by default (`allowDeleteBucketNotEmpty`), so delegating
/// this would turn a misdirected `DeleteBucket` into silent data loss on the backend hypha deploys
/// on (tests/backend.rs pins the divergence).
///
/// The refusal also has to leave the bucket serving — it is a rejected request, not a half-delete.
#[tokio::test]
async fn delete_bucket_refuses_a_non_empty_bucket() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    put(&client, B, "obj", &pattern(64)).await;

    let err = client
        .delete_bucket()
        .bucket(B)
        .send()
        .await
        .expect_err("a non-empty bucket must not delete");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("BucketNotEmpty"));

    assert_eq!(get_all(&client, B, "obj").await, pattern(64));
    put(&client, B, "obj2", &pattern(32)).await;

    for key in ["obj", "obj2"] {
        client
            .delete_object()
            .bucket(B)
            .key(key)
            .send()
            .await
            .expect("empty the bucket");
    }
    client
        .delete_bucket()
        .bucket(B)
        .send()
        .await
        .expect("an emptied bucket deletes");
}

/// The window the emptiness gate opens: between judging the namespace empty and committing the
/// delete, a write must not be able to put something back. A write *arriving* now meets the closed
/// gate (§7) — which on a backend that creates the bucket a PUT addresses (SeaweedFS, again
/// backend.rs) is the difference between a resurrected bucket and a refused request. The refusal is
/// also immediate: the write is told `NoSuchBucket` rather than queued behind the whole drain.
#[tokio::test]
async fn a_write_cannot_slip_into_a_bucket_whose_delete_is_committing() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let remote = h.remote_bucket(B);

    // The remote DeleteBucket is the commit, so pausing it holds the delete open past its gate.
    // The bucket is empty, so no other DELETE under this prefix can be taken for it.
    let mut committing = h
        .remote_faults()
        .pause_next_prefix(hyper::Method::DELETE, format!("/{remote}"));
    let client = h.client();
    let deleting = tokio::spawn(async move { client.delete_bucket().bucket(B).send().await });
    committing.reached().await;

    let err = h
        .client()
        .put_object()
        .bucket(B)
        .key("late")
        .body(pattern(16).into())
        .send()
        .await
        .expect_err("a write into a committing delete must be refused");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("NoSuchBucket"));

    committing.release();
    deleting
        .await
        .expect("delete task")
        .expect("the paused delete still commits");
    assert!(
        h.raw().head_bucket().bucket(&remote).send().await.is_err(),
        "the delete must carry the remote bucket away, not the late write's re-creation of it"
    );
}

/// The other half, and the one a phase check cannot cover: a write that passed its readiness check
/// **before** the delete began. Readiness is a load, not a hold, so that write is already past every
/// gate an announcement could raise — the delete has to observe it.
///
/// It observes it and **refuses**, immediately and without touching anything. The alternative, a
/// barrier the delete waits on, has to close the bucket to writes before it knows whether it will
/// commit — and a delete that is then refused has spent `NoSuchBucket` on a bucket that goes on
/// existing. That is the property this test is really about: through the whole refused delete, the
/// bucket keeps serving writes as if nothing had happened.
///
/// `CompleteMultipartUpload` is the write to use. It takes its claim up front and then spends the
/// whole part-resolution round trip with `<data>` still empty, so unlike a single PUT — whose transit
/// mark makes the bucket read non-empty from the moment it is bracketed — there is nothing here for
/// an emptiness listing to see. Only the in-flight count knows.
#[tokio::test]
async fn a_delete_refuses_rather_than_race_a_write_already_in_flight() {
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let remote = h.remote_bucket(B);
    let client = h.client();

    let body = pattern(64);
    let upload_id = client
        .create_multipart_upload()
        .bucket(B)
        .key("composite")
        .send()
        .await
        .expect("create the upload")
        .upload_id()
        .expect("upload id")
        .to_string();
    let etag = client
        .upload_part()
        .bucket(B)
        .key("composite")
        .upload_id(&upload_id)
        .part_number(1)
        .body(body.clone().into())
        .send()
        .await
        .expect("upload the part")
        .e_tag()
        .expect("part etag")
        .to_string();

    // Complete resolves its parts against the remote before it brackets the key, so pausing that
    // read holds the write in flight with the client namespace still empty.
    let mut completing = h
        .remote_faults()
        .pause_next_prefix(hyper::Method::GET, format!("/{remote}/composite"));
    let writing_client = h.client();
    let writer = tokio::spawn(async move {
        writing_client
            .complete_multipart_upload()
            .bucket(B)
            .key("composite")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
                    .build(),
            )
            .send()
            .await
    });
    completing.reached().await;

    let err = h
        .client()
        .delete_bucket()
        .bucket(B)
        .send()
        .await
        .expect_err("a delete racing a write in flight must refuse");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("OperationAborted"));

    // The refusal cost the bucket nothing: it is not "being deleted", and never was.
    put(&h.client(), B, "unaffected", &pattern(16)).await;
    assert_eq!(get_all(&h.client(), B, "unaffected").await, pattern(16));

    completing.release();
    writer
        .await
        .expect("complete task")
        .expect("the write the delete refused to race must still commit");
    assert_eq!(get_all(&h.client(), B, "composite").await, body);

    // Quiescent now, so the delete gets a real answer about a namespace nothing can change under it.
    let err = h
        .client()
        .delete_bucket()
        .bucket(B)
        .send()
        .await
        .expect_err("the bucket holds the objects it was told about");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("BucketNotEmpty"));

    for key in ["composite", "unaffected"] {
        h.client()
            .delete_object()
            .bucket(B)
            .key(key)
            .send()
            .await
            .expect("empty the bucket");
    }
    h.client()
        .delete_bucket()
        .bucket(B)
        .send()
        .await
        .expect("an emptied, quiescent bucket deletes");
}

/// A bucket recreated under a name that was deleted must start empty. The delete's cache drain is
/// best-effort by design, so this is the assertion that catches a projection surviving it — the
/// shape the pre-barrier race left behind, where `delete_bucket` refused the late write's object
/// and the next create inherited it and served it as its own.
#[tokio::test]
async fn a_recreated_bucket_inherits_nothing_from_the_name_it_reuses() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    put(&client, B, "before", &pattern(32)).await;
    client
        .delete_object()
        .bucket(B)
        .key("before")
        .send()
        .await
        .expect("empty the bucket");
    client
        .delete_bucket()
        .bucket(B)
        .send()
        .await
        .expect("an emptied bucket deletes");

    h.create_bucket(B).await;
    let listed = client
        .list_objects_v2()
        .bucket(B)
        .send()
        .await
        .expect("list the recreated bucket");
    assert!(
        listed.contents().is_empty(),
        "a recreated bucket must not serve the previous incarnation's objects"
    );
    assert_eq!(
        sdk_err_code(
            &client
                .get_object()
                .bucket(B)
                .key("before")
                .send()
                .await
                .expect_err("the old key must be gone")
        )
        .as_deref(),
        Some("NoSuchKey")
    );

    // The fresh gate the create installed admits writes — a closed one inherited from the delete
    // would refuse every write to the new incarnation for the life of the process.
    put(&client, B, "after", &pattern(48)).await;
    assert_eq!(get_all(&client, B, "after").await, pattern(48));
}

/// A bucket whose cache was lost is detected unreconciled on restart (its sync marker gone), served
/// from the remote meanwhile, and rebuilt in the background — the tombstone namespace and marker
/// return, and GET stays correct throughout (§7 restore overlay).
#[tokio::test]
async fn bucket_cache_loss_restores_from_remote() {
    let mut h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let keys = ["a/1", "a/2", "b/3"];
    let bodies: Vec<Vec<u8>> = (0..keys.len())
        .map(|i| pattern_seeded(48, i as u8))
        .collect();
    for (k, body) in keys.iter().zip(&bodies) {
        put(&client, B, k, body).await;
    }

    // Simulate cache-volume loss: wipe both projections' contents — the sync marker, tombstones,
    // and twins — leaving the remote (the source of truth) intact.
    lose_cache_volume(&mut h, false).await;
    let client = h.client();
    for (k, body) in keys.iter().zip(&bodies) {
        let got = client.get_object().bucket(B).key(*k).send().await;
        let data = got
            .expect("get mid-restore")
            .body
            .collect()
            .await
            .unwrap()
            .to_vec();
        assert_eq!(
            &data, body,
            "restore-overlay GET returned the wrong body for {k}"
        );
    }

    // The background restore rebuilds the cache and writes the marker last. Poll for it.
    wait_for_sync_marker(&h, B).await;

    // Cache is authoritative again: one rebuilt tombstone per key in <data>, GET still correct.
    let rebuilt = raw_list(&h.raw(), &h.cache_bucket(B), None).await;
    assert_eq!(
        rebuilt.len(),
        keys.len(),
        "restore rebuilt one tombstone per key"
    );
    for (k, body) in keys.iter().zip(&bodies) {
        let got = client.get_object().bucket(B).key(*k).send().await;
        let data = got
            .expect("get after restore")
            .body
            .collect()
            .await
            .unwrap()
            .to_vec();
        assert_eq!(
            &data, body,
            "post-restore GET returned the wrong body for {k}"
        );
    }
}

/// Whole-volume loss — the cache buckets themselves are gone, not just their contents (§7's "lost
/// whole or not at all"). The overlay still serves (GET + LIST from the remote, a write
/// materializes through `prepare_write`), and the restore re-provisions the buckets.
#[tokio::test]
async fn bucket_cache_volume_loss_restores_from_remote() {
    let mut h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let keys = ["a/1", "a/2", "b/3"];
    let bodies: Vec<Vec<u8>> = (0..keys.len())
        .map(|i| pattern_seeded(48, i as u8))
        .collect();
    let mut etags = Vec::new();
    for (k, body) in keys.iter().zip(&bodies) {
        etags.push(put(&client, B, k, body).await);
    }

    lose_cache_volume(&mut h, true).await;
    let client = h.client();

    // Mid-restore the remote is the read source of truth: GET returns the real bodies, and LIST
    // projects the remote page with facts off each object's trailer. (The LIST may land either
    // side of the restore flip — the projected entries are identical, so the assertion holds.)
    for (k, body) in keys.iter().zip(&bodies) {
        assert_eq!(
            &get_all(&client, B, k).await,
            body,
            "mid-restore GET wrong for {k}"
        );
    }
    let page = client
        .list_objects_v2()
        .bucket(B)
        .send()
        .await
        .expect("mid-restore LIST");
    let mut listed: Vec<(String, i64, String)> = page
        .contents()
        .iter()
        .map(|o| {
            (
                o.key().unwrap_or_default().to_string(),
                o.size().unwrap_or_default(),
                o.e_tag().unwrap_or_default().trim_matches('"').to_string(),
            )
        })
        .collect();
    listed.sort();
    let mut want: Vec<(String, i64, String)> = keys
        .iter()
        .zip(&bodies)
        .zip(&etags)
        .map(|((k, b), e)| (k.to_string(), b.len() as i64, e.clone()))
        .collect();
    want.sort();
    assert_eq!(listed, want, "mid-restore LIST projected the wrong entries");

    // A write mid-restore materializes its key (re-provisioning the buckets if it wins the race
    // with the sweep) and commits normally.
    let new_body = pattern_seeded(64, 9);
    put(&client, B, "c/4", &new_body).await;
    assert_eq!(get_all(&client, B, "c/4").await, new_body);

    wait_for_sync_marker(&h, B).await;

    let mut rebuilt = raw_list(&h.raw(), &h.cache_bucket(B), None).await;
    rebuilt.sort();
    assert_eq!(
        rebuilt,
        vec!["a/1", "a/2", "b/3", "c/4"],
        "one tombstone per key after restore"
    );
    assert_eq!(get_all(&client, B, "c/4").await, new_body);
    for (k, body) in keys.iter().zip(&bodies) {
        assert_eq!(
            &get_all(&client, B, k).await,
            body,
            "post-restore GET wrong for {k}"
        );
    }
}

/// Deleting a bucket that was unreconciled must retire every memo the gate keeps about it — a
/// stale `Restoring` verdict would keep answering from the remote instead of `NoSuchBucket` (§7).
#[tokio::test]
async fn delete_of_an_unreconciled_bucket_resolves_absent() {
    let mut h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    put(&client, B, "obj", &pattern(32)).await;

    lose_cache_volume(&mut h, false).await;
    let client = h.client();

    // First op classifies the bucket as restoring (and memoizes it); the delete then has to undo
    // that, whether or not the sweep has finished by the time it lands.
    assert_eq!(get_all(&client, B, "obj").await, pattern(32));
    client
        .delete_object()
        .bucket(B)
        .key("obj")
        .send()
        .await
        .expect("delete obj");
    client
        .delete_bucket()
        .bucket(B)
        .send()
        .await
        .expect("delete bucket");

    for err in [
        sdk_err_code(
            &client
                .get_object()
                .bucket(B)
                .key("obj")
                .send()
                .await
                .unwrap_err(),
        ),
        sdk_err_code(&client.list_objects_v2().bucket(B).send().await.unwrap_err()),
    ] {
        assert_eq!(
            err.as_deref(),
            Some("NoSuchBucket"),
            "a deleted bucket must not keep answering from a stale readiness memo"
        );
    }
}

/// A burst of writes arriving into a bucket whose cache volume is gone: every one must land, even
/// though none of them can write until the `<data>`/`<meta>` projections exist. Provisioning is a
/// control-plane action, so the writers hand it to the bucket actor, which coalesces the burst onto
/// one round rather than letting each request race to create the same two buckets (§7). The
/// coalescing itself isn't observable from the client side — this pins the correctness half.
#[tokio::test]
async fn concurrent_writes_survive_cache_volume_loss() {
    let mut h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    put(&client, B, "seed", &pattern(32)).await;

    lose_cache_volume(&mut h, true).await;

    // Fired together, before the lazily-triggered restore can provision anything for them.
    let client = h.client();
    let bodies: Vec<Vec<u8>> = (0..16u8).map(|i| pattern_seeded(48, i)).collect();
    let keys: Vec<String> = (0..bodies.len()).map(|i| format!("burst/{i}")).collect();
    let writes = keys
        .iter()
        .zip(&bodies)
        .map(|(k, body)| put(&client, B, k, body));
    futures::future::join_all(writes).await;

    for (k, body) in keys.iter().zip(&bodies) {
        assert_eq!(&get_all(&client, B, k).await, body, "burst write {k} lost");
    }
    assert_eq!(get_all(&client, B, "seed").await, pattern(32));

    // The restore still completes over the union of pre-loss and burst keys.
    wait_for_sync_marker(&h, B).await;
    let mut rebuilt = raw_list(&h.raw(), &h.cache_bucket(B), None).await;
    rebuilt.sort();
    let mut want = keys.clone();
    want.push("seed".to_string());
    want.sort();
    assert_eq!(rebuilt, want, "one tombstone per key after restore");
}

/// hypha is by assumption the only writer of the remote buckets (§7), so an object whose tail
/// trailer does not authenticate means that assumption is broken — foreign writes, or the wrong
/// trailer key. hypha refuses to guess: it logs the object and exits `EXIT_FOREIGN_OBJECT` rather
/// than deleting data it cannot authenticate or serving around it. Runs the real binary, since the
/// fatal path is `process::exit` — which also proves it fires from the background restore actor,
/// where a panic would only have killed the task.
#[tokio::test]
async fn foreign_remote_object_terminates_hypha() {
    let mut h = Harness::durable_subprocess().await;
    h.create_bucket(B).await;
    let client = h.client();
    put(&client, B, "mine", &pattern(32)).await;

    // Out-of-band write straight into the remote bucket: no age envelope, no trailer.
    let raw = h.raw();
    raw.put_object()
        .bucket(h.remote_bucket(B))
        .key("foreign")
        .body(bytes_body(b"not written through hypha"))
        .send()
        .await
        .expect("put foreign object");

    // Drop the cache (marker included) so B's namespace is untrusted again. Stopped first, or the
    // volume watchdog would halt on the live loss instead (§7, I6). Startup resolves every bucket's
    // state, so the restarted process owes B a restore before it serves anything — no client request
    // is needed to reach `foreign`, and none can be made, since the process is expected to exit
    // rather than become ready.
    h.stop_hypha().await;
    for cache in [h.cache_bucket(B), h.meta_bucket(B)] {
        for key in raw_list(&raw, &cache, None).await {
            raw.delete_object()
                .bucket(&cache)
                .key(&key)
                .send()
                .await
                .expect("wipe cache object");
        }
    }
    h.start_hypha_expecting_exit();

    let status = h
        .child()
        .wait_exit(std::time::Duration::from_secs(20))
        .await;
    assert_eq!(
        status.code(),
        Some(hypha::EXIT_INVARIANT_VIOLATION),
        "hypha should exit EXIT_INVARIANT_VIOLATION on an unverifiable remote object"
    );

    // The exit is only half of it: the violation must be *recorded* on the remote, or the next
    // process would come up and serve the same data (`crate::halt`).
    raw.head_object()
        .bucket(h.remote_bucket(B))
        .key(hypha_core::meta::halt_marker_key())
        .send()
        .await
        .expect("the violation must be recorded on the remote before hypha exits");

    // And every run after it must exit on that record alone, without re-deriving the violation —
    // which is what makes a halted deployment present as an ordinary crashloop.
    h.start_hypha_expecting_exit();
    let status = h
        .child()
        .wait_exit(std::time::Duration::from_secs(20))
        .await;
    assert_eq!(
        status.code(),
        Some(hypha::EXIT_INVARIANT_VIOLATION),
        "a run that finds a halt marker must exit before serving anything"
    );
}

/// Poll for a bucket's sync marker on the raw backend. The restore is lazy — some hypha op must
/// already have run against the bucket to trigger it; this only observes the outcome.
async fn wait_for_sync_marker(h: &Harness, bucket: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while h
        .raw()
        .head_object()
        .bucket(h.meta_bucket(bucket))
        .key("\u{1}\u{1}s")
        .send()
        .await
        .is_err()
    {
        assert!(
            std::time::Instant::now() < deadline,
            "restore never rewrote the sync marker"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// PUT→GET identity and ETag correctness across sizes spanning the 64 KiB chunk boundary, plus the
/// at-rest guarantee: what lands on the remote is age ciphertext, never the plaintext.
#[tokio::test]
async fn roundtrip_sizes_etag_and_encryption_at_rest() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    // 0 and 1 byte, sub-chunk, exactly one chunk, chunk±1, and a multi-chunk body.
    let sizes = [0usize, 1, 100, 65_535, 65_536, 65_537, 200_000];
    for &n in &sizes {
        let key = format!("size/{n}");
        let body = pattern(n);
        let etag = put(&client, B, &key, &body).await;
        assert_eq!(
            etag,
            md5_hex(&body),
            "PUT ETag must be the plaintext MD5 (size {n})"
        );

        let got = get_all(&client, B, &key).await;
        assert_eq!(got, body, "GET must return the bytes PUT (size {n})");

        let head = client
            .head_object()
            .bucket(B)
            .key(&key)
            .send()
            .await
            .expect("head");
        assert_eq!(
            head.content_length(),
            Some(n as i64),
            "HEAD length (size {n})"
        );
        assert_eq!(
            head.e_tag().unwrap().trim_matches('"'),
            md5_hex(&body),
            "HEAD ETag (size {n})"
        );
    }

    // At rest: a recognizable plaintext must not appear in the remote object, which must be age.
    let marker = b"TOP-SECRET-PLAINTEXT-MARKER".repeat(64);
    let key = "secret";
    put(&client, B, key, &marker).await;
    let ct = raw_remote_object(&h, B, key).await;
    assert!(
        ct.starts_with(AGE_MAGIC),
        "remote object must be an age file"
    );
    assert!(
        !contains_subslice(&ct, b"TOP-SECRET-PLAINTEXT-MARKER"),
        "plaintext must not appear in the remote ciphertext"
    );
    assert!(
        ct.len() > marker.len(),
        "ciphertext+trailer must exceed the plaintext"
    );
}

/// A durable PUT can commit remotely while its response is lost. The client sees an error, but the
/// transition mark must be repaired to the committed generation before the request returns.
#[tokio::test]
async fn durable_put_lost_response_repairs_the_committed_generation() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "fault/put";
    put(&client, B, key, b"old").await;
    let new = pattern_seeded(64 * 1024, 101);

    let lost = h.remote_faults().fail_response_times(
        hyper::Method::PUT,
        format!("/{}/{key}", h.remote_bucket(B)),
        hyper::StatusCode::FORBIDDEN,
        8,
    );
    client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&new))
        .content_length(new.len() as i64)
        .send()
        .await
        .expect_err("the injected response loss must fail the client request");
    tokio::time::timeout(std::time::Duration::from_secs(5), lost)
        .await
        .expect("remote PUT was never intercepted")
        .expect("fault proxy stopped before the remote PUT");

    assert_eq!(
        get_all(&client, B, key).await,
        new,
        "repair must project the generation that actually committed"
    );
    let head = client
        .head_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("head repaired PUT");
    assert_eq!(
        head.e_tag().map(|e| e.trim_matches('"')),
        Some(md5_hex(&new).as_str())
    );
    let cached = h
        .raw()
        .head_object()
        .bucket(h.cache_bucket(B))
        .key(key)
        .send()
        .await
        .expect("cache projection after PUT repair");
    assert_eq!(
        cached
            .metadata()
            .and_then(|md| md.get(hypha_core::meta::TOMB))
            .map(String::as_str),
        Some(hypha_core::meta::TOMB_EVICT)
    );
}

/// A definite remote failure is the other side of the transition bracket: repair must restore the
/// prior generation after both a refused PUT and a refused DELETE.
#[tokio::test]
async fn durable_remote_failures_restore_the_previous_generation() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "fault/refused-mutations";
    let old = pattern_seeded(32 * 1024, 102);
    let replacement = pattern_seeded(32 * 1024, 103);
    put(&client, B, key, &old).await;
    let path = format!("/{}/{key}", h.remote_bucket(B));
    let faults = h.remote_faults();

    let refused_put = faults.fail_times(
        hyper::Method::PUT,
        &path,
        hyper::StatusCode::PRECONDITION_FAILED,
        8,
    );
    client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&replacement))
        .content_length(replacement.len() as i64)
        .send()
        .await
        .expect_err("the injected remote PUT failure must reach the client");
    tokio::time::timeout(std::time::Duration::from_secs(5), refused_put)
        .await
        .expect("remote PUT was never intercepted")
        .expect("fault proxy stopped before refusing the PUT");
    assert_eq!(
        get_all(&client, B, key).await,
        old,
        "a failed remote PUT must restore the previous generation"
    );

    let refused_delete = faults.fail_times(
        hyper::Method::DELETE,
        &path,
        hyper::StatusCode::PRECONDITION_FAILED,
        8,
    );
    client
        .delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect_err("the injected remote DELETE failure must reach the client");
    tokio::time::timeout(std::time::Duration::from_secs(5), refused_delete)
        .await
        .expect("remote DELETE was never intercepted")
        .expect("fault proxy stopped before refusing the DELETE");
    assert_eq!(
        get_all(&client, B, key).await,
        old,
        "a failed remote DELETE must restore the object"
    );

    let cached = h
        .raw()
        .head_object()
        .bucket(h.cache_bucket(B))
        .key(key)
        .send()
        .await
        .expect("cache projection after failed mutations");
    assert_eq!(
        cached
            .metadata()
            .and_then(|md| md.get(hypha_core::meta::TOMB))
            .map(String::as_str),
        Some(hypha_core::meta::TOMB_EVICT),
        "repair must leave no transition mark"
    );
    assert_eq!(
        cached
            .metadata()
            .and_then(|md| md.get(hypha_core::meta::CETAG))
            .map(String::as_str),
        Some(md5_hex(&old).as_str())
    );
}

/// Ranged GET: offsets, open-ended, suffix, and a range straddling a chunk boundary.
#[tokio::test]
async fn ranged_reads() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let body = pattern(200_000);
    let key = "ranged";
    put(&client, B, key, &body).await;

    assert_eq!(get_range(&client, B, key, 0, 9).await, body[0..10]);
    assert_eq!(
        get_range(&client, B, key, 1000, 2000).await,
        body[1000..2001]
    );
    // Straddle the 65 536-byte chunk boundary.
    assert_eq!(
        get_range(&client, B, key, 65_530, 65_540).await,
        body[65_530..65_541]
    );
    let out = client
        .get_object()
        .bucket(B)
        .key(key)
        .range("bytes=199000-")
        .send()
        .await
        .expect("open-ended range");
    let tail = out.body.collect().await.unwrap().to_vec();
    assert_eq!(tail, body[199_000..]);
    assert_eq!(
        get_suffix(&client, B, key, 128).await,
        body[body.len() - 128..]
    );

    // A range wholly beyond the object is rejected.
    let err = client
        .get_object()
        .bucket(B)
        .key(key)
        .range("bytes=999999-1000000")
        .send()
        .await;
    assert!(err.is_err(), "range past EOF must error");

    put(&client, B, "empty-range", &[]).await;
    for (key, range) in [(key, "bytes=-0"), ("empty-range", "bytes=-1")] {
        let err = client
            .get_object()
            .bucket(B)
            .key(key)
            .range(range)
            .send()
            .await
            .expect_err("an empty byte range must be rejected");
        assert_eq!(
            sdk_err_code(&err).as_deref(),
            Some("InvalidRange"),
            "{range} against {key}"
        );
    }
}

/// `If-None-Match: *` (no double-create) and `If-Match` (no lost update) preconditions.
#[tokio::test]
async fn conditional_writes() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "cond";

    let v1 = pattern_seeded(1000, 1);
    let etag1 = client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&v1))
        .content_length(v1.len() as i64)
        .if_none_match("*")
        .send()
        .await
        .expect("create-if-absent succeeds")
        .e_tag()
        .unwrap()
        .trim_matches('"')
        .to_string();
    assert_eq!(etag1, md5_hex(&v1));

    let dupe = client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&pattern_seeded(1000, 2)))
        .content_length(1000)
        .if_none_match("*")
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&dupe.unwrap_err()).as_deref(),
        Some("PreconditionFailed")
    );
    assert_eq!(
        get_all(&client, B, key).await,
        v1,
        "refused write must not mutate"
    );

    let v2 = pattern_seeded(2000, 3);
    let etag2 = client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&v2))
        .content_length(v2.len() as i64)
        .if_match(&etag1)
        .send()
        .await
        .expect("cas with current etag")
        .e_tag()
        .unwrap()
        .trim_matches('"')
        .to_string();
    assert_eq!(etag2, md5_hex(&v2));
    assert_eq!(get_all(&client, B, key).await, v2);

    let stale = client
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(&pattern_seeded(500, 9)))
        .content_length(500)
        .if_match(&etag1)
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&stale.unwrap_err()).as_deref(),
        Some("PreconditionFailed")
    );
    assert_eq!(
        get_all(&client, B, key).await,
        v2,
        "stale CAS must not mutate"
    );
}

/// DELETE makes a key client-visibly absent (GET/HEAD 404, gone from LIST); it is idempotent.
#[tokio::test]
async fn delete_semantics() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "victim";

    put(&client, B, key, &pattern(4096)).await;
    client
        .delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("delete");

    let get = client.get_object().bucket(B).key(key).send().await;
    assert_eq!(
        sdk_err_code(&get.unwrap_err()).as_deref(),
        Some("NoSuchKey")
    );
    let head = client.head_object().bucket(B).key(key).send().await;
    assert!(head.is_err(), "HEAD of a deleted key must 404");

    let listed = list_keys(&client, B, None).await;
    assert!(
        !listed.contains(&key.to_string()),
        "deleted key must not list"
    );

    client
        .delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("idempotent delete of absent key");
}

/// A durable DELETE can commit while its response is lost. Repair must remove the transition mark
/// and expose the committed absence even though the caller received an error.
#[tokio::test]
async fn durable_delete_lost_response_repairs_the_committed_absence() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let client = h.client();
    let key = "fault/delete";
    put(&client, B, key, b"present").await;

    let lost = h.remote_faults().fail_response_times(
        hyper::Method::DELETE,
        format!("/{}/{key}", h.remote_bucket(B)),
        hyper::StatusCode::FORBIDDEN,
        8,
    );
    client
        .delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect_err("the injected response loss must fail the client request");
    tokio::time::timeout(std::time::Duration::from_secs(5), lost)
        .await
        .expect("remote DELETE was never intercepted")
        .expect("fault proxy stopped before the remote DELETE");

    let get = client.get_object().bucket(B).key(key).send().await;
    assert_eq!(
        sdk_err_code(&get.expect_err("committed delete must remain absent")).as_deref(),
        Some("NoSuchKey")
    );
    assert!(
        h.raw()
            .head_object()
            .bucket(h.cache_bucket(B))
            .key(key)
            .send()
            .await
            .is_err(),
        "repair must remove the transition mark"
    );
}

/// LIST: prefix filtering, delimiter/common-prefixes, pagination, and `start-after`, with
/// plaintext facts (size, ETag) reported for durable-mode (tombstoned) objects.
#[tokio::test]
async fn list_objects() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let bodies = [("a/1", 10usize), ("a/2", 20), ("b/1", 30)];
    for (k, n) in bodies {
        put(&client, B, k, &pattern(n)).await;
    }

    let out = client
        .list_objects_v2()
        .bucket(B)
        .send()
        .await
        .expect("list");
    let objs = out.contents();
    assert_eq!(objs.len(), 3, "all three keys must list");
    for o in objs {
        let (_, want) = bodies.iter().find(|(k, _)| *k == o.key().unwrap()).unwrap();
        assert_eq!(
            o.size(),
            Some(*want as i64),
            "plaintext size for {:?}",
            o.key()
        );
        assert_eq!(
            o.e_tag().unwrap().trim_matches('"'),
            md5_hex(&pattern(*want)),
            "plaintext ETag for {:?}",
            o.key()
        );
    }

    assert_eq!(list_keys(&client, B, Some("a/")).await, vec!["a/1", "a/2"]);

    let d = client
        .list_objects_v2()
        .bucket(B)
        .delimiter("/")
        .send()
        .await
        .expect("delimited list");
    let mut cps: Vec<String> = d
        .common_prefixes()
        .iter()
        .filter_map(|c| c.prefix().map(str::to_string))
        .collect();
    cps.sort();
    assert_eq!(cps, vec!["a/", "b/"]);
    assert!(
        d.contents().is_empty(),
        "delimited list has no direct contents here"
    );

    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = client.list_objects_v2().bucket(B).max_keys(1);
        if let Some(t) = &token {
            req = req.continuation_token(t.clone());
        }
        let page = req.send().await.expect("paged list");
        seen.extend(
            page.contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string)),
        );
        if page.is_truncated() != Some(true) {
            break;
        }
        token = page.next_continuation_token().map(str::to_string);
    }
    assert_eq!(
        seen,
        vec!["a/1", "a/2", "b/1"],
        "pagination must cover all keys in order"
    );

    // start-after skips up to and including its argument.
    let after = client
        .list_objects_v2()
        .bucket(B)
        .start_after("a/1")
        .send()
        .await
        .expect("start-after list");
    let keys: Vec<String> = after
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect();
    assert_eq!(keys, vec!["a/2", "b/1"]);
}

/// LIST pagination under twin dilution. In durable mode every key is an eviction tombstone with a
/// facts twin beside it, so a raw backend page is ~half client-visible: a page may return **fewer**
/// than `MaxKeys` client keys (a short page — valid S3). Following the forwarded continuation token
/// until `IsTruncated` is false must still cover every key exactly once, in order, with no gaps or
/// repeats and never more than `MaxKeys` per page.
#[tokio::test]
async fn list_pagination_short_pages() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let n = 25usize;
    let mut expected: Vec<String> = (0..n).map(|i| format!("obj/{i:03}")).collect();
    expected.sort();
    for k in &expected {
        put(&client, B, k, &pattern(32)).await;
    }

    // The keyspace split (§6) keeps twins out of the client cursor: <data> holds one tombstone per
    // key (no dilution), and the n twins live in <meta>. So the client cursor pages cleanly below.
    let data = raw_list(&h.raw(), &h.cache_bucket(B), None).await;
    assert_eq!(
        data.len(),
        n,
        "one tombstone per key in <data>, no twin dilution"
    );
    // <meta> also holds the bucket's sync marker (§6, the reserved `0x01 0x01` range); twins are
    // range B (`0x01 <K> 0x01 …`), so filter the doubled-control reserved keys back out.
    let meta_objs = raw_list(&h.raw(), &h.meta_bucket(B), None).await;
    let twins: Vec<&String> = meta_objs
        .iter()
        .filter(|k| !k.starts_with("\u{1}\u{1}"))
        .collect();
    assert_eq!(twins.len(), n, "one twin per key in <meta>");

    for page_size in [1i32, 7, 10] {
        let mut collected = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = client.list_objects_v2().bucket(B).max_keys(page_size);
            if let Some(t) = &token {
                req = req.continuation_token(t.clone());
            }
            let page = req.send().await.expect("list page");
            let keys: Vec<String> = page
                .contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string))
                .collect();

            // Short pages are allowed (dilution), but never over MaxKeys, and KeyCount must be honest.
            assert!(
                keys.len() <= page_size as usize,
                "never exceed MaxKeys ({page_size})"
            );
            assert_eq!(
                page.key_count(),
                Some(keys.len() as i32),
                "KeyCount matches contents"
            );
            let more = page.is_truncated() == Some(true);
            // A truncated page must carry a token; a final page must not.
            assert_eq!(
                page.next_continuation_token().is_some(),
                more,
                "NextContinuationToken present iff truncated (size {page_size})"
            );

            collected.extend(keys);
            match page.next_continuation_token() {
                Some(t) if more => token = Some(t.to_string()),
                _ => break,
            }
        }
        assert_eq!(
            collected, expected,
            "page size {page_size}: every key exactly once, in order, no gap/dup/twin-leak"
        );
    }
}

/// LIST v1: the same classifier and plaintext facts as v2, over v1's `marker`/`NextMarker` shell.
/// Paginating under twin dilution must still cover every key exactly once, in order.
#[tokio::test]
async fn list_objects_v1() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let bodies = [("a/1", 10usize), ("a/2", 20), ("b/1", 30)];
    for (k, n) in bodies {
        put(&client, B, k, &pattern(n)).await;
    }

    let out = client
        .list_objects()
        .bucket(B)
        .send()
        .await
        .expect("list v1");
    let objs = out.contents();
    assert_eq!(objs.len(), 3, "all three keys must list");
    for o in objs {
        let (_, want) = bodies.iter().find(|(k, _)| *k == o.key().unwrap()).unwrap();
        // Plaintext facts, not the ciphertext's — the twin is what LIST reads.
        assert_eq!(o.size(), Some(*want as i64), "plaintext size {:?}", o.key());
        assert_eq!(
            o.e_tag().unwrap().trim_matches('"'),
            md5_hex(&pattern(*want)),
            "plaintext ETag {:?}",
            o.key()
        );
    }
    assert_eq!(out.name(), Some(B));
    assert_eq!(out.is_truncated(), Some(false));

    let pfx = client
        .list_objects()
        .bucket(B)
        .prefix("a/")
        .send()
        .await
        .expect("prefixed list v1");
    let keys: Vec<&str> = pfx.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["a/1", "a/2"]);

    let d = client
        .list_objects()
        .bucket(B)
        .delimiter("/")
        .send()
        .await
        .expect("delimited list v1");
    let mut cps: Vec<String> = d
        .common_prefixes()
        .iter()
        .filter_map(|c| c.prefix().map(str::to_string))
        .collect();
    cps.sort();
    assert_eq!(cps, vec!["a/", "b/"]);
    assert!(d.contents().is_empty());

    // `marker` resumes strictly after its argument, and is echoed back.
    let after = client
        .list_objects()
        .bucket(B)
        .marker("a/1")
        .send()
        .await
        .expect("marker list v1");
    assert_eq!(after.marker(), Some("a/1"));
    let keys: Vec<&str> = after.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["a/2", "b/1"]);
}

/// v1 pagination under twin dilution: short pages are valid, but following `NextMarker` (falling
/// back to the last key received, as S3 documents) must cover every key exactly once with no gaps
/// or repeats — and must terminate.
///
/// Unblocked by the §6 keyspace split: `<data><b>` holds only client objects, so the client
/// cursor's last raw key is always an XML-safe, strictly-increasing client key — a valid resume
/// position, where the pre-split interleaved layout (twins at `K ‖ 0x01`) had none (§7).
#[tokio::test]
async fn list_objects_v1_pagination() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let n = 25usize;
    let mut expected: Vec<String> = (0..n).map(|i| format!("obj/{i:03}")).collect();
    expected.sort();
    for k in &expected {
        put(&client, B, k, &pattern(32)).await;
    }

    for page_size in [1i32, 7, 10] {
        let mut collected: Vec<String> = Vec::new();
        let mut marker: Option<String> = None;
        // A bound that can only be hit by a non-terminating pager.
        for _ in 0..(4 * n + 8) {
            let mut req = client.list_objects().bucket(B).max_keys(page_size);
            if let Some(m) = &marker {
                req = req.marker(m.clone());
            }
            let page = req.send().await.expect("list v1 page");
            let keys: Vec<String> = page
                .contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string))
                .collect();
            assert!(
                keys.len() <= page_size as usize,
                "never exceed MaxKeys ({page_size})"
            );
            collected.extend(keys.iter().cloned());

            if page.is_truncated() != Some(true) {
                marker = None;
                break;
            }
            // S3: use NextMarker when present, else the last key of the page.
            marker = page
                .next_marker()
                .map(str::to_string)
                .or_else(|| keys.last().cloned());
            assert!(
                marker.is_some(),
                "a truncated page must leave a resume position (size {page_size})"
            );
        }
        assert!(marker.is_none(), "pagination did not terminate");
        assert_eq!(
            collected, expected,
            "page size {page_size}: every key exactly once, in order, no gap/dup/twin-leak"
        );
    }
}

/// A key over the §6 twin threshold (986 bytes) gets **no** twin, so LIST must recover its facts
/// through the per-key HEAD fallback rather than the twin cursor — and still report them correctly.
/// This is the graceful-degradation path that lets admission accept S3's full 1024-byte keys.
#[tokio::test]
async fn list_over_threshold_key_head_fallback() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    // 999 > 986, so no twin is emitted; 999 ≤ 1024, so admission accepts it. Segmented at 199-byte
    // path components: MinIO's filesystem backend caps a single segment at 255 bytes (a backend
    // limit, not hypha's — the real cache is SeaweedFS, §9), so an unsegmented key wouldn't store.
    let key = vec!["k".repeat(199); 5].join("/");
    assert!(key.len() > 986 && key.len() <= 1024);
    let body = pattern(4096);
    let put_etag = put(&client, B, &key, &body).await;

    // The <meta> bucket carries no twin for this key (its twin would be `0x01 key 0x01 …`).
    let twin = raw_list(&h.raw(), &h.meta_bucket(B), Some(&format!("\u{1}{key}"))).await;
    assert!(
        twin.is_empty(),
        "over-threshold key must have no twin: {twin:?}"
    );

    // LIST still reports the key with correct facts, resolved via the HEAD fallback.
    let listed = client
        .list_objects_v2()
        .bucket(B)
        .send()
        .await
        .expect("list");
    let entry = listed
        .contents()
        .iter()
        .find(|o| o.key() == Some(key.as_str()))
        .expect("over-threshold key must appear in LIST");
    assert_eq!(entry.size(), Some(body.len() as i64));
    assert_eq!(
        entry.e_tag().map(|e| e.trim_matches('"')),
        Some(put_etag.as_str())
    );

    // And it round-trips through GET.
    assert_eq!(get_all(&client, B, &key).await, body);
}

/// Keys with control bytes and prefix-adjacent names round-trip byte-exact through PUT/GET, and
/// prefix-adjacent keys list in correct lexicographic order with their twins paired away.
#[tokio::test]
async fn control_byte_and_prefix_keys() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    // 0x00/0x01 are reserved (twin separator); every other byte is admissible on the write path,
    // where the key rides the percent-encoded URL. Includes bytes XML can't carry (0x02, 0x1f).
    // (No object-vs-`x/` pairs here: MinIO's single-drive backend won't keep a plain object `x`
    // alongside an object `x/y` — a backend quirk, not S3 semantics — so those live in the
    // ordering set below under a shared directory instead.)
    let keys = ["plain", "a!b", "tab\there", "ctrl\x1fx", "low\x02y"];
    for k in keys {
        let body = pattern(64 + k.len());
        put(&client, B, k, &body).await;
        assert_eq!(get_all(&client, B, k).await, body, "roundtrip key {k:?}");
        // HEAD must also handle the byte-exact key.
        client
            .head_object()
            .bucket(B)
            .key(k)
            .send()
            .await
            .unwrap_or_else(|e| panic!("head {k:?}: {e}"));
    }

    // Twin ordering with a base key that is a byte-prefix of a sibling: `d/a` sorts before `d/a!b`,
    // and `d/a`'s twin (`d/a` ‖ 0x01 ‖ facts) must sort between them (0x01 < '!' = 0x21) and be
    // paired away — never swallowing `d/a!b`. All three coexist (no plain object named `d`).
    for k in ["d/a", "d/a!b", "d/b"] {
        put(&client, B, k, &pattern(32)).await;
    }
    let ordered = list_keys(&client, B, Some("d/")).await;
    assert_eq!(
        ordered,
        vec!["d/a".to_string(), "d/a!b".to_string(), "d/b".to_string()],
        "prefix-adjacent keys must list in byte order with no twin leakage"
    );
}

/// Bucket lifecycle: create, HEAD, appears in ListBuckets, delete, then gone.
#[tokio::test]
async fn bucket_lifecycle() {
    let h = Harness::durable().await;
    let client = h.client();
    let bucket = "lifecycle";

    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create");
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("head existing bucket");

    let names: Vec<String> = client
        .list_buckets()
        .send()
        .await
        .expect("list buckets")
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(str::to_string))
        .collect();
    assert!(
        names.contains(&bucket.to_string()),
        "bucket must appear under its client name"
    );

    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("delete bucket");
    let head = client.head_bucket().bucket(bucket).send().await;
    assert!(head.is_err(), "deleted bucket must not HEAD");
}

/// User metadata survives PUT→HEAD/GET, and never collides with the facts sharing the same cache
/// carrier — a client key named `plen` must not shadow the tombstone's own.
///
/// Non-ASCII values come back as an **RFC 2047 encoded-word**, which is what S3 implementations do
/// generally, not an s3s quirk: HTTP field values are US-ASCII (RFC 9110), so a UTF-8 metadata
/// value needs an escape hatch on the response. Measured against this harness's MinIO, driven
/// directly with no hypha in the path, the same value comes back `=?UTF-8?q?caf=C3=A9_=E2=98=95?=`
/// — the Q (quoted-printable) variant where s3s emits B (base64). Both are valid encoded-words
/// decoding to identical bytes. `aws-sdk-s3` encodes neither on request (it parses the value
/// straight into a `HeaderValue`) nor decodes on response, so a client sees the encoded form.
///
/// hypha owns none of that leg — it stores the value s3s hands it — so what this pins for hypha is
/// that the bytes survive the round trip intact, asserted by decoding the payload below.
#[tokio::test]
async fn user_metadata_roundtrips() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let body = pattern(4096);

    client
        .put_object()
        .bucket(B)
        .key("meta/obj")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .metadata("colour", "café ☕")
        .metadata("plain", "value")
        .metadata("plen", "not-the-facts-plen")
        .send()
        .await
        .expect("put with metadata");

    let expected = |md: &std::collections::HashMap<String, String>| {
        assert_eq!(md.get("plain").map(String::as_str), Some("value"));
        assert_eq!(
            md.get("plen").map(String::as_str),
            Some("not-the-facts-plen")
        );
        // s3s's RFC 2047 form; the payload is the original value byte-for-byte.
        let colour = md.get("colour").expect("non-ascii key present");
        assert_eq!(colour, "=?UTF-8?B?Y2Fmw6kg4piV?=");
        assert_eq!(
            String::from_utf8(
                base64_simd::STANDARD
                    .decode_to_vec(
                        colour
                            .trim_start_matches("=?UTF-8?B?")
                            .trim_end_matches("?=")
                            .as_bytes()
                    )
                    .expect("rfc2047 payload is base64")
            )
            .expect("utf-8"),
            "café ☕"
        );
        assert_eq!(
            md.len(),
            3,
            "hypha's own facts must not leak as client keys"
        );
    };

    let head = client
        .head_object()
        .bucket(B)
        .key("meta/obj")
        .send()
        .await
        .expect("head");
    expected(head.metadata().expect("head metadata"));
    // The facts riding the same carrier are unharmed by the colliding client key.
    assert_eq!(head.content_length(), Some(body.len() as i64));

    let got = client
        .get_object()
        .bucket(B)
        .key("meta/obj")
        .send()
        .await
        .expect("get");
    expected(got.metadata().expect("get metadata"));
    assert_eq!(got.body.collect().await.unwrap().to_vec(), body);

    // An object written without metadata reports none, not a stale or defaulted map.
    put(&client, B, "meta/bare", &body).await;
    let bare = client
        .head_object()
        .bucket(B)
        .key("meta/bare")
        .send()
        .await
        .expect("head bare");
    assert!(bare.metadata().is_none_or(|m| m.is_empty()));
}

/// A wrong `Content-MD5` is rejected with `BadDigest`, and — the part that matters — the commit
/// never lands: an existing object at the key is left fully intact (§7's transition bracket, whose
/// repair settles K back from the remote).
#[tokio::test]
async fn content_md5_is_validated() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let original = pattern(8192);
    let original_etag = put(&client, B, "digest/obj", &original).await;

    let body = pattern_seeded(4096, 9);
    let wrong = base64_md5(&pattern_seeded(4096, 200));
    let err = client
        .put_object()
        .bucket(B)
        .key("digest/obj")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .content_md5(wrong)
        .send()
        .await
        .expect_err("wrong Content-MD5 must be rejected");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("BadDigest"));

    assert_eq!(get_all(&client, B, "digest/obj").await, original);
    let head = client
        .head_object()
        .bucket(B)
        .key("digest/obj")
        .send()
        .await
        .expect("head after rejected put");
    assert_eq!(
        head.e_tag().unwrap_or_default().trim_matches('"'),
        original_etag
    );

    client
        .put_object()
        .bucket(B)
        .key("digest/obj")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .content_md5(base64_md5(&body))
        .send()
        .await
        .expect("correct Content-MD5 must be accepted");
    assert_eq!(get_all(&client, B, "digest/obj").await, body);
}

/// Storage class is an echoed label (§7): non-archive classes round-trip, the archive family is
/// refused, and an unset class reads back as STANDARD.
#[tokio::test]
async fn storage_class_passthrough() {
    use aws_sdk_s3::types::StorageClass;

    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();
    let body = pattern(1024);

    client
        .put_object()
        .bucket(B)
        .key("sc/ia")
        .body(bytes_body(&body))
        .content_length(body.len() as i64)
        .storage_class(StorageClass::StandardIa)
        .send()
        .await
        .expect("put with storage class");

    let head = client
        .head_object()
        .bucket(B)
        .key("sc/ia")
        .send()
        .await
        .expect("head");
    assert_eq!(head.storage_class(), Some(&StorageClass::StandardIa));
    let got = client
        .get_object()
        .bucket(B)
        .key("sc/ia")
        .send()
        .await
        .expect("get");
    assert_eq!(got.storage_class(), Some(&StorageClass::StandardIa));

    // Archive classes imply RestoreObject, which one physical tier cannot honour.
    for archive in [StorageClass::Glacier, StorageClass::DeepArchive] {
        let err = client
            .put_object()
            .bucket(B)
            .key("sc/archive")
            .body(bytes_body(&body))
            .content_length(body.len() as i64)
            .storage_class(archive.clone())
            .send()
            .await
            .expect_err("archive storage class must be refused");
        assert_eq!(sdk_err_code(&err).as_deref(), Some("InvalidStorageClass"));
    }

    put(&client, B, "sc/default", &body).await;
    let head = client
        .head_object()
        .bucket(B)
        .key("sc/default")
        .send()
        .await
        .expect("head default");
    assert_eq!(head.storage_class(), Some(&StorageClass::Standard));
}

/// Batch DELETE: a fan-out of independent single-key deletes. Absent keys succeed, a repeated key
/// is not a self-deadlock, untargeted keys survive, and every deleted key leaves the cache clean.
#[tokio::test]
async fn delete_objects_batch() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    for k in ["gone/1", "gone/2", "gone/3", "kept"] {
        put(&client, B, k, &pattern(4096)).await;
    }

    // "missing" was never written and "gone/1" appears twice — both are successes.
    let targets = ["gone/1", "gone/2", "gone/3", "gone/1", "missing"];
    let out = delete_objects(&client, B, &targets, false).await;

    let deleted: Vec<&str> = out
        .deleted()
        .iter()
        .filter_map(|d| d.key())
        .collect::<Vec<_>>();
    assert_eq!(
        deleted.len(),
        targets.len(),
        "non-quiet mode reports one entry per requested object, got {deleted:?}"
    );
    assert!(
        out.errors().is_empty(),
        "no key should fail: {:?}",
        out.errors()
    );

    assert_eq!(list_keys(&client, B, None).await, vec!["kept".to_string()]);
    for k in ["gone/1", "gone/2", "gone/3"] {
        let got = client.get_object().bucket(B).key(k).send().await;
        assert_eq!(
            sdk_err_code(&got.unwrap_err()).as_deref(),
            Some("NoSuchKey")
        );
    }
    // Settle removes the <data> entry and the <meta> twin outright — absent is the authoritative
    // 404. (Twins are prefixed `0x01 gone/…` in <meta>, §6.)
    let cached = raw_list(&h.raw(), &h.cache_bucket(B), Some("gone/")).await;
    assert!(
        cached.is_empty(),
        "deleted keys left <data> state: {cached:?}"
    );
    let twins = raw_list(&h.raw(), &h.meta_bucket(B), Some("\u{1}gone/")).await;
    assert!(
        twins.is_empty(),
        "deleted keys left <meta> twins: {twins:?}"
    );

    // The survivor is untouched, body included.
    assert_eq!(get_all(&client, B, "kept").await, pattern(4096));
}

/// Quiet mode returns errors only, and a key hypha refuses at admission fails *per key* — the rest
/// of the batch still commits.
#[tokio::test]
async fn delete_objects_quiet_and_partial_failure() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    put(&client, B, "ok", &pattern(64)).await;
    // A key hypha refuses at admission (over S3's 1024-byte cap) — XML-valid, so it reaches the
    // per-key admission check rather than the request parser (§6/§7).
    let bad_key = "z".repeat(1025);
    let out = delete_objects(&client, B, &["ok", &bad_key], true).await;

    assert!(
        out.deleted().is_empty(),
        "quiet mode must omit successes: {:?}",
        out.deleted()
    );
    let errors = out.errors();
    assert_eq!(errors.len(), 1, "only the refused key fails: {errors:?}");
    assert_eq!(errors[0].key(), Some(bad_key.as_str()));
    assert_eq!(errors[0].code(), Some("InvalidArgument"));

    // The valid key still committed despite its neighbour's rejection.
    let got = client.get_object().bucket(B).key("ok").send().await;
    assert_eq!(
        sdk_err_code(&got.unwrap_err()).as_deref(),
        Some("NoSuchKey")
    );
}

/// GetObjectAttributes over the HEAD dispatch (§7): a durable single-part object (which settles to
/// an eviction tombstone) reports its size, ETag, and storage class from the tombstone facts, and
/// carries no `ObjectParts` (not multipart).
#[tokio::test]
async fn get_object_attributes_single_part() {
    use aws_sdk_s3::types::ObjectAttributes;
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    let body = pattern(50_000);
    let etag = put(&client, B, "obj", &body).await;

    let out = client
        .get_object_attributes()
        .bucket(B)
        .key("obj")
        .object_attributes(ObjectAttributes::ObjectSize)
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::StorageClass)
        .object_attributes(ObjectAttributes::ObjectParts)
        .send()
        .await
        .expect("get object attributes");

    assert_eq!(out.object_size(), Some(body.len() as i64));
    // AWS returns this ETag unquoted; s3s quotes every ETag uniformly, so trim before comparing.
    assert_eq!(
        out.e_tag().map(|e| e.trim_matches('"')),
        Some(etag.as_str())
    );
    assert_eq!(
        out.storage_class(),
        Some(&aws_sdk_s3::types::StorageClass::Standard)
    );
    assert!(
        out.object_parts().is_none(),
        "single-part has no ObjectParts"
    );

    // A requested-but-unasked attribute is simply absent: omit ObjectSize and it's None.
    let out2 = client
        .get_object_attributes()
        .bucket(B)
        .key("obj")
        .object_attributes(ObjectAttributes::Etag)
        .send()
        .await
        .expect("get object attributes (etag only)");
    assert!(out2.object_size().is_none(), "unrequested field is omitted");
    assert_eq!(
        out2.e_tag().map(|e| e.trim_matches('"')),
        Some(etag.as_str())
    );
}

/// GetBucketVersioning is a benign stub: an empty configuration (no Status), so `aws s3 sync` / boto
/// probes see "not enabled" rather than a 501.
#[tokio::test]
async fn get_bucket_versioning_stub() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let out = h
        .client()
        .get_bucket_versioning()
        .bucket(B)
        .send()
        .await
        .expect("get bucket versioning");
    assert!(out.status().is_none(), "versioning is never enabled");

    let err = h
        .client()
        .get_bucket_versioning()
        .bucket("not-created")
        .send()
        .await
        .expect_err("the stub must still validate bucket existence");
    assert_eq!(sdk_err_code(&err).as_deref(), Some("NoSuchBucket"));
}

/// Simulate cache-volume loss and bring hypha back onto the empty volume.
///
/// Stopped before the wipe, deliberately: taking the volume out from under a *live* ready bucket is
/// a different failure — the one the volume watchdog halts on (§7, I6) — and not what these tests
/// are about. `drop_buckets` removes the projections themselves too, modelling a volume that came
/// back bare rather than one that came back empty.
async fn lose_cache_volume(h: &mut Harness, drop_buckets: bool) {
    h.stop_hypha().await;
    let raw = h.raw();
    for cache in [h.cache_bucket(B), h.meta_bucket(B)] {
        for key in raw_list(&raw, &cache, None).await {
            raw.delete_object()
                .bucket(&cache)
                .key(&key)
                .send()
                .await
                .expect("wipe cache object");
        }
        if drop_buckets {
            raw.delete_bucket()
                .bucket(&cache)
                .send()
                .await
                .expect("wipe cache bucket");
        }
    }
    h.start_hypha().await;
}

// ── helpers ──────────────────────────────────────────────────────────────────────────────────

async fn delete_objects(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    keys: &[&str],
    quiet: bool,
) -> aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    let objects: Vec<ObjectIdentifier> = keys
        .iter()
        .map(|k| {
            ObjectIdentifier::builder()
                .key(*k)
                .build()
                .expect("object id")
        })
        .collect();
    client
        .delete_objects()
        .bucket(bucket)
        .delete(
            Delete::builder()
                .set_objects(Some(objects))
                .quiet(quiet)
                .build()
                .expect("delete container"),
        )
        .send()
        .await
        .expect("delete_objects")
}

/// Client-visible keys, optionally prefix-filtered, in listing order.
async fn list_keys(client: &aws_sdk_s3::Client, bucket: &str, prefix: Option<&str>) -> Vec<String> {
    let mut req = client.list_objects_v2().bucket(bucket);
    if let Some(p) = prefix {
        req = req.prefix(p);
    }
    req.send()
        .await
        .expect("list_objects_v2")
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect()
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Durable mode rejects a reserved-sentinel body too, though it keeps no plaintext in the cache and
/// so has no (size, ETag) classifier of its own to spoof (§6). The remote object outlives the mode
/// that wrote it: a bucket later switched to cached rehydrates that plaintext to bare `K`, where it
/// *is* the classification. One rule at ingest, so no later path has to re-derive the hazard.
#[tokio::test]
async fn durable_put_rejects_reserved_sentinel_body() {
    use hypha_core::meta;

    let h = Harness::durable().await;
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
    // Rejected before the transition bracket opens, so K is not even marked, let alone overwritten.
    assert_eq!(get_all(&c, B, "s").await, good);
}
