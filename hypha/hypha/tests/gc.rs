//! Silent GC outcomes: persisted recency and reclaim of upload records, twins, and stale marks.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use common::*;
use hypha_core::meta;

const B: &str = "gcsweep";

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
        if std::time::Instant::now() > deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Keys in a client bucket's `<meta>` projection.
async fn meta_keys(h: &Harness, prefix: &str) -> Vec<String> {
    keys_in(h, h.meta_bucket(B), prefix).await
}

/// Keys in GC's own bucket — one per deployment, not per client bucket.
async fn gc_keys(h: &Harness, prefix: &str) -> Vec<String> {
    keys_in(h, h.gc_bucket(), prefix).await
}

/// Via [`raw_list`], which asks for `encoding-type=url` and decodes: every key read here carries the
/// `0x01` control byte, which XML cannot represent, and a backend that emits it raw hands back a key
/// whose separator has become U+FFFD — matching nothing, and silently, since the two print alike.
async fn keys_in(h: &Harness, bucket: String, prefix: &str) -> Vec<String> {
    raw_list(&h.raw(), &bucket, Some(prefix)).await
}

/// The ring's whole persistence path, end to end: the touch feed, the fill-driven rotation, and the
/// slice writer's key format surviving a real backend. The harness pins `fill_target` low, so
/// writing past it is what rotates.
#[tokio::test]
async fn writes_feed_the_ring_and_a_full_slice_is_persisted() {
    let h = Harness::durable().await;
    let c = h.client();
    h.create_bucket(B).await;

    for i in 0..24 {
        c.put_object()
            .bucket(B)
            .key(format!("k{i:03}"))
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .expect("put");
    }

    wait_until(5_000, "a retired recency slice in GC's bucket", || async {
        !gc_keys(&h, meta::RECENCY_PREFIX).await.is_empty()
    })
    .await;

    let slices = gc_keys(&h, meta::RECENCY_PREFIX).await;
    for key in &slices {
        assert!(
            meta::parse_recency_seq(key).is_some(),
            "slice key {key:?} did not survive the round trip through LIST"
        );
    }
}

/// Reads feed the ring as well as writes — the property a read-only ring gets backwards, and
/// the one this test has to isolate: a restart leaves the current slice empty, so a rotation after
/// it can only have been driven by the HEADs. It also exercises the restore path, since the slice
/// the writes rotated out is read back at startup.
#[tokio::test]
async fn reads_feed_the_ring() {
    let mut h = Harness::durable().await;
    h.create_bucket(B).await;
    let c = h.client();
    for i in 0..20 {
        c.put_object()
            .bucket(B)
            .key(format!("k{i:03}"))
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .expect("put");
    }
    wait_until(5_000, "the writes' slice to be retired", || async {
        !gc_keys(&h, meta::RECENCY_PREFIX).await.is_empty()
    })
    .await;

    h.restart_hypha().await;
    let retired_by_writes = gc_keys(&h, meta::RECENCY_PREFIX).await.len();

    let c = h.client();
    for i in 0..20 {
        c.head_object()
            .bucket(B)
            .key(format!("k{i:03}"))
            .send()
            .await
            .expect("head");
    }
    wait_until(5_000, "reads alone to rotate a slice", || async {
        gc_keys(&h, meta::RECENCY_PREFIX).await.len() > retired_by_writes
    })
    .await;
}

/// Duplicate touches of one key must not advance the fill, or a single hot key would rotate the ring
/// and displace everything the deployment actually wants protected.
#[tokio::test]
async fn repeated_touches_of_one_key_do_not_rotate() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let c = h.client();

    c.put_object()
        .bucket(B)
        .key("hot")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .expect("put");
    for _ in 0..40 {
        c.head_object()
            .bucket(B)
            .key("hot")
            .send()
            .await
            .expect("head");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        gc_keys(&h, meta::RECENCY_PREFIX).await.is_empty(),
        "one key touched 41 times rotated a slice sized for 16 distinct keys"
    );
}

/// A completed upload's records are left for the sweep rather than deleted on the client path,
/// so the reclaim is what keeps them from accumulating forever.
#[tokio::test]
async fn completed_upload_records_are_reclaimed() {
    let h = Harness::durable().await;
    let c = h.client();
    h.create_bucket(B).await;

    let created = c
        .create_multipart_upload()
        .bucket(B)
        .key("mpu")
        .send()
        .await
        .expect("create mpu");
    let upload_id = created.upload_id().expect("upload id").to_string();
    let part = c
        .upload_part()
        .bucket(B)
        .key("mpu")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"hello multipart"))
        .send()
        .await
        .expect("upload part");
    c.complete_multipart_upload()
        .bucket(B)
        .key("mpu")
        .upload_id(&upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap_or_default())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("complete");

    wait_until(
        5_000,
        "the upload's record range to be reclaimed",
        || async { meta_keys(&h, &meta::mpu_scan_prefix()).await.is_empty() },
    )
    .await;
}

/// Plant a twin for `key` carrying facts nothing at `key` projects — the shape every crash between a
/// twin write and the K write meant to accompany it leaves behind.
async fn plant_twin(h: &Harness, key: &str, mtime_ms: i64) -> String {
    let twin = meta::Facts {
        client_etag: md5_hex(b"whatever this twin claims"),
        plen: 7,
        mtime_ms,
    }
    .twin_key(key)
    .expect("twin key");
    h.raw()
        .put_object()
        .bucket(h.meta_bucket(B))
        .key(&twin)
        .body(ByteStream::from(Vec::new()))
        .send()
        .await
        .expect("plant twin");
    twin
}

async fn twins_of(h: &Harness, key: &str) -> Vec<String> {
    let c = meta::CTRL as char;
    meta_keys(h, &format!("{c}{key}{c}")).await
}

/// Both orphan shapes, and the one twin that must survive them: a twin whose K holds a *different*
/// generation, a twin whose K never existed, and the live projection of a real eviction tombstone.
/// The last is the assertion that matters — a sweep that reclaimed it would push every LIST of that
/// key onto the per-key HEAD fallback.
#[tokio::test]
async fn orphan_twins_are_reclaimed_and_the_live_one_is_not() {
    let h = Harness::durable().await;
    let c = h.client();
    h.create_bucket(B).await;

    // A durable PUT settles K to an eviction tombstone and writes its twin: the live one.
    put(&c, B, "settled", b"hello").await;
    let live = twins_of(&h, "settled").await;
    assert_eq!(live.len(), 1, "a settled key has exactly one twin");

    let superseded = plant_twin(&h, "settled", 424_242).await;
    let keyless = plant_twin(&h, "never-existed", 424_242).await;

    wait_until(5_000, "both orphan twins to be reclaimed", || async {
        let remaining = meta_keys(&h, &(meta::CTRL as char).to_string()).await;
        !remaining.contains(&superseded) && !remaining.contains(&keyless)
    })
    .await;

    assert_eq!(
        twins_of(&h, "settled").await,
        live,
        "the sweep took the twin that was actually projecting a key"
    );
}

/// A transition mark whose bracket died is resolved by any read, so the sweep's job is the keys
/// nothing reads — which would otherwise hold one indefinitely and pay a remote HEAD on every LIST
/// page that covers them. Asserted without touching the key through hypha, since a read would repair
/// it and prove nothing.
#[tokio::test]
async fn a_leftover_transition_mark_is_repaired() {
    let h = Harness::durable().await;
    let c = h.client();
    h.create_bucket(B).await;

    put(&c, B, "stranded", b"hello").await;
    let mut md = HashMap::new();
    md.insert(meta::TOMB.to_string(), meta::TOMB_TRANSIT.to_string());
    raw_cache_put(&h, B, "stranded", meta::TRANSIT_SENTINEL.to_vec(), md).await;

    wait_until(5_000, "the mark to be repaired from the remote", || async {
        let head = h
            .raw()
            .head_object()
            .bucket(h.cache_bucket(B))
            .key("stranded")
            .send()
            .await
            .expect("head");
        meta::tomb_kind(&head.metadata().cloned().unwrap_or_default())
            == Some(meta::TombKind::Evict)
    })
    .await;
}

/// The other half of that gate: an upload the remote is still running must survive every pass, or
/// the sweep would delete the parts of an upload in progress.
#[tokio::test]
async fn in_progress_upload_records_survive_the_sweep() {
    let h = Harness::durable().await;
    let c = h.client();
    h.create_bucket(B).await;

    let created = c
        .create_multipart_upload()
        .bucket(B)
        .key("live")
        .send()
        .await
        .expect("create mpu");
    let upload_id = created.upload_id().expect("upload id").to_string();
    c.upload_part()
        .bucket(B)
        .key("live")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"still going"))
        .send()
        .await
        .expect("upload part");

    // Several sweep intervals, so this is a real exposure and not a race the test happens to win.
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let records = meta_keys(&h, &meta::mpu_scan_prefix()).await;
    assert!(
        records
            .iter()
            .any(|k| meta::parse_mpu_upload_id(k) == Some(upload_id.as_str())),
        "the sweep reclaimed an upload the remote is still running"
    );
}

/// The reverse sweep: a remote multipart upload whose cache `u`-record is gone — a create that
/// died between its remote create and its record write, or one the cache volume lost — is aborted by
/// GC, parts and all. An upload hypha created, whose record exists, survives the same passes.
#[tokio::test]
async fn orphaned_remote_uploads_are_aborted_and_live_ones_survive() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let client = h.client();

    // An upload hypha never created, planted straight on the remote: no cache record exists for it.
    let remote = h.raw_remote();
    let remote_bucket = h.remote_bucket(B);
    let orphan = remote
        .create_multipart_upload()
        .bucket(&remote_bucket)
        .key("orphan")
        .send()
        .await
        .expect("remote create")
        .upload_id()
        .expect("upload id")
        .to_string();
    let orphan_part = pattern_seeded(MIN_PART, 7);
    remote
        .upload_part()
        .bucket(&remote_bucket)
        .key("orphan")
        .upload_id(&orphan)
        .part_number(1)
        .body(bytes_body(&orphan_part))
        .send()
        .await
        .expect("orphan part");

    // A live upload through hypha, whose record must shield it from the same sweep.
    let live = create_mpu(&client, B, "live").await;

    wait_until(
        10_000,
        "the orphaned upload to be aborted by the sweep",
        || {
            let client = client.clone();
            let live = live.clone();
            async move { listed_uploads(&client).await == vec![("live".to_string(), live)] }
        },
    )
    .await;

    // The orphan's part went with it — ListParts on a dead upload fails.
    let parts = remote
        .list_parts()
        .bucket(&remote_bucket)
        .key("orphan")
        .upload_id(&orphan)
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&parts.unwrap_err()).as_deref(),
        Some("NoSuchUpload"),
        "aborting the upload removes its parts"
    );

    // The live upload was never touched: it still completes.
    let live_part = pattern_seeded(MIN_PART, 9);
    let e1 = upload_part(&client, B, "live", &live, 1, &live_part).await;
    let etag = complete_mpu(&client, B, "live", &live, &[(1, e1)]).await;
    assert_eq!(etag, expected_composite_etag(&[&live_part]));
    assert_eq!(get_all(&client, B, "live").await, live_part);
}

/// The try-lock handshake: a create still in flight — remote create done, record not yet
/// written — holds the create lock, so the sweep defers instead of aborting a live upload.
#[tokio::test]
async fn an_in_flight_create_is_not_aborted_by_the_sweep() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let client = h.client();

    // Freeze the create at its record write, after the remote upload already exists. The create
    // lock is held across that whole window.
    let mut paused = h
        .cache_faults()
        .pause_next_prefix(hyper::Method::PUT, format!("/{}/%01%01m", h.meta_bucket(B)));
    let (created_tx, created_rx) = tokio::sync::oneshot::channel();
    let creating = {
        let client = client.clone();
        tokio::spawn(async move {
            let up = create_mpu(&client, B, "paused").await;
            let _ = created_tx.send(up);
        })
    };
    paused.reached().await;
    let in_flight_id = listed_uploads(&client).await.pop().map(|(_, id)| id);

    // Several sweep intervals while the create is frozen: the sweep must defer, never abort.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        listed_uploads(&client).await,
        vec![("paused".to_string(), in_flight_id.clone().unwrap())],
        "the sweep aborted an upload whose create was still in flight"
    );

    paused.release();
    let recorded = tokio::time::timeout(Duration::from_secs(5), created_rx)
        .await
        .expect("create never finished")
        .expect("create failed");
    assert_eq!(
        recorded,
        in_flight_id.unwrap(),
        "the recorded upload is the one the sweep saw"
    );
    creating.await.expect("create task");

    // The record landed; further passes leave it alone and it still completes.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let part = pattern_seeded(MIN_PART, 11);
    let e1 = upload_part(&client, B, "paused", &recorded, 1, &part).await;
    let etag = complete_mpu(&client, B, "paused", &recorded, &[(1, e1)]).await;
    assert_eq!(etag, expected_composite_etag(&[&part]));
}

/// In-progress uploads as `(client key, upload id)`, in listing order.
async fn listed_uploads(client: &aws_sdk_s3::Client) -> Vec<(String, String)> {
    client
        .list_multipart_uploads()
        .bucket(B)
        .send()
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
