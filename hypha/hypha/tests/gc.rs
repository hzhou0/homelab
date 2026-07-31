//! Phase-5 GC (§8). The §11 pass proper is still deferred; what lives here now are the paths that
//! fail **silently** if they are wrong — a recency slice that never reaches `<meta>`, an mpu range
//! that is never reclaimed, an orphan twin that dilutes every LIST page covering it, and a
//! transition mark that quietly costs a remote round trip forever. None of them fails a request,
//! which is exactly why none of them would be noticed.
//!
//! All of it runs against a **durable** harness on purpose: the sampled classes ride the pass's
//! probes, and a durable deployment evicts nothing — so this is also the assertion that those probes
//! are taken for the debris alone, in a mode where eviction would never call for them (§8).

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

/// Keys in GC's own bucket — one per deployment, not per client bucket (§8).
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

/// Reads feed the ring as well as writes (§8) — the property a read-only ring gets backwards, and
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

/// A completed upload's records are left for the sweep (§6) rather than deleted on the client path,
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
/// twin write and the K write meant to accompany it leaves behind (§6).
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

/// A transition mark whose bracket died is resolved by any read (§7), so the sweep's job is the keys
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
