//! Phase-5 GC (§8). The §11 pass proper is still deferred; what lives here now are the two paths
//! that fail **silently** if they are wrong — a recency slice that never reaches `<meta>` and an
//! mpu range that is never reclaimed both degrade quietly rather than failing a request.

mod common;

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

async fn keys_in(h: &Harness, bucket: String, prefix: &str) -> Vec<String> {
    h.raw()
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .expect("list")
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|o| o.key)
        .collect()
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
