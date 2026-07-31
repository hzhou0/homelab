//! Transition-bracket crash cuts and twin coherence.
//!
//! Each cut asserts that readers observe one complete generation and that repair settles from the
//! remote commit point without needing to know what the failed writer attempted.

mod common;

use common::*;
use hyper::{Method, StatusCode};
use hypha_core::meta;

const CUT: StatusCode = StatusCode::FORBIDDEN;

/// How many attempts a cut refuses. A cut has to **stand** rather than fire once: the SDK retries a
/// failed backend call, so a one-shot fault is served from the retry and the step completes after
/// all. Standing also makes the post-cut state deterministic — GC's own repair of the mark is
/// refused by the same rule — so the test's `clear()` is what decides when the repair may happen.
const STANDING: usize = 1_000;

/// Keys sort late on purpose: several assertions here wait on a GC probe, and a probe starts from a
/// random key-shaped position and reads forward, so a key past almost every position is one almost
/// every probe covers.
const B: &str = "bracketbucket";

fn data_path(h: &Harness, key: &str) -> String {
    format!("/{}/{key}", h.cache_bucket(B))
}

fn remote_path(h: &Harness, key: &str) -> String {
    format!("/{}/{key}", h.remote_bucket(B))
}

/// The stable head of every twin of `key` — `0x01 ‖ K ‖ 0x01` — since the facts that follow it carry
/// an mtime the test cannot predict.
fn twin_prefix(h: &Harness, key: &str) -> String {
    let c = meta::CTRL as char;
    format!("/{}/{c}{key}{c}", h.meta_bucket(B))
}

/// `(key, size, etag)` for every entry a LIST reports — the projection that has to agree with what a
/// GET of the same key returns.
async fn listed(c: &aws_sdk_s3::Client, prefix: &str) -> Vec<(String, i64, String)> {
    let page = c
        .list_objects_v2()
        .bucket(B)
        .prefix(prefix)
        .send()
        .await
        .expect("list");
    page.contents()
        .iter()
        .map(|o| {
            (
                o.key().unwrap_or_default().to_string(),
                o.size().unwrap_or_default(),
                o.e_tag().unwrap_or_default().trim_matches('"').to_string(),
            )
        })
        .collect()
}

/// A GET's bytes together with the ETag the same response reported. Comparing the two is how a test
/// asserts "no hybrid" without knowing which generation won: the ETag is hypha's claim about the
/// bytes beside it, so a mismatch *is* the hybrid.
async fn get_with_etag(c: &aws_sdk_s3::Client, key: &str) -> (Vec<u8>, String) {
    let out = c.get_object().bucket(B).key(key).send().await.expect("get");
    let etag = out
        .e_tag()
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();
    let bytes = out.body.collect().await.expect("collect").to_vec();
    (bytes, etag)
}

/// Repair K through the write path, which resolves a leftover mark under K's lock before it evaluates
/// anything (§4). `If-None-Match: *` is the request that repairs and then declines: it cannot alter K,
/// so what it leaves behind is the repair alone.
async fn repair_through_the_write_path(c: &aws_sdk_s3::Client, key: &str) {
    let refused = c
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(b"never lands"))
        .content_length(11)
        .if_none_match("*")
        .send()
        .await;
    assert_eq!(
        sdk_err_code(&refused.unwrap_err()).as_deref(),
        Some("PreconditionFailed"),
        "the repaired key exists, so a create must be refused"
    );
}

/// A cut between the commit and the tombstone: the remote holds the new object, K still holds the
/// mark, and the client was told the write failed. The contract is that the *object* is committed —
/// an unacked write may land — and that every reader agrees on which generation that is.
///
/// LIST is the reader used while the cut stands, because it is the lock-free one: a marked entry
/// costs it a remote HEAD and nothing else. A GET of a marked key **repairs** K when the lock is free
/// (`get.rs`), so under a standing cache-write cut it cannot answer at all — which is why the GET
/// assertions come after the cut is lifted, where the read is itself the repair.
#[tokio::test]
async fn a_settle_cut_before_the_tombstone_leaves_the_new_generation_committed() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz-settle-tombstone";
    let v1 = pattern_seeded(20_000, 1);
    let v2 = pattern_seeded(30_000, 2);
    put(&c, B, key, &v1).await;

    // Suspend the bracket at its commit, so the fault below lands on the settle's write to K rather
    // than on the mark that has already gone in.
    let mut commit = h
        .remote_faults()
        .pause_next(Method::PUT, remote_path(&h, key));
    let writer = {
        let c = h.client();
        let v2 = v2.clone();
        let key = key.to_string();
        tokio::spawn(async move {
            c.put_object()
                .bucket(B)
                .key(&key)
                .body(bytes_body(&v2))
                .content_length(v2.len() as i64)
                .send()
                .await
                .map_err(|e| sdk_err_code(&e))
        })
    };
    commit.reached().await;
    assert_eq!(
        data_class(&h, B, key).await,
        Some(meta::TombKind::Transit),
        "the mark is what makes readers resolve K from the remote for the bracket's duration"
    );
    h.cache_faults()
        .fail_times(Method::PUT, data_path(&h, key), CUT, STANDING);
    commit.release();
    assert!(
        writer.await.expect("writer panicked").is_err(),
        "a settle that could not write K must not report success"
    );

    assert_eq!(
        data_class(&h, B, key).await,
        Some(meta::TombKind::Transit),
        "the settle never landed, so K is still marked"
    );
    assert_eq!(
        listed(&c, key).await,
        vec![(key.to_string(), v2.len() as i64, md5_hex(&v2))],
        "a marked key lists from the remote, not from the bytes of the mark"
    );

    // Lift the cut. The read that follows is the repair — it settles K from the remote with no
    // knowledge of what the writer had been doing — and it must answer with the committed generation
    // whole either way.
    h.cache_faults().clear();
    let (bytes, etag) = get_with_etag(&c, key).await;
    assert_eq!(bytes, v2);
    assert_eq!(etag, md5_hex(&v2));
    assert_eq!(data_class(&h, B, key).await, Some(meta::TombKind::Evict));
    assert_eq!(
        data_metadata(&h, B, key).await.get(meta::CETAG),
        Some(&md5_hex(&v2)),
        "the repaired tombstone carries the committed generation's facts"
    );
    assert_eq!(
        twins_of(&h, B, key).await.len(),
        1,
        "the repair leaves exactly one twin"
    );
}

/// The step before it: the twin write fails, so `refresh_twin` has already deleted the previous
/// generation's twin and K has no twin at all. LIST's per-key HEAD fallback is what has to carry the
/// key until the repair rebuilds one — and it is reading a *mark*, so the facts come from the remote.
#[tokio::test]
async fn a_settle_cut_at_the_twin_leaves_the_key_listable_with_no_twin() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz-settle-twin";
    let v1 = pattern_seeded(20_000, 3);
    let v2 = pattern_seeded(24_000, 4);
    put(&c, B, key, &v1).await;
    assert_eq!(twins_of(&h, B, key).await.len(), 1);

    let mut commit = h
        .remote_faults()
        .pause_next(Method::PUT, remote_path(&h, key));
    let writer = {
        let c = h.client();
        let v2 = v2.clone();
        let key = key.to_string();
        tokio::spawn(async move {
            c.put_object()
                .bucket(B)
                .key(&key)
                .body(bytes_body(&v2))
                .content_length(v2.len() as i64)
                .send()
                .await
                .map_err(|e| sdk_err_code(&e))
        })
    };
    commit.reached().await;
    h.cache_faults()
        .fail_prefix_times(Method::PUT, twin_prefix(&h, key), CUT, STANDING);
    commit.release();
    assert!(writer.await.expect("writer panicked").is_err());

    assert!(
        twins_of(&h, B, key).await.is_empty(),
        "the stale twin is deleted before the fresh one is written, so this cut leaves none"
    );
    let facts = vec![(key.to_string(), v2.len() as i64, md5_hex(&v2))];
    assert_eq!(
        listed(&c, key).await,
        facts,
        "with no twin and a mark at K, the entry is carried by the remote HEAD alone"
    );

    // The write path is the other repair entry point: it resolves a leftover mark under K's lock
    // before it evaluates any precondition, so a request that cannot alter K still settles it.
    h.cache_faults().clear();
    repair_through_the_write_path(&c, key).await;
    assert_eq!(twins_of(&h, B, key).await.len(), 1);
    assert_eq!(
        listed(&c, key).await,
        facts,
        "the rebuilt twin must project the same facts the fallback did"
    );
    assert_eq!(get_all(&c, B, key).await, v2);
}

/// The same cut on the DELETE bracket, where the commit is a removal: the remote object is gone and K
/// still holds the mark, so the key has to read as **absent** — a mark resolves from the remote, and
/// the remote's answer is the authoritative 404 (§7).
#[tokio::test]
async fn a_delete_cut_at_settle_leaves_the_absence_committed() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz-delete-settle";
    put(&c, B, key, &pattern(8_000)).await;

    let mut commit = h
        .remote_faults()
        .pause_next(Method::DELETE, remote_path(&h, key));
    let deleter = {
        let c = h.client();
        let key = key.to_string();
        tokio::spawn(async move { c.delete_object().bucket(B).key(&key).send().await.is_ok() })
    };
    commit.reached().await;
    // settle_absent deletes the twin and then K; failing K's delete is the cut that leaves the mark.
    h.cache_faults()
        .fail_times(Method::DELETE, data_path(&h, key), CUT, STANDING);
    commit.release();
    assert!(
        !deleter.await.expect("deleter panicked"),
        "a settle that could not remove K must not report success"
    );

    assert_eq!(data_class(&h, B, key).await, Some(meta::TombKind::Transit));
    assert!(
        listed(&c, key).await.is_empty(),
        "a marked key whose remote object is gone must not be listed"
    );

    h.cache_faults().clear();
    assert_eq!(
        sdk_err_code(&c.get_object().bucket(B).key(key).send().await.unwrap_err()).as_deref(),
        Some("NoSuchKey"),
        "the delete committed on the remote, so the key is gone however K was marked"
    );
    assert!(
        !raw_exists(&h, &h.cache_bucket(B), key).await,
        "and the repair settles K to absent, which is the authoritative 404"
    );

    // A create then finds nothing to refuse — the state is genuinely absent, not merely unreadable.
    let created = c
        .put_object()
        .bucket(B)
        .key(key)
        .body(bytes_body(b"after the repair"))
        .content_length(16)
        .if_none_match("*")
        .send()
        .await;
    assert!(
        created.is_ok(),
        "a repaired-absent key must accept a create: {created:?}"
    );
    assert_eq!(get_all(&c, B, key).await, b"after the repair");
    assert_eq!(
        twins_of(&h, B, key).await.len(),
        1,
        "and the twin is the new generation's alone"
    );
}

/// The window itself: while a bracket is open, every reader must see the *old* object and see it
/// whole. This is the assertion the mark exists for — K's own bytes are a sentinel during the
/// bracket, so a reader that trusted them would report a 16-byte object.
#[tokio::test]
async fn readers_see_the_old_object_whole_while_a_bracket_is_open() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz-open-bracket";
    let v1 = pattern_seeded(40_000, 5);
    let v2 = pattern_seeded(50_000, 6);
    put(&c, B, key, &v1).await;

    let mut commit = h
        .remote_faults()
        .pause_next(Method::PUT, remote_path(&h, key));
    let writer = {
        let c = h.client();
        let v2 = v2.clone();
        let key = key.to_string();
        tokio::spawn(async move {
            put(&c, B, &key, &v2).await;
        })
    };
    commit.reached().await;

    // Mid-bracket, from three angles that could disagree.
    let (bytes, etag) = get_with_etag(&c, key).await;
    assert_eq!(bytes, v1, "a mid-bracket read must serve the old object");
    assert_eq!(etag, md5_hex(&v1));
    let head = c
        .head_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("mid-bracket head");
    assert_eq!(head.content_length(), Some(v1.len() as i64));
    assert_eq!(
        listed(&c, key).await,
        vec![(key.to_string(), v1.len() as i64, md5_hex(&v1))]
    );
    // A range, since a hybrid would most plausibly appear as bytes clipped against the wrong length.
    assert_eq!(
        get_range(&c, B, key, 100, 199).await,
        v1[100..200],
        "a mid-bracket range must be cut against the old object's length"
    );

    commit.release();
    writer.await.expect("writer panicked");
    let (bytes, etag) = get_with_etag(&c, key).await;
    assert_eq!(bytes, v2, "and the acked write is whole the moment it acks");
    assert_eq!(etag, md5_hex(&v2));
}

/// A composite's complete bracket, cut at the same place: the native complete has concatenated the
/// parts at K on the remote and the settle could not record it. The composite is committed, so its
/// ETag, its length and — the part only the trailer can answer — its ranged reads all have to work
/// off the remote alone, before any repair.
#[tokio::test]
async fn a_complete_cut_at_settle_leaves_the_composite_committed() {
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz-complete-settle";
    let p1 = pattern_seeded(MIN_PART, 7);
    let p2 = pattern_seeded(MIN_PART, 8);
    let whole: Vec<u8> = p1.iter().chain(&p2).copied().collect();
    let composite_etag = expected_composite_etag(&[&p1, &p2]);

    let up = create_mpu(&c, B, key).await;
    let e1 = upload_part(&c, B, key, &up, 1, &p1).await;
    let e2 = upload_part(&c, B, key, &up, 2, &p2).await;

    let mut commit = h
        .remote_faults()
        .pause_next(Method::POST, remote_path(&h, key));
    let completer = {
        let c = h.client();
        let (key, up) = (key.to_string(), up.clone());
        let parts = [(1, e1), (2, e2)];
        tokio::spawn(async move {
            use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
            let completed: Vec<CompletedPart> = parts
                .iter()
                .map(|(n, etag)| {
                    CompletedPart::builder()
                        .part_number(*n)
                        .e_tag(etag.clone())
                        .build()
                })
                .collect();
            c.complete_multipart_upload()
                .bucket(B)
                .key(&key)
                .upload_id(&up)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(completed))
                        .build(),
                )
                .send()
                .await
                .is_ok()
        })
    };
    commit.reached().await;
    h.cache_faults()
        .fail_times(Method::PUT, data_path(&h, key), CUT, STANDING);
    commit.release();
    assert!(
        !completer.await.expect("completer panicked"),
        "a settle that could not write K must not report a completed upload"
    );

    assert_eq!(data_class(&h, B, key).await, Some(meta::TombKind::Transit));
    assert_eq!(
        listed(&c, key).await,
        vec![(key.to_string(), whole.len() as i64, composite_etag.clone())],
        "the composite's total length and composite ETag come off its trailer, not off K"
    );

    h.cache_faults().clear();
    let (bytes, etag) = get_with_etag(&c, key).await;
    assert_eq!(bytes, whole);
    assert_eq!(etag, composite_etag);
    assert_eq!(
        get_range(&c, B, key, MIN_PART as u64 - 10, MIN_PART as u64 + 9).await,
        whole[MIN_PART - 10..MIN_PART + 10],
        "a range across the part boundary is driven off the trailer's offset table"
    );
    assert_eq!(data_class(&h, B, key).await, Some(meta::TombKind::Evict));
    assert_eq!(
        data_metadata(&h, B, key).await.get(meta::CETAG),
        Some(&composite_etag)
    );
}

// ── twin coherence ───────────────────────────────────────────────────────────────────────────

/// A twin beside a **live body** — what an eviction cut between its twin write and its tombstone CAS
/// leaves behind. LIST fetches twins only for the keys it classified as tombstones, so this one is
/// never consulted; the assertion is that the entry reports the body's own facts and not the twin's,
/// and that the sweep then reclaims it (a twin left in place dilutes every page that covers it).
#[tokio::test]
async fn a_twin_beside_a_live_body_is_ignored_by_list_and_swept() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz-live-with-twin";
    let body = pattern(12_345);
    put(&c, B, key, &body).await;

    // Facts nothing at K projects: a different generation, a different length.
    let stale = meta::Facts {
        client_etag: md5_hex(b"some other generation"),
        plen: 7,
        mtime_ms: 424_242,
    }
    .twin_key(key)
    .expect("twin key");
    raw_meta_put(&h, B, &stale, Vec::new(), Default::default()).await;

    assert_eq!(
        listed(&c, key).await,
        vec![(key.to_string(), body.len() as i64, md5_hex(&body))],
        "a live body's facts are its own; a twin beside it says nothing"
    );
    assert_eq!(get_all(&c, B, key).await, body);

    wait_until(10_000, "the orphan twin to be swept", || async {
        twins_of(&h, B, key).await.is_empty()
    })
    .await;
    // The key is unharmed by the sweep that took its twin.
    assert_eq!(
        listed(&c, key).await,
        vec![(key.to_string(), body.len() as i64, md5_hex(&body))]
    );
}

/// The opposite gap — a tombstone whose twin is gone — which LIST answers with a per-key HEAD off the
/// tombstone's own metadata, the authoritative copy (§6). Worth pinning because the fallback is a
/// *correctness* path that only ever shows up as a cost: a LIST that silently dropped the entry, or
/// reported the sentinel's 16 bytes, would both look like a working deployment.
#[tokio::test]
async fn an_eviction_tombstone_with_no_twin_lists_from_its_own_facts() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz-twinless";
    let body = pattern(9_999);
    put(&c, B, key, &body).await;

    for twin in twins_of(&h, B, key).await {
        h.raw()
            .delete_object()
            .bucket(h.meta_bucket(B))
            .key(twin)
            .send()
            .await
            .expect("drop the twin");
    }

    let want = vec![(key.to_string(), body.len() as i64, md5_hex(&body))];
    assert_eq!(listed(&c, key).await, want, "the HEAD fallback carries it");
    // Stable, not a one-shot repair: nothing rebuilds a twin nobody asked to write, so the fallback
    // has to keep answering.
    assert_eq!(listed(&c, key).await, want);
    assert_eq!(get_all(&c, B, key).await, body);
}

/// One twin per key, through a whole generation sequence. Every path that moves K refreshes or deletes
/// its twin, and each of those is two writes — so "≤ 1" is a property of their ordering rather than of
/// any single write, and the only way to see it is to walk the states.
#[tokio::test]
async fn a_key_never_holds_more_than_one_twin_through_a_generation_sequence() {
    let h = Harness::durable().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz-generations";

    let mut expected: Option<Vec<u8>> = None;
    for step in 0..4u8 {
        let body = pattern_seeded(4_096 + step as usize, step);
        put(&c, B, key, &body).await;
        expected = Some(body.clone());
        let twins = twins_of(&h, B, key).await;
        assert_eq!(twins.len(), 1, "after write {step}");
        let (base, facts) = meta::parse_twin(&twins[0]).expect("a parseable twin");
        assert_eq!(base, key);
        assert_eq!(facts.client_etag, md5_hex(&body), "after write {step}");
        assert_eq!(facts.plen, body.len() as u64, "after write {step}");
    }
    assert_eq!(get_all(&c, B, key).await, expected.unwrap());

    c.delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("delete");
    assert!(
        twins_of(&h, B, key).await.is_empty(),
        "an absent key projects nothing, so it keeps no twin"
    );
}

/// A rehydrate is the one transition that *removes* a twin without replacing it: the body becomes
/// live plaintext at K, so its facts are native again and a twin beside it would be debris. The
/// ordering is load-bearing — the body lands first and the twin drop must run to completion — so both
/// ends are asserted, along with the LIST that has to stay correct across the flip.
#[tokio::test]
async fn a_rehydrate_drops_the_twin_it_no_longer_needs() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz-rehydrated";
    let body = pattern(28_000);
    put(&c, B, key, &body).await;
    wait_until(8_000, "the body to reach the remote", || async {
        remote_present(&h, B, key).await && !marker_present(&h, B, key).await
    })
    .await;

    plant_eviction_tombstone(&h, B, key, &body).await;
    assert_eq!(twins_of(&h, B, key).await.len(), 1);
    let want = vec![(key.to_string(), body.len() as i64, md5_hex(&body))];
    assert_eq!(listed(&c, key).await, want, "listed off the twin");

    assert_eq!(get_all(&c, B, key).await, body);
    wait_until(8_000, "the rehydrate to land", || async {
        data_class(&h, B, key).await.is_none()
    })
    .await;
    wait_until(8_000, "and to drop the twin behind it", || async {
        twins_of(&h, B, key).await.is_empty()
    })
    .await;
    assert_eq!(
        listed(&c, key).await,
        want,
        "the same facts, now native to the live body"
    );
}
