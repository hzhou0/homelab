//! Orphaned shadow bodies (§8) — the third obligation of the marker shape, and the one whose failure
//! is pure silence.
//!
//! A shadow is a rehydrated composite's plaintext, keyed by the digest of K. A cached write that
//! supersedes that composite leaves it unreachable *and* unrankable: nothing touches it again, so the
//! recency ring never forms an opinion and eviction only ever takes it as an eventual miss — on a cache
//! that never comes under pressure, never. So the mechanism is three pieces (queue, drain marker,
//! startup backstop), and each is tested from the side that would leak rather than the side that
//! would over-delete, plus the one assertion that keeps over-deletion honest: a shadow K still names
//! must survive all three.
//!
//! The marker tests are the load-bearing ones. A marker that is written when it should not be is worse
//! than no marker at all — it vouches for a bucket that has an orphan in it, and no later run will ever
//! look again.

mod common;

use std::collections::HashMap;

use common::*;
use hypha_core::config::Mode;
use hypha_core::meta;

const B: &str = "shadowbucket";

/// A key nothing ever wrote, so its shadow is reachable only through the `ck` back-pointer — which is
/// the one thing the backstop reads and nothing else does.
const GHOST: &str = "ghost/composite";

async fn shadow_present(h: &Harness, key: &str) -> bool {
    raw_exists(h, &h.meta_bucket(B), &meta::shadow_key(key)).await
}

async fn shadow_clean_marker_present(h: &Harness) -> bool {
    raw_exists(h, &h.meta_bucket(B), &meta::shadow_clean_marker_key()).await
}

/// A composite at `key`, completed and therefore tombstoned at K with its plaintext on the remote
/// (§7). Returns the whole plaintext.
async fn composite(c: &aws_sdk_s3::Client, key: &str, seed: u8) -> Vec<u8> {
    let p1 = pattern_seeded(MIN_PART, seed);
    let p2 = pattern_seeded(MIN_PART, seed.wrapping_add(1));
    let up = create_mpu(c, B, key).await;
    let e1 = upload_part(c, B, key, &up, 1, &p1).await;
    let e2 = upload_part(c, B, key, &up, 2, &p2).await;
    complete_mpu(c, B, key, &up, &[(1, e1), (2, e2)]).await;
    p1.iter().chain(&p2).copied().collect()
}

/// Read `key` once and wait for the rehydrate that read raised to land its shadow.
async fn rehydrate_into_shadow(h: &Harness, c: &aws_sdk_s3::Client, key: &str, whole: &[u8]) {
    assert_eq!(get_all(c, B, key).await, whole, "read off the remote");
    wait_until(10_000, "the composite's shadow to land", || async {
        shadow_present(h, key).await
    })
    .await;
}

/// A shadow left behind by a process that crashed: one nothing can reach from the key side at all,
/// since K never existed. Its metadata is what a real rehydrate writes — the generation it holds and
/// the back-pointer to K.
async fn plant_ghost_shadow(h: &Harness) {
    let mut md = HashMap::new();
    md.insert(
        meta::CETAG.to_string(),
        format!("{}-2", "ab".repeat(16)), // a composite ETag, so nothing mistakes it for a body
    );
    md.insert(
        meta::SHADOW_CLIENT_KEY.to_string(),
        meta::encode_shadow_client_key(GHOST),
    );
    raw_meta_put(
        h,
        B,
        &meta::shadow_key(GHOST),
        b"stale plaintext".to_vec(),
        md,
    )
    .await;
}

/// The queue's own path: a cached PUT over a rehydrated composite makes its shadow unreachable, and the
/// obligation the write handed over is what reclaims it. Nothing here reads K — an unconditional cached
/// PUT deliberately never does (§7) — so the write cannot know it superseded a composite, which is
/// exactly why the obligation is unconditional and the actor resolves it.
#[tokio::test]
async fn a_write_that_supersedes_a_composite_reclaims_its_shadow() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "sup/composite";
    let whole = composite(&c, key, 1).await;
    rehydrate_into_shadow(&h, &c, key, &whole).await;

    let replacement = pattern(4_096);
    put(&c, B, key, &replacement).await;

    wait_until(10_000, "the orphaned shadow to be reclaimed", || async {
        !shadow_present(&h, key).await
    })
    .await;
    assert_eq!(
        get_all(&c, B, key).await,
        replacement,
        "the write that orphaned the shadow is what K holds"
    );
}

/// A DELETE orphans a shadow just as a write does — a deleted K can never name that generation again —
/// and it is worth its own case because the delete path settles K to *absent*, which is the one
/// reachability answer the queue reads without a tombstone to compare against.
#[tokio::test]
async fn a_delete_reclaims_the_shadow_of_the_composite_it_removed() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "del/composite";
    let whole = composite(&c, key, 3).await;
    rehydrate_into_shadow(&h, &c, key, &whole).await;

    c.delete_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("delete the composite");

    wait_until(
        10_000,
        "the deleted key's shadow to be reclaimed",
        || async { !shadow_present(&h, key).await },
    )
    .await;
}

/// The other direction, which is what keeps every reclaim above honest: an obligation for a *different*
/// key must not take a live shadow. One listing settles a whole batch, so a bug here would be a batch
/// reclaiming whatever the listing returned rather than what each obligation named.
#[tokio::test]
async fn an_obligation_for_another_key_leaves_a_live_shadow_alone() {
    let h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "live/composite";
    let whole = composite(&c, key, 5).await;
    rehydrate_into_shadow(&h, &c, key, &whole).await;

    // Writes and a delete against neighbouring keys, each handing over an obligation of its own.
    put(&c, B, "live/neighbour", &pattern(512)).await;
    put(&c, B, "other", &pattern(512)).await;
    c.delete_object()
        .bucket(B)
        .key("other")
        .send()
        .await
        .expect("delete the neighbour");

    stays_false(1_500, "a live shadow was reclaimed", || async {
        !shadow_present(&h, key).await
    })
    .await;
    // And it is still the shadow reads are served from: the bytes come back correct with K's tombstone
    // untouched.
    assert_eq!(get_all(&c, B, key).await, whole);
    assert_eq!(data_class(&h, B, key).await, Some(meta::TombKind::Evict));
}

/// The marker and the backstop together, which is the only way either is meaningful: the marker's
/// presence must **stop** the sweep, and its absence must **cause** one. Asserting only the second
/// would pass just as well if the marker were never read, and a marker that is never read is a marker
/// that eventually vouches for an orphan.
///
/// The first restart is not incidental. A bucket this run *created* is accounted for its pending set
/// but not for its shadow range — it has no shadows, so there is nothing to have judged — so the marker
/// is only earned by a startup sweep that finished.
#[tokio::test]
async fn the_shadow_clean_marker_gates_the_backstop_sweep() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;

    // Startup sweeps the (empty) shadow range and accounts for the bucket; the graceful drain then has
    // the evidence to write the marker.
    h.restart_hypha().await;
    h.await_ready().await;
    h.stop_hypha().await;
    assert!(
        shadow_clean_marker_present(&h).await,
        "a graceful drain with nothing owed must vouch for the bucket it swept"
    );

    // An orphan appearing while nothing is running is the one case the marker gets wrong on purpose:
    // it was true when written, so this run trusts it and never looks.
    plant_ghost_shadow(&h).await;
    h.start_hypha().await;
    h.await_ready().await;
    assert!(
        !shadow_clean_marker_present(&h).await,
        "startup must clear the marker before it can serve a write that orphans a shadow"
    );
    stays_false(1_000, "a vouched-for bucket was swept anyway", || async {
        !shadow_present(&h, GHOST).await
    })
    .await;

    // No drain, so no marker — and the next run owes the sweep that finds it.
    h.kill_hypha().await;
    assert!(!shadow_clean_marker_present(&h).await);
    h.start_hypha().await;
    wait_until(10_000, "the backstop to reclaim the orphan", || async {
        !shadow_present(&h, GHOST).await
    })
    .await;
}

/// The backstop's judgement, on the population that separates the two answers: an orphan whose K never
/// existed, and a live shadow whose K still names its generation. Both are reached by the same listing
/// and the same back-pointer, so a sweep that took the second would be reclaiming exactly the transfer
/// a client just waited for.
#[tokio::test]
async fn the_backstop_reclaims_the_orphan_and_keeps_the_live_shadow() {
    let mut h = Harness::cached().await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "kept/composite";
    let whole = composite(&c, key, 7).await;
    rehydrate_into_shadow(&h, &c, key, &whole).await;
    plant_ghost_shadow(&h).await;

    // A kill leaves no marker of either kind, so this run owes both the shadow sweep and a pending-set
    // rebuild — the sweep must be the one that runs on the shadow range.
    h.kill_hypha().await;
    h.start_hypha().await;

    wait_until(10_000, "the backstop to reclaim the orphan", || async {
        !shadow_present(&h, GHOST).await
    })
    .await;
    assert!(
        shadow_present(&h, key).await,
        "the sweep took a shadow its key still names"
    );
    assert_eq!(
        get_all(&h.client(), B, key).await,
        whole,
        "and it is still servable from that shadow"
    );
}

/// An obligation that cannot be settled has to withhold the marker, exactly as an owed pending marker
/// does — and for the same reason: the marker is a claim about the whole bucket, so one unresolved
/// reclaim makes the claim false. Driven by failing the shadow's own HEAD, which is the narrowest cut
/// that leaves the obligation genuinely unresolvable without disturbing anything else in `<meta>`.
#[tokio::test]
async fn a_shadow_reclaim_still_owed_at_drain_withholds_the_marker() {
    let mut h = Harness::builder(Mode::Cached).with_faults().start().await;
    h.create_bucket(B).await;
    // As above: the accounting the marker needs is earned by a startup sweep, not by the create.
    h.restart_hypha().await;
    let c = h.client();
    let key = "owed/composite";
    let whole = composite(&c, key, 9).await;
    rehydrate_into_shadow(&h, &c, key, &whole).await;

    // Installed only now: the same HEAD is what a rehydrate probes the shadow with.
    h.cache_faults().fail_times(
        hyper::Method::HEAD,
        format!("/{}/{}", h.meta_bucket(B), meta::shadow_key(key)),
        hyper::StatusCode::SERVICE_UNAVAILABLE,
        10_000,
    );
    put(&c, B, key, &pattern(2_048)).await;

    h.stop_hypha().await;
    assert!(
        shadow_present(&h, key).await,
        "the reclaim could not have succeeded; without that this proves nothing"
    );
    assert!(
        !shadow_clean_marker_present(&h).await,
        "an unsettled shadow obligation must leave the bucket unvouched-for"
    );

    // The next run's backstop is what finally reclaims it — the marker's absence is not a loose end,
    // it is the hand-off.
    h.cache_faults().clear();
    h.start_hypha().await;
    wait_until(10_000, "the next run's backstop to reclaim it", || async {
        !shadow_present(&h, key).await
    })
    .await;
}

/// A shadow under pressure is the one reclaim in §8 that takes plaintext a client can ask for and
/// answers to *none* of the three eviction gates: the remote demonstrably holds the composite and K's
/// tombstone points at it throughout, so one conditional delete is the whole transition. Both halves of
/// that are asserted — the shadow goes, K's tombstone and its single twin do not — because a shadow
/// reclaim that disturbed K would push every LIST of that key onto the per-key HEAD fallback.
///
/// The filler writes are how the shadow is made cold: the read that raised the rehydrate touched the
/// *shadow's* key, so it starts in the current window and is not a candidate at the base threshold
/// until competing traffic displaces it.
#[tokio::test]
async fn pressure_reclaims_a_cold_shadow_and_leaves_the_key_intact() {
    let h = Harness::builder(Mode::Cached)
        .with_usage(1_000_000)
        .tune(|c| {
            c.gc.high_water = 0.86;
            c.gc.low_water = 0.85;
        })
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();
    let key = "zz/cold-composite";
    let whole = composite(&c, key, 11).await;
    rehydrate_into_shadow(&h, &c, key, &whole).await;
    let twins_before = twins_of(&h, B, key).await;
    assert_eq!(twins_before.len(), 1, "a settled key has exactly one twin");

    for i in 0..80 {
        put(&c, B, &format!("fill/{i:03}"), b"x").await;
    }
    h.usage().set_ratio(0.865);

    wait_until(15_000, "the cold shadow to be reclaimed", || async {
        !shadow_present(&h, key).await
    })
    .await;
    assert_eq!(
        data_class(&h, B, key).await,
        Some(meta::TombKind::Evict),
        "a shadow reclaim must not touch K's tombstone"
    );
    assert_eq!(
        twins_of(&h, B, key).await,
        twins_before,
        "nor its twin, which is the tombstone's LIST projection"
    );

    // And the object is unharmed: the next read fetches the composite again and re-lands the shadow.
    h.usage().set_ratio(0.10);
    assert_eq!(get_all(&c, B, key).await, whole);
    wait_until(10_000, "the shadow to be re-landed", || async {
        shadow_present(&h, key).await
    })
    .await;
}
