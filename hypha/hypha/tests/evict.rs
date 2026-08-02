//! Pressure-driven eviction and its correctness gates.
//!
//! The three gates are asserted one at a time and from the failing side, because that is the side
//! that loses data: each test drives a state in which the body must *not* be tombstoned and asserts
//! both halves — that eviction declines now, and that it proceeds once the reason to decline is gone.
//! A gate that had silently stopped working would pass the first half of every one of them.

mod common;

use std::time::Duration;

use common::*;
use hypha_core::config::Mode;
use hypha_core::meta;

const B: &str = "evictbucket";

const CAPACITY: u64 = 1_000_000;

/// Water marks that put the byte target within reach of a handful of test objects. The production
/// defaults (0.85/0.70) would owe 150 KB on this capacity, which is more than a test wants to write
/// just to observe one eviction.
const HIGH_WATER: f64 = 0.86;
const LOW_WATER: f64 = 0.85;

/// Usage that puts a pass over the high-water mark, owing ~15 KB.
const PRESSURED: f64 = 0.865;

/// Usage comfortably below the low-water mark: nothing justifies evicting a key the ring vouches for.
const RELAXED: f64 = 0.10;

async fn cached_with_usage() -> Harness {
    Harness::builder(Mode::Cached)
        .with_usage(CAPACITY)
        .tune(|c| {
            c.gc.high_water = HIGH_WATER;
            c.gc.low_water = LOW_WATER;
        })
        .start()
        .await
}

/// Wait out the reconcile sweep: the remote holds `key` and its marker is gone, which is the state
/// every gate below is judged against.
async fn until_durable(h: &Harness, key: &str) {
    wait_until(
        8_000,
        &format!("{key} to reach the remote and clear its marker"),
        || async { remote_present(h, B, key).await && !marker_present(h, B, key).await },
    )
    .await;
}

async fn is_evicted(h: &Harness, key: &str) -> bool {
    data_class(h, B, key).await == Some(meta::TombKind::Evict)
}

/// The full cached-mode cycle, and phase 5's first exit criterion: a body that goes cold under
/// pressure is tombstoned, the key keeps its client-visible facts throughout, and the next read
/// serves it from the remote and brings the plaintext back.
///
/// The unpressured window at the front is the assertion the rest of the file depends on: passes are
/// demonstrably running (the source has been sampled) and taking nothing, so every eviction below is
/// attributable to the pressure and not to a pass that evicts whatever it walks past.
#[tokio::test]
async fn pressure_evicts_a_cold_body_and_the_next_read_rehydrates_it() {
    let h = Harness::builder(Mode::Cached)
        .with_usage(CAPACITY)
        .with_faults()
        .tune(|c| {
            c.gc.high_water = HIGH_WATER;
            c.gc.low_water = LOW_WATER;
        })
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(64_000);
    let etag = put(&c, B, "cold", &body).await;
    until_durable(&h, "cold").await;

    stays_false(
        700,
        "an unpressured pass tombstoned a live body",
        || async { is_evicted(&h, "cold").await },
    )
    .await;
    assert!(
        h.usage().samples() > 0,
        "no pass sampled usage, so the window above proved nothing"
    );

    // Generation verification is one authenticated suffix GET. A preliminary HEAD would be both
    // redundant and the common-case request this fault makes visible.
    h.remote_faults().fail_prefix_times(
        hyper::Method::HEAD,
        format!("/{}/cold", h.remote_bucket(B)),
        hyper::StatusCode::SERVICE_UNAVAILABLE,
        10_000,
    );
    h.usage().set_ratio(PRESSURED);
    wait_until(8_000, "the cold body to be evicted", || async {
        is_evicted(&h, "cold").await
    })
    .await;

    // The key is unchanged as far as a client can tell: same length, same ETag, and the tombstone's
    // facts are what answer for it .
    let head = c
        .head_object()
        .bucket(B)
        .key("cold")
        .send()
        .await
        .expect("head an evicted key");
    assert_eq!(head.content_length(), Some(body.len() as i64));
    assert_eq!(
        head.e_tag().unwrap_or_default().trim_matches('"'),
        etag,
        "eviction must not move the client-visible ETag"
    );

    // Relax before reading, so the rehydrate this asserts is not racing the pass that would take the
    // body straight back — and so the ladder's reset is exercised too.
    h.usage().set_ratio(RELAXED);
    assert_eq!(
        get_all(&c, B, "cold").await,
        body,
        "the evicted generation must read back byte-identical from the remote"
    );
    wait_until(8_000, "the rehydrate to land at K", || async {
        data_class(&h, B, "cold").await.is_none()
    })
    .await;
    assert_eq!(get_all(&c, B, "cold").await, body, "the next read is a hit");
}

/// **Gate 1** — a pending marker means the remote is owed this generation, so the body is not GC's to
/// take however cold it is. Driven by holding the upload back rather than by planting a marker: the
/// marker then means what it means on the write path, and clearing the fault is what proves the
/// deferral was the marker's doing and not the candidate never being probed.
#[tokio::test]
async fn a_pending_marker_defers_eviction_until_the_upload_lands() {
    let h = Harness::builder(Mode::Cached)
        .with_usage(CAPACITY)
        .with_faults()
        .tune(|c| {
            c.gc.high_water = HIGH_WATER;
            c.gc.low_water = LOW_WATER;
        })
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(64_000);

    // Every reconcile upload of this key fails, so its marker can never be discharged.
    h.remote_faults().fail_prefix_times(
        hyper::Method::PUT,
        format!("/{}/pending", h.remote_bucket(B)),
        hyper::StatusCode::SERVICE_UNAVAILABLE,
        10_000,
    );
    put(&c, B, "pending", &body).await;
    h.usage().set_ratio(PRESSURED);

    stays_false(1_500, "eviction took a body the remote is owed", || async {
        is_evicted(&h, "pending").await
    })
    .await;
    assert!(
        marker_present(&h, B, "pending").await,
        "the marker is what the gate was reading; without it this proved nothing"
    );

    h.remote_faults().clear();
    until_durable(&h, "pending").await;
    wait_until(
        8_000,
        "eviction to proceed once the marker is discharged",
        || async { is_evicted(&h, "pending").await },
    )
    .await;
    assert_eq!(get_all(&c, B, "pending").await, body);
}

/// **Gate 2, absent remote** — a cache-only body is not evictable, and the skip is not merely a skip:
/// the check has just established the one thing a pending marker records, so it raises one. That is
/// the self-healing half — the reconcile sweep then makes the key durable and the *next* pass may
/// take it.
///
/// The state planted is the one no write path can produce: a marker lost to a crash (here, the remote
/// object deleted under a settled key), which is exactly why the gate cannot be a process-memory
/// counter of writes in flight.
#[tokio::test]
async fn a_body_the_remote_does_not_hold_owes_a_marker_instead_of_a_tombstone() {
    let h = cached_with_usage().await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(48_000);
    put(&c, B, "undurable", &body).await;
    until_durable(&h, "undurable").await;

    h.raw_remote()
        .delete_object()
        .bucket(h.remote_bucket(B))
        .key("undurable")
        .send()
        .await
        .expect("drop the remote object under a settled key");

    h.usage().set_ratio(PRESSURED);
    wait_until(8_000, "the durability gate to raise a marker", || async {
        marker_present(&h, B, "undurable").await
    })
    .await;
    assert!(
        data_class(&h, B, "undurable").await.is_none(),
        "a body the remote does not hold must still be live in the cache"
    );

    // The marker it raised is a PUT obligation, so the sweep uploads the body and eviction becomes
    // legitimate — no operator action, and no second mechanism.
    until_durable(&h, "undurable").await;
    wait_until(
        8_000,
        "eviction to proceed once the body is durable again",
        || async { is_evicted(&h, "undurable").await },
    )
    .await;
    assert_eq!(get_all(&c, B, "undurable").await, body);
}

/// **Gate 2, wrong generation** — the case a bare presence check would corrupt: the remote holds an
/// *older* generation of the same key, so tombstoning would stamp the cache body's facts over the old
/// plaintext and reads would return the old bytes under the new ETag and length.
///
/// Both generations are the same length on purpose, so only the trailer's `cetag` distinguishes
/// them.
#[tokio::test]
async fn a_remote_holding_an_older_generation_is_not_evicted() {
    let h = Harness::builder(Mode::Cached)
        .with_usage(CAPACITY)
        .with_faults()
        .tune(|c| {
            c.gc.high_water = HIGH_WATER;
            c.gc.low_water = LOW_WATER;
        })
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();
    let v1 = pattern_seeded(40_000, 1);
    let v2 = pattern_seeded(40_000, 2);

    put(&c, B, "gen", &v1).await;
    until_durable(&h, "gen").await;

    // v2 commits in the cache but can never reach the remote…
    h.remote_faults().fail_prefix_times(
        hyper::Method::PUT,
        format!("/{}/gen", h.remote_bucket(B)),
        hyper::StatusCode::SERVICE_UNAVAILABLE,
        10_000,
    );
    put(&c, B, "gen", &v2).await;
    wait_until(8_000, "v2's marker to be written", || async {
        marker_present(&h, B, "gen").await
    })
    .await;
    // …and its marker is lost, the way a crash between the commit and the marker write loses one. The
    // durability gate is now the only thing standing between v2 and a tombstone claiming the remote
    // holds it.
    h.raw()
        .delete_object()
        .bucket(h.meta_bucket(B))
        .key("gen")
        .send()
        .await
        .expect("lose the marker");

    h.usage().set_ratio(PRESSURED);
    stays_false(
        1_500,
        "eviction trusted a stale remote generation",
        || async { is_evicted(&h, "gen").await },
    )
    .await;
    assert!(
        marker_present(&h, B, "gen").await,
        "the generation check owes a marker for the body it declined"
    );
    assert_eq!(
        get_all(&c, B, "gen").await,
        v2,
        "reads must serve the acked generation throughout"
    );

    h.remote_faults().clear();
    until_durable(&h, "gen").await;
    wait_until(8_000, "v2 to become evictable once durable", || async {
        is_evicted(&h, "gen").await
    })
    .await;
    assert_eq!(
        get_all(&c, B, "gen").await,
        v2,
        "the rehydrated bytes must be v2's, not the generation the remote used to hold"
    );
}

/// **Gate 3** — sustained writes against a key GC is trying to evict. The layering is what makes
/// every interleaving auto-healing rather than lossy, and the property that catches a broken gate is
/// not "the last write survives" but that *every* read's bytes match the ETag it was served with: a
/// tombstone written over an unuploaded generation is precisely a read that returns one generation's
/// plaintext under another's facts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_writes_under_eviction_never_serve_bytes_that_disagree_with_their_etag() {
    let h = cached_with_usage().await;
    h.create_bucket(B).await;
    h.usage().set_ratio(PRESSURED);
    let c = h.client();

    let bodies: Vec<Vec<u8>> = (0..24u8).map(|i| pattern_seeded(20_000, i)).collect();
    let mut last = Vec::new();
    for body in &bodies {
        put(&c, B, "hot", body).await;
        last = body.clone();
        // Interleaved reads, each one an independent chance to catch a mismatched pair. The ETag the
        // response carries is hypha's claim about the bytes in it, so comparing the two needs no
        // knowledge of which generation won the race.
        let out = c
            .get_object()
            .bucket(B)
            .key("hot")
            .send()
            .await
            .expect("read under eviction pressure");
        let served_etag = out
            .e_tag()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let bytes = out.body.collect().await.expect("collect").to_vec();
        assert_eq!(
            md5_hex(&bytes),
            served_etag,
            "a read served bytes that do not hash to the ETag it reported"
        );
        assert!(
            bodies.contains(&bytes),
            "a read served a body no client ever wrote"
        );
    }

    h.usage().set_ratio(RELAXED);
    until_durable(&h, "hot").await;
    assert_eq!(
        get_all(&c, B, "hot").await,
        last,
        "the last acked write must survive every eviction attempt that raced it"
    );
    assert!(
        !raw_remote_object(&h, B, "hot").await.is_empty(),
        "the surviving generation is on the remote"
    );
}

/// Ordering, which is the recency ring's whole job: at the base threshold only keys the ring
/// affirmatively vouches nothing has touched are eligible, so a body in the current window survives a
/// pass that takes the one displaced out of the ring.
///
/// The filler writes are how a key is made cold without waiting: recency is denominated in competing
/// traffic , so 80 distinct keys past a 16-key fill target displace anything older by more than
/// the ring's depth. Keys are named to sort *after* every filler so a probe from any position covers
/// them — a random position that lands past the whole keyspace is the only one that wraps.
#[tokio::test]
async fn eviction_takes_the_displaced_body_and_spares_the_one_just_touched() {
    let h = cached_with_usage().await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(32_000);

    put(&c, B, "zz-cold", &body).await;
    for i in 0..80 {
        put(&c, B, &format!("fill/{i:03}"), b"x").await;
    }
    put(&c, B, "zz-hot", &body).await;
    until_durable(&h, "zz-cold").await;
    until_durable(&h, "zz-hot").await;

    h.usage().set_ratio(PRESSURED);
    wait_until(10_000, "the displaced body to be evicted", || async {
        is_evicted(&h, "zz-cold").await
    })
    .await;
    assert!(
        data_class(&h, B, "zz-hot").await.is_none(),
        "a body the ring places in the current window is not a candidate at the base threshold"
    );

    // And it keeps not being one: the pass that took the cold body runs again on the same population.
    stays_false(
        1_000,
        "a warm body was evicted at the base rung",
        || async { is_evicted(&h, "zz-hot").await },
    )
    .await;
}

/// Durable mode holds no bodies to evict, so it never probes on eviction's account — but the probes
/// run anyway, for the debris (`gc.rs`), and they classify live bodies as candidates on the way past.
/// What keeps those candidates safe is the mode gate alone, so plant one and hold pressure on it: a
/// gate that had leaked would tombstone a body no durable-mode read ever expects to have to rehydrate.
#[tokio::test]
async fn durable_mode_evicts_nothing_under_pressure() {
    let h = Harness::builder(Mode::Durable)
        .with_usage(CAPACITY)
        .tune(|c| {
            c.gc.high_water = HIGH_WATER;
            c.gc.low_water = LOW_WATER;
        })
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();
    let body = pattern(64_000);
    put(&c, B, "planted", &body).await;

    // A live plaintext body at K whose generation the remote demonstrably holds — every gate would
    // pass, in a mode that must never reach them.
    raw_cache_put(
        &h,
        B,
        "planted",
        body.clone(),
        std::collections::HashMap::new(),
    )
    .await;
    h.usage().set_ratio(PRESSURED);

    stays_false(2_000, "durable mode evicted a body", || async {
        data_class(&h, B, "planted").await.is_some()
    })
    .await;
    assert!(
        h.usage().samples() > 0,
        "no pass ran, so nothing was gated — the window above proved nothing"
    );
}

/// The accounting gate , which is the second lock on the same door as gate 2: until this run has
/// rebuilt a bucket's pending set, the marker range on disk is known incomplete, and a scavenger
/// reading it as exhaustive is the one way an acked write is lost. So a bucket whose rebuild has not
/// finished is not evictable however cold its bodies are.
///
/// The body here is fully durable before the kill, so every other gate would wave it through — the
/// accounting is the only thing left refusing. Lifting the fault is what proves that: the same body,
/// the same pressure, and the eviction happens the moment the rebuild completes.
#[tokio::test]
async fn a_bucket_whose_pending_set_is_unaccounted_is_not_evicted_from() {
    let mut h = Harness::builder(Mode::Cached)
        .with_usage(CAPACITY)
        .with_faults()
        .tune(|c| {
            c.gc.high_water = HIGH_WATER;
            c.gc.low_water = LOW_WATER;
        })
        .start()
        .await;
    h.create_bucket(B).await;
    let body = pattern(64_000);
    put(&h.client(), B, "unaccounted", &body).await;
    until_durable(&h, "unaccounted").await;

    // A kill leaves no clean marker, so the next run owes a rebuild — and the rebuild's remote cursor
    // cannot read. Path-style renders the bucket-scoped LIST with a trailing slash, so this refuses
    // the listing and no object read.
    h.kill_hypha().await;
    let refused = h.remote_faults().fail_times(
        hyper::Method::GET,
        format!("/{}/", h.remote_bucket(B)),
        hyper::StatusCode::SERVICE_UNAVAILABLE,
        10_000,
    );
    h.start_hypha().await;
    h.usage().set_ratio(PRESSURED);
    tokio::time::timeout(Duration::from_secs(10), refused)
        .await
        .expect("the rebuild never listed the remote, so nothing was held back")
        .expect("fault proxy stopped before the rebuild's listing");

    stays_false(2_000, "an unaccounted bucket was evicted from", || async {
        is_evicted(&h, "unaccounted").await
    })
    .await;
    assert!(
        h.usage().samples() > 0,
        "no pass ran, so nothing was gated — the window above proved nothing"
    );

    h.remote_faults().clear();
    wait_until(
        15_000,
        "eviction to proceed once the rebuild accounts for the bucket",
        || async { is_evicted(&h, "unaccounted").await },
    )
    .await;
    h.usage().set_ratio(RELAXED);
    assert_eq!(get_all(&h.client(), B, "unaccounted").await, body);
}

/// Phase 5's second exit criterion, end to end: the cache volume goes, the namespace is restored from
/// the remote as tombstones, and reads rehydrate off them — then pressure evicts the rehydrated body
/// again and the cycle closes. The restore leaves the pending set empty by construction , which is
/// asserted here because it is also what makes those bodies immediately evictable.
#[tokio::test]
async fn a_cache_wipe_restores_the_namespace_and_reads_rehydrate_off_it() {
    let mut h = cached_with_usage().await;
    h.create_bucket(B).await;
    let c = h.client();
    let keys = ["w/1", "w/2", "w/3"];
    let bodies: Vec<Vec<u8>> = (0..keys.len())
        .map(|i| pattern_seeded(24_000, i as u8))
        .collect();
    for (key, body) in keys.iter().zip(&bodies) {
        put(&c, B, key, body).await;
        until_durable(&h, key).await;
    }

    h.stop_hypha().await;
    for bucket in [h.cache_bucket(B), h.meta_bucket(B)] {
        drop_backend_bucket(&h, &bucket).await;
    }
    h.start_hypha().await;
    let c = h.client();

    // The bucket serves from the remote for the whole window (restore overlay); `w/1` takes that
    // read, and the assertions about what the restore *rebuilt* are taken on the two keys nothing has
    // read — a read landing after the flip rehydrates, so reading a key is not compatible with
    // asserting it is still a tombstone.
    assert_eq!(
        &get_all(&c, B, "w/1").await,
        &bodies[0],
        "read during restore"
    );
    wait_until(15_000, "the namespace restore to complete", || async {
        raw_exists(&h, &h.meta_bucket(B), &meta::sync_marker_key()).await
    })
    .await;

    for key in ["w/2", "w/3"] {
        assert!(
            is_evicted(&h, key).await,
            "the restore rebuilds each key as an eviction tombstone"
        );
        assert!(
            !marker_present(&h, B, key).await,
            "a restored namespace owes the remote nothing, so its pending set is empty"
        );
    }

    // A read off a restored tombstone rehydrates exactly as one off an evicted key does — the two
    // states are the same state, which is the point of the unified design.
    assert_eq!(get_all(&c, B, "w/2").await, bodies[1]);
    wait_until(8_000, "the restored key to rehydrate", || async {
        data_class(&h, B, "w/2").await.is_none()
    })
    .await;

    h.usage().set_ratio(PRESSURED);
    wait_until(
        10_000,
        "the rehydrated body to be evicted again",
        || async { is_evicted(&h, "w/2").await },
    )
    .await;
    h.usage().set_ratio(RELAXED);
    assert_eq!(
        get_all(&c, B, "w/2").await,
        bodies[1],
        "a full wipe → restore → rehydrate → evict → rehydrate cycle must be lossless"
    );
}

// ── the ladder, through the exposition ───────────────────────────────────────────────────────

/// One gauge value from the admin listener's exposition. The ladder is written by a background actor
/// and read by an operator, so the metric is the only place its position is observable at all — which
/// is also why this test runs the real binary.
async fn gauge(h: &Harness, name: &str) -> Option<f64> {
    let url = format!("http://{}/metrics", h.config.serving.admin_listen);
    let body = reqwest::get(&url)
        .await
        .expect("metrics request")
        .text()
        .await
        .expect("metrics body");
    body.lines()
        .filter(|line| !line.starts_with('#'))
        .find_map(|line| line.strip_prefix(name)?.trim().parse::<f64>().ok())
}

/// The escalation order, against a target the deployment cannot meet: every completed pass moves one
/// rung, and the cheap rungs — interval, then concurrency — are spent before the age threshold, which
/// is the only one whose cost is paid by a client. Then usage drops below the low-water mark and the
/// ladder returns to base in a single step, because a ratchet here would leave a deployment evicting
/// its working set forever on the evidence of one burst.
///
/// The bounds are widened from the harness's flat defaults so there are cheap rungs to observe at all;
/// the unmeetable target is what keeps the climb going, since the fake source's figure does not fall
/// as GC reclaims.
#[tokio::test]
async fn the_ladder_climbs_cheap_rungs_first_and_resets_when_pressure_clears() {
    let h = Harness::builder(Mode::Cached)
        .subprocess()
        .with_usage(CAPACITY)
        .tune(|c| {
            c.gc.interval_ms = 400;
            c.gc.min_interval_ms = 100;
            c.gc.concurrency = 1;
            c.gc.max_concurrency = 4;
            c.gc.high_water = HIGH_WATER;
            c.gc.low_water = LOW_WATER;
        })
        .start()
        .await;
    h.create_bucket(B).await;
    put(&h.client(), B, "k", &pattern(4_000)).await;

    wait_until(10_000, "an unpressured pass to publish rung 0", || async {
        gauge(&h, "hypha_gc_ladder_rung").await == Some(0.0)
    })
    .await;

    // 0.95 of capacity against a 0.85 low-water mark owes 100 KB, which a 4 KB keyspace cannot
    // possibly reclaim — so the ladder keeps being handed the same verdict.
    h.usage().set_ratio(0.95);
    // A generous budget: a pass's cost scales with how many buckets the deployment holds, and the
    // shared-backend fixture puts every concurrent test's buckets on one server.
    wait_until(60_000, "the ladder to reach the age threshold", || async {
        // The tuned bounds give three interval rungs and two concurrency rungs, so rung 3 is only
        // reachable by having climbed all five — which is the ordering claim itself.
        gauge(&h, "hypha_gc_ladder_rung").await == Some(3.0)
    })
    .await;
    assert!(
        h.usage().vacuums() > 0,
        "a pressured pass must ask the cache to reclaim dead bytes before evicting live ones"
    );
    let used = gauge(&h, "hypha_cache_used_bytes").await;
    assert_eq!(
        used,
        Some((CAPACITY as f64 * 0.95).floor()),
        "the exposition must carry the usage the ladder was judged against"
    );

    h.usage().set_ratio(RELAXED);
    wait_until(60_000, "the ladder to return to base", || async {
        gauge(&h, "hypha_gc_ladder_rung").await == Some(0.0)
    })
    .await;
}

/// A vacuum is rung 0, so it must not be a pressure-only courtesy that a deployment discovers it
/// needed after evicting live data — but it must also not be asked for on every idle pass, since a
/// dead-byte rewrite is real backend work. Unpressured is the half worth pinning: `gc.rs` already
/// covers what an unpressured pass *does* do.
#[tokio::test]
async fn an_unpressured_pass_asks_for_no_vacuum() {
    let h = cached_with_usage().await;
    h.create_bucket(B).await;
    put(&h.client(), B, "k", &pattern(1_000)).await;

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert!(
        h.usage().samples() > 0,
        "no pass sampled usage, so this proved nothing"
    );
    assert_eq!(
        h.usage().vacuums(),
        0,
        "an unpressured pass has no dead bytes worth paying a rewrite for"
    );
}
