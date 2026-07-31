//! Behavior when a real backing store runs out of space.
//!
//! Which role is undersized is the point of each test, so the fixture is wired to **one** of them and
//! the other stays on the harness's own MinIO. That separation is what makes the cached-mode claims
//! sharp: a full remote must not stop hypha acking (the cache is the commit point), and a full cache
//! must stop it acking (there is nowhere for the commit to land).
//!
//! Each case requires a protocol error rather than a hang, no invariant halt, and preservation of
//! previously acknowledged data.

mod common;

use std::time::Duration;

use common::*;
use hypha_core::config::Mode;
use hypha_core::meta;

const B: &str = "full";

/// Object size for both the ballast and the writes under test: small enough that a 1 MiB volume
/// takes several, large enough that filling the fixture is a few dozen requests rather than
/// thousands.
const CHUNK: usize = 128 * 1024;

/// Ceiling on the fill loop. A fixture that has not refused a write by here is not undersized, and
/// the test says so rather than passing vacuously.
const FILL_ATTEMPTS: usize = 400;

/// The undersized S3, when this run is pointed at one. Absent for a plain `cargo test`, where every
/// test in this file skips — the condition cannot be faked, so there is nothing partial to run.
fn tiny_backend() -> Option<String> {
    std::env::var("TEST_S3_TINY_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty())
}

fn tiny_client(endpoint: &str) -> aws_sdk_s3::Client {
    let (access, secret) = fixture_credentials();
    s3_client(endpoint, &access, &secret)
}

/// Point one role at the undersized fixture. Its credentials are the fixture's own, not the MinIO
/// ones the rest of the config carries — the two stores are different servers, which is the point.
fn point_at(role: &mut hypha_core::config::S3Endpoint, endpoint: String) {
    let (access_key, secret_key) = fixture_credentials();
    role.endpoint = endpoint;
    role.access_key = access_key;
    role.secret_key = secret_key;
}

/// Consume the store's spare capacity through a bucket hypha cannot see.
///
/// Ballast rather than client traffic: the name carries no `bucket_prefix`, and `list_buckets`
/// filters by that (§9), so hypha neither resolves it at startup nor trips **I5** on it.
///
/// This alone does **not** make hypha's next write fail. SeaweedFS allocates volumes per collection,
/// so what runs out here is the store's ability to allocate a *new* one — a bucket that already has
/// one keeps its remaining megabyte. That is why the tests that need a refusal call
/// [`fill_through`] afterwards: ballast takes the store's headroom, and the client's own writes take
/// what is left of hypha's.
async fn exhaust(client: &aws_sdk_s3::Client) {
    let bucket = format!("ballast{:08x}", rand::random::<u32>());
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("create the ballast bucket");
    let body = pattern_seeded(CHUNK, 200);
    for i in 0..FILL_ATTEMPTS {
        let put = client
            .put_object()
            .bucket(&bucket)
            .key(format!("b{i}"))
            .body(bytes_body(&body))
            .send()
            .await;
        if put.is_err() {
            return;
        }
    }
    panic!(
        "the fixture accepted {} MiB of ballast — it is not undersized, so nothing here would be \
         tested",
        (FILL_ATTEMPTS * CHUNK) / (1024 * 1024)
    );
}

/// Write distinct keys through hypha until one is refused. Returns what was acked, and the code the
/// refusal carried.
///
/// The acked prefix is not incidental — it is the set every "still byte-exact afterwards" assertion
/// runs over. An ack is hypha's promise that the bytes are held, and a store that filled up while
/// they were being written is precisely the situation in which a system might quietly break it.
async fn fill_through(c: &aws_sdk_s3::Client, bucket: &str) -> (Vec<(String, Vec<u8>)>, String) {
    let mut acked = Vec::new();
    for i in 0..FILL_ATTEMPTS {
        let key = format!("fill{i}");
        let body = pattern_seeded(CHUNK, i as u8);
        let put = c
            .put_object()
            .bucket(bucket)
            .key(&key)
            .body(bytes_body(&body))
            .send()
            .await;
        match put {
            Ok(out) => {
                assert_eq!(
                    out.e_tag().map(|e| e.trim_matches('"')),
                    Some(md5_hex(&body).as_str()),
                    "an acked write must be acked with its own ETag"
                );
                acked.push((key, body));
            }
            Err(e) => {
                let code = sdk_err_code(&e)
                    .unwrap_or_else(|| panic!("the refusal was not an S3 response: {e:?}"));
                return (acked, code);
            }
        }
    }
    panic!("hypha accepted {FILL_ATTEMPTS} writes without the store refusing one");
}

/// Which of `written`'s keys still carry a pending marker — the obligations the sweep has not
/// discharged (§6).
async fn owed(h: &Harness, written: &[(String, Vec<u8>)]) -> Vec<String> {
    let mut owed = Vec::new();
    for (key, _) in written {
        if marker_present(h, B, key).await {
            owed.push(key.clone());
        }
    }
    owed
}

async fn reached_remote_key(remote: &aws_sdk_s3::Client, bucket: &str, key: &str) -> bool {
    remote
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .is_ok()
}

/// An invariant violation is recorded on the remote before the process exits (§7), so its absence is
/// the check that a full store was handled as an error rather than mistaken for corruption.
async fn halted(h: &Harness, remote: &aws_sdk_s3::Client) -> bool {
    remote
        .head_object()
        .bucket(h.remote_bucket(B))
        .key(meta::halt_marker_key())
        .send()
        .await
        .is_ok()
}

/// The S3 error code a refused request carried. `expect_err` first, so a request that *succeeded*
/// fails the test where it was made rather than as a confusing `None`.
fn refusal<T, E, R>(r: Result<T, aws_sdk_s3::error::SdkError<E, R>>, what: &str) -> String
where
    T: std::fmt::Debug,
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::fmt::Debug,
    R: std::fmt::Debug,
{
    let err = r.expect_err(what);
    // No code at all means the request did not come back as an S3 response — a dropped connection or
    // a hang — which is exactly what "errors cleanly" excludes.
    sdk_err_code(&err).unwrap_or_else(|| panic!("{what}: not an S3 error response: {err:?}"))
}

/// **Durable mode, full remote.** The remote is the commit point, so nothing can be written at all —
/// and the object already committed there must come through untouched, including across a failed
/// overwrite of its own key. That last part is the §7 bracket's whole contract stated against a real
/// failure: mark → commit → settle, where a commit that never happens leaves the old object whole.
///
/// Runs as the shipped binary so "did not panic" is a real check: the process has to still be there,
/// and `/readyz` has to still answer, after every one of these refusals.
#[tokio::test]
async fn a_full_remote_refuses_durable_writes_and_keeps_what_it_committed() {
    let Some(tiny) = tiny_backend() else { return };
    let remote = tiny_client(&tiny);
    let endpoint = tiny.clone();
    let mut h = Harness::builder(Mode::Durable)
        .subprocess()
        .tune(move |c| point_at(&mut c.remote, endpoint))
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();

    let keeper = pattern_seeded(CHUNK, 7);
    let keeper_etag = put(&c, B, "keeper", &keeper).await;

    exhaust(&remote).await;
    let (acked, code) = fill_through(&c, B).await;
    assert_eq!(
        code, "InternalError",
        "a full remote must surface as a server error, not as a success or a hang"
    );

    // Everything acked on the way down is still exactly what it was acked as. This is the assertion
    // the whole file exists for: a store filling up must not retroactively cost a client bytes it was
    // told were held.
    for (key, body) in &acked {
        assert_eq!(
            get_all(&c, B, key).await,
            *body,
            "{key} was acked and must still read back byte-exact"
        );
    }

    // A fresh key against the now-full store: refused, and left cleanly absent rather than
    // half-created.
    let refused = c
        .put_object()
        .bucket(B)
        .key("never")
        .body(bytes_body(&pattern_seeded(CHUNK, 8)))
        .send()
        .await;
    assert_eq!(
        refusal(refused, "a PUT into a full remote"),
        "InternalError",
        "and to keep refusing rather than half-succeeding once full"
    );
    assert_eq!(
        refusal(
            c.get_object().bucket(B).key("never").send().await,
            "a GET of the key that PUT never created"
        ),
        "NoSuchKey",
        "a write that never committed must leave no trace to read"
    );

    // An overwrite of a live key: refused, and the previous generation still whole. This is the one
    // that would show a bracket that marked K and could not settle it.
    refusal(
        c.put_object()
            .bucket(B)
            .key("keeper")
            .body(bytes_body(&pattern_seeded(CHUNK, 9)))
            .send()
            .await,
        "an overwrite against a full remote",
    );
    assert_eq!(
        get_all(&c, B, "keeper").await,
        keeper,
        "a failed overwrite must leave the committed generation byte-exact"
    );
    let head = c
        .head_object()
        .bucket(B)
        .key("keeper")
        .send()
        .await
        .expect("HEAD of a committed key must still answer on a full store");
    assert_eq!(
        head.e_tag().map(|e| e.trim_matches('"')),
        Some(keeper_etag.as_str()),
        "and to carry the same ETag it was acked with"
    );

    // Reads are unaffected by a store that cannot take writes, so the namespace must still
    // enumerate — and enumerate exactly what was acked, with no entry for the writes that failed.
    let listed: Vec<String> = c
        .list_objects_v2()
        .bucket(B)
        .max_keys(1000)
        .send()
        .await
        .expect("LIST must still answer")
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect();
    let mut expected: Vec<String> = acked.iter().map(|(k, _)| k.clone()).collect();
    expected.push("keeper".to_string());
    expected.sort();
    assert_eq!(
        listed, expected,
        "LIST must show the acked set and nothing else"
    );

    // A delete needs no room, so it must still commit — and leave a definite absence, not a key that
    // reads as present through one path and absent through another.
    c.delete_object()
        .bucket(B)
        .key("keeper")
        .send()
        .await
        .expect("a delete frees space rather than needing it");
    assert_eq!(
        refusal(
            c.get_object().bucket(B).key("keeper").send().await,
            "a GET after the delete committed"
        ),
        "NoSuchKey"
    );

    assert!(
        !halted(&h, &remote).await,
        "a store with no room left is an error, not an invariant violation"
    );
    let (status, _) = admin_get(&h, "/readyz").await;
    assert_eq!(status, 200, "the process must still be serving");
    h.stop_hypha().await;
}

/// **Cached mode, full remote.** The commit point is the cache, so a full remote must not stop hypha
/// acking — that separation is the whole reason cached mode exists. What it must not do is *forget*:
/// a marker stays raised for as long as its upload cannot happen, survives a graceful shutdown, and
/// the bytes stay readable from the cache across a restart the remote is no better after.
///
/// It also pins the distinction a full store makes easy to get wrong. The run still ends **clean**:
/// the clean marker says the pending-marker range is a *complete account* of the pending set (§6),
/// not that the set is empty. Every marker here was written — to the healthy cache — so the account
/// is complete, and what is outstanding is the upload, which is what a pending set is for.
#[tokio::test]
async fn a_full_remote_still_acks_cached_writes_and_carries_the_obligation() {
    let Some(tiny) = tiny_backend() else { return };
    let remote = tiny_client(&tiny);
    let endpoint = tiny.clone();
    let mut h = Harness::builder(Mode::Cached)
        .tune(move |c| point_at(&mut c.remote, endpoint))
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();

    let early = pattern_seeded(CHUNK, 11);
    put(&c, B, "early", &early).await;
    let reached_remote = |key: &'static str| {
        let remote = remote.clone();
        let bucket = h.remote_bucket(B);
        async move { reached_remote_key(&remote, &bucket, key).await }
    };
    wait_until(10_000, "the sweep to upload the first object", || {
        reached_remote("early")
    })
    .await;

    exhaust(&remote).await;

    // Write batches until the sweep visibly stalls. Adaptive rather than a fixed count because
    // SeaweedFS's per-volume size limit is soft — an existing volume keeps taking writes past it, so
    // how much a collection swallows before the store refuses is not a number a test should hardcode.
    // Every write acks throughout: the ack is the cache write, and the cache is fine.
    let mut written: Vec<(String, Vec<u8>)> = Vec::new();
    let mut still_owed: Vec<String> = Vec::new();
    for round in 0..24u8 {
        for i in 0..8u8 {
            let key = format!("late{round}_{i}");
            let body = pattern_seeded(CHUNK, round.wrapping_mul(8).wrapping_add(i));
            let etag = put(&c, B, &key, &body).await;
            assert_eq!(
                etag,
                md5_hex(&body),
                "a full remote must not stop the cache acking a write it holds"
            );
            written.push((key, body));
        }
        // Well past the harness's 150 ms cadence, so a marker still standing here is one the remote
        // refused rather than one the sweep has not reached.
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        still_owed = owed(&h, &written).await;
        if !still_owed.is_empty() {
            break;
        }
    }
    // An upload that failed has to leave its marker standing: clearing it is the one thing that would
    // lose the key.
    assert!(
        !still_owed.is_empty(),
        "the sweep drained {} writes without the store ever refusing one — the fixture is not \
         undersized enough for this test to mean anything",
        written.len()
    );
    for key in &still_owed {
        assert!(
            !reached_remote_key(&remote, &h.remote_bucket(B), key).await,
            "{key} is still owed, so the remote must not hold it"
        );
        let body = &written
            .iter()
            .find(|(k, _)| k == key)
            .expect("owed key was written here")
            .1;
        assert_eq!(
            get_all(&c, B, key).await,
            *body,
            "{key} was acked, so the cache must still serve it byte-exact"
        );
    }
    assert!(
        reached_remote("early").await,
        "the object uploaded before the store filled must still be there"
    );

    // A graceful drain still ends **clean**, and that is the distinction worth pinning: the clean
    // marker says the pending-marker range is a *complete* account of the pending set (§6), not that
    // the set is empty. Every marker here was written — to the healthy cache — so the account is
    // complete; what is outstanding is the upload, which is the pending set's whole purpose.
    h.stop_hypha().await;
    assert!(
        raw_exists(&h, &h.meta_bucket(B), &meta::clean_marker_key()).await,
        "an undeliverable upload is a complete pending set, not an unaccounted one"
    );
    for key in &still_owed {
        assert!(
            marker_present(&h, B, key).await,
            "{key}'s obligation must survive the shutdown that vouched for it"
        );
    }

    // And the next run must serve the bytes it inherited, without needing the remote to have caught
    // up first — the pending set is carried, not replayed from a store that still cannot take it.
    h.start_hypha().await;
    let c = h.client();
    for key in &still_owed {
        let body = &written
            .iter()
            .find(|(k, _)| k == key)
            .expect("owed key was written here")
            .1;
        assert_eq!(
            get_all(&c, B, key).await,
            *body,
            "{key} must still read byte-exact after a restart with the remote still full"
        );
    }
    assert!(
        !halted(&h, &remote).await,
        "an unpropagatable write is a pending obligation, not a violated invariant"
    );
    h.stop_hypha().await;
}

/// **Cached mode, full cache.** The mirror image, and the one that must *not* ack: the cache is where
/// a cached write commits, so a cache with no room left has nowhere to put the commit and the client
/// has to be told. An ack here would be the worst failure in the system — a client told its bytes are
/// safe when nothing holds them.
#[tokio::test]
async fn a_full_cache_never_acks_a_cached_write_it_could_not_commit() {
    let Some(tiny) = tiny_backend() else { return };
    let cache = tiny_client(&tiny);
    let endpoint = tiny.clone();
    let h = Harness::builder(Mode::Cached)
        .tune(move |c| point_at(&mut c.cache, endpoint))
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();

    let keeper = pattern_seeded(CHUNK, 21);
    let keeper_etag = put(&c, B, "keeper", &keeper).await;

    exhaust(&cache).await;
    let (acked, code) = fill_through(&c, B).await;
    assert_eq!(
        code, "InternalError",
        "a cache with no room left must surface as a server error, not as a success"
    );
    for (key, body) in &acked {
        assert_eq!(
            get_all(&c, B, key).await,
            *body,
            "{key} was acked and must still read back byte-exact"
        );
    }

    refusal(
        c.put_object()
            .bucket(B)
            .key("keeper")
            .body(bytes_body(&pattern_seeded(CHUNK, 22)))
            .send()
            .await,
        "an overwrite a full cache cannot hold",
    );
    assert_eq!(
        get_all(&c, B, "keeper").await,
        keeper,
        "and the generation it could not replace must survive intact"
    );
    let head = c
        .head_object()
        .bucket(B)
        .key("keeper")
        .send()
        .await
        .expect("HEAD of the surviving generation");
    assert_eq!(
        head.e_tag().map(|e| e.trim_matches('"')),
        Some(keeper_etag.as_str())
    );

    refusal(
        c.put_object()
            .bucket(B)
            .key("fresh")
            .body(bytes_body(&pattern_seeded(CHUNK, 23)))
            .send()
            .await,
        "a write to a key that did not exist",
    );
    assert_eq!(
        refusal(
            c.get_object().bucket(B).key("fresh").send().await,
            "a GET of the key that write never created"
        ),
        "NoSuchKey",
        "which must therefore still not exist"
    );

    // The remote is healthy here, so the halt marker is writable — its absence is a real assertion
    // rather than an artefact of the store being full.
    assert!(
        !halted(&h, &h.raw_remote()).await,
        "a full cache is an error the client is told about, not a violated invariant"
    );
}
