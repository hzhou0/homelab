//! Random faults, indiscriminately, across every backend call — and the properties that must hold
//! anyway.
//!
//! The rest of the suite injects faults the author chose: this method, this path, this many times.
//! That is what makes those tests readable, and also what bounds them to failures somebody already
//! thought of. Here a share of *everything* fails — a HEAD hypha takes for a reason no test names, a
//! twin write, a marker clear, the second leg of a bracket — and instead of asserting a specific
//! outcome, the tests assert the contract that has to survive whatever happened.
//!
//! **The oracle is a set, not a value.** A faulted operation is *indeterminate*: the SDK retries, and
//! a fault injected after the backend acted (`lose_responses`) lands the operation and reports
//! failure anyway, so an error means "committed or not" rather than "not committed". Each key
//! therefore carries the set of values it is allowed to hold — an ack narrows it to one, an error
//! widens it by the value that was attempted — and the check is membership. What that still forbids
//! is everything that matters: a value nobody wrote, a body that disagrees with the ETag served
//! beside it, a LIST that disagrees with GET, and any hybrid of two generations.
//!
//! Faults are lifted before anything is verified. During the storm hypha is *expected* to fail
//! requests; the claim is about the state it leaves behind, and reading that state needs a backend
//! that answers.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use common::*;
use hypha_core::config::Mode;
use hypha_core::meta;

const B: &str = "chaos";

/// Small enough that a storm is fast, wide enough that overwrite, delete-then-recreate and
/// resurrect-after-delete all happen within one run.
const KEYS: usize = 6;
const OPS: usize = 60;

/// Share of backend calls disturbed. High enough that most operations meet at least one fault —
/// a rate that only occasionally breaks something tests the happy path with extra steps.
const RATE: f64 = 0.25;

fn key_of(i: usize) -> String {
    format!("k{}", i % KEYS)
}

/// A seeded op stream. `rand` is already a dev-dependency, but an LCG that lives here means the op
/// sequence a seed produces cannot change under a dependency bump — the seed is printed on failure
/// and has to still mean the same run tomorrow.
struct Ops(u64);

impl Ops {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// One state a key may be in: its bytes and the ETag they must be served under. `None` is
/// client-visible absence.
///
/// The ETag rides along rather than being derived at the assertion, because the rule differs by how
/// the object was written: a PUT's ETag is the plaintext MD5, a composite's is `md5(concat pmd5s)-N`
/// (§6). Carrying it is what lets the check be "the ETag served is the one this *write* would have
/// produced" rather than the weaker "it is some MD5".
type State = Option<(Vec<u8>, String)>;
type Allowed = Vec<State>;

fn widen(model: &mut BTreeMap<String, Allowed>, key: &str, value: State) {
    let allowed = model.entry(key.to_string()).or_insert_with(|| vec![None]);
    if !allowed.contains(&value) {
        allowed.push(value);
    }
}

fn narrow(model: &mut BTreeMap<String, Allowed>, key: &str, value: State) {
    model.insert(key.to_string(), vec![value]);
}

/// A human-readable shape for a state, for the assertion message — the bodies themselves are
/// kilobytes of pattern and print as noise.
fn shape(state: &State) -> String {
    match state {
        None => "absent".to_string(),
        Some((body, etag)) => format!("{} bytes / {etag}", body.len()),
    }
}

/// What hypha actually holds for `key`, with the self-consistency checks that need no model at all:
/// the ETag served with a body must be that body's MD5, and HEAD must agree with GET about its size.
/// Those two are what would catch a generation's bytes served under another's facts.
async fn observe(c: &aws_sdk_s3::Client, key: &str) -> State {
    let got = match c.get_object().bucket(B).key(key).send().await {
        Ok(out) => out,
        Err(e) => {
            assert_eq!(
                sdk_err_code(&e).as_deref(),
                Some("NoSuchKey"),
                "after the storm, a read must either answer or 404 — not {e:?}"
            );
            return None;
        }
    };
    let etag = got
        .e_tag()
        .expect("a served object carries an ETag")
        .trim_matches('"')
        .to_string();
    let body = got.body.collect().await.expect("collect body").to_vec();
    let head = c
        .head_object()
        .bucket(B)
        .key(key)
        .send()
        .await
        .expect("HEAD of a key GET just served");
    assert_eq!(
        head.content_length(),
        Some(body.len() as i64),
        "{key}: HEAD and GET must agree on the size"
    );
    assert_eq!(
        head.e_tag().map(|e| e.trim_matches('"').to_string()),
        Some(etag.clone()),
        "{key}: HEAD and GET must agree on the ETag"
    );
    Some((body, etag))
}

/// Every key against its allowed set, plus LIST against what the reads actually found.
async fn verify(c: &aws_sdk_s3::Client, model: &BTreeMap<String, Allowed>) {
    let mut present: Vec<String> = Vec::new();
    for (key, allowed) in model {
        let observed = observe(c, key).await;
        assert!(
            allowed.contains(&observed),
            "{key}: holds {}, which no acked or attempted operation allows (allowed: {:?})",
            shape(&observed),
            allowed.iter().map(shape).collect::<Vec<_>>()
        );
        if observed.is_some() {
            present.push(key.clone());
        }
    }
    present.sort();

    let listed: Vec<String> = c
        .list_objects_v2()
        .bucket(B)
        .send()
        .await
        .expect("LIST after the storm")
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect();
    assert_eq!(
        listed, present,
        "LIST must name exactly the keys that read back"
    );
}

/// Block until the remote holds exactly the keys hypha serves. Both R2 (which re-raises what the
/// storm cost) and the sweep that discharges what it raises are asynchronous, so this is the shape
/// convergence has to be waited for in.
async fn await_agreement(h: &Harness, c: &aws_sdk_s3::Client, keys: &[String]) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut disagreed = None;
        for key in keys {
            let served = c.head_object().bucket(B).key(key).send().await.is_ok();
            if served != remote_present(h, B, key).await {
                disagreed = Some(key.clone());
                break;
            }
        }
        match disagreed {
            None => return,
            Some(key) => assert!(
                std::time::Instant::now() < deadline,
                "{key}: the two tiers never agreed, so the storm cost an obligation nothing \
                 rebuilt"
            ),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Block until no key in `keys` owes a pending marker. A free function rather than a `wait_until`
/// closure so it borrows the harness for the call and no longer — the caller needs it mutably to
/// restart hypha.
async fn await_drained(h: &Harness, keys: &[String], when: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let mut owed = None;
        for key in keys {
            if marker_present(h, B, key).await {
                owed = Some(key.clone());
                break;
            }
        }
        match owed {
            None => return,
            Some(key) => assert!(
                std::time::Instant::now() < deadline,
                "the pending set never drained {when}: {key} is still owed, so its operation \
                 cannot be replayed"
            ),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Drive `OPS` random operations, updating the model by whether each was acked. Returns the model.
///
/// Multipart is in the mix because its bracket is the longest one hypha has — create, part, complete,
/// settle — so it has the most places to be interrupted, and its failure mode (a composite that
/// half-committed) is the one a single-request op cannot produce.
async fn storm(c: &aws_sdk_s3::Client, seed: u64) -> BTreeMap<String, Allowed> {
    let mut ops = Ops(seed);
    let mut model: BTreeMap<String, Allowed> = BTreeMap::new();
    for key in (0..KEYS).map(key_of) {
        model.insert(key, vec![None]);
    }

    for _ in 0..OPS {
        let key = key_of(ops.below(KEYS));
        match ops.below(8) {
            0 | 1 => {
                let deleted = c.delete_object().bucket(B).key(&key).send().await;
                match deleted.is_ok() {
                    true => narrow(&mut model, &key, None),
                    false => widen(&mut model, &key, None),
                }
            }
            2 => {
                // A one-part upload: legal at any size (the part is final) and cheap, so the bracket
                // is exercised rather than the transfer.
                let body = pattern_seeded(4096 + ops.below(4096), ops.below(255) as u8);
                let created = c.create_multipart_upload().bucket(B).key(&key).send().await;
                let Ok(created) = created else { continue };
                let upload_id = created.upload_id().expect("upload id").to_string();
                let part = c
                    .upload_part()
                    .bucket(B)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(1)
                    .body(bytes_body(&body))
                    .send()
                    .await;
                let Ok(part) = part else { continue };
                let etag = part
                    .e_tag()
                    .expect("part etag")
                    .trim_matches('"')
                    .to_string();
                let completed = c
                    .complete_multipart_upload()
                    .bucket(B)
                    .key(&key)
                    .upload_id(&upload_id)
                    .multipart_upload(
                        aws_sdk_s3::types::CompletedMultipartUpload::builder()
                            .parts(
                                aws_sdk_s3::types::CompletedPart::builder()
                                    .part_number(1)
                                    .e_tag(etag)
                                    .build(),
                            )
                            .build(),
                    )
                    .send()
                    .await;
                // A composite's client ETag is `md5(concat of the parts' plaintext MD5s)-N`, not the
                // whole object's MD5 (§6) — the same object written by PUT would carry a different
                // one, which is why the model records the ETag beside the bytes.
                let state = Some((body.clone(), expected_composite_etag(&[&body])));
                match completed.is_ok() {
                    true => narrow(&mut model, &key, state),
                    false => widen(&mut model, &key, state),
                }
            }
            _ => {
                // Sizes straddle the 64 KiB chunk boundary, where a partially written body would show
                // as a decrypt failure rather than as short bytes.
                let size = [0usize, 1, 4096, 65_535, 65_536, 65_537][ops.below(6)];
                let body = pattern_seeded(size, ops.below(255) as u8);
                let put = c
                    .put_object()
                    .bucket(B)
                    .key(&key)
                    .body(bytes_body(&body))
                    .send()
                    .await;
                let state = Some((body.clone(), md5_hex(&body)));
                match put.is_ok() {
                    true => narrow(&mut model, &key, state),
                    false => widen(&mut model, &key, state),
                }
            }
        }
    }
    model
}

/// Durable mode: the remote is the commit point, so every op's bracket runs against a backend that
/// may refuse either leg — or accept one and hide it. What survives has to be a state some operation
/// asked for, served coherently.
#[tokio::test]
async fn random_backend_faults_leave_every_key_in_a_state_someone_asked_for() {
    let seed = rand::random::<u64>();
    eprintln!("chaos seed {seed}");
    let h = Harness::durable_with_faults().await;
    h.create_bucket(B).await;
    let c = h.client();

    h.cache_faults().chaos(seed, RATE, true);
    h.remote_faults().chaos(seed ^ 0x5DEE_CE66, RATE, true);
    let model = storm(&c, seed).await;
    h.cache_faults().calm();
    h.remote_faults().calm();

    verify(&c, &model).await;
    assert!(
        !raw_exists(&h, &h.remote_bucket(B), &meta::halt_marker_key()).await,
        "a backend that fails requests is not a violated invariant"
    );
}

/// Cached mode adds a second thing to be right about: the storm is over, so the propagation it
/// prevented has to finish. Convergence is the assertion — every key the cache serves reaches the
/// remote, every key it does not is gone from the remote — and a marker cleared for an upload that
/// never happened shows up here as a remote left behind.
///
/// Asserted **across a restart**, because the storm can cost an obligation outright rather than
/// delay one, and that case is recovered by the next run rather than by this one (see below). Note
/// what is waited on afterwards: the *property*, not an empty pending set. Straight after a restart
/// the set is empty because R2 has not raised anything yet, so waiting on emptiness would sample
/// that and call it converged.
#[tokio::test]
async fn random_backend_faults_leave_a_cached_deployment_convergent() {
    let seed = rand::random::<u64>();
    eprintln!("chaos seed {seed}");
    let mut h = Harness::cached_with_faults().await;
    h.create_bucket(B).await;
    let c = h.client();

    h.cache_faults().chaos(seed, RATE, true);
    h.remote_faults().chaos(seed ^ 0x5DEE_CE66, RATE, true);
    let model = storm(&c, seed).await;
    h.cache_faults().calm();
    h.remote_faults().calm();

    verify(&c, &model).await;

    // The sweep retries on its own cadence, so convergence is waited for rather than asserted at
    // once. What is asserted is that it *reaches* it: a pending set that never empties is a marker
    // whose operation cannot be replayed.
    let keys: Vec<String> = model.keys().cloned().collect();
    await_drained(&h, &keys, "after the storm").await;

    // Then a restart, because the storm can have cost an obligation outright: a cached commit that
    // landed and lost its response returns an error from *between* the commit and the marker queue
    // (§7), so the marker was never owed and no sweep will ever fix the divergence. What the run does
    // in that case is refuse to vouch for the bucket — no clean marker — which is precisely an
    // instruction to the next run to rebuild the pending set from both namespaces (R2). Convergence
    // is therefore a property of hypha *across* the restart, not within one run of it.
    h.stop_hypha().await;
    let vouched = raw_exists(&h, &h.meta_bucket(B), &meta::clean_marker_key()).await;
    h.start_hypha().await;
    let c = h.client();
    // Waited on the property itself, not on an empty pending set: right after a restart the set *is*
    // empty, because R2 has not raised anything yet. Polling emptiness would sample that and call it
    // converged.
    await_agreement(&h, &c, &keys).await;

    // The restart must not have changed what any key holds, and the remote must now agree.
    verify(&c, &model).await;
    for key in &keys {
        let cached = observe(&c, key).await;
        let on_remote = remote_present(&h, B, key).await;
        assert_eq!(
            on_remote,
            cached.is_some(),
            "{key}: after a restart and an empty pending set the remote must agree with the cache \
             (cache {}, remote {on_remote}, the run vouched for its pending set: {vouched})",
            shape(&cached),
        );
    }
    assert!(
        !raw_exists(&h, &h.remote_bucket(B), &meta::halt_marker_key()).await,
        "a backend that fails requests is not a violated invariant"
    );
}

/// The claim nothing in-process can make: **the binary is still there**. A panic in a handler is
/// invisible to an in-process harness — hyper turns it into a 500 and the test carries on — and a
/// panic in a background actor is quieter still. Against the real process, a storm at a punishing
/// rate has to leave something that is alive, ready, and serving.
#[tokio::test]
async fn a_storm_of_faults_leaves_the_process_alive_and_ready() {
    let seed = rand::random::<u64>();
    eprintln!("chaos seed {seed}");
    let mut h = Harness::builder(Mode::Cached)
        .subprocess()
        .with_faults()
        .start()
        .await;
    h.create_bucket(B).await;
    let c = h.client();

    // Half of everything, responses lost as well as refused — well past what a deployment would
    // survive usefully, which is the point: the process must not be what breaks.
    h.cache_faults().chaos(seed, 0.5, true);
    h.remote_faults().chaos(seed ^ 0x5DEE_CE66, 0.5, true);
    let model = storm(&c, seed).await;
    // Reads under the storm too — every one must be an S3 answer, never a dropped connection.
    for key in model.keys() {
        if let Err(e) = c.get_object().bucket(B).key(key).send().await {
            assert!(
                sdk_err_code(&e).is_some(),
                "a read during the storm must still come back as an S3 error: {e:?}"
            );
        }
    }
    h.cache_faults().calm();
    h.remote_faults().calm();

    // Background duties keep running on the faults that were still in flight; give them a moment to
    // do whatever they were going to do before asking whether anything died.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (status, _) = admin_get(&h, "/healthz").await;
    assert_eq!(status, 200, "the process must still be alive");
    let (status, _) = admin_get(&h, "/readyz").await;
    assert_eq!(status, 200, "and still ready");

    verify(&c, &model).await;
    assert!(
        !raw_exists(&h, &h.remote_bucket(B), &meta::halt_marker_key()).await,
        "a backend that fails requests is not a violated invariant"
    );

    // A graceful drain has to still be possible: an actor that died under the storm would show up
    // here as a shutdown that never completes.
    h.stop_hypha().await;
}
