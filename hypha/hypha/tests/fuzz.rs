//! Property fuzzing: random sequences of PUT/overwrite/DELETE against hypha over real MinIO,
//! checked against an in-memory `BTreeMap` oracle. A small key alphabet forces frequent
//! overwrites and delete-then-recreate; sizes straddle the 64 KiB chunk boundary. After each
//! sequence, every surviving key must GET/HEAD/range byte-exact and LIST must equal the model's
//! keyset; every absent key must 404. Complements `hypha-format`'s in-crate `RangeReader` fuzzing.
//!
//! proptest is synchronous, so a `TestRunner` is driven by hand over a locally-owned runtime and
//! harness — both drop at the end of the test, so the MinIO process and its data are cleaned up.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use common::*;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestError, TestRunner};

const KEYS: u8 = 6;
static BUCKET_SEQ: AtomicU64 = AtomicU64::new(0);

fn key_of(i: u8) -> String {
    // Flat keys only: none is a `x/`-prefix of another, sidestepping MinIO's object-vs-prefix quirk.
    format!("k{}", i % KEYS)
}

#[test]
fn model_based_fuzz() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let harness = rt.block_on(Harness::durable());

    // (is_put, key index, size). Sizes straddle the chunk boundary; delete otherwise.
    let size = prop::sample::select(vec![0u32, 1, 1000, 65_535, 65_536, 65_537, 131_072]);
    let op = (any::<bool>(), 0u8..KEYS, size);
    let seq = prop::collection::vec(op, 1..9);

    let mut runner = TestRunner::new(Config {
        cases: 20,
        max_shrink_iters: 8,
        ..Config::default()
    });

    let result = runner.run(&seq, |ops| {
        let bucket = format!("fuzz{}", BUCKET_SEQ.fetch_add(1, Ordering::Relaxed));
        rt.block_on(run_sequence(&harness, &bucket, ops))
            .map_err(TestCaseError::fail)?;
        Ok(())
    });

    if let Err(TestError::Fail(reason, ops)) = result {
        panic!("fuzz failure: {reason}\nminimized ops: {ops:?}");
    }
    result.expect("fuzz run");
}

/// Apply `ops` to hypha and to the model in lockstep, then assert hypha's observable state matches
/// the model exactly.
async fn run_sequence(h: &Harness, bucket: &str, ops: Vec<(bool, u8, u32)>) -> Result<(), String> {
    let client = h.client();
    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| format!("create bucket: {e}"))?;

    let mut model: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (is_put, ki, size) in ops {
        let key = key_of(ki);
        if is_put {
            let body = pattern_seeded(size as usize, ki.wrapping_add(size as u8).wrapping_add(1));
            let etag = put(&client, bucket, &key, &body).await;
            if etag != md5_hex(&body) {
                return Err(format!("PUT {key} etag {etag} != md5 {}", md5_hex(&body)));
            }
            model.insert(key, body);
        } else {
            client
                .delete_object()
                .bucket(bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| format!("delete {key}: {e}"))?;
            model.remove(&key);
        }
    }

    // Every surviving key: byte-exact GET, correct HEAD size, and a boundary-spanning range.
    for (key, want) in &model {
        let got = get_all(&client, bucket, key).await;
        if got != *want {
            return Err(format!(
                "GET {key}: {} bytes != model {}",
                got.len(),
                want.len()
            ));
        }
        let head = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| format!("head {key}: {e}"))?;
        if head.content_length() != Some(want.len() as i64) {
            return Err(format!(
                "HEAD {key} size {:?} != {}",
                head.content_length(),
                want.len()
            ));
        }
        if want.len() > 4 {
            let mid = (want.len() / 2) as u64;
            let last = (want.len() as u64 - 1).min(mid + 3);
            let got = get_range(&client, bucket, key, mid, last).await;
            if got != want[mid as usize..=last as usize] {
                return Err(format!("range {key} [{mid}..={last}] mismatch"));
            }
        }
    }

    // LIST equals the model's keyset, in order.
    let listed = list_client_keys(&client, bucket).await;
    let expected: Vec<String> = model.keys().cloned().collect();
    if listed != expected {
        return Err(format!("LIST {listed:?} != model {expected:?}"));
    }

    // Absent keys 404.
    for i in 0..KEYS {
        let key = key_of(i);
        if model.contains_key(&key) {
            continue;
        }
        let get = client.get_object().bucket(bucket).key(&key).send().await;
        match get {
            Err(e) if sdk_err_code(&e).as_deref() == Some("NoSuchKey") => {}
            Err(e) => return Err(format!("absent {key}: unexpected error {e}")),
            Ok(_) => return Err(format!("absent {key} unexpectedly readable")),
        }
    }
    Ok(())
}

async fn list_client_keys(client: &aws_sdk_s3::Client, bucket: &str) -> Vec<String> {
    client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .expect("list")
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect()
}
