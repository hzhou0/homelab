//! Additively rebuilds a bucket's cache projection from the remote.
//!
//! Existing cache entries are current either from an earlier restore attempt or a durable write
//! during this restore, so leaving them untouched makes the pass crash-idempotent.

use futures::TryStreamExt as _;

use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::halt::{Invariant, Violation};
use crate::tier::Tiering;

const PAGE_KEYS: i32 = 1000;

/// Each materialization is a trailer read plus two small writes, so this bounds remote fan-out; the
/// keys are independent, so serializing them would only make a restore slower.
const CONCURRENCY: usize = 16;

const PROBE_KEYS: i32 = 1000;

/// The caller writes the sync marker once this returns — that write is the only "done" signal, so a
/// pass that fails part-way simply re-runs.
pub(super) async fn namespace(tier: &Tiering, bucket: &str) -> Result<()> {
    probe_for_native_objects(tier, bucket).await?;

    let mut token: Option<String> = None;
    loop {
        let page = tier
            .remote
            .list(bucket, None, None, token.take(), None, Some(PAGE_KEYS))
            .await?;
        let keys: Vec<String> = page
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|o| o.key)
            .filter(|k| !meta::is_reserved_remote_key(k))
            .collect();

        futures::stream::iter(keys.into_iter().map(Ok))
            .try_for_each_concurrent(CONCURRENCY, |key| async move {
                let _guard = tier.write_locks.lock(bucket, &key).await;
                tier.materialize_absent_locked(bucket, &key).await
            })
            .await?;

        match page.next_continuation_token {
            Some(t) => token = Some(t),
            None => return Ok(()),
        }
    }
}

/// Invariant **I1**, as a bounded probe: one `<data>` page up front rather than a full cache listing
/// correlated against the remote for the whole pass.
///
/// **A bug detector, not a correctness gate.** The sample is decisive in the failure R1 exists for,
/// where a lost volume leaves `<data>` empty, and merely a sample on a crash-resumed pass. Missing
/// one costs nothing — the restore is additive, so it cannot overwrite the body, and the cached
/// write that produced it owed a pending marker by the ordinary route. What the check buys is
/// catching the leak *loudly*, near the code that caused it.
async fn probe_for_native_objects(tier: &Tiering, bucket: &str) -> Result<()> {
    let page = match tier
        .data
        .list(bucket, None, None, None, None, Some(PROBE_KEYS))
        .await
    {
        Ok(p) => p,
        // Not provisioned yet: the emptiest a namespace gets.
        Err(Error::NoSuchBucket) => return Ok(()),
        Err(e) => return Err(e),
    };
    for o in page.contents.unwrap_or_default() {
        let etag = o.e_tag.clone().unwrap_or_default();
        if meta::classify_entry(o.size.unwrap_or(0), etag.trim_matches('"')).is_some() {
            continue; // a tombstone: exactly what a restore expects to find
        }
        tier.halt
            .raise(Violation {
                invariant: Invariant::PlaintextDuringRestore,
                bucket: bucket.to_string(),
                key: o.key,
                detail: "a live plaintext body is in <data> while this bucket's namespace is \
                         restoring; writes must run with durable semantics for the whole restore, \
                         so the write-mode gate has leaked"
                    .to_string(),
            })
            .await
    }
    Ok(())
}
