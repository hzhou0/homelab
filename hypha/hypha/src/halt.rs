//! Persistent deployment halt for violated storage invariants.
//!
//! A violation makes later answers and recovery unsafe. Serving stops before the marker is retried
//! to durability, then the process exits rather than panicking a single task. Future runs refuse to
//! start until an operator clears the marker.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use hypha_core::error::{Error, Result};
use hypha_core::meta;
use hypha_core::Backend;

/// Distinct from any conventional status (and from `sysexits.h`) so a supervisor can tell this
/// apart from an ordinary crash.
pub const EXIT_INVARIANT_VIOLATION: i32 = 86;

/// Short, because nothing is being served until the record lands.
const RECORD_RETRY: Duration = Duration::from_secs(2);

/// The enumerated properties §7's recoveries assume; a variant is added only alongside the check
/// that detects its violation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Invariant {
    /// A remote object whose tail trailer does not authenticate: either something other than hypha
    /// writes this bucket, or this process holds the wrong trailer key or an unknown format.
    ForeignObject,
    /// A live plaintext body in `<data>` while the bucket's namespace is restoring. Writes run
    /// durable for the whole of a restore precisely so this cannot happen, so one means the mode
    /// gate leaked and an acked write may exist that the restore is about to walk past.
    PlaintextDuringRestore,
    /// An eviction tombstone whose remote object is missing — the remote lost bytes hypha reported
    /// as committed, and the tombstone is the only remaining record that they existed.
    RemoteLostObject,
    /// A live bucket whose cache projection has gone out from under it — its sync marker missing,
    /// or `<meta>` itself absent while the bucket map still holds the bucket. Nothing removes
    /// either under a live bucket, and the run has been answering an absent key as the object's
    /// absence ever since — answers it cannot identify, let alone take back.
    CacheVolumeLost,
    /// A remote bucket hypha neither created nor resolved at startup. hypha owns both backends
    /// outright, so one that appears from nowhere means something else is writing the remote, and
    /// nothing downstream — least of all a restore that would project it into the cache — can
    /// assume the objects in it are hypha's.
    ForeignBucket,
    /// A pending-set rebuild reached a durable-mode deployment, which has no pending set. A
    /// programming error rather than a data one, but it means the recovery classification is wrong,
    /// so the rest of it cannot be trusted either.
    PendingRebuildInDurableMode,
}

impl Invariant {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Invariant::ForeignObject => "foreign-object",
            Invariant::PlaintextDuringRestore => "plaintext-during-restore",
            Invariant::RemoteLostObject => "remote-lost-object",
            Invariant::CacheVolumeLost => "cache-volume-lost",
            Invariant::ForeignBucket => "foreign-bucket",
            Invariant::PendingRebuildInDurableMode => "pending-rebuild-in-durable-mode",
        }
    }
}

pub(crate) struct Violation {
    pub invariant: Invariant,
    pub bucket: String,
    pub key: Option<String>,
    /// Free text for the operator: what was observed, not what it means.
    pub detail: String,
}

impl Violation {
    /// Plain text: the only consumer is a human deciding what to do about it, and a serialization
    /// format for a four-field record would be its own cost.
    fn render(&self) -> Vec<u8> {
        format!(
            "invariant: {}\nbucket: {}\nkey: {}\ndetail: {}\n",
            self.invariant.as_str(),
            self.bucket,
            self.key.as_deref().unwrap_or("-"),
            self.detail,
        )
        .into_bytes()
    }
}

/// Takes the remote alone rather than a `Tiering`, which is what lets `Tiering` hold one.
#[derive(Clone)]
pub(crate) struct Halt {
    remote: Backend,
    stop: Arc<Notify>,
    recording: Arc<AtomicBool>,
}

impl Halt {
    pub(crate) fn new(remote: Backend) -> Self {
        Halt {
            remote,
            stop: Arc::new(Notify::new()),
            recording: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) async fn shutdown_signalled(&self) {
        self.stop.notified().await
    }

    /// Diverges, so a detection site is a single statement with nothing to propagate and no error
    /// path to get wrong.
    pub(crate) async fn raise(&self, violation: Violation) -> ! {
        tracing::error!(
            invariant = violation.invariant.as_str(),
            bucket = violation.bucket,
            key = violation.key.as_deref().unwrap_or("-"),
            detail = violation.detail,
            "invariant violation: hypha's record of this bucket disagrees with the backends. \
             Shutting the server down and recording the halt marker; every restart will exit \
             until an operator resolves the violation and deletes the marker object."
        );

        // First violation wins: it describes the actual divergence, and one observed during the
        // wind-down is most likely its consequence.
        if self.recording.swap(true, Ordering::SeqCst) {
            std::future::pending().await
        }

        // Recording belongs to the process, not the task that happened to detect the violation:
        // connection and actor drains are allowed to abort that task after their budgets expire.
        tokio::spawn(record(self.remote.clone(), violation));
        // `notify_one`, not `notify_waiters`: the latter drops the signal if `serve` happens to be
        // between registrations, and this signal has no second chance.
        self.stop.notify_one();
        std::future::pending().await
    }

    /// One helper is what keeps every trailer-reading site (§6) on the same footing — none may
    /// treat a trailer that does not authenticate as a miss.
    pub(crate) async fn foreign_object(&self, bucket: &str, key: &str) -> ! {
        self.raise(Violation {
            invariant: Invariant::ForeignObject,
            bucket: bucket.to_string(),
            key: Some(key.to_string()),
            detail: "remote object carries no verifiable hypha trailer".to_string(),
        })
        .await
    }
}

async fn record(remote: Backend, violation: Violation) -> ! {
    let mut attempt: u64 = 0;
    while let Err(e) = remote
        .put_small(
            &violation.bucket,
            &meta::halt_marker_key(),
            violation.render(),
            Default::default(),
            None,
            None,
        )
        .await
    {
        attempt += 1;
        tracing::error!(
            bucket = violation.bucket, attempt, error = %e,
            "halt marker not recorded; the server is down and the write is still owed, retrying"
        );
        tokio::time::sleep(RECORD_RETRY).await;
    }
    tracing::error!(
        bucket = violation.bucket,
        invariant = violation.invariant.as_str(),
        "halt marker recorded; exiting"
    );
    std::process::exit(EXIT_INVARIANT_VIOLATION)
}

/// The other half of the crashloop: the run that *recorded* a violation exits from [`Halt::raise`],
/// every run after it exits from here. Must run before the listener opens, so a halted deployment
/// never serves a single request.
pub(crate) async fn exit_if_marked(remote: &Backend) -> Result<()> {
    let key = meta::halt_marker_key();
    for (bucket, _) in remote.list_buckets().await? {
        let present = match remote.head(&bucket, &key).await {
            Ok(_) => true,
            Err(Error::NotFound) | Err(Error::NoSuchBucket) => false,
            Err(e) => return Err(e),
        };
        if present {
            tracing::error!(
                bucket,
                "halt marker present: this deployment recorded an invariant violation and will not \
                 serve. Read the marker object for the original violation, resolve it, then delete \
                 the marker."
            );
            std::process::exit(EXIT_INVARIANT_VIOLATION)
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Everything else here ends the process, so it is left to the integration tests, which can
    /// observe the exit status and the recorded marker.
    #[test]
    fn rendered_marker_names_the_invariant_and_the_key() {
        let body = String::from_utf8(
            Violation {
                invariant: Invariant::RemoteLostObject,
                bucket: "b".into(),
                key: Some("k".into()),
                detail: "d".into(),
            }
            .render(),
        )
        .unwrap();
        assert!(body.contains("invariant: remote-lost-object"));
        assert!(body.contains("bucket: b"));
        assert!(body.contains("key: k"));
    }
}
