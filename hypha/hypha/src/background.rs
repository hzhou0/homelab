//! The background-transition actor (§8) — the owner of every *discardable* per-key transition.
//!
//! A rehydrate is best-effort by construction: the read that raised it is already being served from
//! the remote, so abandoning one costs the next read of that key a remote fetch and nothing else.
//! That is what makes the write lock safe to hand back mid-transition, and it is why these
//! transitions belong on one bounded queue rather than in a detached `tokio::spawn` per read:
//!
//! - **Bounded.** A burst of reads against evicted keys can otherwise spawn an unbounded number of
//!   whole-object downloads. Here they queue, `background.concurrency` run at once, and a full queue
//!   sheds new work instead of blocking the reads that raised it.
//! - **Deduped.** One live job per key. The registry entry *is* the dedup set, so N concurrent reads
//!   of one evicted key enqueue one transition, not N that each take the write lock in turn.
//!   ([`Reconciler::shadow_is_current`] still guards the *other* case — a job submitted after an
//!   earlier one already landed that generation.)
//! - **Cancellable.** §8 has rehydrate hold K's write lock across the whole fetch + decrypt + land,
//!   which would park a same-key conditional PUT, DELETE, or CompleteMultipartUpload behind a
//!   multi-minute transfer. Every client write instead cancels K's background transition first (see
//!   [`crate::s3::Hypha::write_lock`]) and the holder drops the lock at its next await. The spec's
//!   under-lock invariant is untouched: a rehydrate that *completes* still did every step under the
//!   lock. It is only ever abandoned wholesale.
//!
//! **Why the cancel needs no acknowledgement.** A job registers its token before it ever attempts
//! the lock, so a job that is blocking a client necessarily holds the lock and necessarily has a
//! live token — the cancel always finds it. A job that registers *after* the cancel has not taken
//! the lock yet, so it queues behind the client's own guard and blocks nobody. Either way the lock
//! handoff is the rendezvous, so `cancel` is a fire-and-forget map lookup on the write path.
//!
//! Lifecycle mirrors [`crate::bucket_ctl`]: the task holds a [`Reconciler`], never a `Hypha`, so it
//! neither keeps the service's liveness sentinel alive nor needs shutdown plumbing — it drains and
//! exits once the last handle drops.
//!
//! Phase 5 adds the GC scavenger's own transitions (evict, shadow-evict) as further [`Job`]
//! variants: they are discardable on exactly the same grounds — an eviction abandoned because a
//! client wants the key is an eviction that should not have run.

use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

use hypha_core::config;
use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::codec;
use crate::tier::Reconciler;

/// One queued transition.
pub(crate) enum Job {
    /// Fetch K from the remote and land its plaintext in the cache (§8): a single-part body at K
    /// itself, a composite into K's shadow body. `cetag` names the generation the read observed —
    /// the job is abandoned if K has moved on by the time it runs.
    Rehydrate {
        bucket: String,
        key: String,
        cetag: String,
        plen: u64,
    },
}

impl Job {
    fn registry_key(&self) -> String {
        match self {
            Job::Rehydrate { bucket, key, .. } => registry_key(bucket, key),
        }
    }
}

/// Registry key for a `(bucket, key)` pair. Bucket names cannot contain `/`, so the join is
/// unambiguous — and it costs one allocation per submit/cancel rather than an owned pair.
fn registry_key(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}

/// Keys with a transition queued or running → its cancel token. Doubles as the dedup set: an
/// occupied entry means this key already has a job, and a second would duplicate its work.
type Registry = Arc<DashMap<String, CancellationToken>>;

/// Handle onto the actor. Cloneable and cheap — the queue sender plus the registry — so every
/// `Hypha` clone shares one actor.
#[derive(Clone)]
pub struct Background {
    tx: mpsc::Sender<(Job, CancellationToken)>,
    live: Registry,
}

impl Background {
    /// Queue a transition, unless this key already has one. Never blocks and never fails visibly: a
    /// full queue (or an actor already gone at shutdown) drops the job, which is the correct load
    /// response for work whose only value is saving a *future* read a remote fetch.
    pub(crate) fn submit(&self, job: Job) {
        let jk = job.registry_key();
        let token = CancellationToken::new();
        // Scoped so the shard guard is released before `remove` below can want it.
        match self.live.entry(jk.clone()) {
            Entry::Occupied(_) => return,
            Entry::Vacant(vacant) => {
                vacant.insert(token.clone());
            }
        }
        if self.tx.try_send((job, token)).is_err() {
            self.live.remove(&jk);
        }
    }

    /// Tell any queued or running transition for `key` to stop, so a client write can take K's write
    /// lock without waiting out a whole-object fetch (§8). Fire-and-forget — see the module note on
    /// why no acknowledgement is needed.
    pub(crate) fn cancel(&self, bucket: &str, key: &str) {
        // A cancelled-but-not-yet-removed entry is re-cancelled harmlessly; the token is per-job, so
        // this can never poison a *later* transition of the same key.
        if let Some(token) = self.live.get(&registry_key(bucket, key)) {
            token.cancel();
        }
    }

    /// Whether `key` has a transition queued or running — tests and, in phase 5, the scavenger's
    /// skip check.
    #[cfg(test)]
    fn is_live(&self, bucket: &str, key: &str) -> bool {
        self.live.contains_key(&registry_key(bucket, key))
    }
}

pub(crate) fn spawn(tier: Reconciler, cfg: config::Background) -> Background {
    let (tx, rx) = mpsc::channel(cfg.queue_depth.max(1));
    let live: Registry = Arc::new(DashMap::new());
    let actor = Actor {
        rx,
        tier,
        live: live.clone(),
        sem: Arc::new(Semaphore::new(cfg.concurrency.max(1))),
    };
    tokio::spawn(actor.run());
    Background { tx, live }
}

struct Actor {
    rx: mpsc::Receiver<(Job, CancellationToken)>,
    tier: Reconciler,
    live: Registry,
    sem: Arc<Semaphore>,
}

impl Actor {
    /// Drain the queue until every handle drops. Awaiting a permit here rather than inside the
    /// spawned job is deliberate: it is what makes the mpsc back up and `submit` shed, instead of
    /// accumulating parked tasks that each hold a job's worth of state.
    async fn run(mut self) {
        while let Some((job, token)) = self.rx.recv().await {
            let jk = job.registry_key();
            // Cancelled while queued: the client that cancelled is already past us.
            if token.is_cancelled() {
                self.live.remove(&jk);
                continue;
            }
            let Ok(permit) = self.sem.clone().acquire_owned().await else {
                break; // semaphore closed — shutting down
            };
            let tier = self.tier.clone();
            let live = self.live.clone();
            tokio::spawn(async move {
                if let Err(e) = run_job(&tier, &job, &token).await {
                    tracing::debug!(job = %jk, error = %e, "background transition failed; the key's tombstone stands");
                }
                // Safe to remove unconditionally: an entry is only ever inserted into a *vacant*
                // slot and stays occupied for the job's whole life, so no newer token can be sitting
                // under this key.
                live.remove(&jk);
                drop(permit);
            });
        }
    }
}

async fn run_job(tier: &Reconciler, job: &Job, token: &CancellationToken) -> Result<()> {
    match job {
        Job::Rehydrate {
            bucket,
            key,
            cetag,
            plen,
        } => rehydrate(tier, bucket, key, cetag, *plen, token).await,
    }
}

/// Fetch + decrypt K from the remote and land it in the cache (§8), under K's write lock.
/// Re-confirms the eviction tombstone under the lock — a write or delete may have superseded it, in
/// which case there is nothing to rehydrate.
///
/// A composite rehydrate leaves K tombstoned (the plaintext goes to the shadow), so the tombstone
/// re-check alone doesn't stop repeated work: without the shadow-freshness check, a job enqueued
/// after an earlier one landed would re-download the whole object.
///
/// **Generation gate.** A job can sit queued while K is written, reconciled, and evicted afresh, and
/// the land CAS — conditional on a sentinel ETag that is constant across generations, so blind to
/// the evict → rehydrate → re-evict ABA — would then accept the *new* tombstone for the *old*
/// plaintext. So the tombstone's `cetag` is re-read under the lock and must still be the generation
/// the read observed. It also makes the client pass-through safe to take from the live tombstone
/// rather than from the read that raised the job.
async fn rehydrate(
    tier: &Reconciler,
    bucket: &str,
    key: &str,
    cetag: &str,
    plen: u64,
    token: &CancellationToken,
) -> Result<()> {
    let _guard = tier.locks.lock(key).await;

    // Cancelled while we were parked on the lock — or while an earlier holder ran. Checked before
    // any backend call so a cancelled job costs nothing but the lock acquisition.
    if token.is_cancelled() {
        return Ok(());
    }

    let head = match tier.data.head(bucket, key).await {
        Ok(h) => h,
        Err(Error::NotFound) => return Ok(()),
        Err(e) => return Err(e),
    };
    let tomb = head.metadata.clone().unwrap_or_default();
    if meta::tomb_kind(&tomb) != Some(meta::TombKind::Evict) {
        return Ok(());
    }
    if tomb.get(meta::CETAG).map(String::as_str) != Some(cetag) {
        return Ok(());
    }

    if meta::is_composite_etag(cetag) {
        // Already rehydrated by an earlier read of this generation — don't re-fetch.
        if tier.shadow_is_current(bucket, key, cetag).await? {
            return Ok(());
        }
        let land = async {
            let body = codec::blob_to_bytestream(
                tier.decrypt_remote_body(bucket, key, cetag, None).await?,
            );
            tier.land_shadow_locked(bucket, key, body, plen, cetag)
                .await
        };
        // The land PUT consumes the remote stream, so this select covers the whole transfer. A
        // cancel mid-PUT is safe: the shadow write is atomic and lands or doesn't.
        tokio::select! {
            biased;
            _ = token.cancelled() => Ok(()),
            r = land => r,
        }
    } else {
        let md = meta::passthrough_metadata(&tomb);
        let land = async move {
            let body = codec::blob_to_bytestream(
                tier.decrypt_remote_body(bucket, key, cetag, None).await?,
            );
            tier.land_rehydrated_single_locked(bucket, key, body, plen, md)
                .await
        };
        tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            r = land => r?,
        }
        // Outside the cancellable region on purpose: K is live again, and a cancel here would leave
        // it beside a stale twin (see `land_rehydrated_single_locked`).
        tier.delete_twins(bucket, key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A handle whose actor never runs, so queued jobs stay queued and the registry is observable.
    fn handle(depth: usize) -> (Background, mpsc::Receiver<(Job, CancellationToken)>) {
        let (tx, rx) = mpsc::channel(depth);
        (
            Background {
                tx,
                live: Arc::new(DashMap::new()),
            },
            rx,
        )
    }

    fn job(key: &str) -> Job {
        Job::Rehydrate {
            bucket: "b".into(),
            key: key.into(),
            cetag: "e".into(),
            plen: 0,
        }
    }

    #[tokio::test]
    async fn submit_dedups_by_key() {
        let (bg, mut rx) = handle(8);
        bg.submit(job("k"));
        bg.submit(job("k"));
        bg.submit(job("other"));
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok(), "distinct key must enqueue");
        assert!(
            rx.try_recv().is_err(),
            "a second job for a live key must be dropped, not queued"
        );
    }

    #[tokio::test]
    async fn full_queue_sheds_and_leaves_no_registry_entry() {
        let (bg, _rx) = handle(1);
        bg.submit(job("first"));
        bg.submit(job("shed"));
        assert!(bg.is_live("b", "first"), "the queued job stays live");
        assert!(
            !bg.is_live("b", "shed"),
            "a shed job must not leave a registry entry blocking later submits"
        );
    }

    #[tokio::test]
    async fn cancel_trips_the_queued_jobs_token() {
        let (bg, mut rx) = handle(4);
        bg.submit(job("k"));
        bg.cancel("b", "k");
        let (_job, token) = rx.try_recv().expect("job was queued");
        assert!(token.is_cancelled(), "cancel must reach a still-queued job");
    }

    #[tokio::test]
    async fn cancel_of_an_unknown_key_is_a_noop() {
        let (bg, _rx) = handle(4);
        bg.cancel("b", "never-submitted");
    }
}
