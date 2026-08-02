//! Bounded, deduplicated, cancellable best-effort rehydration.
//!
//! Reads already succeed remotely, so overload may shed queued work. A transition registers its
//! cancellation token before taking the key lock; that lock handoff lets client writes cancel
//! without waiting for an acknowledgement or risking a half-applied rehydrate.

use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use hypha_core::config;
use hypha_core::error::{Error, Result};
use hypha_core::meta;

use crate::codec;
use crate::tier::Tiering;

pub(crate) enum Transition {
    /// Fetch K from the remote and land its plaintext in the cache : a single-part body at K
    /// itself, a composite into K's shadow body. `cetag` names the generation the read observed —
    /// the transition is abandoned if K has moved on by the time it runs.
    Rehydrate {
        bucket: String,
        key: String,
        cetag: String,
        plen: u64,
    },
}

impl Transition {
    fn registry_key(&self) -> String {
        match self {
            Transition::Rehydrate { bucket, key, .. } => registry_key(bucket, key),
        }
    }
}

/// Bucket names cannot contain `/`, so the join is unambiguous — and it costs one allocation per
/// submit/cancel rather than an owned pair.
fn registry_key(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}

/// Keys with a transition queued or running → its cancel token. Doubles as the dedup set: an
/// occupied entry means this key already has a transition, and a second would duplicate its work.
type LiveTransitions = Arc<DashMap<String, CancellationToken>>;

#[derive(Clone)]
pub struct Background {
    tx: mpsc::Sender<(Transition, CancellationToken)>,
    live: LiveTransitions,
}

impl Background {
    /// Queue a transition, unless this key already has one. Never blocks and never fails visibly: a
    /// full queue (or an actor already gone at shutdown) drops the transition, which is the correct
    /// load response for work whose only value is saving a *future* read a remote fetch.
    pub(crate) fn submit(&self, transition: Transition) {
        let registered = transition.registry_key();
        let token = CancellationToken::new();
        // Scoped so the shard guard is released before `remove` below can want it.
        match self.live.entry(registered.clone()) {
            Entry::Occupied(_) => return,
            Entry::Vacant(vacant) => {
                vacant.insert(token.clone());
            }
        }
        if self.tx.try_send((transition, token)).is_err() {
            self.live.remove(&registered);
        }
    }

    /// Tell any queued or running transition for `key` to stop, so a client write can take K's write
    /// lock without waiting out a whole-object fetch . Fire-and-forget — see the module note on
    /// why no acknowledgement is needed.
    pub(crate) fn cancel(&self, bucket: &str, key: &str) {
        // A cancelled-but-not-yet-removed entry is re-cancelled harmlessly; the token is
        // per-transition, so this can never poison a *later* transition of the same key.
        if let Some(token) = self.live.get(&registry_key(bucket, key)) {
            token.cancel();
        }
    }

    #[cfg(test)]
    fn is_live(&self, bucket: &str, key: &str) -> bool {
        self.live.contains_key(&registry_key(bucket, key))
    }
}

pub(crate) fn spawn(
    tier: Tiering,
    cfg: config::Background,
    shutdown: CancellationToken,
) -> (Background, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(cfg.queue_depth.max(1));
    let live: LiveTransitions = Arc::new(DashMap::new());
    let actor = TransitionActor {
        rx,
        tier,
        live: live.clone(),
        sem: Arc::new(Semaphore::new(cfg.concurrency.max(1))),
        running: JoinSet::new(),
        shutdown,
    };
    let task = tokio::spawn(actor.run());
    (Background { tx, live }, task)
}

struct TransitionActor {
    rx: mpsc::Receiver<(Transition, CancellationToken)>,
    tier: Tiering,
    live: LiveTransitions,
    sem: Arc<Semaphore>,
    /// The transitions running now. Tracked rather than detached so the drain can wait on them: a
    /// rehydrate killed between landing a body and deleting its twin leaves the one hybrid state
    /// every path orders away, and it is the *last* step of the transition that does that.
    running: JoinSet<()>,
    shutdown: CancellationToken,
}

impl TransitionActor {
    /// Run queued transitions until the service drops or the drain signals. Awaiting a permit here
    /// rather than inside the spawned transition is deliberate: it is what makes the mpsc back up and
    /// `submit` shed, instead of accumulating parked tasks that each hold a transition's worth of
    /// state.
    ///
    /// **At shutdown the queue is shed, not worked through.** A queued transition's whole value is
    /// saving a *future* read a remote fetch, and once the API is closed there are no future reads —
    /// so starting a whole-object download here would spend the drain's budget on nothing. Cancelling
    /// their tokens is how a queued transition is abandoned everywhere else in this module. What is
    /// already *running* is awaited, since that work is mid-transition rather than merely queued.
    async fn run(mut self) {
        loop {
            let received = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => break,
                received = self.rx.recv() => received,
                Some(_) = self.running.join_next(), if !self.running.is_empty() => continue,
            };
            let Some((transition, token)) = received else {
                break; // every handle dropped
            };
            let registered = transition.registry_key();
            // Cancelled while queued: the client that cancelled is already past us.
            if token.is_cancelled() {
                self.live.remove(&registered);
                continue;
            }
            // Cancellation has to reach here too, not just the receive: the permit this waits on frees
            // only when a running transition finishes, which is long enough for the shutdown to have
            // been signalled and this one to no longer be worth starting.
            let permit = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    token.cancel();
                    self.live.remove(&registered);
                    break;
                }
                permit = self.sem.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => break, // semaphore closed — shutting down
                },
            };
            let tier = self.tier.clone();
            let live = self.live.clone();
            self.running.spawn(async move {
                if let Err(e) = run_transition(&tier, &transition, &token).await {
                    tracing::debug!(transition = %registered, error = %e,
                        "background transition failed; the key's tombstone stands");
                }
                // Safe to remove unconditionally: an entry is only ever inserted into a *vacant*
                // slot and stays occupied for the transition's whole life, so no newer token can be
                // sitting under this key.
                live.remove(&registered);
                drop(permit);
            });
        }
        self.abandon_queued();
        while let Some(finished) = self.running.join_next().await {
            if let Err(e) = finished {
                tracing::warn!(error = %e, "background transition did not finish");
            }
        }
    }

    /// Tell every transition still queued to stop, so the one that is mid-`select!` on its token
    /// returns instead of starting a fetch nothing will read.
    fn abandon_queued(&mut self) {
        self.rx.close();
        while let Ok((transition, token)) = self.rx.try_recv() {
            token.cancel();
            self.live.remove(&transition.registry_key());
        }
    }
}

async fn run_transition(
    tier: &Tiering,
    transition: &Transition,
    token: &CancellationToken,
) -> Result<()> {
    match transition {
        Transition::Rehydrate {
            bucket,
            key,
            cetag,
            plen,
        } => rehydrate(tier, bucket, key, cetag, *plen, token).await,
    }
}

/// Fetch + decrypt K from the remote and land it in the cache , under K's write lock.
/// Re-confirms the eviction tombstone under the lock — a write or delete may have superseded it, in
/// which case there is nothing to rehydrate.
///
/// A composite rehydrate leaves K tombstoned (the plaintext goes to the shadow), so the tombstone
/// re-check alone doesn't stop repeated work: without the shadow-freshness check, a transition
/// enqueued after an earlier one landed would re-download the whole object.
///
/// **Generation gate.** A transition can sit queued while K is written, reconciled, and evicted
/// afresh, and the land CAS — conditional on a sentinel ETag that is constant across generations,
/// so blind to the evict → rehydrate → re-evict ABA — would then accept the *new* tombstone for the
/// *old*
/// plaintext. So the tombstone's `cetag` is re-read under the lock and must still be the generation
/// the read observed. It also makes the client pass-through safe to take from the live tombstone
/// rather than from the read that raised the transition.
async fn rehydrate(
    tier: &Tiering,
    bucket: &str,
    key: &str,
    cetag: &str,
    plen: u64,
    token: &CancellationToken,
) -> Result<()> {
    let _guard = tier.write_locks.lock(bucket, key).await;

    // Cancelled while we were parked on the lock — or while an earlier holder ran. Checked before
    // any backend call so a cancelled transition costs nothing but the lock acquisition.
    if token.is_cancelled() {
        return Ok(());
    }

    let head = match tier.data.head(bucket, key).await {
        Ok(h) => h,
        Err(Error::NotFound) => return Ok(()),
        Err(e) => return Err(e),
    };
    // No metadata ⇒ no tombstone; the classifier says the same of an empty map.
    let Some(tomb) = head.metadata.as_ref() else {
        return Ok(());
    };
    if meta::tomb_kind(tomb) != Some(meta::TombKind::Evict) {
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
        let md = meta::passthrough_metadata(tomb);
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

    /// A handle whose actor never runs, so queued transitions stay queued and the registry is
    /// observable.
    fn handle(depth: usize) -> (Background, mpsc::Receiver<(Transition, CancellationToken)>) {
        let (tx, rx) = mpsc::channel(depth);
        (
            Background {
                tx,
                live: Arc::new(DashMap::new()),
            },
            rx,
        )
    }

    fn transition(key: &str) -> Transition {
        Transition::Rehydrate {
            bucket: "b".into(),
            key: key.into(),
            cetag: "e".into(),
            plen: 0,
        }
    }

    #[tokio::test]
    async fn submit_dedups_by_key() {
        let (bg, mut rx) = handle(8);
        bg.submit(transition("k"));
        bg.submit(transition("k"));
        bg.submit(transition("other"));
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok(), "distinct key must enqueue");
        assert!(
            rx.try_recv().is_err(),
            "a second transition for a live key must be dropped, not queued"
        );
    }

    #[tokio::test]
    async fn full_queue_sheds_and_leaves_no_registry_entry() {
        let (bg, _rx) = handle(1);
        bg.submit(transition("first"));
        bg.submit(transition("shed"));
        assert!(bg.is_live("b", "first"), "the queued transition stays live");
        assert!(
            !bg.is_live("b", "shed"),
            "a shed transition must not leave a registry entry blocking later submits"
        );
    }

    #[tokio::test]
    async fn cancel_trips_the_queued_jobs_token() {
        let (bg, mut rx) = handle(4);
        bg.submit(transition("k"));
        bg.cancel("b", "k");
        let (_t, token) = rx.try_recv().expect("transition was queued");
        assert!(
            token.is_cancelled(),
            "cancel must reach a still-queued transition"
        );
    }

    #[tokio::test]
    async fn cancel_of_an_unknown_key_is_a_noop() {
        let (bg, _rx) = handle(4);
        bg.cancel("b", "never-submitted");
    }
}
