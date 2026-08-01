//! The shared skeleton of the two sealed, weakly-held obligation queues (pending markers, orphaned
//! shadows).
//!
//! Both deliver a write path's obligations *after* its commit, so delivery must never fail the acked
//! write: the channel is unbounded and handler-local senders are weak. Each run ends one of two
//! ways — an explicit [`Msg::Seal`] on a graceful drain, or the channel closing when the serving
//! future drops its [`Seal`] — and only the first authorizes clean markers, because a crash closes
//! the channel too.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;

const DRAIN_BATCH: usize = 256;

/// What a queue carries: obligations, and the one message that says the run is ending *gracefully*.
/// Closure alone cannot say that — an aborted process drops the [`Seal`] and closes the channel
/// exactly as a drain would.
pub(crate) enum Msg<P> {
    Owed(P),
    Seal,
}

/// How an obligation identifies itself, so a burst enqueuing the same one twice settles once.
pub(crate) trait Dedup {
    fn dedup_key(&self) -> (String, String);
}

/// Holds the queue open for the life of the run.
///
/// Every other sender is short-lived and handler-local — they upgrade the weak handle, send, and
/// drop it before the handler returns — so once the connections drain this is the only one left,
/// and nothing can enqueue behind what it sends. FIFO therefore puts the [`Msg::Seal`] after every
/// obligation of the run.
///
/// **Dropping this is not sealing it.** The serving future owns it, so an aborted or panicking
/// server drops it and closes the channel exactly as a drain would; if closure alone authorized the
/// clean markers, a killed process would write them on its way out and the next run would skip its
/// recovery scan. Only the explicit message says the run ended gracefully.
pub(crate) struct Seal<P>(mpsc::UnboundedSender<Msg<P>>);

impl<P> Seal<P> {
    pub(crate) fn new(tx: mpsc::UnboundedSender<Msg<P>>) -> Self {
        Seal(tx)
    }

    pub(crate) fn seal(self) {
        let _ = self.0.send(Msg::Seal);
    }
}

/// How a run settles its obligations in each drain cycle.
pub(crate) trait Drain {
    type Payload: Dedup;
    async fn process(&self, owed: &mut HashMap<(String, String), Self::Payload>);
}

/// Run a sealed obligation queue to closure.
///
/// Batches receipts — one wake-up takes whatever a burst deposited — and the retry timer is the
/// backstop for obligations that failed, since it is what makes a run with nothing new still make
/// progress. Then one final attempt, never a retry loop: the drain does not wait out a backoff.
///
/// Returns `(sealed, remaining)`: whether a [`Msg::Seal`] was seen and how many obligations are
/// still owed. The caller's clean markers follow from exactly that pair — a drain that saw no seal,
/// or one that still owes, vouches for nothing.
pub(crate) async fn drain<P, Q>(
    rx: &mut mpsc::UnboundedReceiver<Msg<P>>,
    retry: Duration,
    owed: &mut HashMap<(String, String), P>,
    queue: &Q,
) -> (bool, usize)
where
    P: Dedup,
    Q: Drain<Payload = P>,
{
    let mut batch = Vec::with_capacity(DRAIN_BATCH);
    let mut sealed = false;
    loop {
        tokio::select! {
            n = rx.recv_many(&mut batch, DRAIN_BATCH) => {
                if n == 0 {
                    break; // dropped rather than sealed — the run did not end gracefully
                }
                for msg in batch.drain(..) {
                    match msg {
                        Msg::Owed(p) => {
                            owed.insert(p.dedup_key(), p);
                        }
                        Msg::Seal => sealed = true,
                    }
                }
                queue.process(owed).await;
                if sealed {
                    break;
                }
            }
            () = tokio::time::sleep(retry), if !owed.is_empty() => {
                queue.process(owed).await;
            }
        }
    }
    queue.process(owed).await;
    (sealed, owed.len())
}
