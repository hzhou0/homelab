//! One body, two sinks, one pass.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures::Stream;
use s3s::dto::StreamingBlob;

use super::{blob_to_bytestream, bytestream_to_blob};

/// Split one body into two identical streams, so it can reach two sinks in a single pass — the
/// upload path for a retained part , which must land on the remote *and* in the cache without
/// the encrypt stream running twice. Each source chunk is handed to both branches as a `Bytes`
/// clone, so the split costs a refcount rather than a copy, and per-request memory is one chunk
/// however large the part is.
///
/// Neither branch head-of-line blocks the other, but the source is only advanced once both have
/// taken the current chunk: the slower sink paces the read, which is the intended backpressure.
///
/// A branch that is **dropped stops holding the source back** — it is counted as having taken
/// everything from then on, and the survivor reads the body out in full. A dropped branch is not
/// an error and cannot be told from one: a sink whose request completed on its declared
/// Content-Length is dropped without the body ever being driven to its terminal `None`, which is
/// the ordinary ending for the smaller of the two writes. What keeps a part from landing on one
/// side alone is the caller's `try_join!` over the two requests  — a sink that goes away
/// *without* its bytes fails its own request, and that fails the operation.
pub fn tee(src: ByteStream) -> (ByteStream, ByteStream) {
    let shared = Arc::new(Mutex::new(Tee {
        src: bytestream_to_blob(src),
        chunk: None,
        taken: [false; 2],
        ended: false,
        failed: None,
        wakers: [None, None],
        pending_wake: None,
        live: [true; 2],
    }));
    (branch(&shared, 0), branch(&shared, 1))
}

fn branch(shared: &Arc<Mutex<Tee>>, side: usize) -> ByteStream {
    blob_to_bytestream(StreamingBlob::wrap(Branch {
        shared: shared.clone(),
        side,
    }))
}

struct Tee {
    src: StreamingBlob,
    /// The chunk both branches must take before the source is polled again.
    chunk: Option<Bytes>,
    taken: [bool; 2],
    ended: bool,
    failed: Option<String>,
    wakers: [Option<Waker>; 2],
    /// Woken by the branch that unparked it, after it has let go of the lock.
    pending_wake: Option<Waker>,
    /// Cleared by `Branch::drop`. Only ever *releases* the source; see [`tee`].
    live: [bool; 2],
}

impl Tee {
    fn step(&mut self, side: usize, cx: &mut Context<'_>) -> Poll<Option<io::Result<Bytes>>> {
        let peer = side ^ 1;
        loop {
            if let Some(e) = &self.failed {
                return Poll::Ready(Some(Err(io::Error::other(e.clone()))));
            }
            // A branch nobody holds any more owes the chunk nothing, so it can never be what the
            // survivor waits on.
            if !self.live[peer] {
                self.taken[peer] = true;
            }
            if self.taken == [true; 2] {
                self.chunk = None;
                self.taken = [false; 2];
            }
            if let Some(chunk) = self.chunk.clone() {
                if self.taken[side] {
                    // Wait for the peer to take it; it wakes us when it does.
                    self.wakers[side] = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                self.taken[side] = true;
                self.wake(peer);
                return Poll::Ready(Some(Ok(chunk)));
            }
            if self.ended {
                return Poll::Ready(None);
            }
            match Pin::new(&mut self.src).poll_next(cx) {
                // The source wakes whichever branch polled it last, so both branches park their
                // own waker here and the winner passes the wakeup on.
                Poll::Pending => {
                    self.wakers[side] = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    if !chunk.is_empty() {
                        self.chunk = Some(chunk);
                    }
                    self.wake(peer);
                }
                Poll::Ready(Some(Err(e))) => {
                    self.ended = true;
                    self.failed = Some(format!("tee: reading the source failed mid-stream: {e}"));
                    self.wake(peer);
                }
                Poll::Ready(None) => {
                    self.ended = true;
                    self.wake(peer);
                    return Poll::Ready(None);
                }
            }
        }
    }

    fn wake(&mut self, side: usize) {
        self.pending_wake = self.wakers[side].take().or(self.pending_wake.take());
    }
}

struct Branch {
    shared: Arc<Mutex<Tee>>,
    side: usize,
}

impl Branch {
    /// Both branches take the same lock, so the peer is woken outside it.
    ///
    /// Poisoning is ignored rather than propagated: a source that panics mid-`poll_next` poisons
    /// the lock on the way out, and `Drop` runs during that unwind — a second panic there would
    /// abort the process over one failed request.
    fn locked<T>(&self, f: impl FnOnce(&mut Tee) -> T) -> T {
        let mut tee = self
            .shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let out = f(&mut tee);
        let wake = tee.pending_wake.take();
        drop(tee);
        if let Some(w) = wake {
            w.wake();
        }
        out
    }
}

impl Stream for Branch {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.locked(|tee| tee.step(self.side, cx))
    }
}

impl Drop for Branch {
    fn drop(&mut self) {
        self.locked(|tee| {
            tee.live[self.side] = false;
            tee.wake(self.side ^ 1);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt as _;

    use super::super::testutil::*;
    use super::*;

    fn source(len: usize) -> (Vec<u8>, ByteStream) {
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let body = framed_bytestream(&data, 4_096);
        (data, body)
    }

    /// The branches are handed to two different requests, so they are polled by two different
    /// tasks: a chunk taken by one has to wake the other, and a source that parks has to wake
    /// whichever branch is waiting on it.
    async fn both_branches_agree(slow_side: usize) {
        let (data, src) = source(300_000);
        let (a, b) = tee(src);
        let (fast, slow) = if slow_side == 0 { (b, a) } else { (a, b) };

        let fast = tokio::spawn(async move { collect(bytestream_to_blob(fast)).await });
        let mut slow_blob = bytestream_to_blob(slow);
        let mut got = Vec::new();
        while let Some(frame) = slow_blob.next().await {
            tokio::time::sleep(Duration::from_micros(200)).await;
            got.extend_from_slice(&frame.unwrap());
        }

        assert_eq!(got, data);
        assert_eq!(fast.await.unwrap(), data);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn both_branches_see_the_whole_body_when_the_first_lags() {
        both_branches_agree(0).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn both_branches_see_the_whole_body_when_the_second_lags() {
        both_branches_agree(1).await;
    }

    /// A sink is dropped the moment its request finishes, and a request finishes on its declared
    /// Content-Length — never on the body's terminal `None`. So the survivor must read on, not
    /// fail: whether the departed sink got what it was owed is its own request's verdict, and the
    /// caller's `try_join!` is what refuses a part that landed on one side alone.
    async fn a_lost_sink_leaves_the_body_readable(dropped_side: usize) {
        let (data, src) = source(300_000);
        let (a, b) = tee(src);
        let (dropped, survivor) = if dropped_side == 0 { (a, b) } else { (b, a) };
        drop(dropped);

        assert_eq!(collect(bytestream_to_blob(survivor)).await, data);
    }

    #[tokio::test]
    async fn dropping_the_first_sink_leaves_the_second_readable() {
        a_lost_sink_leaves_the_body_readable(0).await;
    }

    #[tokio::test]
    async fn dropping_the_second_sink_leaves_the_first_readable() {
        a_lost_sink_leaves_the_body_readable(1).await;
    }

    /// The shape the retained-part path actually produces : the part is one frame — a small
    /// final part, encrypted — so the faster sink takes it, satisfies its Content-Length, and is
    /// dropped before the slower sink has polled once. That drop must not cost the slower sink the
    /// body it has not read yet.
    #[tokio::test]
    async fn a_sink_that_finishes_first_does_not_strand_a_slower_one() {
        let data = vec![7u8; 8_000];
        let (a, b) = tee(framed_bytestream(&data, data.len()));

        let mut a = bytestream_to_blob(a);
        assert_eq!(a.next().await.unwrap().unwrap(), data);
        drop(a);

        assert_eq!(collect(bytestream_to_blob(b)).await, data);
    }

    /// The peer can go away while the survivor is *parked on it* — the ordering that deadlocks if a
    /// drop doesn't both release the chunk it was owed and wake whoever was waiting for it.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_sink_dropped_while_the_other_waits_on_it_unparks_it() {
        let (data, src) = source(300_000);
        let (a, b) = tee(src);

        let reader = tokio::spawn(async move { collect(bytestream_to_blob(a)).await });
        // Long enough for `a` to take a chunk and park on `b`, which never polls at all.
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(b);

        let got = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("a drop must unpark the sink waiting on it")
            .unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn a_source_failure_reaches_both_branches() {
        let src = blob_to_bytestream(StreamingBlob::wrap(futures::stream::once(async {
            Err(io::Error::other("the source hung up"))
        })));
        let (a, b) = tee(src);
        for branch in [a, b] {
            let mut branch = bytestream_to_blob(branch);
            assert!(branch.next().await.unwrap().is_err());
        }
    }
}
