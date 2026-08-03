//! Pull-based encryption: one plaintext body becomes one age file (optionally framed with its
//! facts trailer), with the plaintext MD5 folded in on the same pass.
//!
//! The consumer's polls drive everything — source read, chunk encryption, digest, trailer — so
//! nothing runs ahead of the sink that is taking the bytes, and dropping the response drops the
//! source read with it.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use age::stream::StreamWriter;
use aws_sdk_s3::primitives::ByteStream;
use bytes::{Buf as _, Bytes, BytesMut};
use futures::io::AsyncWrite;
use futures::Stream;
use hypha_format::offset::{ciphertext_len, CHUNK_CIPHERTEXT, HLEN};
use hypha_format::{
    encode_trailer, single_trailer_len, ChecksumKind, Envelope, Footer, FooterKind, StoredChecksum,
};
use md5::Digest as _;
use s3s::dto::StreamingBlob;
use tokio::sync::oneshot;

use crate::s3::checksum::{Hasher as ChecksumHasher, RequestedChecksum};

use super::{
    blob_to_bytestream, bytestream_to_blob, DigestMismatch, EtagReceiver, ObjectDigests,
    SingleTrailer,
};

/// One ciphertext byte the gate will not hand over on its own. What keeps a mismatched body short
/// of its declared length — and so refused by the backend rather than committed  — is that
/// age's final chunk is written by the close and released only with the verdict; but that rests on
/// when age chooses to flush, so a release forced by a full gate withholds a byte regardless.
const HOLDBACK: usize = 1;

/// Ciphertext age has produced and the consumer has not yet taken.
type Gate = Arc<Mutex<BytesMut>>;

/// age's sink. Backpressure is a `Pending` **without a registered waker**: the sole poller is the
/// [`EncryptStream`] that owns the other end of the gate, and it drains and re-polls rather than
/// waiting, so there is no wakeup to lose. Nothing else may write here.
struct GateSink(Gate);

impl AsyncWrite for GateSink {
    /// Takes a write whenever what it would add to the *releasable* bytes still fits one age
    /// chunk. Measuring against the releasable bytes rather than the buffer keeps the withheld
    /// byte — which nothing can drain until the digest is in — from counting against the budget,
    /// and admitting any write into a drained gate keeps the several small writes of the age
    /// header from stalling before anything is there to hand over.
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut gate = self.0.lock().unwrap();
        let releasable = gate.len().saturating_sub(HOLDBACK);
        if releasable > 0 && releasable + data.len() > CHUNK_CIPHERTEXT as usize {
            return Poll::Pending;
        }
        gate.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// age closing its sink is not the end of the body — the trailer still follows — so this only
    /// has to not lose what age just wrote.
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

enum Stage {
    /// Feeding plaintext to age.
    Encrypting,
    /// Plaintext is exhausted; age is emitting its final chunk.
    Closing,
    /// age is done: check the digest and hand over the tail it was gating.
    Finishing,
    /// Digest checked, tail emitted.
    Done,
}

#[derive(Default)]
pub struct EncryptOptions {
    pub trailer: Option<SingleTrailer>,
    pub expected_md5: Option<[u8; 16]>,
    pub checksum: Option<RequestedChecksum>,
}

/// Stream-encrypt a plaintext body into hypha's framed single-part form — age ciphertext followed
/// by its [`SingleTrailer`] — with a Content-Length known up front (the age header is a fixed
/// [`HLEN`], so `ciphertext_len` is exact) and no spill. Returns `(framed_len, body)`. The trailer
/// carries the plaintext MD5, computed inline as the body streams  — the reconcile path knows
/// `plen`/mtime from the same cache GET that streams the body, so the framed facts can't disagree.
pub async fn encrypt_stream(
    env: Arc<Envelope>,
    plaintext: ByteStream,
    plen: u64,
    trailer: SingleTrailer,
) -> io::Result<(u64, ByteStream)> {
    let (framed_len, body, _etag) = encrypt_blob_with_etag(
        env,
        bytestream_to_blob(plaintext),
        plen,
        EncryptOptions {
            trailer: Some(trailer),
            ..Default::default()
        },
    )
    .await?;
    Ok((framed_len, body))
}

/// Encrypt a plaintext `StreamingBlob` to age ciphertext, computing the plaintext MD5 alongside the
/// encryption in one pass. `trailer: Some(_)` appends a kind-*single* trailer (built from the
/// computed digest once the last plaintext byte has streamed) behind the ciphertext, so a
/// single-part PUT lands body and facts atomically ; `None` emits a pure age file — a multipart
/// part, whose facts live in the object's terminating trailer part.
///
/// `expect_md5` is the client's `Content-MD5`, checked against the digest the body actually
/// produced — which is only knowable at EOF, by which point most of the ciphertext has gone out.
/// What makes that safe is that age's final chunk never leaves the gate before the verdict: on a
/// mismatch the body ends short of its declared length, so the backend op fails rather than
/// committing a corrupt object (see [`DigestMismatch`]).
///
/// Returns `(body_len, body, etag_receiver)`. `body_len` is exact and synchronous (`HLEN` is
/// constant). Await `etag_receiver` **after** fully consuming `body` (i.e. after the remote op
/// returns): it resolves with the hex MD5 at stream end.
pub async fn encrypt_blob_with_etag(
    env: Arc<Envelope>,
    plaintext: StreamingBlob,
    plen: u64,
    options: EncryptOptions,
) -> io::Result<(u64, ByteStream, EtagReceiver)> {
    let (body_len, stream, etag_rx) = open(env, plaintext, plen, options).await?;
    Ok((
        body_len,
        blob_to_bytestream(StreamingBlob::wrap(stream)),
        etag_rx,
    ))
}

async fn open(
    env: Arc<Envelope>,
    plaintext: StreamingBlob,
    plen: u64,
    options: EncryptOptions,
) -> io::Result<(u64, EncryptStream, EtagReceiver)> {
    let EncryptOptions {
        trailer,
        expected_md5,
        checksum,
    } = options;
    let (etag_tx, etag_rx) = oneshot::channel();
    let body_ct_len = ciphertext_len(plen, HLEN);
    let checksum_algorithm = checksum.as_ref().map(|value| value.algorithm).or_else(|| {
        trailer
            .as_ref()
            .and_then(|trailer| trailer.checksum.as_ref())
            .map(|value| value.algorithm)
    });
    let body_len = body_ct_len
        + if trailer.is_some() {
            single_trailer_len(checksum_algorithm) as u64
        } else {
            0
        };

    // Writes the age header into the gate, so a header that can't be produced fails the request
    // rather than the body.
    let gate: Gate = Arc::new(Mutex::new(BytesMut::new()));
    let writer = env
        .encrypt_async(GateSink(gate.clone()))
        .await
        .map_err(io::Error::other)?;

    let stream = EncryptStream {
        plaintext: Some(plaintext),
        writer,
        gate,
        hasher: md5::Md5::new(),
        checksum_hasher: checksum
            .as_ref()
            .map(|request| ChecksumHasher::new(request.algorithm)),
        checksum,
        inflight: Bytes::new(),
        expect_md5: expected_md5,
        etag_tx: Some(etag_tx),
        trailer,
        plen,
        body_ct_len,
        stage: Stage::Encrypting,
    };
    Ok((body_len, stream, etag_rx))
}

struct EncryptStream {
    /// Taken at plaintext EOF, so the source is dropped as soon as it is spent.
    plaintext: Option<StreamingBlob>,
    writer: StreamWriter<GateSink>,
    gate: Gate,
    hasher: md5::Md5,
    checksum_hasher: Option<ChecksumHasher>,
    checksum: Option<RequestedChecksum>,
    /// Plaintext taken from the source but not yet accepted by age.
    inflight: Bytes,
    expect_md5: Option<[u8; 16]>,
    etag_tx: Option<oneshot::Sender<Result<ObjectDigests, DigestMismatch>>>,
    trailer: Option<SingleTrailer>,
    plen: u64,
    body_ct_len: u64,
    stage: Stage,
}

impl EncryptStream {
    /// Everything in the gate bar the withheld byte.
    fn take_released(&mut self) -> Option<Bytes> {
        let mut gate = self.gate.lock().unwrap();
        (gate.len() > HOLDBACK).then(|| {
            let n = gate.len() - HOLDBACK;
            gate.split_to(n).freeze()
        })
    }

    /// age stalls only on the gate, and the gate stalls only when it is full, so a stalled write
    /// always has something to hand over.
    fn yield_released(&mut self) -> Poll<Option<io::Result<Bytes>>> {
        match self.take_released() {
            Some(out) => Poll::Ready(Some(Ok(out))),
            None => self.fail(io::Error::other("age stalled on an empty encryption gate")),
        }
    }

    /// Waiting on the source, hand over a *full* gate so a finished chunk isn't held hostage to
    /// the next one — but nothing less. Every frame is its own write on the wire, and a body that
    /// arrives in several costs the backend a round trip apiece; a partial gate is either the age
    /// header or a chunk the close is about to complete, and both belong with what follows.
    fn yield_if_full(&mut self) -> Poll<Option<io::Result<Bytes>>> {
        let full =
            self.gate.lock().unwrap().len().saturating_sub(HOLDBACK) >= CHUNK_CIPHERTEXT as usize;
        match full.then(|| self.take_released()).flatten() {
            Some(out) => Poll::Ready(Some(Ok(out))),
            None => Poll::Pending,
        }
    }

    /// A failed body is over: no digest verdict is owed, and neither age nor the source is in a
    /// state to be polled again.
    fn fail(&mut self, e: io::Error) -> Poll<Option<io::Result<Bytes>>> {
        self.stage = Stage::Done;
        Poll::Ready(Some(Err(e)))
    }

    /// The digest verdict and the bytes it was gating: age's final chunk, which never leaves the
    /// gate before the verdict, and the trailer that makes the object's facts land in the same PUT
    /// as its body. One frame, so an object that fits in a chunk crosses the wire as a single
    /// write.
    fn finish(&mut self) -> io::Result<Option<Bytes>> {
        let md5: [u8; 16] = std::mem::take(&mut self.hasher).finalize().into();
        let etag_tx = self.etag_tx.take();
        if self.expect_md5.is_some_and(|want| want != md5) {
            if let Some(tx) = etag_tx {
                let _ = tx.send(Err(DigestMismatch::Md5));
            }
            return Ok(None);
        }
        let mut checksum: Option<StoredChecksum> = self
            .checksum_hasher
            .take()
            .map(|hasher| hasher.finalize(ChecksumKind::FullObject));
        if let (Some(request), Some(actual)) = (&self.checksum, &checksum) {
            if request.verify(actual).is_err() {
                if let Some(tx) = etag_tx {
                    let _ = tx.send(Err(DigestMismatch::Checksum));
                }
                return Ok(None);
            }
        }

        let mut tail = std::mem::take(&mut *self.gate.lock().unwrap());
        if tail.is_empty() {
            return Err(io::Error::other("age emitted no final chunk"));
        }
        if let Some(t) = self.trailer.take() {
            checksum = checksum.or(t.checksum);
            let footer = Footer {
                kind: FooterKind::Single,
                count: 1,
                plen: self.plen,
                mtime_ms: t.mtime_ms,
                md5,
                checksum: checksum.clone(),
            };
            tail.extend_from_slice(&encode_trailer(
                &t.trailer_key,
                &t.object_key,
                self.body_ct_len,
                &footer,
                &[],
            ));
        }
        // Before the last bytes go out, not after: a consumer that stops polling once it has its
        // declared Content-Length would otherwise never let this be sent.
        if let Some(tx) = etag_tx {
            let _ = tx.send(Ok(ObjectDigests {
                etag: hex::encode(md5),
                checksum,
            }));
        }
        Ok(Some(tail.freeze()))
    }
}

impl Stream for EncryptStream {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        loop {
            match me.stage {
                Stage::Encrypting => {
                    if me.inflight.is_empty() {
                        let Some(src) = me.plaintext.as_mut() else {
                            me.stage = Stage::Closing;
                            continue;
                        };
                        match Pin::new(src).poll_next(cx) {
                            Poll::Pending => return me.yield_if_full(),
                            Poll::Ready(Some(Ok(chunk))) => me.inflight = chunk,
                            Poll::Ready(Some(Err(e))) => return me.fail(io::Error::other(e)),
                            Poll::Ready(None) => {
                                me.plaintext = None;
                                me.stage = Stage::Closing;
                                continue;
                            }
                        }
                    }
                    let mut chunk = std::mem::take(&mut me.inflight);
                    match Pin::new(&mut me.writer).poll_write(cx, &chunk) {
                        Poll::Ready(Ok(n)) => {
                            me.hasher.update(&chunk[..n]);
                            if let Some(hasher) = &mut me.checksum_hasher {
                                hasher.update(&chunk[..n]);
                            }
                            chunk.advance(n);
                            me.inflight = chunk;
                        }
                        Poll::Ready(Err(e)) => return me.fail(e),
                        // The gate is full — hand it over and come back for the rest.
                        Poll::Pending => {
                            me.inflight = chunk;
                            return me.yield_released();
                        }
                    }
                }
                Stage::Closing => match Pin::new(&mut me.writer).poll_close(cx) {
                    Poll::Ready(Ok(())) => me.stage = Stage::Finishing,
                    Poll::Ready(Err(e)) => return me.fail(e),
                    Poll::Pending => return me.yield_released(),
                },
                Stage::Finishing => {
                    me.stage = Stage::Done;
                    return match me.finish() {
                        Ok(Some(tail)) => Poll::Ready(Some(Ok(tail))),
                        // The digest disagreed: end short of the declared length rather than
                        // letting the backend commit the object.
                        Ok(None) => Poll::Ready(None),
                        Err(e) => Poll::Ready(Some(Err(e))),
                    };
                }
                Stage::Done => return Poll::Ready(None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::StreamExt as _;
    use hypha_format::offset::CHUNK_PLAINTEXT;
    use hypha_format::{decode_tail, TrailerKey};

    use super::super::testutil::*;
    use super::*;

    const PASSPHRASE: &str = "codec unit test passphrase";

    fn single(object_key: &str) -> SingleTrailer {
        SingleTrailer {
            trailer_key: TrailerKey::derive(PASSPHRASE),
            object_key: object_key.to_string(),
            mtime_ms: 1_700_000_000_000,
            checksum: None,
        }
    }

    async fn frames_of(stream: EncryptStream) -> Vec<Bytes> {
        let mut stream = Box::pin(stream);
        let mut out = Vec::new();
        while let Some(frame) = stream.next().await {
            out.push(frame.expect("encryption failed mid-body"));
        }
        out
    }

    fn body(frames: &[Bytes]) -> Vec<u8> {
        frames.iter().flatten().copied().collect()
    }

    fn decrypt(env: &Envelope, ct: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        io::Read::read_to_end(&mut env.decrypt(ct).unwrap(), &mut out).unwrap();
        out
    }

    /// The gate's admission rule on its own, because the two halves of it are load-bearing in
    /// opposite directions: refusing too eagerly deadlocks a body nobody can drain (the age header
    /// arrives as several small writes), and refusing too late is the buffering this design exists
    /// to remove.
    #[test]
    fn the_gate_takes_a_drained_write_and_makes_a_second_chunk_wait() {
        let gate: Gate = Arc::new(Mutex::new(BytesMut::new()));
        let mut sink = GateSink(gate.clone());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        let chunk = vec![0u8; CHUNK_CIPHERTEXT as usize];

        assert!(matches!(
            Pin::new(&mut sink).poll_write(&mut cx, &[0u8; 149]),
            Poll::Ready(Ok(149))
        ));
        assert!(matches!(
            Pin::new(&mut sink).poll_write(&mut cx, &[0u8; 16]),
            Poll::Ready(Ok(16))
        ));

        gate.lock()
            .unwrap()
            .resize(CHUNK_CIPHERTEXT as usize + HOLDBACK, 0);
        assert!(Pin::new(&mut sink)
            .poll_write(&mut cx, &[0u8; 32])
            .is_pending());

        // Drained to the withheld byte — which must not count against the budget.
        gate.lock().unwrap().resize(HOLDBACK, 0);
        assert!(matches!(
            Pin::new(&mut sink).poll_write(&mut cx, &chunk),
            Poll::Ready(Ok(_))
        ));
        assert_eq!(
            gate.lock().unwrap().len(),
            CHUNK_CIPHERTEXT as usize + HOLDBACK
        );
    }

    /// The lengths where age's chunking changes shape, against a declared Content-Length that is
    /// computed rather than observed: if the two ever disagree the commit is unrecoverable, since
    /// the object is already on the remote by the time anyone could notice.
    #[tokio::test]
    async fn a_framed_body_is_exactly_as_long_as_it_declared() {
        let env = envelope();
        for plen in [0usize, 1, 65_535, 65_536, 65_537, 200_000] {
            let plaintext: Vec<u8> = (0..plen).map(|i| (i % 251) as u8).collect();
            let (body_len, stream, etag) = open(
                env.clone(),
                frames(&plaintext, 7_000, true),
                plen as u64,
                EncryptOptions {
                    trailer: Some(single("k")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            let framed = body(&frames_of(stream).await);
            assert_eq!(framed.len() as u64, body_len, "plen {plen}");

            let tail = decode_tail(&TrailerKey::derive(PASSPHRASE), "k", body_len, &framed)
                .expect("the framed trailer must authenticate");
            let md5: [u8; 16] = md5::Md5::digest(&plaintext).into();
            assert_eq!(tail.footer.plen, plen as u64);
            assert_eq!(tail.footer.md5, md5);
            assert_eq!(etag.await.unwrap().unwrap().etag, hex::encode(md5));
            assert_eq!(
                decrypt(&env, &framed[..tail.body_ct_len as usize]),
                plaintext
            );
        }
    }

    /// The memory claim of the whole design: whatever the body's size or frame pattern, the gate
    /// holds one age ciphertext chunk. Every frame is what the gate was holding, so the largest
    /// frame *is* the high-water mark.
    #[tokio::test]
    async fn the_gate_never_holds_more_than_one_ciphertext_chunk() {
        let plaintext = vec![7u8; 5 * CHUNK_PLAINTEXT as usize + 3];
        // One oversized frame: the source is never the thing pacing the gate here, age is.
        let (_, stream, _etag) = open(
            envelope(),
            frames(&plaintext, plaintext.len(), false),
            plaintext.len() as u64,
            EncryptOptions::default(),
        )
        .await
        .unwrap();

        let peak = frames_of(stream)
            .await
            .iter()
            .map(|f| f.len())
            .max()
            .unwrap();
        assert!(
            peak <= CHUNK_CIPHERTEXT as usize + HOLDBACK,
            "gate reached {peak} bytes"
        );
    }

    /// The digest is only knowable at EOF, by which point most of the ciphertext has already gone
    /// out. What makes that safe is that age's *final* chunk is produced by the close and never
    /// leaves the gate before the verdict: the body therefore ends short of its declared length,
    /// and the backend refuses it rather than committing corrupt ciphertext.
    #[tokio::test]
    async fn a_digest_mismatch_ends_the_body_short_of_its_declared_length() {
        let env = envelope();
        let plaintext = vec![5u8; 100_000];
        for trailer in [None, Some(single("k"))] {
            let (body_len, stream, etag) = open(
                env.clone(),
                frames(&plaintext, 9_000, true),
                plaintext.len() as u64,
                EncryptOptions {
                    trailer,
                    expected_md5: Some([0u8; 16]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            let emitted = body(&frames_of(stream).await).len() as u64;
            assert!(emitted < body_len, "{emitted} of {body_len} bytes emitted");
            // Not merely short: short of a decryptable age file.
            assert!(emitted < ciphertext_len(plaintext.len() as u64, HLEN));
            assert!(matches!(etag.await, Ok(Err(DigestMismatch::Md5))));
        }
    }

    /// Small objects are the common case and the one a frame-per-poll codec ruins: every frame is
    /// a separate write on the wire, and a body split across several pays a round trip apiece
    /// against the backend.
    #[tokio::test]
    async fn an_object_that_fits_one_age_chunk_is_one_frame() {
        let plaintext = vec![4u8; 40_000];
        let (body_len, stream, _etag) = open(
            envelope(),
            frames(&plaintext, 4_096, true),
            plaintext.len() as u64,
            EncryptOptions {
                trailer: Some(single("k")),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let frames = frames_of(stream).await;
        assert_eq!(frames.len(), 1, "{} frames", frames.len());
        assert_eq!(frames[0].len() as u64, body_len);
    }

    #[tokio::test]
    async fn a_matching_digest_is_not_reported_as_a_mismatch() {
        let plaintext = vec![9u8; 70_000];
        let (body_len, stream, etag) = open(
            envelope(),
            frames(&plaintext, 4_096, true),
            plaintext.len() as u64,
            EncryptOptions {
                expected_md5: Some(md5::Md5::digest(&plaintext).into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(body(&frames_of(stream).await).len() as u64, body_len);
        assert!(etag.await.unwrap().is_ok());
    }

    /// A client that hangs up mid-PUT must take the source read with it — the reason for pulling
    /// rather than pumping into a pipe.
    #[tokio::test]
    async fn dropping_the_body_drops_the_source_and_the_etag() {
        let dropped = Arc::new(AtomicBool::new(false));
        let source = Cancelled {
            inner: frames(&vec![1u8; 500_000], 8_192, false),
            dropped: dropped.clone(),
        };
        let (_, stream, etag) = open(
            envelope(),
            StreamingBlob::wrap(source),
            500_000,
            EncryptOptions::default(),
        )
        .await
        .unwrap();

        let mut stream = Box::pin(stream);
        stream.next().await.unwrap().unwrap();
        drop(stream);

        assert!(dropped.load(Ordering::SeqCst));
        assert!(etag.await.is_err(), "an unfinished body owes no ETag");
    }

    #[tokio::test]
    async fn a_source_failure_fails_the_body_and_withholds_the_etag() {
        let (_, stream, etag) = open(
            envelope(),
            StreamingBlob::wrap(futures::stream::once(async {
                Err(io::Error::other("the cache hung up"))
            })),
            10,
            EncryptOptions::default(),
        )
        .await
        .unwrap();

        let mut stream = Box::pin(stream);
        let mut failed = false;
        while let Some(frame) = stream.next().await {
            failed = frame.is_err();
            if failed {
                break;
            }
        }
        assert!(failed, "a source that fails must fail the body");
        drop(stream);
        assert!(etag.await.is_err());
    }

    struct Cancelled {
        inner: StreamingBlob,
        dropped: Arc<AtomicBool>,
    }

    impl Stream for Cancelled {
        type Item = io::Result<Bytes>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.get_mut().inner)
                .poll_next(cx)
                .map(|frame| frame.map(|r| r.map_err(io::Error::other)))
        }
    }

    impl Drop for Cancelled {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
}
