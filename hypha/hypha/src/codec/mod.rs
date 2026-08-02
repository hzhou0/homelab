//! Streaming codecs.
//!
//! Every codec here is **pull-based**: it is a `Stream`/`AsyncRead` the consumer drives, so a
//! request's pipeline memory is one age chunk in flight rather than a pipe's worth of buffer, and
//! a dropped response cancels the remote read instead of leaving a task filling a pipe nobody
//! reads. The one exception is the ranged read, where age's seekable reader is synchronous and
//! has to run on `spawn_blocking`; that path keeps a one-chunk simplex bridge.

mod encrypt;
mod tee;

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures::{Future, Stream, TryStreamExt as _};
use hypha_format::offset::CHUNK_CIPHERTEXT;
use hypha_format::{Envelope, RangeReader, RangeSource};
use s3s::dto::StreamingBlob;
use s3s_aws::conv::{try_from_aws, try_into_aws};
use tokio::io::{AsyncReadExt as _, ReadBuf};
use tokio::runtime::Handle;
use tokio_util::compat::{FuturesAsyncReadCompatExt as _, TokioAsyncReadCompatExt as _};
use tokio_util::io::{ReaderStream, SyncIoBridge};

use hypha_core::Backend;

pub use encrypt::{encrypt_blob_with_etag, encrypt_stream};
pub use tee::tee;

/// The client's `Content-MD5` and the digest its body actually produced disagreed.
#[derive(Debug)]
pub struct DigestMismatch;

/// Resolves with the hex plaintext MD5 once the encrypted body has been handed over in full.
pub type EtagReceiver = tokio::sync::oneshot::Receiver<Result<String, DigestMismatch>>;

/// The facts a single-part commit stamps into its tail trailer, alongside the body. The MD5 isn't
/// here: it's computed inline as the plaintext streams  and folded into the trailer at stream
/// end. `None` to [`encrypt_blob_with_etag`] emits a pure age file (a multipart part), whose facts
/// live in the object's one terminating trailer part instead.
pub struct SingleTrailer {
    pub trailer_key: hypha_format::TrailerKey,
    pub object_key: String,
    pub mtime_ms: i64,
}

pub fn blob_to_bytestream(blob: StreamingBlob) -> ByteStream {
    try_into_aws(blob).expect("StreamingBlob → ByteStream is Infallible")
}

pub fn bytestream_to_blob(bs: ByteStream) -> StreamingBlob {
    try_from_aws(bs).expect("ByteStream → StreamingBlob is Infallible")
}

/// A stream of `body` followed by `tail`, without buffering `body` — the complete-time trailer
/// fold , where the retained part may be gigabytes but the trailer is a few dozen KB.
pub fn append_bytes(body: ByteStream, tail: Vec<u8>) -> ByteStream {
    let chained = body.into_async_read().chain(io::Cursor::new(tail));
    blob_to_bytestream(StreamingBlob::wrap(ReaderStream::new(chained)))
}

/// A truncation or auth failure surfaces to the client as a body that simply ends: once the
/// response headers are out, a mid-stream error can no longer be turned into an HTTP status. Log
/// it where it happens, since that is the only place it is legible.
fn logged<S>(stage: &'static str, stream: S) -> StreamingBlob
where
    S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
{
    StreamingBlob::wrap(stream.inspect_err(move |e| {
        tracing::error!(error = %e, stage, "decrypt failed mid-stream");
    }))
}

/// Decrypt a whole remote object body to a plaintext `StreamingBlob`. One remote GET (the caller
/// already opened `body`); age reads header-then-chunks straight through, driven by the client's
/// own polls. `ct_len` is the age-envelope length — the object's Content-Length minus the tail
/// trailer — so the trailer never reaches the decryptor (it would read as a truncated chunk).
///
/// The header is consumed here rather than on first poll, so a foreign or corrupt one fails the
/// GET instead of truncating a body already committed to a 200.
pub async fn decrypt_full(
    env: Arc<Envelope>,
    body: ByteStream,
    ct_len: u64,
) -> hypha_core::error::Result<StreamingBlob> {
    let src = body.into_async_read().take(ct_len).compat();
    let plaintext = env.decrypt_async(src).await?.compat();
    Ok(logged("full", ReaderStream::new(plaintext)))
}

/// Decrypt plaintext byte range `pt` of a remote object, re-opening ranged ciphertext GETs
/// through [`RemoteRangeSource`] as age seeks . `ct_len` is the object's ciphertext
/// Content-Length (from a prior HEAD), needed for `SeekFrom::End` and range clamping.
///
/// The only codec that still crosses a pipe: age's `Seek` is synchronous, so the work belongs on
/// `spawn_blocking`, and the bridge back is one age chunk deep — enough to keep the decryptor and
/// the client from lock-stepping, small enough that the client's pace still governs.
pub fn decrypt_range(
    env: Arc<Envelope>,
    backend: Backend,
    bucket: String,
    key: String,
    ct_len: u64,
    pt: Range<u64>,
) -> StreamingBlob {
    let (reader, writer) = tokio::io::simplex(CHUNK_CIPHERTEXT as usize);
    let handle = Handle::current();
    let h = handle.clone();
    tokio::task::spawn_blocking(move || {
        let source = RemoteRangeSource {
            backend,
            bucket,
            key,
            base: 0,
            len: ct_len,
            handle: h.clone(),
        };
        let mut dst = SyncIoBridge::new_with_handle(writer, h);
        if let Err(e) = pump_decrypt_range(&env, source, pt.clone(), &mut dst) {
            tracing::error!(error = %e, "decrypt (range) failed mid-stream");
        }
        let _ = dst.shutdown();
    });
    StreamingBlob::wrap(ReaderStream::new(reader))
}

fn pump_decrypt_range(
    env: &Envelope,
    source: RemoteRangeSource,
    pt: Range<u64>,
    dst: &mut impl Write,
) -> hypha_core::error::Result<()> {
    // Decryptor::new reads the age header from ciphertext offset 0 (RangeReader opens there),
    // then the seek maps the plaintext offset to a fresh ranged GET of the covering chunks.
    let mut dec = env.decrypt(RangeReader::new(source))?;
    dec.seek(SeekFrom::Start(pt.start))?;
    let mut limited = dec.take(pt.end - pt.start);
    io::copy(&mut limited, dst)?;
    Ok(())
}

/// A [`RangeSource`] over a byte window `[base, base+len)` of a remote object, re-opened by
/// byte-range GETs. `base = 0, len = ct_len` reads a whole single-part object; a composite part
/// (its own age file inside the concatenation) is a non-zero window. Lives inside the
/// blocking decrypt task, so it drives the async SDK by blocking on the runtime handle (legal
/// off a `spawn_blocking` thread, which is not a runtime worker).
struct RemoteRangeSource {
    backend: Backend,
    bucket: String,
    key: String,
    base: u64,
    len: u64,
    handle: Handle,
}

/// Reads exactly zero bytes — an open at/past the window end, where a ranged GET would 416.
struct EmptyRead;
impl Read for EmptyRead {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
}

impl RangeSource for RemoteRangeSource {
    // The SDK's `into_async_read()` return type is unnameable, so box the bridged sync reader.
    type Reader = Box<dyn Read + Send>;

    fn len(&self) -> u64 {
        self.len
    }

    fn open_at(&mut self, offset: u64) -> io::Result<Self::Reader> {
        if offset >= self.len {
            return Ok(Box::new(EmptyRead));
        }
        // Bounded end, so reads never bleed into the next part of a composite.
        let range = format!("bytes={}-{}", self.base + offset, self.base + self.len - 1);
        let out = self
            .handle
            .block_on(self.backend.get(&self.bucket, &self.key, Some(range)))
            .map_err(io::Error::other)?;
        let reader = SyncIoBridge::new_with_handle(out.body.into_async_read(), self.handle.clone());
        Ok(Box::new(reader))
    }
}

// ── Composite bodies  ───────────────────────────────────────────────────────────────────

/// The one remote body a composite read walks. Shared because age's async reader takes ownership
/// of its source and never hands it back: each part's window borrows the body in turn.
type SharedBody = Arc<Mutex<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>>>;

/// One part's view of [`SharedBody`]: EOF after the part's recorded ciphertext length. That bound
/// is what stops age at the part's final chunk and leaves the body aligned on the next part.
struct PartWindow {
    body: SharedBody,
    remaining: u64,
}

impl futures::io::AsyncRead for PartWindow {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        let cap = buf.len().min(me.remaining as usize);
        if cap == 0 {
            return Poll::Ready(Ok(0));
        }
        let mut read = ReadBuf::new(&mut buf[..cap]);
        let mut body = me.body.lock().unwrap();
        ready!(Pin::new(&mut **body).poll_read(cx, &mut read))?;
        let n = read.filled().len();
        me.remaining -= n as u64;
        Poll::Ready(Ok(n))
    }
}

type PartReader = age::stream::StreamReader<PartWindow>;
type OpenPart =
    Pin<Box<dyn Future<Output = Result<PartReader, hypha_format::Error>> + Send + Sync>>;

enum Part {
    /// Nothing open: the next poll takes the next part off the table, or ends the object.
    Next,
    Opening(OpenPart),
    Reading(PartReader),
    /// A failure, latched. Neither a completed `Opening` future nor a failed age reader may be
    /// polled again, and nothing here is fused against a consumer that polls past an error.
    Failed,
}

/// The parts of a committed composite, decrypted in order off a single body into one plaintext
/// stream. Exactly one part is open at a time, so a 10 000-part object costs what a two-part one
/// does.
struct CompositeReader {
    env: Arc<Envelope>,
    body: SharedBody,
    ct_lens: std::vec::IntoIter<u64>,
    part: Part,
}

impl tokio::io::AsyncRead for CompositeReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        loop {
            match &mut me.part {
                Part::Next => {
                    let Some(len) = me.ct_lens.next() else {
                        return Poll::Ready(Ok(())); // every part emitted: end of the object
                    };
                    let window = PartWindow {
                        body: me.body.clone(),
                        remaining: len,
                    };
                    let env = me.env.clone();
                    me.part =
                        Part::Opening(Box::pin(async move { env.decrypt_async(window).await }));
                }
                Part::Opening(open) => match ready!(open.as_mut().poll(cx)) {
                    Ok(reader) => me.part = Part::Reading(reader),
                    Err(e) => {
                        me.part = Part::Failed;
                        return Poll::Ready(Err(io::Error::other(e)));
                    }
                },
                Part::Reading(reader) => {
                    // A zero-capacity read is answered as the no-op it is. That is indistinguishable
                    // from EOF in the return, so it is only safe because the caller
                    // (`ReaderStream`) always reserves space before reading.
                    let dst = buf.initialize_unfilled();
                    if dst.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                    let n = match ready!(futures::io::AsyncRead::poll_read(
                        Pin::new(reader),
                        cx,
                        dst
                    )) {
                        Ok(n) => n,
                        Err(e) => {
                            me.part = Part::Failed;
                            return Poll::Ready(Err(e));
                        }
                    };
                    if n == 0 {
                        me.part = Part::Next;
                        continue;
                    }
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Part::Failed => {
                    return Poll::Ready(Err(io::Error::other("composite read already failed")))
                }
            }
        }
    }
}

/// Decrypt a whole committed composite in **one GET** : the caller fetches `[0, body_ct_len)`
/// — the concatenated parts, trailer excluded — and hands it here with each part's ciphertext
/// length (from the trailer's parts table). O(1) round trips.
pub fn decrypt_composite_full(
    env: Arc<Envelope>,
    body: ByteStream,
    part_ct_lens: Vec<u64>,
) -> StreamingBlob {
    let reader = CompositeReader {
        env,
        body: Arc::new(Mutex::new(Box::new(body.into_async_read()))),
        ct_lens: part_ct_lens.into_iter(),
        part: Part::Next,
    };
    logged("composite full", ReaderStream::new(reader))
}

/// One part's contribution to a **ranged** composite read: the part's absolute ciphertext window
/// in the remote object, and which plaintext bytes of it to emit.
pub enum PartSegment {
    /// The whole part, start to finish — no plaintext length needed, the age stream ends itself.
    Whole(Range<u64>),
    /// Plaintext range `pt` (offsets *within this part*) of the part at ciphertext window `ct`.
    Partial { ct: Range<u64>, pt: Range<u64> },
}

/// Decrypt selected segments of a committed composite (the ranged read path): each segment's part
/// is decrypted as its own age file, via a per-part ranged GET, in order into one plaintext
/// stream. Whole-object reads take the single-GET [`decrypt_composite_full`] instead; a range
/// touches few parts, so per-part GETs here are bounded.
pub fn decrypt_composite(
    env: Arc<Envelope>,
    backend: Backend,
    bucket: String,
    key: String,
    segments: Vec<PartSegment>,
) -> StreamingBlob {
    let (reader, writer) = tokio::io::simplex(CHUNK_CIPHERTEXT as usize);
    let handle = Handle::current();
    let h = handle.clone();
    tokio::task::spawn_blocking(move || {
        let mut dst = SyncIoBridge::new_with_handle(writer, h.clone());
        if let Err(e) =
            pump_decrypt_composite(&env, &backend, &bucket, &key, segments, &h, &mut dst)
        {
            tracing::error!(error = %e, "decrypt (composite) failed mid-stream");
        }
        let _ = dst.shutdown();
    });
    StreamingBlob::wrap(ReaderStream::new(reader))
}

fn pump_decrypt_composite(
    env: &Envelope,
    backend: &Backend,
    bucket: &str,
    key: &str,
    segments: Vec<PartSegment>,
    handle: &Handle,
    dst: &mut impl Write,
) -> hypha_core::error::Result<()> {
    for seg in segments {
        let (ct, pt) = match seg {
            PartSegment::Whole(ct) => (ct, None),
            PartSegment::Partial { ct, pt } => (ct, Some(pt)),
        };
        let source = RemoteRangeSource {
            backend: backend.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            base: ct.start,
            len: ct.end - ct.start,
            handle: handle.clone(),
        };
        let mut dec = env.decrypt(RangeReader::new(source))?;
        match pt {
            None => {
                io::copy(&mut dec, dst)?;
            }
            Some(pt) => {
                dec.seek(SeekFrom::Start(pt.start))?;
                io::copy(&mut dec.take(pt.end - pt.start), dst)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod testutil {
    use super::*;
    use std::collections::VecDeque;

    /// A body delivered in `chunk`-sized frames, yielding `Pending` between them when `jitter` is
    /// set — the polling pattern a real socket produces, and the one a codec that assumes its
    /// source is always ready gets wrong.
    pub struct Frames {
        chunks: VecDeque<Bytes>,
        jitter: bool,
        pending: bool,
    }

    impl Stream for Frames {
        type Item = io::Result<Bytes>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let me = self.get_mut();
            // Fused: the SDK's body adapter polls once more after the end, and a source that
            // answered `Pending` there would hang the request rather than finish it.
            if me.chunks.is_empty() {
                return Poll::Ready(None);
            }
            if me.jitter && !me.pending {
                me.pending = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            me.pending = false;
            Poll::Ready(me.chunks.pop_front().map(Ok))
        }
    }

    pub fn frames(data: &[u8], chunk: usize, jitter: bool) -> StreamingBlob {
        StreamingBlob::wrap(Frames {
            chunks: data.chunks(chunk).map(Bytes::copy_from_slice).collect(),
            jitter,
            pending: false,
        })
    }

    pub fn framed_bytestream(data: &[u8], chunk: usize) -> ByteStream {
        blob_to_bytestream(frames(data, chunk, true))
    }

    pub async fn collect(blob: StreamingBlob) -> Vec<u8> {
        use futures::StreamExt as _;
        let mut out = Vec::new();
        let mut blob = blob;
        while let Some(frame) = blob.next().await {
            out.extend_from_slice(&frame.expect("the stream failed mid-body"));
        }
        out
    }

    pub fn envelope() -> Arc<Envelope> {
        Arc::new(Envelope::new("codec unit test passphrase").unwrap())
    }

    /// One age file, built the synchronous way, so the async codecs are checked against an
    /// independent implementation rather than themselves.
    pub fn age_file(env: &Envelope, plaintext: &[u8]) -> Vec<u8> {
        let mut ct = Vec::new();
        let mut w = env.encrypt(&mut ct).unwrap();
        w.write_all(plaintext).unwrap();
        w.finish().unwrap();
        ct
    }
}

#[cfg(test)]
mod tests {
    use hypha_format::SINGLE_TRAILER_LEN;

    use super::testutil::*;
    use super::*;

    #[tokio::test]
    async fn a_whole_object_decrypts_without_reading_past_its_envelope() {
        let env = envelope();
        let plaintext: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let mut object = age_file(&env, &plaintext);
        let ct_len = object.len() as u64;
        // A trailer the decryptor must not mistake for a truncated final chunk.
        object.extend_from_slice(&[0xAB; SINGLE_TRAILER_LEN]);

        let body = decrypt_full(env, framed_bytestream(&object, 4096), ct_len)
            .await
            .unwrap();
        assert_eq!(collect(body).await, plaintext);
    }

    #[tokio::test]
    async fn a_foreign_header_fails_the_get_rather_than_the_body() {
        let object = vec![0u8; 4096];
        let err = decrypt_full(envelope(), framed_bytestream(&object, 512), 4096)
            .await
            .expect_err("a body that is not an age file cannot open");
        assert!(matches!(err, hypha_core::error::Error::Crypto(_)), "{err}");
    }

    /// Parts of every awkward shape — empty, sub-chunk, exactly one chunk, spanning chunks — laid
    /// end to end, because each one's window has to leave the shared body aligned on the next.
    #[tokio::test]
    async fn a_composite_decrypts_across_its_part_boundaries() {
        let env = envelope();
        let parts: Vec<Vec<u8>> = vec![
            vec![1u8; 70_000],
            Vec::new(),
            vec![2u8; 3],
            vec![3u8; hypha_format::offset::CHUNK_PLAINTEXT as usize],
            vec![4u8; 11],
        ];
        let mut object = Vec::new();
        let mut ct_lens = Vec::new();
        for part in &parts {
            let ct = age_file(&env, part);
            ct_lens.push(ct.len() as u64);
            object.extend_from_slice(&ct);
        }

        let body = decrypt_composite_full(env, framed_bytestream(&object, 1000), ct_lens);
        assert_eq!(collect(body).await, parts.concat());
    }
}
