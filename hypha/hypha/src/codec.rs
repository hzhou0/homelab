//! Async streaming codecs, with a synchronous bridge only where age's seekable reader is required.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;
use futures::io::AsyncWriteExt as _;
use hypha_format::offset::{ciphertext_len, HLEN};
use hypha_format::{
    encode_trailer, Envelope, Footer, FooterKind, RangeReader, RangeSource, TrailerKey,
    SINGLE_TRAILER_LEN,
};
use md5::Digest as _;
use s3s::dto::StreamingBlob;
use s3s_aws::conv::{try_from_aws, try_into_aws};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio_util::compat::{
    FuturesAsyncReadCompatExt as _, TokioAsyncReadCompatExt as _, TokioAsyncWriteCompatExt as _,
};
use tokio_util::io::{ReaderStream, SyncIoBridge};

use hypha_core::Backend;

/// A handful of age chunks keeps adjacent streaming stages busy without buffering the object.
const PIPE_CAP: usize = 256 * 1024;

/// The facts a single-part commit stamps into its tail trailer, alongside the body. The MD5 isn't
/// here: it's computed inline as the plaintext streams (§6) and folded into the trailer at stream
/// end. `None` to [`encrypt_blob_with_etag`] emits a pure age file (a multipart part), whose facts
/// live in the object's one terminating trailer part instead.
pub struct SingleTrailer {
    pub trailer_key: TrailerKey,
    pub object_key: String,
    pub mtime_ms: i64,
}

#[derive(Debug)]
pub struct DigestMismatch;

pub type EtagReceiver = oneshot::Receiver<Result<String, DigestMismatch>>;

pub fn blob_to_bytestream(blob: StreamingBlob) -> ByteStream {
    try_into_aws(blob).expect("StreamingBlob → ByteStream is Infallible")
}

pub fn bytestream_to_blob(bs: ByteStream) -> StreamingBlob {
    try_from_aws(bs).expect("ByteStream → StreamingBlob is Infallible")
}

/// Split one body into two identical streams, so it can reach two sinks in a single pass — the
/// upload path for a retained part (§7), which must land on the remote *and* in the cache without
/// the encrypt task running twice. A pump reads the source once and writes every chunk into both
/// pipes, so per-request memory stays at pipe capacity however large the part is.
///
/// The two writes are driven concurrently, so neither pipe head-of-line blocks the other; the
/// slower sink still paces the pump, which is the intended backpressure. If either sink goes away
/// — its request failed, or was cancelled because the other did — writing to that pipe fails and
/// the pump exits, closing both and letting the surviving side see a short body and error out.
pub fn tee(src: ByteStream) -> (ByteStream, ByteStream) {
    let (mut a_w, a_r) = tokio::io::duplex(PIPE_CAP);
    let (mut b_w, b_r) = tokio::io::duplex(PIPE_CAP);
    tokio::spawn(async move {
        let mut rd = src.into_async_read();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match rd.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    tracing::error!(error = %e, "tee: reading the source failed mid-stream");
                    break;
                }
            };
            let chunk = &buf[..n];
            if tokio::try_join!(a_w.write_all(chunk), b_w.write_all(chunk)).is_err() {
                // A sink dropped its half; the other will see a short body and fail its request.
                break;
            }
        }
        let _ = a_w.shutdown().await;
        let _ = b_w.shutdown().await;
    });
    (
        blob_to_bytestream(StreamingBlob::wrap(ReaderStream::new(a_r))),
        blob_to_bytestream(StreamingBlob::wrap(ReaderStream::new(b_r))),
    )
}

/// A stream of `body` followed by `tail`, without buffering `body` — the complete-time trailer
/// fold (§7), where the retained part may be gigabytes but the trailer is a few dozen KB.
pub fn append_bytes(body: ByteStream, tail: Vec<u8>) -> ByteStream {
    let chained = body.into_async_read().chain(io::Cursor::new(tail));
    blob_to_bytestream(StreamingBlob::wrap(ReaderStream::new(chained)))
}

/// Decrypt a whole remote object body to a plaintext `StreamingBlob`. One remote GET (the caller
/// already opened `body`); age reads header-then-chunks straight through.
/// `ct_len` is the age-envelope length — the object's Content-Length minus the tail trailer — so
/// the trailer never reaches the decryptor (it would read as a truncated chunk).
pub fn decrypt_full(env: Arc<Envelope>, body: ByteStream, ct_len: u64) -> StreamingBlob {
    let (mut writer, reader) = tokio::io::duplex(PIPE_CAP);
    tokio::spawn(async move {
        let src = body.into_async_read().take(ct_len).compat();
        let result = async {
            let dec = env.decrypt_async(src).await?;
            let mut dec = dec.compat();
            tokio::io::copy(&mut dec, &mut writer).await?;
            Ok::<_, hypha_core::error::Error>(())
        }
        .await;
        // Truncation/auth failures surface as a short read on the client — the encrypted stream
        // simply ends; a mid-stream error can't be turned into an HTTP status once headers are sent.
        if let Err(e) = result {
            tracing::error!(error = %e, "decrypt (full) failed mid-stream");
        }
        let _ = writer.shutdown().await;
    });
    StreamingBlob::wrap(ReaderStream::new(reader))
}

/// Decrypt plaintext byte range `pt` of a remote object, re-opening ranged ciphertext GETs
/// through [`RemoteRangeSource`] as age seeks (§6). `ct_len` is the object's ciphertext
/// Content-Length (from a prior HEAD), needed for `SeekFrom::End` and range clamping.
pub fn decrypt_range(
    env: Arc<Envelope>,
    backend: Backend,
    bucket: String,
    key: String,
    ct_len: u64,
    pt: Range<u64>,
) -> StreamingBlob {
    let handle = Handle::current();
    let (writer, reader) = tokio::io::duplex(PIPE_CAP);
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

/// Stream-encrypt a plaintext body into hypha's framed single-part form — age ciphertext followed
/// by its [`SingleTrailer`] — with a Content-Length known up front (the age header is a fixed
/// [`HLEN`], so `ciphertext_len` is exact) and no spill. Returns `(framed_len, body)`. The trailer
/// carries the plaintext MD5, computed inline as the body streams (§6) — the reconcile path knows
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
        Some(trailer),
        None,
    )
    .await?;
    Ok((framed_len, body))
}

/// Encrypt a plaintext `StreamingBlob` to age ciphertext, computing the plaintext MD5 alongside the
/// encryption in one pass. `trailer: Some(_)` appends a kind-*single* trailer (built from the
/// computed digest once the last plaintext byte has streamed) behind the ciphertext, so a
/// single-part PUT lands body and facts atomically (§6); `None` emits a pure age file — a multipart
/// part, whose facts live in the object's terminating trailer part.
///
/// `expect_md5` is the client's `Content-MD5`, checked against the digest the body actually
/// produced; on a mismatch nothing further is written, so the backend op fails rather than
/// committing a corrupt object (see [`DigestMismatch`]).
///
/// Returns `(body_len, body, etag_receiver)`. `body_len` is exact and synchronous (`HLEN` is
/// constant). Await `etag_receiver` **after** fully consuming `body` (i.e. after the remote op
/// returns): it resolves with the hex MD5 at stream end.
pub async fn encrypt_blob_with_etag(
    env: Arc<Envelope>,
    plaintext: StreamingBlob,
    plen: u64,
    trailer: Option<SingleTrailer>,
    expect_md5: Option<[u8; 16]>,
) -> io::Result<(u64, ByteStream, EtagReceiver)> {
    let (pipe_w, pipe_r) = tokio::io::duplex(PIPE_CAP);
    let (etag_tx, etag_rx) = oneshot::channel::<Result<String, DigestMismatch>>();

    let body_ct_len = ciphertext_len(plen, HLEN);
    let body_len = body_ct_len
        + if trailer.is_some() {
            SINGLE_TRAILER_LEN as u64
        } else {
            0
        };

    tokio::spawn(async move {
        if let Err(e) = pump_encrypt_async(
            env,
            plaintext,
            pipe_w,
            etag_tx,
            plen,
            body_ct_len,
            trailer,
            expect_md5,
        )
        .await
        {
            tracing::error!(error = %e, "encrypt: streaming payload failed");
        }
    });

    let body = blob_to_bytestream(StreamingBlob::wrap(ReaderStream::new(pipe_r)));
    Ok((body_len, body, etag_rx))
}

#[allow(clippy::too_many_arguments)]
async fn pump_encrypt_async(
    env: Arc<Envelope>,
    plaintext: StreamingBlob,
    mut output: tokio::io::DuplexStream,
    etag_tx: oneshot::Sender<Result<String, DigestMismatch>>,
    plen: u64,
    body_ct_len: u64,
    trailer: Option<SingleTrailer>,
    expect_md5: Option<[u8; 16]>,
) -> io::Result<()> {
    let (age_w, mut age_r) = tokio::io::duplex(PIPE_CAP);
    let (digest_tx, digest_rx) = oneshot::channel::<[u8; 16]>();

    let produce = async move {
        let mut src = blob_to_bytestream(plaintext).into_async_read();
        let mut encrypted = env
            .encrypt_async(age_w.compat_write())
            .await
            .map_err(io::Error::other)?;
        let mut hasher = md5::Md5::new();
        let mut buf = vec![0; 64 * 1024];
        loop {
            let n = src.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            encrypted.write_all(&buf[..n]).await?;
        }
        encrypted.close().await?;
        let _ = digest_tx.send(hasher.finalize().into());
        Ok::<_, io::Error>(())
    };

    let consume = async move {
        let mut held = None;
        let mut buf = vec![0; 64 * 1024];
        loop {
            let n = age_r.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            if let Some(byte) = held.take() {
                output.write_all(&[byte]).await?;
            }
            if n > 1 {
                output.write_all(&buf[..n - 1]).await?;
            }
            held = buf[..n].last().copied();
        }

        let md5 = digest_rx
            .await
            .map_err(|_| io::Error::other("encrypt task ended before producing a digest"))?;
        // The digest is unknowable until EOF. Holding one ciphertext byte until this check keeps
        // even a trailerless multipart part short of its declared length on mismatch.
        if expect_md5.is_some_and(|want| want != md5) {
            let _ = etag_tx.send(Err(DigestMismatch));
            return Ok(());
        }

        let final_byte = held.ok_or_else(|| io::Error::other("age emitted no ciphertext"))?;
        output.write_all(&[final_byte]).await?;
        if let Some(t) = trailer {
            let footer = Footer {
                kind: FooterKind::Single,
                count: 1,
                plen,
                mtime_ms: t.mtime_ms,
                md5,
            };
            let blob = encode_trailer(&t.trailer_key, &t.object_key, body_ct_len, &footer, &[]);
            output.write_all(&blob).await?;
        }
        let _ = etag_tx.send(Ok(hex::encode(md5)));
        output.shutdown().await
    };

    let (produce_result, consume_result) = tokio::join!(produce, consume);
    produce_result?;
    consume_result
}

/// A [`RangeSource`] over a byte window `[base, base+len)` of a remote object, re-opened by
/// byte-range GETs. `base = 0, len = ct_len` reads a whole single-part object; a composite part
/// (its own age file inside the concatenation, §7) is a non-zero window. Lives inside the
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

// ── Composite bodies (§7) ───────────────────────────────────────────────────────────────────

/// Decrypt a whole committed composite in **one GET** (§7): the caller fetches `[0, body_ct_len)`
/// — the concatenated parts, trailer excluded — and hands it here with each part's ciphertext
/// length (from the trailer's parts table). Each part is an independent age file, so a `Take`
/// bounded to its exact window makes age stop at that part's final chunk and consume precisely
/// that many bytes, leaving the shared stream aligned on the next part. O(1) round trips.
pub fn decrypt_composite_full(
    env: Arc<Envelope>,
    body: ByteStream,
    part_ct_lens: Vec<u64>,
) -> StreamingBlob {
    let (mut writer, reader) = tokio::io::duplex(PIPE_CAP);
    tokio::spawn(async move {
        let mut src = body.into_async_read();
        let result = async {
            for len in part_ct_lens {
                let part = (&mut src).take(len).compat();
                let dec = env.decrypt_async(part).await?;
                let mut dec = dec.compat();
                tokio::io::copy(&mut dec, &mut writer).await?;
            }
            Ok::<_, hypha_core::error::Error>(())
        }
        .await;
        if let Err(e) = result {
            tracing::error!(error = %e, "decrypt (composite full) failed mid-stream");
        }
        let _ = writer.shutdown().await;
    });
    StreamingBlob::wrap(ReaderStream::new(reader))
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
    let handle = Handle::current();
    let (writer, reader) = tokio::io::duplex(PIPE_CAP);
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
