//! Async↔sync bridges that drive the Phase-1 `hypha-format` codec (sync `std::io`) from the
//! Tokio serving path. The pattern (§5): a `spawn_blocking` task runs the sync
//! encrypt/decrypt loop and pumps bytes through a `tokio::io::duplex` pipe whose async half
//! becomes the s3s / SDK streaming body. Per-request memory stays bounded by the pipe capacity,
//! never the object size.

use std::cell::RefCell;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;
use hypha_format::offset::{chunk_count, TAG};
use hypha_format::{Envelope, Footer, FooterKind, RangeReader, RangeSource, FOOTER_LEN};
use md5::Digest as _;
use s3s::dto::StreamingBlob;
use s3s_aws::conv::{try_from_aws, try_into_aws};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio_util::io::{ReaderStream, SyncIoBridge};

use hypha_core::Backend;

/// Pipe capacity: a handful of age chunks, enough to keep the blocking encrypter/decrypter and
/// the async socket both busy without buffering the object.
const PIPE_CAP: usize = 256 * 1024;

/// Adapt an incoming client `StreamingBlob` into an SDK `ByteStream` (e.g. to write the plaintext
/// straight through to the cache), via s3s-aws's own body bridge. No copy — the bytes stream.
pub fn blob_to_bytestream(blob: StreamingBlob) -> ByteStream {
    try_into_aws(blob).expect("StreamingBlob → ByteStream is Infallible")
}

/// Adapt an SDK `ByteStream` (e.g. a plaintext cache body) into a `StreamingBlob` to hand back to
/// the client, via s3s-aws's own body bridge. No copy — the bytes stream.
pub fn bytestream_to_blob(bs: ByteStream) -> StreamingBlob {
    try_from_aws(bs).expect("ByteStream → StreamingBlob is Infallible")
}

/// Decrypt a whole remote object body to a plaintext `StreamingBlob`. One remote GET (the caller
/// already opened `body`); the sync `StreamReader` reads header-then-chunks straight through.
/// `ct_len` is the age-envelope length — the object's Content-Length minus [`FOOTER_LEN`] — so
/// the trailing footer never reaches the decryptor (it would read as a truncated chunk).
pub fn decrypt_full(env: Arc<Envelope>, body: ByteStream, ct_len: u64) -> StreamingBlob {
    let handle = Handle::current();
    let (writer, reader) = tokio::io::duplex(PIPE_CAP);
    let h = handle.clone();
    tokio::task::spawn_blocking(move || {
        let src = SyncIoBridge::new_with_handle(body.into_async_read(), h.clone());
        let mut dst = SyncIoBridge::new_with_handle(writer, h);
        // Truncation/auth failures surface as a short read on the client — the encrypted stream
        // simply ends; a mid-stream error can't be turned into an HTTP status once headers are sent.
        if let Err(e) = pump_decrypt_full(&env, src.take(ct_len), &mut dst) {
            tracing::error!(error = %e, "decrypt (full) failed mid-stream");
        }
        let _ = dst.shutdown();
    });
    StreamingBlob::wrap(ReaderStream::new(reader))
}

fn pump_decrypt_full<R: Read>(
    env: &Envelope,
    src: R,
    dst: &mut impl Write,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut dec = env.decrypt(src)?;
    io::copy(&mut dec, dst)?;
    Ok(())
}

/// Decrypt plaintext byte range `pt` of a remote object, re-opening ranged ciphertext GETs
/// through [`RemoteRangeSource`] as age seeks (§6). `ct_len` is the object's ciphertext
/// Content-Length (from a prior HEAD), needed for `SeekFrom::End` and range clamping.
pub fn decrypt_range(
    env: Arc<Envelope>,
    backend: Backend,
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Decryptor::new reads the age header from ciphertext offset 0 (RangeReader opens there),
    // then the seek maps the plaintext offset to a fresh ranged GET of the covering chunks.
    let mut dec = env.decrypt(RangeReader::new(source))?;
    dec.seek(SeekFrom::Start(pt.start))?;
    let mut limited = dec.take(pt.end - pt.start);
    io::copy(&mut limited, dst)?;
    Ok(())
}

/// Stream-encrypt a plaintext body into hypha's framed remote form — age ciphertext followed by
/// the caller's [`Footer`] — with a **known Content-Length and no spill**. Returns
/// `(framed_len, body)`: `framed_len` resolves as soon as the blocking task has emitted the age
/// header, so the caller can set the PUT's Content-Length before the body is consumed (§6,
/// capture-and-measure). The footer's fields must be known up front (the reconcile path reads
/// `plen`/ETag from the same cache GET that streams the body).
#[allow(dead_code)] // phase 4: the reconcile sweep's cache-body → remote upload
pub async fn encrypt_stream(
    env: Arc<Envelope>,
    plaintext: ByteStream,
    plen: u64,
    footer: Footer,
) -> io::Result<(u64, ByteStream)> {
    let handle = Handle::current();
    let (pipe_w, pipe_r) = tokio::io::duplex(PIPE_CAP);
    let (ctlen_tx, ctlen_rx) = oneshot::channel::<u64>();
    let h = handle.clone();

    tokio::task::spawn_blocking(move || {
        let out = SyncIoBridge::new_with_handle(pipe_w, h.clone());
        let sink = SplitSink::new(out);
        let w = match env.encrypt(sink.clone()) {
            Ok(w) => w, // wrap_output has now written header+nonce into the sink's buffer
            Err(e) => {
                tracing::error!(error = %e, "encrypt: wrap_output failed");
                return;
            }
        };
        let prefix = sink.buffered_len() as u64;
        let framed_len = prefix + plen + chunk_count(plen) * TAG + FOOTER_LEN;
        // Send before touching the pipe, so the reader (the PutObject) is unblocked to drain it.
        let _ = ctlen_tx.send(framed_len);

        let src = SyncIoBridge::new_with_handle(plaintext.into_async_read(), h);
        let mut sink_tail = sink.clone();
        if let Err(e) = pump_encrypt(&sink, w, src)
            .and_then(|()| sink_tail.write_all(&footer.encode()))
        {
            tracing::error!(error = %e, "encrypt: streaming payload failed");
        }
        let _ = sink.shutdown();
    });

    let framed_len = ctlen_rx
        .await
        .map_err(|_| io::Error::other("encrypt task dropped before header"))?;
    let body = blob_to_bytestream(StreamingBlob::wrap(ReaderStream::new(pipe_r)));
    Ok((framed_len, body))
}

/// Encrypt a plaintext `StreamingBlob` to age ciphertext, computing the plaintext MD5 alongside
/// the encryption in one pass. `footer_mtime_ms: Some(t)` appends a kind-*single* [`Footer`]
/// (built from the computed digest once the last plaintext byte has streamed) behind the
/// ciphertext, so a single-part PUT lands body and facts atomically (§6); `None` emits a pure
/// age file — a multipart part, whose facts live in the object's terminating footer part.
/// Returns `(body_len, body, etag_receiver)`. Await `etag_receiver` **after** fully consuming
/// `body` (i.e. after the remote op returns): it resolves with the hex MD5 at stream end.
pub async fn encrypt_blob_with_etag(
    env: Arc<Envelope>,
    plaintext: StreamingBlob,
    plen: u64,
    footer_mtime_ms: Option<i64>,
) -> io::Result<(u64, ByteStream, oneshot::Receiver<String>)> {
    let handle = Handle::current();
    let (pipe_w, pipe_r) = tokio::io::duplex(PIPE_CAP);
    let (ctlen_tx, ctlen_rx) = oneshot::channel::<u64>();
    let (etag_tx, etag_rx) = oneshot::channel::<String>();
    let h = handle.clone();

    tokio::task::spawn_blocking(move || {
        let out = SyncIoBridge::new_with_handle(pipe_w, h.clone());
        let sink = SplitSink::new(out);
        let w = match env.encrypt(sink.clone()) {
            Ok(w) => w,
            Err(e) => {
                tracing::error!(error = %e, "encrypt: wrap_output failed");
                return;
            }
        };
        let prefix = sink.buffered_len() as u64;
        let ct_len = prefix + plen + chunk_count(plen) * TAG;
        let body_len = ct_len + if footer_mtime_ms.is_some() { FOOTER_LEN } else { 0 };
        let _ = ctlen_tx.send(body_len);

        let bs = blob_to_bytestream(plaintext);
        let src = SyncIoBridge::new_with_handle(bs.into_async_read(), h);
        let mut md5_src = Md5Reader::new(src);
        match pump_encrypt(&sink, w, &mut md5_src) {
            Ok(()) => {
                let md5 = md5_src.finish();
                let footer_written = footer_mtime_ms.map_or(Ok(()), |mtime_ms| {
                    let footer = Footer {
                        kind: FooterKind::Single,
                        count: 1,
                        plen,
                        mtime_ms,
                        md5,
                    };
                    sink.clone().write_all(&footer.encode())
                });
                match footer_written {
                    Ok(()) => {
                        let _ = etag_tx.send(hex::encode(md5));
                    }
                    Err(e) => tracing::error!(error = %e, "encrypt: writing footer failed"),
                }
            }
            Err(e) => tracing::error!(error = %e, "encrypt: streaming payload failed"),
        }
        let _ = sink.shutdown();
    });

    let body_len = ctlen_rx
        .await
        .map_err(|_| io::Error::other("encrypt task dropped before header"))?;
    let body = blob_to_bytestream(StreamingBlob::wrap(ReaderStream::new(pipe_r)));
    Ok((body_len, body, etag_rx))
}

/// A [`Read`] adapter that hashes every byte passing through it, finalized via [`finish`].
/// Wraps any `Read` source; hypha uses it to derive the client ETag alongside encryption so
/// durable-mode PUTs never need a second pass or a cache round-trip.
struct Md5Reader<R> {
    inner: R,
    hasher: md5::Md5,
}

impl<R> Md5Reader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: md5::Md5::new(),
        }
    }

    /// Consume the reader and return the raw MD5 digest of all bytes seen so far.
    fn finish(self) -> [u8; 16] {
        self.hasher.finalize().into()
    }
}

impl<R: Read> Read for Md5Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

/// Header+nonce → pipe, stream the payload, write the age finalizer chunk. The caller appends
/// the footer (whose digest may only exist now) and shuts the sink down so the body ends.
fn pump_encrypt<W: Write>(
    sink: &SplitSink<W>,
    mut w: age::stream::StreamWriter<SplitSink<W>>,
    mut src: impl Read,
) -> io::Result<()> {
    sink.flush_header_and_forward()?;
    io::copy(&mut src, &mut w)?;
    w.finish()?; // consumes the writer
    Ok(())
}

/// A `Write` that buffers everything until [`flush_header_and_forward`], then passes through to
/// its inner writer. Used to capture (and length-measure) the age header+nonce that `wrap_output`
/// emits before the first body byte, without sending it downstream until the ciphertext length is
/// known. Cloneable because `age::Encryptor::wrap_output` takes the writer by value while we keep
/// a handle to inspect and drive it — single blocking thread, so `Rc<RefCell>` is sound.
struct SplitSink<W>(Rc<RefCell<SplitInner<W>>>);

// Clone the handle only — `W` needn't be `Clone` (derive would wrongly demand it).
impl<W> Clone for SplitSink<W> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

struct SplitInner<W> {
    buf: Vec<u8>,
    out: W,
    forwarding: bool,
}

impl<W: Write> SplitSink<W> {
    fn new(out: W) -> Self {
        Self(Rc::new(RefCell::new(SplitInner {
            buf: Vec::new(),
            out,
            forwarding: false,
        })))
    }
    fn buffered_len(&self) -> usize {
        self.0.borrow().buf.len()
    }
    fn flush_header_and_forward(&self) -> io::Result<()> {
        let mut inner = self.0.borrow_mut();
        let buf = std::mem::take(&mut inner.buf);
        inner.out.write_all(&buf)?;
        inner.forwarding = true;
        Ok(())
    }
    fn shutdown(&self) -> io::Result<()>
    where
        W: ShutdownSync,
    {
        self.0.borrow_mut().out.shutdown_sync()
    }
}

impl<W: Write> Write for SplitSink<W> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let mut inner = self.0.borrow_mut();
        if inner.forwarding {
            inner.out.write(b)
        } else {
            inner.buf.extend_from_slice(b);
            Ok(b.len())
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        let mut inner = self.0.borrow_mut();
        if inner.forwarding {
            inner.out.flush()
        } else {
            Ok(())
        }
    }
}

/// Lets the split sink close its underlying async-backed writer to signal body EOF.
trait ShutdownSync {
    fn shutdown_sync(&mut self) -> io::Result<()>;
}
impl<T: tokio::io::AsyncWrite + Unpin> ShutdownSync for SyncIoBridge<T> {
    fn shutdown_sync(&mut self) -> io::Result<()> {
        self.shutdown()
    }
}

/// A [`RangeSource`] over a byte window `[base, base+len)` of a remote object, re-opened by
/// byte-range GETs. `base = 0, len = ct_len` reads a whole single-part object; a composite part
/// (its own age file inside the concatenation, §7) is a non-zero window. Lives inside the
/// blocking decrypt task, so it drives the async SDK by blocking on the runtime handle (legal
/// off a `spawn_blocking` thread, which is not a runtime worker).
struct RemoteRangeSource {
    backend: Backend,
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
            .block_on(self.backend.get(&self.key, Some(range)))
            .map_err(io::Error::other)?;
        let reader = SyncIoBridge::new_with_handle(out.body.into_async_read(), self.handle.clone());
        Ok(Box::new(reader))
    }
}

// ── Composite bodies (§7) ───────────────────────────────────────────────────────────────────

/// One part's contribution to a composite read: the part's absolute ciphertext window in the
/// remote object, and which plaintext bytes of it to emit.
pub enum PartSegment {
    /// The whole part, start to finish — no plaintext length needed, the age stream ends itself.
    Whole(Range<u64>),
    /// Plaintext range `pt` (offsets *within this part*) of the part at ciphertext window `ct`.
    Partial { ct: Range<u64>, pt: Range<u64> },
}

/// Decrypt a committed composite — a concatenation of independent age files — by decrypting each
/// segment's part as its own file, in order, into one plaintext stream. The read path resolved
/// the segments from the remote's part index (§7).
pub fn decrypt_composite(
    env: Arc<Envelope>,
    backend: Backend,
    key: String,
    segments: Vec<PartSegment>,
) -> StreamingBlob {
    let handle = Handle::current();
    let (writer, reader) = tokio::io::duplex(PIPE_CAP);
    let h = handle.clone();
    tokio::task::spawn_blocking(move || {
        let mut dst = SyncIoBridge::new_with_handle(writer, h.clone());
        if let Err(e) = pump_decrypt_composite(&env, &backend, &key, segments, &h, &mut dst) {
            tracing::error!(error = %e, "decrypt (composite) failed mid-stream");
        }
        let _ = dst.shutdown();
    });
    StreamingBlob::wrap(ReaderStream::new(reader))
}

fn pump_decrypt_composite(
    env: &Envelope,
    backend: &Backend,
    key: &str,
    segments: Vec<PartSegment>,
    handle: &Handle,
    dst: &mut impl Write,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for seg in segments {
        let (ct, pt) = match seg {
            PartSegment::Whole(ct) => (ct, None),
            PartSegment::Partial { ct, pt } => (ct, Some(pt)),
        };
        let source = RemoteRangeSource {
            backend: backend.clone(),
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
