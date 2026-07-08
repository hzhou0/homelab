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
use hypha_format::{Envelope, RangeReader, RangeSource};
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
pub fn decrypt_full(env: Arc<Envelope>, body: ByteStream) -> StreamingBlob {
    let handle = Handle::current();
    let (writer, reader) = tokio::io::duplex(PIPE_CAP);
    let h = handle.clone();
    tokio::task::spawn_blocking(move || {
        let src = SyncIoBridge::new_with_handle(body.into_async_read(), h.clone());
        let mut dst = SyncIoBridge::new_with_handle(writer, h);
        // Truncation/auth failures surface as a short read on the client — the encrypted stream
        // simply ends; a mid-stream error can't be turned into an HTTP status once headers are sent.
        if let Err(e) = pump_decrypt_full(&env, src, &mut dst) {
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
            ct_len,
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

/// Stream-encrypt a plaintext body to age ciphertext for a remote PUT with a **known
/// Content-Length and no spill**. Returns `(ct_len, body)`: `ct_len` resolves as soon as the
/// blocking task has emitted the (grease-randomized) age header, so the caller can set the PUT's
/// Content-Length before the body is consumed (§6, capture-and-measure).
///
/// `plen` is the plaintext length (known from the cache HEAD). The blocking task buffers the
/// header+nonce in a [`SplitSink`], measures it, computes
/// `ct_len = header_prefix + plen + ⌈plen/64KiB⌉·TAG`, sends it, then flushes the header and
/// streams the payload through the pipe.
pub async fn encrypt_stream(
    env: Arc<Envelope>,
    plaintext: ByteStream,
    plen: u64,
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
        let ct_len = prefix + plen + chunk_count(plen) * TAG;
        // Send before touching the pipe, so the reader (the PutObject) is unblocked to drain it.
        let _ = ctlen_tx.send(ct_len);

        let src = SyncIoBridge::new_with_handle(plaintext.into_async_read(), h);
        if let Err(e) = pump_encrypt(&sink, w, src) {
            tracing::error!(error = %e, "encrypt: streaming payload failed");
        }
    });

    let ct_len = ctlen_rx
        .await
        .map_err(|_| io::Error::other("encrypt task dropped before header"))?;
    let body = blob_to_bytestream(StreamingBlob::wrap(ReaderStream::new(pipe_r)));
    Ok((ct_len, body))
}

/// Encrypt a plaintext `StreamingBlob` to age ciphertext for a remote PUT, computing the client
/// ETag (MD5 of the plaintext) alongside the encryption in one pass — no cache body write needed.
/// Returns `(ct_len, ciphertext_body, etag_receiver)`. Await `etag_receiver` **after** fully
/// consuming `ciphertext_body` (i.e. after the remote PUT returns): the receiver resolves once the
/// blocking task finishes processing the last plaintext byte.
pub async fn encrypt_blob_with_etag(
    env: Arc<Envelope>,
    plaintext: StreamingBlob,
    plen: u64,
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
        let _ = ctlen_tx.send(ct_len);

        let bs = blob_to_bytestream(plaintext);
        let src = SyncIoBridge::new_with_handle(bs.into_async_read(), h);
        let mut md5_src = Md5Reader::new(src);
        if let Err(e) = pump_encrypt(&sink, w, &mut md5_src) {
            tracing::error!(error = %e, "encrypt: streaming payload failed");
        }
        let _ = etag_tx.send(md5_src.finish());
    });

    let ct_len = ctlen_rx
        .await
        .map_err(|_| io::Error::other("encrypt task dropped before header"))?;
    let body = blob_to_bytestream(StreamingBlob::wrap(ReaderStream::new(pipe_r)));
    Ok((ct_len, body, etag_rx))
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
        Self { inner, hasher: md5::Md5::new() }
    }

    /// Consume the reader and return the hex-encoded MD5 of all bytes seen so far.
    fn finish(self) -> String {
        hex::encode(self.hasher.finalize())
    }
}

impl<R: Read> Read for Md5Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // TODO(human): after delegating to self.inner, update self.hasher with &buf[..n] for
        // every read that returns n > 0.
        self.inner.read(buf)
    }
}

fn pump_encrypt<W: Write + ShutdownSync>(
    sink: &SplitSink<W>,
    mut w: age::stream::StreamWriter<SplitSink<W>>,
    mut src: impl Read,
) -> io::Result<()> {
    sink.flush_header_and_forward()?; // header+nonce → pipe, switch to pass-through
    io::copy(&mut src, &mut w)?;
    w.finish()?; // age finalizer chunk (consumes the writer)
    sink.shutdown() // close the pipe so the PutObject body ends
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

/// A [`RangeSource`] backed by re-openable remote byte-range GETs. Lives inside the blocking
/// decrypt task, so it drives the async SDK by blocking on the runtime handle (legal off a
/// `spawn_blocking` thread, which is not a runtime worker).
struct RemoteRangeSource {
    backend: Backend,
    key: String,
    ct_len: u64,
    handle: Handle,
}

impl RangeSource for RemoteRangeSource {
    // The SDK's `into_async_read()` return type is unnameable, so box the bridged sync reader.
    type Reader = Box<dyn Read + Send>;

    fn len(&self) -> u64 {
        self.ct_len
    }

    fn open_at(&mut self, offset: u64) -> io::Result<Self::Reader> {
        let out = self
            .handle
            .block_on(self.backend.get(&self.key, Some(format!("bytes={offset}-"))))
            .map_err(io::Error::other)?;
        let reader = SyncIoBridge::new_with_handle(out.body.into_async_read(), self.handle.clone());
        Ok(Box::new(reader))
    }
}
