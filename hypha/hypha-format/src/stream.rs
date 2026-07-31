//! Seekable reader over a re-openable byte-range source.
//!
//! An S3 ranged-GET body is a one-shot stream, but age's `StreamReader` is only seekable over a
//! `Read + Seek` source. [`RangeReader`] bridges the two: it satisfies `Seek` by re-opening the
//! source at the new offset (⇒ one fresh byte-range request per seek), except for short forward
//! seeks, which it serves by discarding bytes from the current stream — cheaper than a new
//! request when the gap is small.

use std::io::{self, Read, Seek, SeekFrom};

/// Forward seeks up to this many bytes are read-and-discarded instead of re-opening.
/// One chunk: the common case is age seeking chunk-by-chunk within an already-covering range.
const SKIP_THRESHOLD: u64 = crate::offset::CHUNK_CIPHERTEXT;

pub trait RangeSource {
    type Reader: Read;
    /// Total length of the underlying object (needed for `SeekFrom::End`).
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Open a stream positioned at `offset`, reading to the end.
    fn open_at(&mut self, offset: u64) -> io::Result<Self::Reader>;
}

pub struct RangeReader<S: RangeSource> {
    source: S,
    pos: u64,
    /// Invariant: when `Some`, the reader is positioned exactly at `pos`.
    inner: Option<S::Reader>,
}

impl<S: RangeSource> RangeReader<S> {
    pub fn new(source: S) -> Self {
        Self {
            source,
            pos: 0,
            inner: None,
        }
    }

    pub fn position(&self) -> u64 {
        self.pos
    }
}

impl<S: RangeSource> Read for RangeReader<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let inner = match self.inner.take() {
            Some(r) => self.inner.insert(r),
            None => self.inner.insert(self.source.open_at(self.pos)?),
        };
        let n = inner.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl<S: RangeSource> Seek for RangeReader<S> {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        let len = self.source.len();
        let new = match target {
            SeekFrom::Start(o) => Some(o),
            SeekFrom::End(d) => len.checked_add_signed(d),
            SeekFrom::Current(d) => self.pos.checked_add_signed(d),
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek before byte 0"))?;

        if new != self.pos {
            let short_forward = new > self.pos && new - self.pos <= SKIP_THRESHOLD;
            match self.inner.as_mut() {
                Some(inner) if short_forward => {
                    io::copy(&mut inner.take(new - self.pos), &mut io::sink())?;
                }
                _ => self.inner = None, // next read re-opens at the new offset
            }
            self.pos = new;
        }
        Ok(self.pos)
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    use super::*;
    use std::sync::Arc;

    /// In-memory source that counts `open_at` calls — stands in for ranged GETs in tests.
    pub struct MemSource {
        pub data: Arc<Vec<u8>>,
        pub opens: usize,
    }

    impl MemSource {
        pub fn new(data: Vec<u8>) -> Self {
            Self {
                data: Arc::new(data),
                opens: 0,
            }
        }
    }

    pub struct MemRead {
        data: Arc<Vec<u8>>,
        pos: usize,
    }

    impl Read for MemRead {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let rest = &self.data[self.pos.min(self.data.len())..];
            let n = rest.len().min(buf.len());
            buf[..n].copy_from_slice(&rest[..n]);
            self.pos += n;
            Ok(n)
        }
    }

    impl RangeSource for MemSource {
        type Reader = MemRead;
        fn len(&self) -> u64 {
            self.data.len() as u64
        }
        fn open_at(&mut self, offset: u64) -> io::Result<Self::Reader> {
            self.opens += 1;
            Ok(MemRead {
                data: self.data.clone(),
                pos: offset as usize,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::MemSource;
    use super::*;

    #[test]
    fn seek_and_read() {
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let mut r = RangeReader::new(MemSource::new(data.clone()));

        let mut buf = [0u8; 16];
        r.seek(SeekFrom::Start(100_000)).unwrap();
        r.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], &data[100_000..100_016]);

        r.seek(SeekFrom::End(-16)).unwrap();
        r.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], &data[data.len() - 16..]);

        assert!(r.seek(SeekFrom::Current(-(data.len() as i64 * 2))).is_err());
    }

    #[test]
    fn short_forward_seek_reuses_stream() {
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 241) as u8).collect();
        let mut r = RangeReader::new(MemSource::new(data.clone()));
        let mut buf = [0u8; 8];

        r.read_exact(&mut buf).unwrap(); // opens (1)
        r.seek(SeekFrom::Current(1000)).unwrap(); // short: discard, no re-open
        r.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], &data[1008..1016]);
        assert_eq!(r.source.opens, 1);

        r.seek(SeekFrom::Start(250_000)).unwrap(); // long: re-open (2)
        r.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..], &data[250_000..250_008]);
        assert_eq!(r.source.opens, 2);
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(64))]

        /// Random seek/read sequences against a slice. This reader has three code paths behind one
        /// interface — a re-open, a short-forward discard, and a read that continues an open stream —
        /// and which one a seek takes depends on the *distance* from wherever the last read left the
        /// position. The hand-written cases above pick those distances deliberately; this picks them
        /// adversarially, including the seeks past the end and before byte 0 that a decryptor probing
        /// a chunk boundary can produce.
        #[test]
        fn prop_seek_and_read_track_a_slice(
            len in 1usize..80_000,
            ops in proptest::collection::vec((0u8..=2, 0i64..100_000, 0usize..3_000), 1..24),
        ) {
            let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let mut reader = RangeReader::new(MemSource::new(data.clone()));
            let mut pos: u64 = 0;

            for (kind, magnitude, take) in ops {
                let target = match kind {
                    0 => SeekFrom::Start(magnitude as u64 % (len as u64 + 1)),
                    // Deliberately signed both ways: a negative delta past the start must fail and
                    // leave the position alone.
                    1 => SeekFrom::Current(magnitude - 50_000),
                    _ => SeekFrom::End(-(magnitude % (len as i64 + 1))),
                };
                let model = match target {
                    SeekFrom::Start(o) => Some(o),
                    SeekFrom::Current(d) => pos.checked_add_signed(d),
                    SeekFrom::End(d) => (len as u64).checked_add_signed(d),
                };
                match model {
                    Some(want) => {
                        let got = reader.seek(target).expect("a representable target must seek");
                        proptest::prop_assert_eq!(got, want);
                        pos = want;
                    }
                    None => proptest::prop_assert!(
                        reader.seek(target).is_err(),
                        "a target before byte 0 must be refused"
                    ),
                }
                proptest::prop_assert_eq!(reader.position(), pos);

                let mut got = Vec::new();
                Read::by_ref(&mut reader)
                    .take(take as u64)
                    .read_to_end(&mut got)
                    .expect("an in-memory source cannot fail");
                let lo = (pos as usize).min(len);
                let hi = pos.saturating_add(take as u64).min(len as u64) as usize;
                proptest::prop_assert_eq!(&got[..], &data[lo..hi]);
                pos += got.len() as u64;
                proptest::prop_assert_eq!(reader.position(), pos);
            }
        }
    }
}
