//! The plaintext facts footer at the tail of every remote object (§6) — **one per object**.
//!
//! The remote body is ciphertext, so its own Content-Length and ETag describe ciphertext — but
//! S3 offers no slot that lands facts atomically with a streamed body: user-metadata travels
//! ahead of the body, while the client ETag (MD5 of the plaintext) exists only once the body has
//! streamed. A fixed-size trailer *behind* the ciphertext is the one carrier that is both atomic
//! and at a knowable offset (`len − FOOTER_LEN`), so a cold read, repair, or restore recovers
//! any object's complete facts with one exact tail read — no metadata, no tags, no side records.
//!
//! A single-part object appends the footer in the same `PutObject` stream; a composite's footer
//! is a 48-byte **terminating part** uploaded just before `CompleteMultipartUpload`, so the
//! native complete commits body and facts in one atomic op (its parts stay pure age files).
//!
//! The footer sits **outside** the age envelope, and decrypt paths must stop at
//! `len − FOOTER_LEN`: age's reader is delimited by EOF, not by anything in-band, so trailing
//! bytes get pulled into the final chunk read and fail authentication (guarded by
//! `trailing_bytes_break_decryption` below). DR with a stock age binary is
//! `head -c -48 file | age -d`.

/// Total encoded size. Fixed — the offset math and the tail reads depend on it.
pub const FOOTER_LEN: u64 = 48;

const MAGIC: [u8; 8] = *b"hyphaftr";
const VERSION: u8 = 1;

/// What the object at whose tail this footer sits is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FooterKind {
    /// One age file ‖ footer, written by a single `PutObject`.
    Single = 0,
    /// A native-multipart concatenation of age files with the footer as its terminating part;
    /// `count` is the client part count (the `-N` of the composite ETag).
    Composite = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Footer {
    pub kind: FooterKind,
    /// Client part count ([`FooterKind::Composite`]; 1 for a single-part object).
    pub count: u32,
    /// Total plaintext length of the object.
    pub plen: u64,
    /// Original client-write mtime, unix ms (a composite's is its completion time).
    pub mtime_ms: i64,
    /// Raw MD5: of the plaintext (single) or of the concatenated per-part plaintext MD5s
    /// (composite) — see [`Footer::client_etag`].
    pub md5: [u8; 16],
}

impl Footer {
    /// Layout: magic(8) ‖ version(1) ‖ kind(1) ‖ count u32 LE(4) ‖ reserved(2) ‖ plen u64 LE(8) ‖
    /// mtime_ms i64 LE(8) ‖ md5(16).
    pub fn encode(&self) -> [u8; FOOTER_LEN as usize] {
        let mut b = [0u8; FOOTER_LEN as usize];
        b[..8].copy_from_slice(&MAGIC);
        b[8] = VERSION;
        b[9] = self.kind as u8;
        b[10..14].copy_from_slice(&self.count.to_le_bytes());
        b[16..24].copy_from_slice(&self.plen.to_le_bytes());
        b[24..32].copy_from_slice(&self.mtime_ms.to_le_bytes());
        b[32..48].copy_from_slice(&self.md5);
        b
    }

    /// `None` ⇒ not a hypha footer — the object was never written through hypha.
    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != FOOTER_LEN as usize || b[..8] != MAGIC || b[8] != VERSION {
            return None;
        }
        let kind = match b[9] {
            0 => FooterKind::Single,
            1 => FooterKind::Composite,
            _ => return None,
        };
        Some(Self {
            kind,
            count: u32::from_le_bytes(b[10..14].try_into().unwrap()),
            plen: u64::from_le_bytes(b[16..24].try_into().unwrap()),
            mtime_ms: i64::from_le_bytes(b[24..32].try_into().unwrap()),
            md5: b[32..48].try_into().unwrap(),
        })
    }

    /// The S3 client ETag this footer projects: bare hex MD5 for a single-part object, the
    /// composite `hex-N` form otherwise.
    pub fn client_etag(&self) -> String {
        let hex: String = self.md5.iter().map(|b| format!("{b:02x}")).collect();
        match self.kind {
            FooterKind::Single => hex,
            FooterKind::Composite => format!("{hex}-{}", self.count),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        for (kind, count) in [(FooterKind::Single, 1), (FooterKind::Composite, 17)] {
            let f = Footer {
                kind,
                count,
                plen: 123_456_789,
                mtime_ms: 1_700_000_000_000,
                md5: [0xab; 16],
            };
            let enc = f.encode();
            assert_eq!(enc.len() as u64, FOOTER_LEN);
            assert_eq!(Footer::decode(&enc), Some(f));
        }
    }

    #[test]
    fn client_etag_forms() {
        let mut f = Footer {
            kind: FooterKind::Single,
            count: 1,
            plen: 0,
            mtime_ms: 0,
            md5: [0xab; 16],
        };
        assert_eq!(f.client_etag(), "ab".repeat(16));
        f.kind = FooterKind::Composite;
        f.count = 3;
        assert_eq!(f.client_etag(), format!("{}-3", "ab".repeat(16)));
    }

    #[test]
    fn rejects_foreign_bytes() {
        assert_eq!(Footer::decode(&[0u8; FOOTER_LEN as usize]), None);
        assert_eq!(Footer::decode(&[0u8; 10]), None);
        let mut b = Footer {
            kind: FooterKind::Single,
            count: 1,
            plen: 1,
            mtime_ms: 0,
            md5: [0; 16],
        }
        .encode();
        b[8] = 99; // unknown version
        assert_eq!(Footer::decode(&b), None);
        b[8] = VERSION;
        b[9] = 7; // unknown kind
        assert_eq!(Footer::decode(&b), None);
    }

    /// The reason decrypt paths bound their reads at `len − FOOTER_LEN`: age's reader is
    /// delimited by EOF (`last = chunk.len() < ENCRYPTED_CHUNK_SIZE`, stream.rs), so trailing
    /// footer bytes get pulled into the final chunk read and fail authentication. If a future
    /// age becomes trailing-tolerant this starts failing and the bounds become removable.
    #[test]
    fn trailing_bytes_break_decryption() {
        use std::io::Read;

        let env = crate::Envelope::generate();
        for plen in [100usize, 64 * 1024] {
            let plaintext = vec![7u8; plen];
            let mut ct = Vec::new();
            let mut w = env.encrypt(&mut ct).unwrap();
            std::io::Write::write_all(&mut w, &plaintext).unwrap();
            w.finish().unwrap();
            let ct_len = ct.len();
            ct.extend_from_slice(&[0u8; FOOTER_LEN as usize]);

            let mut out = Vec::new();
            let res = env
                .decrypt(&ct[..])
                .and_then(|mut r| r.read_to_end(&mut out).map_err(crate::Error::Io));
            assert!(res.is_err(), "unbounded decrypt must fail (plen={plen})");

            let mut out = Vec::new();
            let mut r = env.decrypt((&ct[..]).take(ct_len as u64)).unwrap();
            r.read_to_end(&mut out).unwrap();
            assert_eq!(out, plaintext, "bounded decrypt must succeed (plen={plen})");
        }
    }
}
