//! The authenticated facts+table trailer at the tail of every remote object (§6).
//!
//! S3 offers no slot that lands object facts atomically with a streamed body (user-metadata
//! travels ahead of the body; `MD5(plaintext)` exists only once the body has streamed; tags are
//! post-hoc). A trailer *behind* the ciphertext is both atomic and at a knowable offset, so every
//! commit is self-describing. The trailer is **plaintext, authenticated by a truncated
//! HMAC-SHA256 tag** keyed off the master passphrase: truncation, tampering, and foreign objects
//! all fail to verify. Physical tail order is `table ‖ facts ‖ tag(16) ‖ version(2)`; the
//! fixed-size facts struct sits at a known offset from the end, so its `count` sizes the table and
//! the 2-byte version dispatches the format.
//!
//! The trailer sits **outside** the age envelope(s): age's reader is EOF-delimited, so trailing
//! bytes would be pulled into the final chunk and fail authentication (`trailing_bytes_break_*`
//! below). Decrypt paths must therefore stop before it.

use std::mem::size_of;
use std::ops::Range;

use bytemuck::{Pod, Zeroable};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::offset::{plaintext_len_from, HLEN};

type HmacSha256 = Hmac<Sha256>;

/// Trailer format version, stored as the object's final two bytes; bump on any layout or crypto
/// change (e.g. if `HLEN` ever changes — see `offset::HLEN`).
pub const VERSION: u16 = 1;

/// Truncated HMAC-SHA256 tag length (128-bit forgery resistance).
pub const TAG_LEN: usize = 16;
pub const VERSION_LEN: usize = 2;

/// Max client parts in a composite — native part 10000 is reserved for the trailer part.
pub const MAX_PARTS: usize = 9_999;
/// One parts-table entry: a little-endian `u64` cumulative ciphertext end-offset.
pub const PART_ENTRY_LEN: usize = 8;
pub const MAX_TABLE_LEN: usize = MAX_PARTS * PART_ENTRY_LEN;
/// Fixed facts-struct size (the bytemuck wire form).
pub const FACTS_LEN: usize = size_of::<FactsRepr>();
/// A single-part object's trailer: no parts table, so the tail is exactly `facts ‖ tag ‖ version`.
/// Fixed, so a single-part read's envelope length is `content_length − SINGLE_TRAILER_LEN`.
pub const SINGLE_TRAILER_LEN: usize = FACTS_LEN + TAG_LEN + VERSION_LEN;
/// The largest possible trailer. One speculative suffix GET of this many bytes always captures
/// `table ‖ facts ‖ tag ‖ version` for any object, so composite reads never need a second round trip.
pub const MAX_TAIL_LEN: usize = MAX_TABLE_LEN + FACTS_LEN + TAG_LEN + VERSION_LEN;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FooterKind {
    Single = 0,
    Composite = 1,
}

/// Object facts, decoded from (and encoded to) the trailer. `client_etag` projects the S3 ETag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Footer {
    pub kind: FooterKind,
    /// Client part count ([`FooterKind::Composite`]; 1 for single-part).
    pub count: u32,
    /// Total plaintext length of the object.
    pub plen: u64,
    /// Original client-write mtime, unix ms (a composite's is its completion time).
    pub mtime_ms: i64,
    /// Raw MD5: of the plaintext (single) or of the concatenated per-part plaintext MD5s
    /// (composite) — see [`Footer::client_etag`].
    pub md5: [u8; 16],
}

/// Wire form of the facts struct: all little-endian byte arrays ⇒ align 1, no padding, and no
/// native-endian trap (the format is little-endian by contract).
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct FactsRepr {
    kind: u8,
    count: [u8; 4],
    plen: [u8; 8],
    mtime_ms: [u8; 8],
    md5: [u8; 16],
}

impl Footer {
    fn to_repr(&self) -> FactsRepr {
        FactsRepr {
            kind: self.kind as u8,
            count: self.count.to_le_bytes(),
            plen: self.plen.to_le_bytes(),
            mtime_ms: self.mtime_ms.to_le_bytes(),
            md5: self.md5,
        }
    }

    fn from_repr(r: &FactsRepr) -> Option<Self> {
        let kind = match r.kind {
            0 => FooterKind::Single,
            1 => FooterKind::Composite,
            _ => return None,
        };
        Some(Self {
            kind,
            count: u32::from_le_bytes(r.count),
            plen: u64::from_le_bytes(r.plen),
            mtime_ms: i64::from_le_bytes(r.mtime_ms),
            md5: r.md5,
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

/// The trailer MAC key, derived once from the master passphrase and reused across all trailers
/// (an HMAC key is safe under reuse — no nonce, no per-write state).
#[derive(Clone)]
pub struct TrailerKey([u8; 32]);

impl TrailerKey {
    /// One-step KDF off the (full-entropy 256-bit) master passphrase — HMAC-Extract with a
    /// domain-separating label; safe because the passphrase already carries the entropy.
    pub fn derive(passphrase: &str) -> Self {
        let mut mac =
            HmacSha256::new_from_slice(passphrase.as_bytes()).expect("HMAC accepts any key length");
        mac.update(b"hypha-trailer-key-v1");
        let mut k = [0u8; 32];
        k.copy_from_slice(&mac.finalize().into_bytes());
        Self(k)
    }
}

/// The authenticated tag over a trailer. `object_key` goes **last**: everything before it is
/// fixed-width or self-delimiting (version, body length, fixed facts, and a table whose length is
/// `count·8` from the facts), so the variable-length key can't shift a field boundary.
fn compute_tag(
    key: &TrailerKey,
    version: u16,
    object_key: &str,
    body_ct_len: u64,
    facts: &[u8],
    table: &[u8],
) -> [u8; TAG_LEN] {
    let mut mac = HmacSha256::new_from_slice(&key.0).expect("HMAC accepts any key length");
    mac.update(&version.to_le_bytes());
    mac.update(&body_ct_len.to_le_bytes());
    mac.update(facts);
    mac.update(table);
    mac.update(object_key.as_bytes());
    let full = mac.finalize().into_bytes();
    let mut tag = [0u8; TAG_LEN];
    tag.copy_from_slice(&full[..TAG_LEN]);
    tag
}

/// Build the trailer bytes: appended in-stream for single-part, or uploaded as the terminating
/// part for a composite. `table` is empty for single-part; for a composite it is the per-part
/// cumulative ciphertext end-offsets. `body_ct_len` is the ciphertext length preceding the trailer
/// (the age body, or the concatenated parts) — bound into the MAC.
pub fn encode_trailer(
    key: &TrailerKey,
    object_key: &str,
    body_ct_len: u64,
    footer: &Footer,
    table: &[u64],
) -> Vec<u8> {
    let facts = footer.to_repr();
    let facts_bytes = bytemuck::bytes_of(&facts);

    let mut table_bytes = Vec::with_capacity(table.len() * PART_ENTRY_LEN);
    for &off in table {
        table_bytes.extend_from_slice(&off.to_le_bytes());
    }

    let tag = compute_tag(
        key,
        VERSION,
        object_key,
        body_ct_len,
        facts_bytes,
        &table_bytes,
    );

    let mut out = Vec::with_capacity(table_bytes.len() + FACTS_LEN + TAG_LEN + VERSION_LEN);
    out.extend_from_slice(&table_bytes);
    out.extend_from_slice(facts_bytes);
    out.extend_from_slice(&tag);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out
}

/// Everything the read path needs, recovered from one authenticated tail read.
pub struct Tail {
    pub footer: Footer,
    /// Composite only: per-part absolute ciphertext windows `[start, end)` (empty for single-part).
    pub windows: Vec<Range<u64>>,
    /// Composite only: per-part plaintext lengths, closed-form over [`HLEN`] (empty for single-part).
    pub plens: Vec<u64>,
    /// Ciphertext length preceding the trailer — the age body, or the concatenated parts.
    pub body_ct_len: u64,
}

/// Parse and MAC-verify the trailer from `tail` = the last `min(object_len, MAX_TAIL_LEN)` bytes of
/// the object at `object_key`. `None` ⇒ not a valid hypha trailer: foreign, truncated, tampered,
/// or a composite whose parts don't tile to the stamped plaintext length.
pub fn decode_tail(
    key: &TrailerKey,
    object_key: &str,
    object_len: u64,
    tail: &[u8],
) -> Option<Tail> {
    let n = tail.len();
    if n < FACTS_LEN + TAG_LEN + VERSION_LEN {
        return None;
    }
    // version ‖ tag ‖ facts sit at fixed offsets from the end.
    let ver_off = n - VERSION_LEN;
    if u16::from_le_bytes(tail[ver_off..].try_into().ok()?) != VERSION {
        return None;
    }
    let tag_off = ver_off - TAG_LEN;
    let stored_tag = &tail[tag_off..ver_off];
    let facts_off = tag_off - FACTS_LEN;
    let facts_bytes = &tail[facts_off..tag_off];
    let footer = Footer::from_repr(bytemuck::try_from_bytes::<FactsRepr>(facts_bytes).ok()?)?;

    let table_len = match footer.kind {
        FooterKind::Single => 0,
        FooterKind::Composite => (footer.count as usize).checked_mul(PART_ENTRY_LEN)?,
    };
    if facts_off < table_len {
        return None; // table not fully present in the tail buffer
    }
    let table_bytes = &tail[facts_off - table_len..facts_off];

    let trailer_total = (table_len + FACTS_LEN + TAG_LEN + VERSION_LEN) as u64;
    let body_ct_len = object_len.checked_sub(trailer_total)?;

    let expect = compute_tag(
        key,
        VERSION,
        object_key,
        body_ct_len,
        facts_bytes,
        table_bytes,
    );
    if expect[..].ct_eq(stored_tag).unwrap_u8() != 1 {
        return None;
    }

    // Composite geometry: strictly monotonic windows that tile to the stamped plaintext length.
    let mut windows = Vec::new();
    let mut plens = Vec::new();
    if footer.kind == FooterKind::Composite {
        if footer.count == 0 {
            return None;
        }
        let mut prev = 0u64;
        let mut sum_plen = 0u64;
        for entry in table_bytes.chunks_exact(PART_ENTRY_LEN) {
            let end = u64::from_le_bytes(entry.try_into().ok()?);
            if end <= prev || end > body_ct_len {
                return None;
            }
            let plen = plaintext_len_from(end - prev, HLEN)?;
            sum_plen = sum_plen.checked_add(plen)?;
            windows.push(prev..end);
            plens.push(plen);
            prev = end;
        }
        if prev != body_ct_len || sum_plen != footer.plen {
            return None;
        }
    }

    Some(Tail {
        footer,
        windows,
        plens,
        body_ct_len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset::{ciphertext_len, CHUNK_PLAINTEXT};

    fn key() -> TrailerKey {
        TrailerKey::derive("a-256-bit-random-master-passphrase-stand-in")
    }

    fn single(plen: u64) -> Footer {
        Footer {
            kind: FooterKind::Single,
            count: 1,
            plen,
            mtime_ms: 1_700_000_000_000,
            md5: [0xab; 16],
        }
    }

    #[test]
    fn facts_size_is_minimal_and_unpadded() {
        // 1 + 4 + 8 + 8 + 16, align 1 ⇒ no padding.
        assert_eq!(FACTS_LEN, 37);
    }

    #[test]
    fn single_roundtrip() {
        let k = key();
        let body_ct = ciphertext_len(500, HLEN);
        let f = single(500);
        let blob = encode_trailer(&k, "obj/one", body_ct, &f, &[]);
        let object_len = body_ct + blob.len() as u64;

        let tail = decode_tail(&k, "obj/one", object_len, &blob).expect("verifies");
        assert_eq!(tail.footer, f);
        assert_eq!(tail.body_ct_len, body_ct);
        assert!(tail.windows.is_empty());
    }

    #[test]
    fn composite_roundtrip_and_tiling() {
        let k = key();
        // three ragged parts
        let plens = [5 * CHUNK_PLAINTEXT + 3, CHUNK_PLAINTEXT, 42];
        let mut table = Vec::new();
        let mut acc = 0u64;
        for &p in &plens {
            acc += ciphertext_len(p, HLEN);
            table.push(acc);
        }
        let body_ct = acc;
        let f = Footer {
            kind: FooterKind::Composite,
            count: plens.len() as u32,
            plen: plens.iter().sum(),
            mtime_ms: 1,
            md5: [0x11; 16],
        };
        let blob = encode_trailer(&k, "obj/multi", body_ct, &f, &table);
        let object_len = body_ct + blob.len() as u64;

        let tail = decode_tail(&k, "obj/multi", object_len, &blob).expect("verifies");
        assert_eq!(tail.footer, f);
        assert_eq!(tail.plens, plens);
        assert_eq!(tail.windows.len(), 3);
        assert_eq!(tail.windows[0].start, 0);
        assert_eq!(tail.windows.last().unwrap().end, body_ct);
    }

    #[test]
    fn tamper_and_foreign_fail_to_verify() {
        let k = key();
        let body_ct = ciphertext_len(500, HLEN);
        let blob = encode_trailer(&k, "obj/one", body_ct, &single(500), &[]);
        let object_len = body_ct + blob.len() as u64;

        // wrong key
        let other = TrailerKey::derive("a-different-passphrase");
        assert!(decode_tail(&other, "obj/one", object_len, &blob).is_none());
        // wrong object key (AAD binding)
        assert!(decode_tail(&k, "obj/two", object_len, &blob).is_none());
        // flipped facts byte
        let mut bad = blob.clone();
        let i = bad.len() - VERSION_LEN - TAG_LEN - 1;
        bad[i] ^= 0x01;
        assert!(decode_tail(&k, "obj/one", object_len, &bad).is_none());
        // foreign bytes
        assert!(decode_tail(&k, "obj/one", 999, &vec![0u8; MAX_TAIL_LEN]).is_none());
    }

    #[test]
    fn client_etag_forms() {
        let mut f = single(0);
        assert_eq!(f.client_etag(), "ab".repeat(16));
        f.kind = FooterKind::Composite;
        f.count = 3;
        assert_eq!(f.client_etag(), format!("{}-3", "ab".repeat(16)));
    }

    /// The reason decrypt paths bound their reads before the trailer: age's reader is
    /// EOF-delimited, so trailing bytes get pulled into the final chunk and fail authentication.
    /// If a future age becomes trailing-tolerant this starts failing and the bounds become removable.
    #[test]
    fn trailing_bytes_break_decryption() {
        use std::io::{Read, Write};

        let env = crate::Envelope::new("trailing-bytes test passphrase").unwrap();
        for plen in [100usize, 64 * 1024] {
            let plaintext = vec![7u8; plen];
            let mut ct = Vec::new();
            let mut w = env.encrypt(&mut ct).unwrap();
            w.write_all(&plaintext).unwrap();
            w.finish().unwrap();
            let ct_len = ct.len();
            ct.extend_from_slice(&[0u8; FACTS_LEN + TAG_LEN + VERSION_LEN]);

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
