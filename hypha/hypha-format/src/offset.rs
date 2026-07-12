//! Closed-form plaintext-byte ⇄ ciphertext-byte arithmetic for the age v1 payload.
//!
//! Layout of an age file: `header ‖ payload_nonce(16) ‖ chunk₀ ‖ chunk₁ ‖ …` where every chunk
//! is 64 KiB of plaintext + a 16-byte Poly1305 tag, except the last (shorter, never empty —
//! unless the whole plaintext is empty, which still produces one empty chunk). age's v1 spec
//! makes the scrypt stanza a file's sole stanza, so age never greases a hypha header and its
//! length is the compile-time constant [`HLEN`] — every mapping below is pure arithmetic.

use std::ops::Range;

pub const CHUNK_PLAINTEXT: u64 = 64 * 1024;
pub const TAG: u64 = 16;
pub const CHUNK_CIPHERTEXT: u64 = CHUNK_PLAINTEXT + TAG;
pub const PAYLOAD_NONCE: u64 = 16;

/// The age v1 header length for hypha's pinned scrypt recipient. Constant because a scrypt stanza
/// is spec-required to be a file's sole stanza, so age emits no grease and the stanza is
/// fixed-shape (16-byte salt → 22 b64 chars, pinned work factor). Pinned by the `hlen_is_constant`
/// test in `envelope.rs`; a future age that changes it trips that test ⇒ bump the trailer version.
pub const HLEN: u64 = 149;

pub fn chunk_count(plaintext_len: u64) -> u64 {
    if plaintext_len == 0 {
        1 // empty plaintext is one empty (tag-only) chunk, per spec
    } else {
        plaintext_len.div_ceil(CHUNK_PLAINTEXT)
    }
}

pub fn ciphertext_len(plaintext_len: u64, header_len: u64) -> u64 {
    header_len + PAYLOAD_NONCE + plaintext_len + chunk_count(plaintext_len) * TAG
}

/// Chunk holding plaintext byte `offset`.
pub fn chunk_of(offset: u64) -> u64 {
    offset / CHUNK_PLAINTEXT
}

/// Plaintext length from a file's total ciphertext length and its header length — the inverse of
/// [`ciphertext_len`], used to recover a composite part's `plen` from its ciphertext window and
/// [`HLEN`] (§6). Each full ciphertext chunk is `CHUNK_CIPHERTEXT` bytes and a trailing partial
/// chunk still carries a whole tag, so the chunk count falls out of the payload length alone.
/// Returns `None` if the pair is inconsistent (truncated object, or a wrong `header_len`).
pub fn plaintext_len_from(total_ciphertext_len: u64, header_len: u64) -> Option<u64> {
    let payload = total_ciphertext_len.checked_sub(header_len + PAYLOAD_NONCE)?;
    let chunks = payload.div_ceil(CHUNK_CIPHERTEXT).max(1);
    let plen = payload.checked_sub(chunks * TAG)?;
    // Reject payloads that don't correspond to any plaintext length (e.g. a partial chunk of
    // fewer than TAG+1 bytes).
    (ciphertext_len(plen, header_len) == total_ciphertext_len).then_some(plen)
}

pub fn chunk_ciphertext_offset(chunk: u64, header_len: u64) -> u64 {
    header_len + PAYLOAD_NONCE + chunk * CHUNK_CIPHERTEXT
}

/// Ciphertext byte range covering plaintext range `pt` — the byte-range GET to issue.
///
/// The range covers whole chunks (decryption authenticates per chunk) and is clamped to the
/// file's actual ciphertext end. Does **not** include the header: the caller fetches
/// `[0, header_len + PAYLOAD_NONCE)` separately (or coalesces when `pt.start` is in chunk 0).
pub fn ciphertext_range(pt: Range<u64>, plaintext_len: u64, header_len: u64) -> Range<u64> {
    debug_assert!(pt.start <= pt.end && pt.end <= plaintext_len);
    let start = chunk_ciphertext_offset(chunk_of(pt.start), header_len);
    let end = if pt.end == 0 {
        chunk_ciphertext_offset(0, header_len)
    } else {
        // pt.end is exclusive: the last byte read is pt.end - 1
        chunk_ciphertext_offset(chunk_of(pt.end - 1) + 1, header_len)
    };
    start..end.min(ciphertext_len(plaintext_len, header_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_counts() {
        assert_eq!(chunk_count(0), 1);
        assert_eq!(chunk_count(1), 1);
        assert_eq!(chunk_count(CHUNK_PLAINTEXT), 1);
        assert_eq!(chunk_count(CHUNK_PLAINTEXT + 1), 2);
        assert_eq!(chunk_count(3 * CHUNK_PLAINTEXT), 3);
    }

    #[test]
    fn plaintext_len_inverts_ciphertext_len() {
        let h = 200;
        for plen in [
            0u64,
            1,
            CHUNK_PLAINTEXT - 1,
            CHUNK_PLAINTEXT,
            CHUNK_PLAINTEXT + 1,
            3 * CHUNK_PLAINTEXT + 10,
        ] {
            assert_eq!(plaintext_len_from(ciphertext_len(plen, h), h), Some(plen));
        }
        // Impossible payloads are rejected: a trailing partial chunk can't be tag-only-or-less
        // (byte-shaving truncation is *not* detectable here — that's the AEAD finalizer's job).
        assert_eq!(
            plaintext_len_from(h + PAYLOAD_NONCE + CHUNK_CIPHERTEXT + TAG, h),
            None
        );
        assert_eq!(plaintext_len_from(h, h), None);
    }

    #[test]
    fn ranges_cover_whole_chunks() {
        let h = 200;
        let len = 3 * CHUNK_PLAINTEXT + 10;
        // range within chunk 1
        let r = ciphertext_range(CHUNK_PLAINTEXT + 5..CHUNK_PLAINTEXT + 6, len, h);
        assert_eq!(r.start, chunk_ciphertext_offset(1, h));
        assert_eq!(r.end, chunk_ciphertext_offset(2, h));
        // range spanning the chunk-2/3 boundary, clamped at the short final chunk
        let r = ciphertext_range(3 * CHUNK_PLAINTEXT - 1..len, len, h);
        assert_eq!(r.start, chunk_ciphertext_offset(2, h));
        assert_eq!(r.end, ciphertext_len(len, h));
    }
}
