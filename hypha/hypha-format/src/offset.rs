//! Closed-form plaintext-byte ⇄ ciphertext-byte arithmetic for the age v1 payload.
//!
//! Layout of an age file: `header ‖ payload_nonce(16) ‖ chunk₀ ‖ chunk₁ ‖ …` where every chunk
//! is 64 KiB of plaintext + a 16-byte Poly1305 tag, except the last (shorter, never empty —
//! unless the whole plaintext is empty, which still produces one empty chunk). The header
//! length is constant per *file* but not across files (rage greases headers with a random
//! stanza), so it is a per-object fact: recorded in object metadata at upload, or recovered
//! from the ciphertext prefix via [`parse_header_len`]. With it, all mappings below are pure
//! arithmetic — no per-chunk lookup table.

use std::ops::Range;

pub const CHUNK_PLAINTEXT: u64 = 64 * 1024;
pub const TAG: u64 = 16;
pub const CHUNK_CIPHERTEXT: u64 = CHUNK_PLAINTEXT + TAG;
pub const PAYLOAD_NONCE: u64 = 16;

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

/// Header length derived from the plaintext length and the total ciphertext length —
/// no stored metadata and no prefix read: `ct_len − nonce − plen − tag·chunks`.
///
/// This is what makes the varying header (rage greases it with a random stanza) costless:
/// hypha already knows `plen` (it must serve plaintext sizes on HEAD) and `ct_len` is the
/// object's Content-Length. Returns `None` if the pair is inconsistent (truncated object).
pub fn header_len_from(plaintext_len: u64, total_ciphertext_len: u64) -> Option<u64> {
    let payload = PAYLOAD_NONCE + plaintext_len + chunk_count(plaintext_len) * TAG;
    total_ciphertext_len.checked_sub(payload)
}

/// Header length parsed from a ciphertext prefix, or `None` if the prefix is too short.
/// Validation / fallback for [`header_len_from`] when lengths aren't at hand.
///
/// The header is ASCII lines terminated by the MAC line `--- <mac>`; stanza bodies are base64
/// (no `-`), and stanza openers start with `-> `, so `\n--- ` matches only the MAC line.
pub fn parse_header_len(prefix: &[u8]) -> Option<u64> {
    let mac = prefix.windows(5).position(|w| w == b"\n--- ")? + 1;
    let end = mac + prefix[mac..].iter().position(|&b| b == b'\n')? + 1;
    Some(end as u64)
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
