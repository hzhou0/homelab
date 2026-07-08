//! What hypha stamps onto each remote object so a cold read / restore can reconstruct S3-correct
//! plaintext facts — the remote body is age ciphertext, so its own Content-Length and ETag
//! describe ciphertext, not what the client PUT.
//!
//! Two shapes, because single-part routes *through* the cache and multipart routes *around* it:
//! - **Single-part body** (through the cache, which computed the ETag): [`PLEN`] + [`CETAG`],
//!   read only by the §7 restore sweep — steady-state HEAD/LIST/GET come from the cache.
//! - **Multipart part object** (around the cache — one age file per part, §7): [`PLEN`] (the
//!   part's plaintext length) + [`PMD5`] (the part's plaintext MD5). No single cache PutObject
//!   reproduces the composite ETag `md5(concat part md5s)-N`, so hypha composes it from these at
//!   `CompleteMultipartUpload` ([`composite_etag`]). The composite is virtual — a ranged GET
//!   derives part boundaries from each part's own `{plen, ct_len}` (§6), so there is no stored
//!   composite part table.

/// User-metadata key names. The SDK adds the `x-amz-meta-` prefix on the wire.
pub const PLEN: &str = "plen";
pub const CETAG: &str = "cetag";
/// Per-part plaintext MD5 on a multipart part object; input to the composite ETag (§6).
pub const PMD5: &str = "pmd5";
/// Marks a cache object as a tombstone — body is remote-only (§8). Value is the tombstone kind.
pub const TOMB: &str = "tomb";

/// Tombstone kinds (value of the [`TOMB`] metadata key).
pub const TOMB_EVICT: &str = "evict";
pub const TOMB_DELETE: &str = "delete";

/// Fixed 16-byte sentinel body an eviction tombstone carries, so a LIST classifies it from its
/// (size, ETag) pair without a metadata read (§6). 16 bytes saturate an MD5 ETag's entropy, so a
/// collision with a real client body needs both a length and a 2^-128 byte match.
pub const EVICT_SENTINEL: [u8; 16] = *b"hypha:evicted!!\x00";
/// Distinct sentinel for delete tombstones (client-visibly absent, §6).
pub const DELETE_SENTINEL: [u8; 16] = *b"hypha:deleted!!\x00";

/// Whether an object's user-metadata marks it a tombstone of any kind.
pub fn is_tombstone(metadata: &std::collections::HashMap<String, String>) -> bool {
    metadata.contains_key(TOMB)
}

/// Hex MD5 of the eviction sentinel body — the cache ETag every eviction tombstone carries.
/// Constant, so it's the `bound_etag` a facts twin binds to and the `If-Match` token a
/// conditional tombstone/rehydrate uses (§6, §8).
pub fn evict_sentinel_etag() -> String {
    use md5::{Digest, Md5};
    hex::encode(Md5::digest(EVICT_SENTINEL))
}

/// Hex MD5 of the delete sentinel body — lets a LIST classify a delete-tombstone (which it omits)
/// from the (size, ETag) pair alone, no metadata read (§6).
pub fn delete_sentinel_etag() -> String {
    use md5::{Digest, Md5};
    hex::encode(Md5::digest(DELETE_SENTINEL))
}

// ── LIST facts twins (§6) ───────────────────────────────────────────────────────────────────
//
// A twin is a zero-byte cache object at `base_key ‖ 0x01 ‖ facts`. Because `0x01` sorts below
// every admissible client-key byte (see [`validate_client_key`]), a twin sorts immediately after
// its own key and before any longer key, so one LIST pass yields `(K, K's twin)` adjacent and can
// emit correct plaintext facts with no per-key HEAD. The facts live in the *key name* — the one
// field a raw LIST returns per entry. Only tombstones need a twin: a live body (single-part or a
// cached composite) carries its own plaintext size and client ETag natively in the cache.

/// Separator between a key and its twin's facts. Below `0x20`, so it can never appear in an
/// admissible client key and always sorts a twin right after its base key.
pub const TWIN_SEP: u8 = 0x01;

/// What a twin projects for LIST. `bound_etag` ties the twin to a specific state of its base key:
/// the twin applies only if the base `K` entry's actual ETag equals it (else the twin is a stale
/// crash-window leftover → HEAD fallback + sweep).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Facts {
    /// The tombstone kind this twin projects for ([`TOMB_EVICT`]).
    pub kind: String,
    pub bound_etag: String,
    pub plen: u64,
    /// Client-visible ETag (single-part MD5 or composite `hash-N`).
    pub client_etag: String,
    /// Original mtime, unix milliseconds.
    pub mtime_ms: i64,
}

impl Facts {
    /// The twin's full key for a given base key. Fields are `;`-joined after the separator; none
    /// of them contain `;` or bytes `< 0x20`, so the encoding is unambiguous.
    pub fn twin_key(&self, base_key: &str) -> String {
        format!("{base_key}{}{}", TWIN_SEP as char, self.encode())
    }

    fn encode(&self) -> String {
        format!(
            "{};{};{};{};{}",
            self.kind, self.bound_etag, self.plen, self.client_etag, self.mtime_ms
        )
    }
}

/// Split a full cache key into `(base_key, Facts)` if it is a twin, else `None`.
pub fn parse_twin(full_key: &str) -> Option<(&str, Facts)> {
    let sep = full_key.find(TWIN_SEP as char)?;
    let (base, rest) = full_key.split_at(sep);
    let encoded = &rest[1..]; // skip the separator
    let mut it = encoded.splitn(5, ';');
    let facts = Facts {
        kind: it.next()?.to_string(),
        bound_etag: it.next()?.to_string(),
        plen: it.next()?.parse().ok()?,
        client_etag: it.next()?.to_string(),
        mtime_ms: it.next()?.parse().ok()?,
    };
    Some((base, facts))
}

/// The S3-correct composite ETag from the ordered per-part plaintext MD5s: `md5(md5₀‖…‖md5ₙ)-N`
/// (§6). hypha composes this at `CompleteMultipartUpload` — parts route around the cache, so
/// nothing else can produce it.
pub fn composite_etag(part_md5s_hex: &[String]) -> Option<String> {
    use md5::{Digest, Md5};
    if part_md5s_hex.is_empty() {
        return None;
    }
    let mut hasher = Md5::new();
    for hexmd5 in part_md5s_hex {
        hasher.update(hex::decode(hexmd5).ok()?);
    }
    Some(format!("{}-{}", hex::encode(hasher.finalize()), part_md5s_hex.len()))
}

/// hypha constrains client keys beyond stock S3 so the twin scheme sorts correctly and fits
/// (architecture § *S3 surface*): no bytes below `0x20` (so [`TWIN_SEP`] sorts below every key
/// byte) and a length cap short of 1024 leaving suffix headroom for a twin.
pub fn validate_client_key(key: &str) -> Result<(), &'static str> {
    if key.len() > 900 {
        return Err("key too long (max 900 bytes, leaving twin-suffix headroom)");
    }
    if key.bytes().any(|b| b < 0x20) {
        return Err("key contains a control byte (< 0x20), disallowed by hypha");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn twin_roundtrips_and_sorts_after_base() {
        let f = Facts {
            kind: TOMB_EVICT.to_string(),
            bound_etag: evict_sentinel_etag(),
            plen: 4096,
            client_etag: "ab".repeat(16),
            mtime_ms: 1_700_000_000_000,
        };
        let tk = f.twin_key("dir/obj");
        let (base, decoded) = parse_twin(&tk).unwrap();
        assert_eq!(base, "dir/obj");
        assert_eq!(decoded, f);
        // Twin sorts after its base key and before any longer admissible key (0x01 < 0x20).
        assert!(tk.as_str() > "dir/obj");
        assert!(tk.as_str() < "dir/obj\x20");
        assert!(tk.as_str() < "dir/obj/child");
    }

    #[test]
    fn composite_etag_has_part_count_suffix() {
        let e = composite_etag(&["ab".repeat(16), "cd".repeat(16)]).unwrap();
        assert!(e.ends_with("-2"));
        assert!(composite_etag(&[]).is_none());
    }

    #[test]
    fn key_admission() {
        assert!(validate_client_key("normal/key.txt").is_ok());
        assert!(validate_client_key("bad\x01key").is_err());
        assert!(validate_client_key(&"x".repeat(1000)).is_err());
    }
}
