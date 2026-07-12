//! The cache-side structures plaintext facts travel through (§6): tombstones, facts twins, and
//! the reserved-prefix records — plus the composite-ETag arithmetic and key admission.
//!
//! The *remote* carrier of an object's facts is the framed footer behind its age ciphertext
//! (`hypha_format::footer`), landed atomically with every commit; nothing here is stamped onto
//! remote objects. The cache copies below (tombstone metadata, twins) are projections serving
//! steady-state HEAD/LIST without touching the remote.

/// User-metadata key names on cache objects. The SDK adds the `x-amz-meta-` prefix on the wire.
pub const PLEN: &str = "plen";
pub const CETAG: &str = "cetag";
/// Per-part plaintext MD5 on an mpu part record; input to the composite ETag (§6).
pub const PMD5: &str = "pmd5";
/// The *remote's* part ETag (ciphertext MD5) on an mpu part record — what the native
/// CompleteMultipartUpload needs back.
pub const RETAG: &str = "retag";
/// Marks a cache object as a tombstone — body is remote-only (§8). Value is the tombstone kind.
pub const TOMB: &str = "tomb";
/// Original client-write mtime (unix ms) on a tombstone — eviction must not move a key's
/// client-visible LastModified (§6).
pub const MTIME: &str = "mtime";

/// Tombstone kinds (value of the [`TOMB`] metadata key).
pub const TOMB_EVICT: &str = "evict";
pub const TOMB_DELETE: &str = "delete";
pub const TOMB_TRANSIT: &str = "transit";

/// Fixed 16-byte sentinel bodies, compiled in, one per tombstone kind, so a LIST classifies every
/// key from its (size, ETag) pair without a metadata read (§6). 16 bytes saturate an MD5 ETag's
/// entropy: a collision with a real client body needs a length match *and* a 2^-128 byte match.
pub const EVICT_SENTINEL: [u8; 16] = *b"hypha:evicted!!\x00";
/// Client-visibly absent (§6).
pub const DELETE_SENTINEL: [u8; 16] = *b"hypha:deleted!!\x00";
/// K is mid-bracket (§7): cache facts are distrusted and readers resolve K from the remote.
pub const TRANSIT_SENTINEL: [u8; 16] = *b"hypha:intransit\x00";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TombKind {
    Evict,
    Delete,
    Transit,
}

impl TombKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TombKind::Evict => TOMB_EVICT,
            TombKind::Delete => TOMB_DELETE,
            TombKind::Transit => TOMB_TRANSIT,
        }
    }

    pub fn sentinel(self) -> &'static [u8; 16] {
        match self {
            TombKind::Evict => &EVICT_SENTINEL,
            TombKind::Delete => &DELETE_SENTINEL,
            TombKind::Transit => &TRANSIT_SENTINEL,
        }
    }

    /// The sentinel's constant cache ETag — LIST's classification token and the `If-Match` CAS
    /// token eviction/rehydrate use (§6, §8).
    pub fn sentinel_etag(self) -> String {
        use md5::{Digest, Md5};
        hex::encode(Md5::digest(self.sentinel()))
    }
}

/// Classify a cache LIST entry from its (size, ETag) pair alone (§6). `None` ⇒ a live body.
pub fn classify_entry(size: i64, etag: &str) -> Option<TombKind> {
    if size != 16 {
        return None;
    }
    [TombKind::Evict, TombKind::Delete, TombKind::Transit]
        .into_iter()
        .find(|k| k.sentinel_etag() == etag)
}

/// Tombstone kind from an object's user-metadata (the HEAD-path classification).
pub fn tomb_kind(metadata: &std::collections::HashMap<String, String>) -> Option<TombKind> {
    match metadata.get(TOMB).map(String::as_str) {
        Some(TOMB_EVICT) => Some(TombKind::Evict),
        Some(TOMB_DELETE) => Some(TombKind::Delete),
        Some(TOMB_TRANSIT) => Some(TombKind::Transit),
        _ => None,
    }
}

/// Whether an object's user-metadata marks it a tombstone of any kind.
pub fn is_tombstone(metadata: &std::collections::HashMap<String, String>) -> bool {
    metadata.contains_key(TOMB)
}

pub fn evict_sentinel_etag() -> String {
    TombKind::Evict.sentinel_etag()
}

pub fn delete_sentinel_etag() -> String {
    TombKind::Delete.sentinel_etag()
}

pub fn transit_sentinel_etag() -> String {
    TombKind::Transit.sentinel_etag()
}

/// A composite's client ETag is `hash-N`; a single-part's is a bare MD5. The suffix is the
/// read path's composite dispatch (a composite body is per-part age files, not one).
pub fn is_composite_etag(cetag: &str) -> bool {
    cetag.contains('-')
}

// ── The reserved prefix (§6) ────────────────────────────────────────────────────────────────
//
// Everything hypha stores *about* objects — mpu state, and later the phase-4 pending markers —
// lives under one fixed 16-byte key prefix, excluded from client namespaces by key admission
// (below) rather than by an unrepresentable byte: reserved keys must survive XML LIST
// responses, which control bytes would not.

pub const RESERVED_PREFIX: &str = ".hypha-reserved/";

pub fn is_reserved_key(key: &str) -> bool {
    key.starts_with(RESERVED_PREFIX)
}

/// Cache: an upload's own record — the client key as the body (keys may exceed what an ASCII
/// metadata header can carry).
pub fn mpu_upload_key(upload_id: &str) -> String {
    format!("{RESERVED_PREFIX}mpu/{upload_id}/u")
}

/// Cache: per-part facts `{pmd5, plen, retag}`, written as each part completes (§7). Zero-padded
/// so a LIST of [`mpu_prefix`] yields parts in order.
pub fn mpu_part_key(upload_id: &str, part_number: i32) -> String {
    format!("{RESERVED_PREFIX}mpu/{upload_id}/p{part_number:05}")
}

/// Cache: everything recorded for one upload — dropped at complete/abort.
pub fn mpu_prefix(upload_id: &str) -> String {
    format!("{RESERVED_PREFIX}mpu/{upload_id}/")
}

// ── LIST facts twins (§6) ───────────────────────────────────────────────────────────────────
//
// A twin is a zero-byte cache object at `base_key ‖ 0x01 ‖ facts`. Because `0x01` sorts below
// every admissible client-key byte (see [`validate_client_key`]), a twin sorts immediately after
// its own key and before any longer key, so one LIST pass yields `(K, K's twin)` adjacent and can
// emit correct plaintext facts with no per-key HEAD. The facts live in the *key name* — the one
// field a raw LIST returns per entry. Only eviction tombstones need a twin: a live body carries
// its own plaintext size and client ETag natively in the cache.
//
// A twin applies **iff K's own entry classifies as an eviction tombstone** — next to anything
// else it is a crash-window leftover, ignored and swept. The gate is sound because every path
// that replaces an eviction tombstone passes through a live body or a transition mark first, so
// an eviction tombstone is never adjacent to another epoch's twin (§6).

/// Separator between a key and its twin's facts. Below `0x20`, so it can never appear in an
/// admissible client key and always sorts a twin right after its base key.
pub const TWIN_SEP: u8 = 0x01;

/// The facts a twin projects for LIST: exactly what LIST must emit for an evicted key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Facts {
    /// Client-visible ETag (single-part MD5 or composite `hash-N`).
    pub client_etag: String,
    pub plen: u64,
    /// Original client-write mtime, unix milliseconds — eviction must not move LastModified.
    pub mtime_ms: i64,
}

impl Facts {
    /// The twin's full key for a given base key. Fields are `;`-joined after the separator; none
    /// of them contain `;` or bytes `< 0x20`, so the encoding is unambiguous.
    pub fn twin_key(&self, base_key: &str) -> String {
        format!(
            "{base_key}{}{};{};{}",
            TWIN_SEP as char, self.client_etag, self.plen, self.mtime_ms
        )
    }
}

/// Split a full cache key into `(base_key, Facts)` if it is a twin, else `None`.
pub fn parse_twin(full_key: &str) -> Option<(&str, Facts)> {
    let sep = full_key.find(TWIN_SEP as char)?;
    let (base, rest) = full_key.split_at(sep);
    let mut it = rest[1..].splitn(3, ';');
    let facts = Facts {
        client_etag: it.next()?.to_string(),
        plen: it.next()?.parse().ok()?,
        mtime_ms: it.next()?.parse().ok()?,
    };
    Some((base, facts))
}

/// The raw digest half of the composite ETag: `md5(md5₀‖…‖md5ₙ)` over the ordered per-part
/// plaintext MD5s (§6) — what the object footer stores; the `-N` rides its `count` field.
pub fn composite_md5(part_md5s_hex: &[String]) -> Option<[u8; 16]> {
    use md5::{Digest, Md5};
    if part_md5s_hex.is_empty() {
        return None;
    }
    let mut hasher = Md5::new();
    for hexmd5 in part_md5s_hex {
        hasher.update(hex::decode(hexmd5).ok()?);
    }
    Some(hasher.finalize().into())
}

/// The S3-correct composite ETag `md5(md5₀‖…‖md5ₙ)-N` (§6). hypha composes this at
/// `CompleteMultipartUpload` — parts route around the cache, so nothing else can produce it.
pub fn composite_etag(part_md5s_hex: &[String]) -> Option<String> {
    Some(format!(
        "{}-{}",
        hex::encode(composite_md5(part_md5s_hex)?),
        part_md5s_hex.len()
    ))
}

/// hypha constrains client keys beyond stock S3 so the twin scheme sorts correctly and fits
/// (architecture § *S3 surface*): no bytes below `0x20` (so [`TWIN_SEP`] sorts below every key
/// byte), a length cap short of 1024 leaving suffix headroom for a twin, and no
/// [`RESERVED_PREFIX`] collisions.
pub fn validate_client_key(key: &str) -> Result<(), &'static str> {
    if key.len() > 900 {
        return Err("key too long (max 900 bytes, leaving twin-suffix headroom)");
    }
    if key.bytes().any(|b| b < 0x20) {
        return Err("key contains a control byte (< 0x20), disallowed by hypha");
    }
    if is_reserved_key(key) {
        return Err("key prefix is reserved by hypha");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn twin_roundtrips_and_sorts_after_base() {
        let f = Facts {
            client_etag: "ab".repeat(16),
            plen: 4096,
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
    fn sentinels_are_distinct_and_classify() {
        for kind in [TombKind::Evict, TombKind::Delete, TombKind::Transit] {
            assert_eq!(kind.sentinel().len(), 16);
            assert_eq!(classify_entry(16, &kind.sentinel_etag()), Some(kind));
        }
        assert_eq!(classify_entry(16, &"0".repeat(32)), None);
        assert_eq!(classify_entry(17, &TombKind::Evict.sentinel_etag()), None);
    }

    #[test]
    fn composite_etag_has_part_count_suffix() {
        let e = composite_etag(&["ab".repeat(16), "cd".repeat(16)]).unwrap();
        assert!(e.ends_with("-2"));
        assert!(is_composite_etag(&e));
        assert!(!is_composite_etag(&"ab".repeat(16)));
        assert!(composite_etag(&[]).is_none());
    }

    #[test]
    fn key_admission() {
        assert!(validate_client_key("normal/key.txt").is_ok());
        assert!(validate_client_key("bad\x01key").is_err());
        assert!(validate_client_key(&"x".repeat(1000)).is_err());
        assert!(validate_client_key(&mpu_upload_key("id")).is_err());
        assert!(validate_client_key(".hypha-reserved/anything").is_err());
    }

    #[test]
    fn reserved_prefix_is_16_bytes() {
        assert_eq!(RESERVED_PREFIX.len(), 16);
    }
}
