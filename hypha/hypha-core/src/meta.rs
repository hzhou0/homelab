//! The cache-side structures plaintext facts travel through (§6): tombstones, facts twins, and
//! the reserved-prefix records — plus the composite-ETag arithmetic and key admission.
//!
//! The *remote* carrier of an object's facts is the authenticated trailer behind its age
//! ciphertext (`hypha_format::trailer`), landed atomically with every commit; nothing here is
//! stamped onto remote objects. The cache copies below (tombstone metadata, twins) are
//! projections serving steady-state HEAD/LIST without touching the remote.

/// User-metadata key names on cache objects. The SDK adds the `x-amz-meta-` prefix on the wire.
pub const PLEN: &str = "plen";
pub const CETAG: &str = "cetag";
/// Marks a cache object as a tombstone — body is remote-only (§8). Value is the tombstone kind.
pub const TOMB: &str = "tomb";
/// Original client-write mtime (unix ms) on a tombstone — eviction must not move a key's
/// client-visible LastModified (§6).
pub const MTIME: &str = "mtime";
/// Echoed storage class (§7). hypha has one physical tier, so the class is a label the write path
/// records and the read path replays; absent ⇒ [`STANDARD`].
pub const SCLASS: &str = "sc";

pub const STANDARD: &str = "STANDARD";

/// Tombstone kinds (value of the [`TOMB`] metadata key).
pub const TOMB_EVICT: &str = "evict";
pub const TOMB_DELETE: &str = "delete";
pub const TOMB_TRANSIT: &str = "transit";

/// Fixed 16-byte sentinel bodies, compiled in, one per tombstone kind, so a LIST classifies every
/// key from its (size, ETag) pair without a metadata read (§6). Random 16-byte values so no client
/// body collides with the classification token by accident; stable by contract (they are the
/// on-disk classification).
pub const EVICT_SENTINEL: [u8; 16] = [
    0xe4, 0x80, 0xae, 0x85, 0xd6, 0xe7, 0x58, 0x9c, 0x7e, 0x07, 0xb5, 0xa5, 0xac, 0x39, 0x37, 0xaa,
];
/// Client-visibly absent (§6).
pub const DELETE_SENTINEL: [u8; 16] = [
    0x64, 0x58, 0x6a, 0xf5, 0x7f, 0xc3, 0xf6, 0x22, 0xf3, 0x00, 0xd3, 0xbb, 0x42, 0xb8, 0x72, 0x6d,
];
/// K is mid-bracket (§7): cache facts are distrusted and readers resolve K from the remote.
pub const TRANSIT_SENTINEL: [u8; 16] = [
    0xd9, 0xa5, 0xc8, 0x7a, 0x7c, 0x7e, 0x03, 0xc8, 0x04, 0x6c, 0x1a, 0xbf, 0x7c, 0x49, 0x0c, 0x65,
];

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

// ── Client user-metadata, namespaced (§7) ───────────────────────────────────────────────────
//
// A client's `x-amz-meta-*` and hypha's own facts share one carrier — the cache object's
// user-metadata — so they need a namespace split, or a client key named `plen` would shadow the
// tombstone's. hypha's keys stay bare and the client's ride under [`USER_PREFIX`], which is not a
// prefix of any hypha key. Only the cache holds them: the remote's sole facts carrier is the
// trailer (§6), so a repair or restore that rebuilds K from the remote drops the user metadata and
// the storage class back to their defaults — the accepted durability limit of this carrier.

/// Namespace for pass-through client metadata on a cache object.
pub const USER_PREFIX: &str = "u-";

/// Client metadata values are percent-encoded at rest so a non-ASCII or control byte survives the
/// backend's own header round trip byte-exact. (The *client* wire leg is RFC 2047, which s3s
/// encodes and decodes for us — hypha only ever sees decoded values.) Escaping everything outside
/// `[A-Za-z0-9]` covers `%` itself, so the encoding is unambiguous.
const META_ESCAPE: &percent_encoding::AsciiSet = percent_encoding::NON_ALPHANUMERIC;

/// Client `x-amz-meta-*` entries → the cache object's namespaced user-metadata.
pub fn encode_user_metadata(
    client: &std::collections::HashMap<String, String>,
) -> impl Iterator<Item = (String, String)> + '_ {
    client.iter().map(|(k, v)| {
        (
            format!("{USER_PREFIX}{k}"),
            percent_encoding::utf8_percent_encode(v, META_ESCAPE).to_string(),
        )
    })
}

/// The inverse: a cache object's user-metadata → the client `x-amz-meta-*` entries it carries.
/// hypha's own keys don't carry the prefix, so they drop out.
pub fn decode_user_metadata(
    stored: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    stored
        .iter()
        .filter_map(|(k, v)| {
            let name = k.strip_prefix(USER_PREFIX)?;
            let val = percent_encoding::percent_decode_str(v).decode_utf8().ok()?;
            Some((name.to_string(), val.into_owned()))
        })
        .collect()
}

/// The storage class recorded on a cache object, defaulting to [`STANDARD`] — the value for
/// anything written before the class was tracked, and for a key rebuilt from the remote.
pub fn storage_class(metadata: &std::collections::HashMap<String, String>) -> String {
    metadata
        .get(SCLASS)
        .cloned()
        .unwrap_or_else(|| STANDARD.to_string())
}

// ── The reserved prefix (§6) ────────────────────────────────────────────────────────────────
//
// Everything hypha stores *about* objects — mpu state, and later the phase-4 pending markers —
// lives under one fixed reserved key prefix. It leads with U+10FFFD — the **highest
// interchange-safe** codepoint (the last plane-16 private-use char, just below the U+10FFFE/F
// noncharacters that XML serializers may reject) — so the reserved keyspace sorts above every
// client key except one starting with exactly U+10FFFD, clustering at the **end** of a LIST
// instead of interleaving a client's scan. This is efficiency, not correctness: LIST filters
// reserved keys per-entry (`is_reserved_key`), so a client that does use such keys just scans less
// efficiently there. The base64url tail (~128 random bits) is what actually prevents collision —
// a client can't land on the reserved keyspace without guessing it. Stable by contract (existing
// reserved keys carry it).

pub const RESERVED_PREFIX: &str = "\u{10FFFD}3uYGNMVZsD97OWTV3oBHrd/";

pub fn is_reserved_key(key: &str) -> bool {
    key.starts_with(RESERVED_PREFIX)
}

/// Cache: an upload's own record — the client key as the body (keys may exceed what an ASCII
/// metadata header can carry).
pub fn mpu_upload_key(upload_id: &str) -> String {
    format!("{RESERVED_PREFIX}mpu/{upload_id}/u")
}

/// The facts an mpu part record carries in its key (§6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MpuPart<'a> {
    pub part_number: i32,
    /// The remote's ciphertext part ETag — the last-write-wins token complete matches against
    /// `ListParts`.
    pub retag: &'a str,
    /// The part's *plaintext* MD5, the one datum the remote can't reproduce.
    pub pmd5: &'a str,
    /// Names this part's retained ciphertext ([`mpu_stash_key`]); empty when it wasn't retained.
    pub stash_nonce: &'a str,
}

/// Cache: per-part record for a multipart upload, its facts encoded **in the key** so complete
/// recovers them with one LIST and no per-part HEAD (§7). A re-uploaded part writes a *new* key,
/// and the stale one is resolved away at complete by the remote's `ListParts`. `retag` and `pmd5`
/// are hex and `stash_nonce` is base64url, so none contain `;` or a control byte and the
/// `;`-delimited form is unambiguous; the zero-padded number keeps LIST order and lets
/// [`parse_mpu_part`] reject the `/u` upload record.
pub fn mpu_part_key(upload_id: &str, part: MpuPart<'_>) -> String {
    format!(
        "{RESERVED_PREFIX}mpu/{upload_id}/p{:05};{};{};{}",
        part.part_number,
        part.retag.trim_matches('"'),
        part.pmd5,
        part.stash_nonce
    )
}

/// Parse an mpu part record key; `None` for the upload's own `/u` record, a retained-ciphertext
/// `c` record, or a malformed key. Reads only the final path segment, so a full or
/// deployment-stripped key both work.
pub fn parse_mpu_part(key: &str) -> Option<MpuPart<'_>> {
    let mut it = key.rsplit('/').next()?.strip_prefix('p')?.splitn(4, ';');
    let part_number: i32 = it.next()?.parse().ok()?;
    let retag = it.next()?;
    let pmd5 = it.next()?;
    let stash_nonce = it.next()?;
    (!pmd5.is_empty()).then_some(MpuPart {
        part_number,
        retag,
        pmd5,
        stash_nonce,
    })
}

/// Cache: retained ciphertext of a part that **admits no successor** — one below the backend's
/// 5 MiB part minimum (which any S3 backend permits only as the upload's *final* part), or part
/// [`MAX_CLIENT_PART`] (which no number can follow). Either way such a part, if committed, is the
/// object's tail, so it is the one that must carry the terminating trailer; complete re-uploads it
/// as `part ‖ trailer` (§7) and needs the ciphertext back to do so, because an in-progress part
/// isn't readable.
///
/// Keyed by a **nonce** rather than the part's `retag`: this write is fed by a split of the very
/// stream going to the remote, so it starts long before the remote returns an ETag to key it by.
/// The winner→stash mapping instead
/// rides [`MpuPart::stash_nonce`] on the part record, which already disambiguates re-uploads — so
/// concurrent writes each retain under their own nonce and complete folds *exactly* the remote's
/// `ListParts` winner, never a divergent cache last-writer. Prefix `c` — distinct from `p` records
/// and the `/u` upload record — so [`parse_mpu_part`] skips it and it is swept with the rest of the
/// `mpu/<id>/` range at complete/abort.
pub fn mpu_stash_key(upload_id: &str, part_number: i32, nonce: &str) -> String {
    format!("{RESERVED_PREFIX}mpu/{upload_id}/c{part_number:05};{nonce}")
}

/// Highest part number a client may use — S3's own limit, which hypha does not reduce (§7).
pub const MAX_CLIENT_PART: i32 = 10_000;

/// Whether a part **admits no successor**, so that committing it makes it the object's final part.
/// Two conditions, one meaning: S3 exempts only the last part from the 5 MiB minimum, and nothing
/// follows part [`MAX_CLIENT_PART`]. This single predicate drives both decisions that must agree —
/// UploadPart retains such a part's ciphertext ([`mpu_stash_key`]), and complete folds the trailer
/// into it instead of appending a trailer part of its own (§7).
pub fn admits_no_successor(part_number: i32, ct_len: u64, min_remote_part: u64) -> bool {
    ct_len < min_remote_part || part_number >= MAX_CLIENT_PART
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

/// Separator between a key and its twin's facts. Forbidden in client keys (along with `0x00`), so
/// it can never appear in one and always sorts a twin right after its base key. `0x01` (not `0x00`)
/// because NUL is a string-terminator hazard across backends; the LIST round-trip carries it via
/// `encoding-type=url` (`Backend::list`), since `0x01` is not a valid XML character.
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

/// hypha constrains client keys beyond stock S3 only where the twin scheme needs it (architecture
/// § *S3 surface*): no `0x00` or `0x01` — `0x01` is [`TWIN_SEP`], and both sort at or below it, so
/// allowing them could place a client key between a base key and its twin — a length cap short of
/// 1024 leaving twin-suffix headroom, and no [`RESERVED_PREFIX`] collisions. Other control bytes
/// are fine: LIST rides `encoding-type=url` (see `Backend::list`), so any byte round-trips.
pub fn validate_client_key(key: &str) -> Result<(), &'static str> {
    if key.len() > 900 {
        return Err("key too long (max 900 bytes, leaving twin-suffix headroom)");
    }
    if key.bytes().any(|b| b <= TWIN_SEP) {
        return Err("key contains a 0x00 or 0x01 byte, reserved by hypha for the twin separator");
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
    fn user_metadata_namespace_roundtrips() {
        let mut client = std::collections::HashMap::new();
        client.insert("colour".to_string(), "café ☕".to_string());
        client.insert("plain".to_string(), "value".to_string());

        // hypha's own facts share the carrier and must survive untouched, unread as client keys.
        let mut stored: std::collections::HashMap<String, String> =
            encode_user_metadata(&client).collect();
        stored.insert(TOMB.to_string(), TOMB_EVICT.to_string());
        stored.insert(SCLASS.to_string(), "STANDARD_IA".to_string());

        assert_eq!(decode_user_metadata(&stored), client);
        assert_eq!(storage_class(&stored), "STANDARD_IA");
        // Percent-encoded at rest, so no backend header round trip can mangle a value.
        assert!(stored.values().all(|v| v.is_ascii()));

        // A client key colliding with a hypha key name stays namespaced apart.
        let mut shadow = std::collections::HashMap::new();
        shadow.insert(PLEN.to_string(), "99".to_string());
        let stored: std::collections::HashMap<String, String> =
            encode_user_metadata(&shadow).collect();
        assert!(!stored.contains_key(PLEN));
        assert_eq!(storage_class(&stored), STANDARD);
    }

    #[test]
    fn key_admission() {
        assert!(validate_client_key("normal/key.txt").is_ok());
        // Only 0x00 and 0x01 are reserved; other control bytes ride encoding-type=url.
        assert!(validate_client_key("tab\tand\x1fctrl").is_ok());
        assert!(validate_client_key("bad\x00key").is_err());
        assert!(validate_client_key("bad\x01key").is_err());
        assert!(validate_client_key(&"x".repeat(1000)).is_err());
        assert!(validate_client_key(&mpu_upload_key("id")).is_err());
        assert!(validate_client_key(&format!("{RESERVED_PREFIX}anything")).is_err());
    }

    #[test]
    fn reserved_prefix_sorts_after_practical_keys() {
        // Leads with U+10FFFD (highest interchange-safe codepoint), above every practical client
        // key, so the reserved keyspace clusters at the end of a LIST rather than interleaving.
        assert!(RESERVED_PREFIX.starts_with('\u{10FFFD}'));
        assert!(RESERVED_PREFIX.ends_with('/'));
        for k in [
            "",
            "zzz",
            "~~~",
            "\u{FFFF}tail",
            "\u{FFFFF}",
            "\u{FFFFF}zzzz",
        ] {
            assert!(
                k < RESERVED_PREFIX,
                "{k:?} must sort before the reserved prefix"
            );
            assert!(format!("{RESERVED_PREFIX}mpu/x").as_str() > k);
        }
        // Not an admission rule — a client *may* use plane-16 keys, just scanning less efficiently
        // there; only the reserved keyspace itself is off-limits, by prefix.
        assert!(validate_client_key("\u{100000}anything").is_ok());
        assert!(validate_client_key(&format!("{RESERVED_PREFIX}x")).is_err());
    }

    fn part(n: i32, retag: &'static str, pmd5: &'static str) -> MpuPart<'static> {
        MpuPart {
            part_number: n,
            retag,
            pmd5,
            stash_nonce: "",
        }
    }

    #[test]
    fn mpu_part_key_roundtrips_and_rejects_upload_record() {
        let (retag, pmd5): (&'static str, &'static str) =
            ("ab".repeat(16).leak(), "cd".repeat(16).leak());
        let k = mpu_part_key("up-1", part(7, retag, pmd5));
        assert_eq!(parse_mpu_part(&k), Some(part(7, retag, pmd5)));

        // Quoted remote ETags are normalized on the way in.
        let quoted = format!("\"{retag}\"");
        let kq = mpu_part_key(
            "up-1",
            MpuPart {
                retag: &quoted,
                ..part(42, retag, pmd5)
            },
        );
        assert_eq!(parse_mpu_part(&kq), Some(part(42, retag, pmd5)));

        // A retained part carries the nonce naming its ciphertext.
        let stashed = MpuPart {
            stash_nonce: "AAAA-nonce_1",
            ..part(10_000, retag, pmd5)
        };
        let ks = mpu_part_key("up-1", stashed);
        assert_eq!(parse_mpu_part(&ks), Some(stashed));
        assert_eq!(
            mpu_stash_key("up-1", 10_000, "AAAA-nonce_1"),
            format!("{RESERVED_PREFIX}mpu/up-1/c10000;AAAA-nonce_1")
        );
        // `c` records are not part records, so one LIST separates them by prefix alone.
        assert_eq!(parse_mpu_part(&mpu_stash_key("up-1", 10_000, "n")), None);

        // The upload's own record and malformed keys don't parse as parts.
        assert_eq!(parse_mpu_part(&mpu_upload_key("up-1")), None);
        assert_eq!(
            parse_mpu_part(&format!("{RESERVED_PREFIX}mpu/up-1/p00007")),
            None
        );
        // Records sort by part number under one LIST.
        assert!(
            mpu_part_key("up-1", part(2, retag, pmd5))
                < mpu_part_key("up-1", part(10, retag, pmd5))
        );
    }

    #[test]
    fn admits_no_successor_covers_both_terminal_conditions() {
        const MIN: u64 = 5 * 1024 * 1024;
        // Below the 5 MiB minimum: no backend accepts it as a non-final part.
        assert!(admits_no_successor(1, MIN - 1, MIN));
        // The last part number: nothing can follow it, whatever its size.
        assert!(admits_no_successor(MAX_CLIENT_PART, 4 << 30, MIN));
        // An ordinary interior part admits a successor, so the trailer gets its own.
        assert!(!admits_no_successor(1, MIN, MIN));
        assert!(!admits_no_successor(MAX_CLIENT_PART - 1, 4 << 30, MIN));
    }
}
