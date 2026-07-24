//! The cache-side structures plaintext facts travel through (§6): tombstones in the `<data>`
//! bucket, and — in the `<meta>` bucket, keyed apart by the two control bytes client keys may not
//! use — facts twins, pending markers, and mpu records. Plus the composite-ETag arithmetic and key
//! admission.
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
/// encodes and decodes for us — hypha only ever sees decoded values.)
///
/// The set is deliberately **narrow**: this carrier is capped at S3's 2 KB for all user metadata,
/// and hypha shares it with the client, so every byte the encoding adds is a byte of the client's
/// budget spent. Escaping everything outside `[A-Za-z0-9]` cost up to 3× and put hypha's effective
/// limit at roughly a third of S3's — an invisible conformance shortfall. Ordinary ASCII now passes
/// through unchanged and only genuinely unsafe bytes expand:
///
/// - **controls and `DEL`** — illegal in an HTTP header value,
/// - **space** — SigV4 canonicalization collapses runs of whitespace in a header value, so an
///   unescaped double space would sign differently than it transmits, and leading/trailing
///   whitespace is trimmed outright,
/// - **`%`** — what keeps the encoding self-delimiting.
///
/// Non-ASCII needs no entry: `utf8_percent_encode` always escapes bytes above `0x7F`.
const META_ESCAPE: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS.add(b' ').add(b'%');

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

// ── The `<meta>` bucket keyspace (§6) ─────────────────────────────────────────────────────────
//
// hypha's object-side state lives in a *separate* cache bucket (`<meta><b>`) from client bodies
// (`<data><b>`), so the client keyspace stays clean — no reserved prefix, no twin dilution, and a
// LIST page whose last key is always a client key (which is what makes v1's `NextMarker`
// expressible, §7). Within `<meta><b>`, two control bytes — both inadmissible in client keys
// ([`validate_client_key`]) — carve three non-interleaving ranges:
//
//   0x01 0x01 <tag> …       range A: mpu state (`m`), and phase-4/5 sync (`s`) / recency (`r`) /
//                                    shadow-body (`b`) records — prefix-scanned per tag.
//   0x01 <K> 0x01 <facts>   range B: facts twins. K's first byte is >= 0x02 (admission), so the
//                                    single 0x01 lead can never collide with range A's doubled one.
//   <K>                     range C: pending markers, **bare** — zero overhead, so every
//                                    admissible key has one (a marker is a durability signal, §6).
//
// The ranges sort A < B < C and never interleave, so the reconcile sweep reaches the markers with
// one flat `start_after` past the 0x01 block — `O(pending)`, never `O(evicted)` (§7).

/// The control byte both `<meta>` ranges and the twin separator are built from; forbidden in client
/// keys so the split is structural, not probabilistic.
pub const CTRL: u8 = 0x01;

/// Range-A tag for multipart-upload records.
const TAG_MPU: char = 'm';

/// The `0x01 0x01 m <upload-id> 0x01` prefix every record of one upload shares — a range-A prefix
/// scan yields the whole set, and a range delete sweeps it (§7).
fn mpu_range(upload_id: &str) -> String {
    format!("{c}{c}{TAG_MPU}{upload_id}{c}", c = CTRL as char)
}

/// Cache (`<meta>`): an upload's own record — the client key as the body (keys may exceed what an
/// ASCII metadata header can carry).
pub fn mpu_upload_key(upload_id: &str) -> String {
    format!("{}u", mpu_range(upload_id))
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

/// Cache (`<meta>`): per-part record for a multipart upload, its facts encoded **in the key** so
/// complete recovers them with one LIST and no per-part HEAD (§7). A re-uploaded part writes a
/// *new* key, and the stale one is resolved away at complete by the remote's `ListParts`. `retag`
/// and `pmd5` are hex and `stash_nonce` is base64url, so none contain `;` or a control byte and the
/// `;`-delimited form is unambiguous; the zero-padded number keeps LIST order and lets
/// [`parse_mpu_part`] reject the `u` upload record.
pub fn mpu_part_key(upload_id: &str, part: MpuPart<'_>) -> String {
    format!(
        "{}p{:05};{};{};{}",
        mpu_range(upload_id),
        part.part_number,
        part.retag.trim_matches('"'),
        part.pmd5,
        part.stash_nonce
    )
}

/// Parse an mpu part record key; `None` for the upload's own `u` record, a retained-ciphertext
/// `c` record, or a malformed key. Reads only the record segment after the final `0x01`, so the
/// upload id (which never contains `0x01`) can't be mistaken for it.
pub fn parse_mpu_part(key: &str) -> Option<MpuPart<'_>> {
    let mut it = key
        .rsplit(CTRL as char)
        .next()?
        .strip_prefix('p')?
        .splitn(4, ';');
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
/// and the `u` upload record — so [`parse_mpu_part`] skips it and it is swept with the rest of the
/// upload's range at complete/abort.
pub fn mpu_stash_key(upload_id: &str, part_number: i32, nonce: &str) -> String {
    format!("{}c{part_number:05};{nonce}", mpu_range(upload_id))
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

/// Cache (`<meta>`): everything recorded for one upload — dropped at complete/abort.
pub fn mpu_prefix(upload_id: &str) -> String {
    mpu_range(upload_id)
}

/// Cache (`<meta>`): a rehydrated composite's plaintext (cached mode, §6). Range-A tag `b`, keyed by
/// `sha256(K)[..160 bits]` rather than K — the access pattern is a point lookup, so the key can be
/// a hash, which lifts every length condition. SHA-256 (not the MD5 already in the tree) because a
/// collision here would serve another key's plaintext; a second independent digest of K rides the
/// shadow's metadata and is verified on read (§6).
pub fn shadow_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(key.as_bytes());
    format!("{c}{c}b{}", hex::encode(&digest[..20]), c = CTRL as char)
}

// ── LIST facts twins (§6) ───────────────────────────────────────────────────────────────────
//
// A twin is a zero-byte object in the `<meta>` bucket at `0x01 ‖ base_key ‖ 0x01 ‖ facts` (range B
// above). Both `0x01`s are inadmissible in client keys, which makes the twin range
// **order-isomorphic to the client keyspace**: for `A < B`, if `A` is a proper prefix of `B` the
// twins diverge where `twin(A)` holds `0x01` and `twin(B)` a byte >= 0x02, so `twin(A) < twin(B)`;
// otherwise they diverge on a shared byte. LIST therefore pairs twins to keys by a **merge join**
// over the client (`<data>`) cursor and this twin cursor, matched by base-key equality (§7) — not
// by adjacency, since the twins no longer sit beside their keys.
//
// A twin applies **iff K's own entry classifies as an eviction tombstone** — against anything else
// it is a crash-window leftover, ignored and swept. A live body's facts are native, so a stale
// twin can never override them. Only eviction tombstones need a twin.
//
// The facts live in the *key name* — the one field a raw LIST returns per entry — bit-packed into
// a fixed 36-char field (below), so a twin is `2 + 36 = 38` bytes longer than its base key. A key
// longer than [`TWIN_MAX_KEY_LEN`] therefore gets **no** twin ([`Facts::twin_key`] returns `None`),
// and its eviction tombstone resolves through the per-key HEAD fallback LIST already runs for a
// genuinely-missing twin (§6): the tombstone metadata is the authoritative copy, the twin only its
// LIST projection, so a missing one costs a round trip and never correctness.

/// Longest base key that still gets a twin: `1024 − 2·|CTRL| − |facts|`. Above it, twins degrade to
/// the HEAD fallback. A **format constant**, not a tunable — changing the facts encoding moves
/// previously-written twins across it.
pub const TWIN_MAX_KEY_LEN: usize = 1024 - 2 - FACTS_CHARS;

/// The packed facts field width. `{md5(128) ‖ plen(46) ‖ mtime_ms(42) ‖ part-count(14)}` = 230
/// bits, and 36 base-91 chars hold `36·log2(91) ≈ 234.3` bits — the tightest fixed width that fits.
const FACTS_CHARS: usize = 36;
const FACTS_BASE: u64 = 91;
const FACTS_BITS_MD5: u32 = 128;
const FACTS_BITS_PLEN: u32 = 46;
const FACTS_BITS_MTIME: u32 = 42;
const FACTS_BITS_COUNT: u32 = 14;

/// The 91-symbol printable-ASCII alphabet the packed facts render in: `0x21..=0x7E` minus `/` (a
/// `/` would let a twin roll up under a delimiter listing and vanish from the twin cursor, §7),
/// `+` (form-style decoders turn it into a space), and `;` (kept out only by construction). **Space
/// (0x20) is excluded too**: a literal space in a key round-trips through the `encoding-type=url`
/// LIST as `+` on some backends, so a space in the facts would corrupt the twin key hypha reads
/// back — every char here percent-encodes unambiguously and never collides with the `+`/space trap.
const fn facts_alphabet() -> [u8; FACTS_BASE as usize] {
    let mut a = [0u8; FACTS_BASE as usize];
    let mut c = 0x21u8;
    let mut i = 0;
    while c <= 0x7E {
        if c != b'/' && c != b'+' && c != b';' {
            a[i] = c;
            i += 1;
        }
        c += 1;
    }
    a
}
const FACTS_ALPHABET: [u8; FACTS_BASE as usize] = facts_alphabet();

/// Inverse of [`FACTS_ALPHABET`]: byte → digit value, or `-1` for a byte outside the alphabet.
const fn facts_rev() -> [i8; 256] {
    let mut r = [-1i8; 256];
    let a = FACTS_ALPHABET;
    let mut i = 0;
    while i < FACTS_BASE as usize {
        r[a[i] as usize] = i as i8;
        i += 1;
    }
    r
}
const FACTS_REV: [i8; 256] = facts_rev();

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
    /// The twin's full `<meta>`-bucket key for `base_key`: `0x01 ‖ base_key ‖ 0x01 ‖ facts`. `None`
    /// if `base_key` exceeds [`TWIN_MAX_KEY_LEN`] (no twin — HEAD fallback) or the ETag/fields don't
    /// pack (a malformed ETag, or a field beyond the packed width — never happens for real facts).
    pub fn twin_key(&self, base_key: &str) -> Option<String> {
        if base_key.len() > TWIN_MAX_KEY_LEN {
            return None;
        }
        let facts = self.pack()?;
        let c = CTRL as char;
        Some(format!("{c}{base_key}{c}{facts}"))
    }

    /// Bit-pack the facts into their fixed 36-char field, or `None` if the ETag is malformed or a
    /// field overflows its width.
    fn pack(&self) -> Option<String> {
        // Split the client ETag into its raw MD5 and part count: bare 32-hex ⇒ single-part, count
        // 0; `<32-hex>-N` ⇒ composite of N parts. `pack` stores the count and `unpack` rebuilds the
        // exact string, so the twin needn't carry the `-N` suffix literally.
        let (md5_hex, count) = match self.client_etag.rsplit_once('-') {
            Some((h, n)) => (h, n.parse::<u32>().ok()?),
            None => (self.client_etag.as_str(), 0),
        };
        let md5 =
            u128::from_be_bytes(<[u8; 16]>::try_from(hex::decode(md5_hex).ok()?.as_slice()).ok()?);
        if self.plen >> FACTS_BITS_PLEN != 0
            || self.mtime_ms < 0
            || (self.mtime_ms as u64) >> FACTS_BITS_MTIME != 0
            || count >> FACTS_BITS_COUNT != 0
        {
            return None;
        }
        // hi = plen(46) ‖ mtime(42) ‖ count(14) = 102 bits; the 230-bit value is hi·2^128 + md5.
        let hi = ((self.plen as u128) << (FACTS_BITS_MTIME + FACTS_BITS_COUNT))
            | ((self.mtime_ms as u128) << FACTS_BITS_COUNT)
            | count as u128;
        let mut limbs = [md5 as u64, (md5 >> 64) as u64, hi as u64, (hi >> 64) as u64];

        let mut digits = [0u8; FACTS_CHARS];
        for d in digits.iter_mut() {
            *d = FACTS_ALPHABET[divmod_small(&mut limbs, FACTS_BASE) as usize];
        }
        digits.reverse(); // most-significant digit first
        Some(String::from_utf8(digits.to_vec()).expect("alphabet is ASCII"))
    }

    /// Inverse of [`Self::pack`]: 36 alphabet chars → `Facts`, or `None` if any char is off-alphabet
    /// or the decoded value exceeds 230 bits (a corrupt twin key).
    fn unpack(s: &str) -> Option<Facts> {
        if s.len() != FACTS_CHARS {
            return None;
        }
        let mut limbs = [0u64; 4];
        for b in s.bytes() {
            let digit = FACTS_REV[b as usize];
            if digit < 0 {
                return None;
            }
            mul_add_small(&mut limbs, FACTS_BASE, digit as u64);
        }
        // A valid value is < 2^230, so limbs[3] (bits 192..) must be < 2^38.
        if limbs[3]
            >> (FACTS_BITS_MD5 + FACTS_BITS_PLEN + FACTS_BITS_MTIME + FACTS_BITS_COUNT - 192)
            != 0
        {
            return None;
        }
        let md5 = (limbs[0] as u128) | ((limbs[1] as u128) << 64);
        let hi = (limbs[2] as u128) | ((limbs[3] as u128) << 64);
        let count = (hi & ((1 << FACTS_BITS_COUNT) - 1)) as u32;
        let mtime_ms = ((hi >> FACTS_BITS_COUNT) & ((1 << FACTS_BITS_MTIME) - 1)) as i64;
        let plen =
            ((hi >> (FACTS_BITS_COUNT + FACTS_BITS_MTIME)) & ((1 << FACTS_BITS_PLEN) - 1)) as u64;
        let md5_hex = hex::encode(md5.to_be_bytes());
        let client_etag = if count == 0 {
            md5_hex
        } else {
            format!("{md5_hex}-{count}")
        };
        Some(Facts {
            client_etag,
            plen,
            mtime_ms,
        })
    }
}

/// Split a full `<meta>`-bucket key into `(base_key, Facts)` if it is a twin (range B), else `None`
/// — including range-A records (`0x01 0x01 …`), which lead with a doubled `0x01` a twin never does.
pub fn parse_twin(full_key: &str) -> Option<(&str, Facts)> {
    let c = CTRL as char;
    let rest = full_key.strip_prefix(c)?;
    if rest.starts_with(c) {
        return None; // range A, not a twin
    }
    let sep = rest.find(c)?; // base contains no 0x01 (admission), so this is the separator
    let (base, tail) = rest.split_at(sep);
    Some((base, Facts::unpack(&tail[1..])?))
}

/// Long-division of the little-endian 256-bit value in `limbs` by a small `d < 2^32`, returning the
/// remainder. Used to render the packed facts in base 91.
fn divmod_small(limbs: &mut [u64; 4], d: u64) -> u64 {
    let mut rem: u128 = 0;
    for limb in limbs.iter_mut().rev() {
        let cur = (rem << 64) | *limb as u128;
        *limb = (cur / d as u128) as u64;
        rem = cur % d as u128;
    }
    rem as u64
}

/// `limbs = limbs·m + add` over the little-endian 256-bit value — the Horner step decoding base 91.
fn mul_add_small(limbs: &mut [u64; 4], m: u64, add: u64) {
    let mut carry: u128 = add as u128;
    for limb in limbs.iter_mut() {
        let cur = *limb as u128 * m as u128 + carry;
        *limb = cur as u64;
        carry = cur >> 64;
    }
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

/// S3's max key length; hypha does not reduce it (§6) now that twins live in a separate bucket.
pub const MAX_KEY_LEN: usize = 1024;

/// S3's max bucket-name length. The configured bucket prefix (§2) is charged against it, so the
/// client-visible cap is `S3_MAX_BUCKET_NAME − max(prefix length)` (§7 *Buckets*).
pub const S3_MAX_BUCKET_NAME: usize = 63;

/// Key admission (§6): S3's own 1024-byte cap plus the one structural rule the `<meta>` ranges rest
/// on — no `0x00` or `0x01`. Those two bytes build every `<meta>` range, and both sort at or below
/// the twin separator, so either in a client key could fall inside the twin range. Every other byte,
/// control chars included, is permitted: LIST rides `encoding-type=url` (`Backend::list`), so any
/// byte round-trips. Enforced at every op that takes a key.
pub fn validate_client_key(key: &str) -> Result<(), &'static str> {
    if key.len() > MAX_KEY_LEN {
        return Err("key too long (max 1024 bytes)");
    }
    if key.bytes().any(|b| b == 0x00 || b == CTRL) {
        return Err("key contains a 0x00 or 0x01 byte, reserved by hypha");
    }
    Ok(())
}

/// Client bucket-name admission (§7 *Buckets*): the prefix is charged against S3's 63-byte cap, so
/// reject up front with a clean error rather than an opaque backend failure once `max_prefix_len`
/// characters of prefix are prepended.
pub fn validate_bucket_name(name: &str, max_prefix_len: usize) -> Result<(), String> {
    if name.len() + max_prefix_len > S3_MAX_BUCKET_NAME {
        return Err(format!(
            "bucket name too long: {} + {max_prefix_len}-byte prefix exceeds the {S3_MAX_BUCKET_NAME}-byte S3 limit",
            name.len()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn twin_roundtrips() {
        // A single-part twin (bare-MD5 ETag, count 0).
        let single = Facts {
            client_etag: "ab".repeat(16),
            plen: 4096,
            mtime_ms: 1_700_000_000_000,
        };
        // A composite twin (`<md5>-N`), exercising the packed part count.
        let composite = Facts {
            client_etag: format!("{}-137", "cd".repeat(16)),
            plen: (1u64 << 46) - 1, // max plen the 46-bit field holds
            mtime_ms: 1,
        };
        for f in [single, composite] {
            let tk = f.twin_key("dir/obj").unwrap();
            let (base, decoded) = parse_twin(&tk).unwrap();
            assert_eq!(base, "dir/obj");
            assert_eq!(decoded, f);
            // 0x01 lead + base + 0x01 sep + 36 packed facts chars.
            assert_eq!(tk.len(), 1 + "dir/obj".len() + 1 + FACTS_CHARS);
        }
    }

    #[test]
    fn twin_range_is_order_isomorphic() {
        // The merge join (§7) relies on `A < B  ⇒  twin(A) < twin(B)`, including the prefix case.
        let f = Facts {
            client_etag: "ab".repeat(16),
            plen: 1,
            mtime_ms: 1,
        };
        let mut keys = ["a", "a/b", "a!b", "ab", "b", "a\u{7f}"];
        keys.sort();
        for w in keys.windows(2) {
            let ta = f.twin_key(w[0]).unwrap();
            let tb = f.twin_key(w[1]).unwrap();
            assert!(ta < tb, "twin order broke for {:?} < {:?}", w[0], w[1]);
        }
        // A twin never collides with a range-A record, whose doubled 0x01 lead parse_twin rejects.
        assert!(parse_twin(&mpu_upload_key("id")).is_none());
        assert!(parse_twin(&shadow_key("k")).is_none());
    }

    #[test]
    fn twin_degrades_above_threshold() {
        let f = Facts {
            client_etag: "ab".repeat(16),
            plen: 1,
            mtime_ms: 1,
        };
        assert!(f.twin_key(&"k".repeat(TWIN_MAX_KEY_LEN)).is_some());
        // One byte over: no twin — the eviction tombstone resolves via the HEAD fallback (§6).
        assert!(f.twin_key(&"k".repeat(TWIN_MAX_KEY_LEN + 1)).is_none());
    }

    #[test]
    fn facts_alphabet_excludes_delimiter_hazards() {
        assert_eq!(FACTS_ALPHABET.len(), 91);
        for bad in *b"/+;" {
            assert!(!FACTS_ALPHABET.contains(&bad));
            assert!(FACTS_REV[bad as usize] < 0);
        }
        // Every alphabet byte round-trips through the reverse table.
        for (i, &b) in FACTS_ALPHABET.iter().enumerate() {
            assert_eq!(FACTS_REV[b as usize], i as i8);
        }
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
        client.insert("mime".to_string(), "text/plain;charset=utf-8".to_string());
        client.insert("spaced".to_string(), "two  spaces".to_string());

        // hypha's own facts share the carrier and must survive untouched, unread as client keys.
        let mut stored: std::collections::HashMap<String, String> =
            encode_user_metadata(&client).collect();
        stored.insert(TOMB.to_string(), TOMB_EVICT.to_string());
        stored.insert(SCLASS.to_string(), "STANDARD_IA".to_string());

        assert_eq!(decode_user_metadata(&stored), client);
        assert_eq!(storage_class(&stored), "STANDARD_IA");
        // Percent-encoded at rest, so no backend header round trip can mangle a value.
        assert!(stored.values().all(|v| v.is_ascii()));
        // The escape set is narrow on purpose: the carrier is capped at 2 KB and shared with the
        // client, so ordinary ASCII must not spend the client's budget on encoding.
        for bare in ["value", "text/plain;charset=utf-8"] {
            assert!(
                stored.values().any(|v| v == bare),
                "safe ASCII must ride through unencoded, got {stored:?}"
            );
        }
        // Space is the one printable that must still escape: SigV4 canonicalization collapses runs
        // of whitespace in a header value, so `two  spaces` would sign as `two spaces`.
        assert_eq!(stored.get("u-spaced").unwrap(), "two%20%20spaces");

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
        // Full S3 key length is now admissible; only 1025+ is rejected.
        assert!(validate_client_key(&"x".repeat(MAX_KEY_LEN)).is_ok());
        assert!(validate_client_key(&"x".repeat(MAX_KEY_LEN + 1)).is_err());
        // The <meta> ranges live in a separate bucket, but their keys carry 0x01 and so are
        // inadmissible as client keys regardless.
        assert!(validate_client_key(&mpu_upload_key("id")).is_err());
        // Plane-16 keys are fine — nothing reserved in the client keyspace anymore.
        assert!(validate_client_key("\u{100000}anything").is_ok());
    }

    #[test]
    fn bucket_name_budget() {
        // 63-byte S3 cap minus the longest configured prefix.
        assert!(validate_bucket_name(&"b".repeat(61), 2).is_ok());
        assert!(validate_bucket_name(&"b".repeat(62), 2).is_err());
        assert!(validate_bucket_name("bucket", 0).is_ok());
    }

    #[test]
    fn meta_ranges_sort_and_separate() {
        // Range A (0x01 0x01 …) < range B twins (0x01 <K≥0x02> …) < range C markers (bare K).
        let range_a = mpu_upload_key("u");
        let f = Facts {
            client_etag: "ab".repeat(16),
            plen: 1,
            mtime_ms: 1,
        };
        let range_b = f.twin_key("obj").unwrap();
        let range_c = "obj".to_string(); // a bare pending marker
        assert!(range_a < range_b);
        assert!(range_b < range_c);
        // Only range B parses as a twin.
        assert!(parse_twin(&range_a).is_none());
        assert!(parse_twin(&range_b).is_some());
        assert!(parse_twin(&range_c).is_none());
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
            format!("\u{1}\u{1}mup-1\u{1}c10000;AAAA-nonce_1")
        );
        // `c` records are not part records, so one LIST separates them by prefix alone.
        assert_eq!(parse_mpu_part(&mpu_stash_key("up-1", 10_000, "n")), None);

        // The upload's own record and malformed keys don't parse as parts.
        assert_eq!(parse_mpu_part(&mpu_upload_key("up-1")), None);
        // Every record of one upload sorts under its prefix, ahead of the twin/marker ranges.
        assert!(mpu_upload_key("up-1").starts_with(&mpu_prefix("up-1")));
        assert!(ks.starts_with(&mpu_prefix("up-1")));
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
