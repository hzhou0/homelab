//! The cache-side structures plaintext facts travel through (§6): eviction/transition tombstones in
//! the `<data>` bucket, and — in the `<meta>` bucket, keyed apart by the two control bytes client keys
//! may not use — facts twins, pending markers, and mpu records. Plus the composite-ETag arithmetic
//! and key admission.
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
/// Client `Content-Type`. Unlike the rest of the pass-through it is *also* written to the remote
/// object natively (§6, *Remote objects*), so it is the one client value a restore recovers.
pub const CTYPE: &str = "ct";

pub const STANDARD: &str = "STANDARD";

/// Tombstone kinds (value of the [`TOMB`] metadata key).
pub const TOMB_EVICT: &str = "evict";
pub const TOMB_TRANSIT: &str = "transit";

/// Fixed 16-byte tombstone bodies, compiled in, so a LIST classifies every internal data entry from
/// its (size, ETag) pair without a metadata read (§6).
pub const EVICT_SENTINEL: [u8; 16] = [
    0xe4, 0x80, 0xae, 0x85, 0xd6, 0xe7, 0x58, 0x9c, 0x7e, 0x07, 0xb5, 0xa5, 0xac, 0x39, 0x37, 0xaa,
];
/// Reserved generation token carried by cached DELETE markers. A client body with this ETag would
/// make its PUT marker indistinguishable from DELETE, so the exact body remains reserved.
pub const DELETE_MARKER_SENTINEL: [u8; 16] = [
    0x64, 0x58, 0x6a, 0xf5, 0x7f, 0xc3, 0xf6, 0x22, 0xf3, 0x00, 0xd3, 0xbb, 0x42, 0xb8, 0x72, 0x6d,
];
/// K is mid-bracket (§7): cache facts are distrusted and readers resolve K from the remote.
pub const TRANSIT_SENTINEL: [u8; 16] = [
    0xd9, 0xa5, 0xc8, 0x7a, 0x7c, 0x7e, 0x03, 0xc8, 0x04, 0x6c, 0x1a, 0xbf, 0x7c, 0x49, 0x0c, 0x65,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TombKind {
    Evict,
    Transit,
}

impl TombKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TombKind::Evict => TOMB_EVICT,
            TombKind::Transit => TOMB_TRANSIT,
        }
    }

    pub fn sentinel(self) -> &'static [u8; 16] {
        match self {
            TombKind::Evict => &EVICT_SENTINEL,
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

/// Whether `body` equals a reserved internal sentinel (§6). Evict/transition values would spoof the
/// data classifier; the DELETE value would spoof the marker operation discriminator.
pub fn is_reserved_sentinel(body: &[u8]) -> bool {
    body.len() == 16
        && [EVICT_SENTINEL, DELETE_MARKER_SENTINEL, TRANSIT_SENTINEL]
            .iter()
            .any(|s| s.as_slice() == body)
}

/// Classify a cache LIST entry from its (size, ETag) pair alone (§6). `None` ⇒ a live body.
pub fn classify_entry(size: i64, etag: &str) -> Option<TombKind> {
    if size != 16 {
        return None;
    }
    [TombKind::Evict, TombKind::Transit]
        .into_iter()
        .find(|k| k.sentinel_etag() == etag)
}

/// Tombstone kind from an object's user-metadata (the HEAD-path classification).
pub fn tomb_kind(metadata: &std::collections::HashMap<String, String>) -> Option<TombKind> {
    match metadata.get(TOMB).map(String::as_str) {
        Some(TOMB_EVICT) => Some(TombKind::Evict),
        Some(TOMB_TRANSIT) => Some(TombKind::Transit),
        _ => None,
    }
}

pub fn is_tombstone(metadata: &std::collections::HashMap<String, String>) -> bool {
    metadata.contains_key(TOMB)
}

pub fn evict_sentinel_etag() -> String {
    TombKind::Evict.sentinel_etag()
}

pub fn delete_marker_body() -> String {
    use md5::{Digest, Md5};
    hex::encode(Md5::digest(DELETE_MARKER_SENTINEL))
}

/// The marker object's own ETag identifies DELETE while remaining the CAS token returned by LIST.
pub fn delete_marker_etag() -> String {
    use md5::{Digest, Md5};
    hex::encode(Md5::digest(delete_marker_body().as_bytes()))
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
// [`CTYPE`] is the exception, and rides the remote object's native `Content-Type` because a wrong
// media type is a wrong answer rather than a lost label.

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

/// hypha's own keys don't carry [`USER_PREFIX`], so they drop out.
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

/// Client `Content-Type`, escaped like a user metadata value because a media type carries spaces
/// (`text/html; charset=utf-8`) and SigV4 canonicalization rewrites runs of whitespace.
pub fn encode_content_type(value: &str) -> String {
    percent_encoding::utf8_percent_encode(value, META_ESCAPE).to_string()
}

pub fn content_type(stored: &std::collections::HashMap<String, String>) -> Option<String> {
    let raw = stored.get(CTYPE)?;
    Some(
        percent_encoding::percent_decode_str(raw)
            .decode_utf8()
            .ok()?
            .into_owned(),
    )
}

/// The client-visible pass-through carried on a tombstone: its `x-amz-meta-*` (under [`USER_PREFIX`])
/// and echoed storage class ([`SCLASS`]), dropping hypha's own facts (`tomb`/`plen`/`cetag`/`mtime`).
/// Used when rehydrate promotes an eviction tombstone back to a live cache body (§8): the facts
/// become native (size/ETag/mtime), but the pass-through must survive, and a stray `tomb` key would
/// make [`tomb_kind`] mis-classify the live body as a tombstone.
pub fn passthrough_metadata(
    metadata: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    metadata
        .iter()
        .filter(|(k, _)| k.starts_with(USER_PREFIX) || k.as_str() == SCLASS || k.as_str() == CTYPE)
        .map(|(k, v)| (k.clone(), v.clone()))
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

/// Range-A tag for the per-bucket sync marker.
const TAG_SYNC: char = 's';

/// Range-A tag for the per-bucket clean marker.
const TAG_CLEAN: char = 'c';

const TAG_HALT: char = 'h';

/// Range-A tag for rehydrated composites' shadow bodies.
const TAG_SHADOW: char = 'b';

/// Range-A tag for the per-bucket shadow-clean marker.
const TAG_SHADOW_CLEAN: char = 'o';

/// **Remote** (`<remote><b>`): the halt marker — the record of an invariant violation, written by
/// the run that observed it and fatal to every run that finds it (`hypha::halt`).
///
/// The only hypha-internal key that lives on the remote rather than in `<meta>`, and deliberately:
/// a violation says hypha's picture of its own data is wrong, so it has to outlive the cache. The
/// cache is exactly what a namespace restore rebuilds and a volume loss destroys — a halt marker
/// there would be erased by the recovery it exists to block.
///
/// Being in the client keyspace, it leads with the two control bytes no client key may contain, and
/// every remote listing filters it out ([`is_reserved_remote_key`]).
pub fn halt_marker_key() -> String {
    format!("{c}{c}{TAG_HALT}", c = CTRL as char)
}

/// Whether a key returned by a **remote** listing is hypha's own rather than a client object.
///
/// Client keys cannot contain `0x01` ([`validate_client_key`]), so the leading control byte is a
/// complete test. Every path that reads the remote as a client keyspace must apply it — a listing
/// that does not would hand a reserved key to a trailer read, and a reserved key carries no
/// trailer, which is itself an invariant violation (`hypha::halt`).
pub fn is_reserved_remote_key(key: &str) -> bool {
    key.starts_with(CTRL as char)
}

/// Cache (`<meta><b>`): the sync marker (§6). Present iff this bucket's cache namespace has been
/// reconciled from the remote and is therefore authoritative; its absence puts reads on the remote
/// until the restore sweep rewrites it (§7). The presence is the whole signal — the body is empty.
pub fn sync_marker_key() -> String {
    format!("{c}{c}{TAG_SYNC}", c = CTRL as char)
}

/// Cache (`<meta><b>`): the clean marker (§6). Present iff this bucket's pending-marker range is a
/// *complete* account of its pending set — not an empty one; pending markers beside it are the
/// steady state. Written only by a graceful drain, deleted for every bucket at startup before the
/// first request is served, so its absence (the default everywhere) costs a recovery scan rather
/// than a silently non-durable write. Presence is the whole signal — the body is empty.
pub fn clean_marker_key() -> String {
    format!("{c}{c}{TAG_CLEAN}", c = CTRL as char)
}

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

/// Cache (`<meta>`): fixed record proving hypha replaced one retained part with its folded form
/// during an earlier completion attempt. Its metadata identifies the original generation; the
/// retained ciphertext remains at [`mpu_stash_key`].
pub fn mpu_fold_key(upload_id: &str) -> String {
    format!("{}f", mpu_range(upload_id))
}

/// Every upload's records at once — what the §8 debris sweep scans, since no upload id is known to
/// it in advance.
pub fn mpu_scan_prefix() -> String {
    format!("{c}{c}{TAG_MPU}", c = CTRL as char)
}

/// A retired recency slice in **GC's own bucket** (§8) — not `<meta><b>`, since the ring is global.
/// Nothing client-facing shares that bucket, so these keys need none of the control-byte machinery
/// the `<meta>` ranges are built from.
///
/// Zero-padded hex so the listing's lexicographic order **is** seal order — the whole of what a
/// reload needs to rebuild the ring newest-first, and the reason the sequence is a counter rather
/// than a timestamp (§8 keeps wall clock out of the mechanism; naming it would invite reading age
/// off the key).
pub fn recency_slice_key(seq: u64) -> String {
    format!("{RECENCY_PREFIX}{seq:016x}")
}

pub const RECENCY_PREFIX: &str = "recency/";

pub fn parse_recency_seq(key: &str) -> Option<u64> {
    u64::from_str_radix(key.strip_prefix(RECENCY_PREFIX)?, 16).ok()
}

/// The upload a record belongs to. Upload ids cannot contain `0x01` (the remote's own ids are
/// base64/hex), which is what makes the trailing separator an unambiguous terminator.
pub fn parse_mpu_upload_id(key: &str) -> Option<&str> {
    let c = CTRL as char;
    key.strip_prefix(&format!("{c}{c}{TAG_MPU}"))?
        .split(c)
        .next()
        .filter(|id| !id.is_empty())
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

/// Parse an mpu part record key; `None` for the upload's own `u` record, fold-intent `f` record, a
/// retained-ciphertext `c` record, or a malformed key. Reads only the record segment after the final
/// `0x01`, so the upload id (which never contains `0x01`) can't be mistaken for it.
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

pub fn mpu_prefix(upload_id: &str) -> String {
    mpu_range(upload_id)
}

/// Cache (`<meta>`): a rehydrated composite's plaintext (cached mode, §6). Range-A tag `b`, keyed by
/// the **whole** SHA-256 of K rather than by K — the access pattern is a point lookup, so the key can
/// be a digest, which lifts every length condition K would otherwise impose.
///
/// Full width, not a prefix: a truncated digest needs a second, wider digest carried in the shadow's
/// metadata and checked on every read, purely so a collision degrades to a cache miss instead of
/// serving another key's plaintext. Spending all 256 bits in the key deletes the collision case, the
/// metadata field, and the check at once — 32 bytes base64url-unpadded is 43 characters, and nothing
/// prefix-scans this key, so its width costs nothing.
///
/// base64url because every character is control-byte-free and `start-after`-safe, the same reason the
/// twin's packed facts and the mpu stash nonce use it.
pub fn shadow_key(key: &str) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{c}{c}{TAG_SHADOW}{}",
        base64_simd::URL_SAFE_NO_PAD.encode_to_string(Sha256::digest(key.as_bytes())),
        c = CTRL as char
    )
}

/// Every shadow body at once — what §8's shadow probe scans, since it holds no client key to derive
/// one from.
pub fn shadow_scan_prefix() -> String {
    format!("{c}{c}{TAG_SHADOW}", c = CTRL as char)
}

/// Metadata key on a shadow body carrying **K itself** — the back-pointer the digest key cannot
/// provide (§8). Only the orphan backstop reads it: a shadow whose K no longer names this generation
/// is unreachable and unrankable, and there is no other way to ask K about it.
///
/// base64url of K's raw bytes rather than the percent-encoding the client passthrough uses. The
/// encoding has to be unconditional: percent-encoding a 1024-byte non-ASCII key expands 3× and
/// overruns S3's 2 KB user-metadata ceiling, whereas base64url is a flat 4/3 — 1368 characters at the
/// key-length cap, which fits beside `cetag` with room to spare. A shadow's metadata is hypha's alone
/// (no client passthrough shares this carrier, unlike a tombstone's), so the whole budget is available.
pub const SHADOW_CLIENT_KEY: &str = "ck";

pub fn encode_shadow_client_key(key: &str) -> String {
    base64_simd::URL_SAFE_NO_PAD.encode_to_string(key.as_bytes())
}

/// `None` for anything that is not a key this hypha could have written — a truncated value, a foreign
/// encoding, or bytes that are not UTF-8. The backstop treats that as "cannot judge" and leaves the
/// shadow alone, which is the only safe reading: the alternative is deleting a live shadow because its
/// back-pointer was unreadable.
pub fn decode_shadow_client_key(encoded: &str) -> Option<String> {
    let raw = base64_simd::URL_SAFE_NO_PAD.decode_to_vec(encoded).ok()?;
    let key = String::from_utf8(raw).ok()?;
    validate_client_key(&key).ok().map(|()| key)
}

/// Cache (`<meta><b>`): the shadow-clean marker (§8). Present iff no shadow body in this bucket has
/// been orphaned without being reclaimed — the same positive-evidence discipline as the pending set's
/// clean marker ([`clean_marker_key`]), and deliberately a *separate* marker rather than a second
/// meaning bolted onto that one: a failed shadow reclaim is a handful of leaked bytes, and folding it
/// into the clean marker would make it withhold that marker too, sending the next run into a full
/// pending-set rebuild over a leak. Cheap evidence must not be able to trigger expensive recovery.
///
/// Written only by a graceful drain with nothing owed, deleted at startup before the first request,
/// so a running process — which can orphan a shadow at any moment — never has a marker on disk
/// claiming otherwise. Presence is the whole signal; the body is empty.
pub fn shadow_clean_marker_key() -> String {
    format!("{c}{c}{TAG_SHADOW_CLEAN}", c = CTRL as char)
}

/// Cache (`<meta><b>`): the pending marker for `key` — **bare K**, range C (§6). Its body is the PUT
/// body's ETag or [`delete_marker_body`]; its own S3 ETag identifies the operation and is the
/// reconciler's CAS handle. Bare because a marker is a durability signal, not an optimization —
/// every admissible key has one, no threshold. Returned borrowed since it *is* the client key.
pub fn pending_marker_key(key: &str) -> &str {
    key
}

/// `start_after` for the reconcile sweep's flat marker LIST (§7): a value above every range-A/B key
/// (all lead with `0x01`) yet below every range-C bare marker (client keys, whose first byte is
/// ≥ `0x02` by admission), so one LIST past it enumerates only markers — `O(pending)`, never
/// `O(evicted)`. It leads with `0x01` (hence below all client keys) then the maximum code point
/// repeated past the longest possible `0x01`-prefixed key (a `<meta>` key is ≤ 1024 bytes; 256
/// four-byte chars overrun that), so it sorts above them all. The sweep still filters defensively —
/// any residual `0x01`-lead key it sees is skipped — so a boundary miscompare can never mis-handle a
/// twin as a marker.
pub fn marker_scan_start_after() -> String {
    let mut s = String::from(CTRL as char);
    for _ in 0..256 {
        s.push('\u{10FFFF}');
    }
    s
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
// a fixed 39-char field (below), so a twin is `2 + 39 = 41` bytes longer than its base key. A key
// longer than [`TWIN_MAX_KEY_LEN`] therefore gets **no** twin ([`Facts::twin_key`] returns `None`),
// and its eviction tombstone resolves through the per-key HEAD fallback LIST already runs for a
// genuinely-missing twin (§6): the tombstone metadata is the authoritative copy, the twin only its
// LIST projection, so a missing one costs a round trip and never correctness.

/// Longest base key that still gets a twin: `1024 − 2·|CTRL| − |facts|`. Above it, twins degrade to
/// the HEAD fallback. A **format constant**, not a tunable — changing the facts encoding moves
/// previously-written twins across it.
pub const TWIN_MAX_KEY_LEN: usize = 1024 - 2 - FACTS_CHARS;

/// The packed facts field width: `{md5(128) ‖ plen(46) ‖ mtime_ms(42) ‖ part-count(14)}` = 230
/// bits fits 29 bytes, base64url-unpadded → 39 chars, fixed width. base64url because every char is
/// RFC 3986-unreserved — a twin key never needs percent-encoding or XML escaping, and the historic
/// hazards are absent by construction: `/` (a twin would roll up under a delimiter listing and
/// vanish from the twin cursor, §7), the `+`/space pair (form-style decoders turn `+` into a
/// space, and a literal space round-trips through the `encoding-type=url` LIST as `+` on some
/// backends), and `\`/`.` — MinIO splits path components on `\` as well as `/` and rejects any
/// `.`/`..` segment (`XMinioInvalidResourceName`), so either char in the pseudo-random facts made
/// some twin keys unwritable there.
const FACTS_CHARS: usize = 39;
const FACTS_BITS_PLEN: u32 = 46;
const FACTS_BITS_MTIME: u32 = 42;
const FACTS_BITS_COUNT: u32 = 14;

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

    /// Bit-pack the facts into their fixed 39-char field, or `None` if the ETag is malformed or a
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
        // The value is < 2^230, so the top 3 of its 32 big-endian bytes are zero and the low 29
        // encode to exactly 39 unpadded chars.
        let mut be = [0u8; 32];
        be[..16].copy_from_slice(&hi.to_be_bytes());
        be[16..].copy_from_slice(&md5.to_be_bytes());
        Some(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&be[3..]))
    }

    /// Inverse of [`Self::pack`]: 39 chars → `Facts`, or `None` if any char is off-alphabet or the
    /// decoded value exceeds 230 bits (a corrupt twin key).
    fn unpack(s: &str) -> Option<Facts> {
        if s.len() != FACTS_CHARS {
            return None;
        }
        let raw = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(s.as_bytes())
            .ok()?;
        // 29 bytes hold 232 bits; a valid value is < 2^230, so the top 2 bits are zero.
        if raw.len() != 29 || raw[0] & 0xC0 != 0 {
            return None;
        }
        let mut be = [0u8; 32];
        be[3..].copy_from_slice(&raw);
        let hi = u128::from_be_bytes(be[..16].try_into().expect("16 bytes"));
        let md5 = u128::from_be_bytes(be[16..].try_into().expect("16 bytes"));
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

    /// The remote bucket is client keyspace *plus* the halt marker (§6), and every key that gets
    /// past this filter goes to a trailer read — so the test that matters is that the filter is
    /// exactly the control byte no client key may carry, not a name match on the marker.
    #[test]
    fn reserved_remote_keys_are_exactly_the_control_byte_prefix() {
        assert!(is_reserved_remote_key(&halt_marker_key()));
        for client in ["k", "a/b", "\u{2}leading-control", " ", "\u{7f}"] {
            assert!(
                !is_reserved_remote_key(client),
                "{client:?} is admissible as a client key and must not be filtered"
            );
            assert!(validate_client_key(client).is_ok());
        }
    }

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
        // Every field at its maximum: the all-ones 230-bit value must still fit the 39-char field.
        let maxed = maxed_facts();
        for f in [single, composite, maxed] {
            let tk = f.twin_key("dir/obj").unwrap();
            let (base, decoded) = parse_twin(&tk).unwrap();
            assert_eq!(base, "dir/obj");
            assert_eq!(decoded, f);
            // 0x01 lead + base + 0x01 sep + 39 packed facts chars.
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

    /// The whole digest in the key is what deletes the collision check — so the key has to actually
    /// carry all of it, at a fixed width, in an alphabet nothing downstream has to escape.
    #[test]
    fn shadow_key_carries_the_whole_digest() {
        let key = shadow_key("some/client/key");
        let digest = key.strip_prefix(&shadow_scan_prefix()).expect("prefixed");
        assert_eq!(
            base64_simd::URL_SAFE_NO_PAD
                .decode_to_vec(digest)
                .expect("base64url")
                .len(),
            32,
            "a truncated digest would put the collision case back"
        );
        assert!(digest
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));

        // Fixed width regardless of K, which is the length condition the digest key exists to lift.
        let long = shadow_key(&"k".repeat(MAX_KEY_LEN));
        assert_eq!(long.len(), key.len());
        assert_ne!(long, key);
    }

    #[test]
    fn reserved_sentinel_detection() {
        for s in [EVICT_SENTINEL, DELETE_MARKER_SENTINEL, TRANSIT_SENTINEL] {
            assert!(is_reserved_sentinel(&s));
        }
        assert!(
            !is_reserved_sentinel(&[0u8; 16]),
            "an ordinary 16-byte body is not a sentinel"
        );
        // Length gate: only these exact 16-byte bodies carry an internal classification token.
        let mut longer = EVICT_SENTINEL.to_vec();
        longer.push(0);
        assert!(!is_reserved_sentinel(&longer));
        assert!(!is_reserved_sentinel(&EVICT_SENTINEL[..15]));
    }

    #[test]
    fn marker_scan_boundary_splits_ranges() {
        let boundary = marker_scan_start_after();
        // Above every range-A record and range-B twin (all lead with 0x01)…
        let f = Facts {
            client_etag: "ab".repeat(16),
            plen: 1,
            mtime_ms: 1,
        };
        assert!(boundary > mpu_upload_key("some-upload-id"));
        assert!(boundary > sync_marker_key());
        assert!(boundary > clean_marker_key());
        assert!(boundary > shadow_key("\u{10FFFF}".repeat(240).as_str()));
        // A twin over a maximal (all-U+10FFFF) key is the hardest case for the boundary.
        let hard = "\u{10FFFF}".repeat(240);
        assert!(boundary > f.twin_key(&hard).unwrap());
        // …and below every range-C bare marker (client keys start ≥ 0x02).
        for k in ["\u{2}", "obj", "dir/obj", "\u{10FFFF}", &"z".repeat(1024)] {
            assert!(
                boundary.as_str() < pending_marker_key(k),
                "boundary must precede {k:?}"
            );
        }
    }

    #[test]
    fn passthrough_metadata_keeps_client_facts_only() {
        let mut md = std::collections::HashMap::new();
        md.insert(TOMB.to_string(), TOMB_EVICT.to_string());
        md.insert(PLEN.to_string(), "42".to_string());
        md.insert(CETAG.to_string(), "ab".repeat(16));
        md.insert(MTIME.to_string(), "7".to_string());
        md.insert(SCLASS.to_string(), "REDUCED_REDUNDANCY".to_string());
        md.insert(format!("{USER_PREFIX}color"), "blue".to_string());
        let kept = passthrough_metadata(&md);
        assert_eq!(kept.len(), 2);
        assert_eq!(
            kept.get(SCLASS).map(String::as_str),
            Some("REDUCED_REDUNDANCY")
        );
        assert_eq!(
            kept.get(&format!("{USER_PREFIX}color")).map(String::as_str),
            Some("blue")
        );
        assert!(!kept.contains_key(TOMB));
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
    fn packed_facts_use_unreserved_chars() {
        // The rendered field must stay within RFC 3986-unreserved chars — alnum or `-_` — so no
        // historic hazard (`/`, `+`, space, `\`, `.`) is representable in a twin key.
        let maxed = maxed_facts();
        let tk = maxed.twin_key("obj").unwrap();
        let facts = tk.rsplit(CTRL as char).next().unwrap();
        assert_eq!(facts.len(), FACTS_CHARS);
        assert!(facts
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
        // Off-alphabet chars, a wrong length, or a value ≥ 2^230 (39 chars of `_` = 2²³⁴ − 1)
        // don't parse — a corrupt twin degrades to the HEAD fallback instead of misreading facts.
        assert!(Facts::unpack(&"/".repeat(FACTS_CHARS)).is_none());
        assert!(Facts::unpack(&"A".repeat(FACTS_CHARS - 1)).is_none());
        assert!(Facts::unpack(&"_".repeat(FACTS_CHARS)).is_none());
    }

    #[test]
    fn sentinels_are_distinct_and_classify() {
        for kind in [TombKind::Evict, TombKind::Transit] {
            assert_eq!(kind.sentinel().len(), 16);
            assert_eq!(classify_entry(16, &kind.sentinel_etag()), Some(kind));
        }
        assert_eq!(classify_entry(16, &delete_marker_body()), None);
        assert_ne!(delete_marker_body(), delete_marker_etag());
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

    /// Every facts field at its maximum — the all-ones 230-bit value.
    fn maxed_facts() -> Facts {
        Facts {
            client_etag: format!("{}-{}", "ff".repeat(16), (1u32 << 14) - 1),
            plen: (1u64 << 46) - 1,
            mtime_ms: (1i64 << 42) - 1,
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
            "\u{1}\u{1}mup-1\u{1}c10000;AAAA-nonce_1"
        );
        // `c` records are not part records, so one LIST separates them by prefix alone.
        assert_eq!(parse_mpu_part(&mpu_stash_key("up-1", 10_000, "n")), None);

        // The upload's own record and malformed keys don't parse as parts.
        assert_eq!(parse_mpu_part(&mpu_upload_key("up-1")), None);
        assert_eq!(parse_mpu_part(&mpu_fold_key("up-1")), None);
        // Every record of one upload sorts under its prefix, ahead of the twin/marker ranges.
        assert!(mpu_upload_key("up-1").starts_with(&mpu_prefix("up-1")));
        assert!(mpu_fold_key("up-1").starts_with(&mpu_prefix("up-1")));
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
