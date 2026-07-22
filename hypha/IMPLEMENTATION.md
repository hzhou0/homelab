# Hypha — implementation proposal

Companion to [`ARCHITECTURE.md`](./ARCHITECTURE.md), which owns the *what* and *why*. This document
commits to the *how*: runtime, crates, module boundaries, the concurrency model, and the mechanisms
that make the design's guarantees (linearizable conditional writes, sound per-part encryption,
bounded loss window) hold in code. Code comments cite these section numbers.

## 1. Language, runtime, workspace

Rust, edition 2021, async on **Tokio** (`rt-multi-thread`). I/O-bound proxying with a CPU-bound
AEAD step fast enough to stay inline for normal object sizes (§5).

Cargo workspace:

- **`hypha-format`** — the age envelope wrapper: pure sync codec (age 0.11 is sync-only —
  `StreamWriter<W: Write>` / `StreamReader<R: Read>`), closed-form offset arithmetic, and the
  `RangeReader` seek adapter; the serving binary bridges it to async bodies. Standalone so it
  carries the proptest/fuzz/bench suite without a server.
- **`hypha-core`** — shared library: `Backend` (an `aws-sdk-s3` wrapper with bucket-prefix mapping),
  `meta` (tombstones, sentinels, facts twins, composite ETag, key admission), typed config
  (including the mode), error → `s3s::S3Error` mapping.
- **`hypha`** — the serving binary: the `s3s::S3` surface, the sync↔async codec bridges, and the
  shared tiering machinery — `Reconciler` (upload/tombstone primitives over cache + remote) and
  `KeyLocks` (the per-key lock table). Later phases add the reconcile sweep, the GC scavenger, and
  the restore sweep as background tasks of the active replica. Runs **active-passive** (§4).
- **`hypha-fence`** — the fencing controller (§4); enters the workspace in phase 6.

## 2. Dependencies

Both halves of the S3 protocol come from crates:

**Server — [`s3s`](https://github.com/Nugine/s3s) 0.14.** Routing, SigV4, `aws-chunked`, XML, and
an `#[async_trait]` `S3` trait with one method per op, all defaulting to `NotImplemented` — hypha
implements only what it serves. `S3Auth` is a single `get_secret_key(access_key)`; that is where
hypha validates its *own* clients' credentials.

**Clients (cache + remote) — `aws-sdk-s3`** with `aws-config`. Both backends are the same SDK type
pointed at different endpoints; the architecture's loose coupling falls out naturally.

**Encryption — `age` 0.11, native scrypt recipient.** A reviewed streaming AEAD format: per-chunk
authentication, seekable decryption, a finalizer chunk for truncation detection, and per-file
random file keys — which give parallel part encryption without key/nonce coordination, with
per-file key isolation. File keys are wrapped by age's own scrypt recipient with the work factor
pinned to the minimum (§6) — no custom recipients, no plugins, nothing to maintain. The crate is
sync; hypha drives it over adapters bridged via `spawn_blocking` (§5).

| Concern              | Crate(s)                                                            |
|----------------------|---------------------------------------------------------------------|
| Runtime / streaming  | `tokio`, `tokio-util`, `bytes`, `futures`                           |
| S3 server / clients  | `s3s`, `s3s-aws`, `aws-sdk-s3`, `aws-config`                        |
| Encryption / hashing | `age`, `hmac`+`sha2` (trailer MAC, §6), `md-5`, `hex`                 |
| Config / errors      | `serde`, `figment`; `thiserror`, `anyhow` (bootstrap)               |
| Observability        | `tracing`(+`subscriber`); `metrics` + Prometheus exporter (planned) |
| Concurrency          | `dashmap` (planned: the §8 in-flight-PUT ref count)                 |
| Testing              | `proptest`, `criterion`, `cargo fuzz`, `testcontainers`             |

## 3. Module layout

```
hypha-format/src/
  envelope.rs            Encryptor/Decryptor over age's scrypt recipient, work factor pinned (§6)
  trailer.rs             the authenticated facts+table trailer at every remote object's tail (§6)
  offset.rs              plaintext ⇄ ciphertext arithmetic over the constant HLEN (§6)
  stream.rs              RangeReader: sync Read+Seek over ranged GETs (seek ⇒ new byte-range req)

hypha-core/src/
  config.rs              typed config: mode, both endpoints, auth, master passphrase
  backend.rs             Backend over an aws-sdk-s3 client (bucket-prefix mapping, typed errors)
  meta.rs                tombstones, sentinels, facts twins, composite ETag, key admission
  error.rs               error → s3s::S3Error mapping

hypha/src/
  main.rs                config load, s3s server, signal handling
  auth.rs                S3Auth for hypha's own client credentials
  codec.rs               sync age ⇄ async body bridges; inline encrypt + MD5, trailer framing (§6)
  keylocks.rs            per-key async lock table (§4)
  tier.rs                Reconciler: upload / tombstone / twin primitives (§7)
  s3/                    the s3s::S3 impl, split by op group
    put.rs get.rs list_head.rs delete.rs multipart.rs buckets.rs
  replication.rs         (phase 4) the cached-mode reconcile sweep (§7)
  gc/                    (phase 5) scavenger task, active-only (§8); restore sweep (§7)

hypha-fence/src/         (phase 6) fencing controller (§4)
```

The `s3/` modules are thin: parse intent, take the key lock where required, orchestrate `Backend`,
`hypha-format`, `meta`, and `tier`.

## 4. Modes, concurrency, and the linearizability guarantee

### Two modes, one machinery

A deployment runs in one of two modes; **both require the cache and the remote**. The cache is
always the namespace and ETag source of truth — HEAD/LIST and conditional-write evaluation are
cache-served in both modes — and the remote always holds age ciphertext framed with an
authenticated facts trailer (§6) so the restore sweep (§7) can rebuild the cache namespace from it.

- **`durable`** — writes are synchronous: the remote op is the **commit point**, bracketed by a
  transition mark so readers never see torn state (§7). PUT encrypts and uploads inline, settles
  the eviction tombstone (+ facts twin) in the cache, then acks. The cache holds only tombstones
  and twins, and a tombstoned GET decrypts from the remote without repopulating (a restored body
  would immediately be tombstoned again). Ack ⇒ remote-durable: no loss window, at the cost of
  remote latency on every write.
- **`cached`** — writes ack after the cache write plus a pending marker; a background reconcile
  sweep uploads to the remote (§7). GC tombstones cold bodies under pressure and tombstoned GETs
  rehydrate (§8). Low latency, bounded async-lag loss window.

Durable mode is the cached machinery under three constraints: synchronous upload, always
tombstone, never restore. Both modes share `Reconciler` and the tombstone/twin/marker structures
(§6); multipart takes one path regardless of mode (§7).

**Client ETags.** Single-part in cached mode: the cache computes `MD5(plaintext)` natively.
Single-part in durable mode: computed inline alongside encryption (the cache sees no plaintext).
Multipart: the composite `md5(concat part-md5s)-N`, composed at `CompleteMultipartUpload` from
per-part plaintext MD5s hypha accumulates during the upload (§7).

### Single active writer, per-key locks

Serving is **active-passive**: one active replica does all work; the pre-warmed passive
(stateless — "pre-warmed" just means connections open) promotes instantly. Within the single
writer, the **per-key async lock table** (`keylocks.rs`) is the serialization primitive. It is
taken by:

- **conditional writes** — the lock covers HEAD → evaluate → write, and is the linearization
  point: hypha resolves the key's *current client-visible ETag* (below), evaluates the
  precondition, and on success writes unconditionally. Conditional-write semantics are hypha's
  own, whatever the backends provide.
- **durable-mode mutations** — held across the whole transition bracket (§7): precondition →
  mark → remote commit → settle. The remote op *is* the ack path, and same-key commits must not
  reorder against their cache projections.
- **GC eviction's tombstone step and rehydrate** (§8) — so tombstone transitions never
  interleave with conditional writes.

The **cached-mode reconcile** serializes on a second, dedicated per-key **upload lock** instead —
same table primitive, separate instance. Same-key reconcile work must not overlap or reorder (an
unserialized older upload finishing after a newer one leaves the remote stale with an empty
pending set — §7), but a replication upload mutates no client-visible state, so it must only ever
block *other reconciles of the same key*, never make a conditional PUT queue behind a multi-second
transfer.

**Unconditional cached-mode PUTs take no lock** — they race on the cache (S3 last-writer-wins) and
are fenced against eviction by the §8 in-flight ref count and conditional tombstones, not by the
lock.

The cache's own ETag is the **version token**, but not always the client-visible ETag (tombstones
carry a sentinel ETag; the client ETag rides their metadata — §6). A
conditional write resolves by key state: **live body** → native cache ETag;
**eviction-tombstoned** → `cetag` from tombstone metadata; **delete-tombstoned / absent** →
client-visibly absent (`If-Match` 412s; creates proceed); **transition-marked** (always a crash
leftover — the writer that marked it held this lock) → repair from the remote first (§7), then
resolve.

### The allow-policy *is* the lease

"Single writer" cannot rest on observing that the old active is dead (unobservable under
partition); it rests on **fabric fencing**: the `hypha-fence` controller maintains one invariant —
exactly one hypha identity is in the SeaweedFS ingress allow and the OPNsense egress allow to the
remote, and that identity *is* the active. Belief is free; only the network-allowed pod can write,
so the writer set is ≤ 1. Identities are static (a two-pod StatefulSet's
`statefulset.kubernetes.io/pod-name` labels — fencing must never depend on relabeling a node that
may be partitioned); only the destination-side policy moves.

Failover is **ordered fence-before-promote**: (1) lease renewal missed → (2) fence the old
active's identity → (3) wait for Cilium to report the policy revision applied on the SeaweedFS
endpoints — the answerable "is it isolated?" replacing the unanswerable "is it dead?" →
(4) **drain the in-flight window**: reset the fenced identity's established connections (a PUT cut
mid-stream aborts; an incomplete upload doesn't commit) then wait a settle delay bounding
finalize-after-bytes-arrived (small, enforced by server-side request timeouts) → (5) promote the
passive. The fence is applied at the *SeaweedFS nodes*, which are healthy — the partitioned node
never participates in its own fencing, which is why this works where `kubectl delete` (delegated
to the unreachable kubelet) cannot. It narrows the existing default-deny SeaweedFS ingress grant;
the absence of an allow is the fence. Graceful shutdown skips the whole window (release, then
promote).

The remote leg is weaker — Cilium egress is source-enforced and OPNsense may see SNAT'd node IPs —
so a partitioned old active can retain remote reach. Harmless for cached-path PUTs (fenced off the
cache, it has nothing new to upload); the exposed window is an in-flight multipart commit (§12).

Reads take no lock; during the failover gap the surface is briefly write-unavailable, not degraded.

**Request lifecycle.** One task per request; bodies stream as `Bytes` through the codec bridges —
per-request memory is a few age chunks regardless of object size. A global `Semaphore` caps
in-flight concurrency.

## 5. Threading & the AEAD CPU step

ChaCha20-Poly1305 runs at multi-GB/s/core, so 64 KiB chunks encrypt in microseconds — inline on
the async worker is fine; hypha offloads to `spawn_blocking` only when a single contiguous
encrypt/decrypt exceeds a threshold (default 1 MiB). Measured (criterion, `hypha-format`):
~1.5 GiB/s/core encrypt, ~1.3 GiB/s decrypt (measured on the phase-1 X25519 build; the
pinned-work-factor scrypt wrap is the same order of magnitude — re-bench at swap, and assert the
emitted stanza's work factor, since age's *default* auto-tunes toward ~1 s per file) — per-file
key isolation costs noise, one core outruns 10 GbE.

## 6. Data structures

The envelope client bodies travel in, and every object hypha stores around them. Each structure
on the non-commit side of an operation is a **projection**, rebuildable from the committed side
(§7).

### The age envelope

age v1 properties hypha relies on (`offset.rs` implements the math):

- **Fixed 64 KiB chunks** (65552 ciphertext bytes each), so offset math is closed-form. The
  scrypt header length is a **compile-time constant** `HLEN`: age's v1 spec requires the scrypt
  stanza to be a file's sole stanza, so age 0.11.x never greases a hypha header (its
  `no_scrypt()` gate is false), and the stanza is fixed-shape (16-byte salt → constant 22 b64
  chars, pinned work factor). So `ct_len = HLEN + 16 + plen + 16·⌈plen/64 KiB⌉` is a direct
  forward computation from `plen` — no capture-and-measure, no derived `hlen`, no header parse. A
  `hypha-format` round-trip test pins `HLEN`'s exact value and trips if a future age changes it
  (⇒ trailer version bump).
- **Seekable decryption** — an S3 ranged-GET body is one-shot, so `stream.rs`'s `RangeReader`
  satisfies `Read + Seek` by issuing a fresh byte-range GET per seek (one per request in
  practice). A cold ranged GET is two remote reads — header (to unwrap the file key) + chunk
  range — coalesced when the range abuts the head. age's `Seek` lives on the sync path; §5 bridges
  it.
- **Per-file random file keys**, wrapped by age's **native scrypt recipient**
  (`age::scrypt::Recipient` over the 256-bit random master passphrase; fresh 16-byte salt per
  file): ~75 B stanza, post-quantum where X25519 is harvest-now-decrypt-later-exposed and ~20×
  smaller than age's native `mlkem768x25519` (ARCHITECTURE.md has the rationale). **The work
  factor is pinned via `set_work_factor(1)`** — load-bearing, not an optimization: security lives
  in the passphrase's 256 bits, stretching adds nothing, and the crate's default auto-tunes
  toward ~1 s and ~256 MiB *per file* — fatal for a small-object namespace. Wholly stock age, so
  DR is any age binary + the passphrase. The scrypt stanza is spec-required to be a file's sole
  stanza — no multi-recipient; rotation is an accepted flag-day re-encrypt (ARCHITECTURE.md).
  Parallel parts and concurrent PUTs need no key/nonce coordination, and the key separation,
  chunk-index-derived nonces, and finalizer chunk make cross-object splices, reorders, and
  truncation fail authentication.

These lengths are the complete read-side state: a single-part object is decodable from
Content-Length and the constant `HLEN`; a composite is a concatenation of pure per-part age
files whose boundaries come from the trailer's offset table, per-part `plen`s from the closed
form (§6).

### The facts+table trailer

Every remote object ends in a single **authenticated trailer** (`trailer.rs`) carrying its facts
and, for a composite, its parts offset table. It exists because S3 offers no slot that lands facts
atomically with a streamed body (metadata travels ahead of the body; `MD5(plaintext)` exists only
once the body has streamed; tags are post-hoc) — a trailer *behind* the ciphertext is both atomic
and at a knowable offset, so every commit is self-describing with no second carrier to crash
between. A keyed MAC (below) makes truncation, tampering, and foreign objects fail to verify.

**Contents.** A fixed facts struct — a kind byte (*single* | *composite*),
the client part count (the composite ETag's `-N`), total `plen`, the client-write mtime, and the
raw MD5 whose hex form (plus `-N` for composites) is the client ETag — followed, for a composite,
by the **parts table**: `count` little-endian `u64` cumulative ciphertext end-offsets. The table
is the complete read-side geometry: part *i* is ciphertext `[table[i-1], table[i])` (`table[-1]=0`),
its `plen` from the closed form over that window and the constant `HLEN`. hypha keeps no other
part table and never consults the remote's native part index.

**Integrity — a keyed MAC.** The trailer is authenticated by a 16-byte **HMAC-SHA256** tag
(`hmac`/`sha2`, in-tree — age's own header MAC) over
`version ‖ object_key ‖ body_ciphertext_len ‖ facts ‖ table`, keyed by
`footer_key = KDF(master_passphrase)` derived once at boot and reused across all trailers. The
key-binding gives cross-object splice and downgrade resistance; a failed verify (`subtle`
constant-time) marks the object as foreign or corrupt. The facts are legible to the remote —
`plen`, `mtime`, and `count` already surface via Content-Length / LastModified / native part
count, and the offset table is the native part sizing the remote set itself; `MD5(plaintext)` is
the one field it additionally sees, a content-confirmation exposure hypha accepts for the simpler
integrity-only path.

**Framing.** The trailer sits **outside** the age envelope(s), so decrypt paths stop before it:
age's reader is EOF-delimited, so trailing bytes would get pulled into the final chunk read and
fail authentication (`hypha-format`'s `trailing_bytes_break_decryption` documents this). Physical
tail order is `table ‖ facts ‖ tag(16) ‖ version:u16`: the fixed-size facts struct sits at a
known offset from the end, so its `count` sizes the preceding table, and the 2-byte version
dispatches the format (the MAC covers the version, so a tampered tail fails to verify). A
single-part PUT (`count = 1`) appends `facts ‖ tag ‖ version` in the same streaming `PutObject`;
a composite's is the `table ‖ facts ‖ tag ‖ version` **terminating part** (part number above every
client part — hence the 9999 client cap), so `CompleteMultipartUpload` stays the single atomic
commit of body + facts. `hypha-format` owns the layout, `HLEN`, `MAX_PARTS`, and `MAX_TAIL_LEN`
(the one-shot speculative tail-read size), plus the `encode` /
`decode_tail(footer_key, object_len, tail)` API. DR: the body is stock age, and the plaintext
tail parses directly for facts and boundaries.

### Cache objects

Body, tombstone, and marker share one keyspace: a client body lives at `K` and a tombstone
overwrites `K` in place, so a racing GET sees one or the other, never a 404.

**Tombstones** carry fixed 16-byte sentinel bodies, compiled in, one per kind — **eviction** (body
is remote-only, facts in metadata/twin), **delete** (client-visibly absent), and **transition** (K
is mid-bracket, §7; cache facts are distrusted and readers resolve K from the remote). The values
are **random** (CSPRNG-generated once, then fixed), not readable markers: each kind gets a
deterministic (size==16, ETag) pair, so a plain LIST classifies every key with no HEAD, and each
sentinel's constant ETag doubles as a CAS token. Random 16-byte values so no client body collides
with the classification token by accident.

**Facts twins** — a zero-byte object at `K ‖ 0x01 ‖ cetag ; plen ; mtime`, carrying in its key
name (the one field LIST returns per entry) exactly the facts LIST needs for an evicted key: the
client ETag, the plaintext size, and the original client-write mtime. The separator sorts below every admissible key byte, so the twin
arrives adjacent to K in the same LIST page. A twin **applies if K's own entry classifies as an
eviction tombstone**; next to anything else it is a crash-window leftover, ignored and swept — a
live body's facts are native, so a stale twin can never override them. An eviction tombstone
whose twin is missing or unparseable falls back to a per-key HEAD (the tombstone's metadata is
the authoritative copy; the twin is its LIST projection). Twins are written in the same locked
sequence as their tombstone (twin-before-tombstone), and every path that replaces an eviction
tombstone passes through a live body or a transition mark first — so an eviction tombstone is
never adjacent to another epoch's twin, and the classification gate is the entire validity
check.

**Key admission** is what makes the twin scheme sound, and is now as narrow as the scheme allows:
client keys may not contain `0x00` or `0x01` — `0x01` is the separator, and both sort at or below
it, so either would let a client key fall between a base key and its twin — and are capped at 900
bytes for twin-suffix headroom (`meta::validate_client_key`). Every other byte, control chars
included, is permitted: hypha lists with **`encoding-type=url`** and percent-decodes (`Backend::list`),
so keys XML can't represent (the `0x01` separator, or a client's own control bytes) round-trip
safely. Enforced at every op that takes a key.

**Tombstone metadata**: every tombstone carries the full facts — kind, `cetag`, `plen`, original
mtime — in its user-metadata, the authoritative copy; HEAD and GET serve from it, and the twin is
its LIST projection. Eviction never changes a key's client-visible `LastModified`: LIST reads it
from the twin, HEAD from the metadata.

**Shadow body** (cached mode): a rehydrated composite's plaintext at `<marker-prefix>/body/<K>`.
The tombstone and twin at K stay untouched — K never changes classification, so composite
rehydration is invisible to LIST/HEAD and rewrites no twin. A tombstoned GET probes the shadow
before the remote; evicting a shadow is a single delete.

**The pending marker** (cached mode) lives at `<marker-prefix>/<K>` — **one per key**, body = the
body ETag of the most recently acked PUT. Concurrent PUTs overwrite it; last writer wins — the
write-coalescing point: however many PUTs raced, the pending set holds one entry for K and
reconcile uploads the latest cache body. The marker's own S3 ETag (`M_etag`) changes on each
overwrite and is the reconciler's CAS handle. The marker prefix leads with U+10FFFD — the highest
interchange-safe codepoint (last plane-16 PUA char, just below the U+10FFFE/F noncharacters XML
serializers may reject) — above every practical client key, so the reserved keyspace clusters at
the **end** of a LIST rather than interleaving a client's scan. Efficiency, not correctness: LIST
filters reserved keys per-entry, so a client that does use such keys merely scans less efficiently
there. A ~128-bit base64url tail is what prevents collision. Marker and body live on the same cache
volume: both survive a process crash, both die together on volume loss. **The marker set is the
durability signal**, enumerable as one flat LIST.

**Multipart upload state**: one record per uploaded part, key `<marker-prefix>/mpu/<upload-id>/p{n:05};<retag>;<pmd5>`
(empty body) — the part's facts **encoded into the key** so `CompleteMultipartUpload` recovers
them with **one LIST**, no per-part HEAD. The only irreducible datum is `pmd5`, the part's
*plaintext* MD5: hypha hashes it inline during the streaming encrypt and it is never re-derivable,
because the remote only ever sees ciphertext. `retag` is the remote's part ETag (the *ciphertext*
MD5). Everything else the remote can re-tell us — so `ct_len`/`plen` are **not** stored; a part's
plaintext length is `plaintext_len_from(ct_size, HLEN)` over the size the remote reports.

Encoding the facts in the key means a re-uploaded part (legal in S3, last-write-wins) writes a
*new* key rather than overwriting, so several records can coexist for one part number. They are
disambiguated at complete by the one authority that already resolved the race — **the remote's own
`ListParts`**, which returns the winning `(n → retag, size)` for the in-progress upload. hypha
matches each winning `retag` to the record carrying it (a ciphertext MD5 ⇒ the match is exact),
takes that record's `pmd5`, and ignores the losing orphans (swept by the batched delete below).
This is why no hypha-minted version counter is needed: `UploadPart` returns no ordering token, and
a durable monotonic counter across the active/passive pods would be its own distributed problem —
the remote's retag *is* the version. Survives process restarts across a multi-hour upload; dropped
at complete/abort via a batched multi-object delete.

**The sync marker**: a reserved object under the marker prefix, present iff a namespace
reconciliation has completed — namespace trust recorded in the cache itself, dying with the
volume by construction. Present ⇒ reads are cache-authoritative and an absent key is a definitive
404. Absent ⇒ the remote is the read source of truth until the restore sweep rewrites it (§7).

**Recency slices**: sealed Bloom filters under `<marker-prefix>/recency/`, the persisted form of
the §8 recency ring.

### Remote objects

Every remote object is age ciphertext ending in its authenticated facts+table trailer (§6 above);
key names and the trailer are plaintext, the body is not. The trailer is the sole facts carrier —
no user-metadata, no tags.

**Single-part object**: one age file at `K` with `facts ‖ tag ‖ version` appended in the same
`PutObject`.

**Composite**: the remote's own native-multipart object at `K` — a concatenation of pure
per-part age files plus the terminating `table ‖ facts ‖ tag ‖ version` trailer part. Ciphertext
part boundaries come from the trailer's own offset table, and per-part plaintext lengths fall
out of the closed form against the constant `HLEN` — hypha never reads the remote's native part
index, so a part-index-less remote is unrestricted (§9).

**Prefix-distribution hint**: approximate per-prefix key counts at a reserved key, refreshed for
free by the §8 walk cursor — advisory sharding input for the restore sweep (§7).

## 7. Operations

Each client operation, as steps per mode, over `tier.rs`'s `Reconciler` primitives, the §4 lock
discipline, and the §6 structures. Two framing rules make every crash analysis below mechanical:

**The commit point is single and atomic.** In durable mode it is the *remote* operation —
`PutObject`, `CompleteMultipartUpload`, or `DeleteObject` at K, each an atomic single-key
transition on the remote. In cached mode it is the *cache* body write; the remote is trailing
state that readers never consult.

**The transition bracket** (durable mode). Every durable mutation of K runs
**mark → commit → settle**: overwrite K's cache entry with the transition tombstone (§6), perform
the remote op, then write the fresh projection (tombstone + twin, or remove the entry) and ack.
While K is marked, readers resolve K from the remote — facts and bytes from the same side, so no
crash can produce a hybrid read. The writer holds K's write lock across the bracket, so a mark is
only ever observed by lock-free readers mid-bracket (correct: remote-as-truth) or by anyone after
a crash (a leftover). **Repair rule**: whoever meets a leftover mark — a read, a conditional
write acquiring the lock, the maintenance sweep — HEADs the remote and settles K to what it finds
(rewrite the projection, or remove the entry if absent). Repair is idempotent and needs no
knowledge of what the dead writer was doing; a remote op that fails *indeterminately* (timeout)
is handled identically — leave the mark, fail the request, let repair settle K to whichever way
the remote actually landed.

The contract this yields: **acked ⇒ committed and projected; unacked ⇒ either never committed
(the old object fully intact) or committed with the ack lost** — the irreducible ambiguity of any
request/response system — never a hybrid read, never a wrongly-absent key.

### PutObject

**Durable** — all under K's write lock:

1. Resolve K's current client ETag from the cache (live-body ETag / tombstone `cetag` /
   delete-tombstone or absent ⇒ none; leftover mark ⇒ repair first) and evaluate
   `If-Match` / `If-None-Match`.
2. **Mark**: transition tombstone at K.
3. **Commit**: one streaming `PutObject` at K — the request body encrypted (ct length computed
   directly from the constant `HLEN`, §6; client MD5 computed inline) with the authenticated facts
   trailer appended behind the ciphertext, so body and facts land atomically. K stays marked for
   the transfer; readers of K meanwhile resolve from the remote, which atomically holds the old
   object until the PUT completes. Plaintext is capped at 4 GiB — the same
   envelope-vs-5-GiB-ceiling math as a part; larger bodies belong to multipart anyway.
4. **Settle**: eviction tombstone + twin with the same facts. Ack.

Crash before the commit lands: the remote still holds the old object — marked readers serve it,
repair restores its projection; the op never happened. Crash after: committed — marked readers
serve the new object from the remote, repair completes the projection (facts off the trailer);
lost-ack.

**Cached** — the write lock covers steps 1–4 for conditional PUTs; unconditional PUTs take no
lock:

1. *(conditional only)* resolve + evaluate as above.
2. `inc` K's in-flight ref count (§8).
3. **Commit**: `PutObject` plaintext at K — the cache computes the ETag natively.
4. Overwrite the single marker at `<marker-prefix>/<K>` with the body ETag (last writer wins —
   the coalescing point, §6).
5. `dec`. Ack. The remote trails via the reconcile sweep below.

### DeleteObject

**Durable** — under K's write lock:

1. Repair a leftover mark if present.
2. **Mark**: transition tombstone at K — readers keep serving the object from the remote, so an
   unacked delete stays invisible.
3. **Commit**: remote `DeleteObject` (NotFound ⇒ already absent, still committed).
4. **Settle**: remove K's cache entry + twins — absent is the authoritative 404. Ack.

Crash before 3: the object survives; repair restores its projection. Crash after 3: 404
everywhere; repair removes the entry.

**Cached**:

1. *(under the write lock)* **Commit**: overwrite K with the **delete-tombstone** — GET/HEAD
   answer 404 and LIST omits K immediately.
2. Overwrite the marker (K's pending op is now a delete).
3. Ack. Reconcile propagates below; the mask is what keeps a crash from resurrecting K from the
   remote before the delete propagates.

### Multipart — one path, both modes

Parts route **around the cache** onto the remote's own native multipart upload at K (a part
isn't readable until commit; multipart is throughput-bound, so the cache's latency win doesn't
apply).

**CreateMultipartUpload**:

1. Validate the key; create the native upload on the remote; record the upload (its client key)
   in the mpu state (§6).

**UploadPart**:

1. Reject part numbers above **9999** (the number above every client part is reserved for the
   terminating trailer part) and plaintext > **4 GiB** (so the envelope never pushes a part past
   the remote's 5 GiB part cap; transparent re-splitting is a later refinement).
2. Encrypt the part as **its own pure age file** (fresh file key; Content-Length computed from
   `HLEN`), streaming to the remote as the native part; the plaintext MD5 is computed inline. A part
   whose framed ciphertext is **below the backend's 5 MiB part minimum** — which any S3 backend
   permits only as the upload's *final* part — is additionally retained in the cache mpu state
   (keyed by its `retag`, ≤ 5 MiB) so complete can fold the trailer into it rather than append a
   separate trailer part that would demote it to an illegal sub-minimum non-final part (see below).
3. Persist the part's `pmd5` (with `retag`) as a key-encoded mpu record (§6) — an in-progress
   upload's parts aren't readable, so complete needs its own copy of `pmd5`; its loss with the
   cache volume merely fails the eventual complete (never-acked, client retries).
4. Ack on the remote's part ack. Out-of-order / parallel / re-uploaded parts and concurrent
   uploads to one key are the remote's native semantics; per-part file keys make them
   cryptographically independent, and a re-upload's superseded record is resolved away at complete
   by the remote's `ListParts` (§6).

**CompleteMultipartUpload** — under K's write lock:

1. **One LIST** of the upload's mpu records (facts in the keys, §6) and **one `ListParts`** of the
   remote upload (authoritative winning `(n → retag, size)`, last-write-wins already resolved).
   For each client part number, match the remote's winning `retag` to its mpu record to recover
   `pmd5` — no per-part HEAD, and the remote-held bytes are the source of truth for part geometry.
   Compose the client ETag `md5(concat pmd5s)-N` (`meta::composite_etag`), each `plen` from the
   remote's part `size` + `HLEN`, and the cumulative-offset **parts table**. Reject if a requested
   part is absent from `ListParts`, or its client-supplied ETag doesn't match the matched `pmd5`.
2. Build the **terminating trailer** (§6) — `table ‖ facts ‖ tag ‖ version` — and place it as the
   object's final bytes. Normally it rides its **own part** above every client part (highest + 1);
   the trailer's content is identical either way. **But** S3 exempts only the *last* part from the
   5 MiB minimum, so when the highest client part is itself below that minimum — necessarily the
   client's last part, as any backend rejects a smaller non-final part — a separate trailer part
   would leave that part a sub-minimum non-final part the native complete rejects. In that case
   **fold** the trailer into the highest part: re-upload it (from its retained ciphertext, keyed by
   the remote's `ListParts` winning `retag` so the fold takes exactly that winner) as `part ‖
   trailer`, keeping it the final part. The committed object K is **byte-identical** either way —
   the same `age₁…ageₙ ‖ trailer` concatenation — so the read path is unaffected.
3. **Mark** K.
4. **Commit**: native complete on the remote — one atomic op lands the concatenated body *and* its
   facts at K. Its part set is either the client parts plus the separate trailer part, or (folded
   case) the client parts with the trailer riding the last one.
5. **Settle**: eviction tombstone + twin, drop the mpu state (batched multi-object delete). Ack.

Crash before 4: K untouched; the dangling native upload (trailer part included) is an orphan
(swept, aborted). Crash after 4: committed — marked readers serve it from the remote, and
repair (or, after a simultaneous cache loss, the restore sweep) reads the facts off the tail
trailer; lost-ack. In cached mode the composite enters the cache lazily on first GET via
rehydrate (§8); in durable mode it stays tombstoned like everything else.

**AbortMultipartUpload**: native abort on the remote; drop the mpu state. The §8 sweep reclaims
leftovers of abandoned uploads.

### GetObject / HeadObject

1. HEAD the cache at K, dispatch on what's there:
   - **Live body** (cached mode): serve from the cache; ranges forwarded.
   - **Eviction tombstone**: facts from its metadata. In cached mode, probe the shadow body (§6)
     and serve it on a hit; otherwise decrypt from the remote and rehydrate asynchronously (§8) —
     single-part into K, composite into the shadow. Durable mode always reads the remote. A
     single-part range maps to a closed-form chunk range (§6), driven through `RangeReader` +
     age seek and trimmed to the exact `[a,b)`. A composite read first fetches the encrypted
     trailer (one bounded `MAX_TAIL_LEN` tail GET, MAC-verified) for the facts and offset table,
     then issues a **single** streaming GET over the covering span, framing it into per-part age
     decryptors at the table's boundaries — no per-part round-trip, no remote part index (§6).
   - **Delete-tombstone**: 404.
   - **Transition tombstone**: remote-as-truth — HEAD the remote, serve (or 404) per its actual
     state, and opportunistically repair.
   - **Absent**: authoritative 404 under the sync marker (§6); remote-as-truth during resync
     (restore sweep below).

### ListObjectsV2

1. Classify each cache entry from its (size, ETag) sentinel pair (§6): **live body** → native facts
   (any adjacent twin is stale — ignored); **eviction tombstone** → the adjacent twin's
   `{cetag, plen, mtime}`, per-key cache HEAD fallback when the twin is missing;
   **delete-tombstone** →
   omitted; **transition tombstone** → per-key *remote* HEAD (the one classification that leaves
   the cache). Strip the deployment prefix; filter the reserved prefix.
2. **Single page, forwarded pagination.** A facts twin sits beside every eviction tombstone and
   delete-tombstones/reserved records are dropped, so a page can return **fewer** than `MaxKeys`
   client entries even when more exist — a short page. That is valid S3 as long as `IsTruncated` and
   the continuation token are honest, so hypha forwards the **backend's own** (key-position) token
   and truncation flag verbatim; `KeyCount` reports the entries actually emitted. LIST deliberately
   does **not** coalesce cache pages to fill `MaxKeys`: any such backfill would resume either by
   reusing a backend cursor across requests or by a client-entry count, and both weaken S3's
   key-position guarantee — a concurrent insert/delete in the re-listed range could dup or drop an
   untouched key. Short pages are the accepted cost; a client follows the token until `IsTruncated`
   is false.

   *Deferred optimization (revisit post-phase-4):* twin pairing is per cache page, so an eviction
   tombstone that is the **last** raw entry of a page has its twin on the next page → the
   missing-twin **HEAD fallback** fires (correct, but one extra HEAD per cache-page boundary that
   splits a pair). Avoidance is a cross-page carry-over — defer the unpaired trailing base and peek
   the next page's head before falling back to a HEAD; left for later, as the bounded HEAD fallback
   (which must exist anyway for genuinely-missing twins, §6) covers correctness meanwhile.

### Buckets

Buckets map one-to-one across client ⇄ cache ⇄ remote; the client bucket passes through, mapped to
`<bucket_prefix><bucket>` by each `Backend` (§2). Like multipart, the **remote is the source of
truth** and bucket ops are **always durable** (synchronous to the remote regardless of mode). The
cache bucket exists only to host object-side state (bodies, tombstones, twins, mpu records), so it
is created/deleted alongside but is never the authority. Rare control-plane events — no markers.

- **CreateBucket**: create the cache projection, then the remote — the remote create is the durable
  commit; a crash before it leaves a harmless orphan cache bucket (not yet "created" per the remote).
- **DeleteBucket**: delete the remote first (the durable commit that makes the bucket cease to
  exist), then the cache — a crash between leaves a retryable cache orphan, never a remote bucket the
  client believes is gone.
- **ListBuckets**: remote-served, filtered to this deployment's prefix and stripped back to
  client-visible names.
- **HeadBucket / GetBucketLocation**: remote existence check; the latter reports the deployment's
  configured backend region.

### Background: the reconcile sweep (cached mode)

The upload path for acked cache writes — a continual duty of the active (phase 4,
`replication.rs`). Each pass:

1. `ListObjectsV2` the cache at `prefix=<marker-prefix>/` — one entry per pending key,
   `O(pending)` over local NVMe; each yields K and the marker's own ETag `M_etag` (the CAS
   handle).
2. Dispatch on the cache body at K: delete-sentinel ⇒ **delete branch**, anything else ⇒
   **upload branch**.
3. **Upload branch**, under K's *upload* lock (§4 — reconcile-only, so client PUTs never queue
   behind it): GET the cache at K — `plen`, ETag `E_n`, and the body come from the *same
   response*, so the framed facts can never disagree with the uploaded bytes — encrypt
   (ct length from `HLEN`, trailer appended in) and PUT to the remote. Then delete the marker
   with `If-Match: M_etag`. A PUT that landed `E_{n+1}` mid-upload rewrote the marker, so the CAS
   412s and the next pass uploads it — the remote is transiently one version behind, never left
   stale with an empty pending set. **The body stays in the cache**: reconcile marks durability
   by deleting the marker; only GC (§8) tombstones, under pressure.
4. **Delete branch**, under the same upload lock (a delete propagation overlapping an in-flight
   upload of a prior version could otherwise land stale bytes *after* the remote delete,
   resurrecting the object at the next restore sweep): remote `DeleteObject`, clear the
   delete-tombstone with `If-Match: <delete-sentinel-etag>`, delete the marker with
   `If-Match: M_etag`. A concurrent create races the clear benignly (either order yields the same
   client-visible semantics).

**Bounded loss window (cached mode).** A process crash loses nothing: acked bodies and their
markers are in the cache; the new active resumes from the marker LIST. True loss requires the
**cache volume** to die with markers outstanding; the loss set is `O(pending)` and dies with the
volume — nothing to enumerate afterward, by construction. Durable mode has no loss window: its
commits are remote-side.

**Durability gates GC.** A key with a pending marker is never evicted or tombstoned, and eviction
independently confirms the remote object exists before overwriting a body (§8). A body only leaves
local storage once its ciphertext is provably on the remote.

### Background: the restore sweep (both modes)

Runs when the active acquires its claim and finds the sync marker (§6) absent — a fresh or wiped
cache. Until it completes, the remote is the read source of truth: remote LIST pages fan out
bounded per-entry HEADs for facts, and in cached mode are merged with an in-memory **pending
overlay** (acked-but-unuploaded PUTs patched in, pending deletes dropped; rebuilt from the marker
LIST on promotion) so read-after-write holds while the cache is untrusted. The sweep:

1. LIST remote and cache; recreate any bucket missing from the cache.
2. For each remote key with no cache entry (a surviving delete-tombstone counts as present, so
   pending deletes aren't resurrected), write an eviction tombstone + twin. Facts come from the
   object's authenticated tail trailer — one bounded suffix GET per key, single-part and
   composite alike (§6). An object whose trailer fails to verify was never written through
   hypha and is deleted.
3. Write the sync marker; flip reads back to the cache.

Throughput comes from sharding the keyspace — LIST chains are serial per shard — with shard
boundaries from the prefix-distribution hint (§6); a stale or missing hint degrades to
`delimiter=/` discovery with `start-after` splits. Hand-rolled over the SDK paginator + a
semaphore; idempotent (only fills gaps), the marker written only after every shard drains. In
durable mode this rebuild *is* the steady state being recreated — all tombstones. Serving is
never gated: a conditional write to K mid-sweep first materializes K's remote state into the
cache, then runs the normal §4 path.

### Lifecycle

- **Graceful drain.** On SIGTERM: stop accepting, release the active claim (passive promotes
  sub-second, no fence), best-effort one more reconcile pass to shrink the pending set. Sized
  into `terminationGracePeriod` + `preStop`.
- **Remote unavailable** → hot reads fine; tombstoned reads fail cleanly; cached-mode writes
  still ack and markers accumulate; durable-mode writes fail (correctly — they can't be made
  durable).
- **Cache volume loss** → discard and restart: the sync marker is gone, the restore sweep
  rebuilds; the only loss is the cached-mode pending set.

## 8. Tiering / GC — the scavenger task

A single background task of the active (the passive never scavenges), phase 5. In durable mode
there are no bodies to evict — the task only sweeps debris: orphan twins, leftover transition
marks (repaired per §7), abandoned mpu state and native uploads. In
cached mode it additionally evicts under pressure:

**Write-awareness: the in-flight ref count.** The PUT path's only in-process state is a per-key
`Arc<AtomicUsize>` in a swept `DashMap`: `inc` → body write → marker write → `dec`. The window it
covers — body confirmed, marker not yet visible — is exactly what no cache-side observation could
catch: eviction there would see a markerless key whose remote HEAD still finds the *previous*
version present, and tombstone an acked-but-never-uploaded body. The count gates only eviction;
PUTs never block on it.

**The recency ring.** The read path stays write-free: recency is a **Bloom-ring sketch** — one
filter per **fill window**, fed by GET/HEAD; sealed slices persisted per §6, reloaded on
promotion, retained k deep. A slice rotates when its distinct-key fill reaches the design point —
the insert path counts 0→1 bit flips, so fill is exact and duplicate touches of a hot key don't
advance it. Rotating on fill bounds each slice's false-positive rate by construction (no read
rate can silently degrade the ring into protect-everything) and keeps wall time out of the
mechanism entirely: the ring is denominated in distinct keys touched, so recency is relative to
competing traffic and an idle cache holds its working set indefinitely — nothing ages out except
by displacement. A probe returns the index of the **newest** slice containing
the key: a quantized last-access age, k+1 buckets from current-window down to *miss* — colder
than everything the ring remembers. Advisory only — a lost or cold ring (first boot, failover
without a persisted ring) collapses every key into one bucket and ordering degrades to
LastModified for one churnier cycle, never to incorrectness.

**Target-driven eviction — the threshold ratchet.** A pressure-triggered pass owes a byte
target: reclaim from current usage down to the low-water mark. The scavenger walks the keyspace
by rotating cursor, window by window, evicting only candidates at or above the current **age
threshold**, which starts at *miss* — the keys the ring affirmatively vouches nothing has
touched. If the target is unmet when the cursor completes a full loop, the threshold ratchets
one bucket younger and the walk continues — globally coldest-first without buffering the
keyspace, paying extra loops only under the pressure that justifies them, and converging on the
target whenever evictable bytes exist instead of stalling because too much looks recent.
LastModified is the tie-break within a bucket (rehydration lands a fresh mtime, so a
just-restored body sorts young). A pass that meets its target never ratchets younger, but may
keep taking *misses* the walk still encounters, bounded per pass — over-evicting an
affirmatively cold key is nearly free in rehydration risk, yet each eviction still costs a
remote HEAD, a twin write, and a CAS, hence the bound. Recency is priority only: it never
overrides the correctness gates below. Eviction of candidate K with version-token ETag `E_v`:

1. **Skip if ref count > 0.**
2. **Skip if the marker exists** (`HEAD <marker-prefix>/<K>`) — also catches markers from a prior
   process generation whose ref count died with it.
3. **Confirm the remote** (`HEAD` remote K); absent ⇒ not durable ⇒ skip.
4. Under K's lock: delete stale twins, write the fresh twin, then overwrite K with the eviction
   sentinel via `PutObject If-Match: E_v` — metadata carrying `cetag`/`plen`/original mtime. The
   tombstone is an atomic in-place replace: a racing GET sees body or tombstone, never 404.
   Twin-before-tombstone means a sentinel always has its twin; a crash between leaves a twin next
   to a live body — ignored by classification (§6), swept later.

A writer landing anywhere between steps 1 and 4 has moved the ETag, so step 4's `If-Match: E_v`
fails and eviction retries next pass — the layering (ref count → marker → remote HEAD →
conditional CAS) makes every interleaving auto-healing, never lossy. **Shadow bodies** (§6) are
evicted from their own reserved-prefix windows: confirm the remote composite (HEAD), then delete
the shadow — K's tombstone and twin are already in place.

**Rehydrate** (cached mode) is the mirror: fetch + decrypt from the remote under the lock,
holding the ref count while it runs (the sentinel ETag is constant across generations, so the
count is what closes the evict → rehydrate → re-evict ABA a constant-ETag CAS can't see). A
single-part body lands at K with `If-Match: <evict-sentinel-etag>`, then its twin is deleted —
K's facts are native again. A composite lands in the shadow body (§6); K's tombstone and twin
stay untouched.

**Usage from the backend.** The scavenger reads SeaweedFS volume/master metrics (physically
accurate, sees dead bytes), scavenges from high- to low-water mark, and can drive
`volume.vacuum`. Other cache backends plug in their own source.

## 9. Configuration & deployment

`figment` (TOML + `HYPHA_`-prefixed env, `__` nesting), validated at boot. Current surface
(`config.rs`): `remote` and `cache` endpoints (endpoint/region/credentials/**bucket prefix** —
client buckets pass through prefixed, so deployments share a remote account in disjoint bucket
namespaces), `mode` (`durable` | `cached`), `auth` (hypha's own
client credentials for `S3Auth`), `master_passphrase` (the 256-bit random age passphrase, from a Secret; supersedes phase 1's
`master_identity`), `serving.listen` + `serving.offload_threshold` (§5). Later phases add: reconcile pass
interval/concurrency, GC water marks / walk window / recency-ring shape (slice size, depth k,
rotation fill target) / opportunistic-eviction bound, restore fan-out + hint
interval, and the §4 fencing block (identity selectors, lease timings, fence-confirm timeout,
settle delay).

**Backend requirements.** The **cache** must implement conditional `PutObject`/`DeleteObject` —
load-bearing for the eviction/rehydrate/reconcile CAS (§7/§8), not for the client write path,
which linearizes on the §4 lock. SeaweedFS has them as of **4.07**, broken only under
versioning/object-lock, which the cache bucket enables neither of — pin ≥ 4.07; the §11 suite
re-verifies. It must also honor **`encoding-type=url`** on `ListObjectsV2` (universal S3) — hypha
lists with it so twin keys (containing `0x01`) and control-byte client keys survive the response
XML. Everything else the cache does is plain S3 objects, so it stays swappable; the only
SeaweedFS-specific surface is usage/vacuum (§8), already pluggable. The **remote** needs native
multipart including **`ListParts` on an in-progress upload** — core S3 multipart, universal
(SeaweedFS, B2 included), used at complete to resolve the winning part set and its sizes (§6/§7).
This is strictly weaker than the *completed-object* part index the trailer's embedded offset table
(§6) let us drop (`GetObjectAttributes`/`HEAD partNumber=n`, which not every remote implements);
the earlier object-tagging requirement is gone too, so tagging- and part-index-less remotes (e.g.
Backblaze B2's S3 layer) work unrestricted. The `master_passphrase` additionally derives the
trailer MAC key (§6).

Delivered as the `hypha/` chart (cluster-admin installed): the serving **StatefulSet** (2 pods,
active + passive — a StatefulSet so pod-name labels give the static Cilium identities the fence
selects on) + `Service` + `HTTPRoute`, and the `hypha-fence` controller (2 replicas,
leader-elected; RBAC for `CiliumNetworkPolicy`, the OPNsense allow, and Cilium policy-revision
reads). The fence narrows the existing default-deny SeaweedFS ingress grant; the network topology
itself stays owned by the `seaweedfs`/`cilium` charts per repo convention.

## 10. Observability

`tracing` spans per request (op, key, bytes, cache-hit); JSON in-cluster. `metrics` → Prometheus:
rate/latency by op, cache hit ratio, **pending-marker set size + reconcile pass duration**,
remote-upload latency/retries, role + failover count + fence-confirm latency, scavenge throughput
and usage vs. water marks. `/healthz` + `/readyz` (remote reachable); active/passive is a reported
condition, not a readiness gate.

## 11. Testing strategy

- **`hypha-format`**: proptest round-trips (encrypt→decrypt identity; corrupt/truncate/reorder/
  splice ⇒ auth failure); scrypt-wrap round-trips (emitted stanza carries the pinned work factor —
  guards a silent fallback to the ~1 s default; wrong passphrase ⇒ clean failure; interop: stock
  rage decrypts hypha output after the trailer strip);
  the **`HLEN` pin test** (a fresh encryption's header equals the hardcoded constant — trips if
  age ever changes the scrypt header, forcing a version bump; replaces the old
  capture-and-measure guard); trailer MAC round-trips (tag verifies;
  tamper/truncate/foreign-bytes/wrong-key ⇒ verify failure) and offset-table encode/decode +
  tiling validation; the trailing-bytes guard (age's EOF-delimited reader is why decrypt paths
  bound before the trailer);
  offset-arithmetic proptests against the fixed chunk size; a fuzz target
  for `RangeReader` seeks; criterion benches for the §5 threshold. (Largely built.)
- **Concurrency**: hammer conditional writes against one active over real SeaweedFS; assert
  linearizability (no double-create, no lost update) including against tombstoned keys (metadata
  ETag resolution). Pins SeaweedFS `If-Match` (§9). Bursty same-key overwrites: remote converges
  to the last-acked ETag within one reconcile pass.
- **Marker/reconcile**: (a) marker + absent remote ⇒ upload + CAS marker delete; (b) overwrite
  mid-upload ⇒ marker CAS 412s, next pass uploads the newer body; (c) dangling marker with the
  remote already current ⇒ marker deleted, nothing uploaded twice. Kill the active mid-sweep ⇒ the
  new active resumes from the marker LIST, no drops, no double-handling, no eviction before
  resolution. Cache-volume wipe ⇒ loss bounded by the pending set.
- **Eviction vs. writers**: sustained PUTs against a key under eviction; assert the §8 layering
  (ref-count skip, marker skip, `If-Match` abort) never tombstones an acked-but-unuploaded body,
  including the prior-generation-marker case.
- **Twin coherence**: crash-inject every point of twin sequences (delete-stale → write →
  tombstone; rehydrate's body-then-twin-delete); LIST never reports wrong facts — a twin next to
  a non-evict entry is ignored and swept, an evict tombstone with a missing twin HEAD-falls-back,
  ≤ 1 twin per key; shadow-body probe/evict races; lexicographic order holds with prefix-key
  populations (`a`, `a!b`, `a/b`).
- **LIST pagination**: a twin-diluted population paginated at several `MaxKeys` — pages may be
  short (dilution), but following the forwarded continuation token covers every key exactly once in
  order, never over `MaxKeys`, with `IsTruncated`/`NextContinuationToken` consistent. (Built —
  `conformance.rs`.) Keys with control bytes (`0x02`–`0x1f`, tab) and the `0x01`
  twin separator round-trip through the `encoding-type=url` LIST and decode back byte-exact.
- **Transition bracket**: crash-inject at every step of the §7 durable PUT / DELETE / complete
  brackets and assert the contract — readers never see hybrid facts/bytes, an unacked op leaves
  the old object fully readable or the new one fully committed, and repair settles K
  idempotently from the remote regardless of where the writer died.
- **Multipart**: out-of-order / parallel / re-uploaded parts — assert the re-upload's superseded
  mpu record is resolved away at complete by the remote's `ListParts` (winning `retag` matched to
  its `pmd5` record; orphan ignored and swept), including two concurrent same-part uploads;
  process restart mid-upload (`pmd5` recovered from mpu state); composite ETag correctness;
  single-stream composite GET + ranged GET across part boundaries (uniform and ragged part sizes)
  driven off the trailer's offset table; abort cleanup (batched delete); crash at complete *plus*
  cache wipe ⇒ restore decrypts the facts + table off the terminating trailer part.
- **Failover/fencing**: two replicas, partition the active, assert fence→confirm→drain→promote —
  old active's writes refused at the backend before the new active writes; graceful path too.
- **Integration** (`hypha/tests/`, built): an in-process harness drives hypha over an ephemeral
  port with a real `aws-sdk-s3` client against a throwaway **MinIO** serving as *both* cache and
  remote (kept disjoint by each backend's `bucket_prefix`); every fixture is stateless and tears
  down its MinIO + data dir on drop. Covers the durable S3 conformance surface incl. twin-diluted
  **LIST pagination** (`conformance.rs`),
  the multipart scenarios above including the small-final-part **trailer fold** (`multipart.rs`),
  model-based **proptest fuzzing** of random op sequences against a `BTreeMap` oracle (`fuzz.rs`),
  and an `#[ignore]`d **load/concurrency** suite — throughput, no-double-create-under-contention,
  parallel multipart (`load.rs`). Still to add: SeaweedFS as the cache backend, and a real zero-loss
  client (ZeroFS) against the durable endpoint.

## 12. Risks

- **`hypha-fence` is the load-bearing bespoke piece** — its ordered fence→confirm→drain→promote
  *is* the single-writer guarantee. Spike early on real Cilium: per-endpoint policy-revision
  observability and **established-connection reset on deny** (without which the settle delay must
  cover full transfer times). If the fence can't be programmed *and confirmed*, do **not** promote
  — fail-safe, sound here because the flat homelab failure domain means an unreachable-enforcer
  partition also cuts the old active off from the backend. The remote leg stays source-enforced
  (§4): the exposed window is an in-flight multipart commit from a fenced-but-alive active;
  escalate to per-replica remote credentials revoked by the controller if it matters. The
  controller itself is off the data path — its downtime delays failover, never creates two
  writers.
- **`s3s` conditional/chunked corners** — strict ETag quoting is the known sharp edge; the phase-2
  conformance pass is the check.

## 13. Implementation plan

Ordered so every phase ends independently testable — and from phase 2 on, independently
deployable — with the hardest machinery (cache coherence, fencing) landing last on proven layers.

**Phase 1 — `hypha-format`. Done.** Envelope, offset math, `RangeReader`, round-trip tests,
criterion benches (§5 numbers). (The phase-1 grease scare that motivated derived-`hlen` +
capture-and-measure was later resolved: age can't grease a scrypt sole-stanza, so `HLEN` is a
constant and that machinery is removed — §6.)

**Phase 2 — durable serving. Done (vs. MinIO).** `hypha-core`
(config/backend/meta/error, twins, key admission) and the s3s surface over durable mode: PUT
(preconditions, inline encrypt + ETag with the §6 facts trailer appended, the §7 mark → commit →
settle bracket), DELETE
(same bracket), GET (cache-first, remote decrypt, ranges), HEAD/LIST (single-pass twin pairing
under the classification gate; transition marks resolve from the remote), the repair rule on the
read and conditional paths, buckets, auth, `Reconciler` + `KeyLocks`; the slim
`{cetag, plen, mtime}` twin; the §6 pinned-work-factor scrypt envelope + `master_passphrase`
(landed before anything writes a real remote, so quantum-exposed headers never accumulate). Note:
age 0.11.x cannot grease a scrypt sole-stanza header, so `HLEN` is a hardcoded constant (the
`HLEN` pin test guards it) — capture-and-measure and all dynamic-`hlen` code are removed.
*Exit*: integration conformance vs. MinIO — **done** (`hypha/tests/conformance.rs` + `fuzz.rs`;
this pass also caught and fixed a real bug: twin keys carry `0x01`, which XML 1.0 can't represent,
so `delete_twins` must use single-object `DeleteObject`, never the batch `DeleteObjects` whose body
would be rejected — it had broken every durable overwrite/delete of an already-written key).
Remaining: conformance vs. SeaweedFS as the cache backend, and ZeroFS against the durable endpoint.

**Phase 3 — multipart. Done (vs. MinIO).** Trailer + embedded parts table
(single-stream composite read, MAC'd trailer) and the mpu-record retag-match via `ListParts`
(§6/§7) both landed, superseding the original metadata records + completed-object part-index
approach.
Native-remote-multipart proxy (§7): per-part encryption + inline `pmd5`, listable mpu records
(`p{n:05};<retag>;<pmd5>` key-encoded, `pmd5` the sole stored datum) under the reserved prefix;
complete resolves the winning part set via the remote's `ListParts` (retag-matched, geometry from
the remote's sizes), composes the composite ETag, and lands the terminating trailer —
`table ‖ facts ‖ tag ‖ version` — in the same atomic native complete (self-describing, no records
or tags, no *completed-object* part index): as its own part above every client part, or **folded
into the last client part** when that part is under the backend's 5 MiB minimum (this pass added the
fold — a separate trailer part would demote the small final part to an illegal non-final one on any
real S3 backend); abort; the 4 GiB / 9999-part caps; single-stream composite full + ranged GET off
the table; cleanup via batched multi-object delete.
*Exit*: §11 multipart scenarios — restart-mid-upload, re-upload/concurrent-part resolution
(including a small final part), the trailer fold, and trailer-based restore recovery — all covered
in `hypha/tests/multipart.rs` against MinIO.

**Phase 4 — cached mode, single replica.** Marker writes + the in-flight ref count on the PUT
path, the reconcile sweep, cached DELETE propagation, rehydrate (single-part into K + twin
delete; composite into the shadow body). Deployed with one replica and no fencing — a single writer is trivially single, so this ships the
default `s3.internal` deployment with correctness intact, only failover seamlessness missing.
*Exit*: §11 concurrency, marker/reconcile, and eviction-vs-writer suites against real SeaweedFS.

**Phase 5 — GC + restore.** Walk cursor, threshold-ratchet eviction, Bloom ring (fill rotation)
+ slice persistence, usage
source + vacuum, prefix-hint writer, sync marker + parallel restore sweep, debris sweeps (orphan
twins, orphan shadow bodies, leftover transition marks, abandoned mpu
state). *Exit*:
scavenge/rehydrate and cache-wipe → restore-sweep → rehydrate scenarios.

**Phase 6 — `hypha-fence` + active-passive.** Two-pod StatefulSet, leader-elected controller,
lease, fence→confirm→drain→promote, graceful-release fast path. First step: verify the fence
primitives on the live cluster (policy-revision observability, established-connection reset).
*Exit*: the §11 partition harness.

**Phase 7 — chart + operations.** The `hypha/` chart (both workloads, Secrets, `HTTPRoute`, fence
RBAC, per repo networking conventions), dashboards for §10, then the two production installs
(cached + durable). *Exit*: both endpoints live behind the shared Gateway.
