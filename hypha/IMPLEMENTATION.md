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
  `RangeReader` seek adapter; the serving binary bridges itw to async bodies. Standalone so it
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

> **Follow-up (blocked on upstream release):** bump `s3s` to **0.15.0** once it publishes to
> crates.io — the latest release is 0.14.1, which hypha pins. 0.15.0 carries the fix for
> [Nugine/s3s#629](https://github.com/Nugine/s3s/issues/629) (GetObjectAttributes serializes the
> ETag with quotes; AWS's body form is unquoted). On bump: (1) drop the quoted-ETag workaround note
> in §7 *GetObjectAttributes* and in `get.rs`, and flip the `get_object_attributes` tests to assert
> the unquoted value; (2) wire CopyObject's **destination** `If-[None-]Match` preconditions — 0.14.1's
> `CopyObjectInput` predates S3's conditional-copy-on-destination fields, so `copy.rs` today evaluates
> only the `copy-source-if-*` conditions; §7's dest half becomes reachable once the DTO carries the
> fields; (3) re-check the surface for 0.15.0 breaking changes. Not tracked as a task because it
> isn't actionable until the release lands.

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
| Concurrency          | `dashmap` (the §4 key-lock table), `arc-swap` (the §7 bucket state) |
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
  tier.rs                Reconciler: upload / tombstone / twin / restore-sweep primitives (§7)
  bucket_ctl.rs          bucket-control actor: sole writer of the cache substrate; per-bucket restore (§7)
  background.rs          background-transition actor: bounded, deduped, client-cancellable rehydrate/evict (§8)
  s3/                    the s3s::S3 impl, split by op group
    put.rs get.rs list_head.rs delete.rs multipart.rs buckets.rs
    overlay.rs           restore overlay: readiness gate + cache-vs-remote source for reads/writes (§7)
  markers.rs             pending-marker obligations, the clean marker, the recovery scan (§6/§7)
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
writer, the **per-key async lock table** (`keylocks.rs`) is the serialization primitive — a sharded
map (`dashmap`) of weak references to per-key async mutexes, evicted on the last guard's drop, so
the table holds exactly the held-or-awaited keys and no acquisition serializes against an unrelated
key's. It is taken by:

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
are fenced against eviction by §8's remote-generation confirmation and conditional tombstones, not
by the lock.

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
a composite's is the `table ‖ facts ‖ tag ‖ version` **terminating part** — normally its own part
above every client part, and folded into the last client part when no part can follow that one
(§7) — so `CompleteMultipartUpload` stays the single atomic commit of body + facts. Clients get
S3's full 1–10000 part range; the trailer never costs them a part number. `hypha-format` owns the layout, `HLEN`, `MAX_PARTS`, and `MAX_TAIL_LEN`
(the one-shot speculative tail-read size), plus the `encode` /
`decode_tail(footer_key, object_len, tail)` API. DR: the body is stock age, and the plaintext
tail parses directly for facts and boundaries.

### Cache objects

**Two cache buckets, and the client keyspace is clean.** Every deployment maps one client bucket
onto **four** backend buckets (§2): `<data><b>` and `<meta><b>` on the cache, `<remote><b>` on the
remote — the fourth being the client bucket name itself, which never leaves hypha.

- **`<data><b>`** holds *only* client objects: a body at `K`, or a tombstone overwriting `K` in
  place, so a racing GET sees one or the other and never a 404. Nothing hypha-internal lives here.
- **`<meta><b>`** holds everything hypha keeps *about* objects, in three contiguous,
  prefix-separable ranges (below) — the two lowest byte values are inadmissible in client keys,
  which is what makes the split structural rather than probabilistic.

| Range                     | Contents                                              | How it is scanned                                    |
|---------------------------|-------------------------------------------------------|------------------------------------------------------|
| `0x01 0x01 ‖ tag ‖ …`     | mpu state, sync + clean markers, recency slices, shadow bodies | prefix scan per tag                         |
| `0x01 ‖ K ‖ 0x01 ‖ facts` | facts twins                                           | prefix `0x01 ‖ <client prefix>`, delimiter mirrored  |
| `K`                       | pending markers, **bare**                             | `start_after` past the `0x01` block                  |

A twin is `0x01 ‖ K ‖ …` where `K`'s first byte is ≥ `0x02`, so the doubled `0x01` lead cannot
collide with one; the three ranges sort in the order above and never interleave.

This split is what the whole of §7's LIST rests on. Because `<data><b>` contains only client keys,
a LIST page needs no reserved-key filter, carries no twin dilution, and its **last raw key is
always a client key** — which is what makes v1's `NextMarker` expressible at all (§7,
*ListObjectsV2*). It also gives the pending marker a zero-overhead home, so the marker supports the
full 1024-byte key. Markers stay bare and *twins* carry the prefix, deliberately: a twin that cannot
be written costs one HEAD, where a marker that cannot be written costs a namespace scan to
rediscover what it would have named (§7).

**The marker is an index, not the source of truth.** What makes a cached write pending is a state of
the world, not a record hypha keeps: a live body at `K` whose generation the remote does not hold,
or a delete-tombstone at `K` the remote has not yet honoured. Both are derivable from the cache and
the remote alone. The marker's only job is to make that set enumerable in `O(pending)` instead of
`O(keyspace)` — which is why losing one delays durability rather than losing the write, and why the
recovery scan (§7) can rebuild the entire set from first principles.

Splitting markers into a bucket of their own would cost a third: markers and twins are
non-colliding but **interleave**, and the reconcile sweep's `O(pending)` flat LIST (§7) would
become `O(pending + evicted)`, which a pressured cache makes arbitrarily bad. A `delimiter=0x01`
rollup does not rescue it — each twin is its own group, so the response still carries one entry per
evicted key. Prefixing the twins keeps both scans contiguous in one bucket.

Both cache buckets live on the **same volume**: markers and bodies must survive a process crash
together and die together on volume loss, which is what bounds the cached-mode loss window (§7).

**Tombstones** carry fixed 16-byte sentinel bodies, compiled in, one per kind — **eviction** (body
is remote-only, facts in metadata/twin), **delete** (client-visibly absent), and **transition** (K
is mid-bracket, §7; cache facts are distrusted and readers resolve K from the remote). The values
are **random** (CSPRNG-generated once, then fixed), not readable markers: each kind gets a
deterministic (size==16, ETag) pair, so a plain LIST classifies every key with no HEAD, and each
sentinel's constant ETag doubles as a CAS token. Random 16-byte values so no client body collides
with the classification token by accident. In **cached** mode client bodies also live at bare `K` in
`<data>` (durable stores only tombstones there), so the (size, ETag) classification could be
*spoofed* by a client body equal to a sentinel — LIST would hide it, reconcile would reap it. Only a
16-byte body can collide, so **PutObject rejects any body equal to a sentinel**
(`meta::is_reserved_sentinel`) with `InvalidRequest` before it lands — a cryptographically negligible
carve-out (three specific 16-byte values) that keeps the cheap (size, ETag) classifier sound
everywhere it *acts* (reconcile upload/delete).

The check sits **ahead of the mode split**, not on the cached path that needs it: durable mode has no
plaintext at `K` and so nothing to spoof today, but the remote object outlives the mode that wrote
it, and a bucket switched from durable to cached rehydrates that plaintext to bare `K` — where it
*is* the classification, and where landing it would have LIST hide a live key and the recovery scan
(§7) read it as an unpropagated delete and reap the remote object. Enforcing at ingest makes "no
hypha-written object has a reserved sentinel as its plaintext" a property of the whole store rather
than of one mode, so no later path has to re-derive the hazard. PutObject is the only ingest that
needs it: multipart is remote-first in both modes and leaves `K` tombstoned with the plaintext in the
shadow, and copy sources are bodies that already passed this check.

**Facts twins** — a zero-byte object in `<meta><b>` at `0x01 ‖ K ‖ 0x01 ‖ facts`, carrying in its
key name (the one field LIST returns per entry) exactly the facts LIST needs for an evicted key:
the client ETag, the plaintext size, and the original client-write mtime. Both separators sort
below every admissible key byte, which is what makes the twin range **order-isomorphic to the
client keyspace**: for `A < B`, if `A` is a proper prefix of `B` the two diverge where the twin
holds `0x01` and `B` holds a byte ≥ `0x02`, so `twin(A) < twin(B)`; otherwise they diverge on a
byte both keys share. LIST therefore pairs twins to keys by a **merge join** over two cursors
(§7), not by adjacency.

A twin **applies if K's own entry classifies as an eviction tombstone**; against anything else it
is a crash-window leftover, ignored and swept — a live body's facts are native, so a stale twin
can never override them. Pairing is by key equality, so the gate is a direct check rather than a
positional argument. Twins are written in the same locked sequence as their tombstone
(twin-before-tombstone), and every path that replaces an eviction tombstone passes through a live
body or a transition mark first.

**Facts encoding.** `{md5(128) ‖ plen(46) ‖ mtime_ms(42) ‖ part-count(14)}` = 230 bits, packed
big-endian (the top 3 of its 32 bytes are zero) and rendered **base64url, unpadded** — 29 bytes →
**39 chars**, fixed width, via `base64-simd`. Every char is RFC 3986-unreserved, so a twin key
never needs percent-encoding or XML escaping, and the historic hazards are absent by construction:
`/` — a `/` in the facts would make a twin roll up under a delimiter listing and vanish from the
twin cursor (§7); the `+`/space pair — form-style decoders turn `+` into a space, and a literal
space round-trips through the `encoding-type=url` LIST as `+` on some backends, corrupting the
twin key hypha reads back (and then `delete_twins` would miss it); and `\`/`.` — MinIO splits path
components on `\` as well as `/` and rejects any `.`/`..` segment (`XMinioInvalidResourceName`,
surfaced as a 500), so either char in the pseudo-random facts made some twin keys unwritable
there. The part count is 0 for a single-part object (`unpack` rebuilds a bare-MD5
ETag) and `N ≥ 1` for a composite (rebuilds `<md5>-N`), so the twin needn't carry the `-N` literally.

**Twins are optional per key.** Twin overhead is `2 + 39 = 41` bytes, so a key longer than **983**
bytes gets no twin, and its eviction tombstone resolves through the per-key HEAD fallback that
already exists for genuinely-missing twins. This is deliberate: the tombstone's metadata is the
authoritative copy and the twin is only its LIST projection, so a missing twin costs a round trip
and never correctness. Keys containing `0x01` are impossible by admission, so length is the only
condition. The threshold is a **format constant**, not a tunable — changing the facts encoding
moves previously-written twins across it.

> **Why the cap can shrink but never vanish.** Any twin that sorts adjacent to its base must
> *embed* that base, so `|twin| ≥ |K| + |facts|` and some suffix of the key range is always
> untwinnable. Hashing `K` to a fixed width does not escape it: strict order preservation forces
> injectivity, injectivity forces `|output| ≥ log₂|domain|`, and for 1024-byte keys that is
> ~875 bytes of output — order preservation and compression are directly opposed. Order-preserving
> *minimal perfect* hashing reaches `log₂(n)` bits per key but requires a static key set, so one
> PUT would rewrite every twin. The escape hatches are splitting facts across two adjacent twins
> (threshold → ~1004 each, band → 17) or amortizing facts over a run of keys in one object (removes
> the cap, buys write amplification and CAS on shared blocks). Neither is worth it while the
> fallback is correct.

**Key admission** is now S3's own rule plus one structural restriction (`meta::validate_client_key`):
at most **1024 bytes**, and no `0x00` or `0x01` — those two byte values are what the `<meta><b>`
ranges above are built from, and both sort at or below the twin separator, so either would let a
client key fall inside the twin range. Every other byte, control chars included, is permitted:
hypha lists with **`encoding-type=url`** and percent-decodes (`Backend::list`), so keys XML cannot
represent round-trip safely. Enforced at every op that takes a key.

**Tombstone metadata**: every tombstone carries the full facts — kind, `cetag`, `plen`, original
mtime — in its user-metadata, the authoritative copy; HEAD and GET serve from it, and the twin is
its LIST projection. Eviction never changes a key's client-visible `LastModified`: LIST reads it
from the twin, HEAD from the metadata.

**Shadow body** (cached mode): a rehydrated composite's plaintext at
`0x01 0x01 b ‖ sha256(K)[..160 bits]`. The access pattern is a **point lookup** — "is there a
rehydrated plaintext for K" — so the key can be a hash, which removes any length condition; the
eviction path is likewise a point delete driven from K's side. SHA-256 rather than the MD5 already
in the tree: a shadow collision would serve *another key's plaintext*, the worst failure the system
has, so the digest must resist deliberate collision, and a second independent digest of K rides the
shadow's user-metadata to be verified on read (a mismatch is a miss that falls through to the
remote, turning a corruption risk into a cache miss). K itself cannot ride there — a 1024-byte key
percent-encodes past S3's 2 KB metadata ceiling. The tombstone and twin at K stay untouched, so
composite rehydration is invisible to LIST/HEAD and rewrites no twin. Because the shadow key is
deterministic in K, a *later* composite at K overwrites the same shadow — but the key digest alone
can't tell generations apart, so the shadow also carries the rehydrated **client ETag** and a read
serves it only when that equals K's current tombstone `cetag`. A shadow left from a superseded
generation therefore misses and re-rehydrates rather than serving stale bytes under the new ETag.

**The pending marker** (cached mode) lives in `<meta><b>` at **bare `K`** — **one per key**,
body = the body ETag of the most recently acked PUT. Concurrent PUTs overwrite it; last writer
wins — the write-coalescing point: however many PUTs raced, the pending set holds one entry for K
and reconcile uploads the latest cache body. The marker's own S3 ETag (`M_etag`) changes on each
overwrite and is the reconciler's CAS handle.

The marker gets the bare keyspace because it is the one structure here that is a **durability
signal rather than an optimization**: a twin or a shadow that cannot be written costs a round trip,
a marker that cannot be written is data loss. Bare means zero overhead, so every admissible key has
one — no threshold, no degraded case. Marker and body live on the same cache volume: both survive a
process crash, both die together on volume loss. **The marker set is the durability signal**,
enumerable as one flat LIST — `start_after` past the `0x01` block, which is what keeps the sweep
`O(pending)` rather than `O(pending + evicted)`.

Hashing the marker key would be a regression, not an alternative: the sweep's whole efficiency
argument is that one flat LIST yields both K and `M_etag` per pending key, and a hash makes K
unrecoverable from the listing — parking it in the marker body costs a GET per marker per pass, on
a continual duty.

**Multipart upload state**: one record per uploaded part, key
`0x01 0x01 m ‖ <upload-id> ‖ 0x01 ‖ p{n:05};<retag>;<pmd5>;<nonce>`
(empty body) — the part's facts **encoded into the key** so `CompleteMultipartUpload` recovers
them with **one LIST**, no per-part HEAD. The only irreducible datum is `pmd5`, the part's
*plaintext* MD5: hypha hashes it inline during the streaming encrypt and it is never re-derivable,
because the remote only ever sees ciphertext. `retag` is the remote's part ETag (the *ciphertext*
MD5). Everything else the remote can re-tell us — so `ct_len`/`plen` are **not** stored; a part's
plaintext length is `plaintext_len_from(ct_size, HLEN)` over the size the remote reports.

`nonce` is present only for a part that **admits no successor** (§7, *UploadPart*), and names its
retained ciphertext at `0x01 0x01 m ‖ <upload-id> ‖ 0x01 ‖ c{n:05};<nonce>` — the copy complete folds
the trailer into. It is a nonce rather than the `retag` because the retained bytes must be written
*while* the part streams, before the remote has returned an ETag to key them by; carrying it here
costs one field on a record that already exists to disambiguate re-uploads.

Encoding the facts in the key means a re-uploaded part (legal in S3, last-write-wins) writes a
*new* key rather than overwriting, so several records can coexist for one part number. They are
disambiguated at complete by the one authority that already resolved the race — **the remote's own
`ListParts`**, which returns the winning `(n → retag, size)` for the in-progress upload. hypha
matches each winning `retag` to the record carrying it (a ciphertext MD5 ⇒ the match is exact),
takes that record's `pmd5` (and its `nonce`, where a fold needs the retained bytes), and ignores
the losing orphans (swept with the rest of the upload's records). All of an upload's records share
the prefix `0x01 0x01 m ‖ <upload-id> ‖ 0x01`, so that one LIST is a prefix scan and the sweep is a
prefix range.
This is why no hypha-minted version counter is needed: `UploadPart` returns no ordering token, and
a durable monotonic counter across the active/passive pods would be its own distributed problem —
the remote's retag *is* the version. Survives process restarts across a multi-hour upload.

> **Cleanup is deferred to GC (§8), not run inline at complete/abort.** These records carry `0x01`
> (range A), so they can't be batch-deleted (§11 carve-out) — a maxed 10 000-part upload would cost
> that many single-object deletes on the complete/abort path. The cost is real only in that extreme
> and the deletes are pure post-commit cleanup, so the §8 debris sweep reclaims each upload's record
> range (its prefix is self-describing) rather than paying it on the client's critical path. On the
> **local** cache the request-count saving a batch delete would buy is small anyway (sub-ms RTT), so
> deferral — not batching, and not a third bucket to make the keys XML-safe — is the right lever.
> Until phase 5 lands the sweep, complete/abort still drop the range inline as a fallback.

**The sync marker**: an object at `0x01 0x01 s`, present iff a namespace
reconciliation has completed — namespace trust recorded in the cache itself, dying with the
volume by construction. Present ⇒ reads are cache-authoritative and an absent key is a definitive
404. Absent ⇒ the remote is the read source of truth until the restore sweep rewrites it (§7).

**The clean marker**: an object at `0x01 0x01 c`, per bucket, encoding one claim —
*no un-indexed write has happened in this bucket since the last completed drain*. Present ⇒ the
marker range is an exhaustive account of the pending set — **complete, not empty**: pending markers
alongside a clean marker are the ordinary steady state. Absent ⇒ a write may have landed without its
marker, and the bucket owes a recovery scan (§7). Like the sync marker it lives in the cache, so it
dies with the volume, and its write is subject to the same fence as any other — a fenced replica
cannot forge one, so a hard failover always scans and a graceful handover never does.

> **Dirty is the default, and the default is established before anything can run.** *At startup*,
> every bucket's marker is read and then **deleted — all of them, before the first request is
> served**, so from the instant hypha can take a write, no bucket on disk claims to be clean. There
> is no bookkeeping of which buckets a run "touched", because a run that has to remember what it
> touched can forget. A bucket whose marker fails to clear is not served at all: skipping the scan on
> a marker one then fails to delete would skip it again next run, by which time real orphans exist.
> *At the other end*, the marker is written at exactly one point in the drain (§7), so every path
> that does not reach that line leaves it absent — crash, panic, task abort, `SIGKILL` past the grace
> period, an error nobody handled, a code path nobody has written yet. A crash can happen anywhere,
> and everywhere it happens the answer is absent, which costs a scan rather than a missed write.
>
> The drain must therefore *earn* the marker per bucket rather than fail to disqualify one, and what
> it earns it with is not "was this bucket written to" but **does this run account for the bucket's
> pending set**: either its marker was present at startup, or this run's recovery scan rebuilt it.
> That is the run's only per-bucket state, and it is a membership rather than a flag — a bucket left
> dirty by an earlier crash and untouched by this run is simply not in the set, and must end this run
> dirty too, since its orphans are still unindexed and a clean marker would bury them permanently.
> The one other condition is not per-bucket at all: a marker still owed when the drain seals means
> the run did not end gracefully, and **no** bucket is marked clean.

**Recency slices**: sealed Bloom filters under `0x01 0x01 r ‖ …`, the persisted form of
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

**Cached** — the write lock covers steps 1–2 for conditional PUTs; unconditional PUTs take no
lock:

1. *(conditional only)* resolve + evaluate as above.
2. **Commit**: `PutObject` plaintext at K — the cache computes the ETag natively. **Ack.**
3. Hand `(K, body ETag)` to the marker queue, which overwrites the single marker at `<meta><b>`'s
   bare `K` (last writer wins — the coalescing point, §6). The remote trails via the reconcile sweep.

The hand-off is a channel send that cannot block or fail (below), so it costs the ack nothing and
adds no failure mode to it. Writing the marker *inline* before returning would put a second round
trip on every write's critical path, and doing it inline *after* the ack is not expressible — the
response is sent when the handler returns, so anything after that point is another task, which then
has to be joined before the run can claim anything about what landed.

> **The ack is the commit, and the commit is the body write.** The marker follows immediately and
> virtually always lands, but the ack does not *depend* on it — because acking only
> after the marker leaves no good answer when the marker write fails. The body is already live and
> client-visible by then, so returning an error means either abandoning it — a live object the client
> was told does not exist, never durable, never reconciled — or rolling it back, which is destructive
> (deleting K hides a perfectly good remote generation, and the delete races the deliberately
> unserialized unconditional PUT path). Repairing it *behind* a returned error is worse than both:
> hypha would then finish a write it reported as failed, which is exactly the outcome a client
> reasoning from that error — retrying elsewhere, or relying on `If-None-Match` to create once —
> cannot survive. Acking the commit and owing the index is the only option that keeps the ack
> honest, and it costs nothing the marker was carrying: the marker never *was* the durability
> record, the body is (§6).

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
   answer 404 and LIST omits K immediately. **Ack.**
2. Hand K to the marker queue (its pending op is now a delete). Reconcile propagates below; the mask
   is what keeps a crash from resurrecting K from the remote before the delete propagates.

Same ack rule and same reason as the cached PUT: the tombstone is the commit, and a delete-tombstone
the remote has not yet honoured is self-describing without its marker.

### DeleteObjects — non-atomic batch

A fan-out of ≤ 1000 independent single-key deletes, **never a raw backend batch over client
state** — each key needs its own mask, and the S3 contract is per-key (`Deleted`/`Error` list,
`Quiet` ⇒ errors only; deleting an absent key is a *success*; `VersionId` ignored — versioning
exempt). The invariant is per-key, so batching is a transport question, not a correctness one.

**Durable** — the transition bracket widened from one key to the batch, so the *remote* leg
collapses to one native call while the cache leg stays per-key:

0. **Admit**: a key hypha rejects (forbidden byte, over-long) fails *per key* —
   an `InvalidArgument` entry in the reply — and never reaches the bracket; the rest of the batch
   still commits. Keys are **deduplicated** for the bracket: a key repeated in one request would
   otherwise wait on the lock it already holds. The reply is still built per *requested* entry, so
   a repeat gets the same verdict twice.
1. Acquire the batch's write locks in **sorted key order** (two overlapping batch deletes can't
   deadlock; single-key ops take one lock and queue); repair any leftover marks.
2. **Mark** each K — per-key transition tombstones.
3. **Commit**: one native remote `DeleteObjects` over all keys → per-key `Deleted`/`Error`
   (NotFound ⇒ success).
4. **Settle**: each remote-confirmed success removes its cache entry + twins (twins single-object,
   §11 — `0x01`); a remote error **leaves the mark** (an indeterminate outcome the repair rule
   resolves, §7) and records the client error. Release locks. Return the aggregated result.

Each key still has exactly one atomic remote commit (its slice of the batch); the batch being
non-atomic across keys *is* the S3 contract. A crash mid-batch is identical to a single-key
crash — every marked key is repaired on restart. The batched remote body is always XML-safe: twins
never reach the remote, and any client key that arrived *inside* the request was XML-representable
by construction, so it re-encodes into the outbound body (a control-byte key from a percent-encoded
PUT path can appear in neither the client's request nor hypha's batch — it is single-`DeleteObject`
only).

**Cached** — the remote isn't touched here, so there is nothing to batch: per key, under its write
lock, overwrite K with the delete-tombstone and overwrite its marker (as single `DeleteObject`).
Reconcile propagates below and is where the remote batching opportunity lives — a sweep coalesces
its pending *delete* markers into native remote `DeleteObjects` calls (same XML-safe argument,
client keys only).

### Multipart — one path, both modes

Parts route **around the cache** onto the remote's own native multipart upload at K (a part
isn't readable until commit; multipart is throughput-bound, so the cache's latency win doesn't
apply).

**CreateMultipartUpload**:

1. Validate the key; create the native upload on the remote; record the upload (its client key)
   in the mpu state (§6).

**UploadPart**:

1. Reject part numbers outside S3's **1–10000** and plaintext > **4 GiB** (so the envelope never
   pushes a part past the remote's 5 GiB part cap; transparent re-splitting is a later refinement).
   The 4 GiB cap is also what leaves room for a folded trailer (below): 4 GiB of plaintext frames to
   ~4.3 GB, ~1 GiB clear of the part ceiling, so `part ‖ trailer` never overflows.
2. Encrypt the part as **its own pure age file** (fresh file key; Content-Length computed from
   `HLEN`), streaming to the remote as the native part; the plaintext MD5 is computed inline.
3. **Retain the ciphertext if the part admits no successor.** Two conditions, one meaning — a part
   below the backend's **5 MiB minimum** (which any S3 backend permits only as the upload's *final*
   part) and part number **10000** (which nothing can follow) can each only ever be the last part.
   So if such a part lands in the committed set it is the object's tail, and it is the part that
   must carry the terminating trailer; complete cannot re-derive its bytes, because an in-progress
   upload's parts aren't readable. Retaining it in the cache mpu state is what makes the fold
   possible. The encrypted stream is **split** and driven into the remote and the cache in one
   pass — no buffering, and no size distinction, so a 4 KiB retained part and a 4 GiB one take the
   same path with per-request memory bounded by the pipe. This is the one place durable mode's
   cache transiently holds more than tombstones and twins: one part per upload, dropped at
   complete/abort.
4. Persist the part's `pmd5` (with `retag`, and the retained ciphertext's `nonce` if step 3 fired)
   as a key-encoded mpu record (§6) — an in-progress upload's parts aren't readable, so complete
   needs its own copy of `pmd5`; its loss with the cache volume merely fails the eventual complete
   (never-acked, client retries).
5. Ack on the remote's part ack. Out-of-order / parallel / re-uploaded parts and concurrent
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
   object's final bytes. The trailer's content is identical either way; only its placement varies,
   on the one question of whether a part can follow the highest client part:
   - **It can** (highest part < 10000 and at or above the 5 MiB minimum) — the trailer rides its
     **own part** at highest + 1.
   - **It cannot** — the highest part is either below the minimum (necessarily the client's last,
     as any backend rejects a smaller non-final part, so a separate trailer part would demote it to
     a sub-minimum non-final part the native complete rejects) or is part 10000 (nothing can follow
     it). Both mean the same thing, and take the same remedy: **fold** the trailer into that part —
     re-upload it as `part ‖ trailer`, keeping it final. This is why UploadPart retains exactly
     these parts' ciphertext (§7, *UploadPart* step 3): an in-progress part can't be read back. The
     fold takes the retained copy the remote's `ListParts` winner points at — winning `retag` → mpu
     record → `nonce` → retained ciphertext — so a re-uploaded part folds *that* generation and
     never a divergent cache last-writer.

   The committed object K is **byte-identical** either way — the same `age₁…ageₙ ‖ trailer`
   concatenation — so the read path is unaffected.
3. **Mark** K.
4. **Commit**: native complete on the remote — one atomic op lands the concatenated body *and* its
   facts at K. Its part set is either the client parts plus the separate trailer part, or (folded
   case) the client parts with the trailer riding the last one.
5. **Settle**: eviction tombstone + twin. Ack. The mpu state is left for the §8 debris sweep to
   reclaim (deferred, not inline — see *Multipart upload state* in §6); a phase-4/5-less build drops
   it inline as a fallback.

Crash before 4: K untouched; the dangling native upload (trailer part included) is an orphan
(swept, aborted). Crash after 4: committed — marked readers serve it from the remote, and
repair (or, after a simultaneous cache loss, the restore sweep) reads the facts off the tail
trailer; lost-ack. In cached mode the composite enters the cache lazily on first GET via
rehydrate (§8); in durable mode it stays tombstoned like everything else.

**AbortMultipartUpload**: native abort on the remote; the mpu state is left for the §8 debris sweep
(deferred, as at complete), which reclaims it alongside the records of uploads abandoned without
either complete or abort.

**UploadPartCopy** (both modes — the multipart path): copy-source is just an alternate byte source
for `UploadPart`. The `CopyObject` ciphertext-reuse trick does **not** generally apply — every part
is its own independent age file (above) and a `copy-source-range` cuts age chunk boundaries — so the
baseline is the **re-encrypt path**: GET the source, decrypt, apply the range over *plaintext*,
then encrypt that range as a fresh per-part age file with inline `pmd5`, stream it as the native
part, write the mpu record — identical to a normal `UploadPart` past the byte source. *One
optimization:* a whole (unranged) **single-part** source is already one age file, so
`UploadPartCopy` its body range `[0, body_ct_len)` server-side (trailer excluded) with
`pmd5 = source cetag`; composite sources and any ranged copy re-encrypt. Same caps as `UploadPart`
(plaintext ≤ 4 GiB, part ≤ 10000, 5 MiB non-final minimum). The server-side fast path also declines
whenever the part *admits no successor* (final-and-under-5-MiB, or part 10000): complete's trailer
fold re-uploads that part as `part ‖ trailer`, which needs its ciphertext retained in the cache
stash — and a server-side copy never routes the bytes through hypha, so nothing is there to stash.
The re-encrypt path's tee produces the stash as a side effect, so those parts take it instead; the
choice is invisible to the client (the part lands identically either way).

**ListMultipartUploads** (both modes): **proxy the remote's own** `ListMultipartUploads`. The
remote is already the source of truth for multipart, and hypha's mapping is transparent enough that
no translation is needed: the native upload is created *at the client key*, and the remote's upload
id is what hypha handed the client at create. So a remote page already carries
`Upload{Key, UploadId, Initiated}` verbatim, in S3's own `(key, upload_id)` order, with
`prefix`/`delimiter`/`key-marker`/`upload-id-marker`/`max-uploads` forwarding natively — the
key-position pagination guarantee is the backend's, not something hypha synthesises.

Remote-as-truth is also what makes the two crash windows resolve correctly for free: an upload whose
remote create landed but whose cache `/u` record didn't (create writes the remote first) genuinely
exists and is abortable, so listing it is right; and a `/u` record whose remote upload was aborted or
lifecycle-expired out from under it must *not* be listed, which a proxy does by construction. The
cache records are consulted only where the remote cannot answer — `pmd5` at complete and `ListParts`
(§6) — never for existence.

`StorageClass` is reported as the remote gives it (`STANDARD`); the class the client requested at
create lives in the `/u` record and would cost a per-upload fetch to recover — the same cosmetic
corner LIST already accepts for objects (§7, *Storage class*).

*Backend caveat.* `prefix`/`delimiter` are S3-specified here and forward natively, but **MinIO does
not implement them** — it matches only a prefix equal to a key, closed "working as intended"
(minio/minio#20989, #11686) — so a prefixed listing against MinIO returns empty. hypha forwards
rather than emulating: the filter is the remote's to answer, and a deployment whose remote is
MinIO simply doesn't get it. The §11 prefix test is `#[ignore]`d for that reason, since the
integration harness runs MinIO.

*Filtering (from the CopyObject phase on).* Every remote upload in a client bucket is a client
upload today, so the proxy needs no filter. `CopyObject`'s large-body path (§7) creates a transient
native upload at `K_dst`, which would otherwise surface here while the copy runs; from that point the
page is filtered against the client uploads hypha knows about — **one** cache LIST of
`0x01 0x01 m` with `delimiter=0x01`, whose common prefixes carry the upload ids in the key
names, so membership is a set test with no per-entry fetch.

**ListParts** (both modes): proxy the remote's `ListParts` (the winning `(n → retag, size)`),
match each `retag` to its mpu record for `pmd5`, and emit `Part{PartNumber n, ETag = hex(pmd5),
Size = plaintext_len_from(size, HLEN), LastModified}`; the trailer part, when it rides its own (> every client part), is
filtered, re-uploaded duplicates resolve exactly as at complete (§6), and `part-number-marker`/
`max-parts` forward the remote's pagination.

### CopyObject

Copy never re-encrypts a large body and never routes plaintext through the client leg: the age body
ciphertext is **key-independent** (per-file keys, §6), so `age₁…ageₙ` is reusable verbatim across
keys — only the trailer, whose MAC binds `object_key` (§6), is re-minted for the destination. That
binding is not an obstacle so much as the one step of a copy an untrusted remote can't forge:
re-stamping needs `footer_key`, which only hypha holds. A naive remote-side `CopyObject` of the
stored object would carry a `K_src`-bound trailer and fail verify at `K_dst`.

Preconditions split across both keys: `x-amz-copy-source-if-[none-]match` evaluate against the
**source's** current client ETag (resolved as any conditional read resolves it — live-body /
tombstone `cetag` / absent, §4), `If-[None-]Match` against the **destination** as in a normal PUT.
`x-amz-metadata-directive: COPY` carries the source user-metadata forward, `REPLACE` takes the
request's — the same-key + `REPLACE` in-place metadata edit is just a copy onto K. The destination
ETag is the source client ETag unchanged (content-derived, §4) and `plen` carries over; only
`LastModified` moves to now, and since the trailer is re-minted anyway its `mtime` is set with it —
no stale-mtime restore corner.

**Durable** — under `K_dst`'s write lock, the §7 mark → commit → settle bracket with the body
sourced from the remote instead of the client:

1. Resolve + evaluate both preconditions; repair a leftover mark on either key first. One bounded
   `MAX_TAIL_LEN` tail GET of the **source's** remote trailer (MAC-verified at `K_src`) yields
   `{cetag, plen, count, table}`; the source body-ciphertext length is the source object length
   minus the decoded trailer length.
2. **Mark** `K_dst`.
3. **Commit** — one atomic remote op landing `body ‖ fresh-trailer` at `K_dst`:
   - **Large body** (source body ct ≥ the backend's 5 MiB part minimum): a native multipart at
     `K_dst` — **`UploadPartCopy`** the source range `[0, body_ct_len)` as one or more parts, each
     ≤ the 5 GiB part cap and split so the **last** copied part stays ≥ 5 MiB (always possible once
     the total clears the minimum), then a freshly built **trailer part** re-MAC'd over `K_dst` with
     `mtime=now` as the sole final part, then `CompleteMultipartUpload`. The copy is remote→remote
     (no bytes through hypha); the range excludes the source trailer, so single-part and composite
     sources copy identically and a composite's offset table carries over untouched — offsets are
     body-relative, hence key-independent. The trailer is always the final part, so multipart's small
     final-part **fold** never arises here.
   - **Small body** (below the part minimum — `UploadPartCopy` can't stand as a non-final part and a
     copy-part can't absorb the trailer): the re-encrypt path — source GET → decrypt → one streaming
     `PutObject` at `K_dst` with the fresh trailer inline. Cheap precisely because the body is small.
4. **Settle**: eviction tombstone + twin at `K_dst` (`cetag`/`plen` from the source, `mtime=now`).
   Ack.

Crash mirrors durable PutObject: before the commit lands `K_dst` is untouched and the dangling
native upload is a swept orphan; after, it's committed and repair completes the projection off the
tail trailer. Every remote-side step reads only source ciphertext, so a mid-copy crash never exposes
plaintext nor tears the source.

**Cached** — copy produces a hot plaintext body at `K_dst`; the reconcile sweep re-encrypts and
mints the `K_dst`-bound trailer on upload, so key-binding falls out of the normal PUT path with no
special handling. Under `K_dst`'s write lock for the conditional case:

1. *(conditional only)* resolve + evaluate as above.
2. **Commit** — land a plaintext body on the cache at `K_dst`:
   - **Source hot** (live cache body): a **cache→cache** server-side `CopyObject` on SeaweedFS —
     plaintext, same volume, ETag preserved natively, zero bytes through hypha.
   - **Source cold** (evicted / shadow): rehydrate from the remote (§8), then the cache→cache copy;
     equivalently source GET → decrypt → cache `PutObject` at `K_dst`.
3. Ack — same rule as the cached PUT — and hand `K_dst` to the marker queue (last-writer-wins
   coalescing, §6). The remote trails via reconcile.

An unconditional cached copy takes no lock, racing on the cache like an unconditional PUT (§4).

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

### GetObjectAttributes

A read projection over the **same key-state dispatch as HEAD** (live-body / eviction-tombstone /
durable-always-remote / cached-shadow-probe), returning only the requested
`x-amz-object-attributes`: `ObjectSize` = `plen`; `ETag` = client ETag; `StorageClass` = the stored
class (below). `ObjectParts` for a composite comes **straight off the trailer's offset table** — one
bounded MAC-verified tail GET gives the part count (the ETag's `-N`) and per-part *plaintext* sizes
via the closed form, paginated by `max-parts`/`part-number-marker`, with **no remote part index**;
this is the capability that let §11 drop `GetObjectAttributes`/`HEAD partNumber` as a *remote
backend* requirement — hypha now supplies it from the trailer. A single-part object reports no
`ObjectParts` (S3 omits it for non-multipart objects). `Checksum` is omitted (deferred).

> AWS returns this ETag **unquoted** in the GetObjectAttributes body (unlike the quoted HTTP
> header), but s3s 0.14.1 quotes every `ETag` DTO value uniformly, so hypha emits it quoted. This is
> an acknowledged upstream bug — [Nugine/s3s#629](https://github.com/Nugine/s3s/issues/629), fixed
> for v0.15.0 (unreleased as of the current pin) — not a hypha defect; it resolves on the next s3s
> bump. Harmless meanwhile: every S3 client trims ETag quotes.

### ListObjectsV2

**Two cursors, merged.** Facts twins live in `<meta><b>` (§6), not beside their keys, so a page is
assembled from two listings issued **concurrently** against the same cache backend:

- the **client cursor** over `<data><b>` — `prefix`, `delimiter`, `max_keys`, and the client's
  position, all forwarded unchanged;
- the **twin cursor** over `<meta><b>` at `prefix = 0x01 ‖ <client prefix>`, **the same delimiter**,
  and the client's position likewise prefixed.

The delimiter mirrors exactly because the twin range is order-isomorphic to the client keyspace
(§6) and the facts alphabet excludes `/`: `twin("a/1")` rolls up into common prefix `0x01 ‖ a/`
just as `a/1` rolls into `a/`, while `twin("top")` stays a content entry just as `top` does. So the
twin cursor's shape tracks the client cursor's, and no scan runs past a rolled-up group.

1. Classify each client-cursor entry from its (size, ETag) sentinel pair (§6): **live body** →
   native facts (any twin is stale — ignored); **eviction tombstone** → its twin's
   `{cetag, plen, mtime}`, matched from the twin cursor **by key equality**, with a per-key cache
   HEAD fallback when the twin is missing (crash window, page straddle, or a key over the §6
   twin threshold); **delete-tombstone** → omitted; **transition tombstone** → per-key *remote*
   HEAD (the one classification that leaves the cache).
2. **Single page, forwarded pagination.** Delete-tombstones are dropped, so a page can still return
   **fewer** than `MaxKeys` client entries — a short page, valid S3 as long as `IsTruncated` and the
   resume position are honest. hypha forwards the client cursor's **own** truncation flag, and
   `KeyCount` reports the entries actually emitted. LIST deliberately does **not** coalesce pages to
   fill `MaxKeys`: any such backfill would resume either by reusing a backend cursor across requests
   or by a client-entry count, and both weaken S3's key-position guarantee — a concurrent
   insert/delete in the re-listed range could dup or drop an untouched key. Short pages are the
   accepted cost; a client follows the position until `IsTruncated` is false.

   Dilution is gone with the twins: pages are short only where keys were *deleted*, not for every
   evicted key, so the effect is now the exception rather than the norm.

**ListObjects (v1)** reuses the classifier and the two-cursor merge verbatim (s3s does not translate
v1→v2, so it is its own method); only the pagination shell differs — request `marker`, response
`NextMarker` = **the last raw key of the client-cursor page**. Cache-served, both modes identical.

That expression is only available because `<data><b>` holds nothing but client objects: the last
raw key is always a client key, so it is XML-representable, strictly increasing, and skips nothing.
It is worth recording why the pre-split layout could not express it at all, since the failure is
not obvious and both halves were observed:

> **Why v1 needs the split.** v2 forwards an **opaque** continuation token; v1's `NextMarker` is a
> **key**. With twins interleaved at `K ‖ 0x01 ‖ …`, neither candidate worked. *The raw page end*
> can be a twin, whose `0x01` is not a legal XML character — the client's parser rejects the whole
> response. *The last returned client key* (or a trailing twin mapped back to its base) makes a page
> of purely filtered records reproduce the previous marker and loop forever: at `MaxKeys=1`, page 1
> returns `K` and marker `K`; page 2 is `K`'s twin alone, emits nothing, and yields marker `K`
> again. And no third choice existed — a valid marker must satisfy `all twins ≤ S < all client
> keys`, but twins occupy `K ‖ 0x01 ‖ …`, so every candidate begins `K ‖ 0x01` or `K ‖ 0x02`, none
> XML-representable. The same argument rules out a sentinel *below* the client keyspace generally:
> the smallest possible client key is the single byte `0x02`, so any `S < "\x02"` must begin `0x00`
> or `0x01`. This is why the reserved ranges sort at the bottom of `<meta><b>` and are skipped with
> an internal `start_after` constant — a *request* parameter, which carries arbitrary bytes freely —
> rather than being filtered and patched over in a response.

### Buckets

One client bucket maps to **three** backend buckets: `<data><b>` and `<meta><b>` on the cache,
`<remote><b>` on the remote, each `Backend` prepending its own configured prefix (§2, §6). The
**remote is the sole source of truth for bucket existence** and bucket ops are **always durable**
(synchronous to the remote regardless of mode). The two cache buckets are a **rebuildable
substrate**: they only host object-side state (bodies and tombstones; twins, markers, mpu records),
never the authority — so, exactly like an object body, a missing one is *repaired rather than
trusted*. Rare control-plane events — no markers.

**Bucket-name budget.** S3 caps a bucket name at **63 characters** and the prefix is charged
against it, so the client-visible cap is `63 − max(prefix length)`. Prefixes should therefore be
short (`d-`, `m-`, `r-` ⇒ 61) and the effective cap validated up front with `InvalidBucketName`,
rather than surfacing as an opaque backend error. Two configuration invariants, checked at startup
(§9): no prefix may be **empty** when backends share an endpoint — three buckets cannot occupy one
name — and no prefix may be a **prefix of another**, or `ListBuckets`' strip-and-filter
mis-classifies and client buckets leak or vanish.

**The substrate is restored, not assumed — one actor owns it.** The cache runs unreplicated and
durability lives only on the remote, so `<remote><b>` routinely outlives its cache buckets (a lost
cache volume, a pre-restore boot, a partial lifecycle op). By assumption a bucket's cache is lost
**whole or not at all** — never partially — so its per-bucket **sync marker** (§6, `0x01 0x01 s` in
`<meta><b>`) is a trustworthy all-or-nothing readiness signal: marker present ⇒ the projections
survived intact and the cache is authoritative; marker absent ⇒ the cache is not authoritative and
the remote is the read source of truth until a restore rebuilds it. All cache-substrate mutations —
CreateBucket, DeleteBucket, provisioning, and restore — are funnelled through a **single
bucket-control actor** fed by a non-blocking unbounded queue, so the actor is the *sole writer* of
the cache buckets and their serialization is **structural, not lock-based** (no per-bucket locks).
The actor runs **per-bucket-serial, cross-bucket-parallel**: a worker drains one bucket's requests in
arrival order while distinct buckets proceed concurrently, bounded by a global concurrency cap.

Three request classes ride the queue:

- **Client CreateBucket / DeleteBucket** — request-reply, serialized per bucket, **never coalesced**:
  each returns the remote's own result, because a double-delete's loser must see `NoSuchBucket` and
  a create must not merge with a same-name delete. The caller pushes (non-blocking) and awaits its
  reply.
- **Provisioning** — request-reply and **coalesced by waiter list**: the data plane needs the
  `<data>`/`<meta>` projections to exist before a write to an unreconciled bucket can materialize its
  key, and after a lost volume they do not. Writers therefore ask the actor rather than each
  creating the buckets themselves, which would put a head+create pair on the backend per request —
  the very flood the actor exists to absorb. Concurrent first-callers for a bucket attach to one
  in-flight round; once it lands, the memoized answer is a set lookup that never reaches the queue.
  Unlike the other classes this runs *outside* the per-bucket worker, because that worker is
  occupied by the restore sweep the write is racing and serving is never gated (below). Safe there
  only because it exclusively creates, idempotently, and only for a bucket the readiness probe has
  already seen on the remote — bucket *lifecycle* stays the workers' alone.
- **Restore (repair)** — fire-and-forget and **coalesced by dedup**: the first op to find a bucket
  unreconciled (marker absent, remote present) kicks a `Restore` and resolves itself from the remote
  meanwhile — no 503, no waiter list. That op also memoizes the `Restoring` verdict, so the
  classification costs two probes *per restore* rather than per request crossing the window; the
  actor drops the memo when the sweep ends, however it ends, which is what re-triggers a failed
  sweep (and lets a bucket deleted meanwhile resolve `Absent`) on the next access. A success
  publishes `Ready` before dropping it, so the gate never sees a bucket as neither. The actor
  ensures the projections exist, LISTs the remote and rebuilds each object's eviction tombstone +
  twin from its authenticated tail trailer
  (`Reconciler::restore_bucket`, the per-bucket restore sweep below), then writes the marker to flip
  the bucket cache-authoritative. Idempotent, so a crash mid-sweep resumes by re-running; duplicate
  restores collapse to one.

**The restore overlay** keeps serving ungated while a bucket is unreconciled (one interface,
`s3/overlay.rs`): a readiness verdict (memoized in both directions — `Ready` once the marker is
observed, `Restoring` for as long as a sweep is pending) selects each op's source.
Reads resolve a key's facts — and a LIST page's entries — from the cache tombstone namespace once
`Ready`, or straight from the remote (facts off each object's tail trailer, common prefixes and
pagination passing through the same client keyspace) while `Restoring`; an `UploadPartCopy` source
resolves as a read of its own bucket. A write to a `Restoring`
bucket asks the actor to provision the projections (coalesced, above) and then materializes its key
from the remote into the cache under the key lock, so the normal §4 bracket runs against a correct
tombstone. Restore is **lazy** — triggered on first access, not a
startup scan — so a warm cache pays nothing and only touched buckets are rebuilt. On shutdown the
actor **drains its queue first** (pending client Create/Delete complete); in-flight restore is soft
state, re-triggered on the next access.

- **CreateBucket**: routed to the actor. When the remote bucket is absent it resets the cache
  substrate (drain any stale orphan, provision empty projections), creates the remote — the **sole
  commit** — and writes the marker (a fresh empty namespace is trivially reconciled → immediately
  `Ready`). A duplicate create of a live bucket returns the remote's result and leaves cache and
  marker untouched (it may be mid-restore).
- **DeleteBucket**: routed to the actor. It deletes the remote first — the commit that makes the
  bucket cease to exist, and the **emptiness gate** (the remote holds every committed object, so a
  non-empty bucket is rejected here) — then best-effort drains and deletes both cache buckets and
  clears the ready set. A failure or crash after the remote delete leaves a cache-without-remote
  orphan a later restore/GC drops — never a remote bucket the client believes is gone. Leftover
  twins/markers/marks are hypha's own state: drained, never allowed to block the delete.

**Restore follow-ups** (open, deliberately deferred):

- **Mid-life cache loss isn't re-detected without a restart.** The ready set memoizes `Ready`
  permanently, so a volume that dies under a *running* active keeps resolving `Ready` and its ops
  fail hard (cache `NoSuchBucket`) rather than re-restoring. This matches the "cache volume loss ⇒
  discard and restart" operational model (§4/§8) — a restart clears the memo and the overlay
  restores lazily — but a running active does not self-heal a live volume loss. Revisit if cache
  loss should recover without an operator restart (e.g. invalidate the memo on a cache
  `NoSuchBucket` from a supposedly-`Ready` bucket).
- **Restore rebuilds object tombstones only, not multipart state.** `restore_bucket` reconstructs
  the object namespace from remote objects + trailers; in-flight multipart records (`<meta>` range
  A) are *not* rebuilt, so `ListParts`/`CompleteMultipartUpload` for an upload started before a
  cache loss won't find its records after restore. Remote-as-truth for `ListMultipartUploads` (§7)
  covers upload *existence*, not per-part cache state. Fold mpu-record restore in with the Phase-4/5
  reconcile work.
- **The overlay's `Restoring` arms are durable-only.** Reads resolve from the remote and writes
  materialize-then-write with no cached-mode **pending overlay** (acked-but-unuploaded PUTs / pending
  deletes), so read-after-write does not hold mid-restore in cached mode. Deferred to the **Phase-5
  restore** work (restore itself is Phase 5): extend `s3/overlay.rs`'s `Restoring` branches with that
  overlay. Phase 4 covers cached steady state on a `Ready` bucket — its rehydrate machinery is what
  Phase-5 restore then drives.
- **A v2 LIST paginating across the restore flip mixes token domains.** A page fetched while
  `Restoring` forwards the *remote's* continuation token; if the bucket flips `Ready` before the
  next page, that token is fed to the *cache* backend, and opaque tokens aren't formally
  interchangeable. Tokens are key-position-encoded on every backend hypha targets, so in practice
  the flip resumes at the right position or errors once (the client re-lists); v1 is immune (its
  marker is a key). Accepted: hypha-minted resume tokens would add a cache-backend `start_after`
  requirement for a once-per-bucket-lifetime window.
- **ListBuckets**: remote-served, filtered to this deployment's remote prefix and stripped back to
  client-visible names — the cache prefixes never match, so cache buckets cannot leak into the
  listing even when both backends share one account.
- **HeadBucket / GetBucketLocation**: remote existence check; the latter reports the deployment's
  configured backend region.
- **GetBucketVersioning**: a benign stub — an empty `VersioningConfiguration` (no `Status`,
  `MFADelete: Disabled`), no backend call, hypha buckets never carry versioning. Load-bearing for
  compatibility: `aws s3 sync` / boto / `mc` probe it up front and a 501 aborts them where
  "not enabled" passes. Enabling it (`PutBucketVersioning`) stays exempt — rejected.

### User metadata (passthrough, both modes)

Client `x-amz-meta-*` rides the cache object's user-metadata alongside hypha's own facts, so the
two are namespaced apart: hypha's keys stay bare and the client's take a `u-` prefix, which is not
a prefix of any hypha key. The *client* wire leg is RFC 2047 for non-ASCII — s3s encodes and
decodes it, so hypha only ever handles decoded values; MinIO does the same on its own client leg,
while `aws-sdk-s3` neither encodes nor decodes, which is why a round-trip through the SDK shows the
encoded-word form.

**The carrier is capped at S3's 2 KB for all user metadata, and hypha shares it with the client**,
so the at-rest escaping is deliberately narrow: controls and `DEL` (illegal in a header value),
space (SigV4 canonicalization collapses runs of whitespace, so `a  b` would sign as `a b`, and edge
whitespace is trimmed), and `%` (what keeps the encoding self-delimiting). Non-ASCII always escapes.
Ordinary ASCII therefore passes through **unchanged**. The earlier set — everything outside
`[A-Za-z0-9]` — inflated typical values up to 3× and put hypha's effective client budget at roughly
a third of S3's: an invisible conformance shortfall of exactly the kind the key-length cap was.

The remote's sole facts carrier is the trailer (§6), which holds facts and nothing else, so a
repair or restore that rebuilds K from the remote settles user metadata and storage class back to
their defaults — the accepted durability limit of this carrier.

### Storage class (passthrough, both modes)

hypha has one physical tier, so a storage class is an echoed label. On PUT / CopyObject /
CreateMultipartUpload: read `x-amz-storage-class`, **reject the archive family**
(`GLACIER`/`DEEP_ARCHIVE`/`GLACIER_IR`/`SNOW`/`OUTPOSTS`) with `InvalidStorageClass` (they imply
`RestoreObject`), accept the rest, default `STANDARD`; persist it on the **same user-metadata
carrier** as `x-amz-meta-*` and echo on HEAD / GET / GetObjectAttributes. Two accepted cosmetic
corners: **LIST reports `STANDARD`** for every key (the twin's packed facts carry only
`{cetag, plen, mtime, count}`; per-object class would mean a twin-format change), and a cache-loss restore falls the class back to
`STANDARD` (the user-metadata carrier's durability limit).

### Background: the reconcile sweep (cached mode)

The upload path for acked cache writes — a continual duty of the active (phase 4,
`replication.rs`). Each pass:

1. `ListObjectsV2` `<meta><b>` with `start_after` past the `0x01` block (§6) — the markers are the
   bare-`K` range above it, so this is one entry per pending key, `O(pending)` over local NVMe and
   never `O(evicted)`; each yields K directly from the key name and the marker's own ETag `M_etag`
   (the CAS handle).
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

**The marker queue.** Every acked cached write hands its marker here (§7, *PutObject*) and a single
worker writes them, coalesced per key and retried for the life of the process. Order between markers
for one key does not matter — the sweep classifies K from the *data* body and CASes on the marker's
own ETag, so a marker's payload is diagnostic and any of them is as good as the last — but the writes
do run concurrently, since a marker sits on every acked write's path to durability and serializing
them would make the queue the write path's throughput ceiling.

It is **unbounded**, which follows from where the hand-off sits rather than being a choice: it
happens on the write path after the commit, so a bounded queue would either block the ack behind the
marker — reintroducing exactly the coupling §7's ack rule removes — or shed it, and a shed has to be
recorded somewhere, which means a flag whose only job is to be remembered on a failure path. An
enqueue that cannot fail needs neither. Depth is an outage symptom, not a tunable: if it grows, the
cache is refusing small writes, and `markers_owed` (§10) says so.

**Quiescence: what the clean marker is allowed to claim.** The marker asserts that the pending set
on disk is *complete*, so writing one while any write still owes a marker converts a recoverable gap
into a permanent one — the next run trusts the marker, skips the scan, and nothing else ever looks.
Emptiness of the queue is not evidence of this: a write that has committed but not yet attempted its
marker, an attempt in flight against the cache, an entry in the worker's hand mid-backoff, and a
multi-GiB body still streaming are all outstanding work that no observation of the queue can see. So
the drain proves quiescence by **two joins and one closure**, never by inspecting a count:

1. The accept loop stops, and hyper's graceful shutdown signals every live connection — HTTP/1
   finishes its current request and closes, HTTP/2 sends GOAWAY — then resolves only once every
   connection future has completed. When it returns, **every handler has returned and no new one can
   start**, which covers the committed-but-not-yet-attempted and still-streaming cases together and
   makes a commit-point gate unnecessary. If it times out instead, *every* bucket is dirty: the
   claim being made is about work we can no longer bound.
2. Every other sender is **handler-local**: the write path upgrades one from the weak handle the
   service keeps, sends, and drops it before returning, so the service never holds the channel open.
   After step 1 the serving loop's is the only sender left, and nothing can enqueue behind what it
   sends — no join over stray tasks, because nothing but a handler ever sends. It then sends an
   explicit **seal**, which FIFO places after every marker of the run.
3. The seal is a *message*, not the channel closing. The serving future owns the queue handle, so an
   aborted or panicking server closes the channel exactly as a drain does; if closure alone
   authorized the clean markers, a killed process would write them on its way out and rob the next
   run of the very scan meant to catch what it dropped. Closure ends the worker; only a seal lets it
   vouch for anything.
4. On the seal the worker makes one final attempt — never a retry loop, since the drain does not
   wait out a backoff — and marks clean only if **nothing is left owed**. A marker still outstanding means the
   run did not end gracefully, so it vouches for nothing at all rather than guessing which buckets
   the loss touched.

That leaves exactly two pieces of state, and only one of them per bucket: the set of buckets whose
pending set this run accounts for, and whether the queue emptied. Both are positive evidence; there
is no flag anyone must remember to set on a failure path, because every failure is the *absence* of
evidence. What makes the evidence trustworthy is that **the obligation is raised by the same helper
that performs the commit** — a cached body cannot land without a marker owing for it. That is the
invariant to protect: a future write path that commits by some other route and forgets to raise the
obligation is invisible to every mechanism here, and would produce a clean marker that lies.

**The claim is released last, after the markers are written.** A passive promotes sub-second on
release, so releasing first lets the new active admit writes to a bucket the old active is still
about to vouch for; its gate clears the clean marker, but nothing orders that clear against the old
active's write. Draining before releasing costs failover the drain time, which is whatever the marker
queue still holds — empty on any ordinary shutdown.

**The recovery scan.** Startup reads and deletes every bucket's clean marker before serving (§6);
each bucket whose marker was **absent** owes a scan, raised once in the background. Serving is not
gated on it — a markerless body reads correctly, it is only not yet durable — but **eviction is**
(§8), and so is the bucket's eligibility for a clean marker at drain. The scan is idempotent, so a
crash mid-scan just re-runs it next boot. It rebuilds the pending set by triage rather than per-key
round trips:

1. `ListObjectsV2` over `<data><b>` and over the remote bucket — two flat listings, 1000 keys a
   call, no per-key requests.
2. A live body whose key is **absent** from the remote is pending; write its marker. A
   **delete-tombstone with no marker** is pending regardless of what the remote holds — re-propagating
   is idempotent, and it also clears a tombstone stranded by a crash between the remote delete and
   the tombstone clear, which nothing else in the sweep would ever revisit.
3. A key present in both compares by **length**: a single-part remote object's framed size is the
   closed form `ciphertext_len(plen) + |trailer|` over the cache body's plaintext length (§6), so
   every overwrite that changed the plaintext length is caught with no extra request.
4. Only a same-plaintext-length overwrite survives triage, and only those pay the remote's trailer
   (one ranged tail GET) to compare `cetag` against the cache body's ETag.

A markerless live body is always single-part — a composite is tombstoned at K with its plaintext in
the shadow (§6) — so the closed form applies exactly. Eviction tombstones need no examination: an
evicted body is by definition already on the remote.

**Bounded loss window (cached mode).** The losable set is exactly *acked writes not yet on the
remote*, and the loss event is exactly **cache-volume loss** — the set is `O(pending)`, dies with the
volume, and leaves nothing to enumerate afterward. A process crash still loses nothing: acked bodies
are in the cache, and a write whose marker had not yet landed is rediscovered by the scan above. The
marker moving off the ack path therefore costs *time to durability* after a crash, never the write —
the loss event and the losable set are both unchanged. Durable mode has no loss window: its commits
are remote-side.

**Durability gates GC.** A key with a pending marker is never evicted or tombstoned, and eviction
independently confirms the remote holds *this body's generation* before overwriting it (§8). A body
only leaves local storage once its own ciphertext is provably on the remote.

### Background: the restore sweep (both modes)

Runs **per bucket**, owned by the bucket-control actor and triggered by the restore overlay (§7
*Buckets*) the first time an op finds a bucket's sync marker (§6) absent — a fresh or wiped cache.
Until it completes, the overlay makes the remote that bucket's read source of truth: remote LIST
pages fan out bounded per-entry trailer reads for facts, and in cached mode are merged with an
in-memory **pending overlay** (acked-but-unuploaded PUTs patched in, pending deletes dropped; rebuilt
from the marker LIST on promotion) so read-after-write holds while the cache is untrusted. The sweep,
over the one bucket's keyspace:

1. Ensure the bucket's `<data>`/`<meta>` projections exist — a lost volume takes the buckets with it
   — draining any stale orphan first.
2. For each remote key with no cache entry (a surviving delete-tombstone counts as present, so
   pending deletes aren't resurrected), write an eviction tombstone + twin. Facts come from the
   object's authenticated tail trailer — one bounded suffix GET per key, single-part and
   composite alike (§6). An object whose trailer fails to verify is **fatal**: hypha is by
   assumption the only writer of the remote buckets, so a verify failure means either something
   else holds write access or this process carries the wrong trailer key / an unknown format —
   in every case hypha's picture of its own data is wrong. It logs the object and exits
   `86`, rather than deleting data it cannot authenticate or serving around it. This is the rule
   at *every* site that reads a trailer (restore sweep, mid-restore read and LIST projection,
   composite body reads), so no path can route around one.
3. Write the sync marker; flip reads back to the cache.

Throughput comes from sharding the keyspace — LIST chains are serial per shard — with shard
boundaries from the prefix-distribution hint (§6); a stale or missing hint degrades to
`delimiter=/` discovery with `start-after` splits. Hand-rolled over the SDK paginator + a
semaphore; idempotent (only fills gaps), the marker written only after every shard drains. In
durable mode this rebuild *is* the steady state being recreated — all tombstones. Serving is
never gated: a conditional write to K mid-sweep first materializes K's remote state into the
cache, then runs the normal §4 path.

### Lifecycle

- **Startup** (cached mode). Before the listener opens: read and delete every bucket's clean marker
  (§6), recording which were present, and raise a recovery scan for each that was not. A bucket whose
  marker cannot be deleted is not served — readiness fails rather than serving a bucket that will
  skip next run's scan. Sub-second at homelab bucket counts (one HEAD + one DELETE each); the scans
  themselves run in the background behind it.
- **Graceful drain.** On SIGTERM: stop accepting → await hyper's connection drain → close the repair
  queue and let its worker run to `None` → if nothing is left owed, a clean marker (§6) for each
  bucket this run accounted for and for no other → **release the active claim** (passive promotes sub-second,
  no fence). The ordering is the
  quiescence proof of §7, and the release is last for the reason given there. A best-effort final
  reconcile pass can shrink the pending set anywhere in here — it is an optimization, since a clean
  marker claims the pending set is *complete*, not that it is empty. Sized into
  `terminationGracePeriod` + `preStop`; running out of grace period simply leaves the clean markers
  unwritten and the promoted replica scans.
- **Remote unavailable** → hot reads fine; tombstoned reads fail cleanly; cached-mode writes
  still ack and markers accumulate; durable-mode writes fail (correctly — they can't be made
  durable).
- **Cache volume loss** → discard and restart: the sync marker is gone, the restore sweep
  rebuilds; the only loss is the cached-mode pending set.

## 8. Tiering / GC — the scavenger task

A single background task of the active (the passive never scavenges), phase 5. In durable mode
there are no bodies to evict — the task only sweeps debris: orphan twins, leftover transition
marks (repaired per §7), and **all mpu record ranges** — both those of uploads abandoned without
complete/abort *and* the leftovers of completed/aborted uploads, whose inline drop is deferred here
(§6, *Multipart upload state*) so complete/abort never pays a large single-object delete on the
client path. Each range is self-describing by its `0x01 0x01 m ‖ <upload-id> ‖ 0x01` prefix, so the
sweep finds and reclaims it without a side index. In cached mode it additionally evicts under
pressure:

**Write-awareness is a property of the remote, not of process memory.** The hazard is one step:
tombstoning a body the remote does not hold *in that generation*. It was once guarded by a per-key
in-flight PUT counter spanning body write → marker write, but that window no longer belongs to a
single request (§7 — the marker write outlives the ack), and a counter never covered a marker owed
by a process that has since died. So the guard is entirely cache-and-remote observable: eviction
confirms the remote's generation against the candidate body itself. One check subsumes three
hazards — a markerless just-written body, a marker lost to a crash, and the corruption a bare
presence check would allow, where the remote holds an *older* generation and eviction stamps the
tombstone with the cache body's facts, so reads return the old plaintext under the new ETag and
length. The per-key job registry of the transition actor below is consequently the only per-key
structure GC keeps in memory, and it holds nothing durability depends on.

**The recency ring.** Recency is a **Bloom-ring sketch** — one filter per **fill window**; sealed
slices persisted per §6, reloaded on promotion, retained k deep. Every op that resolves or lands a
single key feeds it: GET/HEAD/GetObjectAttributes **and the write path** (PUT,
CompleteMultipartUpload, CopyObject's destination). A touch is an in-memory bit set, never a cache
write, so neither path pays I/O to record one. LIST is deliberately **not** a feeder — one
full-bucket listing would mark the entire keyspace hot and collapse the ring into
protect-everything — and neither is DELETE, which leaves no body to protect.

Writes feed it because a write is the strongest available statement of interest in a key, and a
read-only ring gets write-hot/read-cold keys exactly backwards: they look maximally cold, so they
evict first and the reclaimed bytes come straight back on the next PUT, which overwrites the
tombstone with a live body. The pass under-delivers against its byte target having spent a remote
HEAD, a twin write, and a CAS per key to do it, and a read arriving in the gap pays a rehydrate the
write would have made unnecessary.

A slice rotates when its distinct-key fill reaches the design point —
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

1. **Skip if the marker exists** (`HEAD <meta><b>` at bare `K`) — a cheap local short-circuit that
   spares the remote round trip, not the correctness gate.
2. **Confirm the remote generation** (`HEAD` remote K): absent ⇒ not durable ⇒ skip; framed size ≠
   `ciphertext_len(plen) + |trailer|` for this body ⇒ some other generation ⇒ skip. Only a
   same-plaintext-length candidate is ambiguous, and only it pays the trailer's `cetag` (one ranged
   tail GET) to settle it — the same triage the recovery scan runs (§7).
3. Under K's lock: delete stale twins, write the fresh twin, then overwrite K with the eviction
   sentinel via `PutObject If-Match: E_v` — metadata carrying `cetag`/`plen`/original mtime. The
   tombstone is an atomic in-place replace: a racing GET sees body or tombstone, never 404.
   Twin-before-tombstone means a sentinel always has its twin; a crash between leaves a twin next
   to a live body — ignored by classification (§6), swept later.

A writer landing anywhere between steps 1 and 3 has moved the ETag, so step 3's `If-Match: E_v`
fails and eviction retries next pass — the layering (marker → remote generation → conditional CAS)
makes every interleaving auto-healing, never lossy. **Shadow bodies** (§6) are
evicted from their own reserved-prefix windows: confirm the remote composite (HEAD), then delete
the shadow — K's tombstone and twin are already in place.

**Rehydrate** (cached mode) is the mirror: fetch + decrypt from the remote under the lock. A
single-part body lands at K with `If-Match: <evict-sentinel-etag>`, then its twin is deleted —
K's facts are native again. A composite lands in the shadow body (§6); K's tombstone and twin
stay untouched.

The eviction sentinel's ETag is constant across generations, so that `If-Match` cannot by itself
tell one tombstone from the next: a queued rehydrate can sit while K is rewritten, reconciled, and
evicted afresh, and the CAS would then accept the new tombstone for the old plaintext. The rehydrate
therefore **re-reads the tombstone's `cetag` under the lock** and abandons the job unless it is still
the generation the read observed — and since the re-read and the land are both under the lock,
nothing can move K between them. That same re-read is where the client pass-through metadata comes
from, so a land can never stamp a superseded generation's metadata either.

**The background-transition actor.** Eviction and rehydrate share a property nothing on the client
path has: they are **discardable**. The read that raises a rehydrate is already being served from
the remote, and an eviction abandoned because a client wants the key is an eviction that should not
have run. Both therefore run as jobs on one bounded queue (`background.concurrency` at a time,
`background.queue_depth` waiting) rather than as unbounded detached tasks, with three consequences:

- **Deduped by key.** One live job per key, so N concurrent reads of one evicted key raise one
  transition rather than N that each take the write lock in turn. (The shadow-freshness HEAD still
  covers the other case — a job raised *after* an earlier one landed that generation.)
- **Shed under pressure.** A full queue drops new submissions instead of blocking the reads that
  raised them: the cost of a dropped rehydrate is that the next read of that key fetches from the
  remote, which is what that read is already doing.
- **Cancelled by client writes.** A transition holds K's write lock across a whole-object transfer,
  so a same-key conditional PUT, DELETE, or CompleteMultipartUpload would otherwise park behind it
  for minutes. Every client write instead cancels K's transition before taking the lock, and the
  holder abandons it at the next await. This does not weaken the under-lock rule above: a transition
  that *completes* still performed every step under the lock — it is only ever abandoned wholesale,
  never half-applied. The cancel needs no acknowledgement, because a job registers its cancel token
  before it first attempts the lock: a job blocking a client necessarily holds the lock and so is
  necessarily findable, and a job registering after the cancel has not taken the lock and blocks
  nobody. The lock handoff is the rendezvous.

The one part of a rehydrate outside the cancellable region is the twin delete that follows a
single-part land: the land PUT is what drives the transfer, while dropping the twin is a fast local
pair of calls, and cancelling between the two would leave a live body beside a stale twin — benign
(classification ignores it, §6) but debris nothing reclaims until the next sweep.

**The walk heals markers forward.** The rotating cursor already visits every live body, so it
applies the recovery scan's test as it goes (§7) and writes a marker for any body whose generation
the remote does not hold. The boot scan is what makes that recovery *prompt* after a crash; the walk
is the standing backstop for anything a mid-run failure leaves behind between boots. **Eviction in a
bucket waits for that bucket's scan** — before it completes, the pending set on disk is known
incomplete, and a scavenger reading it as exhaustive is the one way an acked write is lost. The
generation check (step 2 above) independently refuses those bodies, so the ordering rule is the
second of two locks on the same door, not the only one.

**Usage from the backend.** The scavenger reads SeaweedFS volume/master metrics (physically
accurate, sees dead bytes), scavenges from high- to low-water mark, and can drive
`volume.vacuum`. Other cache backends plug in their own source.

## 9. Configuration & deployment

`figment` (TOML + `HYPHA_`-prefixed env, `__` nesting), validated at boot. Current surface
(`config.rs`): `remote` and `cache` endpoints (endpoint/region/credentials/**bucket prefix** —
client buckets pass through prefixed, so deployments share a remote account in disjoint bucket
namespaces), `mode` (`durable` | `cached`), `auth` (hypha's own
client credentials for `S3Auth`), `master_passphrase` (the 256-bit random age passphrase, from a Secret; supersedes phase 1's
`master_identity`), `serving.listen` + `serving.offload_threshold` (§5), `reconcile.interval_ms` +
`reconcile.concurrency` (the §7 sweep's cadence and per-pass fan-out, and the marker queue's write
fan-out; that queue is deliberately unbounded and so has no depth knob) + the drain budget, which must fit inside the pod's
`terminationGracePeriod`: overrunning it is safe but withholds every clean marker,
`background.concurrency` + `background.queue_depth` (the §8 transition actor: whole-object transfers
in flight, and how many wait before submissions are shed — so `concurrency` sits far below
`reconcile.concurrency`, since it bounds remote bandwidth rather than request count). Later phases
add: GC water marks / walk window / recency-ring shape (slice size, depth k,
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
(SeaweedFS, B2 included), used at complete to resolve the winning part set and its sizes (§6/§7) —
and **`ListMultipartUploads`**, which the client-facing op of that name proxies outright (§7):
in-progress uploads are remote state, and the remote's own key-position pagination is what makes
`key-marker`/`upload-id-marker` correct. Same core-multipart family, equally universal.
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
**`markers_owed` and buckets left dirty at drain** (both should be flat zero — markers owed means the
cache is refusing small writes, and it is also the queue's only bound, §7),
remote-upload latency/retries, role + failover count + fence-confirm latency, scavenge throughput
and usage vs. water marks. `/healthz` + `/readyz` (remote reachable, and in cached mode every
bucket's clean marker cleared — §7 *Startup*); active/passive is a reported
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
  (marker skip, remote-generation confirm, `If-Match` abort) never tombstones an
  acked-but-unuploaded body, including the prior-generation-marker case and a remote holding an
  *older* generation of the same key.
- **Marker obligation & the clean marker**: a marker write that fails leaves the object acked and
  live, and the queue's retry lands the marker afterwards (fault-injecting cache wrapper — MinIO
  cannot fail one write selectively). Kill the active without a drain ⇒ no clean marker ⇒ next run
  scans and rebuilds every missing marker; drain gracefully ⇒ clean marker ⇒ no scan, and a marker
  owed at drain (the queue still retrying) withholds it. The quiescence ordering is the part
  worth asserting directly: a write committed but not yet marked must never coexist with a clean
  marker, so drive a body write concurrent with SIGTERM and assert the marker set and the clean
  marker cannot disagree. Assert the **default** too, since it is the property a future change is
  most likely to break: after startup completes, no bucket's marker is present on disk; killing at
  each step of the drain produces none; and a bucket left dirty by a previous run and untouched by
  this one is still dirty after this one drains gracefully — it was never scanned, so this run
  cannot vouch for it.
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
- **External conformance** — third-party suites run against a booted hypha, complementing the
  hand-written cases above (which assert hypha-specific internals the black-box suites can't see):
  - **`s3s-e2e`** (`s3s-project/s3s`, version-matched to our `s3s`): the S3 test suite from the
    framework hypha is built on. A CLI that drives `aws-sdk-s3` (path-style) against an endpoint
    from the standard `AWS_*` env; `scripts/s3s-e2e.sh` boots MinIO + hypha with the integration
    harness's config and points it at hypha. Adopted first — it speaks our exact `s3s`/aws-sdk
    versions, so a failure is a real hypha bug, not a dialect gap. Checksum trailers pinned off
    (`AWS_REQUEST_CHECKSUM_CALCULATION=when_required`) to match the client config in
    `tests/common`.

    First baseline (2026-07) had the in-house tests green but s3s-e2e, exercising the same surface as
    a black box, was **not** — the gaps that kept phase 3 open. Now (with CopyObject landed) every
    in-scope s3s-e2e case passes; the only reds left are the deferred/out-of-scope families below.
    - **Green**: `list_buckets`, `list_objects`, `get_object`, `delete_object`, `head_operations`,
      `put_object` (tiny + larger + `with_metadata` + `non_ascii_metadata` + `content_checksums`),
      **`copy_object`**, `list_objects_with_pagination`, presigned PUT/GET.
    - **Fixed since the baseline (were real gaps in the declared surface):**
      - *User metadata is dropped.* PUT with `x-amz-meta-*` now passes through PUT→HEAD/GET verbatim
        under a reserved namespace, RFC 2047 for non-ASCII (`test_put_object_with_metadata`,
        `..._non_ascii_metadata`).
      - *`Content-MD5` not validated.* A wrong `Content-MD5` is now rejected with `BadDigest`
        (`test_put_object_with_content_checksums`).
      - *Multipart / LIST pagination.* `test_list_objects_with_pagination` is green; the part-count
        mismatch was resolved with the phase-3 machinery. `test_multipart_upload`'s remaining red is
        purely its `checksum_crc32` assertion — the deferred flexible-checksum family below, not a
        multipart defect (hypha's own `multipart.rs` suite is green).
    - **In scope for phase 3 — done.** The full client surface minus the exempt families, designs in
      §7: **`CopyObject`** (server-side body reuse via `UploadPartCopy` + re-minted trailer, or the
      small-body re-encrypt path — `copy.rs` + `tests/copy.rs`), `DeleteObjects` (non-atomic fan-out;
      durable batches the remote leg), `ListObjects` v1, `ListMultipartUploads`, `ListParts`,
      client-facing `UploadPartCopy`, `GetObjectAttributes`, the `GetBucketVersioning` stub, and
      storage-class passthrough. (CopyObject's *destination* `If-[None-]Match` half of §7's
      precondition split is not reachable: s3s 0.14.1's `CopyObjectInput` predates S3's
      conditional-copy-on-destination fields, so only the `copy-source-if-*` conditions apply.)
    - **Deferred, not exempt** — revisit after phase 3: flexible checksums
      (`test_put_object_with_checksum_algorithm`, the `checksum_crc32` asserts) — validate + persist
      inline over plaintext (the `Content-MD5` slot), single-part first, composite checksum-of-
      checksums last.
    - **Out of scope — deselect, don't fix** (feature families that contradict the single-writer /
      intrinsic-encryption / no-versioning model): ACLs, bucket policy, lifecycle, CORS, SSE
      *configuration*, versioning writes + `ListObjectVersions`, object-lock/retention/legal-hold,
      object & bucket tagging, replication/logging/notification/website/accelerate/request-payment/
      ownership/public-access-block, analytics/metrics/inventory configs, `RestoreObject` + archive
      storage classes, `SelectObjectContent`, `GetObjectTorrent`, STS `AssumeRole`, Object-Lambda.
      For bucket-config GET probes the posture is the specific "not configured" error code rather
      than a blanket `NotImplemented`.
  - **Follow-up: Ceph `s3-tests`** (`ceph/s3-tests`, boto3/pytest) — the broad industry
    compatibility suite (thousands of assertions) for deeper corner coverage once the core surface
    is green. Must be curated to hypha's implemented ops (GET/PUT/HEAD/DELETE, LIST v1/v2,
    multipart, `If-Match`/`If-None-Match`, ranged reads); its versioning/ACL/lifecycle/CORS/SSE/
    object-lock families are out of scope and get deselected, not fixed (storage class is stubbed
    passthrough, §7, so its round-trip cases stay in).

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
would be rejected — it had broken every durable overwrite/delete of an already-written key; the
carve-out survives the §6 keyspace split, since a twin key still carries `0x01`).
Remaining: the s3s-e2e black-box pass (§11) later found this surface still drops user
`x-amz-meta-*` metadata and accepts a wrong `Content-MD5` — both fixes land under phase 3; also
conformance vs. SeaweedFS as the cache backend, and ZeroFS against the durable endpoint.

**Phase 3 — multipart + rest of client surface. Done (vs. MinIO).** The s3s-e2e black-box pass
(§11) had reopened it — dropped user `x-amz-meta-*`, an unvalidated `Content-MD5`, and multipart +
LIST-pagination reds — all since resolved: metadata pass-through, `BadDigest` on a wrong
`Content-MD5`, green v1 pagination, and finally **CopyObject** (`copy.rs`), the last in-scope op.
Every in-scope s3s-e2e case now passes; the remaining reds are the deferred flexible-checksum family
and the out-of-scope families (§11). Trailer + embedded parts table
(single-stream composite read, MAC'd trailer) and the mpu-record retag-match via `ListParts`
(§6/§7) both landed, superseding the original metadata records + completed-object part-index
approach.
Native-remote-multipart proxy (§7): per-part encryption + inline `pmd5`, listable mpu records
(`p{n:05};<retag>;<pmd5>;<nonce>` key-encoded, `pmd5` the sole stored datum) in `<meta><b>`;
complete resolves the winning part set via the remote's `ListParts` (retag-matched, geometry from
the remote's sizes), composes the composite ETag, and lands the terminating trailer —
`table ‖ facts ‖ tag ‖ version` — in the same atomic native complete (self-describing, no records
or tags, no *completed-object* part index): as its own part above every client part, or **folded
into the last client part** whenever nothing can follow that part — it is under the backend's 5 MiB
minimum, or it is part 10000 (this pass added the fold, and generalized it from the first condition
to both: a separate trailer part would demote a small final part to an illegal non-final one on any
real S3 backend, and at part 10000 there is no number left to put one at). Clients therefore get
S3's full 1–10000 range; UploadPart retains the ciphertext of exactly the parts that can end an
upload, since an in-progress part can't be read back. Plus abort; the 4 GiB part cap;
single-stream composite full + ranged GET off the table; cleanup via batched multi-object delete. This phase also closes the **rest of the client
surface** (§7, the full API minus the exempt families): the phase-2 metadata/`Content-MD5`/
pagination fixes; **CopyObject** on this same machinery (`UploadPartCopy` the source body range +
a re-minted `K_dst`-bound trailer, or the small-body re-encrypt path; transition bracket in durable,
cache→cache copy in cached); **DeleteObjects** (non-atomic fan-out — durable widens the bracket to
mark-all → one native remote `DeleteObjects` → settle-all, cached is per-key tombstone+marker with
the remote batch deferred to reconcile); **ListObjects v1**, **ListMultipartUploads**,
**ListParts**, client-facing **UploadPartCopy**, **GetObjectAttributes** (parts off the trailer
table); and the **GetBucketVersioning** stub + **storage-class** passthrough. Flexible checksums stay
deferred (§11).
*Exit*: §11 multipart scenarios — restart-mid-upload, re-upload/concurrent-part resolution
(including a small final part), the trailer fold, trailer-based restore recovery — plus the surface
ops: CopyObject (single-part + composite source, `COPY`/`REPLACE`, copy-source preconditions, a
`K_dst` trailer that verifies where a raw remote copy would not), DeleteObjects (partial-failure
result, crash mid-batch repair, XML-clean-keys-only remote batch), v1 LIST, multipart list/parts,
and GetObjectAttributes part geometry — covered in `hypha/tests/multipart.rs`, CopyObject in
`hypha/tests/copy.rs`, and the s3s-e2e pass against MinIO.

**Phase 3a — the keyspace split (§6), a prerequisite for v1 LIST. Done (vs. MinIO).** Sequenced
ahead of the remaining phase-3 surface because v1's `NextMarker` is not expressible under the old
layout at all, and because the twin-suffix headroom it removes is what capped client keys at 900
bytes.

1. **Config + buckets** — the `<data>`/`<meta>` cache split (the `<meta>` backend is a
   `Backend::with_prefix` sibling over the one cache endpoint), `cache_meta_prefix` config with the
   startup prefix-collision invariant, `validate_bucket_name` against the 63-byte budget, the
   create/delete/head lifecycle over three backend buckets, and the remote-as-emptiness-gate delete
   that drains both cache buckets. **Done.**
2. **`meta` module** — the structural `0x01` ranges replacing `RESERVED_PREFIX`, bit-packed
   base64url facts, twin build/parse against the 983-byte threshold, admission relaxed to S3's
   1024. **Done.**
3. **Move twins, markers, and mpu state** into `<meta><b>` (`Reconciler` split into `data`/`meta`
   backends); `refresh_twin` / `delete_twins` / the settle and tombstone paths; `shadow_key` helper
   for `sha256(K)` (unwired until phase 4's rehydrate). `drop_mpu_state` moved to single-object
   deletes — the range-A keys now carry `0x01`, so they inherit the twins' batch-delete carve-out
   (§11). **Done.**
4. **LIST merge join** — the `<data>` client cursor plus a `<meta>` twin cursor
   (`prefix = 0x01 ‖ <client prefix>`, mirrored delimiter, `start_after` past range A), paired by
   base-key equality, HEAD fallback for missing and over-threshold twins. **Done.**
5. **v1 pagination** — `NextMarker` = the client cursor's last raw key; `list_objects_v1_pagination`
   un-ignored and green. **Done.**

> **One spec correction during implementation.** §6 originally specified a **92-symbol** facts
> alphabet (`0x20..=0x7E` minus `/`,`+`,`;`), keeping space. But space is the exact converse of the
> `+` hazard the spec already guarded: a literal space in a twin key round-trips through the
> `encoding-type=url` LIST as `+` on MinIO, so `delete_twins` read back a corrupted key and left the
> real twin behind (caught by `delete_objects_batch`). The alphabet became **91 symbols** —
> `0x21..=0x7E` minus `/`,`+`,`;`, space excluded.
>
> **Second correction, from the phase-4 review (REVIEW.md).** The 91-symbol alphabet still carried
> `\` and `.`, and MinIO splits path components on `\` as well as `/`, rejecting any `.`/`..`
> segment (`XMinioInvalidResourceName`, surfaced as a 500) — so a pseudo-random facts field
> containing `\` + `.`/`..` failed the twin write nondeterministically. Rather than exclude a third
> pair of chars, the alphabet became **base64url** (64 RFC 3986-unreserved symbols, rendered by
> `base64-simd`) — 39 chars, threshold 986 → 983 — ending char-driven exclusions by construction,
> and retiring the hand-rolled bigint base conversion that a non-power-of-two base had required.

*Exit* (met): the v1 pagination test green under twin dilution at several page sizes
(`list_objects_v1_pagination`); a key above the twin threshold round-tripping PUT → LIST → GET
through the HEAD fallback (`list_over_threshold_key_head_fallback`); `ListBuckets` not leaking cache
buckets (`list_buckets_hides_backend_projections`). The reconcile sweep's `O(pending)` marker scan
is a structural property of the bare-`K` marker range (§6, §7) — it lands with the sweep itself in
phase 4, where its complexity is asserted directly.

**Phase 4 — cached mode, single replica. Done (vs. MinIO).** The marker queue behind the write path,
and the clean marker / recovery scan behind it (§7 — the ack is the body write, so a
marker that fails is owed and retried rather than turned into an error), the reconcile sweep
(`replication.rs`, a background duty of
the active tied to the service's liveness sentinel, listing the `O(pending)` bare-`K` marker range
past the `0x01` block), cached DELETE propagation (mask-then-propagate, single + batch), and
rehydrate (single-part into K via `land_rehydrated_single_locked`; composite into the shadow body
via `land_shadow_locked`, with the read path probing/serving the shadow under a full-digest check).
The reconcile upload/delete/marker CAS uses the cache's conditional `PutObject`/`DeleteObject`
(`Backend::delete_if_match`); cached PUT forwards `Content-MD5` to the cache for an atomic
`BadDigest`. Deployed with one replica and no fencing — a single writer is trivially single, so
this ships the default `s3.internal` deployment with correctness intact, only failover seamlessness
missing. *Exit* (met): `hypha/tests/cached.rs` — marker/reconcile upload, cached delete
propagation, conditional-write linearization (incl. `If-None-Match:*` create contention),
`Content-MD5` rejection, the marker scan staying `O(pending)` (evicted keys untouched), and
single-part + composite rehydrate on a tombstoned read. The reconcile CAS's *same-key race*
guarantee needs the cache to honour conditional DELETE (§9, SeaweedFS ≥ 4.07); the MinIO harness
exercises only the non-racy paths, which are backend-conditional-agnostic.

**Phase 5 — GC + restore.** Walk cursor, threshold-ratchet eviction, Bloom ring (fill rotation)
+ slice persistence, usage
source + vacuum, prefix-hint writer, sync marker + parallel restore sweep, debris sweeps (orphan
twins, orphan shadow bodies, leftover transition marks, and **all mpu record ranges** — abandoned
uploads *and* the deferred cleanup of completed/aborted ones, §6/§8, replacing the inline
`drop_mpu_state` fallback). *Exit*:
scavenge/rehydrate and cache-wipe → restore-sweep → rehydrate scenarios; mpu ranges reclaimed
without an inline complete/abort delete.

**Phase 6 — `hypha-fence` + active-passive.** Two-pod StatefulSet, leader-elected controller,
lease, fence→confirm→drain→promote, graceful-release fast path. First step: verify the fence
primitives on the live cluster (policy-revision observability, established-connection reset).
*Exit*: the §11 partition harness.

**Phase 7 — chart + operations.** The `hypha/` chart (both workloads, Secrets, `HTTPRoute`, fence
RBAC, per repo networking conventions), dashboards for §10, then the two production installs
(cached + durable). *Exit*: both endpoints live behind the shared Gateway.
