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
  shared tiering machinery — `Tiering` (upload/tombstone primitives over cache + remote) and
  `KeyLocks` (the per-key lock table). Later phases add the reconcile sweep, the GC scavenger, and
  the two recoveries as background tasks of the active replica. Runs **active-passive** (§4).
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
| Observability        | `tracing`(+`subscriber`); `metrics` + `metrics-exporter-prometheus` (rendered on hypha's own admin listener, so none of the exporter's listener/push features are on), `hyper`+`http-body-util` for it |
| Concurrency          | `dashmap` (the §4 key-lock table), `arc-swap` (the §7 bucket state) |
| GC                   | `fastbloom` (the §8 recency ring; `rand` off, seed pinned so slices survive a restart) |
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
  halt.rs                invariant violations: shut the server down, record on the remote, exit (§7)
  keylocks.rs            per-key async lock table (§4)
  tier.rs                Tiering: upload / tombstone / twin / restore-sweep primitives (§7)
  bucket/                the bucket lifecycle (§7) — mod.rs owns marker ⇒ phase ⇒ which recovery:
                         ctl.rs (the actor: sole writer of the cache substrate),
                         restore.rs (R1, remote walk), rebuild.rs (R2, two-cursor join)
  background.rs          background-transition actor: bounded, deduped, client-cancellable rehydrate (§8)
  s3/                    the s3s::S3 impl, split by op group
    put.rs get.rs list_head.rs delete.rs multipart.rs buckets.rs
    overlay.rs           restore overlay: readiness gate + cache-vs-remote source for reads/writes (§7)
  markers.rs             pending-marker obligations and the clean marker (§6/§7)
  volume_watch.rs        the one failure a running process polls for: cache volume loss (§7)
  metrics.rs             §10's exports, one named function per thing that happened
  admin.rs               §10's listener: /metrics, /healthz, /readyz
  replication.rs         (phase 4) the cached-mode reconcile sweep (§7)
  gc/                    (phase 5) the GC actor, active-only (§8): mod.rs (the actor — cadence,
                         the state it owns, and each pass), ring.rs (the Bloom-ring sketch),
                         ladder.rs (the pressure ladder as one ordered position),
                         scan.rs (probes + the learned per-bucket cold yields and
                         key-prefix distributions),
                         evict.rs (the three eviction gates), orphans.rs (superseded shadow
                         bodies: the queue, the marker, the backstop),
                         usage.rs (cache usage +
                         dead-byte compaction, pluggable per backend),
                         store.rs (GC's own bucket), debris.rs (reclaims with no owner
                         on the client path)

hypha-fence/src/         (phase 6) fencing controller (§4)
```

The `s3/` modules are thin: parse intent, take the key lock where required, orchestrate `Backend`,
`hypha-format`, `meta`, and `tier`.

## 4. Modes, concurrency, and the linearizability guarantee

### Two modes, one machinery

A deployment runs in one of two modes; **both require the cache and the remote**. The cache is
always the namespace and ETag source of truth — HEAD/LIST and conditional-write evaluation are
cache-served in both modes — and the remote always holds age ciphertext framed with an
authenticated facts trailer (§6) so the namespace restore (§7) can rebuild the cache namespace from it.

- **`durable`** — writes are synchronous: the remote op is the **commit point**, bracketed by a
  transition mark so readers never see torn state (§7). PUT encrypts and uploads inline, settles
  the eviction tombstone (+ facts twin) in the cache, then acks. The cache holds only tombstones
  and twins, and a tombstoned GET decrypts from the remote without repopulating (a restored body
  would immediately be tombstoned again). Ack ⇒ remote-durable: no loss window, at the cost of
  remote latency on every write.
- **`cached`** — writes ack after the cache write plus a pending marker; a background reconcile
  sweep uploads to the remote (§7). GC tombstones cold bodies under pressure and tombstoned GETs
  rehydrate (§8). Low latency, bounded async-lag loss window.

The mode is a property of the **bucket at that moment**, not of the process: a cached deployment runs
the durable path for the whole of a bucket's namespace restore (§7), because a cached ack would leave
committed state in a namespace every reader is being told to ignore.

Durable mode is the cached machinery under three constraints: synchronous upload, always
tombstone, never restore. Both modes share `Tiering` and the tombstone/twin/marker structures
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
exclude *other reconciles of the same key*, never make a conditional PUT queue behind a multi-second
transfer.

That exclusion is **`try_lock`, and a pass that loses it drops its attempt** — pending same-key
uploads coalesce onto the in-flight one rather than queuing behind it. A waiter would be redundant:
the holder re-reads K's body under the lock, so it uploads whatever generation is current when it
wins, and any write it cannot account for fails its marker CAS and stands for the next pass. A
waiter is also actively harmful, because unlike a rehydrate — which a client write cancels for free
via the write lock (§8) — an upload holds no write lock and cannot be cancelled. Queued waiters each
re-upload in turn, so on a key written faster than it uploads the newest generation's upload starts
only after the whole redundant queue drains, and the loss window grows without bound. Coalescing
caps the queue at one in-flight upload per key, so the bound stays one pass plus one transfer.

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
remote — the fourth being the client bucket name itself, which never leaves hypha. Each backend name
is `<prefix>-<role>-<b>`, the role a fixed single character (§9), so the three cannot overlap however
the deployment prefix is set. One further bucket, `<prefix>-g`, is not per client bucket at all: it
is GC's own, holding the deployment-wide recency ring (§8).

- **`<data><b>`** holds *only* client objects: a body at `K`, or a tombstone overwriting `K` in
  place, so a racing GET sees one or the other and never a 404. Nothing hypha-internal lives here.
- **`<meta><b>`** holds everything hypha keeps *about* objects, in three contiguous,
  prefix-separable ranges (below) — the two lowest byte values are inadmissible in client keys,
  which is what makes the split structural rather than probabilistic.

| Range                     | Contents                                              | How it is scanned                                    |
|---------------------------|-------------------------------------------------------|------------------------------------------------------|
| `0x01 0x01 ‖ tag ‖ …`     | mpu state, sync + clean + shadow-clean markers, shadow bodies | prefix scan per tag                       |
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
pending-set rebuild (§7) can re-derive the entire set from first principles.

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
*is* the classification, and where landing it would have LIST hide a live key and the pending-set rebuild
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
`0x01 0x01 b ‖ base64url(sha256(K))`. The access pattern is a **point lookup** — "is there a
rehydrated plaintext for K" — so the key can be a digest, which removes any length condition. SHA-256
rather than the MD5 already in the tree: a shadow collision would serve *another key's plaintext*, the
worst failure the system has, so the digest must resist deliberate collision. The **whole** digest
goes in the key, base64url-unpadded — 43 characters, every one control-byte-free and `start-after`-safe
— which is what makes the collision case go away rather than be *detected*: a truncated digest would
need a second, wider digest in the shadow's user-metadata, checked on every read, purely so a collision
degraded to a cache miss. Nothing prefix-scans this key, so its width costs nothing, and the extra 23
characters buy the deletion of a field and a check. (K itself could not ride in metadata either — a
1024-byte key percent-encodes past S3's 2 KB ceiling.)

The tombstone and twin at K stay untouched, so composite rehydration is invisible to LIST/HEAD and
rewrites no twin. Because the shadow key is deterministic in K, a *later* composite at K overwrites the
same shadow — the key cannot tell generations apart — so the shadow carries the rehydrated **client
ETag** and a read serves it only when that equals K's current tombstone `cetag`. A shadow left from a
superseded generation therefore misses and re-rehydrates rather than serving stale bytes under the new
ETag, and that same ETag is what GC's reclaim conditions on (§8).

The shadow also carries **K itself**, base64url-encoded, as the back-pointer the digest key cannot
provide. Only §8's orphan backstop reads it, and it is the one thing that makes that pass possible: a
shadow whose K was deleted or overwritten is unreachable *and* unidentifiable, so there is no way to ask
K about it from the key side. base64url rather than the percent-encoding the client passthrough uses,
because the encoding has to be unconditional — percent-encoding a 1024-byte non-ASCII key expands 3×
and overruns S3's 2 KB user-metadata ceiling, while base64url is a flat 4/3, or 1368 characters at the
key-length cap. A shadow's metadata is hypha's alone (no client passthrough shares this carrier, unlike
a tombstone's), so the whole budget is available.

**The shadow-clean marker** (`0x01 0x01 o`, cached mode): present iff no shadow in this bucket has been
orphaned without being reclaimed. Same positive-evidence discipline and same lifecycle as the pending
set's clean marker — written only by a graceful drain, deleted at startup before the first request, so a
running process never has a marker on disk claiming what it can invalidate at any moment. Separate from
that marker rather than folded into it, because the recoveries their absences trigger differ by orders
of magnitude (§8).

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
> The sweep is the *only* path: an inline drop beside it would be a second mechanism for a reclaim
> the first already finds for free, and could not replace it anyway, since a process that dies
> between the commit and the delete leaves the range regardless. Abort is the case that most looks
> like it wants one and least deserves it — a maxed upload is 10 000 deletes on the client's call to
> say "throw this away".

**The sync marker**: an object at `0x01 0x01 s`, present iff a namespace
reconciliation has completed — namespace trust recorded in the cache itself, dying with the
volume by construction. Present ⇒ reads are cache-authoritative and an absent key is a definitive
404. Absent ⇒ the remote is the read source of truth until the namespace restore rewrites it (§7).

**The clean marker**: an object at `0x01 0x01 c`, per bucket, encoding one claim —
*no un-indexed write has happened in this bucket since the last completed drain*. Present ⇒ the
marker range is an exhaustive account of the pending set — **complete, not empty**: pending markers
alongside a clean marker are the ordinary steady state. Absent ⇒ a write may have landed without its
marker, and the bucket owes a pending-set rebuild (§7). Like the sync marker it lives in the cache, so it
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
> pending set**: either its marker was present at startup, or this run rebuilt it (§7).
> That is the run's only per-bucket state, and it is a membership rather than a flag — a bucket left
> dirty by an earlier crash and untouched by this run is simply not in the set, and must end this run
> dirty too, since its orphans are still unindexed and a clean marker would bury them permanently.
> The one other condition is not per-bucket at all: a marker still owed when the drain seals means
> the run did not end gracefully, and **no** bucket is marked clean.

**The halt marker**: the record of an invariant violation (§7), and the only hypha-internal key that
lives on the **remote** rather than in `<meta>` — at `0x01 0x01 h` in `<remote><b>`. It has to outlive
the cache: the cache is exactly what a namespace restore rebuilds and a volume loss destroys, so a
halt marker there would be erased by the recovery it exists to block.

Being in the client keyspace it leads with the two control bytes no client key may contain, and every
path that reads the remote *as* a client keyspace filters it (`meta::is_reserved_remote_key`) — the
recovery cursors and the restore-time LIST projection. That filter is not cosmetic: every key past it
goes to a trailer read, and hypha's own keys carry no trailer, so an unfiltered one would be reported
as a foreign object — hypha halting on its own bookkeeping.

The write protocol is what makes it trustworthy. On observing a violation the process **shuts the
server down**: the accept loop stops and every live connection is signalled to close, so nothing
further is served on data hypha has just declared wrong. It then records the marker, **retrying until
it lands** — losing the record is the one outcome that must not happen, since it would let the next
process resume serving the same wrong data — and only once the record is durable does it **exit**.
hypha is active-passive with a single active, so there is no second replica to notify: the next
process reads the marker before the listener opens and exits too, and the deployment presents as an
ordinary crashloop, the failure mode every operator's tooling already alerts on.

Exiting is `process::exit`, not a panic: a panic in a spawned task unwinds that task alone and leaves
the rest of the process serving — the exact state the halt exists to prevent.

In-flight connections are **not** drained and `serve` does not return. The handler that observed the
violation is itself in flight and parked on the record loop, so a drain could only time out; and
returning from `serve` would end the process *successfully*, losing the record. The one window where
hypha is up and not serving is between the shutdown and the record, while the remote is unreachable —
deliberate, since exiting first loses the record and serving defeats the halt.

Clearing is an operator action: delete the marker object and let the process restart. Clearing without
fixing what diverged re-trips on the next pass, which is the intent — the marker records a fact about
the data, not about the process.

**Recency slices** live in GC's own bucket (`<prefix>-g`), not in `<meta><b>`: the §8 ring is one
sketch for the whole deployment, keyed by fully qualified `<bucket>/<key>`, so there is no client
bucket it belongs to. Nothing client-facing shares that bucket, which is also why these keys need
none of the control-byte machinery the `<meta>` ranges are built from — a plain `recency/<seq>`,
zero-padded so the listing's order is rotation order.

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

## 7. Operations

Each client operation, as steps per mode, over `tier.rs`'s `Tiering` primitives, the §4 lock
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
repair (or, after a simultaneous cache loss, the namespace restore) reads the facts off the tail
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
     (the namespace restore below).

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
  occupied by the namespace restore the write is racing and serving is never gated (below). Safe there
  only because it exclusively creates, idempotently, and only for a bucket already resolved as present
  on the remote — bucket *lifecycle* stays the workers' alone.
- **Recovery** — fire-and-forget: startup dispatches R1 or R2 by name (below), and a bucket owed R1
  is published `Restoring`, which resolves its reads from the remote meanwhile — no 503, no waiter
  list. Idempotent, so a crash mid-pass resumes by re-running. A pass that could not run **re-queues
  itself** on a fixed delay rather than sleeping in place, which would hold the bucket's concurrency
  permit and block any Create/Delete behind it; the pass is unchanged on retry, since only that pass
  rewrites the markers it was chosen from. A bucket the remote no longer holds is *retired* from the
  map instead, which is what turns later requests into `NoSuchBucket`.

  The **single queue slot** per bucket is what makes §7's two recoveries mutually exclusive on a
  bucket: there is only ever one to dispatch, and nothing is ever merged into the slot.

**One poll survives startup.** Everything above is settled once and held for the run, which is sound
under exactly one assumption: that the cache volume does not vanish underneath a live process. That is
the one thing a running hypha still checks — a background task re-HEADs each `Ready` bucket's sync
marker every `volume_watch_interval_ms` (default 30 s, one HEAD per ready bucket per tick). Its
disappearance is invariant **I7** and halts the deployment. Nothing else needs polling: every other
divergence is either impossible while hypha owns both backends, or is caught by the pass that would
act on it. A backend that cannot answer is not a loss — only a definitive absence is — and a
`DeleteBucket` race is told apart by re-reading the state map after the failed HEAD, since delete
retires the bucket *before* draining its projections.

Only `Ready` buckets are polled, and that is the exact set rather than a convenient one: `Ready` is
the only phase that asserts something falsifiable about the cache — that an absent key is the object's
absence. A `Restoring` bucket asserts the opposite, resolving reads from the remote and committing
writes there, so losing its volume costs the restore its progress and nothing else; nothing acked
lives only in the cache during that window, and the pass is additive and idempotent, so its retry
rebuilds from the remote.

**Bucket state is resolved in full at startup**, before the listener opens: one pass over the
remote's bucket list reads a bucket's **two** markers and settles it outright — sync marker absent ⇒
`Restoring` + R1; present with a clean marker ⇒ `Ready`, accounted; present without one (cached mode)
⇒ `Ready` + R2. The clean marker is deleted as it is read, so from the moment hypha can take a write
no bucket on disk claims to be clean. Choosing the pass here rather than re-deriving it at dispatch is
sound precisely because nothing is being served yet: no marker can move underneath the decision, and
there is no second raiser to reconcile with. hypha owns both backends outright — nothing else creates a
bucket in either — so that list *is* the set of buckets, and the published map is a complete account
rather than a cache of what has been touched. Two things follow. Readiness becomes a pure map lookup
on the request path: **no backend call at all**, and a bucket with no entry is a definitive
`NoSuchBucket` rather than "ask the remote". And recovery no longer waits for traffic — a bucket whose
cache volume was lost is restored because the process started, not because someone read it.

The map stays complete because `CreateBucket` publishes its entry only *after* the remote create
commits, and `DeleteBucket` retires it only after the remote delete does. A remote bucket the map has
no entry for therefore cannot arise, and is invariant **I6**.

**The restore overlay** keeps serving ungated while a bucket is unreconciled (one interface,
`s3/overlay.rs`): the readiness verdict selects each op's source.
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

- **Mid-life cache loss halts rather than self-heals.** A volume that dies under a *running* active
  leaves every `Ready` bucket answering 404 for objects that exist, since cache-absent is the
  authoritative 404 there. The volume watchdog (§7) catches it and halts, which forces the restart
  that resolves the bucket as `Restoring` and rebuilds it — the "cache volume loss ⇒ discard and
  restart" operational model (§4/§8), made automatic rather than left to an operator noticing.
  Repairing in place was rejected: the run has already served 404s it cannot identify or take back.
- **Restore rebuilds object tombstones only, not multipart state.** R1 reconstructs the object
  namespace from remote objects + trailers; in-flight multipart records (`<meta>` range A) are *not*
  rebuilt, so `ListParts`/`CompleteMultipartUpload` for an upload started before a cache loss won't
  find its records after restore. Remote-as-truth for `ListMultipartUploads` (§7) covers upload
  *existence*, not per-part cache state. Fold mpu-record restore in with the phase-5 debris work.
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
   resurrecting the object at the next namespace restore): remote `DeleteObject`, clear the
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

**The pending-set rebuild.** Startup reads and deletes every bucket's clean marker before serving
(§6); each bucket whose marker was **absent** owes a rebuild of its pending set. Serving is not gated
on it — a markerless body reads correctly, it is only not yet durable — but **eviction is** (§8), and
so is the bucket's eligibility for a clean marker at drain. It is idempotent, so a crash mid-pass just
re-runs it next boot.

Startup does not implement it: the same pass that reads the clean marker dispatches R2 on the
bucket-control actor (§7 *Buckets*) — or R1 instead, when that bucket's sync marker is missing too, in
which case the pending set is empty by construction and there is nothing left to rebuild. Cached mode
only: durable writes commit on the remote, so there is no pending set, no clean marker, and no startup
scan.

The pass rebuilds the pending set by triage rather than per-key round trips:

1. A streaming merge-join of `<data><b>` and the remote bucket — both return keys in order, so one
   page per side stays resident however large the keyspace is, and there are no per-key requests.
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
the shadow (§6) — so the closed form applies exactly. An eviction tombstone owes nothing by
definition, since an evicted body is already on the remote; the pass therefore checks only that the
remote still holds it, which is invariant **I3**.

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

### Background: the two recoveries

hypha recovers from exactly two failures, one per marker (§6), and they are **not** variations of
each other. Their premises are opposites, so they are two passes rather than one pass with a flag:

| | **R1 — namespace restore** | **R2 — pending-set rebuild** |
|---|---|---|
| signalled by | the **sync** marker is absent | the **clean** marker is absent |
| recovers from | cache volume loss | writes acked without their marker landing |
| may assume | nothing about the cache namespace | the cache namespace is **authoritative** |
| modes | both | **cached only** — durable has no pending set |
| bucket phase | `Restoring` (reads come from the remote) | `Ready` (reads are cache-served) |
| may mutate | `<data>`/`<meta>`, and only keys the cache **lacks** | **markers only** |
| driven by | the remote cursor | the cache cursor |

Both are owned by the bucket-control actor, whose per-bucket serialization and single recovery slot
are what make them **mutually exclusive on a bucket** structurally rather than by convention. Which
one a bucket needs is decided once, from its two markers, by the startup resolution that dispatches
it (§7 *Buckets*). Both markers absent is **R1**: a bucket with no trustworthy namespace has nothing
for a pending rebuild to work from, and R1 leaves the pending set empty and complete on its own.

**They share no traversal**, because they do not need the same one. R1 walks the **remote cursor
alone**: the check that decides its mutation is made under K's own lock, so a cache listing could
only ever be a stale hint about the same question. R2 needs both sides **correlated** — its question
per cache entry is *does the remote hold this generation*, which a listing answers from §6's closed
form but would otherwise cost a HEAD per key, and invariant I2 is only expressible as *remote keys
the cache has no entry for*. So R2 carries a streaming merge-join (one page resident per side,
whatever the keyspace size) and R1 carries a plain paginated walk. A common traversal would mean each
pass hauling the other's machinery.

Both filter hypha's own keys out of the remote listing (§6, `is_reserved_remote_key`): the remote
bucket is client keyspace *plus* the halt marker, and every key that survives that filter goes to a
trailer read.

**Neither pass overwrites a cache entry from a listing snapshot.** That is what lets both run against
a bucket that is being served the whole time, and it is why neither needs the re-read-under-lock
dance a merge would: R1's one mutation is guarded by an under-lock absence check, and R2 writes
nothing but markers.

**R1 — the namespace restore.** Triggered by the restore overlay (§7 *Buckets*) the first time an op
finds a bucket's sync marker absent. Until it completes the overlay makes the remote that bucket's
read source of truth, and — the load-bearing part — **writes run with durable semantics for the whole
window, in both modes**. A cached write would ack off the cache and leave committed state in a
namespace every reader is being told to ignore and the pass is about to declare authoritative; a
durable one commits on the remote first and settles exactly the tombstone the restore would have
materialized. Running durable is what makes "the cache holds nothing authoritative" *true* rather
than assumed, and it is what the rest of the design leans on: the remote genuinely is the read source
of truth (no pending overlay is needed, and none exists), and the pass can be purely additive.

The pass, over the one bucket's keyspace:

1. Ensure the bucket's `<data>`/`<meta>` projections exist — a lost volume takes the buckets with it
   — draining any stale orphan first.
2. For every remote key the cache has **no entry for**: under K's lock, re-check absence, then settle
   the eviction tombstone + twin by the repair rule. A key the cache *does* hold is left untouched.
   During a restore there are only two ways it can have one — a tombstone this pass (or an earlier,
   crashed run of it) already settled, or the settle of a write committed during the window — and
   both are current. Overwriting either would at best erase the client pass-through the tombstone is
   the only copy of, and at worst roll a committed write back to a superseded generation.

   Additive is also what makes the pass idempotent across crashes and immune to snapshot skew: a
   stale listing can only cause it to skip a key that has since acquired an entry, which is what it
   would do with a fresh listing anyway.
3. Write the sync marker; flip reads back to the cache. The bucket is **accounted** at that moment
   (§6) by construction rather than by enumeration — durable writes owe no pending markers, so the
   pending set is empty and complete when the marker lands.

Facts come from the object's authenticated tail trailer — one bounded suffix GET per key, single-part
and composite alike (§6). Throughput is **fan-out over one cursor**, not a sharded keyspace. The
per-key work is a trailer read plus two small writes, so a page of a thousand keys is three thousand
round trips behind one listing call: a single cursor feeds any concurrency worth running, and
splitting the keyspace to parallelize the *listing* would be optimizing the one part that is already
free. Shard boundaries would also have to come from somewhere, and every source of them —
`delimiter=/` discovery, a persisted key-count sketch — is approximate, so the shards arrive
unbalanced and the pass ends when its unluckiest one does. Fanning out from one cursor is balanced by
construction, and it removes the join a sharded pass would have to make before writing the marker.

**R2 — the pending-set rebuild.** The namespace is authoritative here, which is the whole difference,
so the pass re-derives the *index* and nothing else: walk `<data>`, classify each entry against the
remote, raise a marker wherever the cache holds a generation the remote lacks — a live body it has
never held, or a delete-tombstone it has not honoured. It never materializes a key from the remote
(on a ready bucket cache-absent *is* the client's 404, so rebuilding one would resurrect a deleted
object) and never settles an entry from a listing (which is what would roll an acked write back).

It takes **no per-key locks**. Raising a marker is last-writer-wins and the sweep clears one by
CAS-ing on the marker's own ETag, so a marker raised beside a concurrent upload costs at most one
redundant upload and never a lost write. The only locks it takes are on the two paths about to
declare an invariant violation, where a stale snapshot must not be allowed to halt a healthy
deployment.

Triage keeps the pass to the two listings in the common case: a key the remote lacks diverges
outright, and for one it holds, a single-part object's framed size is the closed form over the cache
body's plaintext length, so any overwrite that changed that length is caught with no extra request.
Only a same-length overwrite is ambiguous, and only it pays a tail read.

**Invariants, and the halt.** Both passes assume properties that cannot be false, and check them.
A violation does not mean a request failed; it means hypha's picture of its own data is wrong, so
every later answer is suspect and the recoveries themselves become unsafe to run. The response is not
an error to propagate: shut the server down, record the violation on the remote, then exit (§6, *The
halt marker*).

| | invariant | detected by |
|---|---|---|
| **I1** | no live plaintext body in `<data>` while the namespace is restoring — the write-mode gate makes one impossible, so one here means the gate leaked | R1, one bounded `<data>` page before the walk |
| **I2** | no remote-only key on a `Ready` bucket — cache-absent is the authoritative 404 there, and no path can produce this: every site that removes a `<data>` entry does so only once the remote object is gone (the delete propagation deletes the remote *first*) | R2 |
| **I3** | no eviction tombstone whose remote object is missing — the remote lost bytes hypha reported as committed, and the tombstone is the only surviving record they existed | R2 |
| **I4** | no pending-set rebuild in durable mode — a data-clean fault, but it means the classification is wrong, and the rest of it cannot be trusted either | dispatch |
| **I5** | every remote object carries a trailer that authenticates | every trailer read |
| **I6** | no remote bucket hypha did not create — it owns both backends outright, and startup resolved every bucket the remote held | `CreateBucket`, against the resolved map |
| **I7** | a `Ready` bucket's sync marker does not disappear — nothing removes it, so its absence is the cache volume dying under a live process | the volume watchdog, every `volume_watch_interval_ms` |

I2 and I3 are re-read under K's lock before being called violations. R2's two cursors are snapshots
taken at different moments and the benign interleavings are ordinary — the cache listing runs, a
client writes K, the sweep uploads it, and the remote listing then sees a key the cache listing could
not. A halt is a deployment-wide outage, so it may only ever be raised on state that is still true at
the moment the process says so.

**I1 is deliberately a sample, not a proof.** It is a bug detector rather than a correctness gate, so
it costs one `<data>` page before the walk instead of a second cursor correlated across the whole
pass — decisive in the failure R1 exists for, where a lost volume leaves `<data>` empty, and a sample
on a crash-resumed pass whose entries are tombstones an earlier run settled. Missing one costs
nothing: R1 is additive so it cannot overwrite the body, and the cached write that produced it owed a
pending marker by the ordinary route, so the reconcile sweep still drains it once the bucket is
ready. What the check buys is catching the leak loudly, near the code that caused it.

Serving is never gated on either pass: a conditional write to K mid-R1 first materializes K's remote
state into the cache, then runs the normal §4 path.

### Lifecycle

- **Startup.** Before the listener opens: walk the remote's bucket list, read both markers per bucket
  (§6), delete the clean marker as it is read, and publish the bucket's readiness plus the recovery it
  needs. A bucket whose clean marker cannot be deleted is not served — startup fails rather than
  serving a bucket that will skip next run's scan. Sub-second at homelab bucket counts (two HEADs and
  at most one DELETE each); the passes themselves run in the background behind it.
- **Graceful drain.** On SIGTERM, in three phases, each with its own budget:

  1. **The API closes** — stop accepting, await hyper's connection drain.
  2. **Obligations settle** — join the startup shadow sweeps (a sweep only earns its bucket's
     accounting by finishing), then close the repair queue and let its worker run to `None` → if
     nothing is left owed, a clean marker (§6) for each bucket this run accounted for and for no
     other. This precedes phase 3 deliberately: the clean markers are the run's durability-relevant
     output and must not queue behind a bucket recovery that can run for minutes.
  3. **The remaining actors quiesce** — the shutdown signal goes out, the last handles drop so each
     queue closes, and every actor is *joined*: it finishes the messages it already holds and returns
     on its own. Nothing is aborted while it still has work. The GC pass in flight is awaited (its
     evictions hold a key's write lock across a twin write and a CAS, and a cut between the two leaves
     a twin beside a live body); rotated recency slices finish their PUTs; the bucket actor completes
     pending Create/Delete and any recovery under way. The one exception is a *queued* background
     rehydrate, which is shed rather than started — its whole value is saving a future read a remote
     fetch, and after phase 1 there are no future reads.

  Then **release the active claim** (passive promotes sub-second, no fence). The phase-1→2 ordering is
  the quiescence proof of §7, and the release is last for the reason given there. A best-effort final
  reconcile pass can shrink the pending set anywhere in here — it is an optimization, since a clean
  marker claims the pending set is *complete*, not that it is empty.

  The budgets are fixed, not configurable: they are a property of the pod's `terminationGracePeriod`,
  which must be at least their sum plus the `preStop` delay, so the numbers only mean anything
  together. A phase that overruns aborts what is left and logs it — the alternative is being SIGKILLed
  with the same work outstanding and nothing in the log to say so. Overrunning phase 1 or 2 leaves the
  clean markers unwritten and the promoted replica scans; overrunning phase 3 leaves only debris that
  the next run's sweeps already look for.
- **Remote unavailable** → hot reads fine; tombstoned reads fail cleanly; cached-mode writes
  still ack and markers accumulate; durable-mode writes fail (correctly — they can't be made
  durable).
- **Cache volume loss** → discard and restart: the sync marker is gone, the namespace restore
  rebuilds; the only loss is the cached-mode pending set.

## 8. Tiering / GC — the scavenger actor

A single actor of the active (the passive never scavenges), phase 5, owning everything GC
remembers — the recency ring, and the yields and pressure rung below. Its loop never awaits I/O:
it decides *when*, and dispatches each pass and each slice persist as their own task, because a
listing-heavy pass run inline would stall the touch queue and shed exactly the traffic GC most wants
to remember. The rest of the process reaches it through one call, `touch`. In durable mode
there are no bodies to evict — the task only sweeps debris: orphan twins, leftover transition
marks (repaired per §7), and **all mpu record ranges** — both those of uploads abandoned without
complete/abort *and* the leftovers of completed/aborted uploads, whose inline drop is deferred here
(§6, *Multipart upload state*) so complete/abort never pays a large single-object delete on the
client path — the sweep is the only path that reclaims one. Each range is self-describing by its
`0x01 0x01 m ‖ <upload-id> ‖ 0x01` prefix, so it is found and reclaimed without a side index.

Only the upload range gets a listing of its own, because only it *names* itself. The other two —
a twin left beside the wrong K, a transition mark nobody came back for — are single objects
interleaved with the live keyspace, so instead of a walk each they **ride the pass's probes**, which
already read both namespaces and already classify every entry to find eviction candidates: a mark
falls out of the `<data>` walk, a twin out of the `<meta>` one. Nothing extra is listed for either.

Which is why **the probes run in both modes and under no pressure**. Read as the eviction scan alone
they would be pointless in a durable deployment, which holds no bodies to evict — but a durable
deployment accrues twins on every write and a mark on every crashed bracket, and those are found
nowhere else. The pass therefore always walks; what pressure and mode decide is only whether a body
it found may be *taken*. The cost is the two probes, which is what the separate debris sweeps they
replaced cost anyway, and the same pages become eviction's the moment there is a byte target.

Both are judged **under K's write lock**, taken with `try_lock` and yielded to whoever holds it —
the only window in which K and the thing beside it are consistent. A twin is written before the
tombstone it projects, so an unlocked test would see that gap and delete the twin of a tombstone
about to exist; a mark under a free lock is exactly the crash-leftover inference the read path
already makes. A twin is judged by re-deriving it: K's own tombstone metadata is the authoritative
copy of the facts (§6), so the twin it *would* be given now either is this key or this key is
debris — which covers the stale-generation and several-twins cases without either being a case. In cached mode it additionally evicts under
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

**The recency ring.** Recency is a **Bloom-ring sketch** — one filter per **fill window**; retired
slices persisted per §6, reloaded on promotion, retained k deep. Every op that resolves or lands a
single key feeds it: GET/HEAD/GetObjectAttributes **and the write path** (PUT,
CompleteMultipartUpload, CopyObject's destination). The ring is state **inside the GC actor**, so a
touch is one send and a request pays no I/O, no lock, and never the rotation its own touch happened
to trigger: the bit set, the rotation and the encode of the retired slice all run on GC's task, and
the persist of the retired slice is dispatched off it. A full touch queue sheds rather than blocks
(§8 *advisory, never incorrect*). LIST is deliberately **not** a feeder — one
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

**Target-driven eviction — the pressure ladder.** A pressure-triggered pass owes a byte
target: reclaim from current usage down to the low-water mark. The scavenger walks the keyspace
by **probabilistic scan**, evicting only candidates at or above the current **age threshold**, which
starts at *miss* — the keys the ring affirmatively vouches nothing has touched. LastModified is the
tie-break within a bucket (rehydration lands a fresh mtime, so a just-restored body sorts young).

**Sampling, not a walk.** A *probe* lists from a random position in a bucket and reads at most five
pages. There are two per bucket, one per namespace, and each returns everything its pages held rather
than only what eviction asked for: `<data>` gives live bodies and transition marks, `<meta>` gives
shadow bodies and twins (the two `<meta>` ranges are disjoint and not contiguous, so its position is
aimed at one or the other with even odds rather than drawn over the space between them). Nothing tracks a cursor, and the position is fresh every probe: a rotating cursor makes
eviction pressure correlate with key name — keys early in the keyspace examined on every boot, keys
late in it only under sustained pressure — and has both replicas sweep in lockstep after a failover.
Sampling also stops the scan cost from scaling with the keyspace rather than with the pressure, which
is what a full loop over a cold, mostly-untouched bucket spends its round trips on.

`start-after` takes a key, so a random position is a random *key-shaped string*, which lands the
probe at the first key at or after it. That is uniform over the keyspace, not over keys, so it
favours regions behind large gaps — and the dominant term is coarse: almost all of the string space
holds no keys at all, so a bucket whose keys all begin `logs/` takes nearly every probe from before
its first key or after its last, piling them onto the head of the one populated run.

Both corrections are **learned from the probes themselves and held in memory**. Across buckets, the
yield feedback below. Within one, a small **prefix distribution** — a decaying sketch of how the
bucket's keys are spread over their leading characters, counted from every page a probe already
reads, and used to draw those leading characters so a position lands where keys are known to be. A
share of positions stays unshaped, for the same reason the yield weighting keeps a floor: a
distribution that only draws where it has already looked cannot find a prefix that appeared
afterwards.

Deliberately **not persisted**, and not a shared object. A cold start costs one round of unshaped
probes — the behaviour that was correct on its own before any of this — so nothing here can be
wrong, only absent, which is what makes an in-memory sketch the whole of the mechanism rather than a
cache in front of one. The alternative considered and rejected was a reserved key holding
approximate per-prefix counts, refreshed by these same probes: a persisted structure, a refresh
cadence, a staleness rule and a fallback path, all to save a first pass from being uninformed.

**Where to probe is learned.** Each bucket carries a running **cold yield** — evictable candidates
per page — and buckets are sampled in proportion to it, so a bucket that is mostly working set stops
consuming probes that a mostly-cold one can use. The weighting keeps a **floor**: pure proportional
sampling locks onto early winners and never revisits a bucket that went cold later, which would be a
scan that learns once and then stops learning.

**The escalation ladder — cheapest response first.** Pressure has four answers, and the order they
are spent in is the design. How *often* the scavenger passes, how *wide* each pass runs, and only
then how *warm* a key it will take:

- **Rung 0 — debris.** A pressured pass sweeps debris (below) *before* it evicts, and those bytes
  count against the target. This is reclaim at zero rehydration risk — nobody was ever going to read
  an abandoned upload's parts — so a target met from debris alone evicts nothing at all.
- **Rung 1 — the interval.** Shorten toward `min_interval_ms`, whose floor is a continuously running
  walk. This is the answer when candidates *do* exist at the current threshold and the walk is
  simply too slow to reach them; it also accelerates the ladder itself, since loops complete sooner.
- **Rung 2 — the concurrency.** Raise toward `max_concurrency`. The answer when the per-key work — a
  remote HEAD, a twin write, a CAS — is the bottleneck rather than the cadence.
- **Rung 3 — the age threshold.** One bucket younger, per the rules below.

Rungs 1 and 2 spend nothing but **work**: round trips, bandwidth, and CPU the deployment already
has, all of it recovered the moment pressure drops. Rung 3 spends the **quality of the decision** — a
warmer key is likelier to be wanted back, so it is paid for later, by a client, as rehydration
latency and (cached mode) a re-upload. Hence the order: exhaust what costs work before spending what
costs the client.

Their bounds are not tuning. The scavenger shares the remote with the client path and the reconcile
sweep, so `max_concurrency` is the promise that an emergency reclaim cannot starve the reads it is
supposed to be protecting.

The threshold moves **in both directions, one rung per completed pass** — a pass being one round of
probes across the sampled buckets, which is the unit of evidence a sampling scan can offer. The same
unit governs the whole ladder, so it is one control law rather than three:

- **up a rung** when a pass completes with the target unmet: interval first, then concurrency, and
  the threshold **younger** only once both are at their bounds — approximately coldest-first without
  buffering the keyspace, paying extra passes only under the pressure that justifies them, and
  converging on the target whenever evictable bytes exist instead of stalling because too much looks
  recent. *Approximately*, because a sampling scan sees a sample: it evicts the coldest of what it
  found, not the coldest that exists, and the yield weighting is what keeps the sample worth taking;
- **down a rung, LIFO**, when a pass completes with the target met: the most recently taken rung is
  the first surrendered, so the threshold moves **older** before any cheap rung is given back and the
  expensive one is never held longer than the evidence supports. The threshold therefore tracks
  *sustained* pressure rather than the worst moment the process ever saw. Without this the mechanism
  is a ratchet: one burst leaves it evicting warm keys forever, which is protect-nothing — the mirror
  of the protect-everything failure the ring's fill-driven rotation exists to prevent;
- **reset to base** when usage falls below the low-water mark: interval and concurrency to their
  configured values, threshold to *miss*, since with no pressure at all nothing justifies evicting a
  key the ring still vouches for.

Capping movement at one rung per pass is what damps the control: nothing can move faster than the
scan can observe what the previous setting actually yielded. Deliberately not a proportional map from
pressure onto a rung — that would pick an aggressive threshold on a spike even when the keyspace is
full of *miss* keys that would have met the target on their own, spending rehydration risk it never
had to.

**One exception, and only for the reversible rungs.** A cache filling faster than a pass completes
never reaches rung 1, because the evidence never arrives. So usage climbing *while a pass is in
flight* sends rungs 1 and 2 straight to their bounds without waiting for one — they cost only work
and are given back the moment a loop meets its target, which is exactly what makes jumping them safe.
The threshold never jumps: its cost is not the deployment's to take back.

The ladder **clamps at its top**: a target still unmet with the interval at its floor, the
concurrency at its ceiling, and the threshold at its youngest bucket means the cache is undersized
for its working set, and the choice is thrashing or running out of space. It keeps evicting —
refusing is the worse failure — but this is the one GC condition that **warns** (§10) rather than
passing silently, because it is the only one an operator must act on.

A pass that meets its target may still keep taking *misses* the walk encounters, bounded per pass —
over-evicting an affirmatively cold key is nearly free in rehydration risk, yet each eviction still
costs a remote HEAD, a twin write, and a CAS, hence the bound. Recency is priority only: it never
overrides the correctness gates below. Eviction of candidate K with version-token ETag `E_v`:

1. **Skip if the marker exists** (`HEAD <meta><b>` at bare `K`) — a cheap local short-circuit that
   spares the remote round trip, not the correctness gate.
2. **Confirm the remote generation** (`HEAD` remote K): absent ⇒ not durable ⇒ skip; framed size ≠
   `ciphertext_len(plen) + |trailer|` for this body ⇒ some other generation ⇒ skip. Only a
   same-plaintext-length candidate is ambiguous, and only it pays the trailer's `cetag` (one ranged
   tail GET) to settle it — the same triage the pending-set rebuild runs (§7). A skip here **raises
   the key's pending marker** on the way out: this HEAD has just established the one thing a marker
   records, and step 1 already established there is none, so the reconcile sweep would otherwise
   never learn about a body only this path can see (below).
3. Under K's lock: delete stale twins, write the fresh twin, then overwrite K with the eviction
   sentinel via `PutObject If-Match: E_v` — metadata carrying `cetag`/`plen`/original mtime. The
   tombstone is an atomic in-place replace: a racing GET sees body or tombstone, never 404.
   Twin-before-tombstone means a sentinel always has its twin; a crash between leaves a twin next
   to a live body — ignored by classification (§6), swept later.

A writer landing anywhere between steps 1 and 3 has moved the ETag, so step 3's `If-Match: E_v`
fails and eviction retries next pass — the layering (marker → remote generation → conditional CAS)
makes every interleaving auto-healing, never lossy.

**Shadow bodies** (§6) are the other thing in the cache holding a client's plaintext, so the same scan
finds them — a prefix probe of `0x01 0x01 b`, since a shadow key is a digest and no client key derives
one. **K is not recoverable from a shadow key**, and that shapes both halves of the recipe above, in
opposite directions.

*The gates fall away entirely.* A shadow exists only because a rehydrate fetched that composite from
the remote and decrypted it, and the land leaves K's tombstone and twin untouched throughout — so the
remote demonstrably holds the object, K still points at it, and dropping the shadow costs at most a
re-fetch on the next read. Nothing to gate, nothing to lock (there is no K to lock on anyway), and
nothing that can be half-applied: one **conditional delete on the shadow's observed ETag**, which is
there not for correctness but so a rehydrate that landed a newer generation mid-pass isn't thrown away
seconds after a client waited for it.

*The ordering has to be arranged in advance.* Ages come from a ring keyed by `<bucket>/<key>`, and a
Bloom filter has no enumerable contents to search backwards through, so a probe holding only a digest
has nothing to ask — unless the ring was fed that same digest in the first place. It is, and it costs
nothing: **a touch records whichever artifact actually holds the plaintext**, which the client ETag
decides. Single-part ⇒ bare K; composite ⇒ K's shadow key, because K then holds a tombstone that is
never an eviction candidate, so recording K would protect nothing that can be taken. That replaces K's
touch rather than adding to it, so ring fill is unchanged, and it is exact even for a shadow that does
not exist yet: the read that raises a rehydrate is precisely the interest the shadow it creates should
inherit. Rejected as worse: carrying K in the shadow's metadata (reintroduces the key-length condition
the digest key exists to lift, so long keys would stay unorderable — a partial fix in the worst place),
and a side index from shadow key back to K (an object and a write per rehydrate, for a mapping the ring
carries for free).

LastModified deserves a note, because it is the obvious fallback and it is *wrong here*: a shadow's
mtime records when the shadow landed and no read ever moves it, so a shadow served ten thousand times
looks exactly as old as one served once — the opposite of the live-body case, where rehydration landing
a fresh mtime is what makes the tie-break meaningful. It stays the within-age tie-break for want of
anything better, but the ring is what actually orders shadows. Getting that ordering wrong costs more
than elsewhere, too: shadows are by construction the *large* objects, so each one returns a lot of bytes
against a target while its miss is a whole multi-part re-fetch and decrypt.

**Orphaned shadows are a third obligation of the marker shape.** A K that is **deleted**, or
overwritten by a **single-part body** or by a **newer composite**, leaves its shadow unreachable — a
read serves one only against K's current tombstone `cetag` — and also *unrankable*, since nothing
touches it again for the ring to have an opinion about. The pressure scan above eventually takes it as
a miss, but on a cache that never fills it simply sits. So it gets the same three-piece treatment §7
gives the pending set, which is what makes the expensive detection conditional on evidence rather than
standing:

1. **The queue.** Every cached write hands K to an unbounded queue after its commit, so the enqueue can
   neither block the ack nor fail. **Unconditional, and that is forced**: an unconditional cached PUT
   takes no lock and never reads K (§7) — it is fenced against eviction by the remote-generation
   confirm, not by a lock — so it cannot know whether it superseded a composite, exactly as it cannot
   know whether it owes a pending marker. The actor is what resolves whether an obligation means
   anything, and one shadow-range listing settles a whole batch: a shadow this run orphaned necessarily
   existed *before* the write that orphaned it, so a listing taken after the batch arrived is exact for
   every obligation in it. Shadows exist only for composites something read back after eviction, so for
   almost every bucket that listing is empty and the batch resolves with no further call. A key that
   does have one costs a HEAD of K — the reclaim goes ahead unless K still names the shadow's
   generation, which is also what keeps it from discarding a rehydrate that landed mid-batch.
2. **The shadow-clean marker** (§6), written at a graceful drain for buckets this run accounted for and
   only if nothing was still owed. Deliberately a **separate marker** from the pending set's, not a
   second meaning bolted onto it: a failed shadow reclaim leaks a handful of bytes, and folding the two
   together would let that withhold the pending-set marker and send the next run into a full two-cursor
   rebuild. Cheap evidence must not be able to trigger expensive recovery.
3. **The backstop**, for a bucket whose marker was absent — the only pass that can find an orphan no
   obligation covered, left by a process that crashed or whose queue never drained. It reads the
   shadow's **`ck` back-pointer** (§6) to recover K, which is the one place that field is used: a shadow
   whose K is gone cannot be reached from the key side at all, so there is nowhere else the question can
   be asked. An unreadable back-pointer is "cannot judge", never "orphan". Not a `Recovery` of the
   bucket actor — that slot is deliberately a single one per bucket so "never both a restore and a
   rebuild" is structural, and a bucket can owe a shadow sweep alongside either. The sweep needs none of
   that serialization: no lock, writes confined to the shadow range, every reclaim idempotent. It is not
   a readiness gate either, since an orphan is invisible to clients.

A **transition mark** at K counts as *reachable* in both the actor's test and the backstop's: K is
mid-bracket and its settle may land a tombstone carrying exactly this generation, so reading it as
unreachable would delete a shadow about to become live again.

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

**Eviction runs in the pass, not on the transition queue.** Rung 2 is the statement that GC's own
concurrency is what bounds the per-key eviction work — the remote HEAD, the twin write, the CAS — so
that work has to be the pass's, or raising `max_concurrency` under pressure would move nothing. It
does not need the queue's other properties either: an eviction holds K's write lock only across a
twin refresh and a 16-byte conditional PUT, not across a transfer, so there is nothing for a client
write to cancel — it simply takes the lock next, and the CAS makes an eviction that lost the race
fail cleanly. Being *in* the pass is also what lets reclaimed bytes be counted against the target,
which is the evidence the whole ladder moves on.

**The background-transition actor.** Rehydrate has a property nothing on the client path does: it is
**discardable**. The read that raised it is already being served from the remote, so abandoning one
costs the next read of that key a remote fetch and nothing else. It therefore runs as a job on one
bounded queue (`background.concurrency` at a time, `background.queue_depth` waiting) rather than as
an unbounded detached task, with three consequences:

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

**Marker healing is not a duty of the scan.** An earlier design had the walk apply the pending-set
rebuild's test to every live body it passed. That is redundant against the paths that can actually
leave a body markerless, all of which end the same way: the marker queue retries for the life of the
process, and anything still owed at drain withholds the clean marker of *every* bucket, which sends
the next run into an exhaustive R2 before it serves a request (§7). A crash, a closed queue under a
live write, a death between the body write and the enqueue — each one is already covered twice, and
neither cover is probabilistic.

Making the scan re-derive it would also mean opening a second cursor on the remote at each probe's
position, and bounding the pass to where the two pages overlap — R2's whole machinery, carried by a
job that never asks R2's question. Sharing it was the same mistake §13 records one level down.

What remains is not a failure but a *bug*: a write path that forgets to owe a marker drains
gracefully, writes a clean marker, and is never rebuilt — leaving a body that is cache-only
indefinitely, never uploaded and never evicted. That one is caught where the evidence is already
paid for. Step 2 above HEADs the remote for every eviction candidate, and today a body the remote
does not hold is simply skipped; it **raises the marker on the way past** instead. No extra round
trip, and it lands on cold keys — precisely the ones no future write would have re-owed.

**Eviction in a bucket waits for that bucket's pending-set rebuild** — before it completes, the
pending set on disk is known incomplete, and a scavenger reading it as exhaustive is the one way an
acked write is lost. The generation check (step 2 above) independently refuses those bodies, so the
ordering rule is the second of two locks on the same door, not the only one.

**Usage from the backend.** The scavenger scavenges from the high- to the low-water mark, and reads
usage through a per-backend source rather than from S3 — physical bytes, which is what sees the dead
bytes and the replication overhead the object sizes cannot. SeaweedFS takes two hops: the master's
`/dir/status` names the volume servers, each server's `/status` reports its own filesystem totals,
and the master's `vol/vacuum` is the dead-byte reclaim a pressured pass asks for before it evicts
anything live (the master applies the garbage threshold per volume, so asking costs nothing when
nothing is dirty enough). The two response shapes that matters depends on are read tolerantly, so a
SeaweedFS that renames a field degrades to "usage unknown" — GC keeps sweeping debris and warns —
rather than reading a missing field as an empty cache.

No source at all is the same degradation made permanent, and it is the right configuration for
durable mode. In cached mode it means the cache will fill, so it warns at boot: without a measure of
pressure there is no byte target, and evicting on a guess spends rehydration latency for nothing.

## 9. Configuration & deployment

`figment` (TOML + `HYPHA_`-prefixed env, `__` nesting), validated at boot. Current surface
(`config.rs`): `remote` and `cache` endpoints (endpoint/region/credentials), one
**`bucket_prefix`** for the whole deployment — every backend bucket is `<prefix>-<role>-<b>` with the
role fixed per §6, so deployments share an account in disjoint namespaces by differing in the
prefix alone, and no two roles can be configured into overlapping namespaces at all,
`mode` (`durable` | `cached`), `auth` (hypha's own
client credentials for `S3Auth`), `master_passphrase` (the 256-bit random age passphrase, from a Secret; supersedes phase 1's
`master_identity`), `serving.listen` + `serving.admin_listen` (§10's metrics and probes, on a
listener of their own) + `serving.offload_threshold` (§5), `reconcile.interval_ms` +
`reconcile.concurrency` (the §7 sweep's cadence and per-pass fan-out, and the marker queue's write
fan-out; that queue and §8's shadow-orphan queue are both deliberately unbounded and so have no depth
knob, and `interval_ms` paces the retry of each),
`background.concurrency` + `background.queue_depth` (the §8 transition actor: whole-object transfers
in flight, and how many wait before submissions are shed — so `concurrency` sits far below
`reconcile.concurrency`, since it bounds remote bandwidth rather than request count),
`gc.high_water`/`gc.low_water` (fractions of cache capacity: where a pass starts evicting and what it
reclaims down to — the gap between them *is* the byte target, so equal marks are rejected),
`gc.probe_pages` + `gc.yield_floor` (§8's sampling: pages per probe, and the share of probes handed
out evenly so the weighting keeps exploring), `gc.opportunistic_evictions` (misses a pass may keep
taking past a met target), the §8 ladder's own bounds (`gc.min_interval_ms`, `gc.max_concurrency`,
with `gc.interval_ms`/`gc.concurrency` naming the unpressured base), `gc.recency`'s ring shape
(rotation fill target, retained depth k, and the per-slice false-positive rate — the slice's bit
count is *derived* from those two rather than configured, so the pair cannot drift into a nominally
deep but saturated filter), and `gc.usage`, the tagged usage-source block (today
`kind = "seaweedfs"` with the master URL and a vacuum `garbage_threshold`). Omitting `gc.usage`
leaves GC unable to measure pressure, so it sweeps debris and never evicts — a warning at boot in
cached mode, and the correct configuration for durable mode, which has no bodies to evict. Phase 6 adds
the §4 fencing block (identity selectors, lease timings, fence-confirm timeout, settle delay).

The **drain budgets are deliberately not configurable**: they are one number per shutdown phase (§9),
meaningful only against the pod's `terminationGracePeriod`, which has to cover their sum plus `preStop`
— two knobs that must move together are better as one constant and a deployment note.

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

`tracing` spans per request (op, bucket, key, bytes, cache-hit); JSON in-cluster, emitted on the
span's **close**, so one line per request carries both the fields and the latency. The span is opened
by the same op table that reports the metric, and the request-side fields are **declared there** —
which of `bucket`/`key` an op has is part of its entry, so a new op cannot be added without
answering that, and a bucket op logs no empty `key` pretending to be one. `bytes` and `cache_hit`
are not knowable from the request, so the handler fills them in where it learns them: the same call
that decides a read resolved against the cache or the remote reports it to both surfaces at once. `metrics` → Prometheus:
rate/latency by op, cache hit ratio, **pending-marker set size + reconcile pass duration**,
**`markers_owed` and buckets left dirty at drain** (both should be flat zero — markers owed means the
cache is refusing small writes, and it is also the queue's only bound, §7),
remote-upload latency, scavenge throughput split by whether the bytes cost a client anything (debris
is free; an eviction is paid back later as rehydration latency), debris reclaimed by class,
usage vs. water marks, and the **rung of §8's escalation ladder currently engaged** — which is what
tells an operator whether GC is coping, and whose top with the target still unmet is the
cache-undersized signal, the one GC condition that warns. Phase 6 adds role + failover count +
fence-confirm latency. An invariant violation needs no metric of its own — the process shuts
down and exits, so it shows up as a crashloop and as the halt marker on the remote
(§6) — but the exit code (`86`) is distinct from any other so a supervisor can tell it apart.

The exports are a **vocabulary, not a facade**: one named function per thing that happened, taking
the numbers its caller already holds, so a metric's name, unit and labels have exactly one home and
a call site reads as the event rather than as instrumentation. The recorder is installed by the
**binary**, and with none installed every export is a no-op — which is what lets the integration
harness run many hyphas in one process.

`/metrics`, `/healthz` and `/readyz` are served on **`serving.admin_listen`**, separate from the S3
port: they are unauthenticated and in-cluster, the S3 port is neither, and they must keep answering
while it is refusing. Readiness reports what makes an answer *wrong* rather than slow — startup not
finished (in cached mode, a clean marker perhaps not yet cleared — §7 *Startup*), a drain begun, or
a remote hypha cannot reach; active/passive is a reported condition, not a readiness gate, since a
passive that failed its probe could not be promoted into.

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
- **The two recoveries** (`recovery.rs`, built): the write-mode gate — a write taken during a
  namespace restore commits on the remote, settles an eviction tombstone rather than a live body, and
  owes no marker — plus the regression it exists for: a second, conditional write to that key must
  not destroy the first. R1's additivity from both sides: a delete taken during the window is not
  resurrected, and an entry the pass *finds* keeps the client pass-through its tombstone is the only
  copy of. R2's confinement: `<data>` byte-identical across the pass, exactly the owed keys gaining
  markers. Each invariant fired directly against the backends (I1 planted as a cached body in an
  untrusted namespace — which also pins "both markers absent ⇒ restore", since a rebuild would have
  quietly raised a marker instead; I2 as a remote-only key; I3 as an eviction tombstone whose remote
  object was deleted), asserting the exit code, that the halt marker reached the **remote before the
  process exited**, and that the *next* run exits on that record alone without re-deriving anything —
  the crashloop is the contract, not a side effect. Plus: a reserved remote key never reaches a
  client and never trips the foreign-object rule. The restore-window tests seed enough remote keys
  that the window is still open when the write lands — startup resolves and owes the restore, so it
  is no longer the first request that opens one — and each asserts something only a write *inside*
  the window produces, so a window that closed early fails loudly instead of passing vacuously.
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
  owed at drain (the queue still retrying) withholds it. **A marker owed to a deleted bucket is
  dropped, not retried** — its `<meta>` projection is gone, so it can never land, and retrying it
  would withhold every *other* bucket's clean marker for the rest of the run; assert that deleting a
  bucket with a marker in flight still leaves the surviving buckets clean at drain. The drop is
  gated on the state map agreeing the bucket is gone, so assert the other side too: a `<meta>`
  projection removed underneath a *live* bucket raises **I7** rather than quietly shortening a
  pending set the run still vouches for. The quiescence ordering is the part
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
  driven off the trailer's offset table; abort cleanup — asserted against the §8 sweep, which is the
  only path that reclaims a record range (§6); crash at complete *plus*
  cache wipe ⇒ restore decrypts the facts + table off the terminating trailer part.
- **Failover/fencing**: two replicas, partition the active, assert fence→confirm→drain→promote —
  old active's writes refused at the backend before the new active writes; graceful path too.
- **Integration** (`hypha/tests/`, built): an in-process harness drives hypha over an ephemeral
  port with a real `aws-sdk-s3` client against a throwaway **MinIO** serving as *both* cache and
  remote (kept disjoint by the per-role bucket names under one `bucket_prefix`); every fixture is stateless and tears
  down its MinIO + data dir on drop. Covers the durable S3 conformance surface incl. twin-diluted
  **LIST pagination** (`conformance.rs`),
  the multipart scenarios above including the small-final-part **trailer fold** (`multipart.rs`),
  the two recoveries and their invariants (`recovery.rs`),
  model-based **proptest fuzzing** of random op sequences against a `BTreeMap` oracle (`fuzz.rs`),
  and an `#[ignore]`d **load/concurrency** suite — throughput, no-double-create-under-contention,
  parallel multipart (`load.rs`). Still to add: SeaweedFS as the cache backend, and a real zero-loss
  client (ZeroFS) against the durable endpoint.
- **GC's quiet paths** (`gc.rs`, built): the ones whose failure is *silent*, which is why they are
  tested ahead of the deferred §8 pass — a recency slice that never reaches GC's bucket, an mpu range
  never reclaimed, an orphan twin diluting every LIST that covers it, a transition mark costing a
  remote HEAD forever. The twin sweep asserts both directions from one population: the
  superseded-generation twin and the one whose key never existed both go, and the twin actually
  projecting a settled key stays — a sweep that took *that* would push its key onto the HEAD
  fallback. The mark is asserted without reading the key through hypha, since a read would repair it
  and prove nothing. All of it runs **durable**, which is the second assertion: the probes these ride
  are taken for the debris alone, in the mode where eviction would never call for them.
- **The operational surface** (`admin.rs`, built): probes and exposition against the real **binary**,
  which is the only place the recorder is installed and the admin port bound at all (§10) — a
  deployment that scrapes nothing while serving perfectly is invisible to every other test. Metric
  names are strings, so the round trip from a call site to the exposition is what pins them; the
  ladder gauge is asserted separately because it is written by a background actor rather than a
  request.
- **External conformance** — third-party suites run against a booted hypha, complementing the
  hand-written cases above (which assert hypha-specific internals the black-box suites can't see):
  - **`s3s-e2e`** (`s3s-project/s3s`, version-matched to our `s3s`): the S3 test suite from the
    framework hypha is built on. A CLI that drives `aws-sdk-s3` (path-style) against an endpoint
    from the standard `AWS_*` env; `scripts/s3s-e2e.sh` boots MinIO + hypha with the integration
    harness's config and points it at hypha. Adopted first — it speaks our exact `s3s`/aws-sdk
    versions, so a failure is a real hypha bug, not a dialect gap. Checksum trailers pinned off
    (`AWS_REQUEST_CHECKSUM_CALCULATION=when_required`) to match the client config in
    `tests/common`.

    Every in-scope case passes (full client surface minus the exempt families, §7); the only reds
    are the deferred/out-of-scope families below. `test_multipart_upload`'s red is purely its
    `checksum_crc32` assertion (deferred flexible-checksum family, not a multipart defect — hypha's
    own `multipart.rs` suite is green). CopyObject's *destination* `If-[None-]Match` half of §7's
    precondition split is not reachable: s3s 0.14.1's `CopyObjectInput` predates S3's
    conditional-copy-on-destination fields, so only the `copy-source-if-*` conditions apply.
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
Each phase's mechanism is specified in §§1–10 above; this section tracks scope and status only.

| Phase | Scope | Status |
|---|---|---|
| 1 | `hypha-format`: envelope, offset math, `RangeReader`, round-trip tests, criterion benches (§5) | Done |
| 2 | Durable serving: `hypha-core` + the s3s surface in durable mode (PUT/DELETE brackets, GET, HEAD/LIST, repair rule, buckets, auth, `Tiering`+`KeyLocks`) | Done vs. MinIO |
| 3 | Multipart (native-remote proxy, trailer + parts table, `ListParts` retag-match) and the rest of the client surface (CopyObject, DeleteObjects, ListObjects v1, ListMultipartUploads, ListParts, UploadPartCopy, GetObjectAttributes, GetBucketVersioning stub, storage-class passthrough) | Done vs. MinIO |
| 3a | The `<data>`/`<meta>` keyspace split (§6) — prerequisite for v1 LIST's `NextMarker` and the twin-suffix headroom | Done vs. MinIO |
| 4 | Cached mode, single replica: marker queue, clean marker, reconcile sweep (`replication.rs`), cached DELETE propagation, rehydrate | Done vs. MinIO |
| 4a | The two recoveries split apart (§7): additive R1 (remote walk) + marker-only R2 (two-cursor join), the per-bucket write-mode gate (durable semantics for a restore window), full startup resolution of bucket state + the volume watchdog, invariants I1–I7 and the halt marker (§6) | Done vs. MinIO |
| 5 | GC: the actor and its passes, Bloom ring + touch feeders + slice persistence, usage source + vacuum and the water marks, probabilistic scan (random-position probes over bodies *and* shadows, yield-weighted bucket choice), the pressure ladder (interval → concurrency → age threshold), threshold eviction — bodies through the three gates, shadows through one conditional delete — the shadow-orphan queue/marker/backstop, mpu-range debris sweep | Done vs. MinIO |
| 5a | The rest of phase 5: the remaining debris classes (orphan twins, leftover transition marks) riding the pass's own probes, which therefore run in both modes and unpressured; the in-memory prefix distribution shaping probe positions, §10's metrics + `/healthz`/`/readyz` on their own listener | Done vs. MinIO |
| 6 | `hypha-fence` + active-passive: two-pod StatefulSet, leader-elected controller, lease, fence→confirm→drain→promote | Not started |
| 7 | `hypha/` chart, dashboards, the two production installs (cached + durable) | Not started |

Per-phase exit criteria live with their test suites (§11) rather than restated here; phases 1–4's
suites are `hypha-format`'s round-trip/bench tests and `hypha/tests/{conformance,fuzz,multipart,
copy,cached,recovery}.rs` plus the s3s-e2e pass (§11). Phase 5's exit is the scavenge/rehydrate and
cache-wipe → restore → rehydrate scenarios — still outstanding, with `gc.rs`/`admin.rs` covering only
the paths that would fail silently; phase 6's is the §11 partition harness; phase 7's
is both endpoints live behind the shared Gateway.

**Phase 5a's correction generalizes past its own two features.** Both dropped items — a persisted
prefix-distribution hint, and sharding the namespace restore — existed to make a *listing* faster,
and in both places listing was never the cost: the restore pays three round trips per key behind one
page of a thousand, and the scan's bias is a property of where it aims rather than of how fast it
reads. The hint would have paid for that with a stored object, a refresh cadence, a staleness rule
and a fallback path; the sharding with approximate boundaries and a join. What replaced them is
smaller in both cases and strictly better on the axis that mattered — an in-memory sketch that
cannot be stale because nothing depends on it being right, and fan-out over one cursor, which is
balanced by construction where hint-derived shards are balanced only to the accuracy of the hint.
The rule is that **a structure invented to speed something up owes an account of which round trips
it removes**, and derived state that must be persisted, refreshed and fallen back from is the most
expensive way to buy an optimization that a cold start would have covered anyway.

**Phase 4a's correction is the one most worth keeping**, because the mistake generalizes. Phases 3a/4
implemented both recoveries as *one* traversal on the grounds that they walk the same two namespaces
and ask the same question. They do walk the same namespaces, but they do not ask the same question:
one may assume nothing about the cache, the other that it is authoritative. Sharing the traversal
therefore forced the pass to be written for the weaker premise — a bidirectional merge that overwrote
`<data>` from a listing snapshot — and that in turn put a repair-from-remote on the serving path,
where a second write into a restoring bucket deleted the first write's acked body and left a pending
marker the sweep then reaped as an orphan. An acked write lost with nothing surviving to show it had
existed. The fix was not to guard the merge but to notice that the shared premise was fictional:
split the policy, and make each pass unable to express the other's mutation.

The traversal was kept shared at first, on the grounds that both passes walk the same two namespaces
— and that was the same error one level down. They walk the same namespaces; they do not need the
same *walk*. R1's absence check is made under K's own lock, so it never needed the cache cursor a
join would give it; only R2 needs the two sides correlated. Sharing left each pass carrying the
other's machinery for nothing. The generalizable rule is that **two jobs touching the same data is
not evidence they share an invariant, or even a traversal**, and where they don't, shared code gets
written for whichever premise is weaker — a cost paid by the job that never needed it.

Two further corrections surfaced during phases 3a/4, since the reasoning likewise
generalizes beyond the specific bug: the facts alphabet (§6, *Facts encoding*) went through two
narrowings — literal space (round-trips through `encoding-type=url` LIST as `+` on MinIO) and then
`\`/`.` (MinIO rejects `.`/`..` path segments) — before landing on base64url, which excludes both
classes by construction rather than by enumeration. The current alphabet and its rationale are
described in full in §6; there is no remaining reason to special-case either excluded character
elsewhere in the code or docs.
