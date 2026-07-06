# Hypha — implementation proposal

Companion to [`ARCHITECTURE.md`](./ARCHITECTURE.md), which owns the *what* and *why* (the two-tier
caching model, encrypted remote, write-through durability, tiering/GC). This document proposes the
*how*:
language runtime, crate selection, module boundaries, the threading and concurrency model, and the
implementation concerns that determine whether the design's guarantees (linearizable conditional
writes, sound per-part encryption, bounded loss window) actually hold in code.

Nothing here changes the architecture; it commits to mechanisms the architecture leaves open.

## 1. Language, runtime, workspace

Rust, edition 2021, async on **Tokio** (`rt-multi-thread`). The workload is I/O-bound proxying with a
CPU-bound AEAD step that is fast enough (multi-GB/s per core for ChaCha20-Poly1305) to stay inline for
normal object sizes; §5 covers the offload threshold.

Split into a **Cargo workspace** — two binaries over a shared core, plus the isolated codec:

- **`hypha-format`** — the encryption envelope (age wrapper). No S3, no Tokio, no I/O. Pure
  functions and `Stream`/`Sink` adapters over `Bytes`; offset arithmetic; `StreamReader`/`Writer`
  adapters. Keeping it standalone lets it carry a `cargo fuzz` target and `proptest` suite without
  spinning up a server.
- **`hypha-core`** — the shared library the binaries link: backend S3 client wrapper, object/tombstone
  metadata model, the in-flight-PUT ref count consulted by eviction, and config.
- **`hypha`** — the **serving** binary: the S3 protocol surface (`s3s`), the data path, conditional-write
  pass-through to the cache (§4), pending-marker write, write-through replication as a reconcile sweep
  (§7), telemetry, and — as a background task that runs only while this replica holds the active claim
  — the GC scavenger (§8). Runs **active-passive** (one active writer, one pre-warmed passive standby;
  §4). Folding GC in keeps a single writer to the remote, with cache-side conditional operations
  (not an in-process per-key lock) serializing concurrent writers and eviction.
- **`hypha-fence`** — the **controller** that makes the Cilium/OPNsense allow-policy the single source of
  truth for which replica is active, and drives ordered fence-before-promote failover (§4).

## 2. Dependencies

The single most important choice is **not hand-rolling the S3 protocol on either side**.

**S3 server surface — [`s3s`](https://github.com/Nugine/s3s).** Implements the server half of S3:
request routing, SigV4 verification, `aws-chunked` decoding, XML (de)serialization, and a typed `S3`
trait with one method per operation (`get_object`, `put_object`, `create_multipart_upload`,
`upload_part`, `complete_multipart_upload`, `head_object`, `list_objects_v2`, `delete_object`, …).
Hypha *is* an implementation of that trait. `s3s` runs on `hyper`/`tower`, so the HTTP server,
keep-alive, and streaming bodies come for free. `s3s`'s `S3Auth` trait is where hypha validates the
credentials its *own* clients present (distinct from the remote's credentials).

**S3 client surface (cache + remote) — [`aws-sdk-s3`](https://crates.io/crates/aws-sdk-s3)** with
`aws-config`. Path-style addressing, custom endpoints, byte-range GET, multipart, presigning — all
needed, all supported. Both the cache and the remote are just two independently-configured client
instances; the architecture's "loose coupling" falls out naturally because both sides are the same
SDK type pointed at different endpoints.

| Concern | Crate(s) |
|---|---|
| Async runtime | `tokio` (`rt-multi-thread, macros, net, io-util, sync, time, signal, fs`) |
| S3 server | `s3s`, `s3s-aws` (type bridge to the SDK) |
| S3 clients | `aws-sdk-s3`, `aws-config`, `aws-smithy-types` (ByteStream) |
| Encryption | `age` (the reviewed age v1 envelope; provides Encryptor, Decryptor, and `StreamReader: Seek` for ranged reads), with the `async` feature for `futures`-interop |
| Key hygiene | `zeroize` (age uses it internally; hypha only zeroizes its loaded X25519 identity) |
| Bytes / streaming | `bytes`, `futures`, `tokio-util` (codec, `io`) |
| Concurrency | `dashmap` (swept per-key counter for write-aware eviction), `tokio::sync::Semaphore` (upload concurrency) |
| Config | `serde`, `figment` (file + env layering) |
| Errors | `thiserror` (library layers), `anyhow` (bootstrap only) |
| Observability | `tracing`, `tracing-subscriber`, `metrics` + `metrics-exporter-prometheus` |
| Testing | `proptest`, `criterion`, `testcontainers` (real SeaweedFS/MinIO in integration tests) |

Using `age` rather than hand-rolling an AEAD framing is a deliberate crypto-audit choice: age v1 is
a reviewed streaming AEAD format with per-chunk authentication, seekable decryption (the crate's
`StreamReader` implements `std::io::Seek` when the underlying reader does — hypha supplies a
seekable adapter over ranged GETs, §6), and a finalizer chunk for truncation. Per-file random file keys give
hypha the same parallel-`UploadPart`-without-coordination property the prior custom frame format
sought via 192-bit random nonces — but with per-file key isolation instead of a single master key
encrypting every frame.

## 3. Module layout

```
hypha-format/            (thin wrapper around the age crate)
  envelope.rs            Encryptor/Decryptor setup against hypha's static X25519 identity
  offset.rs              plaintext-byte ⇄ age-chunk ciphertext-byte arithmetic (see §6)
  stream.rs              seekable reader over ranged GETs (seek ⇒ new byte-range request) + write adapters

hypha-core/src/          (shared by the binaries)
  config.rs              typed config + validation (fail fast on bad values)
  backend.rs             ObjectStore abstraction over an aws-sdk-s3 client
  meta.rs                object metadata + tombstone model, (de)serialization
  manifest.rs            part table: per-part headers + composite-object metadata (§6)
  in_flight.rs           per-key in-flight-PUT ref count consulted by eviction (§8)
  error.rs               error → s3s::S3Error mapping

hypha/src/               (serving binary — active-passive)
  main.rs                config load, runtime, s3s server, signal handling, drain
  auth.rs                S3Auth impl for hypha's own client credentials
  s3/                    the s3s::S3 trait implementation, split by op group
    get.rs put.rs multipart.rs list_head.rs delete.rs conditional.rs
  replication.rs         remote-upload orchestration (§7); single-object PUT has no queue
  gc/                    scavenger task, runs only while active (§8)
    walk.rs              partial-scan cursor + windowed LRU eviction (single-part objects only)
    reconcile.rs         continual pending-marker sweep → remote upload, shares this task with GC (§7)
    restore.rs           sync-marker check + parallel namespace reconciliation (§10)
    recency.rs           Bloom-ring recency sketch + slice persistence (§8)
    usage.rs             backend usage measurement (SeaweedFS metrics)
  telemetry.rs           tracing + metrics wiring, health endpoint

hypha-fence/src/         (fencing controller — HA via leader election)
  main.rs                watch liveness, reconcile the active-identity allow-policy
  fence.rs               ordered fence-before-promote; Cilium/OPNsense policy writes (§4)
```

The `s3/` modules are thin: they parse intent, take the key lock, and orchestrate `backend`,
`hypha-format`, `meta`, and `replication`. Business rules live in `hypha-core` so they're unit-testable
without an HTTP layer.

## 4. Concurrency model & the linearizability guarantee

The architecture promises **linearizable create/update** for conditional writes "regardless of what
the underlying backends guarantee." That guarantee has to be *manufactured* by hypha, and it drives
the whole concurrency design.

### Single active writer, enforced at the fabric

Serving runs **active-passive**: exactly one **active** replica does all work, with a **pre-warmed
passive** standby. Because a replica is stateless (§10) and holds no side index, "pre-warmed" just
means already running with connections open to cache/remote — on promotion it has nothing to load and
serves immediately.

With a single writer, single-object conditional writes need **no in-process lock at all**: hypha
forwards the client's `If-Match` / `If-None-Match` straight to the cache `PutObject` at the same
key K that the client named, and the cache's own conditional write **is** the linearization point.
The cache must implement `If-Match` on `PutObject` (SeaweedFS does; § *Risks* flags the spike).
Authoritative existence/ETag stay in the self-describing backend objects; there is no metadata
database (the architecture's "no side index" holds in full) and no per-key lock table for the PUT
path. **Multipart takes the cacheless path** (§7): parts stream straight to the remote, where
S3's own per-(upload-id, part-number) write semantics serialize them, and `CompleteMultipartUpload`
is the durability commit. No in-process session lock is needed — the remote is the serializing
resource, not hypha.

The cache's own ETag is the **version token**, but it is not always the client-visible ETag: a
multipart composite's ETag (`md5(part-md5s)-N`) cannot be reproduced by a single cache `PutObject`,
and a tombstone's cache ETag is the fixed sentinel UUID's ETag (§8/§9), not the client-visible ETag
of the object it stands in for. Hypha stores the client-visible ETag in cache user-metadata
whenever the two differ, and a conditional write resolves by key state:

- **Ordinary body** (cache ETag == client ETag, the common case): forward the client's condition
  as-is — the cache compares.
- **Composite in cache**: `HEAD` K for the cache ETag `E_c` and the metadata client-ETag; compare
  the client's `If-Match` against the metadata, then issue the `PutObject` with `If-Match: E_c`.
  The cache ETag stays the atomicity token; the metadata is only the client-facing mapping.
- **Eviction-tombstoned key**: **restore, then compare** — rehydrate the body from the remote with
  `If-Match: <eviction-sentinel-etag>` (a concurrent writer aborts the restore), turning the key
  back into one of the two cases above. `If-None-Match: *` needs no restore: the tombstone already
  proves existence.
- **Delete-tombstoned key**: client-visibly absent, so `If-Match` fails with 412 outright, and a
  create (`If-None-Match: *` or plain PUT) is forwarded as `If-Match: <delete-sentinel-etag>` — the
  sentinel ETag stands in for "still deleted" as the atomicity token.

Marker, tombstone, and body share one keyspace — no `cache/` prefix on the key, no separate
tombstone object path. The body lives at `K`; the tombstone overwrites `K` in place (§8).
Tombstones carry **fixed sentinel bodies** — one reserved 16-byte value for eviction tombstones,
another for delete tombstones (§9), compiled in like the marker prefix — so each type has a
deterministic (size, ETag) pair and a plain LIST classifies every key as live / evicted / deleted
with no HEAD or metadata read. (A zero-byte tombstone would not: every empty body shares the
empty-MD5 ETag, so it would be indistinguishable from a legitimately empty client object.) The
16-byte width fully saturates an MD5 ETag's 128-bit entropy, so a collision with a client body
needs both a length match *and* a 2^-128 byte match; that holds well past a billion keys (the
fixed-target collision probability is `N / 2^128`, dominated by the length-match precondition in
practice — most client bodies aren't 16 bytes) and needs no admission-reject rule.

Evicted keys and cached composites additionally carry a **facts twin** for LIST (§9) — a
zero-byte object at `K ‖ 0x01 ‖ facts`. Two admission rules make the twin scheme sound: client
keys may not contain bytes below `0x20` (so the `0x01` separator sorts below every admissible key
byte and a twin sorts immediately after its own key without disturbing LIST's lexicographic
order — the prefix-key case is where a higher separator would flip it), and key length is capped
to leave suffix headroom.

The per-snapshot
pending marker lives at `<marker-uuid-prefix>/<K>/<upload-id>`, where the prefix is a fixed
reserved 16-byte value compiled into hypha — same collision math, same non-need for an
admission-reject rule. Markers apply only to single-object PUTs (§7); multipart uploads don't touch
the cache or the marker namespace.

The correctness of "single writer" cannot rest on *observing* that the old active is dead — a remote
observer can never distinguish dead from partitioned-but-still-writing. So it rests on **fabric
fencing**: the network path to the backend is the authority for who may write.

### The allow-policy *is* the lease

Rather than keep a `Lease` object and a fence policy in sync (two sources of truth that can diverge),
collapse them into one invariant, maintained by a small **`hypha-fence` controller**:

> Exactly one hypha identity is in the SeaweedFS ingress allow-list and the OPNsense egress allow to the
> remote — and that identity *is* the active.

"Who holds the lease" and "who can write" become the same fact. Two pods may each *believe* they are
active, but **belief is free — only the network-allowed pod can write**, so the writer set is always
≤ 1. The old active's belief is harmless because its packets to the backend are dropped.

**Identities are static; only the policy moves.** A pod's Cilium identity is computed from its
labels by the agent on its *own* node, so fencing must never depend on relabeling the active — a
partitioned node would never apply the change, and remote ipcaches would keep honoring the old
identity. Serving therefore runs as a **two-pod StatefulSet**: the automatic
`statefulset.kubernetes.io/pod-name` label gives each pod a distinct identity from birth, and
`hypha-fence` only flips which of the two identities the destination-side allow admits.

**Failover is ordered fence-before-promote** (correctness is in the ordering, not atomicity):

1. Active misses its lease renewal → liveness lost.
2. Controller **fences the old active** — narrows the SeaweedFS ingress allow off its identity and adds
   the OPNsense egress deny to the remote.
3. Controller **waits for the fence to be confirmed programmed** — Cilium reports the applied policy
   revision on the SeaweedFS endpoints. This answerable "is it isolated?" replaces the unanswerable
   "is it dead?".
4. **Drain the in-flight window** (see below): reset the fenced identity's established connections, then
   wait a bounded settle interval so any write already accepted by the backend has resolved.
5. **Only then** promote the passive (add its allow).
6. Passive begins serving; its in-process locks are now the sole writer's locks.

**The fence-confirm alone does not stop in-flight writes.** Programming the deny closes *new*
connections, but two windows survive it: (a) an already-**established** connection from the old active
can keep streaming, and (b) a request whose bytes SeaweedFS has already received will commit regardless
— network fencing can't un-send a write the backend already holds, and S3 has no fencing token for the
backend to reject it. Without handling this, a slow-but-alive old active with an in-flight `PUT` to key
K can have it land *after* the new active reads K → lost update. Step 4 closes it:

- **Reset established connections** on the deny (force-drop + drop conntrack for the fenced identity),
  not just block new ones. A `PUT` whose connection is killed mid-stream aborts — an incomplete S3
  upload isn't committed — so this collapses window (a) and most of (b) at once.
- **Settle delay** sized to the max time SeaweedFS needs to *finalize a request whose bytes already
  arrived*. Because connection-reset aborts long transfers, this is a small, hard-boundable quantity
  (enforced by aggressive server-side/request timeouts), not "how long can a multi-GB upload run." It
  is a *local, bounded* wait — unlike the arbitrary-process-pause problem — so it genuinely closes the
  window rather than merely shrinking it.

The gap `[fence → promote]` is the failover window: writes unavailable for fence-confirm + reset +
settle (low seconds). A graceful shutdown (deploy, drain) skips all of it — the active drains its own
in-flight requests and releases before the passive is promoted, so neither in-flight window arises.

**Why this is partition-safe.** Fencing the old active **does not require reaching its node.** The
allow/deny is keyed on the old pod's identity/IP and applied at the *SeaweedFS nodes* (and OPNsense),
which are healthy and observe the change from the API/kvstore; the partitioned node never participates
in its own fencing. That is exactly why this works where `kubectl delete` cannot — k8s delegates the
kill to the (unreachable) kubelet, whereas the fence is enforced where the failure isn't. It also
builds on existing posture: SeaweedFS ingress is **already default-deny** with a named `hypha` grant
(§ *Access to the cache surfaces* in the architecture, and the cluster's east-west default-deny
baseline), so fencing is just narrowing "allow hypha's namespace" to "allow the *active* hypha
identity" — the *absence* of an allow is the fence.

The remote leg is weaker: Cilium *egress* policy is enforced at the source node — the partitioned
one — and OPNsense may see only SNAT'd node IPs, so a partitioned-but-alive old active can retain
remote reach. Harmless on the cached PUT path (fenced off the cache, it has no new snapshots to
upload), but multipart writes go straight to the remote (§7): an in-flight
`CompleteMultipartUpload` from the old active can commit after promotion. If that window matters,
escalate the fence to the remote itself — per-replica remote credentials that `hypha-fence`
revokes, an S3/IAM policy being destination-enforced like the Cilium ingress deny. Until then it
is a documented carve-out (§14).

Reads/HEAD/LIST take no lock either way. Since serving is active-passive, the passive does not serve;
during the failover gap the surface is briefly write-unavailable, not degraded.

**Request lifecycle.** One Tokio task per request (Tokio/hyper default). Bodies never fully buffer:
they flow as `Stream<Item = Bytes>` through the encrypt/decrypt adapters into/out of the SDK's
`ByteStream`, so per-request memory is bounded by a few age chunks regardless of object size. A global
`Semaphore` caps in-flight request concurrency to bound total memory and backend connection pressure.

## 5. Threading & the AEAD CPU step

ChaCha20-Poly1305 (the AEAD age v1 uses internally) runs at multi-GB/s/core, so each 64 KiB chunk
encrypts in single-digit microseconds — short enough to run **inline on the async worker** without
starving the runtime. The age `StreamWriter` already drives the AEAD chunk-at-a-time from the async
writer, so hypha doesn't hand-roll a chunk loop; to keep any single `poll` bounded we offload to
`tokio::task::spawn_blocking` only when a single contiguous encrypt/decrypt would exceed a threshold
(configurable, default ~1 MiB of pending plaintext). This avoids a blanket `rayon` pool while
protecting tail latency under large sequential transfers. The `criterion` bench in `hypha-format`
calibrates the threshold per machine; first measurements: ~1.5 GiB/s/core encrypt, ~1.3 GiB/s
decrypt steady-state (so 1 MiB ≈ 0.7 ms of poll time), plus a per-file fixed cost of ~60–90 µs
for the X25519 file-key wrap/unwrap — which prices the per-file key isolation at noise level, and
puts one core comfortably ahead of a 10 GbE link.

## 6. age envelope, offsets, and the part table

`ARCHITECTURE.md` describes the age envelope; the implementation relies on three fixed properties of
age v1:

- **Fixed 64 KiB chunks** (65536 plaintext bytes + 16-byte Poly1305 tag = 65552 ciphertext bytes per
  chunk), so offset math is **closed-form**, not a per-chunk lookup table:
  - Ciphertext offset of chunk *i* = `i * 65552` plus the age header length and the 16-byte payload
    nonce (both constant per file, so folded into a one-time offset for the part). The header
    length varies **across** files — rage greases headers with a random stanza (found empirically;
    it broke the constant-header assumption in `hypha-format`'s first test run) — but needs no
    stored metadata: it derives in closed form from lengths hypha already has,
    `hlen = ct_len − 16 − plen − 16·⌈plen/64 KiB⌉`, where `ct_len` is the remote object's
    Content-Length and `plen` (plaintext length) is stamped on every remote object anyway (HEAD
    must report plaintext sizes). A ciphertext-prefix parse (`--- <mac>` ends the header
    unambiguously; stanza bodies are base64, no `-`) remains as validation/fallback.
  - A ranged GET for plaintext bytes `[a, b)` covers chunks `⌊a / 65536⌋ .. ⌊b / 65536⌋`, one
    contiguous ciphertext range. A cold ranged GET is **two remote reads** — the age header at
    offset 0 (to unwrap the file key) and the chunk range — coalesced into one when the range abuts
    the file head; caching unwrapped file keys for hot objects is a later win.
- **Seekable decryption.** age's `StreamReader` implements `std::io::Seek` when the underlying reader
  does. An S3 ranged-GET body is *not* seekable — it is a one-shot stream — so
  `hypha-format/stream.rs` supplies the adapter that satisfies `Seek` by issuing a fresh byte-range
  GET (one seek per request in practice: to the plaintext offset, then sequential reads). age's
  `Seek` lives on the sync `Read` path, so ranged decrypt drives the sync reader over the adapter
  (bridged per §5). Hypha still **does not reimplement chunk decryption**; the crate's deterministic
  nonces (chunk-index-derived) are what make the seek meaningful, since no chunk's decrypt depends
  on a prior chunk's state.
- **Per-file file key.** Each `Encryptor::with_recipients` invocation generates a fresh random file
  key, wrapped to hypha's static X25519 recipient in the age header on the same remote object.
  Parallel `UploadPart` workers and concurrent PUTs need no key/nonce coordination — the random file
  key *is* the coordination-free property.

This collapses the "frame manifest" from a per-frame offset table into, at most, **one plaintext
length per part** (parts may have unequal sizes, so cumulative part offsets still need the part
table; age chunks *within* a part are arithmetic). Consequences:

- **Single-part PUT (the common case): no part table at all** — the stamped plaintext length +
  the object's Content-Length + the fixed age chunk size suffice (`hlen` is derived, not stored);
  a ranged GET needs nothing beyond them.
- **Multipart: the part table is distributed across the parts themselves.** Each remote part object
  already exists and already carries S3 user-metadata, so hypha records each part's **plaintext
  length** and **plaintext MD5** (`x-amz-meta-plen`, `x-amz-meta-pmd5`) on the part object that
  holds that part's age ciphertext (per-part `hlen` derives from `plen` + the part's own size). There is no separate manifest artifact — the per-part facts live on
  the per-part objects the remote already stores.
- **`CompleteMultipartUpload` assembles the composite.** It `ListParts`s the remote upload, reads the
  per-part headers, and writes a small **composite part table** (`Vec<PartLen>` + `Vec<PartMD5>`,
  ~40 B/part) onto the composite object's **user-metadata**, plus the composite ETag (§9). The table
  is what lets a ranged GET map a plaintext range across part boundaries without a remote round-trip.
- **Overflow degrades to `ListParts`, not to a sidecar.** S3 user-metadata caps around 2 KiB, so the
  inline composite table covers ~50 parts; beyond that, `CompleteMultipartUpload` records only the
  ETag and part count inline and **omits** the part table. A cold ranged GET of a tableless object
  falls back to one `ListParts` call to recover the per-part headers from the part objects — a single
  round-trip on a path that is already a cache miss, never a second remote write. The tombstone /
  metadata records whether the inline table is present.

**Why no sidecar.** Distributing the part facts onto the objects the remote already stores makes the
multipart state **atomic by construction**: a half-finished upload simply presents as "part *k*
missing" via `ListParts` — detectable as incomplete, never as corrupt. There is no second remote write
(the sidecar) to order against the body, so the sidecar-atomicity hazard (the one ordering risk the
design used to carry) does not arise.

**Why no per-frame AAD binding.** The custom-format design bound `object_uuid ‖ part_number ‖
frame_index ‖ flags` into the AEAD's AAD slot per frame, to make splice/reorder/cross-object
tampering fail authentication. age v1 doesn't expose an AAD slot — instead it achieves the same three
properties by (a) **per-file key separation** (a chunk from object A dropped into object B's slot
fails to decrypt with B's file key — clean key-separation beats AAD binding for the cross-object
case, depending only on the AEAD primitive rather than on per-call AAD handling), (b) **chunk index
in the nonce derivation** (reordered chunks fail authentication), and (c) **finalizer chunk** with a
special tag (truncation detectable cleanly). Hypha does not need to assign a stable per-object UUID
for AAD binding; age's per-file file key is the binding.

## 7. Write-through replication & durability tracking

Two code paths, selected by whether a cache is configured:

**Cached (default).** Hypha forwards the client's `If-Match` / `If-None-Match` straight to the
cache `PutObject` at the client's key K — the cache's own conditional write is the linearization
point (§4). On the way through the request body, hypha streams plaintext to K (the cache absorbs
the overwrite atomically with the precondition check) and computes the ETag. Once the cache
confirms the conditional PUT, hypha writes a small **pending marker** to the cache at
`<marker-uuid-prefix>/<K>/<upload-id>` — a per-snapshot marker carrying the etag of the cache body
it represents — and **acks the client**. No remote round-trip before ack; the marker is the only
extra cache write on the hot path. The marker prefix is a fixed reserved UUID compiled into hypha
(§4); no admission-reject rule is needed. The marker and the body it tracks both live in the cache,
on the same NVMe volume; both survive a process crash, both die together on cache-volume loss. There
is no in-process upload queue, no per-key upload lock, and no `durable` flag in cache object metadata
— the pending set IS the durability signal, in the local tier where it is cheap to enumerate.

**Cacheless.** PUT frame-encrypts and uploads to the remote **inline**, acking only after the remote
confirms. No marker, no pending set, no loss window, higher per-op latency — exactly the zero-loss
profile the architecture reserves for clients like ZeroFS. **Multipart always takes this path**,
cached deployment or not — `UploadPart` encrypts and streams straight to the remote per part,
`CompleteMultipartUpload` is the durability commit (parts already durable on the remote, this just
glues them into the composite at K) — and, in a cached deployment, it also writes an **eviction
tombstone into the cache at K** plus its facts twin carrying the composite ETag/`plen`/mtime
(twin-before-tombstone, §8/§9; conditional on the prior cache ETag when one exists), atomically
replacing any stale cached body and keeping the cache namespace complete for LIST.
`AbortMultipartUpload` deletes the orphaned parts. Parts stream around the cache; only
`CompleteMultipartUpload` touches it (with the eviction tombstone above). The composite becomes
cachable later, on first read, via the same rehydrate path used for tombstoned bodies (§8) — a GET
fetches and decrypts from the remote, then asynchronously populates the cache from the decrypted
plaintext.

**Reconcile is the upload path (`replication.rs` + `gc/reconcile.rs`).** There is no in-memory upload
queue; reconcile drives remote uploads. It runs as a continual background duty of the active, sharing
the scavenger task/thread with GC (§8) — one task, so same-key uploads never overlap. Each pass:

1. `ListObjectsV2` the **cache** with `prefix=<marker-uuid-prefix>/` — a local SeaweedFS LIST over
   NVMe, `O(in-flight + pending)` per pass, not a full-keyspace scan and not a remote round-trip.
2. Group markers by key, and **dispatch per key on the cache body at K**: a key whose cache body is
   the delete-sentinel UUID is a DELETE pending (§9); any other body (including the eviction
   sentinel, which restore has already turned back into a live body) is an upload pending. The two
   branches share the marker machinery but propagate different operations to the remote. The
   cache-side conditional-write chain IS the ack order in both cases, so cache(K)'s state is the
   linearization result, no in-process ordering invariant needed.
3. **Upload branch.** For each key K dispatched here, `HEAD` the cache at K for its current etag —
   the latest acked version. Among K's pending markers, the one whose etag matches cache(K)'s
   current etag is the snapshot to upload. The others are older snapshots superseded in cache
   before they reached the remote — drop them (their conditional delete at step 6 also clears
   them). No LIFO, no per-key upload lock: the cache's etag *is* the truth of "what to upload,"
   and reconcile just reads it.
4. Upload the cache body (frame-encrypted) to the remote at key K **unconditionally**. A concurrent
   overwrite of cache(K) racing the read is harmless: the body reconcile read is a coherent
   snapshot of `E_n`; if cache moves to `E_{n+1}` mid-upload, that snapshot lands on the remote
   and the next pass uploads `E_{n+1}`. Self-heals within one pass; the remote lags the cache by
   ≤ one pass under sustained overwrite contention on K, which clients cannot observe (cache is
   current, remote is read only on tombstoned post-eviction GET, by which time reconcile has
   caught up).
5. **Delete branch.** For each key K dispatched here, issue the remote `DeleteObject` at K, then
   clear the delete-tombstone in the cache with `If-Match: <delete-sentinel-etag>` (K goes back to
   an ordinary absent key for LIST) and conditionally delete the marker. A concurrent create at K
   races the tombstone clear benignly: its `PutObject If-Match: <delete-sentinel-etag>` either
   wins first (K is live; the clear's `If-Match` fails and reconcile drops only the marker, leaving
   the live body) or loses (the clear wins, then the create fails 412 and the client retries — the
   same semantics as if the delete had fully propagated before the create arrived).
6. **Conditionally delete** each remaining marker for K with `If-Match: <etag-the-marker-was-written-with>`.
   Load-bearing for two races in one: (a) if reconcile is ever scaled to multiple workers on the
   same key, two workers can both upload the same snapshot but only one's conditional delete
   succeeds; (b) if a writer C lands on cache(K) between step 3's HEAD and step 4, C writes a
   new marker for `E_{n+1}` — reconcile's conditional delete against the old marker still
   succeeds, because per-snapshot markers are append-only (writers create new markers; they never
   overwrite old ones in place).

Reconcile runs concurrently with serving; the durability-gates-GC fence below keeps markered
cache objects safe until reconcile resolves them, so neither serving nor promotion blocks on it. On
failover the new active's sweep simply lists the marker prefix from the start — markers and bodies
both survived in the shared cache. The set is bounded by `O(in-flight + pending)`, so each pass is
cheap; the cacheless path writes no markers and has nothing to reconcile. Without reconcile, a
marker whose delete was lost would remain forever in the cache — a small local-space leak (markers
are sub-1-KB objects), never a correctness bug, since a present marker only means "worth checking
the remote," not "the remote is missing the data."

**Bounded loss window.** A *process* crash drops nothing important: every acked body is in the
cache and every unfinished upload still has its marker; the new active's reconcile re-uploads from
the cache body — no loss from a process crash alone. True data loss requires the **cache volume**
to be lost (disk failure, volume corruption) while uploads are still pending; the loss set is
`O(in-flight + pending)`, bounded by the pending set, the same invariant as before. The markers
and the bodies they track are lost together with the volume, so there is nothing to enumerate or
re-upload after that, by construction.

**Durability gates GC.** A cache object whose key still has any pending marker is never evicted
(§8) and never tombstoned — the scavenger confirms the remote data object is present (per-victim
HEAD-remote, §8) before deleting the body. This is the invariant that keeps the no-local-redundancy
design safe: a body only leaves local storage once its ciphertext is provably on the remote.

## 8. Tiering / GC — the scavenger task

GC and pending-marker reconcile (§7) share one **background task inside the active**, gated on
holding the active claim — the passive never scavenges. The eviction path operates only on
**single-part cache-resident objects** — multipart composites never reach the cache through the write
path (§7); they enter only via the lazy rehydrate-on-GET described below, after which they are
ordinary cache bodies and follow the same eviction rules. Under the single-active-writer model (§4)
this is what keeps eviction from becoming a second writer: the scavenger operates only on the
cache the active is allowed to reach, and uses **cache-side conditional operations** plus a per-key
**in-flight-PUT ref count** — not an in-process lock on the PUT path — to fence against a writer
landing on K mid-eviction. No cross-process coordination, no internal RPC, no re-resolving
a writer across failover. On promotion the new active starts the scavenger; on demotion/shutdown it
stops. Serving holds **no persistent eviction state** — no LRU index, nothing lost on restart.

**Write-awareness on the eviction path.** The PUT path's only in-process state is a per-key
`Arc<AtomicUsize>` ref count of in-flight PUT handlers, kept in a swept `DashMap` (the same
data structure the original §4 lock table used, but as a counter — not a mutex — and only consulted
by eviction). The PUT handler: `inc` on enter → cache `PutObject` at K (conditional, the
linearization point) → cache `PutObject` at `<marker-uuid-prefix>/<K>/<upload-id>` (the marker) →
`dec` on exit. The `inc ⇒ dec` window therefore covers the otherwise-racy gap between
body-write-confirmed and marker-write-confirmed, which is exactly the window eviction can't
otherwise observe. Eviction skips any key with `count > 0`, so it can never pick up a body whose
marker is still in flight. The ref count gates only eviction; PUTs themselves never block on it
(different keys still parallelize; same-key writers serialize via the cache-side conditional PUT,
the §4 linearization point — not via this counter). Eviction re-checks the pending-LIST late in its
pipeline (below) to also catch markers from a prior hypha generation that wouldn't show up in the
in-process ref count.

**No global LRU index; windowed CLOCK by partial scan.** Eviction need not be exact, so no
per-object access time is ever written — the read path stays write-free. Recency is a **Bloom-ring
sketch** in the active: one Bloom filter per time slice, GET/HEAD insert the key, and the newest
k slices together answer "accessed recently?". Sealed slices persist to the cache under the
reserved prefix (`<marker-uuid-prefix>/recency/<slice>`), are loaded to warm the ring on
promotion, and are retained k deep (older ones swept). The sketch is advisory, same class as the
prefix hint: a Bloom false positive only spares an object a cycle, and a fully lost or corrupt
ring — or first boot — falls back to **LastModified ordering alone**, costing one churnier GC
cycle, never correctness. Each pass, the scavenger advances a **rotating cursor** over a
contiguous keyspace slice (`LIST` window), orders candidates by LastModified (free in every LIST
entry), skips any key the ring has seen recently, and evicts the oldest *within the window*. A
hot-but-old object that does get evicted simply rehydrates on its next read with a fresh mtime,
protecting it for the next cycle — rehydration is the second-chance bit, so this is CLOCK with
the ring suppressing the churn. Eviction of a candidate K runs in four steps; the layering
is what makes eviction auto-healing under concurrent writers on K:

1. **Skip if writer in flight.** Read the in-flight-PUT ref count for K; if `> 0`, skip K for this
   pass. This covers the body-confirmed-but-marker-not-yet-written window that no purely
   cache-side observation could otherwise catch.
2. **Skip if pending.** `ListObjectsV2` the cache with `prefix=<marker-uuid-prefix>/<K>/` — if any
   marker is present, skip K. This catches markers from a previous hypha generation (e.g., a
   failover where the
   writer died between body and marker write and the in-flight counter died with it). Same LIST
   serves §7 reconcile.
3. **Confirm remote.** `HEAD` the remote at K. If absent, K isn't durable yet — skip (§7's
   durability-gates-GC invariant; rare given step 2 typically catches this first, but covers a
   marker-delete racing an eviction pass).
4. **Twin, then conditional tombstone overwrite.** Delete any stale facts twins for K, write the
   fresh twin (§9: facts in the key, bound to the sentinel ETag), then cache `PutObject` at K with
   `If-Match: E_v`, body the **eviction-sentinel UUID** (§4), user-metadata recording the
   client-visible ETag, plaintext length, and original mtime; no part table (recoverable from the
   remote's per-part metadata). Twin-before-tombstone means a sentinel always has its twin; a
   crash in between leaves only a twin bound to the sentinel next to a live body — unbound,
   HEAD-fallback, swept (§9). The tombstone write itself is an **atomic replace** of the body at
   the same key, not a separate object: a GET racing the eviction sees either the body or the
   tombstone, never a 404, by S3's single-object atomicity; no separate "write tombstone then
   delete body" sequence is needed.

The reverse path — rehydrate — is conditional the same way (`If-Match: <eviction-sentinel-etag>`),
so eviction and rehydrate can never clobber a client write in either direction. The sentinel ETag
is constant across tombstone generations, so rehydrate also holds the in-flight ref count while it
runs — the scavenger skips the key, closing the evict → rehydrate → re-evict ABA that a
constant-ETag CAS alone cannot see. Rehydrate also settles the twin: a simple body deletes it
after landing (unbound meanwhile — HEAD fallback, never wrong), a composite **rebinds** it to the
new body's cache ETag after landing, since a cached composite keeps needing its ETag override
(§9). Writers overwriting a restored key delete the twin the same way, after the body lands.

Two race windows remain by construction, both auto-healing — neither aborts durability:

- **Writer lands body between steps 1 and 4.** Step 1 had `count = 0` (writer not yet entered) or
  `count > 0` (skipped, retry next pass). The dangerous sub-case is `count = 0` at step 1 because
  the writer enters after that read: the body lands, marker write is in flight, eviction's step 4
  does a conditional tombstone `PutObject If-Match: E_v` against the *pre-writer* `E_v`. The
  writer's conditional `PutObject If-Match: <client's prev-etag>` has flipped the etag, so the
  eviction's `If-Match: E_v` fails → eviction aborts for this pass, retries next pass when the
  writer has finished (`count = 0` again) and the new marker is visible at step 2. No loss.
- **Writer lands body between steps 3 (HEAD-etag sample) and 4 (conditional tombstone).** Same as
  above — the writer's conditional PUT has moved the etag, the eviction's tombstone
  `If-Match: E_v` fails. Eviction retries next pass. No loss.

Repeat until the slice has freed its share toward the low-water mark. Successive passes cover the
whole namespace: exact-LRU inside each window, approximate-LRU globally, with no index. Evicting a
still-warm object costs only a rehydrating cache miss, never data. The same sweep reclaims
orphaned age files from aborted multipart uploads.

**Usage measured from the backend.** Per-object byte accounting scattered across the data path is
fragile, so hypha keeps no `internal` usage counter. The scavenger reads **SeaweedFS's volume/master
status/metrics** directly (per-volume sizes, deleted bytes, disk free) for a global, physically-accurate
figure, scavenges while above the high-water mark until the low-water mark, and can drive
`volume.vacuum` to reclaim dead bytes. A different cache backend supplies its own measurement in
`gc/usage.rs`.

## 9. S3 semantics to get exactly right

- **ETag over plaintext.** Clients must see stable, S3-correct ETags computed on the *plaintext* they
  sent, independent of framing/encryption. Single-part = MD5 of the object; multipart = the composite
  `MD5(concat of part MD5s)-N`. Per-part plaintext MD5s are carried as `x-amz-meta-pmd5` on each remote
  part object; `CompleteMultipartUpload` reads them and composes the ETag (§6).
- **Range GET** maps to a computed chunk range plus the age header read (§6), then drives the age
  `StreamReader` with `seek` to the plaintext offset — the crate handles per-chunk decrypt-authenticate — and trims
  to the exact requested `[a,b)` before emitting.
- **Buckets map one-to-one** across client ⇄ cache ⇄ remote. `CreateBucket` is synchronous
  write-through: create on the cache, then the remote, ack only after both succeed (bucket ops are
  rare control-plane events — no marker machinery). `DeleteBucket` mirrors it in reverse order
  (remote first, so a crash between leaves a retryable still-visible bucket, never a remote orphan
  for the §10 sweep to resurrect). Nothing else touches the remote, so `ListBuckets` — like all
  namespace reads — is served from the cache alone.
- **HEAD/LIST** are served from the cache while the **sync marker** is present (§10) — the cache is
  then namespace-complete (every remote key has a cache body or tombstone), so an absent key is
  authoritatively a 404. While the marker is absent, the **remote is the source of truth** for
  GET/HEAD/LIST until reconciliation completes. The client-visible ETag comes from user-metadata
  where it differs from the cache's own (§4). LIST filters hypha's internals from responses — the
  marker prefix and delete-tombstoned keys — classifying each entry from its (size, ETag) sentinel
  pair alone (§4), no per-key HEAD.
- **LIST entries must report plaintext sizes, client ETags, and original mtimes**, and a raw
  `ListObjectsV2` entry has the wrong facts for tombstoned keys (sentinel size/ETag), rehydrated
  composites (cache MD5, not `hash-N`), and remote objects (ciphertext length; `plen` is not
  derivable from a LIST entry — the §6 `hlen` derivation needs `plen` as input). Cache LISTs
  solve this with **facts twins**: a zero-byte cache object at `K ‖ 0x01 ‖ facts`, the facts
  (type, bound ETag, `plen`, client ETag, mtime) encoded in the twin's *key name* — the one
  per-entry field LIST does return. Because the separator sorts below every admissible key byte,
  a twin sorts immediately after its own key: one backend page yields `(K, K's twin)` adjacent,
  and a single pass emits correct client entries with **zero extra requests**. A twin applies
  only when its **bound ETag** equals the adjacent `K` entry's actual ETag (the sentinel ETag for
  tombstones, the body's cache ETag for composites); an unbound twin — a crash-window leftover —
  triggers a per-key cache `HEAD` fallback (the tombstone/object user-metadata stays the
  authoritative copy of the facts; the twin is only their LIST-visible projection) and is swept.
  Delete-tombstones need no twin — LIST omits them, classified from the sentinel pair alone.
  Against a plain S3 endpoint — the cacheless deployment's remote, or the remote during the
  resync window — no projection exists: LIST pages fan out per-entry `HEAD`s under a concurrency
  bound, and during resync the page is additionally merged with the active's in-memory **pending
  overlay** (acked-but-unuploaded PUTs patched in, pending DELETEs dropped; rebuilt from the
  marker LIST on promotion) so read-after-write holds while the cache is untrusted.
- **Multipart** takes the §7 cacheless path: each `UploadPart` is encrypted as its own age file
  (fresh random file key per part) and streamed straight to the remote, with that part's plaintext
  length and MD5 stamped onto the part object's user-metadata. The cache is bypassed entirely on the
  write path — a part isn't readable until the composite commits anyway, and a multipart's size is
  throughput-bound, so the cache's latency win doesn't apply. `CompleteMultipartUpload` `ListParts`s
  the remote upload, composes the ETag from the per-part MD5s, and writes the small composite part
  table (or, on overflow, only ETag+count) onto the composite object's user-metadata (§6).
  Out-of-order, parallel, and re-uploaded parts are inherent-safe because each part is its own age
  file with a fresh file key — no cross-part coordination. A client `UploadPart` whose plaintext
  exceeds **4 GiB** is rejected at admission (one line below the S3 5 GiB cap) so the age envelope
  overhead never pushes the framed part past the remote's part-size limit; transparent re-splitting
  at 4 GiB is a later refinement. `AbortMultipartUpload` deletes the orphaned parts; the §8 scavenger
  also reclaims any orphans from aborted/abandoned uploads. A completed composite enters the cache
  lazily — on first GET, hypha fetches and decrypts from the remote, then asynchronously populates
  the cache body from the decrypted plaintext, the same rehydrate path used for tombstoned bodies;
  thereafter the composite is an ordinary cache body subject to §8 LRU eviction.
- **DELETE** is write-through via the same marker machinery as PUT: overwrite K with a
  **delete-tombstone** (body the delete-sentinel UUID, §4; GET answers 404, LIST omits it) and
  write a pending marker; reconcile propagates the remote `DeleteObject`, then clears the marker
  and the tombstone with `If-Match: <delete-sentinel-etag>`. Without the mask,
  a crash after a local-only delete would let the next GET resurrect the object from the remote.
  Recovery from deletion stays a property of the remote bucket (versioning/object-lock), per the
  architecture.

## 10. Startup, shutdown, failure

- **Stateless startup + sync marker.** No LRU index to warm, no process-local state — a replica
  starts serving immediately, which is what makes the pre-warmed passive's promotion instant.
  Whether the cache namespace is trustworthy is recorded **in the cache itself**: a reserved
  **sync marker** object (a distinguished key under the marker prefix), present iff a namespace
  reconciliation has completed — it dies with the cache volume by construction, so no external
  state can go stale about the cache's health. On claim acquisition the active checks it.
  **Present** → cache-authoritative reads (§9), no sweep — the steady-state path for every restart
  and failover. **Absent** → the remote becomes the source of truth for GET/HEAD/LIST, and the
  active runs a **full namespace reconciliation**: LIST all buckets and keys on remote and cache,
  recreate any bucket missing from the cache, write an eviction tombstone for any remote key with
  no cache entry (body or tombstone — either counts as present, so pending deletes are not
  resurrected while their tombstone survives), then write the sync marker and flip reads back to
  the cache. The sweep is **parallel**: a LIST page chain is inherently serial (continuation
  tokens), so throughput scales by sharding the keyspace. Shard boundaries come from a **prefix
  distribution hint** — a small file the active periodically writes to the remote at a reserved key
  under the marker-UUID prefix (so LIST filtering already excludes it), holding approximate key
  counts per prefix; the §8 walk cursor is already listing keyspace windows, so maintaining the
  histogram is free. The hint is advisory, not an index: it carries no per-object facts, every key
  is still listed, and a stale or missing hint costs only shard balance — restore then falls back
  to `delimiter=/` discovery (recursing until shards exceed the concurrency budget) with
  `start-after` range splits inside any giant flat prefix. Shards feed a pooled tombstone-writer
  over the cache (tombstones are sentinel-sized, so the cache side is bounded by request rate, not
  bytes). No off-the-shelf crate implements the fan-out (aws-samples' `s3-fast-list` is a CLI
  wanting pre-segmented prefixes — its input file is exactly this hint), so `restore.rs` hand-rolls
  it over the SDK paginator + a semaphore. Parallelism doesn't disturb idempotence: the sweep only
  fills gaps, and the marker is written only after every shard drains.

  Serving is never gated; a conditional write to K during resync first materializes K's
  remote state into the cache (a tombstone, if K exists remotely with no cache entry), then runs
  the normal §4 path — the cache-ETag linearization point holds per key even mid-sweep. Pending
  markers lost with a wiped cache are lost outright: un-uploaded writes are the bounded loss
  window, and a pending-but-unpropagated DELETE is resurrected by the sweep (the remote still
  holds the object) — the same cache-volume-loss window §7 already prices in.
- **Graceful drain.** On `SIGTERM`, the active stops accepting new requests, releases its active claim
  so `hypha-fence` promotes the passive cleanly (sub-second, no fence needed), then best-effort runs
  one more reconcile pass before exit to *shrink* the pending set (markers + bodies stay in the cache
  either way; the new active's sweep continues from there). Kubernetes `terminationGracePeriod` and
  a `preStop` should allow for it.
- **Remote unavailable** → hot reads unaffected; tombstoned reads fail cleanly; new conditional PUTs
  still ack (cache confirmed), markers accumulate, reconcile retries uploads once the remote returns.
  **Cache/body loss** → served transparently from the remote; a lost local body is indistinguishable
  from a tombstone at read time.

## 11. Configuration & deployment

Config via `figment` (file + env), validated at boot. Surface (maps to chart values / Secrets):

- **remote**: endpoint, region, bucket, credentials (Secret), **key prefix** (namespacing per §
  *Caching is optional*).
- **cache** (optional): endpoint, bucket, credentials — omit for a cacheless deployment.
- **master key**: 32 bytes from a Secret, `zeroize`d in memory; never logged.
- **hypha's own credentials**: the access-key/secret its clients authenticate with (`S3Auth`).
- **fencing**: the per-pod identity selectors `hypha-fence` flips the allow between, the lease/renew timings, the
  fence-confirm timeout, and the post-fence **settle delay** + server-side request timeout that bound
  the in-flight-write drain (§4).
- **serving tuning**: offload threshold (§5), **reconcile pass interval/concurrency**,
  max in-flight conditional PUTs to the cache, remote-LIST `HEAD` fan-out concurrency (§9).
- **GC tuning**: high/low water marks, walk window size, scavenge interval; recency ring (slice
  duration, slice count k, filter size) (§8); restore-sweep fan-out (max concurrent LIST shards,
  tombstone-writer concurrency) and prefix-hint refresh interval (§10).

Delivered as the `hypha/` chart described in the architecture, installed by cluster-admin. It renders
**two workloads** sharing config/Secret refs: the serving **StatefulSet** (2 pods — active +
pre-warmed passive, GC inside the active; a StatefulSet so the automatic
`statefulset.kubernetes.io/pod-name` label gives each pod the distinct Cilium identity the §4
fence selects on) + `Service` + `HTTPRoute`, and the `hypha-fence`
controller (2 replicas, leader-elected; RBAC to write `CiliumNetworkPolicy` and the OPNsense allow, and
to read Cilium endpoint policy revisions for fence confirmation). The active-identity fence is the
**write-path** narrowing of the default-deny SeaweedFS ingress grant; that grant and the rest of the
network topology stay owned by the `seaweedfs`/`cilium` network CRDs per the repo networking convention
— `hypha-fence` only flips which identity the existing grant admits.

## 12. Observability

`tracing` spans per request (op, key, part, bytes, cache-hit/miss); structured JSON in-cluster.
`metrics` → Prometheus: request rate/latency by op, cache hit ratio, **pending-marker set size and
reconcile pass duration**, remote-upload latency and retries, active/passive role + failover count +
fence-confirm latency (from `hypha-fence`), and — from the active's GC task — scavenge throughput,
evictions/bytes reclaimed, and measured cache usage vs. water marks. A `/healthz` (liveness) and
`/readyz` (remote reachable) endpoint for probes; the active/passive role is a separate reported
condition, not a readiness gate (the passive is intentionally ready-but-idle).

## 13. Testing strategy

- **`hypha-format`**: `proptest` round-trips of the age envelope adapters (encrypt→decrypt identity;
  corrupt/truncate/reorder/cross-object-splice → authentication failure, exercising age's file-key
  separation, chunk-index-in-nonce, and finalizer-chunk guarantees via hypha's own wrapper, not by
  re-testing age itself); offset-arithmetic proptest (plaintext ⇄ ciphertext byte math against the
  fixed 64 KiB chunk size); a `cargo fuzz` decode target for `StreamReader::seek` on adversarial
  bytestreams; and `criterion` benches to set the §5 offload threshold empirically.
- **Semantics**: ETag reproduction, offset arithmetic, composite part table inline vs. `ListParts`
  fallback.
- **Concurrency**: hammer concurrent CAS against a single active with the cache behind it, and
  assert linearizability of the *cache* (no double-create, no lost update, `If-Match` honored). The
  cache's conditional PUT is the linearization point — this test pins SeaweedFS's `If-Match`
  semantics as the load-bearing piece (§ *Risks*). Cover CAS against tombstoned keys
  (restore-then-compare) and cached composites (metadata-ETag mapping) per §4. A second test exercises bursty overwrite contention
  on one key: many writers ack in some order, then assert the remote converges to the last-acked etag
  within one reconcile pass, with no intermediate ordering visible post-eviction.
- **Twin coherence (LIST facts)**: crash-inject at every point of the twin sequences (§8's
  delete-stale → write-twin → tombstone; rehydrate's body → delete/rebind) and assert LIST never
  reports wrong facts — an unbound twin must trigger the HEAD fallback, at most one twin exists
  per key, and the sweep collects orphans. Separately, LIST under concurrent evict / rehydrate /
  overwrite of the same keys, plus the ordering property: an interleaved population of keys where
  some are prefixes of others (`a`, `a!b`, `a/b`) lists in correct lexicographic order with twins
  present.
- **Eviction vs. concurrent writer**: drive sustained PUTs to one key while the scavenger is
  evicting candidates including K, and assert that the four-step §8 pipeline never deletes a body
  whose marker is in flight or whose etag has already moved. Specifically: (a) a writer landing
  between step 1 (ref-count check) and step 4 (conditional delete) must cause the conditional
  delete's `If-Match` to fail and eviction aborts for the pass — no loss; (b) a writer that
  completes its full `inc → body → marker → dec` cycle while the scavenger is between candidate
  selection and step 1 is caught at step 1 (`count > 0`) and eviction skips — no loss; (c) a
  marker from a prior hypha generation (writer died mid-PUT, ref count lost with the dead process)
  is caught at step 2's late pending-LIST — no loss. Confirm via repeated runs that every
  acked-but-not-yet-remote write is recoverable through the §7 reconcile sweep.
- **Failover / fencing**: a harness that starts two replicas, partitions the active (drop its backend
  path), and asserts `hypha-fence` fences-then-promotes in order — the old active's writes must be
  refused at the backend *before* the new active writes, with no two-writer overlap. Cover the graceful
  (lease-release) fast path too.
- **Pending-marker reconcile**: with the active running, set up the in-flight scenarios the design
  exercises — (a) a marker whose etag matches cache(K) and an absent remote → reconcile uploads the
  cache body and conditionally-deletes the marker; (b) multiple markers for K (older acked, then
  newer acked) and cache(K) at the newer etag → reconcile uploads the newer, drops the older as
  superseded (no LIFO, no per-key lock); (c) a remote that already has the data (re-pass after a
  previous reconcile completed) → reconcile just deletes the dangling marker. Then kill the active
  mid-sweep, promote the passive, and assert the new active's sweep resumes from the marker-prefix
  listing without dropping or double-handling the in-flight set, with no evictions occurring before
  reconcile resolves. A separate test wipes the cache volume while uploads are still pending and
  asserts loss is bounded by the pending set — markers and bodies die together on the cache, so the
  loss set is not enumerable afterward, only bounded in size. Keys whose remote data object *was*
  uploaded remain recoverable via rehydrate from remote.
- **Integration**: `testcontainers` bringing up real SeaweedFS (cache) and MinIO (remote); run an S3
  conformance pass and the specific conditional-write, multipart-out-of-order, range-GET, scavenge/
  rehydrate, and cache-wipe → restore-sweep → rehydrate scenarios end to end.

## 14. Risks & open questions

- **Master-key rotation** (carried from the architecture's open question). The X25519 identity age
  wraps file keys to is one identity; rotation implies re-encrypting the remote. age natively supports
  encrypting to **multiple recipients** in one file, so a rotation path is forward-compatible from day
  one: add the new recipient to the encryptor's recipient set, deploy, then run a (deferred) re-encrypt
  pass over the remote to drop the old recipient from existing files. No key-epoch tag in metadata
  needed — the recipient set in each age file *is* the epoch. Re-encryption job is deferred.
- **Both backends are assumed to speak the full S3 API, conditional writes included.** The cache's
  `If-Match` on `PutObject`/`DeleteObject` is the linearization point for the PUT path (§4) and the
  eviction path (§8); the remote's conditional PUT / `CompleteMultipartUpload` serialize the
  cacheless and multipart paths. An absent or buggy implementation on either side breaks
  linearizability on contended writes. The only SeaweedFS-specific surface left is the
  usage/vacuum API (§8), already pluggable — LIST facts, recency, and every marker ride ordinary
  S3 objects, so the cache stays swappable. SeaweedFS implements conditional PUT/DELETE as of
  **4.07**,
  broken under versioning/object-lock (seaweedfs#8073) — the cache bucket enables neither, so pin
  ≥ 4.07 and let the §13 concurrency test re-verify. If it still cannot be relied on, the fallback
  is a short-hold in-process per-key lock held through the cache-side RMW — same overhead as the
  original §4 design, with all the async-latency caveats that brought.
- **`hypha-fence` is a bespoke controller, and the load-bearing one.** It replaces distributed locking
  with fabric fencing, so its correctness *is* the single-writer guarantee. The ordered
  fence→confirm→drain→promote sequence and the Cilium policy-revision confirmation must be gotten exactly
  right; it deserves the most careful testing (§13) and a spike against real Cilium early.
- **Cilium must reset established connections on the deny.** The in-flight-write drain (§4 step 4)
  depends on the fenced identity's *existing* connections being force-closed, not merely new ones
  blocked — verify this behavior (config/version) in the same early Cilium spike, since without it the
  settle delay would have to cover full in-flight transfer time.
- **Unfenceable partition.** If `hypha-fence` cannot reach the enforcers (SeaweedFS nodes + OPNsense) to
  program *and confirm* the fence, it must **not** promote — fail-safe. This is sound only because the
  flat homelab failure domain means an unreachable-enforcer partition also cuts the old active off from
  the backend (and a near-side passive couldn't serve it either). Document the assumption; it would not
  hold if cache/remote lived in a separate failure domain from the control path.
- **The remote leg of the fence is source-enforced.** Cilium egress applies at the (partitioned)
  source node and OPNsense may see only SNAT'd node IPs, so the fence guarantees isolation from the
  *cache*, not the remote; the exposed window is an in-flight multipart commit from the old active
  (§4). Escalation if it matters: per-replica remote credentials revoked by `hypha-fence` —
  destination-enforced like the ingress deny.
- **`hypha-fence` availability is a liveness, not safety, concern.** It sits off the data path and only
  acts on transitions, so while it is down the running active is unaffected and Cilium/OPNsense keep
  enforcing the existing allow. The only cost is a *delayed failover* if the active dies during that
  window; its absence can never produce two writers. Running it leader-elected (2 replicas) closes that
  window and is safe because the controller is idempotent — controller split-brain reconciles the same
  policy to the same value, unlike a data-plane split-brain.
- **`s3s` coverage** for every conditional-write / SigV4-chunked corner hypha's clients use should be
  spiked early — it's the load-bearing dependency; a gap there is the highest-impact unknown.

## 15. Implementation plan

Ordered so that every phase ends in something independently testable — and from phase 2 on,
independently deployable — with the hardest machinery (cache coherence, fencing) landing last, on
top of already-proven layers. The former §14 unknowns resolved by research: SeaweedFS implements
conditional PUT/DELETE as of **4.07** (broken only under versioning/object-lock, which the cache
bucket doesn't enable — pin ≥ 4.07; the §13 integration suite re-verifies), and `s3s` surfaces the
precondition headers through its DTOs (strict ETag-quoting is the known sharp edge). Cilium's
established-connection behavior on deny stays a phase-6 deploy-time check, with the settle delay
as fallback.

**Phase 1 — `hypha-format`.** Pure codec: envelope, offset arithmetic, seekable-reader adapter.
*Exit*: §13 proptest/fuzz suite green; `criterion` bench sets the §5 offload threshold.

**Phase 2 — cacheless serving MVP.** `hypha-core` (config, backend, meta, error) + the `s3s` trait
over the cacheless path only: inline-encrypt PUT, GET/range via the seek adapter, HEAD/LIST/buckets
from the remote, DELETE, `S3Auth`. This is the `s3-direct` deployment in full — a shippable
encrypting proxy that proves s3s + age + SDK end-to-end while deferring every cache mechanism.
*Exit*: integration conformance pass against MinIO; a real zero-loss client (ZeroFS) works.

**Phase 3 — multipart.** Per-part age files, part-object metadata, `CompleteMultipartUpload`
composite table/ETag, abort, the 4 GiB admission cap. Extends phase 2 only. *Exit*: §13 multipart
scenarios (out-of-order, re-upload, `ListParts` overflow fallback).

**Phase 4 — cached path, single replica.** Conditional pass-through with the §4 ETag rules
(version-token mapping, restore-then-compare), pending markers, the reconcile sweep, delete
tombstones, CMU cache tombstone, rehydrate. Deployed with one replica and **no fencing** — a single
writer is trivially single, so this ships the default `s3.internal` deployment with correctness
intact and only failover seamlessness missing. *Exit*: §13 concurrency, reconcile, and
eviction-vs-writer suites against real SeaweedFS.

**Phase 5 — GC + restore.** Walk cursor, windowed CLOCK, recency ring + slice persistence, usage source +
vacuum, prefix-hint writer, sync marker + parallel restore sweep. *Exit*: scavenge/rehydrate and
cache-wipe → restore-sweep → rehydrate scenarios.

**Phase 6 — `hypha-fence` + active-passive.** Two-pod StatefulSet, leader-elected controller,
lease, fence→confirm→drain→promote, graceful-release fast path. Its first step verifies the fence
primitives on the live cluster — per-endpoint policy-revision observability and
established-connection reset on deny — before the controller logic lands. *Exit*: the §13
partition harness — old active's writes refused at the backend before the new active writes, plus
the graceful path.

**Phase 7 — chart + operations.** The `hypha/` Helm chart (both workloads, Secrets, `HTTPRoute`,
the fence's policy RBAC, per repo networking conventions), dashboards for the §12 metrics, then the
two production installs (cached + cacheless). *Exit*: both endpoints live behind the shared
Gateway.
