# Hypha — implementation proposal

Companion to [`ARCHITECTURE.md`](./ARCHITECTURE.md), which owns the *what* and *why* (the two-tier
caching model, framed AEAD, write-through durability, tiering/GC). This document proposes the *how*:
language runtime, crate selection, module boundaries, the threading and concurrency model, and the
implementation concerns that determine whether the design's guarantees (linearizable conditional
writes, sound per-part encryption, bounded loss window) actually hold in code.

Nothing here changes the architecture; it commits to mechanisms the architecture leaves open.

## 1. Language, runtime, workspace

Rust, edition 2021, async on **Tokio** (`rt-multi-thread`). The workload is I/O-bound proxying with a
CPU-bound AEAD step that is fast enough (multi-GB/s per core for ChaCha20-Poly1305) to stay inline for
normal frame sizes; §5 covers the offload threshold.

Split into a **Cargo workspace** — two binaries over a shared core, plus the isolated codec:

- **`hypha-format`** — the framed-ciphertext codec (frame layout, AEAD, AAD binding, streaming
  encrypt/decrypt, offset arithmetic). No S3, no Tokio, no I/O. Pure functions and `Stream`/`Sink`
  adapters over `Bytes`. This is the security-critical core; keeping it standalone lets it carry a
  `cargo fuzz` target and `proptest` suite without spinning up a server.
- **`hypha-core`** — the shared library the binaries link: backend S3 client wrapper, object/tombstone
  metadata model, the in-process lock table, and config.
- **`hypha`** — the **serving** binary: the S3 protocol surface (`s3s`), the data path, conditional-write
  serialization, write-through replication, telemetry, and — as a background task that runs only while
  this replica holds the active claim — the GC scavenger (§8). Runs **active-passive** (one active
  writer, one pre-warmed passive standby; §4). Folding GC in keeps a single writer with one in-process
  lock table and no cross-process eviction coordination.
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
| AEAD | `chacha20poly1305` (RustCrypto, `XChaCha20Poly1305`), `getrandom` for nonces |
| Key hygiene | `zeroize` (wipe the master key on drop) |
| Bytes / streaming | `bytes`, `futures`, `tokio-util` (codec, `io`) |
| Concurrency | `dashmap` (sharded maps), `tokio::sync` (Mutex/Semaphore/mpsc/watch) |
| Config | `serde`, `figment` (file + env layering) |
| Errors | `thiserror` (library layers), `anyhow` (bootstrap only) |
| Observability | `tracing`, `tracing-subscriber`, `metrics` + `metrics-exporter-prometheus` |
| Testing | `proptest`, `criterion`, `testcontainers` (real SeaweedFS/MinIO in integration tests) |

RustCrypto over `ring`/`aws-lc-rs` for the AEAD specifically because the design uses a **custom frame
format**, not a standard streaming-AEAD construction — the low-level `XChaCha20Poly1305: AeadInPlace`
API is exactly the primitive we want to drive per-frame, and its 192-bit nonce is the crux the
architecture relies on.

## 3. Module layout

```
hypha-format/            (the fuzzable codec)
  frame.rs               frame layout, encode/decode one frame, AAD construction
  stream.rs              plaintext↔ciphertext Stream/Sink adapters over Bytes
  offset.rs              plaintext-byte ⇄ ciphertext-byte arithmetic (see §6)

hypha-core/src/          (shared by the binaries)
  config.rs              typed config + validation (fail fast on bad values)
  backend.rs             ObjectStore abstraction over an aws-sdk-s3 client
  meta.rs                object metadata + tombstone model, (de)serialization
  manifest.rs            part table: per-part headers + composite-object metadata (§6)
  locks.rs               in-process per-key lock table for conditional writes (§4)
  error.rs               error → s3s::S3Error mapping

hypha/src/               (serving binary — active-passive)
  main.rs                config load, runtime, s3s server, signal handling, drain
  auth.rs                S3Auth impl for hypha's own client credentials
  s3/                    the s3s::S3 trait implementation, split by op group
    get.rs put.rs multipart.rs list_head.rs delete.rs conditional.rs
  replication.rs         write-through queue, durability state (§7)
  gc/                    scavenger task, runs only while active (§8)
    walk.rs              partial-scan cursor + windowed LRU eviction
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

With a single writer, conditional writes need **no distributed lock**: the active serializes CAS with
an **in-process** per-key lock table (a sharded `DashMap<ObjectKey, tokio::sync::Mutex>`), so the RMW —
read current metadata → check `If-None-Match: *` / `If-Match: <etag>` → commit — runs locally at zero
coordination latency. Authoritative existence/ETag stay in the self-describing backend objects; there
is no metadata database (the architecture's "no side index" holds in full).

The correctness of "single writer" cannot rest on *observing* that the old active is dead — a remote
observer can never distinguish dead from partitioned-but-still-writing. So it rests on **fabric
fencing**: the network path to the backend is the authority for who may write.

### The allow-policy *is* the lease

Rather than keep a `Lease` object and a fence policy in sync (two sources of truth that can diverge),
collapse them into one invariant, maintained by a small **`hypha-fence` controller**:

> Exactly one hypha identity is in the SeaweedFS ingress allow-list and the OPNsense egress allow to the
> remote — and that identity *is* the active.

"Who holds the lease" and "who can write" become the same fact. Two pods may each *believe* they are
active (a partitioned old active still holds its in-process locks), but **belief is free — only the
network-allowed pod can write**, so the writer set is always ≤ 1. The old active's stale lock state is
harmless because its packets to the backend are dropped.

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

Reads/HEAD/LIST take no lock either way. Since serving is active-passive, the passive does not serve;
during the failover gap the surface is briefly write-unavailable, not degraded.

**Request lifecycle.** One Tokio task per request (Tokio/hyper default). Bodies never fully buffer:
they flow as `Stream<Item = Bytes>` through the encrypt/decrypt adapters into/out of the SDK's
`ByteStream`, so per-request memory is bounded by a few frames regardless of object size. A global
`Semaphore` caps in-flight request concurrency to bound total memory and backend connection pressure.

## 5. Threading & the AEAD CPU step

ChaCha20-Poly1305 runs at multi-GB/s/core, so at the proposed 1 MiB frame size a frame encrypts in
tens of microseconds — short enough to run **inline on the async worker** without starving the
runtime. To keep any single `poll` bounded we (a) fix a moderate frame size and (b) offload to
`tokio::task::spawn_blocking` only when a single contiguous encrypt/decrypt would exceed a threshold
(configurable, default ~4 MiB of pending plaintext). This avoids a blanket `rayon` pool while
protecting tail latency under large sequential transfers. The choice is measured, not assumed: a
`criterion` bench in `hypha-format` sets the frame size and the offload threshold empirically.

## 6. Frame format, offsets, and the manifest

`ARCHITECTURE.md` defines the frame; the implementation adds one decision that simplifies everything
downstream: **a fixed plaintext frame size `F`** (per object, recorded once).

With fixed `F`, every ciphertext frame is exactly `F + 44` bytes (`4` len + `24` nonce + `16` tag),
except a possibly-short final frame per part. That makes offset math **closed-form** instead of a
per-frame lookup table:

- Ciphertext offset of frame *i* = `i * (F + 44)`.
- A ranged GET for plaintext bytes `[a, b)` covers frames `⌊a/F⌋ .. ⌊b/F⌋`, which maps to a single
  contiguous **byte-range GET** on the remote — no manifest consultation for a single-part object.

This collapses the "frame manifest" from a per-frame offset table into, at most, **one plaintext
length per part** (parts may have unequal sizes, so cumulative part offsets still need the part
table; frames *within* a part are arithmetic). Consequences:

- **Single-part PUT (the common case): no part table at all** — total length + `F` is sufficient.
- **Multipart: the part table is distributed across the parts themselves.** Each remote part object
  already exists and already carries S3 user-metadata, so hypha records each part's **plaintext
  length** and **plaintext MD5** (`x-amz-meta-plen`, `x-amz-meta-pmd5`) on the part object that holds
  that part's ciphertext frames. There is no separate manifest artifact — the per-part facts live on
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

The AAD binds `object_uuid ‖ part_number ‖ frame_index_in_part ‖ flags`, so per-part independent
encryption stays sound (a stable per-object UUID is assigned at first write and stored in metadata).

## 7. Write-through replication & durability tracking

Two code paths, selected by whether a cache is configured:

**Cached (default).** PUT streams plaintext to the cache, computes the ETag on the way through, writes
metadata with `durable = false`, and **acks the client**. A background worker then reads the object
back from the (fast, local) cache, frame-encrypts, uploads to the remote, and flips `durable = true`.
Reading back from cache avoids a `tee` of the request body into two sinks — simpler, and NVMe read
cost is acceptable; a zero-copy `tokio::io::duplex` tee is a later optimization, not launch scope.

**Cacheless.** PUT frame-encrypts and uploads to the remote **inline**, acking only after the remote
confirms. No queue, no loss window, higher per-op latency — exactly the zero-loss profile the
architecture reserves for clients like ZeroFS.

**The replication queue (`replication.rs`).** A bounded `mpsc` of work items feeding a pool of upload
workers. Bounding it is what makes the "bounded loss window" real and gives backpressure: when the
queue is full, new writes **block on enqueue** (degrading gracefully toward synchronous) rather than
growing memory unboundedly. Uploads retry with exponential backoff; the queue is in-memory by design
— a crash loses exactly the not-yet-uploaded set, which is the accepted, bounded window.

**Durability gates GC.** An object with `durable = false` is never evicted (§8) and never tombstoned
— eviction first *confirms* the remote copy. This is the invariant that keeps the no-local-redundancy
design safe: a body only leaves local storage once its ciphertext is provably on the remote.

## 8. Tiering / GC — the scavenger task

GC runs as a **background task inside the active** (gated on holding the active claim — the passive
never scavenges). Under the single-active-writer model (§4) this is what keeps eviction from becoming a
second writer: it reuses the active's **in-process key lock** for the same read-modify-write the data
path uses, with no cross-process coordination, no internal RPC, and no re-resolving a writer across
failover. On promotion the new active starts its scavenger; on demotion/shutdown it stops. Serving
holds **no persistent eviction state** — no LRU index, nothing lost on restart.

**No global LRU index; windowed LRU by partial scan.** Eviction need not be exact, so the scavenger
keeps no in-memory recency index — nothing to rebuild or lose on restart. Each cache object carries a
**last-access timestamp** stamped by the active on GET/HEAD, but **coarsely**: only rewritten once it
has aged past the LRU granularity, so a hot object costs at most ~one metadata touch per granularity
window rather than one write per read. On the SeaweedFS cache this is a cheap filer attribute update,
not a full object rewrite. Each pass, the scavenger advances a **rotating cursor** over a contiguous
keyspace slice (`LIST` window), reads the access times in that slice, and evicts the least-recently-used
*within the window* — for each `durable` victim, under the key lock: re-confirm the remote copy (§7
invariant), delete the body, leave a tombstone — until it has freed the slice's share toward the
low-water mark. Successive passes cover the whole namespace: exact-LRU inside
each window, approximate-LRU globally, with no index. Evicting a still-warm object costs only a
rehydrating cache miss, never data. The same sweep reclaims orphaned frames from aborted multipart
uploads.

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
- **Range GET** maps to a computed remote byte-range GET (§6), decrypt-authenticate the covering
  frames, then trim to the exact requested `[a,b)` before emitting.
- **HEAD/LIST** served from cache metadata when warm, else the remote (same keys/metadata, plaintext
  there), correct whether a body is local or tombstoned.
- **Multipart** encrypts each `UploadPart` independently into whole frames streamed straight to the
  remote, with that part's plaintext length and MD5 stamped onto the part object's user-metadata.
  `CompleteMultipartUpload` `ListParts`s the remote upload, composes the ETag from the per-part MD5s,
  and writes the small composite part table (or, on overflow, only ETag+count) onto the composite
  object's metadata (§6). Out-of-order, parallel, and re-uploaded parts are inherent-safe because each
  part is freshly random-nonced.
- **DELETE** removes locally and enqueues the remote delete (write-through, so it *propagates* — the
  versioning/object-lock recovery story is a property of the remote bucket, per the architecture).

## 10. Startup, shutdown, failure

- **Stateless startup.** No rebuild, no filer replication, no metadata restore, no LRU index to warm —
  a replica starts serving immediately and LIST/GET fall through to the remote and rewarm the cache.
  This is what makes the pre-warmed passive's promotion instant.
- **Graceful drain.** On `SIGTERM`, the active stops accepting new requests, releases its active claim
  so `hypha-fence` promotes the passive cleanly (sub-second, no fence needed), then best-effort drains
  the replication queue before exit to *shrink* the loss window. Kubernetes `terminationGracePeriod`
  and a `preStop` should allow for it.
- **Remote unavailable** → hot reads unaffected; tombstoned reads fail cleanly; uploads queue/retry.
  **Cache/body loss** → served transparently from the remote; a lost local body is indistinguishable
  from a tombstone at read time.

## 11. Configuration & deployment

Config via `figment` (file + env), validated at boot. Surface (maps to chart values / Secrets):

- **remote**: endpoint, region, bucket, credentials (Secret), **key prefix** (namespacing per §
  *Caching is optional*).
- **cache** (optional): endpoint, bucket, credentials — omit for a cacheless deployment.
- **master key**: 32 bytes from a Secret, `zeroize`d in memory; never logged.
- **hypha's own credentials**: the access-key/secret its clients authenticate with (`S3Auth`).
- **fencing**: the active-identity label/selector `hypha-fence` toggles, the lease/renew timings, the
  fence-confirm timeout, and the post-fence **settle delay** + server-side request timeout that bound
  the in-flight-write drain (§4).
- **serving tuning**: frame size `F`, offload threshold, replication queue depth + worker count, max
  in-flight concurrency.
- **GC tuning**: high/low water marks, walk window size, scavenge interval.

Delivered as the `hypha/` chart described in the architecture, installed by cluster-admin. It renders
**two workloads** sharing config/Secret refs: the serving `Deployment` (`replicas: 2` — active +
pre-warmed passive, GC running inside the active) + `Service` + `HTTPRoute`, and the `hypha-fence`
controller (2 replicas, leader-elected; RBAC to write `CiliumNetworkPolicy` and the OPNsense allow, and
to read Cilium endpoint policy revisions for fence confirmation). The active-identity fence is the
**write-path** narrowing of the default-deny SeaweedFS ingress grant; that grant and the rest of the
network topology stay owned by the `seaweedfs`/`cilium` network CRDs per the repo networking convention
— `hypha-fence` only flips which identity the existing grant admits.

## 12. Observability

`tracing` spans per request (op, key, part, bytes, cache-hit/miss, durable-latency); structured JSON
in-cluster. `metrics` → Prometheus: request rate/latency by op, cache hit ratio, replication queue
depth and lag, upload retries, active/passive role + failover count + fence-confirm latency (from
`hypha-fence`), and — from the active's GC task — scavenge throughput, evictions/bytes reclaimed, and
measured cache usage vs. water marks. A `/healthz` (liveness) and `/readyz` (remote reachable) endpoint
for probes; the active/passive role is a separate reported condition, not a readiness gate (the passive
is intentionally ready-but-idle).

## 13. Testing strategy

- **`hypha-format`**: `proptest` round-trips (encrypt→decrypt identity; corrupt/truncate/reorder/
  cross-object-splice → authentication failure), a `cargo fuzz` decode target, and `criterion` benches
  to fix `F` and the offload threshold.
- **Semantics**: ETag reproduction, offset arithmetic, composite part table inline vs. `ListParts`
  fallback.
- **Concurrency**: hammer concurrent CAS against a single active and assert linearizability (no
  double-create, no lost update) — trivially correct with one in-process lock table, but worth locking
  in as a regression guard.
- **Failover / fencing**: a harness that starts two replicas, partitions the active (drop its backend
  path), and asserts `hypha-fence` fences-then-promotes in order — the old active's writes must be
  refused at the backend *before* the new active writes, with no two-writer overlap. Cover the graceful
  (lease-release) fast path too.
- **Integration**: `testcontainers` bringing up real SeaweedFS (cache) and MinIO (remote); run an S3
  conformance pass and the specific conditional-write, multipart-out-of-order, range-GET, scavenge/
  rehydrate, and cache-wipe-then-rewarm scenarios end to end.

## 14. Risks & open questions

- **Master-key rotation** (carried from the architecture's open question). One master key encrypts
  everything; rotation implies a re-encrypt pass over the remote or a key-epoch tag in the frame AAD
  so old and new frames coexist. Adding a `key_epoch` to the AAD/metadata now is cheap and keeps the
  door open; the re-encryption job is deferred.
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
- **`hypha-fence` availability is a liveness, not safety, concern.** It sits off the data path and only
  acts on transitions, so while it is down the running active is unaffected and Cilium/OPNsense keep
  enforcing the existing allow. The only cost is a *delayed failover* if the active dies during that
  window; its absence can never produce two writers. Running it leader-elected (2 replicas) closes that
  window and is safe because the controller is idempotent — controller split-brain reconciles the same
  policy to the same value, unlike a data-plane split-brain.
- **`s3s` coverage** for every conditional-write / SigV4-chunked corner hypha's clients use should be
  spiked early — it's the load-bearing dependency; a gap there is the highest-impact unknown.
