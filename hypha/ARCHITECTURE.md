# Hypha — caching + encrypting S3 gateway

Hypha is the homelab's object storage. It is a **caching and encrypting proxy** that speaks the full
S3 API (including conditional writes) to its clients. It keeps a plaintext copy in a **cache S3
endpoint** as the hot working tier and mirrors an encrypted copy to a **remote S3 endpoint**
for durability. Both are just S3 endpoints in hypha's configuration — **loosely coupled and
interchangeable**: the homelab implements the cache with SeaweedFS on TopoLVM (deployed separately,
below) and can point the remote at any S3-compatible provider, but hypha embeds neither. When the
cache fills up, hypha garbage-collects the least-recently-used object bodies, replacing them with
tombstones that point at the encrypted remote copy.

It replaces the previously-planned Ceph/Rook RGW stack (which was built but never deployed, so there is
no data to migrate). Hypha's in-cluster endpoint — `s3.internal.haustorium.net`, via the shared Cilium
ingress Gateway — takes over the role Ceph's `rook-ceph-rgw-s3-store` would have served for application
buckets, backups, and any other S3 consumer.

## Why this instead of Ceph

- **Latency first.** SeaweedFS with no replication and node-local NVMe is a flat, fast path. There is no
  MON/OSD peering, no placement-group rebalancing, no cross-node write amplification on the hot path.
- **Encryption is intrinsic, not bolted on.** The remote copy is always ciphertext, produced inline as
  data is written — there is no separate nightly `rclone crypt` job to schedule, monitor, or fall behind.
- **No local redundancy; the encrypted remote is the durability copy.** SeaweedFS runs unreplicated and
  hypha continuously mirrors an encrypted copy to the remote. Whether that copy is a plain mirror or a
  point-in-time backup is a property of the remote bucket, not of hypha (see *Write-through durability*).
- **Full S3 surface, including conditional writes.** Hypha owns the semantics its clients need
  (compare-and-swap creates and updates) rather than inheriting whatever a given backend does or doesn't
  implement.

The name follows the `haustorium.net` theme: a *hypha* is a filament of fungal mycelium — a thin conduit
that moves nutrients between a store and the outside.

## Architecture

```
   S3 clients
             │  S3 API (+ conditional writes)
             ▼
        ┌─────────┐        plaintext (S3)     ┌───────────────────────┐
        │  hypha  │ ─────────────────────────▶ │ cache S3 endpoint     │
        │ (Rust)  │ ◀───────────────────────── │  hot · working tier   │
        └─────────┘                            │  (SeaweedFS / TopoLVM)│
             │                                 └───────────────────────┘
             │  ciphertext (age v1), write-through + GC eviction
             ▼
   ┌────────────────────────────┐
   │ remote S3 endpoint         │  cold · durable · encrypted
   └────────────────────────────┘
```

- **Loose coupling.** Hypha talks plain S3 to a *remote* and a *cache* — both required. Neither is
  embedded in the proxy: swap SeaweedFS for any S3 cache, or the remote for any S3 provider, without
  touching hypha. In this homelab the cache is `homelab-seaweedfs` (backed by `homelab-topolvm`),
  each its own chart.
- **The cache** holds the hot working set and the freshest writes — buckets, keys, metadata, and ETags
  as S3 objects, plus object *bodies* while they are hot. It is a disposable read-/write-through cache,
  not a store to reconstruct.
- **The remote** holds an encrypted copy of every object *body*; the key name and metadata stay in
  plaintext, as with standard S3 client-side encryption. The provider sees names and sizes, never contents.
- **Hypha** is the only component clients talk to. It brokers between the cache (when configured) and the
  remote, owns the encryption, and owns the S3 semantics (conditional writes, multipart, range reads).

## Two modes — compose tiers with deployments

Hypha always has a **remote** and a **cache**; a deployment runs in one of two modes that differ only
in *when* a write becomes durable on the remote and whether the cache retains bodies. Rather than
build durability tiers into one service, run hypha more than once, each deployment scoped to its own
remote namespace — a distinct account/bucket, or a shared remote under a forced key prefix:

- **Cached deployment (default)** — `s3.internal.haustorium.net`. Reads and writes go through the
  cache; writes are acked locally and replicated to the remote asynchronously (write-through). Low
  latency, at the cost of the bounded async-lag loss window (see *Write-through durability*).
- **Durable deployment** — `s3-direct.internal.haustorium.net`. A write is **not acked until the
  object is durably on the remote**, and the cache holds no bodies — only the tombstones and metadata
  that make it the namespace and ETag source of truth (HEAD/LIST and conditional writes are still
  cache-served). No loss window — for clients that cannot tolerate any loss, such as ZeroFS. Higher
  per-op write latency, and it still depends on the SeaweedFS cache; the only guarantee it trades
  away from the cached mode is exposure to the bounded loss window.

Each deployment prepends a configured **remote prefix** to every object key it stores (and strips it on
read), so deployments that share one remote account or bucket still land in disjoint key-spaces. Because
their namespaces don't overlap — whether by separate account or by prefix — there is no shared-key
cache-coherence concern. Both apply the same client-side body encryption.

## Storage substrate — TopoLVM

SeaweedFS volumes sit on **TopoLVM**, a CSI driver that carves node-local logical volumes out of an LVM
volume group. The volume group lives on the partition previously reserved for Ceph: `/dev/nvme0n1p3`
(GPT label `ceph`) on the server and compute nodes. That partition becomes an LVM PV in a volume group
(`vg-nvme`); TopoLVM's `lvmd` manages it and provisions PVs on demand.

- The `topolvm-provisioner` StorageClass uses `volumeBindingMode: WaitForFirstConsumer` so a SeaweedFS
  volume server binds storage on the node it actually lands on — node-local NVMe, no network round-trip.
- Because this is a cluster-scoped CSI driver that needs privileged node access to LVM, it is installed
  by cluster-admin (the same class of foundational component as Cilium and cert-manager) — the
  autonomous operator cannot install it.
- Storage nodes are selected by a `vg=nvme` label, replacing the old `ceph-osd=true` label.
  The `db` and `compute-spot` roles have no `p3` partition and do not participate.

## SeaweedFS configuration

SeaweedFS runs as master + volume servers + filer + S3, one volume server per storage node with a
node-local PVC from TopoLVM.

- **Redundancy off** (`replication: "000"`). There is no local second copy; durability comes from the
  encrypted remote. This is the deliberate latency/simplicity trade.
- **Tuned for latency**: in-memory needle maps, NVMe-backed volumes, local filer metadata store.
- **Self-documenting objects, no separate index.** hypha keeps no side index. A live object is just a
  cache object; an evicted one is a **tombstone** — a cache object at the same key holding a fixed
  sentinel body (so its size and ETag identify it straight off a LIST) whose S3 user-metadata
  records that the body now lives only on the remote (plus the client-visible ETag), paired with a
  zero-byte *facts twin* at a suffixed key that carries the LIST-visible facts. There is no
  per-object key material to track on the cache side: hypha encrypts each remote object as an age file
  whose fresh random file key is wrapped under the master secret in the age header stored on the
  remote object itself, and the cache stores only plaintext. Because the remote keeps the real key name and
  metadata in plaintext (standard S3 client-side encryption), the filer is only a cache index — discard
  it and hypha's startup reconciliation rebuilds the namespace from the remote as tombstones, with
  bodies rehydrating on read.

## S3 surface and conditional writes

Hypha presents a standard S3 API. The semantically important part is **conditional writes**, which it
enforces atomically against the object metadata it owns in the cache:

- `If-None-Match: *` — atomic create; succeeds only if the key does not already exist.
- `If-Match: <etag>` — compare-and-swap update; succeeds only if the current ETag matches.

Hypha constrains client keys slightly beyond stock S3: no bytes below `0x20` (AWS already tells
clients to avoid them) and a length cap short of 1024 — what makes the suffixed facts-twin scheme
sort correctly and fit.

Hypha serializes these against the current object state so concurrent writers see linearizable
create/update semantics, regardless of what the underlying backends guarantee on their own. `HEAD` and
`LIST` are served entirely from metadata and are correct whether an object's body is local or tombstoned.

## Encryption

- **One symmetric master secret** — a 256-bit random string used as an age passphrase — delivered to
  hypha through a Kubernetes Secret kept out of git; the authoritative copy lives outside the cluster
  (password manager / safe). Losing it renders the remote copies unrecoverable — the same key-custody
  rule as the rest of the homelab's backups. Symmetric rather than age's native X25519 because the
  remote provider keeps the ciphertext forever: a harvest-now-decrypt-later adversary defeats any later
  migration off a quantum-vulnerable KEM, so the key wrap is post-quantum from the first byte or never.
  (age's native PQ stanza, `mlkem768x25519`, buys the same property at ~1.6 KiB of KEM encapsulation in
  every file header — the wrong trade for a namespace heavy in small objects.)
- **Envelope format: [age](https://age-encryption.org/v1), native scrypt recipient.** Rather than
  design a custom AEAD framing — or even a custom stanza — hypha uses the reviewed age v1 format,
  Filippo's modern streaming AEAD, entirely stock: it has the exact properties hypha needs (per-chunk
  authentication, seekable decryption for range GET, splice/truncation detection via a finalizer
  chunk), and disaster recovery is any age binary plus the passphrase. Each remote object (single-part
  body, or one age file per multipart part) is an independent age file; age generates a fresh random
  **file key** and scrypt salt per invocation, so parallel `UploadPart` workers and concurrent PUTs
  need no nonce or key coordination — the per-file file key *is* the coordination-free property,
  per-file key isolation removes any cross-object nonce-reuse budget, and one file's ciphertext leaks
  nothing about another's. The **scrypt work factor is pinned to the minimum**: the KDF's stretching
  exists to protect low-entropy human passphrases, and hypha's passphrase is full-entropy — security
  lives in its 256 bits, not the work factor. (Left at age's default — auto-tuned to ~1 s and ~256 MiB
  *per file* — the wrap would dominate every small-object operation.)
- **Rotation is a flag day, accepted deliberately.** The age spec requires an scrypt stanza to be the
  only stanza in a file, so there is no multi-recipient lazy re-wrap. That forfeits little:
  harvest-now-decrypt-later already means rotation cannot retroactively protect harvested ciphertext —
  the true response to a compromised key is a full re-encrypt sweep under a new secret, and that
  remains available under any scheme.
- **Only bodies are encrypted — standard S3 client-side encryption.** The key name and object metadata
  are stored in plaintext on the remote, exactly as an S3 client-side-encryption client does; only the
  body is ciphertext. The provider can see names and sizes, never contents.
- The cache itself stores plaintext — it is node-local NVMe inside the trusted network, and keeping it
  plaintext is what keeps the hot path fast.

### age format specifics that hypha relies on

age v1 chunks the plaintext into fixed **64 KiB chunks** (65536 plaintext bytes + 16-byte
Poly1305 tag = 65552 ciphertext bytes per chunk), each independently authenticated with ChaCha20-Poly1305
under a key derived from the file key. Nonces are *deterministic* — derived from the chunk index, not
random — so:

- **Range GET** maps a plaintext byte range to a contiguous ciphertext byte range (`chunk_index =
  floor(plaintext_byte / 65536)`, ciphertext offset = `chunk_index * 65552` plus the per-file
  header + payload-nonce offset, derived from the object's plaintext and ciphertext lengths).
  age's `StreamReader` implements `std::io::Seek` when its underlying reader does; an S3 GET body
  is a one-shot stream, so hypha supplies a small adapter that satisfies `Seek` by issuing a fresh
  byte-range GET per seek (one per request in practice). Hypha does not reimplement chunk
  decryption.
- **Per-part independence** is achieved by giving each multipart part its own age file (and therefore
  its own fresh file key and its own nonce space starting at chunk 0). No cross-part coordination; a
  re-upload of a part just creates a new age file with a fresh random file key. Equivalent to the
  previous custom-format property, achieved without relying on a 192-bit random-nonce collision budget
  under one shared key.
- **Splice / truncation / reorder detection** falls out of key separation (a chunk from object A dropped
  into object B's slot fails to decrypt with B's file key) plus chunk-index-in-nonce derivation (reordered
  chunks fail authentication) plus age's finalizer chunk (truncation is detectable cleanly).

## Multipart upload with encryption

Multipart uploads **route parts around the cache** — parts are not readable individually until
`CompleteMultipartUpload` commits the composite, the cache's latency win doesn't help throughput-bound
multipart traffic, and routing parts through the cache would impose S3 multipart plumbing on the
cache with no upside. Multipart takes the durable data path regardless of the deployment's mode:
hypha proxies the multipart ops onto the **remote's own native multipart upload** at the same key,
just with streaming encryption per part. Only `CompleteMultipartUpload` touches the cache, atomically
writing a tombstone at the composite's key to replace any stale cached body and keep the cache
namespace complete for LIST. In a cached deployment a completed composite becomes cachable on first
read: a GET fetches and decrypts from the remote, then asynchronously populates the cache from the
decrypted plaintext — the same rehydrate path used for tombstoned bodies.

Each part is encrypted **directly as its own age file**, using the format above — there is no
completion-time re-encryption pass.

- Each `UploadPart` is encrypted into an age file (fresh random file key per part) and streamed to
  the remote as that upload's native part. Because each part has its own file key and its own nonce
  space starting at chunk 0, a part is encrypted with no knowledge of the other parts. Hypha computes
  the part's **plaintext MD5** inline while encrypting and accumulates the per-part facts in the
  upload's state (persisted under a reserved cache prefix, so a restart mid-upload doesn't lose them).
- Parts may arrive out of order, in parallel, or be re-uploaded; concurrent uploads to one key are
  the remote's native multipart semantics. A re-upload is just a new age file with a fresh file key.
- `CompleteMultipartUpload` composes the S3-correct composite ETag from the accumulated per-part
  MD5s, uploads it with the total plaintext size as a small fixed-size **terminating footer
  part**, and completes the upload on the remote, which concatenates the parts into a single
  object at the key — one atomic op that is both the durability commit and the facts carrier.
  The committed object is self-describing: a discarded cache is rebuilt by reading the ETag and
  size off the object's tail, with no side records or tags to crash between. The facts also
  live on the cache tombstone (and its facts twin). There is no stored part table: a ranged GET
  recovers ciphertext part boundaries from the remote's own part index and derives each part's
  plaintext length from its ciphertext length in closed form (the header length is shared
  across parts and checked by tiling to the recorded total). Truncation and cross-object
  splicing are detected by per-part file-key separation plus age's chunk-index-in-nonce
  derivation plus the finalizer chunk.
- Each part's plaintext size is capped at **4 GiB** (one line below the S3 `UploadPart` 5 GiB max) so
  the age envelope (~1.3 MiB overhead per GiB plus a ~200 B header) never pushes the framed part over
  the remote's part-size cap. Homelab parts are 5–128 MiB in practice; transparent re-splitting of a
  larger client part at the 4 GiB boundary is a later refinement, not launch scope.
- `AbortMultipartUpload` maps to the remote's native abort; abandoned uploads are reclaimed by the
  same GC pass that manages eviction.

## Data path

- **PUT** (single-object) → in a cached deployment, write plaintext to the cache, write a pending
  marker, and ack; a background reconcile pass frame-encrypts and uploads to the remote (write-through
  async). In a durable deployment, frame-encrypt and upload straight to the remote — the commit —
  with the key marked in-transition so readers resolve it from the remote and never see torn
  state; then settle the cache tombstone and ack.
- **UploadPart** → routed around the cache in both deployments: frame-encrypt and stream straight to
  the remote as a native multipart part, ack once the remote confirms. The composite is not
  readable until `CompleteMultipartUpload` commits it, and a multipart's size is throughput-bound, so
  the cache's latency win doesn't apply.
- **GET** → if the body is local, serve from the cache. If tombstoned (or the local body was lost to a
  node failure), fetch the covering age chunks from the remote, authenticate and decrypt, and stream
  to the client; in a cached deployment, **rehydrate the body locally and bump its LRU position** —
  this is also how a completed multipart composite first enters the cache. A durable deployment never
  rehydrates: the body would immediately be tombstoned again.
- **HEAD / LIST** → served from the cache while its **sync marker** is present — a reserved cache
  object recording that reconciliation has made the namespace complete (every remote key has a
  local body or tombstone, so an absent key is authoritatively a 404). While the marker is absent,
  reads use the remote as the source of truth until reconciliation finishes and rewrites it.
  LIST entries always report plaintext sizes and client ETags: for tombstoned keys and cached
  multipart composites these ride **facts twins** — zero-byte cache objects whose key is the
  object's key plus a low-sorting suffix encoding the facts, so they arrive adjacent to their key
  inside the same LIST page. Listing the remote (the resync window) instead requires a bounded
  per-entry HEAD fan-out.
  Buckets map one-to-one across cache and remote; bucket create/delete is synchronous
  write-through (acked only once both sides confirm), and nothing else touches the remote, so
  `ListBuckets` follows the same rule.
- **DELETE** → in a cached deployment, overwrite the local body at K with a delete-tombstone (so
  GET answers 404 and LIST omits K) and write a pending marker; the background reconcile propagates
  `DeleteObject` to the remote, then clears marker and tombstone — the mask keeps the local
  namespace authoritative and a crash mid-delete cannot resurrect the object. In a durable
  deployment the remote delete is the commit: K is marked in-transition (readers keep seeing the
  object from the remote until the delete lands, so an unacked delete stays invisible), then the
  cache entry is cleared before the ack.

## Write-through durability

Every write is mirrored to the remote as it happens, so the encrypted remote is a *continuous* copy, not
a periodic snapshot — always current to within the async upload lag, which is what lets the cache run
with no local redundancy and supersedes the old nightly `rclone crypt` sync.

Clients that cannot tolerate the async-lag window use a **durable deployment** (see *Two modes*),
whose writes ack only after the remote confirms.

This is **replication, not a backup**: it protects against node/disk loss but faithfully propagates
destructive operations. A client `DELETE` or overwrite is written through to the remote, so the live
mirror alone does not recover from accidental or malicious deletion. Whether that gap is closed is a
property of the **remote bucket, not of hypha** — hypha's behaviour is identical either way, so the two
are simple to switch between:

- **A — mirror only.** The remote holds exactly the current object set. Durable against hardware loss; a
  deletion is unrecoverable once propagated.
- **B — versioned backup.** With versioning + object-lock/retention enabled on the remote bucket, the
  same write-through accumulates history, so deletes and overwrites stay recoverable for the retention
  window, at the cost of storing prior versions.

## Tiering and garbage collection

The cache is bounded storage over an unbounded remote. Hypha watches cache usage — reported by a
pluggable *usage source* (below) — against a high-water and low-water mark:

- Crossing the **high-water mark** starts GC with a byte target: reclaim down to the
  **low-water mark**, evicting the coldest object *bodies* first. Eviction confirms the remote
  copy is durable, deletes the local body from the cache, and leaves a **tombstone** — the
  metadata stays, marking the body as remote-only.
- Recency is tracked by an in-memory **Bloom-ring sketch** (one filter per fill window — a slice
  rotates when enough distinct keys have been touched — fed by GET/HEAD,
  sealed slices persisted to the cache). The newest slice containing a key quantizes its
  last-access age, so eviction works coldest-first: keys the ring has no memory of, then
  progressively younger age buckets until the target is met, LastModified breaking ties within a
  bucket. If the sketch is lost or absent (first boot), every key falls into one bucket and
  eviction degrades to LastModified alone — churnier for one cycle, never incorrect.
- Reading a tombstoned object rehydrates it (optionally) and refreshes its LRU position, so working sets
  stay hot and cold data drifts out to the remote.
- The same pass reclaims orphaned age files from aborted multipart uploads.

**Usage source (pluggable).** How hypha measures cache usage is an interface with several
implementations; a deployment picks the highest-fidelity one its cache supports:

- **`internal`** — hypha's own accounting (sum of the object bytes it has written). Backend-agnostic —
  works with any S3 cache — but blind to backend overhead and to space held by deleted-but-uncompacted
  data. The default and the fallback.
- **`seaweedfs`** — reads SeaweedFS's status/metrics APIs (per-volume sizes, deleted-byte counts, disk
  free) for physically accurate usage, and can drive `volume.vacuum` to reclaim dead bytes before the LV
  fills.

A new cache backend adds its own provider; hypha falls back to `internal` when no higher-fidelity source
is configured.

Tombstones are cheap (metadata only), so the object namespace can far exceed local capacity while hot
data stays on NVMe.

## Failure modes and disaster recovery

- **Volume-server (body) loss.** Hot bodies on that node's NVMe are gone, but the write-through remote
  copy means hypha transparently serves those objects from the remote — a lost local body is
  indistinguishable from a tombstone at read time.
- **Cache loss is not fatal — just discard it.** The cache holds nothing the remote doesn't (keys and
  metadata are plaintext there, bodies are encrypted there), so there is nothing to reconstruct beyond
  the namespace itself. Start with an empty cache: the sync marker is gone with it, so hypha serves
  reads from the remote while reconciliation relists it and rebuilds every bucket and key as
  tombstones, then restores the marker; bodies rehydrate on read. No filer replication, no
  metadata backup. The only unrecoverable loss is the **bounded** set of operations still within the
  async write-through lag — written (or deleted) but not yet propagated to the remote — which is
  accepted under the no-redundancy design. This applies only to a cached deployment;
  a durable one writes synchronously to its remote, so it has no such window.
- **Remote unavailable.** Reads of hot (local) objects are unaffected; reads of tombstoned objects fail
  cleanly until the remote returns. Write-through uploads queue and retry.
- **Clean swap from Ceph.** Because Ceph was never deployed, standing up hypha is a greenfield install
  with no migration.

## Deployment

Hypha is foundational tooling, installed by cluster-admin, not by the autonomous operator (it owns
cluster-scoped storage and needs privileged node access). It is delivered as its own top-level charts,
each in its own namespace, mirroring the `cilium` / `cert-manager` / `monitoring` pattern:

- `topolvm/` — the CSI driver and the `topolvm-provisioner` StorageClass.
- `seaweedfs/` — SeaweedFS (master / volume / filer / S3), redundancy off.
- `hypha/` — the Rust gateway: a two-pod **StatefulSet** (active + pre-warmed passive; pod-name
  labels give each pod the static Cilium identity the fencing controller selects on) + `Service` +
  `HTTPRoute`, the `hypha-fence` failover controller, plus references to the master-key Secret and
  the remote S3 credentials Secret. Mode and the remote prefix are chart values: install it once in
  cached mode (`s3.internal.haustorium.net`) as the default tier, and again in durable mode
  (`s3-direct.internal.haustorium.net`) for zero-loss clients — each scoped to its own remote account
  or, on a shared remote, its own key prefix.

### Access to the cache surfaces

SeaweedFS's surfaces (S3, filer, volume, master) are plaintext and unauthenticated, so the network is
their only fence. Their namespace is default-deny ingress, and each cross-namespace consumer is named
explicitly by the `seaweedfs` chart (a `CiliumNetworkPolicy` per surface) — hypha for the S3 data path
and, if the `seaweedfs` usage source is enabled, the master/volume status APIs.

These grants are scoped by source **namespace** — the cluster's identity boundary, since namespaces
are single-tenant and pod labels are self-applied (see the design doc, §11.4). So "hypha may read the
cache" means "workloads in hypha's namespace may."

Hypha will reach seaweedFs by its cluster ip. 