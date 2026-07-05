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
ingress Gateway — takes over the role Ceph's `rook-ceph-rgw-s3-store` would have served for CloudNativePG
backups, application buckets, and any other S3 consumer.

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
   S3 clients (CNPG Barman, apps, rclone, ...)
             │  S3 API (+ conditional writes)
             ▼
        ┌─────────┐        plaintext (S3)     ┌───────────────────────┐
        │  hypha  │ ─────────────────────────▶ │ cache S3 endpoint     │
        │ (Rust)  │ ◀───────────────────────── │  hot · working tier   │
        └─────────┘                            │  (SeaweedFS / TopoLVM)│
             │                                 └───────────────────────┘
             │  ciphertext (framed AEAD), write-through + GC eviction
             ▼
   ┌────────────────────────────┐
   │ remote S3 endpoint         │  cold · durable · encrypted
   └────────────────────────────┘
```

- **Loose coupling.** Hypha talks plain S3 to a required *remote* and an optional *cache*. Neither is
  embedded in the proxy: swap SeaweedFS for any S3 cache, or the remote for any S3 provider, without
  touching hypha; omit the cache entirely for a synchronous pass-through. In this homelab the cache is
  `homelab-seaweedfs` (backed by `homelab-topolvm`), each its own chart.
- **The cache** holds the hot working set and the freshest writes — buckets, keys, metadata, and ETags
  as S3 objects, plus object *bodies* while they are hot. It is a disposable read-/write-through cache,
  not a store to reconstruct.
- **The remote** holds an encrypted copy of every object *body*; the key name and metadata stay in
  plaintext, as with standard S3 client-side encryption. The provider sees names and sizes, never contents.
- **Hypha** is the only component clients talk to. It brokers between the cache (when configured) and the
  remote, owns the encryption, and owns the S3 semantics (conditional writes, multipart, range reads).

## Caching is optional — compose tiers with deployments

Hypha is a single-mode encrypting S3 proxy: it always has a **remote**, and a **cache is optional**.
Rather than build durability tiers into one service, run hypha more than once, each deployment scoped to
its own remote namespace — a distinct account/bucket, or a shared remote under a forced key prefix:

- **Cached deployment (default)** — `s3.internal.haustorium.net`, backed by the SeaweedFS cache plus a
  remote. Reads and writes go through the cache; writes are acked locally and replicated to the remote
  asynchronously (write-through). Low latency, at the cost of the bounded async-lag loss window (see
  *Write-through durability*).
- **Cacheless deployment** — `s3-direct.internal.haustorium.net`, backed by a remote only. A pass-through
  encrypting proxy: a write is **not acked until the object is durably on the remote**, and reads come
  straight from the remote. No cache, so no loss window — for clients that cannot tolerate any loss, such
  as ZeroFS. Higher per-op latency, and it does not depend on SeaweedFS.

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
  cache object; an evicted one is a **tombstone** — a zero-byte cache object at the same key whose S3
  user-metadata records that the body now lives only on the remote (plus its frame offsets). There is no
  per-object key material to track: a single master key encrypts every body and the nonces travel inside
  the ciphertext frames. Because the remote keeps the real key name and metadata in plaintext (standard
  S3 client-side encryption), the filer is only a cache index — discard it and it repopulates lazily
  from the remote.

## S3 surface and conditional writes

Hypha presents a standard S3 API. The semantically important part is **conditional writes**, which it
enforces atomically against the object metadata it owns in the cache:

- `If-None-Match: *` — atomic create; succeeds only if the key does not already exist.
- `If-Match: <etag>` — compare-and-swap update; succeeds only if the current ETag matches.

Hypha serializes these against the current object state so concurrent writers see linearizable
create/update semantics, regardless of what the underlying backends guarantee on their own. `HEAD` and
`LIST` are served entirely from metadata and are correct whether an object's body is local or tombstoned.

## Encryption

- **One symmetric master key**, 32 bytes, delivered to hypha through a Kubernetes Secret kept out of
  git; the authoritative copy lives outside the cluster (password manager / safe). Losing it renders the
  remote copies unrecoverable — the same key-custody rule as the rest of the homelab's backups.
- **AEAD: XChaCha20-Poly1305.** Its 192-bit nonce is the crux of the design: nonces are chosen at random
  per frame, and a 192-bit random nonce is collision-safe across an effectively unbounded number of
  frames under a single key. That removes any need for a global counter or per-writer nonce coordination,
  which in turn is what makes independent per-part encryption (below) sound. (AES-256-GCM-SIV is a viable
  nonce-misuse-resistant alternative but is not the primary choice.)
- **Only bodies are encrypted — standard S3 client-side encryption.** The key name and object metadata
  are stored in plaintext on the remote, exactly as an S3 client-side-encryption client does; only the
  body is ciphertext. The provider can see names and sizes, never contents.
- The cache itself stores plaintext — it is node-local NVMe inside the trusted network, and keeping it
  plaintext is what keeps the hot path fast.

### Framed ciphertext format

An object's ciphertext is a sequence of self-describing **frames**:

```
frame := [ u32 plaintext_len ] [ 24B nonce ] [ ciphertext ] [ 16B Poly1305 tag ]
AAD   := object_uuid ‖ part_number ‖ frame_index_in_part ‖ flags   (last-frame, ...)
```

Every frame is independently authenticated and carries its own random nonce, so frames — and therefore
whole encrypted parts — can be produced independently and concatenated in order to form a valid object.
The AAD binds each frame to its object and position, so truncating, reordering, or splicing frames from
another object fails authentication.

## Multipart upload with encryption

Multipart is supported by encrypting **each part directly**, using the framing format above — there is
no completion-time re-encryption pass.

- Each `UploadPart` is encrypted into an integral number of frames and streamed straight to the remote
  (write-through). Because frames are self-describing and independently keyed by random nonce, a part is
  encrypted with no knowledge of the other parts.
- Parts may arrive out of order, in parallel, or be re-uploaded. A re-upload simply replaces that part's
  frames with freshly-nonced ones — there is no nonce to reuse and nothing to coordinate.
- Parts also land **plaintext in the cache**, which handles multipart natively, so reads are hot
  immediately. A simple (non-multipart) `PUT` is just the single-part degenerate case on the same path.
- `CompleteMultipartUpload` writes and authenticates the **frame manifest** — the ordered list of parts
  and, per frame, its plaintext length and ciphertext offset — into the object's metadata. The manifest
  makes the concatenation canonical and lets hypha:
  - serve **ranged GETs** by fetching only the covering frames from the remote (byte-range GET), and
  - detect truncation or cross-object splicing when reassembling.
- An aborted multipart upload leaves orphaned frames on the remote keyed under the upload id; these are
  reclaimed by the same GC pass that manages eviction.

## Data path

- **PUT / UploadPart** → write plaintext to the cache; frame-encrypt and stream to the remote
  asynchronously (write-through); record the manifest + remote key in metadata; mark the object durable
  once the remote write is confirmed.
- **GET** → if the body is local, serve from the cache. If tombstoned (or the local body was lost to a
  node failure), consult the manifest, fetch the covering frames from the remote, authenticate and
  decrypt, and stream to the client; optionally rehydrate the body locally and bump its LRU position.
- **HEAD / LIST** → served from cache metadata when warm, otherwise from the remote (which holds the same
  keys and metadata).
- **DELETE** → remove locally and enqueue the remote deletion.
- **Cacheless deployment** → the same operations with no cache: a PUT frame-encrypts and writes straight
  to the remote, acking only once it is durable; a GET fetches and decrypts from the remote.

## Write-through durability

Every write is mirrored to the remote as it happens, so the encrypted remote is a *continuous* copy, not
a periodic snapshot — always current to within the async upload lag, which is what lets the cache run
with no local redundancy and supersedes the old nightly `rclone crypt` sync.

Clients that cannot tolerate the async-lag window use a **cacheless deployment** (see *Caching is
optional*), whose writes ack only after the remote confirms.

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

- Crossing the **high-water mark** starts GC: evict least-recently-used object *bodies*. Eviction
  confirms the remote copy is durable, deletes the local body from the cache, and leaves a **tombstone** —
  the metadata and frame manifest stay, marking the body as remote-only. GC continues until the
  **low-water mark** is reached.
- LRU is tracked by hypha (access time updated on GET/HEAD) in object metadata.
- Reading a tombstoned object rehydrates it (optionally) and refreshes its LRU position, so working sets
  stay hot and cold data drifts out to the remote.
- The same pass reclaims orphaned frames from aborted multipart uploads.

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
  metadata are plaintext there, bodies are encrypted there), so there is nothing to reconstruct. Start
  with an empty cache and it repopulates lazily: a LIST or GET falls through to the remote, and reads warm
  the cache on the way back. No rebuild, no filer replication, no metadata backup. The only unrecoverable
  loss is the **bounded** set of objects still within the async write-through lag — written but not yet on
  the remote — which is accepted under the no-redundancy design. This applies only to a cached deployment;
  a cacheless one writes synchronously to its remote, so it has no such window.
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
- `hypha/` — the Rust gateway `Deployment` + `Service` + `HTTPRoute`, plus references to the master-key
  Secret and the remote S3 credentials Secret. Caching and the remote prefix are chart values: install it
  once with a cache (`s3.internal.haustorium.net`) as the default tier, and again cacheless
  (`s3-direct.internal.haustorium.net`) for zero-loss clients — each scoped to its own remote account or,
  on a shared remote, its own key prefix.

### Access to the cache surfaces

SeaweedFS's surfaces (S3, filer, volume, master) are plaintext and unauthenticated, so the network is
their only fence. Their namespace is default-deny ingress, and each cross-namespace consumer is named
explicitly by the `seaweedfs` chart (a `CiliumNetworkPolicy` per surface) — hypha for the S3 data path
and, if the `seaweedfs` usage source is enabled, the master/volume status APIs.

These grants are scoped by source **namespace** — the cluster's identity boundary, since namespaces
are single-tenant and pod labels are self-applied (see the design doc, §11.4). So "hypha may read the
cache" means "workloads in hypha's namespace may."

The master UI is additionally reachable through the shared gateway; the data surfaces are not routed.

## Open questions

- Rehydrate-on-read policy: always, never, or size-thresholded.
- Remote key rotation / re-encryption strategy for the single master key.