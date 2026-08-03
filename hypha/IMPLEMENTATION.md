# Hypha — implementation snapshot

This document describes the code that exists today. [`ARCHITECTURE.md`](./ARCHITECTURE.md) owns the
system goals and rationale; implementation history and future phases are intentionally omitted here.

## Workspace

Hypha is a Rust 2021 workspace running on Tokio:

| Crate | Responsibility |
|---|---|
| `hypha-format` | age envelope, authenticated object trailer, ciphertext offset arithmetic, seekable ranged reads |
| `hypha-core` | configuration, S3 backend wrapper, cache metadata formats, shared errors |
| `hypha` | S3 server, tiering, recovery, reconciliation, GC, metrics, and the serving binary |

The client-facing server uses `s3s` 0.14. Backends use `aws-sdk-s3`; the cache and remote are
independently configured instances of the same backend type. TLS is expected to terminate upstream
of Hypha.

The main implementation areas are:

```text
hypha-format/src/       encryption, trailer, ranges
hypha-core/src/         backend, config, metadata
hypha/src/s3/           client-facing S3 operations
hypha/src/bucket/       bucket lifecycle and recovery
hypha/src/gc/           eviction, recency, debris, usage
hypha/src/tier.rs       shared cache/remote transitions
hypha/src/replication.rs
hypha/src/markers.rs
hypha/src/background.rs
hypha/src/admin.rs
```

## Storage model

Every client bucket maps to three backend buckets:

| Backend bucket | Endpoint | Contents |
|---|---|---|
| `<prefix>-d-<bucket>` | cache | plaintext bodies or tombstones at client keys |
| `<prefix>-m-<bucket>` | cache | facts twins, pending markers, multipart records, and recovery markers |
| `<prefix>-r-<bucket>` | remote | encrypted, self-describing client objects |

`<prefix>-g` is a deployment-wide cache bucket for GC state.

The cache is the steady-state namespace and client-ETag authority in both modes. The remote is the
durable encrypted store and the recovery authority after cache-volume loss. Bucket existence is
authoritative on the remote.

### Modes

| | `durable` | `cached` |
|---|---|---|
| PUT commit point | encrypted remote PUT | plaintext cache PUT |
| DELETE commit point | remote delete | cache absence |
| acknowledgement | after remote commit | after cache commit |
| cache body after remote durability | eviction tombstone | retained until GC |
| remote propagation | synchronous | pending marker + reconcile |
| cache-volume loss | no acknowledged data loss | acknowledged but unreconciled writes may be lost |
| tombstoned GET | remote decrypt | remote decrypt, then asynchronous rehydrate |

Multipart completion is always remote-durable, including in cached mode. Bucket create and delete
are also always remote-durable.

While a bucket namespace is being restored, reads use the remote and all writes temporarily use
durable semantics. This prevents a cached acknowledgement from landing in a namespace that readers
are intentionally ignoring.

## Remote object format

Each uploaded unit is a stock age v1 file encrypted with age's native scrypt recipient. The
deployment master passphrase is expected to contain 256 bits of entropy, so the scrypt work factor
is pinned to `1`; the passphrase supplies the security rather than password stretching. Each file
still receives a fresh age file key and salt.

age uses fixed 64 KiB plaintext chunks. `hypha-format` pins the scrypt header length and implements
closed-form plaintext/ciphertext offset conversion. A remote ranged GET reads only the required age
header and encrypted chunks.

Every completed remote object ends with a versioned trailer:

```text
single-part: age-file | facts | mac | version
multipart:   age-file... | part-end-offsets | facts | mac | version
```

The facts contain:

- object kind: single or composite;
- plaintext length;
- client-visible modification time;
- raw MD5 used by the client ETag;
- optional plaintext flexible checksum, including its algorithm and full-object/composite type;
- client part count.

For multipart objects, cumulative ciphertext offsets define the encrypted window of every client
part. The client ETag is the plaintext MD5 for a single-part object and
`md5(concat(part-md5s))-N` for multipart.

The trailer has a truncated HMAC-SHA256 tag derived from the master passphrase. It binds the facts,
part table, body length, format version, and object key. A remote object without a valid trailer is
treated as foreign or corrupt and triggers the persistent halt path.

Single-part facts are appended to the streaming PUT. Multipart facts are committed by native
multipart completion. Normally the trailer occupies a final internal part; when the last client
part cannot have a successor, Hypha retains that part's ciphertext and folds the trailer into it.
A persisted fold intent lets a retry restore the pure part before attempting another completion.

## Cache representation

`<data>` contains only client keys:

- a plaintext body in cached mode;
- an eviction tombstone for a remote-resident object;
- a transition tombstone while a durable mutation is unresolved;
- no entry for a deleted key.

Tombstones have fixed 16-byte bodies. Eviction-tombstone metadata carries plaintext length, client
ETag, checksum, original modification time, content type, storage class, and client metadata;
transition tombstones carry only their kind because the remote is authoritative until they settle.
Client PUTs equal to an internal sentinel are rejected so LIST classification by `(size, ETag)`
cannot be spoofed.

`<meta>` divides its keyspace into non-overlapping ranges:

```text
0x01 0x01 <tag> ...       multipart, recovery, recency, and shadow records
0x01 <client-key> ...     LIST facts twins
<client-key>              pending PUT/DELETE marker
```

Client keys are limited to 1024 bytes and may not contain `0x00` or `0x01`. Other control and
non-ASCII characters are supported through `encoding-type=url`.

Facts twins project tombstone facts into LIST without a HEAD per object. A twin is optional when its
derived key would exceed the backend key limit; LIST falls back to HEAD in that case. Tombstone
metadata remains authoritative.

Rehydrated single-part plaintext replaces its eviction tombstone at the client key. Rehydrated
multipart plaintext uses a generation-tagged shadow object in `<meta>` so the tombstone and its
composite facts remain intact.

Client `x-amz-meta-*` values and storage-class labels are cache-resident. They survive ordinary
tiering but not cache-volume loss. `Content-Type` additionally rides on the remote object and is
restored.

## Concurrency and commit safety

Hypha currently runs as one serving process. Correctness within that process uses three keyed lock
tables:

- the write lock serializes conditional writes, durable mutation brackets, multipart completion,
  eviction, and rehydrate;
- the upload lock serializes same-key reconcile work without placing client writes behind remote
  transfers;
- the multipart-part lock serializes replacement or folding of one upload's part while allowing
  different parts to upload concurrently.

Cached unconditional PUTs use the backend's last-writer-wins behavior and do not take the write
lock. Conditional PUTs resolve the current client ETag and evaluate `If-Match`/`If-None-Match`
under the write lock.

Durable mutations use:

```text
precondition -> transition tombstone -> remote commit -> cache settle
```

Readers that encounter a transition tombstone resolve from the remote. If a process dies inside the
bracket, the next access settles the cache to whichever state the atomic remote operation committed.

Cached writes commit to the cache and then hand a PUT or DELETE obligation to the marker actor.
Marker delivery is outside the acknowledgement path. A graceful run records positive evidence that
the pending-marker set is complete; without that evidence, the next run rebuilds it from the cache
and remote namespaces.

Reconcile uploads use only the upload lock and coalesce concurrent attempts. Reconcile deletes take
both the upload and write locks, re-check that the key is still absent, and issue an ordinary remote
`DeleteObject`. These locks exclude every Hypha remote writer, so the remote does not need
conditional delete. Marker removal remains an `If-Match` delete against the cache so a newer
obligation cannot be cleared.

### Bucket write gate

Every data-plane op — write or read — that touches a bucket crosses a per-bucket admission gate, one
`AtomicU64` stored beside the accounting in the state map's entry, so the readiness/delete word and
the memos are one atomic publication and one lifecycle. The word is:

```text
[ ready:1 | closed:1 | epoch:30 | rcount:16 | wcount:16 ]
```

`ready` is the flip that ends a restore (`Restoring` → `Ready`); `closed` is a `DeleteBucket` past
its emptiness check; `epoch` makes an admission that begins and ends during the emptiness listing
visible to the close's CAS; `wcount` counts writes in flight; `rcount` counts reads that committed
to the remote while the bucket was `Restoring`.

`DeleteBucket`:

1. refuses if a write is already in flight (`wcount > 0`);
2. checks the client-visible namespace is empty;
3. closes the exact observed gate state with CAS.

`rcount` is masked out of the close's compare: deletes do not wait on readers, so a reader passing
during the listing must not fail the close. The epoch makes a come-and-go write visible even with
`wcount` back at zero — the ABA case. The gate never blocks ordinary writes, and a refused delete
leaves it unchanged. An uncommitted close (failed remote drain, or a panic between the two) reopens
on drop, rather than leaving a live bucket that refuses every op for the rest of the run. Once
closed and committed, everything addressed to the bucket answers the retryable `OperationAborted`
rather than a permanent `NoSuchBucket` — the delete may still fail.

Writes are admitted through a CAS that classifies the write at the same time: a `Ready` bucket with
no restoring reader in flight admits a cached-mode deployment's cache-first write; everything else —
`Restoring`, or `Ready` with a reader in flight — commits durably. Reads take a ticket when
`Restoring` and hold it for the whole remote answer, which is what keeps a cached-mode write from
committing into a namespace a remote read is about to look at. The flip is idempotent, never
reverts, and keeps the gate its in-flight writes are counted in.

Bucket lifecycle is serialized by the bucket-control actor. It determines existence from its
remote-derived state map and emptiness from Hypha's client namespace rather than relying on backend
`DeleteBucket` behavior.

## Recovery and lifecycle

Two cache markers select two distinct recovery passes:

| Recovery | Trigger | Authority | Mutation |
|---|---|---|---|
| namespace restore | missing sync marker | remote | add missing tombstones and twins |
| pending-set rebuild | missing clean marker | cache namespace | recreate pending markers only |

Namespace restore is additive and idempotent. It never overwrites an entry that appeared while the
pass was running. Pending-set rebuild performs a streaming cache/remote merge and authenticates the
remote trailer of each intersecting live body to detect missing or stale remote generations.

Startup lists remote buckets and surveys them in one pass — sync marker, both clean markers, and the
bucket's pending-marker count — across buckets concurrently. The survey **reads only**, so any
unexpected error exits with the cache exactly as it was found and the next run resolves from the
same evidence. Only once every bucket has answered are the clean markers deleted, the backpressure
gate seeded, and recovery dispatched. Nothing is served in between, so nothing can move between a
marker being seen and being taken.

The seed must be published before the first recovery is queued: a rebuild raises markers through the
same counter and the seed is a *store*, so a raise landing between the count and the publish would
be overwritten with nothing to re-seed it for the rest of the run. For the same reason every bucket
is counted, unclean ones included — a rebuild counts only the markers it creates, leaving those the
crashed run already wrote uncounted.

The volume watcher periodically verifies that every ready bucket still has its sync marker.

A ready classification is derived once at startup and held for the run — the bucket map is the
single authority, which is exactly what makes a lost volume detectable: a ready bucket whose marker
vanished is a disagreement, not a namespace to re-derive. Recovery is therefore restart, never
in-place repair: restart re-runs resolution before anything is served, whereas re-deriving the map
under a live volume would trust a cache that may already have served false 404s.

Detected violations—including a foreign remote object, a vanished remote object behind an eviction
tombstone, or a ready namespace losing its sync marker—write a halt marker to the remote, stop the
server, and exit with code `86`. A later start sees the halt marker and exits before spawning
background work.

Graceful shutdown:

1. stops accepting S3 requests and drains connections;
2. seals the run and writes the pending-set clean markers, then joins the startup shadow sweeps and
   writes the shadow-clean evidence they earned;
3. stops taking background work, drains actor queues, and joins actors.

Step 2 is in that order deliberately. A sweep reads and deletes only shadow bodies, which are
range-A keys, while the pending set the run seal vouches for is range C — so the run seal does not
depend on the sweeps finishing. It does owe them the *shadow-clean* marker, which is why that seal
comes after the join. Putting the join first would hold the marker that decides whether the next run
rescans the whole pending set behind work whose worst case is leaked bytes. Both run concurrently
either way, so the ordering costs no wall time.

If a bounded shutdown phase times out, work is aborted without writing evidence that would let the
next run skip recovery.

## Implemented S3 surface

| Family | Operations and behavior |
|---|---|
| Buckets | `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListBuckets`, `GetBucketLocation`, `GetBucketVersioning` |
| Objects | `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects`, `CopyObject` |
| Listing | `ListObjects`, `ListObjectsV2` with prefix, delimiter, and pagination |
| Attributes | `GetObjectAttributes` for ETag, size, storage class, and multipart part sizes |
| Multipart | `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads` |

Implemented behavior includes:

- SigV4 authentication against one configured client access-key pair;
- plaintext ETags, `Content-MD5`, and flexible checksums. Single-part writes support CRC32, CRC32C,
  CRC64NVME, SHA1, and SHA256; multipart writes support composite checksums and full-object CRCs;
- byte-range GETs;
- PUT `If-Match` and `If-None-Match`;
- copy-source ETag and time preconditions;
- generation-bound copy-source reads through the same cache/remote view as other key reads;
- `CopyObject` up to 5 GiB: live sources copy atomically within the cache, while remote-resident
  sources use an internal multipart copy of the encrypted body plus a fresh destination-bound
  trailer and mtime. It preserves source ETag/part geometry and unchanged checksums; selecting a
  different checksum restreams the source plaintext to derive it;
- `UploadPartCopy` always restreams the exact plaintext range through decrypt, checksum, encrypt,
  remote part upload, and a matching durable MPU record;
- client metadata, `Content-Type`, and non-archive storage-class labels;
- up to 1,000 keys in `DeleteObjects`;
- part numbers 1–10,000 and S3 multipart ordering/minimum-size rules;
- repeated part numbers selected at completion by the client-returned plaintext ETag.

Current surface limits:

- `PutObject` and `UploadPart` require `Content-Length` and accept at most 4 GiB of plaintext;
- destination `CopyObject` conditions are unavailable in the `s3s` 0.14 request type;
- bucket versioning is always reported disabled and versioned operations are not implemented;
- ACL, policy, lifecycle, CORS, tagging, object lock, retention, archive restore, replication
  configuration, and SSE configuration are outside the implemented surface.

## Background work and GC

Cached-mode reconciliation periodically enumerates pending markers in `O(pending)` and uploads or
deletes the corresponding remote generation. Failed work remains represented by its marker and is
retried. `reconcile.concurrency` bounds uploads in flight, and each runs on its own task: the
codecs encrypt on whichever task drives them, so a pass that multiplexed its uploads onto one task
would run the whole sweep's encryption on a single core.

`reconcile.backpressure` bounds the async lag window: once the pending set crosses
`max_pending` markers, or the oldest pending marker crosses `max_age_ms`, cached PUT/DELETE/copy
are refused immediately with `503 SlowDown` (SDKs retry with backoff) instead of acking writes the
remote will keep falling behind. The counter is seeded once at startup, before the listener opens,
by a full census of every bucket's pending markers, and is exact thereafter: a marker counts once
when raised (create-only, so an overwrite never double-counts), once when the sweep's CAS actually
removes it, and wholesale when `DeleteBucket` drains a `<meta>` projection. Both thresholds at `0`
disable the gate. The age is sampled where the sweep already enumerates the pending set; the census
lives in the reconcile domain but runs from `Lifecycle::startup` because a one-time seed races
bucket classification — and an under-counted seed would turn the sweep's clears negative.

Cached tombstone reads are served immediately from the remote. Rehydrate submission is non-blocking
and deduplicated; a bounded worker pool warms the cache for later reads. A client write cancels a
same-key rehydrate before waiting for the write lock.

GC runs in both modes:

- it removes abandoned multipart records, stale twins, transition debris, and orphan shadows;
- an in-progress remote multipart upload whose cache record is gone is **aborted, never restored**:
  a record-less upload is one no client can address (`require_upload` refuses it), and the create
  lock's read-shared/exclusive-probe handshake guarantees a record-less upload is a leak rather than
  a create whose record has not landed yet;
- in cached mode, it may evict durable plaintext bodies when measured cache usage exceeds the high
  water mark;
- it persists a deployment-wide Bloom recency ring and uses bounded, yield-weighted probes rather
  than scanning the entire namespace each pass;
- pressure first shortens the pass interval, then increases concurrency, then relaxes the age
  threshold;
- an eviction is allowed only after pending-marker exclusion, remote-generation confirmation, and
  a conditional cache tombstone write.

The implemented usage source is SeaweedFS's HTTP topology/disk API, including vacuum requests for
dead-byte compaction. Without `gc.usage`, debris is still swept but bodies are never evicted.

## Configuration

Configuration is loaded from `hypha.toml`, overlaid by `HYPHA_` environment variables. Double
underscores select nested fields, for example `HYPHA_REMOTE__ENDPOINT`.

Required top-level configuration:

```toml
bucket_prefix = "hypha"
mode = "cached" # or "durable"
master_passphrase = "..."

[auth]
access_key = "..."
secret_key = "..."

[cache]
endpoint = "http://seaweedfs-s3:8333"
region = "us-east-1"
access_key = "..."
secret_key = "..."

[remote]
endpoint = "https://remote.example"
region = "us-east-1"
access_key = "..."
secret_key = "..."
```

Optional settings and defaults:

| Setting | Default |
|---|---:|
| `serving.listen` | `0.0.0.0:8014` |
| `serving.admin_listen` | `0.0.0.0:9014` |
| `reconcile.interval_ms` | 5,000 |
| `reconcile.concurrency` | 16 |
| `reconcile.backpressure.max_pending` | 0 (off) |
| `reconcile.backpressure.max_age_ms` | 0 (off) |
| `background.concurrency` | 4 |
| `background.queue_depth` | 256 |
| `volume_watch_interval_ms` | 30,000 |
| `gc.interval_ms` / `gc.min_interval_ms` | 300,000 / 1,000 |
| `gc.concurrency` / `gc.max_concurrency` | 4 / 16 |
| `gc.high_water` / `gc.low_water` | 0.85 / 0.70 |
| `gc.probe_pages` / `gc.yield_floor` | 5 / 0.2 |
| `gc.opportunistic_evictions` | 64 |
| `gc.recency.fill_target` / `depth` / `false_positive_rate` | 100,000 / 7 / 0.01 |

Invalid water marks, concurrency bounds, recency parameters, and bucket prefixes fail at startup.

## Backend contract

Cache requirements:

- unversioned buckets without Object Lock;
- conditional `PutObject` and `DeleteObject` with enforced `If-Match`;
- `CopyObject` with an enforced source `If-Match`;
- `ListObjectsV2` with `encoding-type=url`;
- ordinary S3 bucket, object, range, listing, and metadata behavior.

SeaweedFS 4.37 is the tested cache. MinIO does not enforce conditional delete and therefore is not a
valid production cache for the marker and shadow CAS paths.

Remote requirements:

- unversioned buckets without Object Lock;
- strongly consistent object and listing behavior;
- ranged GET;
- native multipart create, upload, ranged part copy, complete, abort, `ListParts`, and
  `ListMultipartUploads`;
- completion must honor the exact `(part number, ETag)` generations Hypha supplies;
- ordinary `DeleteObject`; conditional delete is not required.

The mapped remote namespace is expected to be private to one Hypha deployment. Foreign buckets or
objects inside that namespace are treated as invariant violations rather than ignored.

## Observability

The S3 listener emits JSON request spans with operation, bucket, key, bytes, cache-hit state,
outcome, and latency.

The separate unauthenticated admin listener serves:

- `/healthz`: process liveness;
- `/readyz`: startup/drain state plus a live remote reachability check;
- `/metrics`: Prometheus metrics for S3 traffic, cache hits, markers, reconciliation, uploads, GC,
  cache usage, water marks, backpressure throttling, and shutdown accounting.

## Verification

The default integration harness starts an isolated MinIO remote and cache per test:

```sh
cargo test --workspace
```

The production cache contract is exercised with SeaweedFS and a MinIO remote:

```sh
scripts/test-seaweedfs.sh
```

Additional suites:

```sh
scripts/test-seaweedfs-tiny.sh  # real backend exhaustion
scripts/s3s-e2e.sh              # upstream s3s client cases in Hypha's supported surface
```

The tests cover format round trips and corruption, S3 conformance, conditional-write concurrency,
multipart replacement and folding, restore and pending-set recovery, crash brackets, reconciliation,
GC and rehydrate races, backend exhaustion, injected faults, admin endpoints, tracing, and load.

## Deployment boundary

The repository implements the single-process data plane. Exactly one write-capable Hypha process may
run against a deployment prefix, and Stage 6 makes that a property of the workload rather than
something the process enforces. The chart that carries it is Stage 7.

## Outstanding work

### Data plane

- **Upgrade `s3s` to 0.15.0 when published.** The current crates.io release remains 0.14.1. On the
  upgrade:
  - remove the quoted-ETag workaround for `GetObjectAttributes` and update its tests;
  - implement destination `CopyObject` `If-Match`/`If-None-Match`, whose fields are absent from the
    0.14 request type;
  - review the remaining 0.15 breaking changes and rerun the external conformance suite.
  The relevant upstream fix is [s3s#629](https://github.com/s3s-project/s3s/issues/629).
### Additional verification

- Add stock `rage` interoperability coverage after stripping Hypha's trailer.
- Run a real zero-loss client, currently expected to be ZeroFS, against the durable endpoint.
- Curate Ceph's `s3-tests` to the implemented surface: basic objects, LIST v1/v2, multipart,
  conditions, and ranged reads. Versioning, ACL, lifecycle, CORS, SSE configuration, and object-lock
  families remain out of scope rather than expected failures to fix.

## Stage 6 — single writer by construction

Status: **decided; the work is Stage 7 chart work and startup latency**.

Hypha runs as **one** replica. There is no active-passive pair, no ownership lease, no fencing
controller, and no promotion protocol. Exclusivity is a property of the workload object, not
something the process asserts about itself.

### Why not active-passive

An earlier design contended for a Kubernetes Lease and retargeted Cilium policies at the winning Pod
UID. It was abandoned for three reasons.

**The fence could not be more than best-effort.** Cross-object invariants are held by in-process key
locks (`hypha/src/keylocks.rs`) and per-key conditional writes, so two live processes corrupt state
in ways recovery does not cover. A Lease is a *time-based* claim: safe between cooperating
processes, worthless against the stalled or partitioned former holder that is the whole reason to
fence. The network fence that was meant to cover that case is eventually consistent — Cilium 1.19.3
publishes no realized-policy acknowledgement — so the strongest guarantee available was still
probabilistic.

**The passive saved about a second and cost seventeen.** A promoting passive runs the same
`Lifecycle::startup` as a cold process; it pre-pays only binary start and config load. Against that,
the protocol added a 2 s acquire poll and, on crash, a 15 s lease expiry. A single-pod restart is
faster than lease failover for both graceful shutdown and process crash.

**Node loss is already fatal one layer down.** The SeaweedFS cache runs `replicas: 1`, node-pinned,
with `replicationPlacement: "000"`, and both modes depend on it — durable mode keeps it as the
namespace and ETag projection. Whole-node loss is the one scenario active-passive wins, and there is
nothing left to fail over to.

Storage-side fencing via SeaweedFS IAM keys was considered and rejected: it cannot cover the remote
(which is not required to have an IAM API), whoever mints keys holds admin credentials the zombie
would hold too, and identity propagation is no more synchronous than Cilium's.

### Required mechanism

- A **single-replica StatefulSet**. The controller creates no replacement until the old Pod object
  is fully deleted, so a partitioned node's still-running Pod blocks its own successor. This is
  strictly stronger than the Lease it replaces, and it is why a `replicas: 1` Deployment will not
  do: taint-based eviction marks the unreachable Pod terminating and the ReplicaSet immediately
  creates a second one alongside it.
- A plain Service selecting the workload labels. No UID selector, no sentinel, no selector patching.
- `terminationGracePeriodSeconds` covering the request-drain, obligation-seal and actor-drain
  budgets plus any `preStop` delay, so a planned restart always exits clean and the next start takes
  the accounted path rather than a pending-set rebuild.
- No Hypha egress default-deny. It existed only as the baseline the dynamic active-egress policy sat
  on; with no passive to deny it fences nothing, and as exfil containment it is weak (the allow-list
  must include the external remote) and costly (`toFQDNs` puts DNS through Cilium's proxy on a
  latency-first path). The cluster is ingress default-deny only by deliberate choice.

The accepted cost is that **node loss requires an explicit assertion that the node is down** before a
replacement runs — the `node.kubernetes.io/out-of-service` taint, applied by a human or by something
that can verify power state. This is correct rather than regrettable: fencing is a physical
assertion, and every scheme that tries to infer it from a peer lands on best-effort.

### Exit requirements

- A rollout shows exactly one running Pod at every instant.
- SIGTERM → next process serving is measured for a clean shutdown and for `SIGKILL`, and recorded
  against the ~17 s lease path this replaces.
- Recovery after an ungraceful stop preserves every acknowledged durable write, with only the
  documented cached-mode loss window remaining.

## Stage 7 — packaging and production installs

Status: **not started**.

Stage 7 delivers the cluster-admin-installed `hypha/` Helm chart:

- the single-replica serving StatefulSet, Service, and HTTPRoute;
- references to the master-passphrase and remote-credential Secrets;
- mode, backend, bucket-prefix, GC, reconcile, and shutdown settings;
- a node-loss runbook: verify the machine is down, then apply `node.kubernetes.io/out-of-service`.
  Never force-delete on suspicion;
- dashboards and alerts for request health, cache hits, pending work, recovery, GC pressure, and
  startup duration split by clean versus rebuild path.

Install the chart twice with distinct deployment prefixes or remote accounts:

- cached mode at `s3.internal.haustorium.net`;
- durable mode at `s3-direct.internal.haustorium.net`.

The pod termination grace period must cover the fixed request-drain, obligation-seal, and actor-drain
budgets plus any `preStop` delay.

### Exit requirements

- `helm lint hypha` and rendered-manifest validation pass.
- The full workspace, SeaweedFS cache-contract suite, exhaustion suite, and `s3s-e2e` supported
  selection pass against the packaged deployment.
- The Stage 6 single-writer and restart-timing checks pass against the charted resources.
- Both cached and durable endpoints are live behind the shared Gateway.
- A real client completes an end-to-end write/read/restart exercise against each endpoint, including
  the ZeroFS durable-endpoint check.
- Dashboards expose pending-marker growth, dirty drains, cache pressure, invariant halts, and
  restart duration before either endpoint is considered production-ready.
