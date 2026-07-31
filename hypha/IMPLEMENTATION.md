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
ETag, original modification time, content type, storage class, and client metadata; transition
tombstones carry only their kind because the remote is authoritative until they settle. Client PUTs
equal to an internal sentinel are rejected so LIST classification by `(size, ETag)` cannot be
spoofed.

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

Every write holds a per-bucket admission guard until its commit and follow-up obligation are
established. The gate is one `AtomicU64` containing:

```text
closed:1 | admission-epoch:31 | in-flight-count:32
```

`DeleteBucket`:

1. refuses if a write is already in flight;
2. checks the client-visible namespace is empty;
3. closes the exact observed gate state with CAS.

The epoch makes an admission that begins and ends during the namespace listing visible to the CAS.
The gate never blocks ordinary writes, and a refused delete leaves it unchanged. Once closed, no
write can recreate a backend bucket behind deletion.

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
pass was running. Pending-set rebuild performs a streaming cache/remote merge and uses object size,
then authenticated trailer facts where necessary, to detect missing or stale remote generations.

Startup lists remote buckets, resolves their markers, removes clean markers before serving, and
dispatches required recovery. The volume watcher periodically verifies that every ready bucket
still has its sync marker.

Detected violations—including a foreign remote object, a vanished remote object behind an eviction
tombstone, or a ready namespace losing its sync marker—write a halt marker to the remote, stop the
server, and exit with code `86`. A later start sees the halt marker and exits before spawning
background work.

Graceful shutdown:

1. stops accepting S3 requests and drains connections;
2. joins marker and shadow obligations and writes clean evidence where earned;
3. stops taking background work, drains actor queues, and joins actors.

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
- plaintext ETags and `Content-MD5` validation;
- byte-range GETs;
- PUT `If-Match` and `If-None-Match`;
- copy-source ETag and time preconditions;
- client metadata, `Content-Type`, and non-archive storage-class labels;
- up to 1,000 keys in `DeleteObjects`;
- part numbers 1–10,000 and S3 multipart ordering/minimum-size rules;
- repeated part numbers selected at completion by the client-returned plaintext ETag.

Current surface limits:

- `PutObject` and `UploadPart` require `Content-Length` and accept at most 4 GiB of plaintext;
- `CopyObject` is implemented only in durable mode;
- destination `CopyObject` conditions are unavailable in the `s3s` 0.14 request type;
- flexible checksum fields are not implemented;
- bucket versioning is always reported disabled and versioned operations are not implemented;
- ACL, policy, lifecycle, CORS, tagging, object lock, retention, archive restore, replication
  configuration, and SSE configuration are outside the implemented surface.

## Background work and GC

Cached-mode reconciliation periodically enumerates pending markers in `O(pending)` and uploads or
deletes the corresponding remote generation. Failed work remains represented by its marker and is
retried.

Cached tombstone reads are served immediately from the remote. Rehydrate submission is non-blocking
and deduplicated; a bounded worker pool warms the cache for later reads. A client write cancels a
same-key rehydrate before waiting for the write lock.

GC runs in both modes:

- it removes abandoned multipart records, stale twins, transition debris, and orphan shadows;
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
| `serving.offload_threshold` | 1 MiB; currently all codec bridges offload |
| `reconcile.interval_ms` | 5,000 |
| `reconcile.concurrency` | 16 |
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
  cache usage, water marks, and shutdown accounting.

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

The repository currently implements the single-process data plane. It does not contain the
active-passive fencing controller, StatefulSet/chart, or promotion protocol described by the
architecture. Running more than one write-capable Hypha process against the same deployment prefix
is therefore unsupported.

## Outstanding work

### Data plane

- **Upgrade `s3s` to 0.15.0 when published.** The current crates.io release remains 0.14.1. On the
  upgrade:
  - remove the quoted-ETag workaround for `GetObjectAttributes` and update its tests;
  - implement destination `CopyObject` `If-Match`/`If-None-Match`, whose fields are absent from the
    0.14 request type;
  - review the remaining 0.15 breaking changes and rerun the external conformance suite.
  The relevant upstream fix is [s3s#629](https://github.com/s3s-project/s3s/issues/629).
- **Implement cached-mode `CopyObject`.** It currently returns `NotImplemented`; durable mode is the
  only implemented copy path.
- **Implement flexible checksums.** Validate and persist single-part plaintext checksums inline,
  then add multipart checksum-of-checksums behavior. The checksum cases in `s3s-e2e` remain
  intentionally deselected.
- **Restore multipart upload state after cache loss.** Namespace restore reconstructs completed
  objects but not the per-part records needed by `ListParts` and `CompleteMultipartUpload`.
  `ListMultipartUploads` still sees the remote upload, but it cannot be completed through Hypha
  after its cache records are lost.
- **Decide whether to replace restart recovery for mid-life cache loss.** The current volume watcher
  halts the process because a ready bucket may already have served false 404s. Restart then selects
  namespace restore. In-place recovery remains deliberately deferred.
- **Resolve the v2 LIST restore-flip token boundary if required.** A page read while `Restoring`
  carries the remote's opaque continuation token; after the bucket becomes `Ready`, the next page
  may send it to the cache. Target backends encode key positions compatibly, and a failure is
  retryable. Hypha-owned continuation tokens would remove that assumption.
- **Make `serving.offload_threshold` effective or remove it.** The setting is accepted today, but all
  codec bridges use `spawn_blocking`.

### Additional verification

- Add stock `rage` interoperability coverage after stripping Hypha's trailer.
- Run a real zero-loss client, currently expected to be ZeroFS, against the durable endpoint.
- Curate Ceph's `s3-tests` to the implemented surface: basic objects, LIST v1/v2, multipart,
  conditions, and ranged reads. Versioning, ACL, lifecycle, CORS, SSE configuration, and object-lock
  families remain out of scope rather than expected failures to fix.

## Stage 6 — active-passive fencing

Status: **not started**.

Stage 6 adds `hypha-fence` and turns the single-process data plane into a two-replica
active-passive service. Exactly one replica may serve or run active background duties. The passive
keeps connections warm but owns no mutable data-plane state.

### Required mechanism

- Run two Hypha pods with stable StatefulSet pod identities. Fencing must select immutable pod-name
  identities; it must not depend on relabeling an unreachable pod.
- Run two leader-elected fencing-controller replicas.
- Maintain an active lease and the invariant that only the lease holder is admitted to the cache
  and remote path.
- Implement failover in this order:
  1. detect missed lease renewal;
  2. remove the old active from the backend allow policy;
  3. wait until the policy revision is confirmed on the cache endpoints;
  4. reset the old identity's established connections and wait the configured settle interval;
  5. promote the passive.
- Never promote when the fence cannot be programmed and confirmed. Controller unavailability may
  delay failover but must never create two writers.
- Graceful handoff must finish the existing data-plane drain and clean-marker seal before releasing
  the active claim.

The cache-side fence is the load-bearing exclusion because cached writes cannot commit without it.
The remote leg may be weaker when source-enforced egress survives a partition or is obscured by
SNAT. The remaining exposure is a fenced old active finalizing an already in-flight multipart
commit. If that risk is unacceptable, Stage 6 must use per-replica remote credentials that the
controller can revoke.

### Configuration and observability

Add configuration for stable identity selectors, lease timings, fence-confirmation timeout,
connection-drain behavior, and settle delay. Export the current role, failover count, and
fence-confirmation latency. A passive must remain health-checkable and ready for promotion without
being admitted as an active writer.

### Exit requirements

An automated two-replica partition harness must prove:

- the old active is refused by the backend before the new active can write;
- an in-flight request cannot commit outside the confirmed drain window;
- inability to confirm the fence prevents promotion;
- graceful handoff seals obligations before promotion;
- recovery after forced failover preserves every acknowledged durable write and only the documented
  cached-mode loss window remains.

## Stage 7 — packaging and production installs

Status: **not started**.

Stage 7 delivers the cluster-admin-installed `hypha/` Helm chart:

- the two-pod serving StatefulSet, Service, and HTTPRoute;
- the leader-elected `hypha-fence` deployment and its least-privilege RBAC;
- references to the master-passphrase and remote-credential Secrets;
- mode, backend, bucket-prefix, GC, reconcile, fencing, and shutdown settings;
- dashboards and alerts for request health, cache hits, pending work, recovery, GC pressure, role,
  failovers, and fence confirmation.

Install the chart twice with distinct deployment prefixes or remote accounts:

- cached mode at `s3.internal.haustorium.net`;
- durable mode at `s3-direct.internal.haustorium.net`.

The pod termination grace period must cover the fixed request-drain, obligation-seal, and actor-drain
budgets plus any `preStop` delay.

### Exit requirements

- `helm lint hypha` and rendered-manifest validation pass.
- The full workspace, SeaweedFS cache-contract suite, exhaustion suite, and `s3s-e2e` supported
  selection pass against the packaged deployment.
- The Stage 6 partition and graceful-handoff harness passes with the charted resources.
- Both cached and durable endpoints are live behind the shared Gateway.
- A real client completes an end-to-end write/read/restart exercise against each endpoint, including
  the ZeroFS durable-endpoint check.
- Dashboards expose pending-marker growth, dirty drains, cache pressure, invariant halts, role, and
  failover/fence health before either endpoint is considered production-ready.
