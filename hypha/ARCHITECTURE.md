# Hypha — caching + encrypting S3 gateway

Hypha is an S3 gateway that puts a hot plaintext cache in front of an encrypted durable store. It
speaks the full S3 API to its clients — conditional writes, multipart, ranged reads — keeps a
plaintext copy in a **cache S3 endpoint** as the working tier, and mirrors an encrypted copy to a
**remote S3 endpoint** for durability. Hypha connects to both through their ordinary S3 APIs. When the
cache fills, it evicts the coldest object bodies and leaves tombstones for objects that remain on the
remote.

```
   S3 clients
             │  S3 API (+ conditional writes)
             ▼
        ┌─────────┐        plaintext (S3)     ┌───────────────────────┐
        │  hypha  │ ─────────────────────────▶ │ cache S3 endpoint     │
        │ (Rust)  │ ◀───────────────────────── │  hot · working tier   │
        └─────────┘                            └───────────────────────┘
             │
             │  ciphertext (age v1), write-through + GC eviction
             ▼
   ┌────────────────────────────┐
   │ remote S3 endpoint         │  cold · durable · encrypted
   └────────────────────────────┘
```

## Design

Six commitments explain most of what follows.

### 1. Facts live in the object

Every durable fact about an object sits *in that object*, in an authenticated trailer at its tail. The
object is self-describing, so the remote stores the complete durable state.

Memory works the same way. Per-key lock tables hold **weak** references, so an entry disappears when
its last guard drops. State that must exist is recomputable. The pending-upload set is an index over
work and can be rebuilt by diffing the cache against the remote. Losing it adds recovery latency.

The cache is disposable. It holds only state implied by the remote, so Hypha can serve from the remote
while it rebuilds an empty cache.

### 2. Every ack bounds its loss window

Every write path answers one question: *if the process dies exactly here, what does the client already
believe?* Ack ordering follows from the answer.

An obligation is handed off **after** its commit. Queues are unbounded and handler-side senders are
weak, so acknowledgements do not wait on queue delivery. When the mirror falls behind, Hypha refuses writes
with `503 SlowDown`: the lag window is a budget. Durable mode serves clients that tolerate no window
at all, by moving the commit point to the remote.

Loss windows are stated numerically wherever they exist.

### 3. Hypha owns the S3 contract

Clients get the semantics S3 promises, whatever the backends beneath guarantee.

A conditional write holds one lock across **resolve → evaluate → commit → hand off the marker**. That
single linearization point gives concurrent compare-and-swap writers a serial order. A read against a
restoring namespace holds a ticket for the whole remote answer, so a write admitted meanwhile cannot
supersede what the read is about to report.

Error codes follow *what a client may conclude*. An absent bucket is `NoSuchBucket`: definitive,
cacheable. A bucket whose delete has yet to decide is `OperationAborted`: retryable, because that
delete may still fail and a cached 404 would be wrong.

### 4. A broken invariant halts the process

Invariants are a closed enum. Each variant arrives with the check that detects its violation: a remote
trailer that fails to authenticate, a plaintext body appearing during a restore, an eviction tombstone
whose remote object has vanished, a live bucket whose cache projection changed underneath it.

A violation writes a halt marker durably and exits with a distinctive code. Later starts wait for an
operator to clear it before serving.

Detection is designed in. A bucket's ready classification is derived once at startup and held for the
whole run, so a vanished marker registers as a disagreement.

### 5. Lock occupancy is evidence

A cheap probe provides the required coordination state.

A lock-free read that finds a leftover transition mark tries the write lock without blocking. Failure
is the answer: a held lock means the writer that placed the mark is alive mid-bracket, so the mark
stands. The reconcile sweep uses the same trick to coalesce redundant same-key uploads onto whichever
is already in flight.

The multipart create lock inverts the usual reader/writer roles. Every in-flight create holds it
**shared**; only the orphan sweep takes it **exclusively**. Creators run concurrently. An exclusive
acquisition confirms that no create is in flight, which separates an abandoned upload worth reclaiming from
a live one that has yet to write its record.

Lock identity is fully qualified: bucket rides every key, so the same key name in two buckets stays
independent.

### 6. Speed comes from doing less

Performance comes from fewer round trips and passes, arithmetic in place of I/O, and structural
backpressure. See *Performance* below.

## Two modes

A deployment runs in one of two modes, differing only in *when* a write becomes durable on the remote
and whether the cache retains bodies.

|  | `durable` | `cached` |
|---|---|---|
| PUT commit point | encrypted remote PUT | plaintext cache PUT |
| DELETE commit point | remote delete | cache absence |
| acknowledgement | after remote commit | after cache commit |
| cache body after remote durability | eviction tombstone | retained until GC |
| remote propagation | synchronous | pending marker + reconcile |
| cache-volume loss | no acknowledged data loss | unreconciled writes may be lost |
| tombstoned GET | remote decrypt | remote decrypt, then async rehydrate |

Durable mode still depends on the cache, which remains its namespace and ETag authority; the only
guarantee it trades away from cached mode is exposure to the lag window. Multipart completion, bucket
create, and bucket delete are always remote-durable in both modes.

Run the same binary more than once to create independent tiers. Each deployment
prepends its configured **bucket prefix** to every client bucket name and strips it back off on
`ListBuckets`, so deployments sharing one remote account occupy disjoint bucket namespaces and cannot
interfere. Keys within a bucket pass through verbatim.

While a bucket namespace is being restored, reads use the remote and *all* writes take durable
semantics — otherwise a cached acknowledgement would land in a namespace readers are deliberately
ignoring.

## Storage model

Every client bucket maps to three backend buckets:

| Backend bucket | Endpoint | Contents |
|---|---|---|
| `<prefix>-d-<bucket>` | cache | plaintext bodies or tombstones at client keys |
| `<prefix>-m-<bucket>` | cache | facts twins, pending markers, multipart records, recovery markers |
| `<prefix>-r-<bucket>` | remote | encrypted, self-describing client objects |

`<prefix>-g` is a deployment-wide cache bucket for GC state.

The cache is the steady-state namespace and client-ETag authority in both modes. The remote is the
durable store and recovery authority. **The remote is authoritative for bucket existence.**

### Cache representation

The data projection holds only client keys, in one of four states: a plaintext body; an **eviction
tombstone** for a remote-resident object; a **transition tombstone** while a durable mutation is
unresolved; or an empty projection, for a deleted key.

Tombstones have fixed 16-byte bodies, so a LIST classifies one by `(size, ETag)` with no HEAD.
Eviction-tombstone metadata carries plaintext length, client ETag, checksum, original modification
time, content type, storage class, and client metadata; transition tombstones carry only their kind,
because the remote is authoritative until they settle. A client PUT whose body equals the internal
sentinel is rejected, so that classification cannot be spoofed.

The meta projection divides its keyspace into non-overlapping ranges:

```text
A:  0x01 0x01 <tag> ...       multipart, recovery, recency, and shadow records
B:  0x01 <client-key> ...     LIST facts twins
C:  <client-key>              pending PUT/DELETE marker
```

**Facts twins** (range B) are zero-byte objects whose key is the client key plus a low-sorting suffix
encoding the object's LIST-visible facts — so the facts arrive in the same LIST page as the entry they
describe, and a listing costs one round trip however much of the namespace is tombstoned. A twin is
omitted when its derived key would exceed the backend key limit; LIST falls back to HEAD there, and
tombstone metadata stays authoritative either way.

Client keys are limited to 1024 bytes and exclude `0x00` and `0x01`, which lets the suffixed twin sort
next to its key and fit within the backend key limit. Other control and non-ASCII bytes are supported
through `encoding-type=url`.

Rehydrated single-part plaintext replaces its eviction tombstone at the client key. Rehydrated
multipart plaintext uses a generation-tagged **shadow** object in range A instead, so the tombstone
and its composite facts stay intact.

Client `x-amz-meta-*` values and storage-class labels are cache-resident: they survive ordinary
tiering but not cache-volume loss. `Content-Type` additionally rides the remote object and is
restored.

## Consistency

Hypha presents linearizable per-key semantics on top of two backends with weaker guarantees.

**Conditional writes.** `If-None-Match: *` is an atomic create; `If-Match: <etag>` is a
compare-and-swap update. Both take the per-key write lock across resolve, evaluate, commit, and marker
hand-off, making that span the linearization point. Unconditional cached PUTs deliberately skip the
lock and use the backend's last-writer-wins behaviour, since they assert nothing about prior state.

**Read-after-write.** A committed write is visible to the next read, in every mode and every recovery
state:

- In cached mode the cache write *is* the commit, and reads consult the cache first — visibility is
  immediate by construction.
- In durable mode the commit is on the remote, and the key is marked in-transition for the whole
  bracket, so readers resolve it from the remote rather than from a cache entry that has not settled
  yet. A reader never observes a torn intermediate.
- During a restore, reads resolve remotely and hold a ticket for the entire answer, while writes are
  forced durable. A cached-mode write admitted concurrently cannot commit into a namespace the read is
  ignoring.
- A `DELETE` in durable mode keeps the object readable from the remote until the delete actually
  lands, so an unacknowledged delete is never visible.

**Negative results have explicit authority.** A key absent from the cache is
authoritatively a 404 only while the **sync marker** is present — the reserved object recording that
reconciliation has made the cache namespace authoritative. Without that marker Hypha reads through to
the remote until reconciliation restores it.

**Error codes preserve the difference between "no" and "not yet."** `NoSuchBucket` is definitive.
`OperationAborted` says a delete is in progress and may still fail, so retry rather than conclude.
`503 SlowDown` says the write was refused, not lost.

## Encryption

One symmetric master secret — a 256-bit random string used as an age passphrase — is supplied out of
band; losing it renders the remote copies unrecoverable.

The wrap uses a symmetric passphrase rather than a public-key KEM. The remote keeps ciphertext
indefinitely, so protection against harvest-now-decrypt-later attacks must apply from the first byte.
age's native PQ stanza adds about 1.6 KiB to every file header, which is costly for namespaces with
many small objects.

The envelope is [age v1](https://age-encryption.org/v1) with its native scrypt recipient, used
entirely stock: per-chunk authentication, seekable decryption for range GET, and splice/truncation
detection via a finalizer chunk are all already there. Disaster recovery is any age binary plus the
passphrase.

The **scrypt work factor is pinned to 1.** That KDF's stretching exists to protect low-entropy human
passphrases; hypha's is full-entropy, so security lives in its 256 bits, not the work factor. At age's
default — auto-tuned to ~1 s and ~256 MiB *per file* — the wrap would dominate every small-object
operation.

Rotation is a flag day: the age spec requires an scrypt stanza to be a file's only stanza, so there is
no multi-recipient lazy re-wrap. Rotation cannot retroactively protect ciphertext that an attacker has
already collected.

**Only bodies are encrypted.** Key names and metadata remain plaintext on the remote, as with any S3
client-side-encryption client does; the provider sees names and sizes, never contents. The cache
stores plaintext throughout, which is what keeps the hot path fast.

### What the age format gives us

age chunks plaintext into fixed **64 KiB chunks** (65536 plaintext bytes + 16-byte Poly1305 tag =
65552 ciphertext bytes), each independently authenticated under a key derived from the file key, with
nonces derived *deterministically* from the chunk index. This provides three properties:

- **A range GET is a range GET.** A plaintext range maps to a contiguous ciphertext range:
  `chunk_index = floor(plaintext_byte / 65536)`, ciphertext offset `chunk_index * 65552` plus the
  per-file header and payload-nonce offset. `hypha-format` pins the scrypt header length, making the
  conversion closed-form in both directions — the mapping is arithmetic, never a lookup.
- **Parts are independent.** Each object, and each multipart part, is its own age file with its own
  random file key and its own nonce space starting at chunk 0. Two parts uploading in parallel, a
  re-uploaded part, two concurrent PUTs to one key: none need to agree on anything, because none share
  a key or a nonce space. The per-file key is what removes coordination.
- **Splicing, truncation and reordering are all detected** — key separation catches a chunk moved
  between objects, chunk-index-in-nonce catches reordering, the finalizer chunk catches truncation.

The age reader is EOF-delimited, so the decrypt bounds derived from the object length are
load-bearing: the trailer must be excluded from the ciphertext window, or the final chunk fails to
authenticate.

### The object trailer

Every completed remote object ends with a versioned trailer:

```text
single-part: age-file | facts | mac | version
multipart:   age-file... | part-end-offsets | facts | mac | version
```

The facts carry object kind, plaintext length, client-visible modification time, the raw MD5 behind
the client ETag, an optional plaintext flexible checksum with its algorithm and type, and the client
part count. For composites the cumulative ciphertext offsets define the encrypted window of every
client part; each part's plaintext length follows from its ciphertext length in closed form. The
client ETag is the plaintext MD5 for a single-part object and `md5(concat(part-md5s))-N` for a
composite.

A truncated HMAC-SHA256 tag derived from the master passphrase binds the facts, the part table, the
body length, the format version, **and the object key** — so a valid object cannot be relocated to
another key. An object without a valid trailer is foreign or corrupt, and trips the halt path rather
than being ignored.

## Multipart upload

Multipart **routes parts around the cache** in both modes: parts aren't individually readable until
completion commits the composite, multipart traffic is throughput-bound rather than latency-bound, and
routing parts through the cache would impose S3 multipart plumbing on it for nothing. Hypha proxies
the ops onto the remote's **own native multipart upload** at the same key, adding streaming encryption
per part.

- Each `UploadPart` becomes an age file with a fresh file key, streamed to the remote as that upload's
  native part. The part's plaintext MD5 is computed inline while encrypting, and per-part facts
  accumulate in the upload's record. Hypha stores part facts only in the upload record.
- Parts may arrive out of order, in parallel, or be re-uploaded — a re-upload is just a new age file
  with a fresh key. A repeated part number is resolved at completion by the client-returned plaintext
  ETag.
- **`CompleteMultipartUpload` is simultaneously the durability commit and the facts carrier.** Hypha
  composes the S3-correct composite ETag, lands the trailer as the object's final bytes, and completes
  the upload on the remote, which concatenates the parts into one object at the key. The completed
  object requires no later reconciliation.
- The trailer normally rides its own part above every client part. But S3 exempts only the *last* part
  from its 5 MiB minimum, so when the client's last part is itself under that, appending a separate
  trailer part would demote the small part to an illegal non-final one. Instead the trailer is **folded
  into it** — the part is re-uploaded as `part ‖ trailer`, streamed rather than buffered. The committed
  object is byte-identical either way. A persisted fold intent lets a retry restore the pure part
  before attempting another completion.
- Each part's plaintext is capped just under S3's 5 GiB `UploadPart` max: the framed part must fit
  under it, and framing costs a 165 B header, 16 B per 64 KiB chunk (~1.25 MiB at this size), plus
  room for the composite trailer a terminal part may have to carry.
- Completion also writes a tombstone at the composite's key, replacing any stale cached body and
  keeping the namespace complete for LIST. In cached mode the composite becomes cachable on first
  read, through the same rehydrate path tombstoned bodies use.
- `AbortMultipartUpload` maps to the remote's native abort; abandoned uploads are reclaimed by GC.

## S3 surface

| Family | Operations |
|---|---|
| Buckets | `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListBuckets`, `GetBucketLocation`, `GetBucketVersioning` |
| Objects | `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects`, `CopyObject` |
| Listing | `ListObjects`, `ListObjectsV2` with prefix, delimiter, pagination |
| Attributes | `GetObjectAttributes` for ETag, size, storage class, multipart part sizes |
| Multipart | `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads` |

Also implemented: SigV4 against one client access-key pair; plaintext ETags, `Content-MD5`, and
flexible checksums (single-part CRC32/CRC32C/CRC64NVME/SHA1/SHA256, multipart composite checksums and
full-object CRCs); byte-range GETs; copy-source ETag and time preconditions; destination `CopyObject`
ETag preconditions; generation-bound copy-source reads through the same cache/remote view as other
reads; client metadata, `Content-Type`, and non-archive storage-class labels; up to 1,000 keys per
`DeleteObjects`; part numbers 1–10,000 with S3's ordering and minimum-size rules.

`CopyObject` handles up to 5 GiB: live sources copy atomically within the cache, while remote-resident
sources use an internal multipart copy of the encrypted body plus a fresh destination-bound trailer
and mtime, preserving source ETag/part geometry and unchanged checksums. Selecting a *different*
checksum restreams the source plaintext to derive it. `UploadPartCopy` always restreams the exact
plaintext range through decrypt, checksum, encrypt, remote part upload, and a matching durable record.

Surface limits:

- `PutObject` and `UploadPart` require `Content-Length` and accept just under 5 GiB of plaintext —
  the remote's 5 GiB per-PUT limit applies to the *framed* body, so the cap is that limit less age's
  per-chunk tags and the trailer the leg may have to carry;
- bucket versioning is always reported disabled; versioned operations are not implemented;
- ACL, policy, lifecycle, CORS, tagging, object lock, retention, archive restore, replication
  configuration, and SSE configuration are outside the surface.

## Data path

- **PUT** — cached mode writes plaintext to the cache and acks; that write *is* the commit. A pending
  marker is then handed to the marker queue, never written inline, because the ack must not depend on
  it. Durable mode encrypts and uploads straight to the remote — that upload is the commit — with the
  key marked in-transition throughout, then settles the cache tombstone and acks.
- **UploadPart** — routed around the cache in both modes: encrypt and stream straight to the remote as
  a native part, ack once the remote confirms.
- **GET** — a local body serves from the cache. A tombstoned key (or a body lost to node failure)
  fetches the covering age chunks from the remote, authenticates, decrypts, and streams to the client;
  cached mode also **rehydrates the body locally and bumps its recency position**. Durable mode never
  rehydrates — the body would be tombstoned again immediately.
- **HEAD / LIST** — served from the cache while its sync marker is present, reading plaintext sizes and
  client ETags off facts twins for tombstoned keys and cached composites. Listing the remote instead
  requires a bounded per-entry trailer fan-out.
- **DELETE** — cached mode removes the local body, acks, and queues a DELETE marker; the sweep HEADs
  the remote object and conditionally deletes that exact generation before clearing the marker.
  Durable mode makes the remote delete the commit, keeping the key readable from the remote until it
  lands.
- **Buckets** — the remote is their source of truth and they are always durable in both modes. Create
  and delete are synchronous to both sides, acked only once both confirm. `ListBuckets` is answered
  from the remote; `HeadBucket` from the process's own bucket map, the only source that distinguishes
  a deleted bucket from one whose delete has not yet decided.

## Locks and concurrency

Hypha runs as one serving process. Correctness rests on four keyed lock tables plus per-key
conditional writes.

The tables share one implementation: a sharded map of **weakly-held** async locks. An entry is created
on first acquisition and removed when the last guard drops — evaluated under the shard lock, with a
pointer check so a newer epoch installed under the same key is never evicted by an older guard's drop.
Idle keys leave the table automatically. Contending for
an already-held key borrows the table's own key rather than allocating, so a hot key costs a refcount
per acquisition, not a string.

| Table | Kind | Serializes |
|---|---|---|
| write | exclusive | conditional writes, durable brackets, multipart completion, eviction, rehydrate |
| upload | exclusive | same-key reconcile work, kept off the client write path |
| mpu-part | exclusive | replacement or folding of one part of one upload |
| mpu-create | shared/exclusive | creators share; only the orphan sweep excludes |

Separate table *instances* — not just separate keys — are what keep a reconcile upload from
serializing a client's conditional PUT on the same object.

**Occupancy as evidence.** `try_lock` failures provide state information. A read that finds a leftover
transition mark probes the write lock: if it is held, the writer is alive mid-bracket, so there is
nothing to repair and the read proceeds without queueing. The reconcile sweep coalesces same-key
uploads onto whichever is in flight the same way, so the upload table never accumulates waiters.

**The inverted RwLock.** Every multipart create holds `mpu-create` shared, from *before* the remote
`CreateMultipartUpload` (the upload becomes listable the instant that call returns) until its cache
record is written. Only the orphan sweep takes it exclusively. Creators therefore never serialize
against each other, and a successful exclusive probe shows that no create is in flight. This separates
a leaked upload from a live upload whose record has not landed yet.

Durable mutations run a bracket:

```text
precondition -> transition tombstone -> remote commit -> cache settle
```

If the process dies inside it, the next access settles the cache to whatever the atomic remote
operation committed.

Reconcile deletes take both the upload and write locks and re-check that the key is still absent.
Because those locks exclude every hypha remote writer, **the remote is not required to support
conditional delete at all** — owning concurrency in-process keeps a requirement off the backend
contract. Marker removal is still an `If-Match` delete against the cache, so a stale sweep cannot clear
a newer obligation.

### The bucket admission gate

Every data-plane op crossing a bucket passes a per-bucket gate: one `AtomicU64` stored beside the
accounting in the state map's entry, so the readiness/delete word and the memos are one atomic
publication with one lifecycle.

```text
[ ready:1 | closed:1 | epoch:30 | rcount:16 | wcount:16 ]
```

`DeleteBucket` is optimistic: load the word, refuse if a write is in flight, check the client-visible
namespace is empty, then CAS-close the exact state it observed. One emptiness check, no waiting;
losers get the retryable `OperationAborted`.

The subtleties live in the layout. `rcount` is masked out of the close's compare, because deletes do
not wait on readers and a reader passing during the listing must not fail the close. The `epoch` makes
a write that begins *and* ends during the listing visible even with `wcount` back at zero — the ABA
case. An uncommitted close reopens on drop, rather than leaving a live bucket that refuses every op
for the rest of the run.

Writes are admitted through a CAS that classifies them simultaneously: a `Ready` bucket with no
restoring reader in flight admits a cached-mode cache-first write; everything else — `Restoring`, or
`Ready` with a reader in flight — commits durably. The readiness flip is idempotent, never reverts,
and keeps the gate its in-flight writes are counted in.

## Durability and loss windows

Every write is mirrored to the remote as it happens, so the encrypted copy is *continuous*, current to
within the async upload lag.

**The lag is a budget.** Once the pending set crosses `max_pending` markers, or the
oldest marker crosses `max_age_ms`, cached-mode PUT/DELETE/copy are refused with `503 SlowDown` — which
SDKs retry with backoff — rather than acking writes the mirror cannot keep up with. Both thresholds at
`0` disable the gate.

The count is seeded once at startup by a full census of every bucket's markers, before the listener
opens, and is exact thereafter: a create-only marker raises it once (an overwrite replaces, never
adds), the sweep's CAS clear removes it, a bucket delete drains its projection wholesale. The seed
must be published before the first recovery is queued — a rebuild raises markers through the same
counter, and the seed is a *store*, so a raise landing between the count and the publish would be
overwritten with nothing left to re-seed it.

**Obligations do not delay acknowledgements.** The marker and orphan queues are unbounded, and every
handler-local sender is weak: a handler upgrades, sends, and drops before returning. Delivery happens
strictly after the commit, so it can neither block nor fail an acknowledged write. Each run ends
either with an explicit seal message or with the channel closing — and **only the explicit seal
authorizes clean markers**, because a killed process closes the channel exactly as a graceful drain
would. Absence of evidence is treated as evidence of absence, in the safe direction.

This is **replication, not backup**: it propagates destructive operations faithfully. Whether that gap
is closed is a property of the remote bucket, not of hypha — enable versioning plus object-lock
retention there and the same write-through accumulates recoverable history; leave it off and the
remote holds exactly the current object set.

## Tiering and garbage collection

The cache is bounded storage over an unbounded remote, watched against a high-water and low-water
mark.

- Crossing the **high-water mark** starts GC with a byte target: reclaim to the low-water mark,
  evicting coldest-first. An eviction is allowed only after pending-marker exclusion, remote-generation
  confirmation, and a conditional tombstone write — a body is never dropped before its encrypted copy
  is known good.
- Recency is a **Bloom ring**: one filter per fill window, a slice rotating once enough distinct keys
  have been touched, fed by reads *and* writes but never by LIST, which would mark a whole bucket hot.
  Retired slices persist, and the ring pins its own hasher seed so a slice sealed by one process still
  means something to the next. The newest slice containing a key quantizes its last-access age. If the
  sketch is absent — first boot, or a lost cache — every key falls into one bucket and eviction
  degrades to LastModified alone: churnier for one cycle, never incorrect.
- GC runs in **both** modes, since debris is not a cached-mode concern: abandoned multipart records,
  stale twins, transition debris, orphan shadows. An in-progress remote upload whose cache record is
  gone is **aborted, never restored** — the create-lock handshake above proves it is a leak.
- Under pressure it climbs a ladder: first shorten the pass interval, then raise concurrency, then
  relax the age threshold. Only one rung moves per pass, on that pass's evidence.

**Usage source.** `internal` accounting (bytes hypha wrote) is backend-agnostic but blind to backend
overhead and uncompacted deletions; it is the default and fallback. A backend-specific source can read
real disk topology and trigger compaction. Without one, debris is still swept but bodies are never
evicted.

## Recovery and lifecycle

### Failure modes

- **Cache body loss.** Write-through means those objects still serve from the remote — a lost local
  body is indistinguishable from a tombstone at read time.
- **Whole-cache loss.** Discard it. The sync marker is gone with it, so hypha serves reads from the
  remote while reconciliation relists and rebuilds every bucket and key as tombstones, then restores
  the marker; bodies rehydrate on read. The only unrecoverable loss is the bounded set of cached-mode
  operations still inside the write-through lag. Durable mode has no such window.
- **Remote unavailable.** Hot reads are unaffected; tombstoned reads fail cleanly; uploads queue and
  retry.

### The two recovery passes

| Recovery | Trigger | Authority | Mutation |
|---|---|---|---|
| namespace restore | missing sync marker | remote | add missing tombstones and twins |
| pending-set rebuild | missing clean marker | cache namespace | recreate pending markers only |

A missing cache projection also lacks its marker, so it selects namespace restore. A new cache and a
lost cache therefore use the same recovery path.

Namespace restore is additive and idempotent; it never overwrites an entry that appeared while the
pass was running. Pending-set rebuild performs a streaming cache/remote merge and authenticates the
remote trailer of every intersecting live body, detecting missing or stale generations.

### Startup

Startup lists remote buckets and surveys them concurrently in one pass — sync marker, both clean
markers, pending-marker count. The survey **reads only**, so an unexpected error exits with the cache
unchanged and the next run resolves from the same evidence. After every bucket has answered, clean
markers are deleted, the backpressure gate is seeded, and recovery is dispatched. Serving begins after
this sequence, so the observed marker state remains consistent.

A ready classification is derived once and held for the run. Recovery is therefore restart, never
in-place repair. A volume watcher periodically re-verifies that every ready bucket still has its sync
marker.

### The halt path

Invariant violations write a halt marker to the remote, stop the server, and exit with code **86** —
chosen to be distinct from conventional statuses and from `sysexits.h`, so a supervisor can tell it
from an ordinary crash. Serving stops *before* the marker is retried to durability. A later start sees
the marker and exits before spawning any background work.

### Graceful shutdown

1. stop accepting requests and drain connections;
2. seal the run and write the pending-set clean markers, then join the startup shadow sweeps and write
   the shadow-clean evidence they earned;
3. stop taking background work, drain actor queues, join actors.

Step 2's ordering is deliberate. Shadow sweeps touch only range-A keys, while the pending set the run
seal vouches for is range C — so the seal does not depend on the sweeps finishing, though it does owe
them the shadow-clean marker. Joining first would hold the marker that decides whether the next run
rescans the entire pending set behind work whose worst case is leaked bytes. Both run concurrently
either way, so the ordering costs no wall time.

A bounded phase that times out aborts without writing evidence that would let the next run skip
recovery.

## Performance

Everything on the data path is **pull-based**. A codec is a stream the consumer drives, so a request's
pipeline memory is one age chunk in flight rather than a pipe's worth of buffer, and a dropped
response cancels the upstream read instead of leaving a task filling a buffer nobody will read. The
only exception is the ranged read, where age's seekable reader is synchronous and must run on
`spawn_blocking` behind a one-chunk bridge.

Round trips are reduced directly:

- A **single speculative suffix GET** of the maximum trailer length captures `table ‖ facts ‖ tag ‖
  version` for any object, so a composite read recovers its parts table without a second fetch. Object
  length comes from that response's own `Content-Range`.
- A whole composite decrypts in **one GET** — the concatenated parts, trailer excluded — because the
  parts table gives every part's ciphertext length up front. O(1) round trips regardless of part count.
- Plaintext↔ciphertext offset conversion is closed-form arithmetic, so ranged reads compute their
  window rather than probing for it.
- Facts twins put LIST metadata in the same page as the entries, eliminating per-object HEAD fan-out.
- One body reaching two sinks is **teed in a single pass**, not buffered and replayed.
- Digests ride the encryption pass — the plaintext MD5 is folded in as bytes stream, never a second
  traversal.

### Learnings

**Pull pipelines reduce memory and task overhead.** A push pipeline needs a task and buffer per request
and must cancel both when the client disconnects. A pull codec encrypts on the task that drives it, so
the sweep starts one task per key to distribute encryption across cores.

**Backpressure can be structural.** Inside the encrypt codec, age's sink returns `Pending` *without
registering a waker*. The sole poller owns the other end and drains and re-polls rather than waiting.
No wakeup to lose, none to manage.

**A semantic verdict can become a transport failure.** The encrypt gate withholds one ciphertext byte
until the digest verdict lands. A body that doesn't match its declared `Content-MD5` then ends *short
of its declared Content-Length*, so the backend refuses it outright. Nothing is committed and later
compensated for.

**Once headers are out, errors stop being status codes.** Mid-stream truncation or authentication
failure can only surface as a body that ends early. Log it where it happens; that is the last point
where it is still legible.

**GC pressure uses a geometric ladder.** Its interval halves between a 5-minute base and a 1-second
floor, giving more resolution where pressure changes quickly.

**Weak-valued lock tables bound idle state automatically.** Entries leave the table when their last
guard drops, so the table needs neither a sweeper nor an expiry policy.

**Speculative reads trade bytes for latency.** Reading the maximum possible trailer costs tens of
kilobytes and avoids a round trip.

## Single writer by construction

Hypha runs as **one** replica and uses no promotion protocol. Cross-object invariants are held by
in-process key locks and per-key conditional writes, so two live
processes would corrupt state in ways recovery does not cover — and exclusivity is therefore made a
property of the deployment object rather than something the process asserts about itself.

A **single-replica StatefulSet** provides it: the controller creates no replacement until the old Pod
object is fully deleted, so a partitioned node's still-running Pod blocks its own successor. A
`replicas: 1` Deployment is insufficient: taint-based eviction can mark the unreachable Pod
terminating while the ReplicaSet creates a second Pod.

`terminationGracePeriodSeconds` must cover the request-drain, obligation-seal and actor-drain budgets
plus any `preStop` delay, so a planned restart always exits clean and the next start takes the
accounted path rather than a pending-set rebuild.

**Node loss requires an explicit assertion that the node is down** before a replacement runs. Apply
the `node.kubernetes.io/out-of-service` taint manually or through a system that can verify power state.
Fencing requires a physical assertion; peer-based inference is best-effort.

## Packaging

`chart/` is a Helm chart carrying the serving StatefulSet, Service, HTTPRoute, ConfigMap, dashboards
and alerts. It references the master-passphrase, client-auth, remote and cache Secrets by name, keeping
credentials out of values files and release history.

One release can carry several deployments, as entries in the chart's `deployments` map — each with its
own process and full set of resources, and its own **distinct bucket prefix**, which is what keeps
their backend namespaces disjoint on a shared remote account.

TLS terminates upstream of the pod. Settings render to `hypha.toml` in a ConfigMap. The pod sets `enableServiceLinks: false`, which is
load-bearing: kubelet would otherwise inject `HYPHA_*_SERVICE_*` variables for every Service in the
namespace, they would land in the `HYPHA_`-prefixed config layer, and that layer rejects unknown
fields — the process would refuse to boot next to its own Service.

## Configuration

Configuration loads from `hypha.toml`, overlaid by `HYPHA_` environment variables; double underscores
select nested fields (`HYPHA_REMOTE__ENDPOINT`). The layer **rejects unknown fields**, which is what
makes stray injected variables fatal rather than silently ignored.

Required: `bucket_prefix`, `mode`, `master_passphrase`, an `[auth]` client key pair, and `[cache]` and
`[remote]` blocks each with endpoint, region and credentials. Everything else has a default. Invalid
water marks, concurrency bounds, recency parameters and bucket prefixes fail at startup rather than at
first use.

## Observability

The S3 listener emits JSON request spans — operation, bucket, key, bytes, cache-hit state, outcome,
latency — one line per request, on span close.

A separate unauthenticated admin listener serves:

- `/healthz` — process liveness. Every condition hypha can diagnose either exits the process or is
  something a restart would not fix, so liveness probes point here and never at `/readyz`.
- `/readyz` — startup/drain state plus a live remote reachability check. Reports not-ready for the
  whole shutdown, taking the process out of rotation while it can still serve what it holds.
- `/metrics` — Prometheus metrics for S3 traffic, cache hits, markers, reconciliation, uploads, GC,
  cache usage, water marks, backpressure throttling, shutdown accounting, and `hypha_startup_seconds`
  labelled `clean` or `rebuild`.

## Backend contract

Cache requirements:

- unversioned buckets without Object Lock;
- conditional `PutObject` and `DeleteObject` with **enforced** `If-Match`;
- `CopyObject` with an enforced source `If-Match`;
- `ListObjectsV2` with `encoding-type=url`;
- ordinary S3 bucket, object, range, listing and metadata behaviour.

SeaweedFS 4.37 is the tested cache. **MinIO does not enforce `If-Match` on `DeleteObject`** and is
therefore not a valid production cache for the marker and shadow CAS paths, though it remains the
default test remote.

Remote requirements:

- unversioned buckets without Object Lock;
- strongly consistent object and listing behaviour;
- ranged GET;
- native multipart create, upload, ranged part copy, complete, abort, `ListParts`, `ListMultipartUploads`;
- completion must honour the exact `(part number, ETag)` generations hypha supplies;
- ordinary `DeleteObject` — conditional delete is *not* required.

The mapped remote namespace is expected to be private to one deployment. Foreign buckets or objects
inside it are invariant violations, not things to ignore.

## Verification

`cargo test --workspace` runs the default harness: an isolated MinIO remote and cache per test, hypha
driven in-process over an ephemeral port by a real S3 client, every fixture cleaning up on drop.
`scripts/test-seaweedfs.sh` runs the same workspace against a throwaway SeaweedFS cache — the
production contract — with `scripts/test-seaweedfs-tiny.sh` for real-backend exhaustion and
`scripts/s3s-e2e.sh` for upstream `s3s` client cases inside hypha's surface. The whole workspace
passes on both backends.

The suites cover format round trips and corruption, S3 conformance, conditional-write concurrency,
multipart replacement and folding, restore and pending-set recovery, crash brackets, reconciliation,
GC and rehydrate races, backend exhaustion, injected faults, admin endpoints, tracing, and load.

## Outstanding work

Further verification wanted: stock `rage` interoperability coverage after stripping hypha's trailer; a
real zero-loss client against the durable endpoint; and Ceph's `s3-tests` curated to the implemented
surface — basic objects, LIST v1/v2, multipart, conditions, ranged reads — with versioning, ACL,
lifecycle, CORS, SSE and object-lock families out of scope rather than expected failures.

Transparent re-splitting of a client part whose framed form exceeds the remote's part limit remains a
later refinement. Until then a client pinned to exactly 5 GiB parts — `multipart_chunksize = 5GB` in
the AWS CLI, `--s3-chunk-size 5G` in rclone — must be lowered, since framing puts any such part
~1.25 MiB over the limit no matter how the cap is drawn.
