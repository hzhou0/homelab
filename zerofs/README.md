# homelab-zerofs

ZeroFS as a CSI driver: a leader/standby gateway pair holds one POSIX filesystem inside an S3 bucket,
and the driver hands out directories of that filesystem as PersistentVolumes mounted over 9P.
**Installed by cluster-admin**, into a namespace of its own.

## Why its own chart

Foundational, same rationale as TopoLVM and Cilium: a cluster-scoped CSI driver whose node plugin
runs privileged, opens `/dev/fuse`, and mounts into the kubelet's tree. Installing that takes
cluster-admin, so it lives outside the tiers the autonomous operator governs.

## Why a second CSI driver

TopoLVM carves logical volumes out of a node's own NVMe: fast, and reachable only from that node.
ZeroFS volumes are directories on a filesystem that lives in object storage, so any number of pods on
any number of nodes mount the same one, and a rescheduled pod finds its data wherever it lands.

The two are complements. Everything latency-bound — databases, etcd, the SeaweedFS volumes
themselves — belongs on TopoLVM. ZeroFS holds shared and durable state, reading through a local cache
in front of a remote object store.

## The gateway pair

ZeroFS is single-writer per storage path, so the driver shares one gateway across every volume of its
StorageClass. Availability comes from a standby: the leader ships writes before acknowledging them,
and on heartbeat silence the standby claims a durable marker, epoch-fences the old writer through the
object store, and takes over. Published mounts block through the gap and resume by inode id, so a
failover costs a pause and keeps the mount. Two nodes, one writer, always — this buys availability
rather than write throughput.

Both halves are configured identically apart from their identity and their bootstrap role hint, and
both name the same storage URL and encryption password: they are two views of one filesystem. The
configured role is a hint, and at startup each node asks its peer who is leading, so a restarted node
defers to a live leader.

Readiness deliberately probes the replication port rather than 9P. A standby binds replication early
to receive ships and answer heartbeats, and binds 9P once it takes over; probing 9P would drop the
standby from its Service and cut off the stream it exists to consume.

## Trust boundary

Every one of the gateway's ports is unauthenticated, exactly like NFS with `AUTH_SYS`: a 9P attach
name provides namespacing, and the admin RPC runs every call as uid 0 with the power to delete any
volume. Reachability is therefore the entire security model, which makes the chart's
CiliumNetworkPolicy load-bearing rather than defence in depth — it is what confines the filesystem
root to the node plugins. Data is encrypted before it reaches the object store under a key wrapped by
the encryption password, so the bucket's own credentials read ciphertext.

## Credentials

Credentials live in a Secret created before the release, which keeps every one of them out of values
files and the release history. The rendered config references them as `${VAR}` and ZeroFS expands
them at start, so a rotated key reaches a gateway only when its pod restarts.

The encryption password wraps the storage key rather than being it, so `zerofs change-password`
rotates it without rewriting data. Losing it loses the filesystem, and the Secret must always match
what the store already expects.

## Storage backend

The install requires a storage URL: there is no backend worth guessing. ZeroFS fences writers with
put-if-not-exists, so the backend must reject a conditional PUT against an existing object — that
rejection is the fence. A store that answers success instead needs a Redis to arbitrate, or both
halves of the pair can come to believe they lead. Verify this before pointing the chart at a new S3
implementation.

Whatever tier is chosen must be hot and instant-access. ZeroFS reads the manifest, the SSTs and
segment objects continuously, so an archive class makes the filesystem unreadable rather than merely
slow, and an infrequent-access class usually costs more than the hot one.

## Install

```sh
kubectl create namespace zerofs
kubectl -n zerofs create secret generic zerofs \
  --from-literal=encryptionPassword=... \
  --from-literal=accessKeyId=... \
  --from-literal=secretAccessKey=...
helm install zerofs zerofs -n zerofs
```

Nodes must expose `/dev/fuse`: the node plugin mounts it from the host to publish a volume.

## Rebinding after a lost control plane

A volume is a directory named by the PersistentVolume's handle, and the filesystem records nothing
about which workload owned it. The bucket therefore survives a destroyed control plane intact but
anonymous. The binding is the sole record of which claim held which directory, and stating it takes
both objects: a claim in a manifest is a request for storage, while the identity of the volume
answering it is assigned at bind time and lives only here.

Both are exported verbatim, because a backup that decides in advance which fields matter can be wrong
about it silently, long before anyone looks. The subtractions belong on the way back in, where each
one is a deliberate choice. Restoring is a script run by hand, as whoever invokes it, for the same
reason: reattaching is something a cluster being rebuilt does on purpose.

Order matters twice over — volumes before claims, and both before the workloads return. Applied
first, each volume already reserves the claim about to appear, and the workload adopts the directory
holding its data. Applied afterwards, that claim receives a freshly provisioned empty directory and
the real one is orphaned. The claims' namespaces must exist by then.

Restoring assumes the directory survives, which a retaining reclaim policy guarantees, and that the
release kept its name and namespace: the gateway addresses live in each volume's attributes, so
either rename invalidates every exported binding at once.

## Operating limits

- **The filesystem quota is the only ceiling.** It covers every volume at once, so a PVC's requested
  size is a label.
- **The node plugin owns that node's mounts.** The FUSE clients are its children, so an upgrade takes
  them down with it and proceeds node by node, drain first.
- **Checkpoints cover the whole filesystem.** Snapshots are filesystem-wide rather than per volume.
- **Volumes mount as filesystems.** A workload needing a fixed-size disk creates an image file inside
  its volume.
- **StorageClass `mountOptions` are rejected outright**, so a mount option has to reach the gateway
  another way.
