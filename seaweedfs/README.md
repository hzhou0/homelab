# homelab-seaweedfs

SeaweedFS for the homelab, **installed by cluster-admin** into `seaweedfs`. Wraps the upstream
`seaweedfs` chart and runs it as **hypha's hot cache tier**: no redundancy, tuned for latency, with
master/volume/filer data on node-local NVMe via the `topolvm-provisioner` StorageClass (the
`homelab-topolvm` chart). It exposes the SeaweedFS **S3 API** in-cluster on a
`homelab-seaweedfs-s3` Service (`:8333`).

## Loose coupling — hypha is separate

hypha reaches this store **only over S3**. SeaweedFS is not embedded in the proxy; it is just the
homelab's chosen implementation of "a caching S3 endpoint." Point hypha at any other S3 cache and
nothing here changes, and vice versa. Likewise the encrypted **remote** is any S3-compatible endpoint,
configured on hypha — not here. See `agents/operator/../../hypha/ARCHITECTURE.md`.

## Why its own chart

Foundational: it owns a `StatefulSet` and node-scoped `PVC`s and depends on a cluster StorageClass, so
it can't be an operator-deployed `tool-*` release. It lives in its own `seaweedfs` namespace.

## No redundancy, by design

`replication: "000"` and `enableReplication: false`. There is no local second copy — durability is
hypha's continuous, encrypted write-through to the remote S3 endpoint. This is the same
"backups, not replication" stance the main design document takes for the whole homelab.

## Storage & scheduling

All components are pinned to `vg=nvme` nodes (`nodeSelector`) and their data PVCs use
`topolvm-provisioner`, so each lands on node-local NVMe carved from `vg-nvme`. `volume.replicas`
should equal the number of storage nodes (one volume server each); keep them in sync.

## S3 endpoint

The dedicated `s3` component gives hypha a stable Service to target. Auth is off because the namespace
NetworkPolicy fences access and hypha is the only client; enable `seaweedfs.s3.enableAuth` and supply
credentials if you widen access.

## Install

```sh
helm dependency build seaweedfs
helm install homelab-seaweedfs seaweedfs -n seaweedfs --create-namespace
```

Requires the `homelab-topolvm` chart (the `topolvm-provisioner` StorageClass) to be installed first.