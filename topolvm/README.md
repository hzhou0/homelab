# homelab-topolvm

TopoLVM CSI for the homelab, **installed by cluster-admin** into `topolvm-system`. Wraps the upstream
`topolvm` chart and ships one StorageClass, **`topolvm-provisioner`**, that carves node-local logical
volumes out of the **`vg-nvme`** LVM volume group. That volume group lives on `/dev/nvme0n1p3` — the
partition the compute/server nodes formerly reserved for Ceph — on nodes labelled `vg=nvme`.

This is a generic node-local block-storage layer, not scoped to any particular consumer. The
`homelab-seaweedfs` chart is its current consumer, binding its master/volume/filer PVCs to this
StorageClass, but nothing here assumes that.

## Why its own chart

Foundational, same rationale as `cilium`/`cert-manager`: a cluster-scoped CSI driver whose `lvmd` and
node plugin need **privileged host access to LVM**. The autonomous operator can't install it, so it
lives in `topolvm-system`, not an `app-*`/`tool-*` namespace.

## Node scoping

`lvmd` and the node CSI plugin are pinned via `nodeSelector: vg=nvme` (`topolvm.lvmd` /
`topolvm.node` in `values.yaml`). Only server/compute nodes carry `nvme0n1p3` → `vg-nvme`; `db` and
`compute-spot` have no such partition and are deliberately excluded, so `lvmd` never starts on a node
without the volume group.

## The volume group is created out-of-band

This chart manages only the CSI/StorageClass side. The `vg-nvme` volume group itself is created on
each storage node at bootstrap (`pvcreate`/`vgcreate` on `/dev/nvme0n1p3`) and by labelling the node
`vg=nvme` — the same lifecycle the `ceph-osd=true` label had.

## Scheduling

`topolvm-provisioner` uses `volumeBindingMode: WaitForFirstConsumer` so an LV is provisioned on the
node its consumer pod is scheduled to. Placement relies on CSI **storage-capacity tracking** rather
than TopoLVM's scheduler extender (which would require patching the k3s scheduler config) — hence
`topolvm.scheduler.enabled: false`.

## Install

```sh
helm dependency build topolvm
helm install homelab-topolvm topolvm -n topolvm-system --create-namespace
```