# Node provisioning

Nodes are **not** built by hand. Each role has an Alpine ISO that self-provisions: boot it on bare
metal, and the node partitions its disk, installs Alpine, joins the cluster, and configures its
storage without an operator at the console. This file covers what the ISOs don't do — building them,
and the post-join steps that need a `kubectl` against the control plane.

## How a node comes up

`build.sh` bakes two scripts into every image's apkovl. Only phase 1 sits in `/etc/local.d`; phase 2
rides along as `/etc/bootstrap.start` and is moved into `local.d` on the installed root, so the ISO
never runs both at once:

| | Script | Runs | Does |
|---|---|---|---|
| Phase 1 | `10-provision.start` | from the ISO ramdisk | partitions the install disk, installs Alpine to disk, copies the role's phase-2 script into the installed root, reboots |
| Phase 2 | `20-bootstrap.start` | from the installed root, once | installs k3s, sets up node-local storage, marks itself done |

Phase 1 installs to the node's NVMe if it has one and to `/dev/sda` otherwise, and records the disk
and its LVM partition in `/etc/node-disk` / `/etc/node-lvm-part` so phase 2 doesn't have to guess.

Phase 2 is guarded by `/etc/local.d/20-bootstrap.done` and is **not** re-runnable — its `pvcreate` /
`vgcreate` / `zpool create` steps assume virgin storage and abort under `set -e` against a volume
group or pool that already exists. To rebuild the k3s layer on a live node, run the k3s parts by hand
rather than re-running the script.

Progress lands in `/var/log/provision.log` then `/var/log/bootstrap.log`; both phases detach from the
`local` service, so a console that returns to a prompt means nothing on its own.

A freshly provisioned node is `NotReady` and stays that way: the bootstrap disables flannel, and the
CNI arrives only when a cluster-admin installs the `cilium/` chart.

## Roles

| Role | Hostname | Disk layout | Storage |
|---|---|---|---|
| `k3s-server` | `k3s-server` | boot / root / third partition | `vg-nvme` LVM VG on the third partition, backs TopoLVM |
| `k3s-compute` | `k3s-compute-<6hex>` | boot / root / third partition | `vg-nvme` LVM VG on the third partition, backs TopoLVM |
| `k3s-compute-spot` | `k3s-compute-spot-<6hex>` | boot / root (whole disk) | none — reclaimable, holds no cluster data |
| `k3s-db` | `k3s-db` | boot / root (whole disk) | ZFS `raidz` pool `tablespaces` over `sda`–`sdd`, mounted at `/mnt/tablespaces` |

Compute roles can have many instances, so their hostname takes a 6-char hash of the DMI product UUID
to stay unique; the singleton roles keep plain names. The control plane is **`10.0.0.22`** via DHCP
reservation — agents dial that hardcoded, and it's also `k8sServiceHost` for Cilium's kube-proxy
replacement, so changing it means changing both.

## Build

```sh
K3S_TOKEN=<pre-shared token> ./build.sh [k3s-server|k3s-compute|k3s-compute-spot|k3s-db|all]
```

Needs privileged Docker; ISOs land in `output/`. The token is a **pre-shared** value baked into the
image at `/etc/k3s-token`, not the server-generated one — the server is installed with it and agents
present it, so the same token works for every role and survives a control-plane rebuild. Treat the
ISOs as secret-bearing.

## After a node joins

Neither phase can label or taint a node — those need the API. From the control plane:

| Label / taint | On | Why |
|---|---|---|
| `vg=nvme` | server, compute | TopoLVM's lvmd/node DaemonSets and SeaweedFS select on it. The server bootstrap applies this to itself; compute nodes need it applied manually. |
| `gvisor=true` | every node running gVisor | `utils/gvisor-runtime.yaml`'s RuntimeClass `nodeSelector` |
| `node.kubernetes.io/capacity-type=spot` | spot nodes **only** | Anti-affinity in `cilium`, `platform`, `cert-manager`, `monitoring`, `opnsense-operator` keeps singletons off reclaimable capacity |
| `node-role=database` + taint `workload=database:NoSchedule` | db | Reserves the node for database workloads |

`capacity-type` is the one that fails silently. It's an *exclusion*, so omitting it doesn't error —
the control plane and every singleton just quietly become schedulable onto a node you intend to
reclaim.

**gVisor** is not in the images. Install it per node with `utils/gvisor.sh`, which drops `runsc` +
the containerd shim into `/usr/local/bin`, registers the runtime via containerd config template, and
restarts k3s. It does everything node-side and nothing cluster-side, so the `gvisor=true` label above
is yours to apply from the control plane once the node is back.

## Networking

All nodes run **dhcpcd**, which brings up whichever interface has a carrier, so NIC names don't have
to be known ahead of time; `/etc/network/interfaces` manages loopback only and DNS comes from the
DHCP lease. Only the control plane's address is pinned (reservation, above).

The images set `net.ipv6.conf.{all,default}.accept_ra=2`. The `2` is load-bearing: k3s and Cilium
enable IPv6 forwarding, and a forwarding host ignores router advertisements at the default `1`, which
would leave nodes without the SLAAC GUA that the dual-stack setup depends on.

The cluster is dual-stack — see `migration/DUAL-STACK-MIGRATION.md` for the addressing plan and the
CIDRs baked into the server's `INSTALL_K3S_EXEC`. Both CIDRs are fixed at server init and can only be
changed by destroying and rebuilding the cluster.

## Decommissioning a spot node

A spot node is meant to be disposable — it holds no cluster storage and can be moved to another host,
where dhcpcd picks up whatever port has a link.

```sh
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data
kubectl delete node <node>
# on the node:
rc-service k3s-agent stop && rc-update del k3s-agent default
```
