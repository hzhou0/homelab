# homelab-neon

Neon's storage layer, the proxy, and `neon-ctl` — the control plane Neon deliberately does not
ship. Foundational: cluster-admin installs it into its own namespace, not an `app-*`/`tool-*` one,
so the platform's admission constraints do not apply and the privileged access these components
need is not something an autonomous operator can grant itself.

## What it renders, and what it does not

Only what is true at deploy time. Branches and computes are runtime objects that `neon-ctl` owns:
nothing here creates one, and the chart has no notion of how many exist. What it does supply is the
pod every compute is stamped from, published as a value rather than built into the control plane's
code, so a compute's image, resources and placement stay chart configuration even though no compute
is rendered.

## Why the pieces are shaped as they are

**One StatefulSet per pageserver and per safekeeper, not one with N replicas.** A node id is
permanent — it keys generation numbers in the controller's database, and reusing one for a
different machine reattaches its tenants. Reading it off a pod ordinal makes it a function of list
order, so it is stated instead. Per-instance placement falls out of the same decision.

**Pageservers register themselves; safekeepers are registered for them.** A pageserver reads a
metadata file beside its config at startup and sends it to the controller. Safekeepers have no
equivalent, so a hook job posts them after install, and until every one of them is posted the
controller cannot place a timeline for want of a quorum. Re-run it with `helm upgrade` if the
controller's database is ever rebuilt.

**Config lives inside the pageserver's working directory**, which is also where tenant data lives,
so an init container writes it into the volume rather than mounting it over one.

**Safekeepers are launched with `--remote-storage`.** The WAL retention horizon includes
`backup_lsn`, and `backup_lsn` advances only when the offload task has somewhere to write. Without
it no segment is ever reclaimed, the volumes fill, and writes stall cluster-wide. This is the defect
that made the off-the-shelf operator unusable, and nothing else in the system compensates for it.

**The controller is told to put timelines onto safekeepers.** That behaviour is off by default, and
with it off there is no membership, no safekeeper GUC on a compute, and no notification for the
control plane to act on — the safekeepers would run and serve nothing.

**The proxy is exposed at L4.** Clients speak the Postgres wire protocol, so it takes a Cilium LB
IPAM address rather than a route through the HTTP gateway. It reads its certificate once at startup
and has no reload path, which is why `neon-ctl` restarts it when the secret it mounts is renewed.

## The fence

Components authenticate nothing between themselves — the controller runs `--dev` — so the namespace
boundary is the entire fence, and the grant that admits the namespace to itself is what makes the
storage mesh, the notification path and the runtime-created computes reachable. It names no ports,
because a component added later would otherwise fail in a way that reads as a Neon bug. Client
traffic arrives from outside the cluster and needs no east-west grant; `accessGrants` is for
in-cluster consumers of the proxy.

## Placement

Safekeeper WAL that has not been offloaded is the one tier with no other copy, so no two safekeepers
share a node with each other, and none shares a node with the pageserver holding the layers their
WAL would be replayed into. The pageserver is the only workload that grows without bound, so it gets
the node with the most free space.

Nothing here keeps a component off a reclaimable node. That belongs to the node, not to this chart:
a node that can vanish should carry a taint.

## Install

Create the bucket on the durable gateway first — nothing here creates one, and neither Neon
component creates one lazily. Then the credentials Secret named by `secretName`, holding
`bucketAccessKey`, `bucketSecretKey` and `controllerDbPassword`; the chart creates no Secret and
takes no literal key, so nothing sensitive reaches a values file or the release history.

Pageservers appear under `GET /control/v1/node` on their own once running. Safekeepers appear under
`GET /control/v1/safekeeper` once the hook job has run. A timeline cannot be created until both
lists are populated.
