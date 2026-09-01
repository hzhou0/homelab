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
metadata file beside its config at startup and sends it to the controller. A safekeeper has no
notion of the controller at all, and the controller has no way to discover one, so something has to
assert that it exists — and until every one of them is asserted the controller cannot place a
timeline for want of a quorum.

That assertion is `neon-ctl` reading these Services rather than a job posting a list, which makes
registration a fact that re-converges instead of an event that has to be re-run: a safekeeper added
later, or a controller database rebuilt, heals on its own. The Service is the whole record — the
id, the zone, the ports and the host it resolves to are all on it, so nothing is stated twice. The
write is an upsert that leaves a scheduling policy set by hand alone, which is what makes repeating
it safe.

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

Two layers, because neither is sufficient alone.

The namespace boundary comes first: one grant admits the namespace to itself, which is what makes
the storage mesh, the notification path and the runtime-created computes reachable. It names no
ports, because a component added later would otherwise fail in a way that reads as a Neon bug.
Client traffic arrives from outside the cluster and needs no east-west grant; `accessGrants` is for
in-cluster consumers of the proxy.

Inside that boundary every storage call is authenticated with an EdDSA JWT, and the only thing
stored is the private key. The public half and every token are functions of it, so each pod derives
what it needs into an emptyDir before its main container starts, rather than being handed material
minted somewhere else. Derivation is deterministic, so pods computing it independently agree without
anything being generated, ordered, or kept in step — and rotating the deployment is replacing one
Secret value.

`neon-ctl` is the exception that needs the key itself rather than a derivative, because it is the
only component that signs at runtime: its own token for the controller, a tenant-scoped one into
every compute spec, and the signature it checks on inbound notifications.

This is what allows the controller to run without `--dev`. That flag is not a mode: it suppresses a
startup assertion, and the assertion it suppresses includes the requirement that a timeline gets
three safekeepers. Running with it means a quorum of two would boot silently.

## Placement

Safekeeper WAL that has not been offloaded is the one tier with no other copy, so no two safekeepers
share a node with each other, and none shares a node with the pageserver holding the layers their
WAL would be replayed into. The pageserver is the only workload that grows without bound, so it gets
the node with the most free space.

Nothing here keeps a component off a reclaimable node. That belongs to the node, not to this chart:
a node that can vanish should carry a taint.
