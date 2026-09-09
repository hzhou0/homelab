# neon

Postgres with compute and storage separated: data durable in hypha, computes disposable, branching
available when wanted. Open-source Neon ships the storage layer and `compute_ctl` but deliberately
not a control plane — that is the part Neon Inc. keeps — so this supplies it.

`chart/` is the `homelab-neon` Helm chart, `ctl/` is `neon-ctl`, the control plane.

Foundational: cluster-admin installs it into the `neon` namespace, not an `app-*`/`tool-*` one, so
the platform's admission constraints do not apply.

## Prerequisites

- A bucket on the durable hypha gateway. Nothing here creates one and neither Neon component
  creates one lazily.
- `topolvm-provisioner` for pageserver and safekeeper volumes, `zerofs` for the controller
  database. The controller database holds generation numbers and must survive node loss: reset that
  counter and a stale writer is blessed, which is silent corruption a dump cannot undo.
- A node labelled for each pageserver and safekeeper in `values.yaml`. Placement is stated per
  instance rather than scheduled, because the ids are permanent.

## Install

One Secret holds every credential. The chart creates none and takes no literal key, so nothing
sensitive reaches a values file or the release history.

```sh
openssl genpkey -algorithm ed25519 -out auth.pem

kubectl create namespace neon
kubectl -n neon create secret generic neon-credentials \
  --from-literal=bucketAccessKey=... \
  --from-literal=bucketSecretKey=... \
  --from-literal=controllerDbPassword=... \
  --from-file=authPrivateKey=auth.pem

helm install neon neon/chart -n neon
```

That key is the whole of the storage layer's authentication. Every public key and every token is a
function of it, so each pod derives what it needs into an emptyDir before its main container
starts. Rotating is replacing that one Secret value and restarting the deployments.

## Bootstrap

Nothing is created at install. A project is a tenant, a branch is a timeline plus a compute pointed
at it, and all of them are runtime objects `neon-ctl` owns. The published host serves both a page
and the API behind it; the page is the whole of the interface, and every action on it is a request
somebody could have made by hand.

A branch keeps a SCRAM verifier and never the password behind it, so the page can print a
connection string for any branch but can only print one that carries a password at the moment the
password is set — creating a branch, or asking for a new one.

Postgres is the source of truth for what roles and databases exist, and its catalog is read before
anything about them is shown. Whatever it holds is recorded, so a role made with SQL authenticates
through the proxy like any other and can be dropped from the page like any other. Recording it is
not a convenience: the proxy resolves a verifier before it wakes a compute, so there is a moment
when the registry is the only place one exists. What belongs to Postgres and to compute_ctl is left
out — the reserved `pg_` roles, `cloud_admin`, `neon_superuser`, and the databases the compute needs
for itself — because those are not a branch's to manage.

Nothing is shown that was not just confirmed there. A branch whose compute stopped cleanly was read
on the way out, and the names from that read are enough to build the string that starts it again. A
branch that was never read is not partly known, it is unknown: the page says so and offers nothing
but the button that starts it. That is why a snapshot is discarded when a compute starts rather than
when it stops — one exists only for a branch that went down cleanly and has not run since.

A role's password is normally not kept at all — only the verifier derived from it, which cannot be
read back, so the password exists for exactly as long as the answer that set it. Supplying a key
changes that: the password is then sealed under it, bound to the role's name, and can be shown
again. The key is deliberately not the storage one, whose rotation is a routine operation; there is
no rotation for this one, and replacing it makes every stored password unreadable without affecting
anything else.

Every catalog change is carried out by a compute and by nothing else, so a change is accepted only
once a compute has taken it, and a sleeping branch is woken for one. A branch nothing is known about
is not woken: it has to be started deliberately first, because a change made against a guess about
what is there is how the two ends stop agreeing. The page never leaves one in that state — creating
a branch starts it, and the suspender takes it down again once nobody is using it. Additions could in principle wait for the next
start, since a spec states what should exist; drops could not, because a catalog cannot say that
something should stop existing.

Nothing is undone when a change fails. A spec that fails partway may still have applied some of
itself, so putting the record back would be a guess; the next read is what corrects it, and the next
read happens before anything is displayed.

The same rule decides what happens when the registry cannot be read while a compute is starting. A
spec is read once, at startup, and nothing revisits it, so serving one without the catalog would
leave a branch running that no role can log in to. It is refused instead, and the pod restart is
the retry.

`neon-ctl` authenticates nobody. It is told who the caller is by the authenticating proxy in front
of it, in headers, so publishing it on the gateway without that proxy would let anyone claim any
identity. The rest of its routes are not published at all: the hooks and the proxy's auth endpoint
answer callers inside the namespace, and one of them serves role verifiers.

A fork inherits its ancestor's Postgres version and cannot differ, and it can always read what its
ancestor could — which is why a grant is per project and never per branch.

Clients connect to `<endpoint>.pg.internal.haustorium.net`, which the gateway's own DNS wildcard
already answers — a DNS wildcard synthesises at any depth, unlike a TLS one, which matches a single
label and is why the proxy's certificate has to be the wildcard one level below that suffix. The
proxy takes the endpoint from the SNI name; a client that cannot send SNI names it in the startup
options instead.

Postgres is carried on the shared gateway's TCP listener rather than an address of its own. The
gateway forwards the port without reading it, because the protocol upgrades to TLS mid-connection
and so offers an SNI router nothing to match on the first byte — and the proxy needs the name
anyway.

## Verify

Work down this list; each rung depends on the ones above it.

1. Every pageserver appears under `GET /control/v1/node`, every safekeeper under
   `GET /control/v1/safekeeper`, each in a **distinct** availability zone. A timeline cannot be
   placed until both lists are populated, and the controller spreads members one per zone.
2. Heartbeat rounds report nothing offline. A scope mismatch shows up here as a JWT error, and for
   pageservers it shows only as `warming-up` — check the pageserver's own log too.
3. Volumes bind on the expected classes and nodes; topolvm's advertised capacity drops as predicted.
4. Both bucket prefixes receive writes. Watch hypha's metrics for 4xx — that is the real
   compatibility test.
5. **`backup_lsn` advances** past `timeline_start_lsn` on a safekeeper's `timeline_status`. This is
   the check that proves WAL is actually being reclaimed; without it the volumes fill until writes
   stall cluster-wide. Pair it with a standing alert on safekeeper volume use.
6. A query succeeds from a granted namespace and is refused from an ungranted one.
7. **Exercise the hooks.** Migrate a tenant to another pageserver and confirm `neon-ctl` resolves the
   node id, rebuilds the spec, and reconfigures the compute *without* restarting it. A migrate
   returns 200 without moving anything in two separate cases, and both are silent: the optimiser
   reverts a sub-optimal placement unless the tenant's scheduling policy is `Essential` and
   `override_scheduler` is set, and a migration defaults to `prewarm`, which waits for a secondary
   that has no heatmap yet on a freshly written tenant. Pass `prewarm: false` to cut over cold.
   Then delete a compute pod and confirm it returns correctly bound, which exercises the pull path.
8. **Exercise the cold path.** Let a branch idle to zero, connect, and confirm the proxy wakes it.
   Measure how long it takes.

`ctl/e2e/cluster.sh` builds a throwaway k3d cluster to run 1–8 against, with MinIO standing in for
hypha; the checks themselves are manual. It pins the k3s version to the homelab's, because a CRD
schema that the real API server accepts is half of what is being tested. `k3d cluster create`
switches the current kubeconfig context even though every command here names its own.

## Operating

**Placement is stated, not scheduled.** Pageservers and safekeepers name their node, because ids
are permanent and a safekeeper that shares a failure domain with the pageserver its WAL replays into
defeats the point of a quorum. The singletons instead exclude reclaimable capacity: the spot node
exists to run disposable work, so a taint would push all of it off, and an exclusion keeps only what
cannot survive a reclaim away. The proxy is exempt, having replicas and no state.

**Adding a safekeeper** is an entry in `values.yaml` and an upgrade. `neon-ctl` registers it from
its Service within 10 seconds; until then the controller has not heard of it and a timeline created in
that window fails to place.

**Removing one** needs its scheduling policy set through the controller first. Deleting the Service
stops it being re-registered but does not retire the record.

**The controller database must not be lost.** It holds the generation numbers that fence a stale
pageserver out of object storage, and losing them is the one failure here that corrupts data
silently rather than stopping it. A backup does not rescue it: restoring an old dump re-issues
generations already handed out, which is precisely what the fencing exists to prevent, and
rebuilding them by scanning object storage is not a supported path. The requirement is that it
survives, not that it can be restored — hence durable storage rather than a node-local volume. If it
is rebuilt anyway, placement and safekeeper registration re-converge on their own; generations
do not.

**The registry is what to back up.** Branch names and per-branch settings — small, and the only
state object storage and the controller cannot reconstruct between them. A stale copy is still
useful, which is what makes this a backup problem rather than a durability one: losing it loses
names, not data. It is held under an exclusive file lock, which is why `neon-ctl` runs one replica
with a `Recreate` rollout.

**An upgrade means re-checking the compute spec.** It is Neon's internal format and changes across
releases; a rendered spec is pinned as a golden document, so a shape change fails a test rather than
producing a compute that will not start. Pin the storage and compute images to the same tag and read
that diff on every bump.

## Known limits

- **The control plane is ours.** Small, but a bug means computes silently keep dialling a detached
  pageserver. Rungs 7 and 8 are the guard.
- **Durable mode is WAN-backed.** The painful paths are cold pageserver reads and WAL offload
  throughput, and only one safekeeper is elected offloader at a time. Measure before trusting.
- **hypha is a single writer.** Fine at this scale, a ceiling to remember.
