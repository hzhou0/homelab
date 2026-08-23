# agent-state-backup

A scheduled export of everything the operator agent has deployed into its tier namespaces, written to
hypha. **Applied by cluster-admin**, into a namespace of its own.

## Why this exists

The agent writes namespaces and workloads directly, from a conversation. The resources it creates are
the sole record that they were ever asked for, where every other workload in this cluster is
reconciled from something in this repo. `migration/DUAL-STACK-MIGRATION.md` established that the hard
way, capturing this same set by hand before the cluster was destroyed. This is that query on a
schedule.

## Isolation from the agent

The agent creates and deletes freely inside its own tiers, so the backup lives beyond its RBAC and
beyond the governance that applies to it: the mistake this exists to undo can never reach the record
of it. The credential Secret is pre-created, keeping every credential out of this repo, and the
bucket appears on the first run.

## Why the durable tier

An acknowledged write to the durable deployment is already on the off-site remote. A cached-mode
write sits on local disk until reconciliation catches up — and local disk is precisely what this
exists to survive losing.

## Install

```sh
kubectl create namespace agent-backup
kubectl -n agent-backup create secret generic hypha-client \
  --from-literal=accessKeyId=... \
  --from-literal=secretAccessKey=...
helm install agent-backup agents/backup -n agent-backup
```

## What is captured

Every namespaced kind the cluster serves, discovered at run time. Discovery keeps the capture correct
as the governance policies move: a hand-written list becomes a second copy of the allowed-kinds
constraints, and the day the two disagree is the day something the agent deployed stops being backed
up in silence. Exclusions stay confined to derived state — whatever a controller rebuilds from what
the export already holds, or describes the cluster rather than the deployment.

Objects go up as the API server returned them. The restore script owns the pruning, where each
subtraction is a deliberate choice made against the cluster it is about to land in.

## Trust boundary

The export holds the agent's Secrets, and base64 is encoding rather than encryption. Durable-mode
writes reach the off-site remote encrypted and pass through the cache tier in plaintext, and that
filer's browser is routed on the internal gateway — so read access to the cache is read access to
every secret the agent holds. Run-time discovery likewise gives the ServiceAccount cluster-wide read
across every kind, the gateway's encryption password included.

Scope is the API objects. A claim is captured here; the data it binds to belongs to whatever
provisioned it.
