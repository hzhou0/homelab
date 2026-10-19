# forgejo

The git server the operator agent's deployments are reconciled from. It holds the record of
everything the agent may change, so the agent must never be able to change it — hence foundational.

## Storage

SQLite on the shared filesystem is safe because its single-writer gateway gives SQLite real locking,
provided only one process ever opens the file. The data already lives off-node, so there is no
backup job.

## Authentication

The gateway's authorizer checks Basic credentials against the directory, so it refuses every Forgejo
token. The git paths therefore skip the filter and Forgejo checks the token itself; everything else,
the API included, needs a portal session, so no token reaches the API. A clone URL without `.git`
falls on the filtered side and fails.

In-cluster clients resolve the zone to the gateway too, so every client takes the same route and
nothing reaches Forgejo around it.

Tokens are scoped by permission, not by repository, so each client gets an account of its own that
can see only its repository.

## Install

```sh
kubectl create namespace forgejo
kubectl -n forgejo create secret generic forgejo-oidc \
  --from-literal=key=forgejo --from-literal=secret=...
helm dependency build forgejo
helm install forgejo forgejo -n forgejo
```

Install before the cilium chart publishes the route, whose ReferenceGrant lands in this namespace.
