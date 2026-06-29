# homelab-cert-manager

cert-manager for the homelab, **installed by cluster-admin** into `cert-manager`. Wraps the
upstream jetstack chart and ships a Let's Encrypt **ClusterIssuer** (`letsencrypt-cloudflare`) that
solves **DNS-01 via Cloudflare**, used to issue the wildcard `*.internal.haustorium.net` cert for
the Cilium ingress Gateway.

## Why its own chart

Foundational: CRDs, cluster RBAC, and a validating webhook — same rationale as `cilium/`. The
autonomous operator can't install it. DNS-01 issues a publicly-trusted cert for an internal name
without any inbound reachability.

## Cloudflare token

Create an API token scoped to **Zone → DNS → Edit** on `haustorium.net`, store it as a Secret in
the `cert-manager` namespace (out of git), and set `cloudflare.existingSecret` in `values.yaml`.
Alternatively, `cloudflare.apiToken` has the chart create the Secret inline.

## Install (two passes)

The `ClusterIssuer` is validated by cert-manager's own webhook, so it can't be created in the same
pass that installs cert-manager. Install with `acme.enabled=false` first, wait for the controller
to be ready, then upgrade with `acme.enabled=true`. Install this **before** `homelab-cilium` so
the Gateway's `Certificate` can resolve its issuer.
