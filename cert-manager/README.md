# homelab-cert-manager

cert-manager for the homelab, **installed by cluster-admin** into `cert-manager`. Wraps the
upstream jetstack chart and ships a Let's Encrypt **ClusterIssuer** (`letsencrypt-cloudflare`) that
solves **DNS-01 via Cloudflare**, used to issue the wildcard `*.internal.haustorium.net` cert for
the Cilium ingress Gateway (`homelab-cilium`).

## Why its own chart

Like `cilium`, cert-manager is foundational: CRDs, cluster RBAC and a validating
webhook, in its own namespace (not an `app-*`/`tool-*` tier). The autonomous operator can't install
it. DNS-01 issues a publicly-trusted cert for an **internal-only** name without any inbound
reachability — the host never needs to be on the internet, only its DNS zone (Cloudflare) under our
control.

## Cloudflare token

Create an API token scoped to **Zone → DNS → Edit** on the `haustorium.net` zone, then store it as a
Secret in the `cert-manager` namespace (keep it out of git) and reference it:

```sh
kubectl create namespace cert-manager
kubectl -n cert-manager create secret generic cloudflare-api-token \
  --from-literal=api-token='<CLOUDFLARE_API_TOKEN>'
# then set cloudflare.existingSecret=cloudflare-api-token in values.yaml
```

(Alternatively set `cloudflare.apiToken` in values to have the chart create the Secret — less
secure.)

## Install (two passes)

The `ClusterIssuer` is a cert-manager CRD validated by cert-manager's webhook, so it can't be created
in the same pass that first installs cert-manager. Install cert-manager first, then enable the issuer
— same pattern as `platform`.

```sh
helm dependency build cert-manager

# 1. cert-manager itself (CRDs, controller, webhook, cainjector) — no issuer yet.
helm install homelab-cert-manager cert-manager -n cert-manager --create-namespace \
  --set acme.enabled=false

# 2. Wait for it to be ready, then add the ClusterIssuer. Set acme.enabled=true EXPLICITLY
#    (a plain upgrade may carry the pass-1 false forward).
kubectl -n cert-manager wait --for=condition=available deploy --all --timeout=120s
helm upgrade homelab-cert-manager cert-manager -n cert-manager \
  --set acme.enabled=true
```

Install this **before** `homelab-cilium` so the Gateway's `Certificate` can resolve its issuer.

## Verify

```sh
kubectl -n cert-manager get pods
kubectl get clusterissuer letsencrypt-cloudflare        # Ready=True
# After homelab-cilium is installed:
kubectl -n cilium-gateway get certificate,secret        # wildcard cert Ready
```
