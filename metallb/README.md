# homelab-metallb

MetalLB for the homelab, installed by **cluster-admin** into `metallb-system`. It wraps the
upstream MetalLB chart (L2 mode, no BGP/FRR) and ships the lab IP pool
(`10.0.0.100-10.0.0.150`) from `homelab-design-document.md`.

## Why this is separate from `homelab-platform`

MetalLB is foundational networking that needs cluster-admin: CRDs, cluster RBAC, a validating
webhook, and a **privileged** namespace (the L2 speaker uses `hostNetwork` + `NET_RAW`, which
baseline/restricted Pod Security forbid). The autonomous operator can't install it.

It's a **separate Helm release** because a Helm subchart shares its parent's release
namespace — MetalLB must live in `metallb-system`, while Kyverno lives in `kyverno`. One
release can't span both. `metallb-system` is not an `app-*`/`tool-*` namespace, so the
Kyverno tier policies don't touch it.

## Install (two passes)

The `IPAddressPool`/`L2Advertisement` are custom resources whose CRDs ship with MetalLB.
Helm resolves every object's type before applying, so they can't be created in the same pass
that first installs those CRDs (and they also need MetalLB's webhook running). So install
MetalLB first with the pool disabled, then enable it on a follow-up upgrade.

```sh
# 1. Privileged namespace for the L2 speaker (hostNetwork/NET_RAW).
kubectl create namespace metallb-system
kubectl label namespace metallb-system \
  pod-security.kubernetes.io/enforce=privileged \
  pod-security.kubernetes.io/warn=privileged \
  pod-security.kubernetes.io/audit=privileged

# 2. Install MetalLB itself (CRDs, controller, speaker, webhook) — no pool yet.
helm dependency build agents/metallb
helm install metallb agents/metallb -n metallb-system --set pool.enabled=false

# 3. Wait for MetalLB to be ready, then apply the pool. Set pool.enabled=true
#    EXPLICITLY — a plain upgrade carries the pass-1 `pool.enabled=false` forward
#    (Helm keeps previously-set values), so the pool would never render.
kubectl wait -n metallb-system --for=condition=available deploy --all --timeout=120s
helm upgrade metallb agents/metallb -n metallb-system --set pool.enabled=true
```

## Configure

Adjust the pool and toggles in `values.yaml`. Add more pools/advertisements as templates if
you later need multiple ranges. To enable BGP/FRR, set `metallb.frrk8s.enabled: true`.

## Verify

```sh
kubectl get pods -n metallb-system
kubectl get ipaddresspool,l2advertisement -n metallb-system
# A LoadBalancer Service should now get an IP from 10.0.0.100-150:
kubectl get svc -A | grep LoadBalancer
```
