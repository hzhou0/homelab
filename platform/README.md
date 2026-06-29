# homelab-platform

A Helm chart that installs **Kyverno** plus the **constraint surface** an autonomous
(LLM) operator deploys into. Kyverno encodes the cluster's architecture and standards as
admission constraints: every manifest the operator submits is either accepted (inside the
feasible region) or hard-rejected with a reason it can act on. The operator gets broad
deploy power but **cannot alter the constraints themselves**.

## Two tiers

| | `app-*` namespaces | `tool-*` namespaces |
|---|---|---|
| Purpose | stateless application deployments | cluster tooling |
| Workloads | `Deployment` only (no PVC, no bare Pods) | + StatefulSet/DaemonSet/Job/CronJob/PVC |
| Pod Security | `restricted` | `baseline` |
| Resources | per-runtime envelope (see below) | LimitRange defaults + ceiling |
| Images | allowlist only | allowlist only |

Every `app-*`/`tool-*` namespace is auto-governed the moment it is created: Kyverno
generates a `ResourceQuota`, `LimitRange` and default-ingress `NetworkPolicy`, and stamps
the Pod Security Admission labels. `synchronize: true` reverts any drift, so the operator
cannot widen its own sandbox.

## Per-runtime resource ranges

An `app-*` `Deployment` must carry the pod-template label `homelab.lab/runtime`
(e.g. `go`, `node`, `python`, `jvm`, `rust`, `static`). Each runtime maps to a cpu/mem
envelope in the `runtime-profiles` ConfigMap (see `values.yaml`); containers whose
requests/limits fall outside the envelope for their declared runtime are rejected. The
operator declares the language; the platform fixes the legal resource window.

## Image allowlist

Only the exact container images listed in `values.yaml` → `imageAllowlist` may run. To add
an image, edit values and `helm upgrade` — a deliberate, auditable step that keeps the
autonomous operator on vetted images.

## Install

```sh
helm dependency build platform
./platform/install-crds.sh                              # Kyverno CRDs, out-of-band (see below)
helm install homelab-platform platform -n kyverno --create-namespace
```

> **Kyverno CRDs (installed out-of-band).** Helm stores the **entire chart** inside its release
> Secret, and Kyverno bundles ~5.6MB of CRD source (the `crds` + `kyverno-api` subcharts). That
> exceeds Kubernetes' 1MB Secret limit (`data: Too long: may not be more than 1048576 bytes`) no
> matter what *renders* — per-CRD `crds.groups.*` toggles only affect rendering, not the stored
> chart, so they can't fix it. So `values.yaml` sets **`kyverno.crds.install: false`**, which makes
> Helm **prune** those conditional subcharts from the release entirely (the kyverno chart drops from
> ~6.5MB to ~885KB; the release Secret lands ~220KB). The CRDs are applied separately by
> **`./platform/install-crds.sh`** (`kubectl apply --server-side`, rendered from the pinned chart so
> they track the engine version). Run it before the first `helm install`, and again before any
> `helm upgrade` that bumps the Kyverno chart version.
>
> **Migrating an existing release that previously bundled the CRDs** — a plain upgrade to
> `crds.install: false` would make Helm *prune (delete)* the CRDs as they leave the manifest, which
> cascades to **every policy and custom resource**. Tell Helm to keep them first:
> ```sh
> ./platform/install-crds.sh                            # ensure CRDs exist + adopt the kubectl field manager
> kubectl get crd -o name | grep -E 'kyverno\.io|wgpolicyk8s\.io' \
>   | xargs -I{} kubectl annotate {} helm.sh/resource-policy=keep --overwrite
> helm upgrade homelab-platform platform -n kyverno     # now small; CRDs survive the prune
> ```
>
> If you hit a CRD-not-found race on a fresh install, gate the policies off for the first pass
> (`--set policies.enabled=false --set operator.enabled=false`) then re-enable them explicitly on
> a follow-up `helm upgrade` (a plain upgrade may carry the disables forward via `--reuse-values`).

Tune everything (quota sizes, runtime profiles, image allowlist, PSA levels, operator
names) in `values.yaml`.

## Operator kubeconfig

The chart creates the operator's `ServiceAccount`, `ClusterRole` and binding. Mint a
kubeconfig for an out-of-cluster operator with:

```sh
./platform/gen-kubeconfig.sh            # 1-year token -> operator.kubeconfig
./platform/gen-kubeconfig.sh -d 0       # non-expiring bound-Secret token
```

In-cluster, an operator pod in the `tool-operator` namespace just uses the ServiceAccount
directly — no kubeconfig needed. The operator's runtime/LLM logic lives in
`operator/` and is out of scope for this chart; this chart delivers its **identity,
permissions, and the constraint surface**.

## What enforces what

| Concern | Mechanism |
|---|---|
| Allowed kinds per tier | `ValidatingPolicy` `allowed-kinds-{app,tool}` (deny complementary kinds) |
| Stateless / no bare Pods | `ValidatingPolicy` `no-bare-pods` + `no-pvc-volumes-app` |
| Pod Security | native PSA labels (stamped by `MutatingPolicy` `namespace-psa-labels`) + defaults auto-filled by `harden-defaults` |
| Image allowlist | `ValidatingPolicy` `image-allowlist` + ConfigMap |
| App resource ranges | Kyverno `runtime-label` + `runtime-ranges` + ConfigMap |
| **Tool resource ranges** | the generated **`LimitRange` + `ResourceQuota`** (no extra policy needed) |
| One chart per `tool-*` ns | Kyverno `tool-single-release` (inspects Helm release Secrets) |
| Namespace provenance metadata | Kyverno `namespace-metadata` (src/notes annotations + recommended labels) |
| Per-namespace caps | generated `ResourceQuota` |
| Constraint immutability | operator `ClusterRole` omits kyverno/RBAC/quota writes + `protect-constraints` |
| Operator confined to tiers | `homelab-operator-deploy` bound per-namespace by `namespace-governance` + `restrict-operator-namespaces` |
| Operator can't alter network topology | `ValidatingPolicy` `operator-no-network-topology` (deny Gateway/GatewayClass, L4 routes, Cilium network CRDs, Ingress) + deploy `ClusterRole` omits `ingresses`/gateways |
| Operator can't expose TCP services | `ValidatingPolicy` `operator-no-service-exposure` (only ClusterIP/ExternalName + UDP-only LoadBalancers; TCP enters via the shared Gateway/HTTPRoute) |

## Notes & follow-ups

- **Tool resource governance** is handled entirely by the generated `LimitRange`
  (defaults + min/max) and `ResourceQuota`, which Kubernetes' built-in admission enforces —
  so there is no separate `tool-resources` Kyverno policy. App ranges need Kyverno because
  they vary by the runtime label, which a static `LimitRange` cannot express.
- **One chart per `tool-*` namespace** (`oneChartPerToolNamespace.enabled`, default on):
  Helm records each release as an `owner=helm` Secret; the `tool-single-release` policy
  rejects a new release Secret if the namespace already holds a *different* release. This
  requires Kyverno's admission controller to read Secrets, so the chart adds an aggregation
  `ClusterRole` (`<release>:kyverno-secrets-reader`) granting `secrets: get/list/watch`. Set
  the value to `false` to drop both the policy and that grant. Note this bounds *Helm
  releases*, not raw `kubectl apply` resources.
- **Operator confinement (RBAC allowlist).** The operator is confined to `app-*`/`tool-*`
  *by RBAC*, not by a blocklist. Its cluster-bound `homelab-operator` `ClusterRole` holds
  only cluster-scoped + read rights (create/delete/list namespaces — gated to
  app-*/tool-* names by `restrict-operator-namespaces` — read nodes, read policies/reports). All namespaced write power lives in a separate
  `homelab-operator-deploy` `ClusterRole` that is **never** bound cluster-wide —
  `namespace-governance` generates a `RoleBinding` for it into each `app-*`/`tool-*`
  namespace, so the operator can write *only* there. It therefore has zero write (and no
  Secret read) in `kube-system`, `default`, etc., independent of Kyverno's namespace
  exclusions. `restrict-operator-namespaces` (a native `ValidatingAdmissionPolicy`, not
  Kyverno — because Kyverno skips its own namespace and `kube-node-lease`, so it couldn't
  stop the operator deleting the `kyverno` namespace) stops it creating *or deleting* any
  non-tier namespace, and `protect-constraints` remains as defense-in-depth. This needs Kyverno's background
  controller to manage `RoleBinding`s + `bind` the deploy role (aggregation `ClusterRole`
  `<release>:kyverno-operator-rolebinding-manager`). A scoped `Role` lets the operator read
  the platform ConfigMaps in the release namespace for introspection.
  - *Propagation note:* right after the operator creates an `app-*`/`tool-*` namespace, the
    deploy `RoleBinding` is generated asynchronously (a moment later) — deploys may need a
    brief retry until it lands.
- **Foundational tooling** that needs cluster-admin (CRDs, cluster RBAC, webhooks, or a
  privileged namespace) can't be operator-installed and doesn't belong in this release's
  namespace. **`cilium`** (CNI + service mesh + LB IPAM/L2, in `kube-system`) and
  **`cert-manager`** (in `cert-manager`) are the examples in-repo, each its own cluster-admin
  chart. Rook/CloudNativePG would follow the same pattern. (LoadBalancer IPs are handled by
  Cilium's built-in LB IPAM + L2 announcements — MetalLB was removed.)
- **CEL policies, not `ClusterPolicy`.** Kyverno deprecated `ClusterPolicy` in 1.17 (removal
  in v1.20). All constraints are the GA CEL types (`ValidatingPolicy`, `MutatingPolicy`,
  `GeneratingPolicy`) under `policies.kyverno.io/v1`. Key consequence of the CEL engine:
  the pod-level `image-allowlist`/`no-bare-pods`/`harden-defaults` checks run at **Pod**
  admission (podController autogen is disabled because its namespace rewrite is broken), so
  a bad image or missing securityContext surfaces as a ReplicaSet `FailedCreate` rather than
  a Deployment rejection. `harden-defaults` auto-fills restricted securityContext fields on
  `app-*` pods using multi-step JSONPatch (not ApplyConfiguration — CEL's ApplyConfiguration
  engine rejects mutations to atomic fields like `capabilities.drop`; JSONPatch bypasses the
  typed schema and `dyn({"drop": dyn(["ALL"])})` correctly unwraps the value). PSA
  `restricted` still hard-enforces the outcome; the mutation makes the operator's job
  ergonomic.
- Optional: re-enable gVisor for `app-*` (infra exists at `utils/gvisor-runtime.yaml`),
  the Kyverno reports controller for a policy-report dashboard, and GitOps for an audit
  trail of operator actions.
