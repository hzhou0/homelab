# AGENTS.md

Repo layout for the homelab k3s cluster. Each top-level directory is a self-contained concern;
there is no root build system or shared Go module beyond `opnsense-operator/`.

| Path | Kind |
|---|---|
| `platform/` | `homelab-platform` Helm chart — Kyverno + the admission constraints an autonomous operator deploys inside |
| `cilium/`, `cert-manager/`, `topolvm/`, `seaweedfs/`, `monitoring/` | foundational cluster tooling, each its own Helm chart |
| `opnsense-operator/` | Go controller-runtime operator + its own Helm chart (`opnsense-operator/chart/`) |
| `hypha/` | design only (`ARCHITECTURE.md`); the Rust S3 gateway, not yet built here |
| `kube-node/` | Alpine/k3s node image build scripts + `nodes.md` provisioning runbook |
| `agents/operator/` | sandboxed autonomous-operator agent — own `CLAUDE.md`, `homelab-design-document.md`, `references/` |
| `utils/` | misc node scripts (gVisor install) |

## Conventions

### Comment style

Comments explain **why**, not what. The code and key names already say what. Prefer the shortest
comment that captures the non-obvious reason; if nothing non-obvious remains, write no comment.

Keep:
- Non-obvious constraints or invariants (e.g. "empty `{}` also matches pods")
- Design decisions that aren't derivable from the code (e.g. why a native VAP instead of Kyverno)
- Ordering dependencies or subtle interactions between resources

Remove — assume the reader knows the tools and can inspect the code:
- Basic Kubernetes/Helm syntax or semantics — what a field, selector, or resource kind does. Never
  explain a standard behavior the reader can look up (e.g. "matchLabels selects pods with these
  labels", "quote renders the int as a string").
- Cluster architecture, topology, or facts visible by inspecting the manifests, values, or a sibling
  resource (e.g. re-describing which ports a component listens on, or restating the node layout).
- Prose restating what the resource type or field name already says.
- Step-by-step operational commands (those belong in the README).
- Cross-references to the README or design doc (the reader can find those).

Same rule applies to README files: architecture and rationale stay; shell command sequences,
migration procedures, and explanations of standard tooling go.

### Networking commentary

Keep networking commentary (LB IPAM/L2, east-west default-deny, gateway L3 allow-list,
HTTPRoute topology, NetworkPolicy fences) **on the network CRD templates that implement it**
(`networkpolicies`, `ciliumnetworkpolicies`, `httproutes`, `gateways`, `gatewayclasses`) or in
the `cilium/` chart. Do **not** re-describe that topology in `values.yaml` files,
RBAC templates, or governance/`GeneratingPolicy` templates that aren't themselves network CRDs
— when readers need it, they'll read the network CRD. A one-line pointer is fine where a value
has no meaning without the network context, but don't restate the cilium / NetworkPolicy design
prose in another chart's values.

`agents/operator/CLAUDE.md` is the operator agent's operating manual — read it before editing
anything under `agents/operator/`.

## Pre-commit (run `pre-commit install` once after clone)

The single hook (`.pre-commit-config.yaml`) regenerates `agents/operator/references/*.md` by
copying each top-level chart's `README.md` with a generated header, into the sandboxed agent
context (it can't follow symlinks). It **exits non-zero if anything changed** — when you edit a
chart README, the hook will block the commit; stage the updated `references/*.md` and re-commit.
Never edit `references/` by hand — it is generated.

## Helm charts

- `helm lint <chart>` per chart is the only static check at repo level; there is no root lint/test/typecheck runner. Run it after touching chart templates/values.
- Foundational charts are installed by a human (cluster-admin), each in its **own non-tier
  namespace** (e.g. `opnsense-operator`, `kyverno`), deliberately **not** `app-*`/`tool-*` so
  Kyverno governance does not apply. The autonomous operator must not install them.
- One Helm release per `tool-*` namespace is enforced by `platform`'s `tool-single-release`
  policy — don't add a second chart to an existing tool namespace.
- `platform` install is a two-pass dance because Kyverno CRDs exceed the 1 MB Secret limit and
  are applied out-of-band: `helm dependency build platform` → `./platform/install-crds.sh` →
  pass 1 (`--set policies.enabled=false --set operator.enabled=false`) → wait for Kyverno →
  pass 2 (`helm upgrade` with full values). Re-run `install-crds.sh` only when bumping the
  Kyverno chart version. `values.yaml` must keep `kyverno.crds.install: false`.

## opnsense-operator (Go)

- Module: `github.com/hzhou0/homelab/opnsense-operator`; Go 1.22; controller-runtime, no CRDs.
- `Makefile` is the source of truth: `make all` = `fmt vet test build`; `make test` =
  `go test ./... -count=1`; `make helm-lint` and `make helm-template` (the latter needs the
  dummy `--set opnsense.apiKey/apiSecret` shown in the Makefile, otherwise values validation
  fails). There is no separate lint/typecheck — `make fmt && make vet` covers it.
- CI (`.github/workflows/opnsense-operator.yml`) builds and pushes the operator image to GHCR
  on changes under `opnsense-operator/**`. Tag `opnsense-operator/v*` to ship a versioned image.
  Chart-only or unrelated changes do **not** trigger an image build.
- The operator consumes `github.com/hzhou0/opnsense-sdk/go-sdk` as a published module — do not
  vendor it.

## Gotchas

- LoadBalancer IPs are assigned and L2-announced by **Cilium LB IPAM**; MetalLB was removed.
  Don't reference MetalLB in new config.
- `homelab-platform` constraints use the GA CEL policy types
  (`ValidatingPolicy`/`MutatingPolicy`/`GeneratingPolicy`), not Kyverno `ClusterPolicy`. Pod-level
  checks run at Pod admission, so a bad image surfaces as a ReplicaSet `FailedCreate`, not a
  Deployment rejection.
- Foundational charts (`cilium`, `cert-manager`, `topolvm`, `seaweedfs`, `hypha`, plus
  `opnsense-operator`) need privileged/cluster-scoped access and are human-installed; the
  autonomous operator's RBAC is deliberately confined to `app-*`/`tool-*` namespaces.