# OPNsense external-dns + port-forward controller

## Context

The homelab (OPNsense + k3s + Cilium) exposes services two ways (design doc §5.6): all
TCP enters through one Cilium Gateway behind a single LoadBalancer IP, and UDP services each get
their own LoadBalancer IP. LoadBalancer IPs are assigned and L2-announced by Cilium (LB IPAM +
L2 announcements; MetalLB was removed). Two things must be wired into the OPNsense firewall for
each exposed service: an **internal DNS** record in Unbound (wildcard for the Gateway, or a
specific name) pointing at the LB IP, and a **WAN port-forward (DNAT)** so the service is
reachable from the internet. Today both are manual.

This controller automates both, external-dns style: it watches LoadBalancer `Service`s and
Gateway-API `Gateway`s, reads `homelab.lab/*` annotations, and reconciles Unbound host
overrides + firewall DNAT rules in OPNsense via the `github.com/hzhou0/opnsense-sdk` Go SDK.
It owns only the OPNsense objects it creates (tagged by description), and cleans them up on
deletion via a finalizer. This is the operator described in design doc §5.6
("annotation-driven, like the AWS Load Balancer Controller"), now reconciling DNS as well as
port-forwards.

## OPNsense SDK

Consumed as the published module `github.com/hzhou0/opnsense-sdk/go-sdk` (tag `go-sdk/v0.1.0`).
Surface used:

- Construct: `opnsensesdk.NewClient(opnsensesdk.Options{BaseURL, APIKey, APISecret, HTTPClient})
  (*generated.Client, error)` — Basic auth, custom `*http.Client` for self-signed TLS.
- Unbound host override: `UnboundSettingsControllerAdd/Set/Del/Get/Search/ToggleHostOverrideAction`.
  Apply with `UnboundServiceControllerReconfigureAction(ctx)`.
  Add/Set body = `{ host: { Enabled, Hostname*, Domain, Rr (A/AAAA), Server* (the IP),
  Description* } }`. Wildcard = `Hostname:"*"`, `Domain:"lab"`.
- Firewall DNAT (port-forward): `FirewallDNatControllerAdd/Set/Del/Get/Search/ToggleRuleAction`.
  Apply with `FirewallDNatControllerApplyAction(ctx, rollbackRevision)` (pass `""`).
  Add/Set body = `{ rule: { Interface ("wan"), Protocol (tcp/udp), Port (external),
  Target (LB IP), LocalPort (internal), Description, Disabled, Pass } }`. `Pass` is set so the
  auto-associated filter rule lets traffic through (WAN is default-deny otherwise).
- **All client methods return raw `(*http.Response, error)`; only request bodies are typed.**
  Responses are JSON-decoded by the wrapper. OPNsense conventions: Search → `{"rows":[...]}`;
  Add → `{"result":"saved","uuid":"..."}`; errors → `{"result":"failed","validations":{...}}`.

## Layout

```
opnsense-operator/
  go.mod                     module github.com/hzhou0/homelab/opnsense-operator
  cmd/manager/main.go        wire manager, config, start controllers
  internal/
    controller/
      source.go              parse homelab.lab/* annotations -> DesiredExposure; diff helpers
      service_controller.go  reconcile type=LoadBalancer Services
      gateway_controller.go  reconcile Gateways (resolve backing cilium LB Service IP)
      reconcile.go           shared reconcile core + finalizer + status
    opnsense/
      client.go              wraps *generated.Client: typed Search/Add/Set/Del + decode + apply
      dns.go                 host-override CRUD
      nat.go                 port-forward CRUD
      model.go               HostOverride, PortForward, owner tag helpers
    config/config.go         env-driven config
  chart/                     Helm chart
  Dockerfile / Makefile
```

Framework: **controller-runtime**, no CRDs — watch built-in `corev1.Service` and
`gatewayv1.Gateway`. Source of truth is annotations on existing objects (the external-dns
model).

## Annotation contract (`homelab.lab/` prefix)

On a `Service` (type=LoadBalancer) or `Gateway`:

- `homelab.lab/hostname` — comma-separated DNS names (wildcard allowed, e.g. `*.lab`) mapped to
  the object's LB IP. Split into host/domain on the first dot; `*` → wildcard host.
- `homelab.lab/expose` = `"true"` — create a WAN DNAT port-forward to the object's IP.
- `homelab.lab/external-port` — external WAN port (default: the service/listener port).
- `homelab.lab/protocol` — `tcp`|`udp` (default: service port protocol; Gateways = tcp).
- `homelab.lab/internal-port` — forwarded-to port (default: the service port).

A controller-wide **managed-domains** filter rejects hostnames outside the managed zones.

## Reconcile algorithm

1. **Resolve IP.** Service → `.status.loadBalancer.ingress[0].ip`. Gateway → backing Cilium LB
   Service (`cilium-gateway-<name>`), read its LB IP. No IP yet → requeue.
2. **Desired state** from annotations → `DesiredExposure{ Hostnames, IP, PortForward }`.
3. **Ownership tag.** Each OPNsense object carries
   `Description = "k8s:opnsense-operator:<kind>/<ns>/<name>[ ...]"`. Only rows matching our
   prefix + this owner are read/modified/deleted.
4. **Diff.** Search overrides + DNAT rules, filter to owner, compute add/update/delete vs.
   desired, apply via Add/Set(uuid)/Del.
5. **Apply once** per reconcile (only if changed): `FirewallDNatControllerApplyAction` and/or
   `UnboundServiceControllerReconfigureAction`, under a process-global mutex.
6. **Finalizer** `homelab.lab/opnsense-operator`: on delete, remove all owned OPNsense objects,
   apply, then drop the finalizer.
7. **Status.** Do not touch `Service.status.loadBalancer` (the LB controller — Cilium LB IPAM — owns it). Record results via
   annotation `homelab.lab/exposed` + Events.

## Concurrency / config

- Global apply mutex in `opnsense.Client` serializes mutate+apply.
- Requeue with backoff on transient API errors; reconcile is level-triggered.
- Env config: `OPNSENSE_URL`, `OPNSENSE_API_KEY`, `OPNSENSE_API_SECRET`,
  `OPNSENSE_WAN_INTERFACE` (default `wan`), `MANAGED_DOMAINS` (comma list),
  `OPNSENSE_INSECURE_TLS`.

## Helm chart

Deployment (1 replica, distroless non-root), ServiceAccount, ClusterRole/Binding (watch
Services + Gateways, patch their metadata/finalizers, emit Events), Secret (OPNsense creds),
values.yaml. Installed by a human via Helm (like the `cilium`/`cert-manager` charts) into its own namespace
`opnsense-operator` — deliberately **not** an `app-*`/`tool-*` namespace, so the platform's
Kyverno governance (which only targets those prefixes) does not apply. The image is also
listed in the platform image allowlist for a complete catalogue.

## Verification

- Unit (table-driven): annotation parsing → `DesiredExposure`; diff sets; owner tagging.
- Wrapper: `httptest.Server` faking OPNsense (search/add/apply) asserting bodies + decode.
- E2E (manual): deploy chart; apply a LoadBalancer Service with `homelab.lab/hostname` +
  `homelab.lab/expose`; confirm the Unbound override + WAN DNAT appear and `dig` resolves;
  delete and confirm cleanup.

## Risks / to verify

- Exact JSON field names for Search **rows** / Add **uuid** (decode against a live response).
- DNAT `Pass`/`associated-rule-id`: confirm one Add creates both NAT + companion pass rule.
- Gateway→backing-Service resolution naming on the installed Cilium version.
- Whether `rollbackRevision`/savepoint is needed, or empty-string apply suffices for a
  single-writer controller.
