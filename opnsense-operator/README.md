# opnsense-operator

An [external-dns](https://github.com/kubernetes-sigs/external-dns)-style Kubernetes controller
for an OPNsense-fronted homelab. It watches `type: LoadBalancer` **Services** and Gateway-API
**Gateways** and, from `homelab.lab/*` annotations, reconciles three things in OPNsense via the
[`opnsense-sdk`](https://github.com/hzhou0/opnsense-sdk) Go SDK:

- **Internal DNS** — Unbound A and AAAA host overrides (wildcards supported) for every assigned
  LoadBalancer IP.
- **WAN port-forward** — an IPv4 firewall DNAT rule translating the WAN address to the
  LoadBalancer's RFC1918 address.
- **WAN pass rule** — an IPv6 firewall rule admitting traffic to the LoadBalancer's globally
  routable address, which is routed directly and so needs no translation.

See [DESIGN.md](DESIGN.md) for the full architecture.

## Annotations

Put these on a `LoadBalancer` Service or a `Gateway`:

| Annotation | Meaning | Default |
|---|---|---|
| `homelab.lab/hostname` | Comma-separated DNS names (wildcard ok, e.g. `*.lab`) | — |
| `homelab.lab/expose` | `"true"` to open WAN access (IPv4 port-forward, IPv6 pass rule) | off |
| `homelab.lab/external-port` | External WAN port | service/listener port |
| `homelab.lab/protocol` | `tcp` or `udp` | service port protocol (Gateways: tcp) |
| `homelab.lab/internal-port` | Forwarded-to port (IPv4 only; see below) | service/listener port |

Hostnames outside the configured `MANAGED_DOMAINS` are rejected. The controller owns only the
OPNsense objects it creates (tagged `k8s:opnsense-operator owner=<Kind>/<ns>/<name>`) and removes
them via a finalizer on deletion.

A dual-stack object gets records and firewall state for both families: DNAT for its first IPv4
address, one pass rule per IPv6 address. Each pass rule admits exactly one address, port and
protocol; everything else stays default-denied.

The operator deliberately does not redirect ports over IPv6 — only translation can remap a port,
and translating a globally routable address gives up the end-to-end reachability that is the point
of having one. `internal-port` therefore applies to IPv4 only, and setting it to something other
than `external-port` on an object that has an IPv6 address is **rejected** with an
`InvalidExposure` event rather than silently exposing only the IPv4 half.

Pass rules are written to OPNsense's **automation** ruleset, which is evaluated ahead of the
hand-maintained per-interface rules.

## Install

```sh
helm install opnsense-operator ./chart \
  --namespace opnsense-operator --create-namespace \
  --set opnsense.existingSecret=opnsense-operator-creds
helm upgrade opnsense-operator opnsense-operator/chart --namespace opnsense-operator
```

Credentials go in a pre-created Secret (`OPNSENSE_API_KEY` / `OPNSENSE_API_SECRET`), or set
`opnsense.apiKey`/`opnsense.apiSecret` to have the chart create it.
