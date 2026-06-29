# homelab-monitoring

Prometheus + Grafana for the homelab, **installed by cluster-admin** into `monitoring`. Wraps the
upstream prometheus-community **kube-prometheus-stack** (Prometheus Operator, Prometheus,
Alertmanager, Grafana, node-exporter, kube-state-metrics) and ships an **HTTPRoute** that exposes
Grafana through the shared Cilium ingress Gateway at `grafana.internal.haustorium.net`.

## Why its own chart

Foundational, same rationale as `cilium/` and `cert-manager/`: it owns cluster-scoped CRDs
(`Prometheus`, `ServiceMonitor`, ...) and needs cluster RBAC to discover and scrape targets across
every namespace. The autonomous operator can't install it, so it lives in its own `monitoring`
namespace rather than an `app-*`/`tool-*` one.

## Grafana exposure

The HTTPRoute attaches to the `internal` Gateway in `cilium-gateway` (matching `gateway.name` /
`gateway.namespace` here with the `homelab-cilium` chart). TLS is terminated by the gateway's
wildcard cert and access is bounded by the gateway's L3 allow-list (`gateway.allowedCIDRs` over
there). Grafana's own Ingress is disabled in favour of the Gateway API.

Add `monitoring` to `gateway.backendsExcludeNamespaces` in the `homelab-cilium` chart so the
gateway→backends allow policy doesn't flip this non-governed namespace to default-deny.

## Scraping governed namespaces

`app-*`/`tool-*` namespaces carry a platform default-deny ingress NetworkPolicy. The platform
chart's generated `default-ingress` policy admits traffic from this `monitoring` namespace
(`networkPolicy.monitoringNamespace`, matched by the immutable `kubernetes.io/metadata.name`
label), so Prometheus can scrape app pods' `/metrics` there with no per-namespace setup. Keep this
chart's release namespace and that value in sync if you rename it.

## Persistence

Prometheus defaults to `emptyDir` (metrics lost on restart). Set
`kube-prometheus-stack.prometheus.prometheusSpec.storageSpec` to a Ceph-backed PVC for durability —
a commented template is in `values.yaml`.

## Grafana admin password

Defaults to the upstream `prom-operator`. Override out of git via
`kube-prometheus-stack.grafana.admin.existingSecret` pointing at a Secret pre-created in
`monitoring`.

## Install

```sh
helm dependency build monitoring
helm install homelab-monitoring monitoring -n monitoring --create-namespace
```

The release name `homelab-monitoring` is assumed by `gateway.serviceName`
(`homelab-monitoring-grafana`); change both together if you rename the release.
