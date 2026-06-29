# homelab-cilium

Cilium for the homelab, **installed by cluster-admin** into `kube-system`. Cilium was first
brought up imperatively with the `cilium` CLI at node bootstrap
(`kube-node/20-bootstrap-server.start`). This chart **adopts that running install** — the existing
`cilium` Helm release — with its values **unchanged**, then layers on the **Cilium service mesh**
(Gateway API + L7/Envoy), the **LoadBalancer stack** (Cilium LB IPAM + L2 announcements, which
**replaces MetalLB**), and a **single ingress Gateway** for `*.internal.haustorium.net`.

## Why this is its own chart (not under `homelab-platform`)

Cilium is the CNI: foundational networking that needs cluster-admin and lives in `kube-system`,
which the platform's Kyverno tier policies don't touch. Same rationale as `cert-manager/`. The
autonomous operator can't install or change it.

## Adopted config (do not "fix" it)

`cilium/values.yaml` → `cilium:` is a near-verbatim copy of the live release values
(`helm -n kube-system get values cilium`): pod CIDR `10.42.0.0/16`, `kubeProxyReplacement: true`
(already enabled on this cluster), `routingMode: tunnel` / `tunnelProtocol: vxlan`. Keeping these
identical makes the mesh/LB rollout a no-op for existing networking. The additions on top are
`gatewayAPI.enabled: true` and `l2announcements.enabled: true` (+ a raised `k8sClientRateLimit`).

> The live release recorded `k8sServiceHost: 127.0.0.1` (what the CLI inferred from k3s's loopback
> kubeconfig). We pin the server's routable `10.0.0.22` instead, so agent nodes can reach the API
> server under kube-proxy replacement — a deliberate, intended difference from the loopback value.

## Service mesh prerequisites

- **Gateway API CRDs** must be installed. They already are on this cluster — confirm with
  `kubectl get crd gateways.gateway.networking.k8s.io`.
- **kube-proxy replacement** is already in effect (`kubeProxyReplacement: true` in the live
  values). On a *fresh* node this is set up by the updated bootstrap, which starts k3s with
  `--disable-kube-proxy` and installs Cilium with `--set=kubeProxyReplacement=true`
  (`kube-node/20-bootstrap-server.start`). k3s runs kube-proxy in-process (not as a pod), so a node
  that already runs it must be reconfigured out-of-band before flipping the replacement on — these
  are Alpine nodes, so use `sh`:

  ```sh
  # re-run the k3s installer on the node with kube-proxy disabled
  kubectl node-shell k3s-server -- sh -c \
    'curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--disable-kube-proxy ..." sh -'
  ```

## LoadBalancer IPs (Cilium LB IPAM + L2 — replaces MetalLB)

Cilium assigns and ARP-announces LoadBalancer IPs itself, so **MetalLB is removed**. Configured under
`loadBalancer:` in `values.yaml`:

- **`gateway-pool`** — a single stable IP (`10.0.0.100`) for the ingress Gateway, selected by the
  gateway Service's `io.cilium.gateway/owning-gateway` label. This is the key reason Cilium (not
  MetalLB) does LB here: the gateway Service has **no endpoints** (Envoy is fronted by Cilium's BPF),
  and MetalLB-L2 refuses to announce an endpoint-less Service — Cilium has no such restriction.
- **`lab-pool`** — `10.0.0.101–150` for every other LoadBalancer Service (e.g. UDP services, which
  have real endpoints and announce fine).
- **`CiliumL2AnnouncementPolicy`** — answers ARP, **pinned to `k3s-server`** via `nodeSelector`
  (only that node is on the `10.0.0.x` LAN segment; `k3s-compute-spot` is `10.0.1.x` and can't ARP
  the pool). `l2announcements` needs `kubeProxyReplacement` (already on) and adds API load, so the
  chart raises `k8sClientRateLimit`.

### Migrating off MetalLB (one-time)

Do this **before/with** the cilium upgrade so the two IPAM controllers never fight over the range:

```sh
helm uninstall metallb -n metallb-system        # remove MetalLB controller/speaker + its pools
kubectl delete namespace metallb-system         # optional cleanup
helm upgrade cilium cilium -n kube-system        # installs the Cilium pools + L2 policy
kubectl -n kube-system rollout restart deployment/cilium-operator daemonset/cilium  # pick up l2announcements
```

## Install / adopt

```sh
helm dependency build cilium

# Upgrade the EXISTING release in place (release name stays `cilium`). Because the
# resources already belong to that release, Helm updates them rather than erroring on
# ownership — no --take-ownership needed.
helm upgrade cilium cilium -n kube-system

cilium status --wait
```

> If you ever install under a *different* release name, Helm will refuse the CLI-created objects
> ("exists and cannot be imported"); use `--take-ownership` (Helm ≥3.17) or re-annotate them.

## Host firewall (node lockdown)

TCP/SCTP into the **nodes** (the host network namespace — API server `6443`, kubelet `10250`, …)
is restricted to **in-cluster sources only**, plus the admin hosts in `hostFirewall.adminCIDRs`.
This is a Cilium *host policy* (`host-firewall.yaml` — a `CiliumClusterwideNetworkPolicy` with a
`nodeSelector`, enabled by `cilium.hostFirewall.enabled`). Cilium identities express "cluster"
directly, so the **DHCP agent nodes need no static IPs** — `remote-node`/`cluster` track them.

UDP is deliberately left open. Selecting the host flips host-ingress to default-deny for *all*
protocols (Cilium's default-deny is per-direction, not per-protocol), so the policy re-allows all
UDP from any source to keep **DHCP lease renewal, DNS, NTP and vxlan** alive. Net effect: only
**TCP/SCTP from `world`** (the rest of the LAN + internet) is dropped.

> **Roll out in audit mode first — a wrong allow-set can lock out the kubelet and brick the node.**
> There is no CRD field for audit; set it on each node's host endpoint (identity `1`), watch, then
> enforce. Hubble is **not** required — `cilium monitor` reads policy verdicts straight off the agent.
> Run this **per node** (once for each `cilium` pod):
> ```sh
> CILIUM=$(kubectl -n kube-system get pod -l k8s-app=cilium -o name | head -1)   # repeat per node
> HOST_EP=$(kubectl -n kube-system exec $CILIUM -- cilium endpoint list -o json \
>   | jq '.[] | select(.status.identity.id==1) | .id')
> kubectl -n kube-system exec $CILIUM -- cilium endpoint config $HOST_EP PolicyAuditMode=Enabled
>
> helm upgrade cilium cilium -n kube-system          # apply the policy (now audited, nothing dropped)
>
> # watch verdicts: in audit mode, traffic that WOULD drop is logged as `action audit` but allowed.
> # Confirm only unwanted world TCP/SCTP shows up (nothing from nodes/pods/admin hosts):
> hubble observe --type policy-verdict --verdict AUDIT           # cluster-wide, via Hubble relay
> #   or, with no Hubble at all, per-node off the agent:
> kubectl -n kube-system exec $CILIUM -- cilium monitor -t policy-verdict
>
> # once clean, enforce by turning audit back off:
> kubectl -n kube-system exec $CILIUM -- cilium endpoint config $HOST_EP PolicyAuditMode=Disabled
> ```
> Set `hostFirewall.adminCIDRs: []` for strictly cluster-only (no external kubectl path), or
> `hostFirewall.enabled: false` to drop the lockdown entirely.

## Hubble (observability)

`hubble.enabled` + `relay` + `ui` are on (`values.yaml` → `cilium.hubble`). The per-node Hubble
servers stream flows; `hubble-relay` aggregates them cluster-wide; `hubble-ui` is the dashboard.
TLS between agent and relay is auto-provisioned. Access the UI:

```sh
cilium hubble ui                         # port-forwards hubble-ui and opens a browser
# CLI flows (port-forward relay once: `cilium hubble port-forward &`):
hubble observe --follow
hubble status
```

To reach the UI through the shared Gateway instead of a port-forward, add an `HTTPRoute` for
`hubble.internal.haustorium.net` targeting the `hubble-ui` Service in `kube-system` — the gateway's
IP allow-list already restricts who can connect. (Not shipped by default; it's an ops dashboard.)

## The ingress Gateway

One `Gateway` (`gatewayClassName: cilium`) named `internal` in the `cilium-gateway` namespace, with
an HTTPS:443 + HTTP:80 listener for `*.internal.haustorium.net` and `allowedRoutes.from: All` so any
`app-*`/`tool-*` `HTTPRoute` can attach. The namespace carries `homelab.lab/ingress: "true"` so the
platform's generated default-ingress NetworkPolicy admits gateway→backend traffic.

- **DNS:** the `homelab.lab/hostname: "*.internal.haustorium.net"` annotation makes the
  opnsense-operator create a wildcard Unbound override pointing at the gateway's LB IP — the
  Cilium-assigned `10.0.0.100` on its backing `cilium-gateway-internal` Service. No
  `homelab.lab/expose`, so it's **internal-only** (no WAN port-forward).
- **NetworkPolicy (important):** Cilium enforces Gateway traffic at **two** policy boundaries —
  *clients → `ingress` proxy* and *`ingress` proxy → backend* — both using Cilium's reserved
  `ingress` identity. The platform generates a **default-deny** ingress NetworkPolicy per
  `app-*`/`tool-*` namespace, and a vanilla k8s NetworkPolicy can match neither boundary, so the
  route fails: **503** if the proxy→backend hop is dropped, **403 "Access denied"** (from the
  `cilium.l7policy` filter) if the client→proxy hop is denied. This chart ships two
  `CiliumClusterwideNetworkPolicy`s (gated by `gateway.allowBackendsFromIngress`):
  `allow-clients-to-gateway-ingress` (world/cluster → `ingress`) and
  `allow-gateway-ingress-to-backends` (`ingress` → all backends). They **union** with the
  per-namespace deny — they don't replace it — and since the `ingress` identity only originates
  from this gateway's Envoy, pod-to-pod isolation between namespaces is unchanged. (Labelling the
  gateway namespace `homelab.lab/ingress=true` does **not** help — there's no pod there to match;
  the traffic is the reserved `ingress` entity.)
- **TLS:** a cert-manager `Certificate` requests a wildcard cert for `*.internal.haustorium.net`
  into the `internal-haustorium-wildcard-tls` Secret, via the `letsencrypt-cloudflare` ClusterIssuer
  shipped by the **`cert-manager` chart — install that first.**

Tune the Gateway (name, namespace, hostname, issuer, TLS secret, `expose`) under `gateway:` in
`values.yaml`.

## Verify

```sh
kubectl get gatewayclass cilium                       # Accepted
kubectl get ciliumloadbalancerippool,ciliuml2announcementpolicy
kubectl -n cilium-gateway get gateway,certificate,secret
kubectl -n cilium-gateway get svc cilium-gateway-internal   # EXTERNAL-IP 10.0.0.100 (Cilium LB IPAM)
dig +short foo.internal.haustorium.net @10.0.0.1      # resolves to the gateway IP
```

Then attach an app: create an `HTTPRoute` in an `app-*` namespace with
`parentRefs: [{name: internal, namespace: cilium-gateway}]` and a hostname under the zone, and curl
it over HTTPS.
