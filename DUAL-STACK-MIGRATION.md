# Dual-stack (IPv4 + IPv6) migration plan

## Objective

Give **public-facing UDP services** (WebRTC media, and similar) a natively-routable **IPv6**
LoadBalancer IP, **without host networking** and **without re-imaging nodes**.

## Why this requires a cluster change (not a proxy)

- The v6 requirement is **UDP-only**. An HTTP/TCP edge proxy (HAProxy on OPNsense) can bridge
  v6→v4 for HTTP services with zero cluster changes, but it does **not** cleanly carry UDP/WebRTC
  (ICE needs the media server to *announce* a reachable candidate; UDP proxying is fiddly).
- A Kubernetes `LoadBalancer` Service cannot hold an IPv6 address unless the cluster is
  **dual-stack** — `spec.ipFamilies` is gated by the `service-cidr`. So Cilium LB IPAM has nothing
  to assign a v6 pool to in a single-stack cluster.
- A v6 Service needs **v6 endpoints**, i.e. pods with a v6 address. That means the **pod CIDR** must
  be dual-stack too (ULA is fine — private, not routable).
- Host networking is rejected: it puts the workload in the node netns (binds any node port, sees all
  host interfaces, sidesteps NetworkPolicy) — a strictly larger attack surface than a pod behind an LB.

Conclusion: dual-stack the cluster. Pods get a **private ULA** v6 (no routable space burned, internal
services unaffected); only services that opt in via `ipFamilyPolicy` get a routable v6 LB IP.

## Why a re-image isn't needed

- k3s bakes `--cluster-cidr` / `--service-cidr` at **server init**; they can't be safely mutated on a
  live cluster (existing Service ClusterIPs and node podCIDRs are allocated from the old ranges).
- But the fix is to **reset the k3s layer, not the OS**: `k3s-uninstall.sh` removes k3s + its
  datastore + CNI state and **leaves the node image intact**. Re-init with dual-stack flags, re-apply
  declarative state.
- **No stateful data**: PVCs carry nothing to preserve. The only non-Git state is the ad-hoc
  resources in `app-*` / `tool-*` namespaces. `agents/operator` is an **LLM sandbox** — those
  resources were created ad-hoc and **nothing reconciles them back**, so the transformed export *is*
  their source of truth: we capture → prune → transform → **recreate** them ourselves. "Infra as
  data": the migration is essentially a transform pass over that export.

> **Do NOT snapshot-restore.** An etcd/k3s snapshot restore re-imposes the old single-stack config and
> allocations — it undoes the whole point. Re-apply declarative manifests instead.

## Addressing plan

Delegated prefix: **`2001:db8:PREFIX::/56`** — *replace every `2001:db8:PREFIX` below with your real
delegated /56.* Pods/services use **ULA** (private); only the LB pool is globally routable.

Default announcement is **L2/NDP** (see "v6 LB announcement options"), so the **LB v6 pool is a slice
of the LAN /64, on-link** — symmetric with the v4 `lab-pool` sitting inside the LAN /24. (Only the
routed-fallback path uses a separate routed /64.)

| Prefix | Role | Routable? |
|---|---|---|
| `2001:db8:PREFIX:0::/64` | LAN segment (SLAAC to hosts; OPNsense Track Interface) — **also holds the LB v6 pool** | yes (on-link) |
| ↳ e.g. `2001:db8:PREFIX:0:ffff::/112` | **LB v6 pool** (Cilium LB IPAM), NDP-announced on-link by `k3s-server` | on-link |
| `2001:db8:PREFIX:100::/64` | *(routed-fallback only)* dedicated LB /64, static-routed / BGP to `k3s-server` | yes (routed) |
| `fd00:42::/56` | pod CIDR (ULA, private) | no |
| `fd00:43::/112` | service CIDR v6 (ULA, private) | no |

Existing single-stack (unchanged, kept as primary family):
- pod CIDR `10.42.0.0/16`, service CIDR `10.43.0.0/16` (k3s default), LB `lab-pool` `10.0.0.101–150`,
  gateway `10.0.0.100`.

## Prerequisites (verify before the window)

- [ ] **SSH on every node** — up to now access was `kubectl node-shell`, which **dies the moment k3s
      is uninstalled**. Install SSH *while the cluster is still up*, via node-shell, on each node
      (Alpine). Temporary + password auth is acceptable; removed in the final phase.
      ```sh
      # kubectl node-shell <node>, then:
      apk add openssh
      printf 'PermitRootLogin yes\nPasswordAuthentication yes\n' >> /etc/ssh/sshd_config
      passwd                      # set a temporary root password
      rc-update add sshd && rc-service sshd start
      ```
      Verify you can `ssh root@<node-ip>` to **every** node before proceeding — this is the only path
      to run the teardown/reinit once k3s (and node-shell) are gone.
- [ ] OPNsense DHCPv6-PD is delivering the **/56** (LAN hosts have a `2xxx`/`3xxx` GUA; `ip -6 addr`
      on `k3s-server`).
- [ ] v6 LB announcement (see "v6 LB announcement options"): **default L2/NDP** — confirm the Cilium
      version you'll run **includes #46332** (≥ 1.19.6 or 1.20; `cilium version`), and that your LAN
      path doesn't drop NDP multicast (MLD snooping, [#46274](https://github.com/cilium/cilium/issues/46274)).
      If the version is older or snooping bites, fall back to **static route** (routed /64).
- [ ] Confirm the host firewall already allows inbound UDP from `world` for v6 (the `host-firewall`
      CCNP re-allows all UDP from `world`, which covers both families) — so only an **OPNsense WAN
      firewall *allow*** is needed for inbound v6 UDP to the LB IPs (v6 has no DNAT either way).
- [ ] Planned downtime window agreed (cluster is fully down during the reset).

## Phase 0 — Capture non-Git state

Nothing in PVCs to back up. Everything below runs while the cluster is still up (before Phase 1).
The `app-*`/`tool-*` contents are ad-hoc LLM-sandbox output — no controller recreates them, so this
export is the **only** source for recreating them afterward. Capture:

1. **Ad-hoc workloads** in `app-*` / `tool-*` namespaces (owners only — not
   ReplicaSets/Pods/EndpointSlices, not Kyverno-generated objects):

   ```sh
   for ns in $(kubectl get ns -o name | grep -oE '(app|tool)-[a-z0-9-]+'); do
     kubectl get -n "$ns" deploy,sts,svc,cm,secret,httproute,cronjob -o yaml
   done > operator-state.raw.yaml
   ```

2. **Prune + transform** (drop generated objects; strip cluster-assigned identity; **strip Service
   family fields so they re-default in the new cluster**). `kubectl-neat` does most of it; the
   dual-stack-critical strips it may miss:
   - `.status`, `.metadata.{uid,resourceVersion,generation,creationTimestamp,managedFields,ownerReferences}`
   - Services: **`.spec.clusterIP`, `.spec.clusterIPs`, `.spec.ipFamilies`, `.spec.ipFamilyPolicy`**,
     `.spec.ports[].nodePort`
   - drop Kyverno-generated resources (`generate.kyverno.io/*` / `managed-by: kyverno`) and
     auto-created `kubernetes.io/service-account-token` Secrets

   Review the result by hand — this is the moment to **prune dead namespaces / obsolete workloads**.

3. **Manual Secrets** (not templated by any chart), e.g. `wireguard-privatekey`,
   `opnsense-operator` creds. cert-manager certs re-issue — skip.

## Phase 1 — Reset the k3s layer (OS untouched)

Run over **SSH** (node-shell is gone once k3s is uninstalled).

```sh
# agents first
/usr/local/bin/k3s-agent-uninstall.sh    # on each worker (k3s-compute, spot, ...)
# then the server
/usr/local/bin/k3s-uninstall.sh          # on k3s-server
```

**Clean up Cilium's kernel state.** `k3s-uninstall.sh` removes k3s + the CNI conf, but **not**
Cilium's datapath state — the `cilium_host` / `cilium_net` / `cilium_vxlan` / `cilium_health`
interfaces, the `lxc*` veths, the pinned eBPF maps under `/sys/fs/bpf`, and Cilium's iptables chains
all persist and will corrupt a fresh CNI install. Two options, per node:

- **Simplest (recommended): `reboot`.** A reboot clears all of the above (interfaces, BPF maps,
  non-persisted iptables). You're in a downtime window and re-initing anyway, so just reboot each
  node after uninstall.
- **Without reboot:** run Cilium's cleanup, then remove residue:
  ```sh
  cilium-dbg post-uninstall-cleanup -f    # if the binary is present on the node
  # else manually:
  for i in cilium_host cilium_net cilium_vxlan cilium_health; do ip link del "$i" 2>/dev/null; done
  ip -o link show | awk -F': ' '/lxc/{print $2}' | cut -d@ -f1 | xargs -rn1 ip link del
  rm -rf /sys/fs/bpf/tc/globals/cilium_* /var/run/cilium /var/lib/cilium
  rm -f /etc/cni/net.d/05-cilium.conf* /var/lib/rancher/k3s/agent/etc/cni/net.d/05-cilium.conf*
  ```

## Phase 2 — Re-init dual-stack

1. **k3s server** (in `kube-node/` bootstrap): add the second family to both CIDRs.
   ```
   --cluster-cidr=10.42.0.0/16,fd00:42::/56
   --service-cidr=10.43.0.0/16,fd00:43::/112
   # if the controller-manager doesn't infer it:
   --kube-controller-manager-arg=node-cidr-mask-size-ipv6=64
   ```
   Keep the existing flags (`--disable-kube-proxy`, flannel disabled for Cilium, `k8sServiceHost`, …).
2. **Cilium** (`cilium/values.yaml`) — enable v6 + add the LB pool (default: on-link, NDP-announced):
   ```yaml
   cilium:
     ipv6:
       enabled: true
     ipam:
       operator:
         clusterPoolIPv4PodCIDRList: 10.42.0.0/16
         clusterPoolIPv6PodCIDRList: [ fd00:42::/56 ]   # ULA, private
   loadBalancer:
     v6Pool:                                            # slice of the LAN /64 (on-link)
       name: lab-pool-v6
       cidr: "2001:db8:PREFIX:0:ffff::/112"
   ```
   - Add a `CiliumLoadBalancerIPPool` for the v6 blocks (mirror `loadbalancer.yaml`).
   - Default (L2/NDP): **extend the `CiliumL2AnnouncementPolicy` to cover the v6 pool** (same policy
     pinned to `k3s-server` that already does v4 ARP). Fallback (routed): drop the L2 policy for v6 and
     use a static route / BGP instead — see "v6 LB announcement options".
3. **Rejoin agents** — fresh registration → dual podCIDRs from birth (no per-node surgery).

## Phase 3 — Re-apply declarative state

Order matters (see root `CLAUDE.md`):

1. Foundational charts (human-installed, own namespaces): **cilium** → cert-manager → topolvm →
   seaweedfs → monitoring → opnsense-operator.
2. **platform** two-pass: `helm dependency build platform` → `./platform/install-crds.sh` →
   pass 1 (`policies.enabled=false operator.enabled=false`) → wait for Kyverno → pass 2 (full values).
3. Re-create the **manual Secrets** from Phase 0.
4. `kubectl apply -f operator-state.clean.yaml` (the pruned/transformed export). This is a plain
   recreate — nothing else reconciles these ad-hoc resources, so there's no ownership conflict to
   worry about; Kyverno re-generates the governance/default-ingress NetworkPolicies on top.
5. **CoreDNS: strip AAAA for pod egress** (thin dual-stack — pods have a ULA v6 but no v6 egress
   route). Without this, a pod resolving a dual-stack external name may try the AAAA over the
   non-routable ULA and fall back to v4 (happy-eyeballs delay). Return **NODATA** for AAAA so pods
   see only A. Safe here: internal services are v4-only, and external clients resolve the public v6
   service via **OPNsense Unbound, not CoreDNS**, so their AAAA is unaffected. Set it via the k3s
   CoreDNS override (the managed `coredns` ConfigMap / addon), not a hand-edit that gets reconciled:
   ```
   .:53 {
       kubernetes cluster.local in-addr.arpa ip6.arpa {
           pods insecure
           fallthrough in-addr.arpa ip6.arpa
       }
       template ANY AAAA {
           rcode NOERROR      # NODATA ("no AAAA here") -> clients use A. NOT NXDOMAIN.
       }
       forward . /etc/resolv.conf
       cache 30
       # ...rest of the default Corefile unchanged
   }
   ```
   Verify from a pod: `nslookup -type=AAAA example.com` → NODATA; `-type=A` still resolves;
   in-cluster names still resolve.

## v6 LB announcement options

The IPv4 path uses **Cilium L2 announcements (ARP)** — the LB IP is on-link in the LAN /24 and
`k3s-server` answers ARP. For IPv6, use the symmetric NDP path if the Cilium version is new enough,
else fall back to routing:

- **L2 / NDP — default (needs a fixed Cilium).** Cilium gained IPv6 L2 announcement (NDP) in **1.19.0**
  (PR #39648). The early releases had a blocking bug — the announcer answered *unicast* NDP but never
  joined the LoadBalancer IP's solicited-node multicast group, so on-segment discovery failed
  ([#44311](https://github.com/cilium/cilium/issues/44311),
  [#43774](https://github.com/cilium/cilium/issues/43774)). **That fix is merged**
  ([#46332](https://github.com/cilium/cilium/pull/46332), main 2026‑06‑30, uses L3 sockets + MLD joins)
  and **backported to v1.19** (done 2026‑07‑08, [#46845](https://github.com/cilium/cilium/pull/46845)),
  so it ships in the **next 1.19 patch (≥ 1.19.6)** and 1.20+. On a fixed version this is the cleanest
  option and **symmetric with the v4 setup**: the v6 LB pool lives **on-link in the LAN /64**, Cilium
  NDP-announces it, OPNsense resolves it as an on-segment neighbor — **no routed /64, no static route,
  no BGP.** Just extend the `CiliumL2AnnouncementPolicy` to the v6 pool and add the WAN firewall allow.
  - **Gate:** confirm the running Cilium actually includes #46332 (`cilium version`; ≥ 1.19.6 or 1.20)
    before relying on it — on an older 1.19.x it's still broken.
  - **Caveat:** upstream switches/bridges doing **MLD/multicast snooping** can still drop the NDP
    multicast even with the fix ([#46274](https://github.com/cilium/cilium/issues/46274)) — verify on
    your actual LAN path (notably relevant if a node sits behind a Proxmox/Linux bridge).
- **Static route — fallback.** If you're on a Cilium without the fix, dedicate a *routed* /64
  (`lab-pool-v6`) and route it on OPNsense to `k3s-server`'s GUA; Cilium's BPF LB serves it. Zero
  daemons, single next-hop (no failover). **Verify** Cilium serves a *routed* LB IP it didn't announce
  (expected — BPF LB matches on destination — but test it; see Open questions).
- **BGP — if you want failover.** Cilium BGP control plane peers with OPNsense FRR and advertises the
  LB prefix; gives failover/ECMP at the cost of a BGP daemon on the firewall (os-frr) + a session to
  maintain.

## Phase 4 — Expose a UDP service on v6

For each public UDP service that needs v6 (e.g. WebRTC media / TURN):

1. Service: dual-stack + request the v6 pool.
   ```yaml
   spec:
     type: LoadBalancer
     ipFamilyPolicy: PreferDualStack
     ipFamilies: [IPv4, IPv6]
     externalTrafficPolicy: Local   # preserve client IP; pins delivery to a node with an endpoint
   ```
   - v4 comes from `lab-pool`, v6 from `lab-pool-v6`.
   - With `Local`, pin the pod to the announcing node (`k3s-server`) — same pattern as WireGuard — or
     use `Cluster` and accept SNAT.
2. **OPNsense**: add a **WAN inbound v6 firewall `pass`** to the LB IPs on the media/relay UDP ports.
   No DNAT. With the default L2/NDP path the LB IP is on-link, so **no route is needed** — OPNsense
   NDP-resolves it as a LAN neighbor. (Routed-fallback only: add the static route to `k3s-server`.)
3. **App config**: set the media server's `announcedIp` / `external-ip` to the **LB v4 and v6** so ICE
   advertises correct candidates (the pod sees only its own pod IP).

## Phase 5 — Remove SSH

Once the control plane is back and `kubectl node-shell` works again, undo the temporary SSH access on
every node (it was a bootstrap crutch, not a permanent surface):

```sh
# via node-shell again:
rc-service sshd stop && rc-update del sshd
apk del openssh
# (or, to keep sshd but drop the temporary exposure: remove the PermitRootLogin/PasswordAuthentication
#  lines and expire the root password)
```

Confirm port 22 is closed on each node afterward.

## Verification

- [ ] `kubectl get node -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.podCIDRs}{"\n"}{end}'`
      → each node lists a v4 **and** v6 podCIDR.
- [ ] A fresh pod has both a v4 and an `fd00:42:…` address.
- [ ] A `PreferDualStack` Service shows a v6 in `.status.loadBalancer.ingress` from the v6 pool.
- [ ] From off-LAN over IPv6: traffic reaches the LB IP (firewall/route correct) and the pod responds.
- [ ] Internal/default services are still **v4-only** (unchanged) — only opted-in services are v6.

## Rollback

The cluster is declarative and stateless here, so rollback = re-init single-stack and re-apply:

1. `k3s-uninstall.sh` / agent uninstall.
2. Re-init with the **original single-stack** CIDRs, single-stack Cilium values.
3. Re-apply charts + `operator-state.clean.yaml` (the family-field strips are forward-compatible).

Keep `operator-state.clean.yaml` and the pre-change `cilium/values.yaml` until the migration is
verified in production.

## Open questions / decisions

- **v6 LB announcement**: default **L2/NDP** (needs Cilium ≥ 1.19.6 / 1.20 with #46332); fallbacks
  static route or BGP — see "v6 LB announcement options". At migration time, confirm the running
  Cilium includes #46332 and that your LAN path passes NDP multicast (no MLD-snooping drop, #46274);
  if either fails, use the routed /64 static-route fallback and verify Cilium serves a routed LB IP.
- **Real /56 subnetting**: confirm the LAN /64 OPNsense already uses (Track Interface); the default
  L2/NDP `lab-pool-v6` is a slice *of that same /64* (on-link). Only the routed fallback needs a
  separate /64 from the /56.
- **`externalTrafficPolicy`** per UDP service: `Local` (client IP, node-pinned) vs `Cluster` (float,
  SNAT) — likely `Local` + pin to `k3s-server`.
- **Cilium cleanup method**: reboot (simplest) vs `cilium-dbg post-uninstall-cleanup` — confirm the
  binary ships on the Alpine node image, else default to reboot.
