# Homelab Infrastructure Design Document

**OPNsense + k3s + Hypha**

---

## Table of Contents

1. [Overview](#1-overview)
2. [Hardware](#2-hardware)
3. [Network Architecture](#3-network-architecture)
4. [ISP & WAN Connectivity](#4-isp--wan-connectivity)
5. [Kubernetes (k3s) Cluster](#5-kubernetes-k3s-cluster)
6. [Container Networking (CNI)](#6-container-networking-cni)
7. [Storage Architecture](#7-storage-architecture)
8. [Database Layer](#8-database-layer)
9. [Backup & Disaster Recovery](#9-backup--disaster-recovery)
10. [DNS & Privacy](#10-dns--privacy)
11. [Security Considerations](#11-security-considerations)
12. [Operational Procedures](#12-operational-procedures)
13. [IP Address Plan](#13-ip-address-plan)

---

## 1. Overview

This document describes the design of a personal homelab infrastructure located. The system provides network segregation, Kubernetes-based container orchestration, a caching and encrypting S3 gateway (Hypha), encrypted offsite backups, and privacy-preserving DNS resolution.

The design prioritizes simplicity, minimal resource overhead, and strong backup/recovery over high availability and replication. The assumption is that downtime during recovery is acceptable for a homelab, but data loss is not.

---

## 2. Hardware

### 2.1 Bill of Materials

| Component | Model | Role |
|-----------|-------|------|
| Firewall/Router | Topton M1 (Intel N100, 4× 2.5G i226-V) | OPNsense firewall, router, DHCP, DNS |
| Managed Switch | Not required (4 NIC ports available) | N/A |
| Unmanaged Switch | Horaco (8-port, unmanaged) | LAN port multiplier on Port 2 |
| Wireless AP | TP-Link Archer AX55 (AP mode) | WiFi subnet, dedicated Port 3 |
| Compute Nodes | Multiple N100 mini PCs (Alpine Linux) | k3s workers, SeaweedFS storage nodes |
| Database Node | Dedicated mini PC | PostgreSQL, local path provisioner |
| DAS Enclosure | TerraMaster D4-320 (USB 3.2 Gen2, 4-bay) | Database tablespaces via direct mount |

### 2.2 Topton M1 Port Assignments

| Port | Interface | Connection | Subnet |
|------|-----------|------------|--------|
| Port 1 | WAN | Telus NAH (bridge mode) | DHCP from ISP |
| Port 2 | LAN | Horaco unmanaged switch | 10.0.0.0/24 |
| Port 3 | WIFI | AX55 (AP mode, direct) | 10.0.1.0/24 |
| Port 4 | Spare | Future use (IoT, guest, lab) | TBD |

### 2.3 TerraMaster D4-320 Configuration

Connected to the database node via USB 3.2 Gen2 (10Gbps). Each drive is presented as an individual block device (no hardware RAID). Software RAID5 via mdadm provides single-drive fault tolerance. The RAID array is used for PostgreSQL tablespaces housing large/cold datasets.

```bash
mdadm --create /dev/md0 --level=raid5 --raid-devices=4 \
  /dev/sda /dev/sdb /dev/sdc /dev/sdd
```

---

## 3. Network Architecture

### 3.1 Topology

```
Internet
  → Telus NAH (bridge mode, XGS-PON auth)
    → Topton M1 Port 1 (WAN, public IP via DHCP)
    → Topton M1 Port 2 (LAN) → Horaco switch → wired devices, k3s nodes
    → Topton M1 Port 3 (WIFI) → AX55 (AP mode) → wireless clients
    → Topton M1 Port 4 (spare)
```

### 3.2 Subnet Design

Each physical interface on OPNsense has its own subnet. Network segregation is achieved physically via dedicated ports rather than VLANs, eliminating the need for a managed switch.

| Subnet | CIDR | Purpose | Gateway |
|--------|------|---------|---------|
| LAN | 10.0.0.0/24 | Trusted wired devices, k3s nodes | 10.0.0.1 |
| WIFI | 10.0.1.0/24 | Wireless clients (segregated) | 10.0.1.1 |
| k3s Pods | 10.42.0.0/16 | Internal to cluster (CNI managed) | N/A |
| k3s Services | 10.43.0.0/16 | Internal to cluster | N/A |
| LB IP pool | 10.0.0.100–150 | LoadBalancer service IPs (Cilium LB IPAM + L2) | N/A |

### 3.3 OPNsense Interface Configuration

**LAN interface:**
- IPv4 Configuration: Static
- IP: 10.0.0.1/24
- DHCP range: 10.0.0.200–250

**WIFI interface (renamed from OPT1):**
- IPv4 Configuration: Static
- IP: 10.0.1.1/24
- DHCP range: 10.0.1.100–200

### 3.4 Firewall Rules

The WiFi subnet is fully segregated from the LAN. Rules on the WIFI interface (order matters):

1. **Block** — Source: WIFI subnet → Destination: LAN subnet
2. **Block** — Source: WIFI subnet → Destination: OPNsense GUI
3. **Allow** — Source: WIFI subnet → Destination: any (internet)

### 3.5 DNS Redirect

A NAT port forward rule intercepts all outbound DNS (port 53) from all interfaces and redirects to OPNsense's Unbound resolver. This prevents devices from bypassing local DNS by hardcoding external resolvers (e.g., Google devices using 8.8.8.8).

**Firewall → NAT → Port Forward:**
- Interface: LAN, WIFI
- Protocol: TCP/UDP
- Destination: any (but not OPNsense itself)
- Destination port: 53
- Redirect target: 127.0.0.1:53

### 3.6 Wireless AP Configuration

The TP-Link Archer AX55 runs in AP mode. It does not perform NAT, DHCP, or routing. OPNsense handles DHCP and gateway duties for all wireless clients via the Port 3 interface. From OPNsense's perspective, each WiFi client is individually visible with its own IP and MAC address, as if plugged directly into Port 3 with a cable.

---

## 4. ISP & WAN Connectivity

### 4.1 Telus PureFibre

The connection is Telus PureFibre. The Telus-provided NAH (Network Access Hub) is a combo unit containing an XGS-PON SFP module and integrated router.

### 4.2 Bridge Mode

The NAH is configured in bridge mode. In this mode, the NAH continues to handle XGS-PON authentication and PPPoE at L2 but passes the resulting connection through to OPNsense transparently. OPNsense receives a public IP via DHCP. The NAH performs no NAT, no firewall, and no routing.

The L2 flow in bridge mode:

```
OPNsense DHCP Discover (broadcast)
  → passes through NAH bridge
  → NAH encapsulates inside PPPoE session
  → reaches Telus DHCP server

Telus DHCP Offer (public IP)
  → comes down PPPoE session to NAH
  → NAH passes through bridge to OPNsense

OPNsense now has public IP on WAN interface
```

### 4.3 Authentication

Authentication is handled entirely at the optical/SFP layer. On XGS-PON, both the SFP serial number and NAH serial number authenticate with Telus's OLT via PPPoE. OPNsense is not involved in authentication and does not need ISP credentials.

### 4.4 XGS-PON Architecture

XGS-PON provides 10Gbps shared capacity over a single fibre strand using wavelength division multiplexing (WDM). Up to 32 homes share one fibre via passive optical splitters.

| Direction | Wavelength | Protocol |
|-----------|------------|----------|
| XGS-PON downstream | 1577nm | Broadcast, AES encrypted |
| XGS-PON upstream | 1270nm | TDMA time slots |
| GPON downstream | 1490nm | Broadcast, optional AES |
| GPON upstream | 1310nm | TDMA time slots |

GPON and XGS-PON coexist on the same fibre using different wavelengths.

### 4.5 Speed Considerations

The Topton M1 has 2.5G WAN ports, capping effective throughput at 2.5Gbps regardless of ISP plan. ISP plans above 1Gbps use statistical multiplexing and advertise "up to" speeds. For homelab use, 1Gbps is sufficient for most workloads. Internal network speed (2.5G switch-to-node) is a more meaningful upgrade target than WAN bandwidth.

---

## 5. Kubernetes (k3s) Cluster

### 5.1 Architecture Overview

k3s is chosen for its single-binary design, minimal resource footprint, and clean power-loss recovery. The control plane (API server, scheduler, controller manager, etcd) runs as a host process on the server node, not as pods inside the cluster. This eliminates bootstrap/recovery complexity — the control plane doesn't depend on the cluster being healthy to function.

### 5.2 Node Topology

| Node | Role | IP | Storage |
|------|------|----|---------|
| k3s-server | Server + worker | 10.0.0.21 | NVMe (OS 100GB, vg-nvme remainder) |
| compute-2 | Worker | 10.0.0.22 | NVMe (OS 100GB, vg-nvme remainder) |
| compute-3 | Worker | 10.0.0.23 | NVMe (OS 100GB, vg-nvme remainder) |
| db | Worker (tainted) | 10.0.0.24 | NVMe (OS) + D4-320 (4× HDD) |

### 5.3 Installation

Server node:

```bash
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--disable=traefik --flannel-backend=none --disable-network-policy" sh -
```

Agent nodes:

```bash
curl -sfL https://get.k3s.io | K3S_URL=https://10.0.0.21:6443 K3S_TOKEN=<token> sh -
```

### 5.4 Disabled Default Components

- **Traefik:** Disabled. Cilium Gateway API handles all TCP ingress through a single shared Gateway (one LoadBalancer IP). UDP services that cannot traverse the Gateway get their own dedicated LoadBalancer IP. LoadBalancer IPs are assigned by Cilium's built-in **LB IPAM** and ARP-announced by Cilium **L2 announcements** — MetalLB is not used (see `references/cilium.md`).
- **Default CNI (Flannel):** Replaced with Cilium in native routing mode.
- **Default network policy controller:** Disabled when using Cilium.

### 5.5 Database Node Isolation

The database node is tainted to prevent non-database workloads from scheduling on it:

```bash
kubectl taint nodes db workload=database:NoSchedule
```

Database pods include tolerations and node affinity:

```yaml
spec:
  tolerations:
    - key: workload
      value: database
      effect: NoSchedule
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
        - matchExpressions:
            - key: node-role
              operator: In
              values: ["database"]
```

### 5.6 Service Exposure

Traffic flows through three layers:

1. **Service mesh (east-west TCP)** — Cilium's sidecarless service mesh intercepts all TCP traffic between pods using per-node Envoy proxies and eBPF socket redirection. No sidecars are injected into pods. This provides L7 observability (via Hubble), traffic management, and a consistent mTLS-capable data plane for all in-cluster TCP communication.

2. **North-south ingress** — The only externally accessible entry point for TCP is a shared `Gateway` object backed by a single LoadBalancer IP (assigned by Cilium LB IPAM, ARP-announced by Cilium L2). Traffic is routed by hostname via `HTTPRoute` objects. No TCP service is exposed via `type: LoadBalancer` directly — all inbound TCP must enter through the Gateway. UDP services that cannot traverse the Gateway each get their own dedicated LoadBalancer IP via `type: LoadBalancer`.

3. **WAN exposure** — A custom OPNsense operator watches `Gateway` and `Service` objects for an annotation and creates/removes the corresponding WAN port forward rule via the OPNsense API. Status is written back to the standard Kubernetes status fields (`status.addresses` on `Gateway`, `status.loadBalancer.ingress` on `Service`), so `kubectl get gateway,svc -A` shows the full external exposure map.

This follows the same annotation-driven pattern as the AWS Load Balancer Controller in EKS.

**HTTP/HTTPS (shared gateway):**

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: homelab-gateway
  annotations:
    opnsense.lab/expose: "true"       # operator creates WAN:443 → LoadBalancer IP
spec:
  gatewayClassName: cilium
  listeners:
  - name: https
    port: 443
    protocol: HTTPS
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: grafana
spec:
  parentRefs:
  - name: homelab-gateway
  hostnames: ["grafana.lab", "grafana.home.example.com"]
  rules:
  - backendRefs:
    - name: grafana
      port: 3000
```

**UDP (dedicated IP, unchanged):**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: game-server-udp
  annotations:
    opnsense.lab/expose: "true"
    opnsense.lab/external-port: "27015"
spec:
  type: LoadBalancer
  ports:
  - port: 27015
    protocol: UDP
```

**DNS** — Two wildcard zones both resolve to the Gateway's LoadBalancer IP, allowing any `HTTPRoute` hostname to work automatically without per-service DNS entries:

- **Private (`*.lab`)** — Unbound wildcard host override: `*.lab → 10.0.0.100`. Any `.lab` query from the LAN resolves to the Gateway. See section 13.4.
- **Public (`*.home.example.com`)** — Wildcard A record at the registrar pointing to the WAN IP. The OPNsense operator's port forward on the annotated Gateway routes inbound HTTPS to the Gateway LoadBalancer IP.

UDP services are excluded from these wildcards; each retains an individual Unbound host override pointing to its own dedicated LoadBalancer IP.

### 5.7 Power Loss Recovery

k3s recovers automatically from hard power loss. The control plane runs as a host process and etcd replays its WAL on startup. Agent nodes retry connection to the server URL indefinitely until the server comes up. Boot order does not matter.

Realistic power outage timeline:

```
0:00  — Power returns
0:30  — Nodes POST and boot Alpine
1:00  — k3s server starts, etcd recovers
1:15  — k3s agents connect, nodes go Ready
1:30  — Scheduler starts placing pods
2:00  — TopoLVM node plugins start, SeaweedFS volume servers come online
2:30  — SeaweedFS S3 ready, hypha starts, S3 available
3:00  — Everything running normally
```

### 5.8 Upgrades

k3s upgrades are performed one minor version at a time (e.g., v1.32 → v1.33 → v1.34, never skipping). Server node is upgraded first, then agent nodes. Verify Cilium/SeaweedFS compatibility before upgrading. The Rancher System Upgrade Controller can automate this process.

---

## 6. Container Networking (CNI)

### 6.1 Options Considered

| CNI | Overlay | Network Policy | RAM/Node | Notes |
|-----|---------|----------------|----------|-------|
| Flannel (default) | VXLAN (+50 bytes/pkt) | No | ~20–30MB | Simplest, lowest overhead |
| Cilium (native routing) | None | Yes (eBPF) | ~100–150MB | Better routing, extensible |
| Calico | Optional (BGP default) | Yes | ~50–100MB | Good middle ground |

### 6.2 Recommended Configuration

Cilium in native routing mode with Gateway API, Hubble observability, and the Cilium service mesh enabled in sidecarless mode.

Gateway API CRDs must be installed before Cilium:

```sh
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.1/standard-install.yaml
```

Cilium install flags (run during bootstrap):

```sh
cilium install \
  --set=ipam.operator.clusterPoolIPv4PodCIDRList="10.42.0.0/16" \
  --set=gatewayAPI.enabled=true \
  --set=hubble.relay.enabled=true \
  --set=hubble.ui.enabled=true \
  --set=envoy.enabled=true
```

**Hubble** taps the eBPF datapath and provides per-flow metrics, DNS query visibility, HTTP request/response metadata, service dependency maps, and drop reasons — all with zero application instrumentation. The Hubble UI is exposed as a service on the cluster.

**Cilium service mesh (sidecarless)** — Unlike Istio and Linkerd, Cilium does not inject sidecar proxies into pods. Instead, a shared Envoy proxy runs on each node, and eBPF socket redirection transparently routes TCP traffic through it. This provides L7 traffic management and enhanced Hubble visibility for all TCP services with no changes to application pods. All external TCP access is restricted to the shared Gateway; pods and ClusterIP services are not directly reachable from outside the cluster.

### 6.3 Cross-Node Pod Communication

With Flannel (VXLAN), cross-node pod traffic is encapsulated inside regular UDP packets between node IPs. The Horaco switch sees normal LAN traffic. OPNsense is not involved in same-subnet traffic.

With Cilium native routing, pod traffic routes directly between nodes without encapsulation, using eBPF-programmed routing tables. Lower overhead, same external behavior.

In both cases, k3s handles cross-node pod networking automatically — no static routes or special OPNsense configuration required. The physical network only ever sees regular node-to-node communication.

---

## 7. Storage Architecture

### 7.1 Design Philosophy

Storage is split by workload type. The database node uses local storage for performance. Server and compute nodes contribute spare NVMe capacity to the cluster's object storage, provided by **Hypha** — a caching + encrypting S3 gateway fronting SeaweedFS. This replaces the previously-planned Ceph/Rook RGW stack, which was built but never deployed (so there is no data to migrate).

Hypha's design — data path, conditional writes, encryption, tiering, and recovery — lives in [`hypha/ARCHITECTURE.md`](../../hypha/ARCHITECTURE.md). This section covers only how it sits in the cluster.

### 7.2 Storage Layout

| Node Type | Storage | Provisioner | Use Case |
|-----------|---------|-------------|----------|
| Database node | Internal NVMe | Local Path Provisioner | WAL, default tablespace |
| Database node | D4-320 (4× HDD, RAID5) | Direct mount | Large tablespaces |
| Server + compute nodes | Spare NVMe partition | TopoLVM (`vg-nvme`) | SeaweedFS volumes (hypha's S3 cache) |

### 7.3 Storage Node Disk Partitioning

Each server/compute node's NVMe is partitioned during Alpine installation (`kube-node/10-provision.start`):

| Partition | Size | Format | Purpose |
|-----------|------|--------|---------|
| nvme0n1p1 | 512MB | FAT32 (ESP) | EFI boot |
| nvme0n1p2 | 100GB | ext4 | Alpine OS + k3s |
| nvme0n1p3 | Remainder | LVM PV (raw) | `vg-nvme` — SeaweedFS via TopoLVM |

`p3` is the partition formerly earmarked for Ceph (still GPT-labelled `ceph`); it is now an LVM physical volume in the `vg-nvme` volume group. The `db` and `compute-spot` roles have no `p3` — root claims the whole disk — and provide no object storage.

### 7.4 Object Storage — Hypha

Object storage is three foundational charts, all cluster-admin installed (not operator-deployed):

- **`homelab-topolvm`** — CSI driver provisioning node-local logical volumes from the `vg-nvme` volume group (each storage node's `nvme0n1p3`, the ex-Ceph partition). Provides the `topolvm-provisioner` StorageClass; scoped to nodes labelled `vg=nvme` (replacing Ceph's `ceph-osd=true`).
- **`homelab-seaweedfs`** — SeaweedFS as the hot cache tier: one volume server per storage node on `topolvm-provisioner` PVCs, replication off (no local redundancy).
- **`hypha`** — the gateway, an encrypting S3 proxy whose cache is optional. It is deployed twice: a cached deployment (`s3.internal.haustorium.net`, via the shared Cilium Gateway, replacing `rook-ceph-rgw-s3-store`) fronting SeaweedFS as the default tier, and a cacheless deployment (`s3-direct.internal.haustorium.net`) that writes synchronously to the remote for clients that cannot tolerate loss (e.g. ZeroFS). Each deployment is scoped to its own remote namespace — a separate account/bucket, or a shared remote under a forced key prefix.

Object bodies are encrypted client-side and continuously replicated to the remote for durability (local redundancy is off; names and metadata stay plaintext, as with standard S3 client-side encryption). The cache holds no unique state — on loss it is discarded and repopulates from the remote. The encryption scheme, conditional writes, tiering, and the mirror-vs-versioned-backup trade-off are all covered in [`hypha/ARCHITECTURE.md`](../../hypha/ARCHITECTURE.md).

---

## 8. Database Layer

> **Deferred.** The database design (PostgreSQL via CloudNativePG, Barman Cloud offsite, tablespace/WAL layout) is out of date and being reworked; it will be documented in a later revision. The database node hardware and its scheduling isolation (§5.5) still stand.

---

## 9. Backup & Disaster Recovery

### 9.1 Design Principles

- All backups are encrypted client-side before leaving the network. The backup provider sees only encrypted blobs.
- Offsite copies go to an S3-compatible remote (e.g. Backblaze B2, ~$6/TB/month).
- Replication is not used for local data protection. For object storage, hypha continuously replicates an encrypted copy to the remote; whether that copy is a plain mirror or a versioned backup is a remote-bucket configuration choice (see §7.4), not a property of hypha.
- Encryption keys must be stored outside the cluster (password manager, printed in a safe). Losing them renders all backups unrecoverable.

### 9.2 Object Storage Offsite (Hypha)

Object storage has no separate backup job: Hypha continuously replicates an encrypted copy of every object to a remote S3 endpoint as it is written (see §7.4). Whether that remote copy is a plain mirror or a point-in-time backup (recoverable deletes/overwrites) is a remote-bucket versioning choice. Full detail in [`hypha/ARCHITECTURE.md`](../../hypha/ARCHITECTURE.md).

### 9.3 Full Cluster Disaster Recovery

In the event of complete cluster loss, the rebuild order is:

1. Install Alpine on all nodes (automated via custom ISO)
2. Install k3s (server first, then agents)
3. Deploy the cilium chart (service mesh + LB IPAM/L2 + ingress Gateway), cert-manager, and the custom OPNsense operator
4. Deploy homelab-topolvm and homelab-seaweedfs, then hypha; the cache starts empty and repopulates from the remote on demand
5. Reconfigure OPNsense port forwards (operator handles automatically)

Steps 1–4 should be scripted and stored in an external git repository for rapid rebuilds. Target recovery time: under 1 hour excluding data restore.

### 9.4 Offsite Architecture Diagram

```
Object storage (SeaweedFS on server/compute NVMe)
  → Hypha (write-through, encrypted, continuous)
    → remote S3 endpoint

Encrypted client-side. The remote provider sees nothing.
```

---

## 10. DNS & Privacy

### 10.1 Architecture

OPNsense runs Unbound as the local DNS resolver for all subnets. All devices use OPNsense (10.0.0.1) as their DNS server, assigned via DHCP. A NAT redirect rule on port 53 forces all DNS traffic through Unbound regardless of device configuration.

Unbound forwards to dnscrypt-proxy on localhost, which provides Anonymized DNSCrypt resolution.

```
Device DNS query
  → OPNsense Unbound (10.0.0.1:53)
    → dnscrypt-proxy (127.0.0.1:5300)
      → Anonymized DNSCrypt relay (encrypted, can't read query)
        → DNSCrypt resolver (decrypts, resolves, sees only relay IP)
```

### 10.2 Anonymized DNSCrypt Privacy Model

- The **relay** sees the client's IP but cannot decrypt the query (end-to-end encrypted to the resolver). Zero trust required.
- The **resolver** sees the query content but only the relay's IP, not the client's.
- No single entity has both the client's identity and query content simultaneously.
- Resolvers and relays are selected from different operators to prevent collusion.
- Only DNSCrypt protocol is enabled; DoH is disabled since it cannot be anonymized through relays.

### 10.3 dnscrypt-proxy Configuration

```toml
listen_addresses = ['127.0.0.1:5300']

require_dnssec = true
require_nolog = true
require_nofilter = true

dnscrypt_servers = true
doh_servers = false

# Disable these as resolvers since we use them as relays
disabled_server_names = ['cs-fr', 'bcn-dnscrypt']

[sources]
  [sources.public-resolvers]
  urls = ['https://raw.githubusercontent.com/DNSCrypt/dnscrypt-resolvers/master/v3/public-resolvers.md']
  cache_file = '/var/cache/dnscrypt-proxy/public-resolvers.md'
  minisign_key = 'RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1CBM0QTaLn73Y7GFO3'

  [sources.relays]
  urls = ['https://raw.githubusercontent.com/DNSCrypt/dnscrypt-resolvers/master/v3/relays.md']
  cache_file = '/var/cache/dnscrypt-proxy/relays.md'
  minisign_key = 'RWQf6LRCGA9i53mlYecO4IzT51TGPpvWucNSCh1CBM0QTaLn73Y7GFO3'

[anonymized_dns]
routes = [
  { server_name='*', via=['anon-cs-fr', 'anon-bcn'] }
]
skip_incompatible = true
```

### 10.4 Internal DNS

OPNsense's DHCP static mappings automatically register hostnames in Unbound. Devices set their own hostname during provisioning, and OPNsense picks it up during DHCP lease registration.

Enable in OPNsense: **Services → Unbound DNS → General → Register DHCP leases ✅**

**Wildcard zone for TCP services** — Instead of per-service host overrides, a single Unbound wildcard entry covers all TCP services routed through the Gateway:

```
Host Override: *.lab → 10.0.0.100  (Gateway LoadBalancer IP)
```

Any `<service>.lab` query from a LAN or WiFi device resolves to the Gateway. The Gateway then routes by `Host:` header to the correct backend via `HTTPRoute`. Adding a new TCP service requires only a new `HTTPRoute` — no DNS change needed.

**UDP services** — Each UDP service retains an individual Unbound host override pointing to its own dedicated LoadBalancer IP (unchanged from before).

**Public wildcard zone** — A wildcard A record `*.home.example.com → <WAN IP>` is configured at the DNS registrar. The OPNsense operator's WAN port forward on the annotated Gateway routes inbound HTTPS to the same Gateway LoadBalancer IP. An `HTTPRoute` can match on both the `.lab` and `.home.example.com` hostnames simultaneously to serve private and public clients from the same backend. See section 5.6.

### 10.5 Comparison of DNS Privacy Approaches

| Approach | ISP Sees Queries | Resolver Sees IP | Latency |
|----------|-----------------|------------------|---------|
| Plain recursive | Yes | N/A (distributed) | Low |
| DoT to forwarder | No | Yes | Low |
| ODoH | No | No (if no collusion) | Medium |
| Anonymized DNSCrypt | No | No | Low–Medium |
| DNS over Tor | No | No | High |

Anonymized DNSCrypt provides equivalent privacy to ODoH with a larger relay ecosystem, lighter protocol (UDP vs HTTPS), and more resolver diversity.

---

## 11. Security Considerations

### 11.1 Network Security

- WiFi subnet is physically segregated from LAN via dedicated OPNsense port with firewall rules blocking cross-subnet traffic.
- The Horaco unmanaged switch has no management interface, eliminating remote management attack surface. Physical access is the only concern.
- All DNS traffic is encrypted via Anonymized DNSCrypt. Rogue DNS is prevented by port 53 NAT redirect.
- OPNsense WAN blocks all unsolicited inbound traffic by default.

### 11.2 ISP / L1 Security

GPON upstream traffic is unencrypted at the optical layer per specification. XGS-PON adds AES encryption but downstream is broadcast to all subscribers on the same splitter. Known vulnerabilities include rogue ONT disruption (transmitting outside assigned time slots), potential eavesdropping via fibre trunk tapping, and weak PLOAM key exchange.

Practical mitigation: encrypt all traffic at L3+ (HTTPS, WireGuard, TLS, encrypted DNS). The GPON/XGS-PON layer is not a security boundary — application-layer encryption is the real protection.

### 11.3 Backup Security

- Hypha: object bodies encrypted client-side before they reach the remote (standard S3 client-side encryption; key names and metadata are not encrypted)
- The offsite remote has no visibility into object contents (bodies are encrypted); it does see object names and sizes
- Encryption keys stored outside the cluster infrastructure

### 11.4 Kubernetes Security

- **Namespaces are single-tenant.** Every namespace holds one app/tool/component, so the namespace
  *is* the workload's identity. NetworkPolicy grants are therefore scoped by source namespace, never
  by pod labels — a pod can self-apply any label, so labels are not a trust boundary; namespace
  membership is, because creating a pod there is gated by RBAC. Granting a namespace access means
  trusting every pod in it equally, which single-tenancy makes safe.
- Every namespace is default-deny ingress. The fence is the static cilium `east-west-default-deny`
  CCNP (so it holds even if Kyverno stops reconciling — fail closed); the platform
  `namespace-default-ingress` policy layers the same-namespace and scrape allows on top. East-west
  reachability past those requires an explicit grant.
- Database node is tainted — only authorized workloads schedule there
- Storage nodes are labelled `vg=nvme` — TopoLVM's lvmd/node plugins are scoped by label
- TopoLVM manages only the `vg-nvme` volume group, so it never claims unintended devices

---

## 12. Operational Procedures

### 12.1 Node Provisioning

Compute nodes are provisioned via a custom Alpine Linux ISO built with mkimage. The ISO embeds an answer file and deploy script that automates base configuration, disk partitioning (OS + vg-nvme split), SSH key installation, and hostname assignment.

Deploy script usage:

```bash
# Usage: deploy-node.sh <node-number>
# Sets hostname to k3s-node-N, IP assigned via OPNsense DHCP reservation

sh deploy-node.sh 4
```

After OS install, join the cluster and enable storage:

```bash
curl -sfL https://get.k3s.io | K3S_URL=https://10.0.0.21:6443 K3S_TOKEN=<token> sh -
kubectl label nodes k3s-node-4 vg=nvme
```

### 12.2 IP Address Management

All infrastructure IPs are managed centrally in OPNsense via DHCP static mappings (**Services → DHCPv4 → LAN → Static Mappings**). Each mapping associates a MAC address with a fixed IP and hostname. Unbound automatically registers these hostnames for DNS resolution.

Nodes use DHCP to receive their addresses. OPNsense boots fast on the Topton N100 and is ready to serve DHCP before compute nodes request an IP. In the unlikely event a node boots before OPNsense, the DHCP client retries every few seconds until it receives a response.

### 12.3 k3s Upgrades

1. Read release notes for API deprecations and breaking changes.
2. Verify Cilium/SeaweedFS compatibility with the new version.
3. Upgrade server node first, then agents one at a time.
4. Never skip minor versions (e.g., v1.32 → v1.33 → v1.34).
5. Run `kubectl get nodes` to verify all nodes are Ready on the new version.

### 12.4 Adding Storage

To add a new compute node to the SeaweedFS/hypha pool:

1. Provision the node with the standard Alpine ISO (includes the `vg-nvme` partition)
2. Join the k3s cluster
3. Create the volume group at bootstrap (`vgcreate vg-nvme /dev/nvme0n1p3`), then label the node: `kubectl label nodes compute-N vg=nvme`
4. TopoLVM's lvmd/node plugins start on the node; add a SeaweedFS volume server (bump `volume.replicas`) to use the new capacity

### 12.5 UPS Recommendations

A small UPS (~$60–80 CAD) is recommended for the OPNsense router and database node. These are the only stateful devices requiring graceful shutdown. Compute nodes are stateless and recover cleanly from hard power loss. Configure `apcupsd` or `nut` to trigger automatic shutdown when battery reaches low threshold.

---

## 13. IP Address Plan

### 13.1 LAN (10.0.0.0/24)

| Range | Assignment |
|-------|------------|
| 10.0.0.1 | OPNsense LAN gateway |
| 10.0.0.2 | Horaco unmanaged switch |
| 10.0.0.3 | AX55 AP (AP mode) |
| 10.0.0.4–20 | Reserved for infrastructure |
| 10.0.0.21 | k3s server node |
| 10.0.0.22–40 | k3s compute nodes |
| 10.0.0.41–50 | Database / storage nodes |
| 10.0.0.100–150 | Cilium LoadBalancer IP pool (LB IPAM + L2; .100 = shared Gateway) |
| 10.0.0.200–250 | DHCP for other wired devices |

### 13.2 WIFI (10.0.1.0/24)

| Range | Assignment |
|-------|------------|
| 10.0.1.1 | OPNsense WIFI gateway |
| 10.0.1.100–200 | DHCP for wireless clients |

### 13.3 Kubernetes Internal

| Range | Assignment |
|-------|------------|
| 10.42.0.0/16 | k3s pod network (CNI managed) |
| 10.43.0.0/16 | k3s service network |

### 13.4 Service DNS

**Private wildcard (Unbound host override):**

| Hostname | IP | Purpose |
|----------|----|---------|
| `*.lab` (wildcard) | 10.0.0.100 | All TCP services — resolves to the Gateway LoadBalancer IP |
| `<udp-svc>.lab` (per-service) | 10.0.0.101+ | Each UDP service — resolves to its own dedicated LoadBalancer IP |

**Public wildcard (registrar DNS):**

| Hostname | IP | Purpose |
|----------|----|---------|
| `*.home.example.com` (wildcard) | WAN IP | All public TCP services — routes via OPNsense port forward to Gateway |

TCP services need only an `HTTPRoute` with the desired hostname — no additional DNS entry is required for either the private or public zone. UDP services each require a dedicated Unbound host override and their own LoadBalancer IP.

### 13.5 Reserved Ranges for Future Subnets

| Range | Potential Use |
|-------|--------------|
| 10.0.2.0/24 | IoT devices |
| 10.0.3.0/24 | Lab / experimental |
| 10.0.4.0/24 | Guest network |
| 10.0.10.0/24+ | Future expansion |
