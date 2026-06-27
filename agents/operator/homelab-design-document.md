# Homelab Infrastructure Design Document

**OPNsense + k3s + Ceph + PostgreSQL**
**Vancouver, BC — May 2026**

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

This document describes the design of a personal homelab infrastructure located in Vancouver, BC. The system provides network segregation, Kubernetes-based container orchestration, distributed object storage, managed PostgreSQL databases, encrypted offsite backups, and privacy-preserving DNS resolution.

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
| Compute Nodes | Multiple N100 mini PCs (Alpine Linux) | k3s workers, Ceph OSD contributors |
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
| MetalLB Pool | 10.0.0.100–150 | LoadBalancer service IPs | N/A |

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

The connection is Telus PureFibre in Vancouver, BC. The Telus-provided NAH (Network Access Hub) is a combo unit containing an XGS-PON SFP module and integrated router.

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
| k3s-server | Server + worker | 10.0.0.21 | NVMe (OS 100GB, Ceph remainder) |
| compute-2 | Worker | 10.0.0.22 | NVMe (OS 100GB, Ceph remainder) |
| compute-3 | Worker | 10.0.0.23 | NVMe (OS 100GB, Ceph remainder) |
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

- **Traefik:** Disabled. Cilium Gateway API handles all TCP ingress through a single shared Gateway (one MetalLB IP). UDP services that cannot traverse the Gateway get their own dedicated MetalLB IP.
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

2. **North-south ingress** — The only externally accessible entry point for TCP is a shared `Gateway` object backed by a single MetalLB IP. Traffic is routed by hostname via `HTTPRoute` objects. No TCP service is exposed via `type: LoadBalancer` directly — all inbound TCP must enter through the Gateway. UDP services that cannot traverse the Gateway each get their own dedicated MetalLB IP via `type: LoadBalancer`, unchanged from before.

3. **WAN exposure** — A custom OPNsense operator watches `Gateway` and `Service` objects for an annotation and creates/removes the corresponding WAN port forward rule via the OPNsense API. Status is written back to the standard Kubernetes status fields (`status.addresses` on `Gateway`, `status.loadBalancer.ingress` on `Service`), so `kubectl get gateway,svc -A` shows the full external exposure map.

This follows the same annotation-driven pattern as the AWS Load Balancer Controller in EKS.

**HTTP/HTTPS (shared gateway):**

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: homelab-gateway
  annotations:
    opnsense.lab/expose: "true"       # operator creates WAN:443 → MetalLB IP
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

**DNS** — Two wildcard zones both resolve to the Gateway's MetalLB IP, allowing any `HTTPRoute` hostname to work automatically without per-service DNS entries:

- **Private (`*.lab`)** — Unbound wildcard host override: `*.lab → 10.0.0.100`. Any `.lab` query from the LAN resolves to the Gateway. See section 13.4.
- **Public (`*.home.example.com`)** — Wildcard A record at the registrar pointing to the WAN IP. The OPNsense operator's port forward on the annotated Gateway routes inbound HTTPS to the Gateway MetalLB IP.

UDP services are excluded from these wildcards; each retains an individual Unbound host override pointing to its own dedicated MetalLB IP.

### 5.7 Power Loss Recovery

k3s recovers automatically from hard power loss. The control plane runs as a host process and etcd replays its WAL on startup. Agent nodes retry connection to the server URL indefinitely until the server comes up. Boot order does not matter.

Realistic power outage timeline:

```
0:00  — Power returns
0:30  — Nodes POST and boot Alpine
1:00  — k3s server starts, etcd recovers
1:15  — k3s agents connect, nodes go Ready
1:30  — Scheduler starts placing pods
2:00  — Ceph OSDs come online, peer with each other
2:30  — Ceph healthy, RGW pod starts, S3 available
3:00  — PostgreSQL crash recovery completes
3:30  — Everything running normally
```

### 5.8 Upgrades

k3s upgrades are performed one minor version at a time (e.g., v1.32 → v1.33 → v1.34, never skipping). Server node is upgraded first, then agent nodes. Verify Cilium/Ceph/CloudNativePG compatibility before upgrading. The Rancher System Upgrade Controller can automate this process.

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

Storage is split by workload type. The database node uses local storage for performance. Compute nodes contribute spare NVMe capacity to a Ceph cluster for S3 object storage. No persistent block devices (CephBlockPool) or shared filesystems (CephFS) are provisioned through Ceph — only RGW (S3 gateway).

### 7.2 Storage Layout

| Node Type | Storage | Provisioner | Use Case |
|-----------|---------|-------------|----------|
| Database node | Internal NVMe | Local Path Provisioner | WAL, default tablespace |
| Database node | D4-320 (4× HDD, RAID5) | Direct mount | Large tablespaces |
| Compute nodes | Spare NVMe partition | Ceph OSD | S3 object store pool |

### 7.3 Compute Node Disk Partitioning

Each compute node's NVMe is partitioned during Alpine installation:

| Partition | Size | Format | Purpose |
|-----------|------|--------|---------|
| nvme0n1p1 | 512MB | FAT32 (ESP) | EFI boot |
| nvme0n1p2 | 100GB | ext4 | Alpine OS + k3s |
| nvme0n1p3 | 4GB | swap | Swap |
| nvme0n1p4 | Remainder | Raw (unformatted) | Ceph OSD |

The Ceph partition must remain unformatted — Rook requires raw block devices.

### 7.4 Ceph Cluster Configuration

Ceph is deployed via the Rook operator. Only RGW (S3 gateway) is enabled.

```yaml
apiVersion: ceph.rook.io/v1
kind: CephCluster
metadata:
  name: rook-ceph
  namespace: rook-ceph
spec:
  cephVersion:
    image: quay.io/ceph/ceph:v18
  mon:
    count: 3
    allowMultiplePerNode: false
  mgr:
    count: 2
  storage:
    useAllNodes: false
    useAllDevices: false
    deviceFilter: "^nvme0n1p4"
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: ceph-osd
                operator: In
                values: ["true"]
```

Adding a new compute node to the Ceph cluster:

```bash
kubectl label nodes compute-N ceph-osd=true
```

Rook discovers the raw partition, creates an OSD, and Ceph rebalances automatically.

### 7.5 Replication Strategy

| Pool | Replication | Rationale |
|------|-------------|-----------|
| S3 metadata pool | size: 2 | Enables incremental recovery; metadata is tiny |
| S3 data pool | size: 1 | No replication; backups handle data protection |
| CephBlockPool | Not created | Not needed |
| CephFS | Not created | Not needed |

With data at size: 1, usable capacity equals raw capacity. With size: 3, usable capacity would be one-third of raw.

### 7.6 S3 Object Store (RGW)

```yaml
apiVersion: ceph.rook.io/v1
kind: CephObjectStore
metadata:
  name: s3-store
  namespace: rook-ceph
spec:
  metadataPool:
    replicated:
      size: 2
  dataPool:
    replicated:
      size: 1
  gateway:
    port: 80
    instances: 2
```

Two RGW instances provide zero-downtime S3 availability during single node failures. The gateway is stateless — if a node dies, Kubernetes reschedules the pod to a surviving node automatically.

S3 endpoint within the cluster: `rook-ceph-rgw-s3-store.rook-ceph.svc`

Exposed externally via MetalLB LoadBalancer for services outside the cluster.

### 7.7 Ceph Resource Overhead

| Daemon | Count | RAM Estimate |
|--------|-------|--------------|
| MON | 3 | 300–500MB each |
| MGR | 2 | 300–500MB each |
| OSD | 1 per compute node | 500MB–1GB each |
| RGW | 2 instances | 200–300MB each |

Total estimated overhead: 3–4GB across the cluster.

### 7.8 S3 Failure Modes

With metadata replicated (size: 2) and data unreplicated (size: 1), a node failure results in:

- Metadata survives — bucket listings remain complete
- Some data objects are lost (those stored on the dead OSD)
- S3 GET requests for lost objects return clean HTTP 500/503 errors
- rclone can perform incremental recovery by comparing against the Backblaze B2 offsite copy and re-uploading only missing objects
- Uploads targeting the dead OSD fail with HTTP 503; clients retry

Without metadata replication, a node failure could make the entire bucket unopenable, requiring full restore from Backblaze.

---

## 8. Database Layer

### 8.1 CloudNativePG

PostgreSQL is managed by the CloudNativePG operator, which provides declarative cluster management, automated minor/major version upgrades (via pg_upgrade), and integrated backup via Barman Cloud. A single instance is deployed with no replicas.

### 8.2 Cluster Definition

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: mydb
  namespace: database
spec:
  instances: 1
  imageName: ghcr.io/cloudnative-pg/postgresql:17.5-minimal-bullseye

  postgresql:
    parameters:
      wal_level: "replica"
      max_wal_senders: "1"

  storage:
    storageClass: local-path
    size: 50Gi

  tablespaces:
    - name: historical
      storage:
        storageClass: local-path
        size: 500Gi
    - name: archive
      storage:
        storageClass: local-path
        size: 500Gi

  backup:
    barmanObjectStore:
      destinationPath: s3://pg-backups/
      endpointURL: https://s3.us-west-000.backblazeb2.com
      s3Credentials:
        accessKeyId:
          name: b2-creds
          key: ACCESS_KEY
        secretAccessKey:
          name: b2-creds
          key: SECRET_KEY
      wal:
        compression: gzip
        encryption: AES256
      data:
        compression: gzip
        encryption: AES256
    retentionPolicy: "14d"

  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: node-role
                operator: In
                values: ["database"]
    tolerations:
      - key: workload
        value: database
        effect: NoSchedule
```

### 8.3 Tablespace Strategy

Hot data (current, frequently accessed) resides on the internal NVMe for maximum IOPS. Cold/large data (historical records, archives, logs) resides on the D4-320 HDDs via PostgreSQL tablespaces. The WAL is kept on NVMe for write performance and durability.

```sql
CREATE TABLESPACE historical_data LOCATION '/mnt/disk1/pg_tablespace';
ALTER TABLE events SET TABLESPACE historical_data;
```

### 8.4 Major Version Upgrades

CloudNativePG handles major PostgreSQL upgrades (e.g., 16 → 17) via pg_upgrade with `--link` mode. This creates hard links instead of copying data, making even multi-hundred-GB tablespace upgrades complete in seconds. A Barman Cloud backup is taken automatically before each upgrade. Post-upgrade, `ANALYZE` is run on all databases to refresh query planner statistics.

### 8.5 WAL Configuration

`wal_level` is set to `replica` (not `minimal`) to support continuous WAL archiving via Barman Cloud. `max_wal_senders` is set to 1 — just enough for WAL archiving with no replicas.

---

## 9. Backup & Disaster Recovery

### 9.1 Design Principles

- All backups are encrypted client-side before leaving the network. The backup provider sees only encrypted blobs.
- Offsite backups go to Backblaze B2 (S3-compatible, ~$6/TB/month).
- Replication is not used for data protection. Backups are the disaster recovery mechanism.
- Encryption keys must be stored outside the cluster (password manager, printed in a safe). Losing them renders all backups unrecoverable.

### 9.2 PostgreSQL Backup (Barman Cloud)

CloudNativePG's integrated Barman Cloud ships continuous WAL archives and scheduled base backups directly to Backblaze B2. WAL archiving provides near-zero RPO (Recovery Point Objective). Point-in-time recovery (PITR) to any second in time is supported.

Scheduled backup:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: ScheduledBackup
metadata:
  name: mydb-weekly
spec:
  schedule: "0 2 * * 0"
  cluster:
    name: mydb
  backupOwnerReference: self
```

| Backup Type | Frequency | Retention |
|-------------|-----------|-----------|
| Full base backup | Weekly (Sunday 2am) | 2 full backups |
| WAL archiving | Continuous | Tied to base backup retention |

### 9.3 PostgreSQL Restore

Recovery is declarative. Create a new Cluster resource specifying the backup source and optional target time:

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: mydb-restored
spec:
  instances: 1
  bootstrap:
    recovery:
      source: mydb-backup
      recoveryTarget:
        targetTime: "2026-04-25 14:30:00"
  externalClusters:
    - name: mydb-backup
      barmanObjectStore:
        destinationPath: s3://pg-backups/
        endpointURL: https://s3.us-west-000.backblazeb2.com
        s3Credentials:
          accessKeyId:
            name: b2-creds
            key: ACCESS_KEY
          secretAccessKey:
            name: b2-creds
            key: SECRET_KEY
```

### 9.4 Ceph S3 Backup (rclone)

A nightly CronJob runs rclone sync from the Ceph RGW S3 endpoint to Backblaze B2 through an rclone crypt overlay. File contents, filenames, and directory names are all encrypted (XSalsa20/Poly1305) before upload.

```ini
# ~/.config/rclone/rclone.conf

[b2-raw]
type = s3
provider = Other
endpoint = s3.us-west-000.backblazeb2.com
access_key_id = your-key
secret_access_key = your-secret

[b2-encrypted]
type = crypt
remote = b2-raw:my-backup-bucket
password = your-encryption-password
password2 = your-salt-password
filename_encryption = standard
directory_name_encryption = true
```

CronJob:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ceph-offsite-sync
spec:
  schedule: "0 3 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: rclone
            image: rclone/rclone
            command:
              - rclone
              - sync
              - ceph-s3:my-data
              - b2-encrypted:
              - --transfers=4
              - --fast-list
            volumeMounts:
              - name: rclone-config
                mountPath: /config/rclone
          volumes:
            - name: rclone-config
              secret:
                secretName: rclone-config
```

### 9.5 Full Cluster Disaster Recovery

In the event of complete cluster loss, the rebuild order is:

1. Install Alpine on all nodes (automated via custom ISO)
2. Install k3s (server first, then agents)
3. Deploy Ceph via Rook, wait for OSDs to peer
4. Deploy MetalLB, custom OPNsense operator
5. Restore Ceph S3 data from Backblaze via rclone
6. Deploy CloudNativePG, restore database from Backblaze B2 backup
7. Reconfigure OPNsense port forwards (operator handles automatically)

Steps 1–4 should be scripted and stored in an external git repository for rapid rebuilds. Target recovery time: under 1 hour excluding data restore.

### 9.6 Backup Architecture Diagram

```
Database node (local NVMe + D4-320)
  → Barman Cloud (encrypted, continuous WAL + weekly full)
    → Backblaze B2

Ceph cluster (compute node spare NVMe)
  → rclone crypt (nightly sync, encrypted)
    → Backblaze B2

Both encrypted client-side. Backblaze sees nothing.
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
Host Override: *.lab → 10.0.0.100  (Gateway MetalLB IP)
```

Any `<service>.lab` query from a LAN or WiFi device resolves to the Gateway. The Gateway then routes by `Host:` header to the correct backend via `HTTPRoute`. Adding a new TCP service requires only a new `HTTPRoute` — no DNS change needed.

**UDP services** — Each UDP service retains an individual Unbound host override pointing to its own dedicated MetalLB IP (unchanged from before).

**Public wildcard zone** — A wildcard A record `*.home.example.com → <WAN IP>` is configured at the DNS registrar. The OPNsense operator's WAN port forward on the annotated Gateway routes inbound HTTPS to the same Gateway MetalLB IP. An `HTTPRoute` can match on both the `.lab` and `.home.example.com` hostnames simultaneously to serve private and public clients from the same backend. See section 5.6.

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

- Barman Cloud: AES-256 encryption of WAL and base backups
- rclone: XSalsa20/Poly1305 encryption with encrypted filenames and directory names
- Backblaze B2 has zero visibility into backup contents
- Encryption keys stored outside the cluster infrastructure

### 11.4 Kubernetes Security

- Database node is tainted — only authorized workloads schedule there
- Ceph OSD nodes are labelled — storage auto-discovery is scoped by label
- Device filter (`^nvme0n1p4`) prevents Rook from claiming unintended devices

---

## 12. Operational Procedures

### 12.1 Node Provisioning

Compute nodes are provisioned via a custom Alpine Linux ISO built with mkimage. The ISO embeds an answer file and deploy script that automates base configuration, disk partitioning (OS + Ceph split), SSH key installation, and hostname assignment.

Deploy script usage:

```bash
# Usage: deploy-node.sh <node-number>
# Sets hostname to k3s-node-N, IP assigned via OPNsense DHCP reservation

sh deploy-node.sh 4
```

After OS install, join the cluster and enable Ceph:

```bash
curl -sfL https://get.k3s.io | K3S_URL=https://10.0.0.21:6443 K3S_TOKEN=<token> sh -
kubectl label nodes k3s-node-4 ceph-osd=true
```

### 12.2 IP Address Management

All infrastructure IPs are managed centrally in OPNsense via DHCP static mappings (**Services → DHCPv4 → LAN → Static Mappings**). Each mapping associates a MAC address with a fixed IP and hostname. Unbound automatically registers these hostnames for DNS resolution.

Nodes use DHCP to receive their addresses. OPNsense boots fast on the Topton N100 and is ready to serve DHCP before compute nodes request an IP. In the unlikely event a node boots before OPNsense, the DHCP client retries every few seconds until it receives a response.

### 12.3 k3s Upgrades

1. Read release notes for API deprecations and breaking changes.
2. Verify Cilium/Ceph/CloudNativePG compatibility with the new version.
3. Upgrade server node first, then agents one at a time.
4. Never skip minor versions (e.g., v1.32 → v1.33 → v1.34).
5. Run `kubectl get nodes` to verify all nodes are Ready on the new version.

### 12.4 PostgreSQL Major Upgrades

Handled declaratively by CloudNativePG. The operator performs pg_upgrade with `--link` mode, preserving tablespace data. A Barman Cloud backup is taken automatically before upgrading. Post-upgrade, `ANALYZE` is run on all databases to refresh query planner statistics.

### 12.5 Adding Ceph Storage

To add a new compute node to the Ceph pool:

1. Provision the node with the standard Alpine ISO (includes Ceph partition)
2. Join the k3s cluster
3. Label the node: `kubectl label nodes compute-N ceph-osd=true`
4. Rook auto-discovers the raw partition, creates an OSD, Ceph rebalances

### 12.6 UPS Recommendations

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
| 10.0.0.100–150 | MetalLB LoadBalancer pool |
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
| `*.lab` (wildcard) | 10.0.0.100 | All TCP services — resolves to the Gateway MetalLB IP |
| `<udp-svc>.lab` (per-service) | 10.0.0.101+ | Each UDP service — resolves to its own dedicated MetalLB IP |

**Public wildcard (registrar DNS):**

| Hostname | IP | Purpose |
|----------|----|---------|
| `*.home.example.com` (wildcard) | WAN IP | All public TCP services — routes via OPNsense port forward to Gateway |

TCP services need only an `HTTPRoute` with the desired hostname — no additional DNS entry is required for either the private or public zone. UDP services each require a dedicated Unbound host override and their own MetalLB IP.

### 13.5 Reserved Ranges for Future Subnets

| Range | Potential Use |
|-------|--------------|
| 10.0.2.0/24 | IoT devices |
| 10.0.3.0/24 | Lab / experimental |
| 10.0.4.0/24 | Guest network |
| 10.0.10.0/24+ | Future expansion |
