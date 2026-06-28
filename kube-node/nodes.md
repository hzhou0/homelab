I'd recommend dropping Sysbox from the bootstrap. Sysbox officially supports CRI-O only, and while there's been a recent fix for containerd, it requires building from source. Users are still hitting issues like mount permission errors on recent k3s versions. It's not stable enough for a default install on Alpine.

gVisor is solid though — straightforward binary install, works well with k3s containerd. Here's the updated bootstrap:

## Networking & DNS

All nodes use **dhcpcd** for addressing. It brings up whichever interface has a
carrier and DHCPs it, so the physical port / NIC name (`eth0` vs `eth1`, etc.)
doesn't need to be known ahead of time. `/etc/network/interfaces` manages
loopback only. DNS comes from the DHCP lease (the homelab DHCP server hands out
`10.0.0.1`), so there's no static resolver — hence `DNSOPTS=none` and the
`dhcpcd` package + service in each setup below.

Node IPs are not assumed stable, except the control plane, which is single-NIC
and pinned to `10.0.0.22` by a DHCP reservation (agents dial that hardcoded URL).

## Control Plane Node

### Boot from Alpine ISO, then:

```bash
cat > /root/answerfile <<EOF
KEYMAPOPTS="us us"
HOSTNAMEOPTS="-n k3s-server"
INTERFACESOPTS="auto lo
iface lo inet loopback
"
DNSOPTS=none
TIMEZONEOPTS="-z UTC"
PROXYOPTS=none
APKREPOSOPTS="-1 -c"
NTPOPTS="-c chrony"
DISKOPTS=none
EOF

setup-alpine -f /root/answerfile

apk add e2fsprogs parted grub grub-efi dosfstools dhcpcd
rc-update add dhcpcd boot

DISK="/dev/nvme0n1"

parted -s -a optimal $DISK -- \
  mklabel gpt \
  mkpart boot fat32 1MiB 512MiB \
  set 1 esp on \
  mkpart root ext4 512MiB 100GiB \
  mkpart ceph 100GiB 100%

mkfs.vfat -F32 ${DISK}p1
mkfs.ext4 -q ${DISK}p2

mount -t ext4 ${DISK}p2 /mnt
mkdir -p /mnt/boot
mount -t vfat ${DISK}p1 /mnt/boot

USE_EFI=1 BOOTLOADER=grub setup-disk -m sys /mnt

reboot
```

### After reboot:

```bash
# Cgroups
cat >> /etc/rc.conf <<'EOF'
rc_cgroup_mode="unified"
EOF
apk add cgroup-tools
rc-update add cgroups boot
rc-service cgroups start

# Install gVisor
ARCH=$(uname -m)
URL=https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}
wget ${URL}/runsc ${URL}/runsc.sha512 \
  ${URL}/containerd-shim-runsc-v1 ${URL}/containerd-shim-runsc-v1.sha512
sha512sum -c runsc.sha512 -c containerd-shim-runsc-v1.sha512
rm -f *.sha512
chmod a+rx runsc containerd-shim-runsc-v1
mv runsc containerd-shim-runsc-v1 /usr/local/bin

# Install k3s
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="\
  --disable=traefik \
  --disable=servicelb \
  --write-kubeconfig-mode=644 \
  --protect-kernel-defaults \
  --secrets-encryption \
" sh -

# Register gVisor with containerd
mkdir -p /var/lib/rancher/k3s/agent/etc/containerd
cat > /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl <<'TMPL'
{{ template "base" . }}

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.gvisor]
  runtime_type = "io.containerd.runsc.v1"
TMPL

systemctl restart k3s 2>/dev/null || rc-service k3s restart

# Label for Ceph
kubectl label nodes k3s-server ceph-osd=true

# Print token for agent nodes
cat /var/lib/rancher/k3s/server/node-token
```

---

## Compute Node

### Boot from Alpine ISO, then:

```bash
HOSTNAME="compute-2"  # change per node

cat > /root/answerfile <<EOF
KEYMAPOPTS="us us"
HOSTNAMEOPTS="-n ${HOSTNAME}"
INTERFACESOPTS="auto lo
iface lo inet loopback
"
DNSOPTS=none
TIMEZONEOPTS="-z UTC"
PROXYOPTS=none
APKREPOSOPTS="-1 -c"
NTPOPTS="-c chrony"
DISKOPTS=none
EOF

setup-alpine -f /root/answerfile

apk add e2fsprogs parted grub grub-efi dosfstools dhcpcd
rc-update add dhcpcd boot

DISK="/dev/nvme0n1"

parted -s -a optimal $DISK -- \
  mklabel gpt \
  mkpart boot fat32 1MiB 512MiB \
  set 1 esp on \
  mkpart root ext4 512MiB 100GiB \
  mkpart ceph 100GiB 100%

mkfs.vfat -F32 ${DISK}p1
mkfs.ext4 -q ${DISK}p2

mount -t ext4 ${DISK}p2 /mnt
mkdir -p /mnt/boot
mount -t vfat ${DISK}p1 /mnt/boot

USE_EFI=1 BOOTLOADER=grub setup-disk -m sys /mnt

reboot
```

### After reboot:

```bash
# Cgroups
cat >> /etc/rc.conf <<'EOF'
rc_cgroup_mode="unified"
EOF
apk add cgroup-tools
rc-update add cgroups boot
rc-service cgroups start

# Install gVisor
ARCH=$(uname -m)
URL=https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}
wget ${URL}/runsc ${URL}/runsc.sha512 \
  ${URL}/containerd-shim-runsc-v1 ${URL}/containerd-shim-runsc-v1.sha512
sha512sum -c runsc.sha512 -c containerd-shim-runsc-v1.sha512
rm -f *.sha512
chmod a+rx runsc containerd-shim-runsc-v1
mv runsc containerd-shim-runsc-v1 /usr/local/bin

# Join cluster
curl -sfL https://get.k3s.io | K3S_URL=https://10.0.0.21:6443 K3S_TOKEN=<token> sh -

# Register gVisor with containerd
mkdir -p /var/lib/rancher/k3s/agent/etc/containerd
cat > /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl <<'TMPL'
{{ template "base" . }}

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.gvisor]
  runtime_type = "io.containerd.runsc.v1"
TMPL

rc-service k3s-agent restart
```

### From control plane:

```bash
kubectl label nodes compute-2 ceph-osd=true
```

---

## Compute Spot Node

Identical to a Compute Node except it does **not** participate in Ceph: the disk
is not carved up for a Ceph OSD partition — the root filesystem claims the whole
disk, and you never apply the `ceph-osd=true` label to a spot node.

A spot node is expected to be ephemeral: it can be drained, have k3s turned off,
disconnected from the cluster, and repurposed elsewhere without affecting Ceph
or cluster storage. The fleet-wide dhcpcd networking (see above) means it picks
up an address on whatever port has a link on the host it's moved to.

### Boot from Alpine ISO, then:

```bash
HOSTNAME="compute-spot-1"  # change per node

cat > /root/answerfile <<EOF
KEYMAPOPTS="us us"
HOSTNAMEOPTS="-n ${HOSTNAME}"
INTERFACESOPTS="auto lo
iface lo inet loopback
"
DNSOPTS=none
TIMEZONEOPTS="-z UTC"
PROXYOPTS=none
APKREPOSOPTS="-1 -c"
NTPOPTS="-c chrony"
DISKOPTS=none
EOF

setup-alpine -f /root/answerfile

apk add e2fsprogs parted grub grub-efi dosfstools dhcpcd
rc-update add dhcpcd boot

DISK="/dev/nvme0n1"

# No ceph partition — root takes the whole disk.
parted -s -a optimal $DISK -- \
  mklabel gpt \
  mkpart boot fat32 1MiB 512MiB \
  set 1 esp on \
  mkpart root ext4 512MiB 100%

mkfs.vfat -F32 ${DISK}p1
mkfs.ext4 -q ${DISK}p2

mount -t ext4 ${DISK}p2 /mnt
mkdir -p /mnt/boot
mount -t vfat ${DISK}p1 /mnt/boot

USE_EFI=1 BOOTLOADER=grub setup-disk -m sys /mnt

reboot
```

### After reboot:

```bash
# Cgroups
cat >> /etc/rc.conf <<'EOF'
rc_cgroup_mode="unified"
EOF
apk add cgroup-tools
rc-update add cgroups boot
rc-service cgroups start

# Install gVisor
ARCH=$(uname -m)
URL=https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}
wget ${URL}/runsc ${URL}/runsc.sha512 \
  ${URL}/containerd-shim-runsc-v1 ${URL}/containerd-shim-runsc-v1.sha512
sha512sum -c runsc.sha512 -c containerd-shim-runsc-v1.sha512
rm -f *.sha512
chmod a+rx runsc containerd-shim-runsc-v1
mv runsc containerd-shim-runsc-v1 /usr/local/bin

# Join cluster
curl -sfL https://get.k3s.io | K3S_URL=https://10.0.0.21:6443 K3S_TOKEN=<token> sh -

# Register gVisor with containerd
mkdir -p /var/lib/rancher/k3s/agent/etc/containerd
cat > /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl <<'TMPL'
{{ template "base" . }}

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.gvisor]
  runtime_type = "io.containerd.runsc.v1"
TMPL

rc-service k3s-agent restart
```

### From control plane:

```bash
# gVisor only — do NOT label ceph-osd on a spot node.
kubectl label nodes compute-spot-1 gvisor=true
```

### Decommissioning / repurposing:

```bash
# From control plane, before taking the node offline:
kubectl drain compute-spot-1 --ignore-daemonsets --delete-emptydir-data
kubectl delete node compute-spot-1

# On the node:
rc-service k3s-agent stop
rc-update del k3s-agent default
```

---

## Database Node

### Boot from Alpine ISO, then:

```bash
cat > /root/answerfile <<EOF
KEYMAPOPTS="us us"
HOSTNAMEOPTS="-n db"
INTERFACESOPTS="auto lo
iface lo inet loopback
"
DNSOPTS=none
TIMEZONEOPTS="-z UTC"
PROXYOPTS=none
APKREPOSOPTS="-1 -c"
NTPOPTS="-c chrony"
DISKOPTS=none
EOF

setup-alpine -f /root/answerfile

apk add e2fsprogs parted grub grub-efi dosfstools dhcpcd
rc-update add dhcpcd boot

DISK="/dev/nvme0n1"

parted -s -a optimal $DISK -- \
  mklabel gpt \
  mkpart boot fat32 1MiB 512MiB \
  set 1 esp on \
  mkpart root ext4 512MiB 100%

mkfs.vfat -F32 ${DISK}p1
mkfs.ext4 -q ${DISK}p2

mount -t ext4 ${DISK}p2 /mnt
mkdir -p /mnt/boot
mount -t vfat ${DISK}p1 /mnt/boot

USE_EFI=1 BOOTLOADER=grub setup-disk -m sys /mnt

reboot
```

### After reboot:

```bash
# Cgroups
cat >> /etc/rc.conf <<'EOF'
rc_cgroup_mode="unified"
EOF
apk add cgroup-tools mdadm
rc-update add cgroups boot
rc-service cgroups start

# Install gVisor
ARCH=$(uname -m)
URL=https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}
wget ${URL}/runsc ${URL}/runsc.sha512 \
  ${URL}/containerd-shim-runsc-v1 ${URL}/containerd-shim-runsc-v1.sha512
sha512sum -c runsc.sha512 -c containerd-shim-runsc-v1.sha512
rm -f *.sha512
chmod a+rx runsc containerd-shim-runsc-v1
mv runsc containerd-shim-runsc-v1 /usr/local/bin

# D4-320 RAID5
mdadm --create /dev/md0 --level=raid5 --raid-devices=4 \
  /dev/sda /dev/sdb /dev/sdc /dev/sdd
mkfs.ext4 /dev/md0
mkdir -p /mnt/tablespaces
mount /dev/md0 /mnt/tablespaces
echo '/dev/md0 /mnt/tablespaces ext4 defaults 0 2' >> /etc/fstab
mdadm --detail --scan >> /etc/mdadm.conf
echo -1 > /sys/bus/usb/devices/*/power/autosuspend

# Join cluster
curl -sfL https://get.k3s.io | K3S_URL=https://10.0.0.21:6443 K3S_TOKEN=<token> sh -

# Register gVisor with containerd
mkdir -p /var/lib/rancher/k3s/agent/etc/containerd
cat > /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl <<'TMPL'
{{ template "base" . }}

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.gvisor]
  runtime_type = "io.containerd.runsc.v1"
TMPL

rc-service k3s-agent restart
```

### From control plane:

```bash
kubectl label nodes db node-role=database
kubectl taint nodes db workload=database:NoSchedule
```

---

Changes from previous version: removed swap, removed SSH, removed user account, UTC timezone, gVisor installed on all nodes, secrets encryption enabled, kernel defaults protection enabled, Sysbox dropped (unstable on k3s/containerd), networking switched to dhcpcd fleet-wide (carrier-based interface selection, DNS from DHCP, no static resolver or fixed eth0). RuntimeClass and Pod Security Standards go through Argo.