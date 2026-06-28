#!/bin/sh
# Runs inside Docker. Values injected via env: PROFILE, NODE_ROLE, ALPINE_VERSION, ARCH, K3S_TOKEN
set -e

apk add --no-cache abuild alpine-conf xorriso squashfs-tools grub grub-efi mtools

OVERLAY=/tmp/overlay
cp -r /work/overlays/"$PROFILE" "$OVERLAY"
mkdir -p "$OVERLAY/etc/local.d"
printf '%s' "$K3S_TOKEN" > "$OVERLAY/etc/k3s-token"
cp /work/10-provision.start "$OVERLAY/etc/local.d/10-provision.start"
cp /work/20-bootstrap-"$NODE_ROLE".start "$OVERLAY/etc/local.d/20-bootstrap-$NODE_ROLE.start"
chmod +x "$OVERLAY/etc/local.d/"*.start

mkdir -p "$OVERLAY/etc/network"
# All nodes use dhcpcd to DHCP whichever interface has a carrier, so
# /etc/network/interfaces only manages loopback.
cat > "$OVERLAY/etc/network/interfaces" <<'EOF'
auto lo
iface lo inet loopback
EOF

rc_add() {
  mkdir -p "$OVERLAY/etc/runlevels/$2"
  ln -sf /etc/init.d/"$1" "$OVERLAY/etc/runlevels/$2/$1"
}

rc_add devfs    sysinit
rc_add dmesg    sysinit
rc_add mdev     sysinit
rc_add hwdrivers sysinit
rc_add modloop  sysinit

rc_add hwclock  boot
rc_add modules  boot
rc_add sysctl   boot
rc_add hostname boot
rc_add bootmisc boot
rc_add syslog   boot
rc_add networking boot

rc_add mount-ro  shutdown
rc_add killprocs shutdown
rc_add savecache shutdown

rc_add local    default

SCRIPTS=/tmp/scripts
cp -r /scripts "$SCRIPTS"
cp /work/mkimg."$PROFILE".sh "$SCRIPTS"/

# mkimage requires apkovl to be an executable genapkovl script, not a directory path
GENAPKOVL_NAME="genapkovl-$PROFILE.sh"
printf '#!/bin/sh -e\ntar -c -C "%s" . | gzip -9n > "$1.apkovl.tar.gz"\n' \
  "$OVERLAY" > "$SCRIPTS/$GENAPKOVL_NAME"
chmod +x "$SCRIPTS/$GENAPKOVL_NAME"
sed -i "s|apkovl=.*|apkovl=\"$GENAPKOVL_NAME\"|" "$SCRIPTS/mkimg.$PROFILE.sh"

REPO_VERSION=$(echo "$ALPINE_VERSION" | cut -d. -f1,2)
REPO_BASE="https://dl-cdn.alpinelinux.org/alpine/v${REPO_VERSION}"

abuild-keygen -n -a -q
PACKAGER_PRIVKEY=$(find /root/.abuild -name "*.rsa" | head -1)
[ -n "$PACKAGER_PRIVKEY" ] || { echo "ERROR: abuild key not generated"; exit 1; }
export PACKAGER_PRIVKEY  # needed for abuild-sign to sign the APKINDEX
cp "${PACKAGER_PRIVKEY}.pub" /etc/apk/keys/  # --hostkeys picks this up for the initramfs

PROFILE_FUNC=$(echo "$PROFILE" | tr '-' '_')
cd "$SCRIPTS"
./mkimage.sh \
  --tag "$ALPINE_VERSION" \
  --outdir /output \
  --arch "$ARCH" \
  --workdir /tmp/mkimage-work \
  --repository "$REPO_BASE/main" \
  --repository "$REPO_BASE/community" \
  --hostkeys \
  --profile "$PROFILE_FUNC"
