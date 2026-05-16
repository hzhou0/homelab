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

SCRIPTS=/tmp/scripts
cp -r /scripts "$SCRIPTS"
cp /work/mkimg."$PROFILE".sh "$SCRIPTS"/
sed -i "s|apkovl=.*|apkovl=\"$OVERLAY\"|" "$SCRIPTS/mkimg.$PROFILE.sh"

REPO_VERSION=$(echo "$ALPINE_VERSION" | cut -d. -f1,2)
REPO_BASE="https://dl-cdn.alpinelinux.org/alpine/v${REPO_VERSION}"

adduser -D builder
addgroup builder abuild
addgroup builder wheel
echo 'builder ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers
chown -R builder:abuild "$SCRIPTS" "$OVERLAY" /output /tmp/mkimage-work 2>/dev/null || true

PROFILE_FUNC=$(echo "$PROFILE" | tr '-' '_')
su builder -c "
  abuild-keygen -n -a
  cd $SCRIPTS
  ./mkimage.sh \
    --tag '$ALPINE_VERSION' \
    --outdir /output \
    --arch '$ARCH' \
    --workdir /tmp/mkimage-work \
    --repository '$REPO_BASE/main' \
    --repository '$REPO_BASE/community' \
    --profile '$PROFILE_FUNC'
"
