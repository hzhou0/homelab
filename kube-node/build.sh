#!/bin/sh
# Build Alpine ISO images for k3s cluster nodes using mkimage.sh via Docker.
#
# Usage:
#   ./build.sh [k3s-server|k3s-compute|k3s-compute-spot|k3s-db|all]
#
# Output ISOs land in ./output/.
# Requires: docker (with privileged capability)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/output"
ALPINE_VERSION="${ALPINE_VERSION:-3.22.4}"
ARCH="${ARCH:-$(uname -m)}"
APORTS_DIR="$SCRIPT_DIR/.aports"

PROFILES="${1:-all}"

# ── helpers ──────────────────────────────────────────────────────────────────

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

require_docker() {
  command -v docker >/dev/null 2>&1 || die "docker is not installed"
  docker info >/dev/null 2>&1 || die "docker daemon is not running"
}

require_token() {
  [ -n "$K3S_TOKEN" ] || die "K3S_TOKEN is not set"
}

fetch_aports() {
  if [ ! -d "$APORTS_DIR/.git" ]; then
    log "Cloning Alpine aports (scripts only)..."
    git clone --depth=1 --sparse \
      https://gitlab.alpinelinux.org/alpine/aports.git "$APORTS_DIR"
    git -C "$APORTS_DIR" sparse-checkout set scripts
  else
    log "aports already present, skipping clone"
  fi
}

# ── build ─────────────────────────────────────────────────────────────────────

build_profile() {
  PROFILE="$1"
  log "Building profile: $PROFILE"

  OVERLAY_DIR="$SCRIPT_DIR/overlays/$PROFILE"
  [ -d "$OVERLAY_DIR" ] || die "Overlay directory not found: $OVERLAY_DIR"

  NODE_ROLE=$(cat "$OVERLAY_DIR/etc/node-role")

  docker run --rm --privileged \
    -v "$APORTS_DIR/scripts:/scripts:ro" \
    -v "$SCRIPT_DIR:/work:ro" \
    -v "$OUTPUT_DIR:/output" \
    -e "PROFILE=$PROFILE" \
    -e "NODE_ROLE=$NODE_ROLE" \
    -e "ALPINE_VERSION=$ALPINE_VERSION" \
    -e "ARCH=$ARCH" \
    -e "K3S_TOKEN=$K3S_TOKEN" \
    alpine:${ALPINE_VERSION} \
    sh /work/inner-build.sh

  log "Built: $OUTPUT_DIR/alpine-${PROFILE}-${ALPINE_VERSION}-${ARCH}.iso"
}

# ── main ──────────────────────────────────────────────────────────────────────

require_docker
require_token
fetch_aports
mkdir -p "$OUTPUT_DIR"

case "$PROFILES" in
  all)
    build_profile k3s-server
    build_profile k3s-compute
    build_profile k3s-compute-spot
    build_profile k3s-db
    ;;
  k3s-server|k3s-compute|k3s-compute-spot|k3s-db)
    build_profile "$PROFILES"
    ;;
  *)
    die "Unknown profile '$PROFILES'. Valid: k3s-server, k3s-compute, k3s-compute-spot, k3s-db, all"
    ;;
esac

log "Done. ISOs are in $OUTPUT_DIR/"
