#!/usr/bin/env bash
#
# Install Kyverno's CRDs out-of-band (i.e. NOT through the homelab-platform Helm release).
#
# Why: Helm stores the entire chart inside its release Secret, and Kyverno bundles ~5.6MB of CRD
# source. That exceeds Kubernetes' 1MB Secret limit ("data: Too long: may not be more than 1048576
# bytes"), so the chart sets `kyverno.crds.install: false` (which prunes the CRD subcharts from the
# release). The CRDs therefore have to be applied separately — this script does that, rendering them
# straight from the pinned `kyverno` chart dependency so they stay in lockstep with the engine
# version. Server-side apply is required: the full CRD schemas exceed the 256KB client-side
# `last-applied-configuration` annotation limit.
#
# Run this BEFORE the first `helm install`, and again before any `helm upgrade` that bumps the
# Kyverno chart version. It is idempotent.
#
# Usage:
#   ./platform/install-crds.sh            # apply the CRDs
#   ./platform/install-crds.sh --dry-run  # print the CRDs without applying
set -euo pipefail

CHART_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

# Pull the kyverno dependency if it isn't vendored yet.
[ -n "$(ls "$CHART_DIR"/charts/kyverno-*.tgz 2>/dev/null)" ] || helm dependency build "$CHART_DIR" >&2

# Render the chart with CRDs explicitly enabled, then keep only the CustomResourceDefinition docs.
render() {
  helm template homelab-platform "$CHART_DIR" -n kyverno \
    --set kyverno.crds.install=true \
    --set policies.enabled=false --set operator.enabled=false \
  | awk 'BEGIN{RS="\n---\n"} /kind: CustomResourceDefinition/{print "---"; print}'
}

if [ "$DRY_RUN" = 1 ]; then
  render
else
  render | kubectl apply --server-side --force-conflicts -f -
  echo "Kyverno CRDs applied. Now (re)run: helm upgrade --install homelab-platform $CHART_DIR -n kyverno" >&2
fi
