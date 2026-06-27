#!/usr/bin/env bash
# Renders the Helm chart to .rendered/ then runs all kyverno policy unit tests.
# Requires: helm, kubectl-kyverno (krew: kubectl kyverno)
set -euo pipefail

TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"
CHART_DIR="$TESTS_DIR/.."
RENDERED_DIR="$TESTS_DIR/.rendered"

echo "→ Rendering chart to $RENDERED_DIR"
rm -rf "$RENDERED_DIR"
helm template homelab-platform "$CHART_DIR" \
  --set policies.enabled=true \
  --set operator.enabled=true \
  --set namespaceMetadata.enabled=true \
  --set oneChartPerToolNamespace.enabled=true \
  --output-dir "$RENDERED_DIR"

echo "→ Running kyverno policy tests"
kubectl kyverno test "$TESTS_DIR"

# tool-single-release uses resource.List() which kyverno test cannot mock offline;
# it is verified via the live cluster (helm upgrade + kubectl apply round-trip).
