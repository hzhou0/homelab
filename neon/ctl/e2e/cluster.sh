#!/usr/bin/env bash
# Brings up a throwaway k3s cluster with the two things the chart expects from the homelab that a
# fresh cluster has not got: an S3 endpoint, and the CRD the network grants are written against.
#
# Every command names the context explicitly. Switching the current context instead would leave a
# failed create pointing the next command at whatever was selected before, which is the homelab.
set -euo pipefail

CLUSTER="${K3D_CLUSTER:-neon-e2e}"
CONTEXT="k3d-$CLUSTER"
NAMESPACE="${NEON_NAMESPACE:-neon}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART="$HERE/../../chart"
PGPORT="${NEON_PGPORT:-55432}"
CILIUM_VERSION="${CILIUM_VERSION:-v1.20.0}"

k() { kubectl --context "$CONTEXT" "$@"; }

case "${1:-up}" in
up)
  if ! k3d cluster list "$CLUSTER" >/dev/null 2>&1; then
    # servicelb stays: it is what gives the proxy's LoadBalancer an address, and binding the
    # service port on the node is what makes the host port mapping reach it.
    k3d cluster create "$CLUSTER" \
      --agents 1 \
      --k3s-arg "--disable=traefik@server:*" \
      -p "$PGPORT:5432@server:0"
  fi

  # Flannel does not enforce these, but the objects still have to apply. Pinned to the version the
  # homelab runs, because that is the schema they have to satisfy.
  k apply -f "https://raw.githubusercontent.com/cilium/cilium/$CILIUM_VERSION/pkg/k8s/apis/cilium.io/client/crds/v2/ciliumnetworkpolicies.yaml"

  k apply -f "$HERE/minio.yaml"
  k -n minio rollout status deploy/minio --timeout=180s
  k -n minio wait --for=condition=complete job/minio-bucket --timeout=180s

  docker build -t neon-ctl:e2e "$HERE/.."
  k3d image import neon-ctl:e2e -c "$CLUSTER"

  k create namespace "$NAMESPACE" --dry-run=client -o yaml | k apply -f -
  k -n "$NAMESPACE" create secret generic neon-e2e-credentials \
    --from-literal=bucketAccessKey=neon \
    --from-literal=bucketSecretKey=neonneon \
    --from-literal=controllerDbPassword=neonneon \
    --dry-run=client -o yaml | k apply -f -

  helm --kube-context "$CONTEXT" upgrade --install neon "$CHART" \
    -f "$CHART/values-e2e.yaml" -n "$NAMESPACE" --wait --timeout 10m
  ;;
down)
  k3d cluster delete "$CLUSTER"
  ;;
*)
  echo "usage: $0 [up|down]" >&2
  exit 2
  ;;
esac
