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
CILIUM_VERSION="${CILIUM_VERSION:-v1.20.0}"
GATEWAY_API_VERSION="${GATEWAY_API_VERSION:-v1.6.1}"
# The homelab's server version: CRD schemas carry CEL the API server has to be new enough to
# compile, so an older default silently tests against a validator the real cluster does not use.
K3S_VERSION="${K3S_VERSION:-v1.36.3-k3s1}"

k() { kubectl --context "$CONTEXT" "$@"; }

case "${1:-up}" in
up)
  if ! k3d cluster list "$CLUSTER" >/dev/null 2>&1; then
    k3d cluster create "$CLUSTER" \
      --image "rancher/k3s:$K3S_VERSION" \
      --agents 1 \
      --k3s-arg "--disable=traefik@server:*"
  fi

  # Flannel does not enforce these, but the objects still have to apply. Pinned to the version the
  # homelab runs, because that is the schema they have to satisfy.
  k apply -f "https://raw.githubusercontent.com/cilium/cilium/$CILIUM_VERSION/pkg/k8s/apis/cilium.io/client/crds/v2/ciliumnetworkpolicies.yaml"

  # The experimental channel, because TCPRoute is only published there and the proxy's listener is
  # a TCPRoute. Nothing here routes through a gateway; the objects still have to apply.
  k apply --server-side -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/$GATEWAY_API_VERSION/experimental-install.yaml"

  k apply -f "$HERE/minio.yaml"
  k -n minio rollout status deploy/minio --timeout=180s
  k -n minio wait --for=condition=complete job/minio-bucket --timeout=180s

  docker build -t neon-ctl:e2e "$HERE/.."
  k3d image import neon-ctl:e2e -c "$CLUSTER"

  k create namespace "$NAMESPACE" --dry-run=client -o yaml | k apply -f -

  # One key, per run. Everything else each pod derives for itself, which is also what the real
  # deployment does — without it the storage controller refuses to start outside --dev.
  keys="$(mktemp -d)"
  trap 'rm -rf "$keys"' EXIT
  openssl genpkey -algorithm ed25519 -out "$keys/auth.pem" 2>/dev/null

  k -n "$NAMESPACE" create secret generic neon-e2e-credentials \
    --from-literal=bucketAccessKey=neon \
    --from-literal=bucketSecretKey=neonneon \
    --from-literal=controllerDbPassword=neonneon \
    --from-file=authPrivateKey="$keys/auth.pem" \
    --dry-run=client -o yaml | k apply -f -

  helm --kube-context "$CONTEXT" upgrade --install neon "$CHART" \
    -f "$CHART/values-e2e.yaml" -n "$NAMESPACE" --wait --timeout 10m

  # The proxy is a ClusterIP behind a TCPRoute, and no gateway runs here to serve that route.
  echo "proxy: kubectl --context $CONTEXT -n $NAMESPACE port-forward svc/neon-proxy 15432:5432"
  ;;
down)
  k3d cluster delete "$CLUSTER"
  ;;
*)
  echo "usage: $0 [up|down]" >&2
  exit 2
  ;;
esac
