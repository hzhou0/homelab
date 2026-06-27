#!/usr/bin/env bash
#
# Mint a kubeconfig for the homelab autonomous operator ServiceAccount.
#
# The operator deploys with this kubeconfig. Its power is bounded entirely by the
# homelab-operator ClusterRole (broad deploy, no kyverno/RBAC/quota writes) and by
# the Kyverno admission constraints -- so this file is "deploy anything inside the
# rules" but cannot move the rules.
#
# Usage:
#   ./gen-kubeconfig.sh [-n NAMESPACE] [-s SA_NAME] [-d DURATION] [-o OUTFILE] [-a API_SERVER]
#
# Defaults match the chart values: namespace=tool-operator, sa=homelab-operator.
# DURATION uses kubectl token TTL syntax (e.g. 24h, 8760h). For a non-expiring
# token, pass -d 0 and the script falls back to a bound Secret token.
set -euo pipefail

NAMESPACE="tool-operator"
SA_NAME="homelab-operator"
DURATION="8760h"          # ~1 year
OUTFILE="operator.kubeconfig"
API_SERVER=""             # autodetected from current context if empty
CONTEXT_NAME="homelab-operator"

while getopts "n:s:d:o:a:h" opt; do
  case "$opt" in
    n) NAMESPACE="$OPTARG" ;;
    s) SA_NAME="$OPTARG" ;;
    d) DURATION="$OPTARG" ;;
    o) OUTFILE="$OPTARG" ;;
    a) API_SERVER="$OPTARG" ;;
    h) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "invalid option" >&2; exit 1 ;;
  esac
done

command -v kubectl >/dev/null || { echo "kubectl not found" >&2; exit 1; }

# Resolve API server + cluster CA from the current (admin) context.
CLUSTER_NAME="$(kubectl config view --minify -o jsonpath='{.clusters[0].name}')"
if [[ -z "$API_SERVER" ]]; then
  API_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
fi
CA_DATA="$(kubectl config view --minify --flatten -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
if [[ -z "$CA_DATA" ]]; then
  echo "Could not read certificate-authority-data from current context." >&2
  echo "Point KUBECONFIG at an admin kubeconfig that embeds the cluster CA." >&2
  exit 1
fi

# Mint a token for the SA.
if [[ "$DURATION" == "0" ]]; then
  echo "Creating a long-lived bound Secret token for ${NAMESPACE}/${SA_NAME}..."
  SECRET_NAME="${SA_NAME}-token"
  kubectl -n "$NAMESPACE" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${SECRET_NAME}
  annotations:
    kubernetes.io/service-account.name: ${SA_NAME}
type: kubernetes.io/service-account-token
EOF
  # Wait for the controller to populate the token.
  for _ in $(seq 1 30); do
    TOKEN="$(kubectl -n "$NAMESPACE" get secret "$SECRET_NAME" -o jsonpath='{.data.token}' 2>/dev/null | base64 -d || true)"
    [[ -n "$TOKEN" ]] && break
    sleep 1
  done
  [[ -n "${TOKEN:-}" ]] || { echo "token Secret not populated" >&2; exit 1; }
else
  echo "Minting a ${DURATION} token for ${NAMESPACE}/${SA_NAME}..."
  TOKEN="$(kubectl -n "$NAMESPACE" create token "$SA_NAME" --duration="$DURATION")"
fi

# Assemble the kubeconfig.
cat > "$OUTFILE" <<EOF
apiVersion: v1
kind: Config
clusters:
  - name: ${CLUSTER_NAME}
    cluster:
      server: ${API_SERVER}
      certificate-authority-data: ${CA_DATA}
contexts:
  - name: ${CONTEXT_NAME}
    context:
      cluster: ${CLUSTER_NAME}
      namespace: ${NAMESPACE}
      user: ${SA_NAME}
current-context: ${CONTEXT_NAME}
users:
  - name: ${SA_NAME}
    user:
      token: ${TOKEN}
EOF

chmod 600 "$OUTFILE"
echo "Wrote ${OUTFILE}"
echo "Sanity check: kubectl --kubeconfig ${OUTFILE} auth can-i create deployments -n app-demo"
