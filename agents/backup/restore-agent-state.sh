#!/usr/bin/env bash
#
# Recreate the operator agent's ad-hoc app-*/tool-* resources on a rebuilt cluster.
#
# Why: nothing reconciles what the agent deploys. There is no chart and no manifest in this repo
# behind those namespaces, so the nightly export is their only source of truth — the same position
# migration/DUAL-STACK-MIGRATION.md was in at Phase 0, where this capture was done by hand.
#
# The export is faithful, so the pruning that doc did before writing the file happens here instead:
# server-assigned identity comes off every object, and Services give up the addresses and families
# the old cluster allocated so the new one can allocate its own.
#
# Runs as whoever invokes it, which must be someone who can write outside app-*/tool-* — the agent's
# own credentials cannot recreate its namespaces.
#
# Usage:
#   ./agents/backup/restore-agent-state.sh                            # newest snapshot
#   ./agents/backup/restore-agent-state.sh agent-state-2026...Z.json  # a specific one
#   ./agents/backup/restore-agent-state.sh --dry-run                  # admit it, write nothing
set -euo pipefail

NAMESPACE="${NAMESPACE:-agent-backup}"
SECRET="${SECRET:-hypha-client}"
CRONJOB="${CRONJOB:-agent-state-backup}"

DRY_RUN=0
KEY=""
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    -*) echo "unknown option: $arg" >&2; exit 2 ;;
    *) KEY="$arg" ;;
  esac
done
KEY="${KEY:-agent-state-latest.json}"

# The CronJob writing the snapshots is the record of where they go, so asking it keeps a second copy
# of the bucket and endpoint out of this script.
backup_env() {
  kubectl -n "$NAMESPACE" get cronjob "$CRONJOB" \
    -o jsonpath="{.spec.jobTemplate.spec.template.spec.containers[0].env[?(@.name=='$1')].value}"
}
BUCKET="${BUCKET:-$(backup_env BUCKET)}"
ENDPOINT="${ENDPOINT:-$(backup_env ENDPOINT)}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$(backup_env AWS_DEFAULT_REGION)}"
if [ -z "$BUCKET" ] || [ -z "$ENDPOINT" ]; then
  echo "cannot read the backup destination from cronjob/$CRONJOB; set BUCKET and ENDPOINT" >&2
  exit 1
fi

secret_value() {
  kubectl -n "$NAMESPACE" get secret "$SECRET" -o jsonpath="{.data.$1}" | base64 -d
}
AWS_ACCESS_KEY_ID="$(secret_value accessKeyId)"
AWS_SECRET_ACCESS_KEY="$(secret_value secretAccessKey)"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

aws s3 cp "s3://$BUCKET/$KEY" "$work/snapshot.json" --endpoint-url "$ENDPOINT"

jq '
  .items |= map(
    del(
      .metadata.uid,
      .metadata.resourceVersion,
      .metadata.generation,
      .metadata.creationTimestamp,
      .metadata.managedFields,
      .metadata.ownerReferences,
      .metadata.finalizers,
      .metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"],
      .metadata.annotations["deployment.kubernetes.io/revision"],
      .metadata.annotations["pv.kubernetes.io/bind-completed"],
      .metadata.annotations["pv.kubernetes.io/bound-by-controller"],
      .metadata.annotations["volume.kubernetes.io/storage-provisioner"],
      .metadata.annotations["volume.beta.kubernetes.io/storage-provisioner"],
      .status
    )
    # Addresses and families the old cluster allocated. Kept, they are either rejected outright or
    # pin a service to an IP family the rebuilt cluster may no longer offer.
    | if .kind == "Service"
      then del(.spec.clusterIP, .spec.clusterIPs, .spec.ipFamilies, .spec.ipFamilyPolicy)
           | .spec.ports |= map(del(.nodePort))
      else .
      end
  )
' "$work/snapshot.json" > "$work/restore.json"

# Namespaces alone first: the tier guardrails are generated on create, and a workload admitted
# before them lands in a namespace with no quota, no limits and no default-deny.
jq '.items |= map(select(.kind == "Namespace"))' "$work/restore.json" > "$work/namespaces.json"
jq '.items |= map(select(.kind != "Namespace"))' "$work/restore.json" > "$work/contents.json"

kubectl apply --dry-run=server -f "$work/namespaces.json"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "dry run: namespaces admitted, contents not checked (they need the namespaces to exist)"
  exit 0
fi

kubectl apply -f "$work/namespaces.json"

# The generated guardrails and the operator's own write permission attach a moment after the
# namespace exists, so the first attempt at its contents can be rejected on timing alone.
for attempt in 1 2 3; do
  if kubectl apply -f "$work/contents.json"; then
    exit 0
  fi
  echo "retrying namespace contents (attempt $attempt)" >&2
  sleep 5
done
echo "contents failed to apply after 3 attempts" >&2
exit 1
