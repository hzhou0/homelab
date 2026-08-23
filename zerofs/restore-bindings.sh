#!/usr/bin/env bash
#
# Reattach ZeroFS volumes to a rebuilt cluster from a binding snapshot.
#
# Why: a volume is a directory named by its handle, and the objects naming it are the only record of
# which workload owned which directory. The backup CronJob uploads them verbatim, so everything the
# API server assigns has to come back off before they will be accepted as new objects. That is the
# subtraction this performs.
#
# Runs as whoever invokes it — no ServiceAccount, no in-cluster role — so it needs read access to the
# gateway's Secret for the object-store credentials, and write access to volumes and claims.
#
# Run this BEFORE reinstalling the workloads: a claim admitted first is handed a new empty directory,
# orphaning the one holding the data.
#
# Usage:
#   ./zerofs/restore-bindings.sh                          # newest snapshot
#   ./zerofs/restore-bindings.sh bindings-2026...Z.json   # a specific one
#   ./zerofs/restore-bindings.sh --dry-run                # admit it, write nothing
set -euo pipefail

NAMESPACE="${NAMESPACE:-zerofs}"
SECRET="${SECRET:-zerofs}"
CRONJOB="${CRONJOB:-zerofs-binding-backup}"

DRY_RUN=0
KEY=""
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    -*) echo "unknown option: $arg" >&2; exit 2 ;;
    *) KEY="$arg" ;;
  esac
done
KEY="${KEY:-bindings-latest.json}"

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
      .metadata.creationTimestamp,
      .metadata.generation,
      .metadata.managedFields,
      .metadata.finalizers,
      .metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"],
      .metadata.annotations["pv.kubernetes.io/bind-completed"],
      .metadata.annotations["pv.kubernetes.io/bound-by-controller"],
      .metadata.annotations["volume.kubernetes.io/storage-provisioner"],
      .metadata.annotations["volume.beta.kubernetes.io/storage-provisioner"],
      .spec.claimRef.uid,
      .spec.claimRef.resourceVersion,
      .status
    )
    | if .kind == "PersistentVolume"
      then .spec.persistentVolumeReclaimPolicy = "Retain"
      else .
      end
  )
' "$work/snapshot.json" > "$work/restore.json"

# Nothing is written until the whole set is known to be admissible: a partial restore leaves volumes
# reserved for claims that never arrive.
kubectl apply --dry-run=server -f "$work/restore.json"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "dry run: nothing applied"
  exit 0
fi

# Volumes first. A claim admitted before the volume reserving it exists is a claim the provisioner
# hands a new empty directory.
for kind in PersistentVolume PersistentVolumeClaim; do
  jq --arg kind "$kind" '.items |= map(select(.kind == $kind))' "$work/restore.json" > "$work/$kind.json"
  kubectl apply -f "$work/$kind.json"
done
