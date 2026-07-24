#!/usr/bin/env bash
# Run the `s3s-e2e` conformance suite against a freshly booted hypha.
#
# Boots a throwaway MinIO (both cache and remote, kept disjoint by bucket_prefix — the same
# topology as the in-process harness in tests/common), starts hypha in durable mode in front of
# it, then drives `s3s-e2e` at hypha over the standard AWS_* env. Everything is torn down on exit.
#
# Requires `minio` and `s3s-e2e` on PATH:  cargo install s3s-e2e --locked
# Extra args pass through to s3s-e2e, e.g.:  scripts/s3s-e2e.sh --list
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Same fixture values the integration harness uses (tests/common/mod.rs), so a failure here and a
# failure there mean the same thing.
MINIO_USER=minioadmin
MINIO_PASS=minioadmin
HYPHA_ACCESS=hyphatestaccess
HYPHA_SECRET=hyphatestsecretkey
MASTER_PASSPHRASE=integration-test-master-passphrase-0123456789abcdef

for bin in minio s3s-e2e; do
  command -v "$bin" >/dev/null || { echo "error: '$bin' not on PATH" >&2; exit 1; }
done

# A free loopback port, pure bash: a failed /dev/tcp connect means nothing is listening there.
free_port() {
  local p
  while :; do
    p=$(( (RANDOM % 20000) + 20000 ))
    (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null || { echo "$p"; return; }
  done
}
MINIO_PORT=$(free_port); MINIO_CONSOLE=$(free_port); HYPHA_PORT=$(free_port)
DATA_DIR=$(mktemp -d)

MINIO_PID=""; HYPHA_PID=""
cleanup() {
  [ -n "$HYPHA_PID" ] && kill "$HYPHA_PID" 2>/dev/null || true
  [ -n "$MINIO_PID" ] && kill "$MINIO_PID" 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo "→ building hypha"
cargo build --quiet --manifest-path "$REPO/Cargo.toml" --bin hypha

echo "→ starting MinIO on :$MINIO_PORT"
MINIO_ROOT_USER=$MINIO_USER MINIO_ROOT_PASSWORD=$MINIO_PASS MINIO_UPDATE=off \
  minio server "$DATA_DIR" --address "127.0.0.1:$MINIO_PORT" \
  --console-address "127.0.0.1:$MINIO_CONSOLE" >/dev/null 2>&1 &
MINIO_PID=$!
until curl -fsS "http://127.0.0.1:$MINIO_PORT/minio/health/live" >/dev/null 2>&1; do sleep 0.2; done

echo "→ starting hypha on :$HYPHA_PORT"
ENDPOINT="http://127.0.0.1:$MINIO_PORT"
env -C "$DATA_DIR" \
  HYPHA_MODE=durable \
  HYPHA_MASTER_PASSPHRASE="$MASTER_PASSPHRASE" \
  HYPHA_SERVING__LISTEN="127.0.0.1:$HYPHA_PORT" \
  HYPHA_AUTH__ACCESS_KEY="$HYPHA_ACCESS" HYPHA_AUTH__SECRET_KEY="$HYPHA_SECRET" \
  HYPHA_REMOTE__ENDPOINT="$ENDPOINT" HYPHA_REMOTE__ACCESS_KEY="$MINIO_USER" \
  HYPHA_REMOTE__SECRET_KEY="$MINIO_PASS" HYPHA_REMOTE__BUCKET_PREFIX=remote- \
  HYPHA_CACHE__ENDPOINT="$ENDPOINT" HYPHA_CACHE__ACCESS_KEY="$MINIO_USER" \
  HYPHA_CACHE__SECRET_KEY="$MINIO_PASS" HYPHA_CACHE__BUCKET_PREFIX=cache- \
  "$REPO/target/debug/hypha" >/dev/null 2>&1 &
HYPHA_PID=$!
until curl -s -o /dev/null "http://127.0.0.1:$HYPHA_PORT/"; do
  kill -0 "$HYPHA_PID" 2>/dev/null || { echo "hypha exited during startup" >&2; exit 1; }
  sleep 0.2
done

echo "→ running s3s-e2e"
# Path-style + checksum trailers off, matching the aws-sdk client config in tests/common (hypha's
# SigV4 verification doesn't accept the SDK's default flexible-checksum trailers).
# Not `exec`: the EXIT trap must still fire to tear down hypha + MinIO.
env \
  AWS_ENDPOINT_URL="http://127.0.0.1:$HYPHA_PORT" \
  AWS_ACCESS_KEY_ID="$HYPHA_ACCESS" AWS_SECRET_ACCESS_KEY="$HYPHA_SECRET" \
  AWS_REGION=us-east-1 \
  AWS_REQUEST_CHECKSUM_CALCULATION=when_required \
  AWS_RESPONSE_CHECKSUM_VALIDATION=when_required \
  AWS_EC2_METADATA_DISABLED=true \
  s3s-e2e "$@"
