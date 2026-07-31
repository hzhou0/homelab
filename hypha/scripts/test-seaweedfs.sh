#!/usr/bin/env bash
# Run the integration suite with **SeaweedFS as the cache** and a per-test MinIO remote.
#
# MinIO ignores `If-Match` on `DeleteObject`, so cache-side marker and shadow CAS need the backend the
# cluster actually uses. The remote stays on MinIO: its deletes are serialized inside Hypha and need
# no conditional-delete extension.
#
# One server for the whole run rather than one per test: the fixture costs ~10 s to become ready, and
# tests are isolated by their per-harness `bucket_prefix` anyway (`list_buckets` filters by it, §9).
# The volume is the only state a run leaves behind, and `down -v` at exit is what drops it — on
# success, on failure, and on Ctrl-C alike.
#
# Requires docker with the compose plugin. Extra args replace the default test selection:
#   scripts/test-seaweedfs.sh                        # the whole workspace
#   scripts/test-seaweedfs.sh --test evict           # one suite
# HYPHA_TEST_THREADS bounds the parallelism (default 4); the fixture is one server for the whole run.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$REPO/scripts/seaweedfs-test.yml"
# Its own project name, so a run cannot collide with — or tear down — anything else on the host.
PROJECT="hypha-test-seaweedfs-$$"

command -v docker >/dev/null || { echo "error: docker is not on PATH" >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "error: the docker compose plugin is required" >&2; exit 1; }

# A free loopback port, pure bash: a failed /dev/tcp connect means nothing is listening there.
free_port() {
  local p
  while :; do
    p=$(( (RANDOM % 20000) + 20000 ))
    (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null || { echo "$p"; return; }
  done
}
export SEAWEED_S3_PORT="${SEAWEED_S3_PORT:-$(free_port)}"

compose() { docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"; }

cleanup() {
  local status=$?
  echo "→ tearing down the fixture and its volume"
  # -v is the point: the volume is the run's only residue, and a suite that left one behind would
  # hand the next run a keyspace it did not create.
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT INT TERM

echo "→ starting SeaweedFS on :$SEAWEED_S3_PORT"
# --wait blocks on the healthcheck, which is a bucket round trip, so this returns only once a test
# could actually run.
if ! compose up -d --wait; then
  echo "error: SeaweedFS did not become healthy" >&2
  compose logs --tail 40 seaweedfs >&2 || true
  exit 1
fi

DEFAULT_ARGS=(--workspace)
if [ "$#" -gt 0 ]; then
  CARGO_ARGS=("$@")
else
  CARGO_ARGS=("${DEFAULT_ARGS[@]}")
fi

# Cache traffic from every fixture shares this server; bound the suite so its deliberately tight
# reconcile and GC cadences do not turn backend saturation into false failures.
TEST_THREADS="${HYPHA_TEST_THREADS:-4}"

SEAWEED_ENDPOINT="http://127.0.0.1:$SEAWEED_S3_PORT"

echo "→ cargo test ${CARGO_ARGS[*]} -- --test-threads=$TEST_THREADS"
TEST_CACHE_S3_ENDPOINT="$SEAWEED_ENDPOINT" \
  cargo test --manifest-path "$REPO/Cargo.toml" "${CARGO_ARGS[@]}" \
  -- --test-threads="$TEST_THREADS"

if [ "$#" -eq 0 ]; then
  echo "→ cache conditional-delete convergence"
  TEST_CACHE_S3_ENDPOINT="$SEAWEED_ENDPOINT" \
    cargo test --manifest-path "$REPO/Cargo.toml" --test cached \
    bursty_same_key_overwrites_converge_on_the_last_acked_generation \
    -- --exact --ignored
fi
