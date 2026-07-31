#!/usr/bin/env bash
# Run the integration suite against a throwaway **SeaweedFS** instead of the per-test MinIO.
#
# Why: MinIO ignores `If-Match` on `DeleteObject` (tests/backend.rs), so against it every path whose
# correctness rests on a conditional delete — the reconcile sweep's marker CAS, the cached DELETE
# branch's generation-bound remote delete, both shadow reclaims — is exercised with the precondition
# doing nothing. SeaweedFS enforces it, and it is what the cluster runs (§9), so this is where those
# paths are actually tested and where the `#[ignore]`d convergence test passes.
#
# One server for the whole run rather than one per test: the fixture costs ~10 s to become ready, and
# tests are isolated by their per-harness `bucket_prefix` anyway (`list_buckets` filters by it, §9).
# The volume is the only state a run leaves behind, and `down -v` at exit is what drops it — on
# success, on failure, and on Ctrl-C alike.
#
# The dialect differences this fixture found are recorded in tests/backend.rs and §12; hypha now
# depends on none of them, so the whole workspace passes here as it does on MinIO. There is no
# expected-red list — a standing red would train the eye past the ones that mean something.
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

# `--include-ignored` is what picks up the two tests that cannot pass on MinIO — the convergence test
# and the ListMultipartUploads prefix filter — and running them is the whole point of the fixture.
DEFAULT_ARGS=(--workspace)
if [ "$#" -gt 0 ]; then
  CARGO_ARGS=("$@")
else
  CARGO_ARGS=("${DEFAULT_ARGS[@]}")
fi

# **Bounded, unlike the default run.** With a MinIO per test the suite's parallelism costs nothing
# shared; here every fixture's client path, reconcile sweep and GC actor lands on one server, and the
# tight cadences the harness runs them at multiply. Left unbounded the server saturates and tests
# fail on backend errors that say nothing about hypha.
TEST_THREADS="${HYPHA_TEST_THREADS:-4}"

echo "→ cargo test ${CARGO_ARGS[*]} -- --include-ignored --test-threads=$TEST_THREADS"
TEST_S3_ENDPOINT="http://127.0.0.1:$SEAWEED_S3_PORT" \
  cargo test --manifest-path "$REPO/Cargo.toml" "${CARGO_ARGS[@]}" \
  -- --include-ignored --test-threads="$TEST_THREADS"
