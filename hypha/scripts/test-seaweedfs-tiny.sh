#!/usr/bin/env bash
# Run `tests/exhaustion.rs` against a SeaweedFS sized to **run out**, so hypha meets a backing store
# that has genuinely stopped accepting bytes rather than a proxy pretending to be one.
#
# The fixture is the same compose file as `test-seaweedfs.sh`, started with a one-megabyte volume
# size and a handful of volumes. Exhausting it is a few megabytes of ballast; SeaweedFS does not
# reclaim a deleted object's space without a vacuum, so once full it stays full — which is what makes
# the assertions stable rather than racy.
#
# **One fixture per test**, which is the whole reason this is a script and not another selection in
# `test-seaweedfs.sh`: exhaustion is permanent, so a second test sharing the first one's fixture would
# start on a store too full to create its own buckets on. Each test therefore gets its own, and the
# loop below is what gives it one.
#
# The fixture is wired to a *single* role — `TEST_S3_TINY_ENDPOINT` — and the tests point either the
# remote or the cache at it while the other stays on the harness's own MinIO. Undersizing both at once
# would only ever prove "everything fails"; undersizing one is what separates "the remote is full, so
# a cached write still acks" from "the cache is full, so it must not".
#
# Requires docker with the compose plugin, and MinIO on PATH for the healthy half.
#   scripts/test-seaweedfs-tiny.sh                    # every exhaustion test, a fixture each
#   scripts/test-seaweedfs-tiny.sh <substring> ...    # only these
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$REPO/scripts/seaweedfs-test.yml"

command -v docker >/dev/null || { echo "error: docker is not on PATH" >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "error: the docker compose plugin is required" >&2; exit 1; }

# Small enough to exhaust in a few megabytes, with room for the buckets hypha provisions before any
# ballast lands: a client bucket is one collection per role, and a collection takes a volume of its
# own on first write.
export SEAWEED_VOLUME_SIZE_MB=1
export SEAWEED_VOLUME_MAX=8

free_port() {
  local p
  while :; do
    p=$(( (RANDOM % 20000) + 20000 ))
    (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null || { echo "$p"; return; }
  done
}

TESTS=(
  a_full_remote_refuses_durable_writes_and_keeps_what_it_committed
  a_full_remote_still_acks_cached_writes_and_carries_the_obligation
  a_full_cache_never_acks_a_cached_write_it_could_not_commit
)
if [ "$#" -gt 0 ]; then
  TESTS=("$@")
fi

PROJECT=""
compose() { docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"; }

cleanup() {
  local status=$?
  [ -n "$PROJECT" ] && compose down -v --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT INT TERM

# Build once. Each test then runs against a fresh fixture with no compile in between, so a slow build
# cannot be mistaken for a slow fixture.
cargo build --manifest-path "$REPO/Cargo.toml" --tests >/dev/null

for test in "${TESTS[@]}"; do
  PROJECT="hypha-test-tiny-$$-${test:0:20}"
  export SEAWEED_S3_PORT="$(free_port)"
  echo "→ $test — SeaweedFS on :$SEAWEED_S3_PORT (${SEAWEED_VOLUME_MAX} × ${SEAWEED_VOLUME_SIZE_MB} MiB)"
  if ! compose up -d --wait; then
    echo "error: the undersized SeaweedFS did not become healthy" >&2
    compose logs --tail 40 seaweedfs >&2 || true
    exit 1
  fi
  # No TEST_S3_ENDPOINT: the *healthy* role stays on a per-test MinIO, and only the role under test
  # is pointed at the fixture.
  TEST_S3_TINY_ENDPOINT="http://127.0.0.1:$SEAWEED_S3_PORT" \
    cargo test --manifest-path "$REPO/Cargo.toml" --test exhaustion -- --exact "$test" --nocapture
  compose down -v --remove-orphans >/dev/null 2>&1
  PROJECT=""
done
