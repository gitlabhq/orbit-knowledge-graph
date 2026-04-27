#!/usr/bin/env bash
set -euo pipefail

# Run (or archive) unit + non-Docker integration tests.
#
# Usage:
#   run-unit-tests.sh [run] [extra args...]      # default: cargo nextest run
#   run-unit-tests.sh archive [extra args...]    # cargo nextest archive
#
# Test targets are auto-discovered from tests/*.rs files in the
# integration-tests crate, excluding "containers" (Docker-required) and
# "cli" (has its own job that builds the orbit binary).
#
# Extra arguments are forwarded to cargo nextest (e.g. --profile ci,
# --archive-file target/nextest-archive.tar.zst).

command -v jq >/dev/null 2>&1 || { echo "error: jq is required but not found" >&2; exit 1; }

mode="run"
if [[ ${1:-} == "archive" || ${1:-} == "run" ]]; then
  mode="$1"
  shift
fi

NON_DOCKER_TESTS=$(
  cargo metadata --no-deps --format-version 1 | \
  jq -r '.packages[]
    | select(.name == "integration-tests")
    | .targets[]
    | select((.kind | index("test")) and .name != "containers" and .name != "cli")
    | .name'
)

# --lib --bins covers unit tests from all workspace crates; each --test flag
# adds a discovered integration test target. integration-tests-codegraph,
# gkg-fuzz, and query-profiler are excluded — they have separate jobs or are
# not test crates.
args=(cargo nextest "$mode" --workspace \
  --exclude integration-tests-codegraph \
  --exclude gkg-fuzz \
  --exclude query-profiler \
  --lib --bins)
while IFS= read -r t; do
  [[ -n "$t" ]] && args+=(--test "$t")
done <<<"$NON_DOCKER_TESTS"

args+=("$@")

echo "+ ${args[*]}"
exec "${args[@]}"
