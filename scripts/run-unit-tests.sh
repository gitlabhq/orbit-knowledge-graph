#!/usr/bin/env bash
set -euo pipefail

# Discover all integration-tests test targets except "containers" (which
# needs Docker/testcontainers) and run them alongside lib + bin tests.
#
# Test targets are auto-discovered from tests/*.rs files in the
# integration-tests crate — no Cargo.toml or CI changes needed.
#
# Extra arguments are forwarded to cargo nextest (e.g. --profile ci).

command -v jq >/dev/null 2>&1 || { echo "error: jq is required but not found" >&2; exit 1; }

# Query cargo metadata for integration-tests test targets, excluding the
# "containers" target which requires Docker via testcontainers.
NON_DOCKER_TESTS=$(
  cargo metadata --no-deps --format-version 1 | \
  jq -r '.packages[]
    | select(.name == "integration-tests")
    | .targets[]
    | select((.kind | index("test")) and .name != "containers" and .name != "cli")
    | .name'
)

# Build the command as an array to preserve argument boundaries.
# --lib --bins runs unit tests from all workspace crates, then each
# --test flag adds a discovered integration test target.
#
# integration-tests-codegraph is excluded here — it has its own CI job
# to avoid pulling code-graph/lance-graph/datafusion deps into this job.
args=(cargo nextest run --workspace \
  --exclude integration-tests-codegraph \
  --exclude gkg-fuzz \
  --exclude query-profiler \
  --lib --bins)
while IFS= read -r t; do
  [[ -n "$t" ]] && args+=(--test "$t")
done <<<"$NON_DOCKER_TESTS"

# Append caller's arguments last (e.g. --profile ci) so they can
# override defaults.
args+=("$@")

echo "+ ${args[*]}"
exec "${args[@]}"
