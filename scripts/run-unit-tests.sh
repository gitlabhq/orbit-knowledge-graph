#!/usr/bin/env bash
set -euo pipefail

# Run unit tests: --lib tests from all workspace crates plus --bin tests
# for binary-only crates that contain test modules.
#
# integration-tests targets (local, containers, cli) run in their own CI
# jobs to avoid pulling the entire dependency graph into this job.
# integration-tests-codegraph has its own job too (lance-graph/datafusion).
#
# Extra arguments are forwarded to cargo nextest (e.g. --profile ci).

args=(cargo nextest run --workspace \
  --exclude integration-tests \
  --exclude integration-tests-codegraph \
  --exclude gkg-fuzz \
  --exclude query-profiler \
  --lib --bin xtask)

# Append caller's arguments last (e.g. --profile ci) so they can
# override defaults.
args+=("$@")

echo "+ ${args[*]}"
exec "${args[@]}"
