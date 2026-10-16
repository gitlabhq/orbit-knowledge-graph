#!/usr/bin/env bash
set -euo pipefail

# Run unit tests: --lib tests from all workspace crates plus --bin tests
# for binary-only crates that contain test modules, plus the integration
# test targets of code-graph-incremental (one --test per file in its tests/).
#
# integration-tests targets (local, containers, cli) run in their own CI
# jobs to avoid pulling the entire dependency graph into this job.
# integration-tests-codegraph has its own job too (lance-graph/datafusion).
#
# Extra arguments are forwarded to cargo nextest (e.g. --profile ci).

args=(cargo nextest run --workspace \
  --features orbit-prompts/skill-validation \
  --features code-graph-incremental/pretty-print \
  --exclude integration-tests \
  --exclude integration-tests-codegraph \
  --exclude orbit-fuzz \
  --exclude query-profiler \
  --lib --bin xtask --bin orbit \
  --test vfs_paths --test safe_fs \
  --test budgets --test export --test link --test parse --test pipeline \
  --test resolve --test rewrite --test snapshot --test tree_storage)

# Append caller's arguments last (e.g. --profile ci) so they can
# override defaults.
args+=("$@")

echo "+ ${args[*]}"
exec "${args[@]}"
