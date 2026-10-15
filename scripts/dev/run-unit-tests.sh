#!/usr/bin/env bash
set -euo pipefail

# Run unit tests: --lib tests from all workspace crates plus --bin tests
# for binary-only crates that contain test modules, then every integration
# test target of code-graph-incremental.
#
# integration-tests targets (local, containers, cli) run in their own CI
# jobs to avoid pulling the entire dependency graph into this job.
# integration-tests-codegraph has its own job too (lance-graph/datafusion).
#
# NEXTEST_PROFILE selects the nextest profile; the second run uses the
# profile with a -codegraph suffix so both runs keep their JUnit report.
# Extra arguments are forwarded to both cargo nextest runs.

args=(cargo nextest run --workspace \
  --features orbit-prompts/skill-validation \
  --features code-graph-incremental/pretty-print \
  --exclude integration-tests \
  --exclude integration-tests-codegraph \
  --exclude orbit-fuzz \
  --exclude query-profiler \
  --lib --bin xtask --bin orbit \
  --test vfs_paths --test safe_fs)
targets=(cargo nextest run -p code-graph-incremental --test '*')

# Append caller's arguments last so they can override defaults.
args+=("$@")
targets+=("$@")

status=0
echo "+ ${args[*]}"
"${args[@]}" || status=$?
echo "+ ${targets[*]}"
NEXTEST_PROFILE="${NEXTEST_PROFILE:+${NEXTEST_PROFILE}-codegraph}" "${targets[@]}" || status=$?
exit "$status"
