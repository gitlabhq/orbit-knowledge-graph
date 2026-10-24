#!/usr/bin/env bash
set -euo pipefail

# Run unit tests: --lib tests from all workspace crates plus --bin tests
# for binary-only crates that contain test modules, plus the integration
# test targets of code-graph-incremental (one --test per file in its tests/),
# plus the workspace doctests.
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
  --test resolve --test rewrite --test snapshot --test tree_storage \
  --test macros)

# Append caller's arguments last (e.g. --profile ci) so they can
# override defaults.
args+=("$@")

echo "+ ${args[*]}"
"${args[@]}"

# Doctests: nextest does not run them. indexer/testkit gates items that
# examples use. Not --all-features: it enables orbit-server/regenerate-protos,
# which writes into src/proto.
doc_tests=(cargo test --doc --workspace
  --exclude integration-tests
  --exclude integration-tests-codegraph
  --exclude orbit-fuzz
  --exclude query-profiler
  --features orbit-prompts/skill-validation
  --features code-graph-incremental/pretty-print
  --features indexer/testkit)
echo "+ ${doc_tests[*]}"
doc_log=$(mktemp)
trap 'rm -f "$doc_log"' EXIT
"${doc_tests[@]}" 2>&1 | tee "$doc_log"

# cargo exits 0 when doctests are ignored, so count them from rustdoc's own
# summary instead of grepping sources for the tag.
ignored=$(awk '/^test result:/ { for (i = 1; i < NF; i++) if ($(i + 1) == "ignored;") n += $i } END { print n + 0 }' "$doc_log")
if [[ "$ignored" -ne 0 ]]; then
  echo "error: $ignored doctest(s) are ignored; \`ignore\` doc examples are not allowed." >&2
  echo "Use a plain or \`no_run\` block with hidden \`# \` setup lines for real APIs," >&2
  echo "\`text\` for pseudo-code, or prose with an intra-doc link for private items." >&2
  echo "Offending blocks:" >&2
  grep -E '^test .* \.\.\. ignored' "$doc_log" >&2 || true
  exit 1
fi
