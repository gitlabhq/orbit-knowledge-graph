#!/usr/bin/env bash
set -euo pipefail

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
  --test macros --test introduced_versions \
  --test build --test config --test dedupe --test errors --test jobs \
  --test lanes --test pools --test reconcilers --test sweeps --test testkit \
  --test protocol --test simulation)

args+=("$@")

echo "+ ${args[*]}"
"${args[@]}"

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

ignored=$(awk '/^test result:/ { for (i = 1; i < NF; i++) if ($(i + 1) == "ignored;") n += $i } END { print n + 0 }' "$doc_log")
if [[ "$ignored" -ne 0 ]]; then
  echo "error: $ignored doctest(s) are ignored; \`ignore\` doc examples are not allowed." >&2
  echo "Use a plain or \`no_run\` block with hidden \`# \` setup lines for real APIs," >&2
  echo "\`text\` for pseudo-code, or prose with an intra-doc link for private items." >&2
  echo "Offending blocks:" >&2
  grep -E '^test .* \.\.\. ignored' "$doc_log" >&2 || true
  exit 1
fi
