#!/usr/bin/env bash

set -euo pipefail

readonly CI_CONFIG="${CI_CONFIG:-.gitlab-ci.yml}"
readonly -a LANES=(
  integration-test
  integration-test-data-correctness
  corpus-smoke-test
)

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

list_tests() {
  local output=$1
  local filter=${2:-}
  local -a command=(
    cargo nextest list
    --all-features
    --test containers
    -p integration-tests
    --message-format oneline
  )

  if [[ -n "$filter" ]]; then
    command+=(-E "$filter")
  fi

  "${command[@]}" | awk '{print $2}' | sort > "$output"
}

list_tests "$tmp_dir/all"

for lane in "${LANES[@]}"; do
  filter=$(yq -r ".\"$lane\".variables.NEXTEST_FILTER" "$CI_CONFIG")
  if [[ -z "$filter" || "$filter" == "null" ]]; then
    echo "Missing NEXTEST_FILTER for $lane in $CI_CONFIG" >&2
    exit 1
  fi

  list_tests "$tmp_dir/$lane" "$filter"
  printf '%s: %s tests\n' "$lane" "$(wc -l < "$tmp_dir/$lane")"
done

cat "${LANES[@]/#/$tmp_dir/}" > "$tmp_dir/combined"
sort "$tmp_dir/combined" > "$tmp_dir/combined-sorted"
sort -u "$tmp_dir/combined" > "$tmp_dir/union"

duplicates=$(uniq -d "$tmp_dir/combined-sorted")
if [[ -n "$duplicates" ]]; then
  printf 'Tests assigned to more than one integration lane:\n%s\n' "$duplicates" >&2
  exit 1
fi

if ! diff -u "$tmp_dir/all" "$tmp_dir/union"; then
  echo "Integration lane filters do not cover every container test exactly once." >&2
  exit 1
fi

printf 'All %s container tests are assigned to exactly one integration lane.\n' "$(wc -l < "$tmp_dir/all")"
