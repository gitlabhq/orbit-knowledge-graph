#!/usr/bin/env bash
set -euo pipefail

archive="${1:?usage: $0 <orbit-cli-*.tar.gz>}"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
tar -xzf "$archive" -C "$work"
orbit="$work/orbit"

export HOME="$work/home" ORBIT_TELEMETRY_ENABLED=false
mkdir -p "$HOME"

repo="$work/repo"
mkdir -p "$repo"
printf 'def helper():\n    return 1\n' >"$repo/app.py"
git -C "$repo" init -q
git -C "$repo" add app.py
git -C "$repo" -c user.name=smoke -c user.email=smoke@example.com commit -qm init

"$orbit" version
"$orbit" index "$repo"
grep_output=$(cd "$repo" && "$orbit" grep helper)
grep -q 'app\.helper' <<<"$grep_output"
context_output=$(cd "$repo" && "$orbit" context app.py)
grep -q 'app\.helper' <<<"$context_output"

for extensions in "$HOME/.duckdb/extensions" "$HOME/.gitlab/orbit/duckdb-extensions"; do
  if [ -d "$extensions" ]; then
    echo "FAIL: $archive downloaded DuckDB extensions to $extensions" >&2
    exit 1
  fi
done

echo "OK: $archive indexes and queries on $(. /etc/os-release && echo "$PRETTY_NAME")"
