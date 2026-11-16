#!/usr/bin/env bash
set -euo pipefail

TLA_TOOLS_VERSION="1.7.4"
TLA_TOOLS_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_TOOLS_VERSION}/tla2tools.jar"
TLC_EXIT_SAFETY_VIOLATION=12

repo_root="$(git rev-parse --show-toplevel)"
spec_dir="$repo_root/specs/indexing-engine"
cache_dir="$repo_root/target/tla2tools/$TLA_TOOLS_VERSION"
jar="$cache_dir/tla2tools.jar"

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

fetch_tla_tools() {
  [[ -f "$jar" ]] && return
  mkdir -p "$cache_dir"
  local download="$cache_dir/tla2tools.jar.download"
  curl --fail --silent --show-error --location --output "$download" "$TLA_TOOLS_URL"
  local actual
  actual="$(sha256_of "$download")"
  if [[ "$actual" != "$TLA_TOOLS_SHA256" ]]; then
    rm -f "$download"
    echo "tla2tools.jar checksum mismatch: expected $TLA_TOOLS_SHA256, got $actual" >&2
    exit 1
  fi
  mv "$download" "$jar"
}

run_tlc() {
  local config="$1"
  local name
  name="$(basename "$config" .cfg)"
  java -XX:+UseParallelGC -cp "$jar" tlc2.TLC \
    -workers auto \
    -cleanup \
    -metadir "$cache_dir/states/$name" \
    -config "$config" \
    "$spec_dir/LeaseDedupe.tla"
}

expect_pass() {
  for config in "$spec_dir"/*.cfg; do
    echo "==> $(basename "$config"): expect no violation"
    run_tlc "$config"
  done
}

expect_counterexample() {
  local failed=0
  for config in "$spec_dir"/counterexamples/*.cfg; do
    echo "==> counterexamples/$(basename "$config"): expect a violation"
    local status=0
    run_tlc "$config" || status=$?
    if [[ "$status" -ne "$TLC_EXIT_SAFETY_VIOLATION" ]]; then
      echo "expected TLC exit $TLC_EXIT_SAFETY_VIOLATION (invariant violated), got $status" >&2
      failed=1
    fi
  done
  return "$failed"
}

fetch_tla_tools
case "${1:-pass}" in
  pass) expect_pass ;;
  counterexample) expect_counterexample ;;
  *) echo "usage: $0 [pass|counterexample]" >&2; exit 2 ;;
esac
