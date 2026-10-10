#!/usr/bin/env bash
# Checks the dependency graphs from Cargo.lock: the server must resolve the
# AWS-LC FIPS module and no ring, while the orbit CLI stays non-FIPS because
# aws-lc-fips-sys does not build for its Windows and macOS targets.
set -euo pipefail

expect_absent() {
  local package="$1" crate="$2"
  local tree
  tree=$(cargo tree -p "$package" -e normal -i "$crate" 2>/dev/null || true)
  if [ -n "$tree" ]; then
    echo "FAIL: $crate is in the $package dependency graph:" >&2
    echo "$tree" >&2
    exit 1
  fi
}

expect_present() {
  local package="$1" crate="$2"
  if ! cargo tree -p "$package" -e normal -i "$crate" >/dev/null 2>&1; then
    echo "FAIL: $crate is missing from the $package dependency graph" >&2
    exit 1
  fi
}

expect_present orbit-server aws-lc-fips-sys
expect_absent orbit-server ring
expect_absent orbit-cli aws-lc-fips-sys

echo "OK: orbit-server resolves aws-lc-fips-sys without ring; orbit-cli stays non-FIPS"
