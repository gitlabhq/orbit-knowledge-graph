#!/usr/bin/env bash
# Proves a gkg-server binary is linked against the AWS-LC FIPS module and
# carries no other TLS crypto backend, then boots it once so the module's
# power-on self-tests run in the build environment.
set -euo pipefail

binary="${1:?usage: $0 <path-to-gkg-server>}"

symbols=$(nm --defined-only "$binary")

fips_symbols=$(grep -c ' _\?aws_lc_fips_' <<<"$symbols" || true)
if [ "$fips_symbols" -eq 0 ]; then
  echo "FAIL: $binary has no aws_lc_fips_* symbols; the AWS-LC FIPS module is not linked" >&2
  exit 1
fi

if grep -q ' _\?ring_core_' <<<"$symbols"; then
  echo "FAIL: $binary links ring; a non-FIPS TLS backend reached the server graph" >&2
  exit 1
fi

"$binary" --help >/dev/null

echo "OK: $binary links the AWS-LC FIPS module ($fips_symbols symbols) and passes FIPS self-tests"
