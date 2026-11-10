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

if grep -q ' _\?aws_lc_[0-9]' <<<"$symbols"; then
  echo "FAIL: $binary links the non-FIPS AWS-LC library next to the FIPS module" >&2
  exit 1
fi

# DuckDB compiles mbedtls in for hashing and its encryption fallback. Its TLS, X.509 and network
# modules must never link, so all TLS stays in the FIPS module.
if grep -qE 'mbedtls_(ssl|x509|net)_' <<<"$symbols"; then
  echo "FAIL: $binary links mbedtls TLS, X.509 or network code; only the AWS-LC FIPS module may do TLS" >&2
  exit 1
fi
mbedtls_symbols=$(grep -c 'mbedtls_' <<<"$symbols" || true)

"$binary" --help >/dev/null

echo "OK: $binary links the AWS-LC FIPS module ($fips_symbols symbols) and passes FIPS self-tests; DuckDB's mbedtls has $mbedtls_symbols crypto symbols and no TLS"
