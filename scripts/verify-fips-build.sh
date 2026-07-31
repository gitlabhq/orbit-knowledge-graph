#!/bin/sh

set -eu

BINARY="${1:-target/release/gkg-server}"
TREE=$(cargo tree --locked -p gkg-server --features fips --edges normal)

echo "$TREE" | grep -q 'aws-lc-fips-sys v'

FIPS_CACHE=$(find target -path '*/build/aws-lc-fips-sys-*/out/build/CMakeCache.txt' -print -quit)
if [ -z "$FIPS_CACHE" ] || ! grep -Eq '^FIPS(:[^=]+)?=1$' "$FIPS_CACHE"; then
  echo "aws-lc-fips-sys was not built from bundled source with FIPS enabled" >&2
  exit 1
fi

if [ -n "${AWS_LC_FIPS_SYS_SYSTEM_DIR:-}" ]; then
  echo "AWS_LC_FIPS_SYS_SYSTEM_DIR must be unset for the bundled-source POC" >&2
  exit 1
fi

if ! strings "$BINARY" | grep -Eq 'aws_lc_fips_[0-9_]+|aws_lc_fips_selftest_pass|AWS_LC_FIPS'; then
  echo "gkg-server does not contain an AWS-LC FIPS marker" >&2
  exit 1
fi

if strings "$BINARY" | grep -q 'ring_core_'; then
  echo "WARNING: ring symbols remain linked into gkg-server; see the async-nats known gap" >&2
else
  echo "No ring symbol marker found in gkg-server"
fi
