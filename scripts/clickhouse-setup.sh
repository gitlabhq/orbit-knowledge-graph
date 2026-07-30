#!/usr/bin/env bash
# shellcheck disable=SC2016  # single-quoted ${...} are deliberate: literal token patterns
# Apply the GKG ClickHouse setup contract (config/clickhouse-setup.sql) to a local or
# dev ClickHouse, filling the tokens from env vars. The production environments run
# the same file through their own provisioning (e2e initdb, Dedicated's ansible).
#
# Usage:
#   GKG_WRITER_PASSWORD=w GKG_READER_PASSWORD=r GKG_SIPHON_READER_PASSWORD=s \
#     scripts/clickhouse-setup.sh [clickhouse-client args, e.g. --host ch.local]
# Optional: GRAPH_DB (default gkg), DATALAKE_DB (default datalake), TEARDOWN=1.

set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
file="clickhouse-setup.sql"
if [[ "${TEARDOWN:-}" == "1" ]]; then
  file="clickhouse-teardown.sql"
else
  : "${GKG_WRITER_PASSWORD:?set GKG_WRITER_PASSWORD}"
  : "${GKG_READER_PASSWORD:?set GKG_READER_PASSWORD}"
  : "${GKG_SIPHON_READER_PASSWORD:?set GKG_SIPHON_READER_PASSWORD}"
fi

sql="$(<"${root_dir}/config/${file}")"
sql="${sql//'${GRAPH_DB}'/${GRAPH_DB:-gkg}}"
sql="${sql//'${DATALAKE_DB}'/${DATALAKE_DB:-datalake}}"
sql="${sql//'${GKG_WRITER_PASSWORD}'/${GKG_WRITER_PASSWORD:-}}"
sql="${sql//'${GKG_READER_PASSWORD}'/${GKG_READER_PASSWORD:-}}"
sql="${sql//'${GKG_SIPHON_READER_PASSWORD}'/${GKG_SIPHON_READER_PASSWORD:-}}"

echo "${sql}" | clickhouse-client --multiquery "$@"
echo "applied config/${file}"
