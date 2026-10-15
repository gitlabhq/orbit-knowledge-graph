#!/usr/bin/env bash
# Apply the Orbit graph schema (config/graph.sql) to a ClickHouse database over HTTP.
#
# Usage:
#   scripts/dev/apply-graph-schema.sh [OPTIONS]
#
# Options:
#   --url URL         ClickHouse HTTP URL (default: http://127.0.0.1:8123)
#   --database DB     Target database (default: gkg)
#   --user USER       ClickHouse user (default: gitlab-dev-stack)
#   --schema FILE     Path to SQL file (default: config/graph.sql)
#   --dry-run         Print statements without executing
#   -h, --help        Show this help
#
# Environment variables (override defaults):
#   CLICKHOUSE_URL, CLICKHOUSE_DATABASE, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
#
# The script applies each SQL statement individually because ClickHouse
# does not support multi-statement DDL in one request. HTTP is used because
# `mise run dev:connect` forwards only the HTTP port from the Caproni cluster.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

URL="${CLICKHOUSE_URL:-http://127.0.0.1:8123}"
DATABASE="${CLICKHOUSE_DATABASE:-gkg}"
USER_NAME="${CLICKHOUSE_USER:-gitlab-dev-stack}"
PASSWORD="${CLICKHOUSE_PASSWORD:-}"
SCHEMA="${REPO_ROOT}/config/graph.sql"
DRY_RUN=false

usage() {
    sed -n '2,/^$/s/^# \{0,1\}//p' "$0"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --url)       URL="$2";       shift 2 ;;
        --database)  DATABASE="$2";  shift 2 ;;
        --user)      USER_NAME="$2"; shift 2 ;;
        --schema)    SCHEMA="$2";    shift 2 ;;
        --dry-run)   DRY_RUN=true;   shift ;;
        -h|--help)   usage ;;
        *) echo "Unknown option: $1" >&2; usage ;;
    esac
done

[[ -f "$SCHEMA" ]] || { echo "Schema file not found: $SCHEMA" >&2; exit 1; }

# ClickHouse reads the password from the X-ClickHouse-Key header, so it never
# appears in a URL or in `ps` output.
ch_query() {
    local database="$1" stmt="$2"
    curl -sS --fail-with-body "$URL/?database=$database" \
        -H "X-ClickHouse-User: $USER_NAME" \
        -H "X-ClickHouse-Key: $PASSWORD" \
        --data-binary "$stmt"
}

statements() {
    python3 - "$SCHEMA" <<'PY'
import sys
from pathlib import Path
sql = Path(sys.argv[1]).read_text()
parts = []
for line in sql.splitlines():
    stripped = line.split("--", 1)[0].strip()
    if stripped:
        parts.append(stripped)
for stmt in " ".join(parts).split(";"):
    stmt = stmt.strip()
    if stmt:
        print(stmt + ";")
PY
}

if [[ "$DRY_RUN" == true ]]; then
    echo "CREATE DATABASE IF NOT EXISTS \`$DATABASE\`;"
    statements
    exit 0
fi

ch_query default "CREATE DATABASE IF NOT EXISTS \`$DATABASE\`"
count=0
while IFS= read -r stmt; do
    ch_query "$DATABASE" "$stmt"
    count=$((count + 1))
done < <(statements)
echo "Applied $count statements from $SCHEMA to $URL database $DATABASE"
