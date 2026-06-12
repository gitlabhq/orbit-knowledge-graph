#!/usr/bin/env bash
# Apply the GKG graph schema (config/graph.sql) to a ClickHouse database.
#
# Usage:
#   scripts/apply-graph-schema.sh [OPTIONS]
#
# Options:
#   --host HOST       ClickHouse host (default: localhost)
#   --port PORT       ClickHouse native TCP port (default: 9001)
#   --database DB     Target database (default: gkg-development)
#   --schema FILE     Path to SQL file (default: config/graph.sql)
#   --dry-run         Print statements without executing
#   -h, --help        Show this help
#
# Environment variables (override defaults):
#   CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE
#
# The script applies each SQL statement individually because ClickHouse
# does not support multi-statement DDL execution over the native protocol.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults (overridable by env vars, then by flags)
HOST="${CLICKHOUSE_HOST:-localhost}"
PORT="${CLICKHOUSE_PORT:-9001}"
DATABASE="${CLICKHOUSE_DATABASE:-gkg-development}"
SCHEMA="${REPO_ROOT}/config/graph.sql"
DRY_RUN=false

usage() {
    sed -n '2,/^$/s/^# \{0,1\}//p' "$0"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)      HOST="$2";     shift 2 ;;
        --port)      PORT="$2";     shift 2 ;;
        --database)  DATABASE="$2"; shift 2 ;;
        --schema)    SCHEMA="$2";   shift 2 ;;
        --dry-run)   DRY_RUN=true;  shift ;;
        -h|--help)   usage ;;
        *)           echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -f "$SCHEMA" ]]; then
    echo "Error: schema file not found: $SCHEMA" >&2
    exit 1
fi

# Strip SQL single-line comments (-- ...) while preserving lines that
# contain '--' inside string literals. This simple approach handles the
# common case; statements with '--' inside strings are rare in DDL.
# Then split on semicolons and execute each statement.
#
# We read the entire file, collapse newlines, split on ';', and trim
# whitespace to extract individual statements.

statements=()
current=""

while IFS= read -r line; do
    # Remove single-line comments (lines starting with --, or trailing --)
    cleaned="${line%%--*}"
    current="${current} ${cleaned}"
done < "$SCHEMA"

# Split accumulated text on semicolons
IFS=';' read -ra parts <<< "$current"

for part in "${parts[@]}"; do
    # Trim leading/trailing whitespace
    stmt="$(echo "$part" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    if [[ -n "$stmt" ]]; then
        statements+=("$stmt")
    fi
done

echo "Applying ${#statements[@]} statements to ${HOST}:${PORT}/${DATABASE}"

applied=0
failed=0

for stmt in "${statements[@]}"; do
    if "$DRY_RUN"; then
        echo "[DRY RUN] $stmt;"
        applied=$((applied + 1))
        continue
    fi

    if clickhouse client --host "$HOST" --port "$PORT" \
        --database "$DATABASE" --query "$stmt"; then
        applied=$((applied + 1))
    else
        echo "Error: statement failed: ${stmt:0:80}..." >&2
        failed=$((failed + 1))
    fi
done

echo "Done: ${applied} applied, ${failed} failed"

if [[ "$failed" -gt 0 ]]; then
    exit 1
fi

