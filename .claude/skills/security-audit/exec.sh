#!/usr/bin/env bash
# exec.sh — Run a command with sanitized output.
#
# Pipes stdout+stderr through sed to swap local paths for placeholders.
#
# Env vars (set before calling):
#   AUDIT_REPO      — target repository path
#   AUDIT_ORBIT     — orbit binary path
#   AUDIT_OUTPUT    — scratch / output directory
#   AUDIT_GRAPH_DB  — DuckDB path (default: ~/.orbit/graph.duckdb)
#
# Usage:  ./exec.sh <command> [args...]

AUDIT_GRAPH_DB="${AUDIT_GRAPH_DB:-$HOME/.orbit/graph.duckdb}"

"$@" 2>&1 | sed \
  -e "s|${AUDIT_ORBIT:-__NOOP__}|<orbit>|g" \
  -e "s|${AUDIT_GRAPH_DB}|<graph.db>|g" \
  -e "s|${AUDIT_OUTPUT:-__NOOP__}|<output>|g" \
  -e "s|${AUDIT_REPO:-__NOOP__}|<repo>|g" \
  -e "s|${HOME}|~|g" \
  -e 's|/tmp/[^ ]*|<tmpdir>|g' \
  -e 's|/var/folders/[^ ]*|<tmpdir>|g' \
  -e 's/\([0-9a-f]\{7\}\)[0-9a-f]\{33\}/\1/g'
