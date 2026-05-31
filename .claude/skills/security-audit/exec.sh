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

"$@" 2>&1 | perl -pe '
  BEGIN { $| = 1 }
  s|\Q'"${AUDIT_ORBIT:-__NOOP__}"'\E|<orbit>|g;
  s|\Q'"${AUDIT_GRAPH_DB}"'\E|<graph.db>|g;
  s|\Q'"${AUDIT_OUTPUT:-__NOOP__}"'\E|<output>|g;
  s|\Q'"${AUDIT_REPO:-__NOOP__}"'\E|<repo>|g;
  s|\Q'"${HOME}"'\E|~|g;
  s|/tmp/[^ ]*|<tmpdir>|g;
  s|/var/folders/[^ ]*|<tmpdir>|g;
  s/([0-9a-f]{7})[0-9a-f]{33}/$1/g;
'
