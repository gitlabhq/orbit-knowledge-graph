#!/usr/bin/env bash
#
# Run the GKG dispatcher locally with the same configuration as the local helm chart.
# Uses environment variables to override config/default.yaml values.
#
# All settings can be overridden via environment variables, e.g.:
#   GKG_DATALAKE__DATABASE=my_db ./scripts/run-dispatcher.sh
#
set -euo pipefail

GDK_ROOT="${GDK_ROOT:-$HOME/gitlab/gdk}"

export RUST_LOG="${RUST_LOG:-info}"

# Datalake ClickHouse — only override database since url/username match default.yaml
export GKG_DATALAKE__DATABASE="${GKG_DATALAKE__DATABASE:-gitlab_clickhouse_development}"

# Graph ClickHouse — only override database since url/username match default.yaml
export GKG_GRAPH__DATABASE="${GKG_GRAPH__DATABASE:-gkg-development}"

# JWT verifying key (same logic as server:start in mise.toml)
export GKG_GITLAB__JWT__VERIFYING_KEY="${GKG_GITLAB__JWT__VERIFYING_KEY:-$(cat "$GDK_ROOT/gitlab/.gitlab_knowledge_graph_secret" 2>/dev/null || cat "$GDK_ROOT/gitlab/.gitlab_shell_secret" 2>/dev/null || echo "development-secret-at-least-32-bytes")}"

exec cargo run -p gkg-server -- --mode=dispatch-indexing "$@"
