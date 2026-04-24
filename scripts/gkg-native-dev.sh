#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -f "$REPO_ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$REPO_ROOT/.env"
  set +a
fi

# Prerequisite: enable GDK-native `nats`, `clickhouse`, and `siphon` in
# `gdk.yml`. This script intentionally targets the existing GDK services rather
# than starting extra local infrastructure.

parse_gdk_value() {
  local expr="$1"
  local dot_expr=".${expr}"
  local value
  # Look up the key in the user's gdk.yml first; fall back to gdk.example.yml for
  # keys the user hasn't overridden. yq returns "null" (not empty string) for
  # missing keys, so we treat both as "not found".
  value="$(yq "$dot_expr" "$GDK_YML" 2>/dev/null)"
  if [[ "$value" == "null" || -z "$value" ]]; then
    value="$(yq "$dot_expr" "$GDK_DEFAULT_YML" 2>/dev/null)"
  fi
  [[ "$value" != "null" && -n "$value" ]] || return 1
  echo "$value"
}

gdk_enabled() {
  [[ "$(parse_gdk_value "$1.enabled" 2>/dev/null || true)" == "true" ]]
}

if [[ -z "${GDK_ROOT:-${GDK_DIR:-}}" ]]; then
  cat <<'EOF'
ERROR: GDK_ROOT is not set.

Set it to the path of your GDK installation:
  export GDK_ROOT=/path/to/your/gdk
  mise run dev

GDK_DIR is also accepted as an alias for GDK_ROOT.
EOF
  exit 1
fi

GDK_ROOT="${GDK_ROOT:-${GDK_DIR}}"
GDK_ROOT="${GDK_ROOT/#\~/$HOME}"
GDK_YML="$GDK_ROOT/gdk.yml"
GDK_DEFAULT_YML="$GDK_ROOT/gdk.example.yml"
GITLAB_ROOT="${GITLAB_ROOT:-$GDK_ROOT/gitlab}"

WEB_HTTP="127.0.0.1:${GKG_SERVER_PORT:-8090}"
WEB_GRPC="127.0.0.1:${GKG_SERVER_GRPC_PORT:-50054}"
IDX_HEALTH="127.0.0.1:${GKG_INDEXER_PORT:-4202}"
GKG_HEALTHCHECK_BIND_ADDRESS="${GKG_HEALTHCHECK_BIND_ADDRESS:-127.0.0.1:4201}"

GDK_CLICKHOUSE_HTTP_PORT="$(parse_gdk_value clickhouse.http_port 2>/dev/null || echo 8123)"
GDK_CLICKHOUSE_TCP_PORT="$(parse_gdk_value clickhouse.tcp_port 2>/dev/null || echo 9001)"
GDK_POSTGRES_PORT="$(parse_gdk_value postgresql.port 2>/dev/null || echo 5432)"
# gdk.example.yml contains a hardcoded absolute path for postgresql.host
# (e.g. /home/git/gdk/postgresql) which won't exist on macOS. If the resolved
# path doesn't exist, fall back to $GDK_ROOT/postgresql.
GDK_POSTGRES_HOST="$(parse_gdk_value postgresql.host 2>/dev/null || echo "$GDK_ROOT/postgresql")"
if [[ "$GDK_POSTGRES_HOST" == /* && ! -d "$GDK_POSTGRES_HOST" ]]; then
  GDK_POSTGRES_HOST="$GDK_ROOT/postgresql"
fi

# Derive the GDK external URL from gdk.yml hostname/port/https settings.
# GDK may be configured with HTTPS + nginx, in which case Rails is not
# directly reachable on a TCP port — traffic goes through nginx/workhorse.
GDK_HOSTNAME="$(parse_gdk_value hostname 2>/dev/null || echo "127.0.0.1")"
GDK_PORT="$(parse_gdk_value port 2>/dev/null || echo 3000)"
GDK_HTTPS_ENABLED="$(parse_gdk_value https.enabled 2>/dev/null || echo false)"

if [[ "$GDK_HTTPS_ENABLED" == "true" ]]; then
  GDK_GITLAB_URL="https://${GDK_HOSTNAME}:${GDK_PORT}"
else
  GDK_GITLAB_URL="http://${GDK_HOSTNAME}:${GDK_PORT}"
fi

GKG_NATS__URL="${GKG_NATS__URL:-nats://127.0.0.1:4222}"
GKG_DATALAKE__URL="${GKG_DATALAKE__URL:-http://127.0.0.1:${GDK_CLICKHOUSE_HTTP_PORT}}"
GKG_DATALAKE__DATABASE="${GKG_DATALAKE__DATABASE:-gitlab_clickhouse_development}"
GKG_DATALAKE__USERNAME="${GKG_DATALAKE__USERNAME:-default}"
GKG_GRAPH__URL="${GKG_GRAPH__URL:-http://127.0.0.1:${GDK_CLICKHOUSE_HTTP_PORT}}"
GKG_GRAPH__DATABASE="${GKG_GRAPH__DATABASE:-gkg-development}"
GKG_GRAPH__USERNAME="${GKG_GRAPH__USERNAME:-default}"
GKG_GITLAB__BASE_URL="${GKG_GITLAB__BASE_URL:-${GDK_GITLAB_URL}}"
GKG_SIPHON_STREAM_NAME="${GKG_SIPHON_STREAM_NAME:-siphon_stream}"
GKG_ENABLE_METRICS="${GKG_ENABLE_METRICS:-false}"

GITALY_TCP_ADDR="$(python3 - "$GDK_ROOT/gitaly/gitaly.config.toml" <<'PY'
import re
import sys
from pathlib import Path
path = Path(sys.argv[1])
if not path.exists():
    sys.exit(0)
text = path.read_text()
match = re.search(r'^listen_addr\s*=\s*"([^"]+)"', text, re.M)
if match:
    print(match.group(1))
PY
)"

if [[ -z "${GKG_GITLAB__JWT__VERIFYING_KEY:-}" ]]; then
  GKG_GITLAB__JWT__VERIFYING_KEY="$(cat "$GITLAB_ROOT/.gitlab_knowledge_graph_secret" 2>/dev/null || cat "$GITLAB_ROOT/.gitlab_shell_secret" 2>/dev/null || echo "development-secret-at-least-32-bytes")"
fi

if [[ -z "${GKG_GITLAB__JWT__SIGNING_KEY:-}" ]]; then
  GKG_GITLAB__JWT__SIGNING_KEY="$GKG_GITLAB__JWT__VERIFYING_KEY"
fi

if [[ -z "${GKG_DATALAKE__PASSWORD:-}" && -f "$GITLAB_ROOT/config/click_house.yml" ]]; then
  clickhouse_password="$(ruby -e 'require "yaml"; require "erb"; path=ARGV[0]; data=YAML.safe_load(ERB.new(File.read(path)).result, aliases: true) rescue {}; dev=(data["development"] || {}); puts(dev["password"] || "")' "$GITLAB_ROOT/config/click_house.yml" 2>/dev/null || true)"
  if [[ -n "$clickhouse_password" ]]; then
    GKG_DATALAKE__PASSWORD="$clickhouse_password"
    GKG_GRAPH__PASSWORD="$clickhouse_password"
  fi
fi

export GKG_NATS__URL
export GKG_DATALAKE__URL
export GKG_DATALAKE__DATABASE
export GKG_DATALAKE__USERNAME
export GKG_GRAPH__URL
export GKG_GRAPH__DATABASE
export GKG_GRAPH__USERNAME
export GKG_GITLAB__BASE_URL
export GKG_GITLAB__JWT__VERIFYING_KEY
export GKG_GITLAB__JWT__SIGNING_KEY
export GKG_SCHEDULE__TASKS__CODE_INDEXING_TASK__EVENTS_STREAM_NAME="$GKG_SIPHON_STREAM_NAME"
export GKG_SCHEDULE__TASKS__NAMESPACE_CODE_BACKFILL__EVENTS_STREAM_NAME="$GKG_SIPHON_STREAM_NAME"

if [[ -n "${GKG_DATALAKE__PASSWORD:-}" ]]; then
  export GKG_DATALAKE__PASSWORD
fi

if [[ -n "${GKG_GRAPH__PASSWORD:-${GKG_DATALAKE__PASSWORD:-}}" ]]; then
  export GKG_GRAPH__PASSWORD="${GKG_GRAPH__PASSWORD:-${GKG_DATALAKE__PASSWORD:-}}"
fi

run_checks() {
  local failures=0
  printf "Checking lightweight native-process prerequisites...\n\n"

  for tool in cargo clickhouse ruby; do
    if command -v "$tool" >/dev/null 2>&1; then
      printf "[ok] %s found: %s\n" "$tool" "$(command -v "$tool")"
    else
      printf "[fail] %s not found\n" "$tool"
      failures=$((failures + 1))
    fi
  done

  if [[ -d "$GDK_ROOT" ]]; then
    printf "[ok] GDK_ROOT exists: %s\n" "$GDK_ROOT"
  else
    printf "[fail] GDK_ROOT not found: %s\n" "$GDK_ROOT"
    failures=$((failures + 1))
  fi

  if [[ ! -f "$GDK_YML" ]]; then
    printf "\nERROR: GDK is not configured for GKG development.\n\n"
    printf "Missing configuration in %s:\n" "$GDK_YML"
    cat <<'EOF'
  nats:
    enabled: true
  clickhouse:
    enabled: true
  siphon:
    enabled: true

Add the above to your gdk.yml, then run: cd ~/gdk && gdk reconfigure
EOF
    return 1
  fi

  if ! gdk_enabled nats || ! gdk_enabled clickhouse || ! gdk_enabled siphon; then
    printf "\nERROR: GDK is not configured for GKG development.\n\n"
    printf "Missing configuration in %s:\n" "$GDK_YML"
    cat <<'EOF'
  nats:
    enabled: true
  clickhouse:
    enabled: true
  siphon:
    enabled: true

Add the above to your gdk.yml, then run: cd ~/gdk && gdk reconfigure
EOF
    return 1
  fi

  printf "[ok] gdk.yml enables nats, clickhouse, and siphon\n"

  if ! gdk_enabled duo_workflow; then
    printf "[warn] gdk.yml does not enable duo_workflow — add the following to gdk.yml:\n"
    cat <<'EOF'
  duo_workflow:
    enabled: true

Then run: cd ~/gdk && gdk reconfigure
EOF
  else
    printf "[ok] gdk.yml enables duo_workflow\n"
  fi

  if command -v gdk >/dev/null 2>&1; then
    if (cd "$GDK_ROOT" && gdk status >/dev/null 2>&1); then
      printf "[ok] gdk status succeeded\n"
    else
      printf "[fail] GDK is not running — start it with: cd %s && gdk start\n" "$GDK_ROOT"
      failures=$((failures + 1))
    fi
  elif [[ -x "$GDK_ROOT/bin/gdk" ]]; then
    if (cd "$GDK_ROOT" && "$GDK_ROOT/bin/gdk" status >/dev/null 2>&1); then
      printf "[ok] gdk status succeeded\n"
    else
      printf "[fail] GDK is not running — start it with: cd %s && bin/gdk start\n" "$GDK_ROOT"
      failures=$((failures + 1))
    fi
  else
    printf "[warn] gdk executable not found; falling back to port checks only\n"
  fi

  if [[ -f "$GITLAB_ROOT/.gitlab_knowledge_graph_secret" || -f "$GITLAB_ROOT/.gitlab_shell_secret" ]]; then
    printf "[ok] GitLab JWT secret file found\n"
  else
    printf "[warn] GitLab JWT secret file not found under %s\n" "$GITLAB_ROOT"
  fi

  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 4222 >/dev/null 2>&1 && printf "[ok] NATS reachable on localhost:4222\n" || { printf "[fail] NATS not running — enable it in gdk.yml: nats:\n  enabled: true\n"; failures=$((failures + 1)); }
    nc -z 127.0.0.1 "$GDK_CLICKHOUSE_HTTP_PORT" >/dev/null 2>&1 && printf "[ok] ClickHouse reachable on localhost:%s\n" "$GDK_CLICKHOUSE_HTTP_PORT" || { printf "[fail] ClickHouse not running — enable it in gdk.yml: clickhouse:\n  enabled: true\n"; failures=$((failures + 1)); }

    # PostgreSQL: GDK may use a Unix socket (default) or TCP depending on
    # postgresql.host in gdk.yml. Try the socket first, then fall back to TCP.
    local pg_reachable=false
    if [[ "$GDK_POSTGRES_HOST" != "localhost" && -S "${GDK_POSTGRES_HOST}/.s.PGSQL.${GDK_POSTGRES_PORT}" ]]; then
      printf "[ok] PostgreSQL reachable via Unix socket at %s (port %s)\n" "$GDK_POSTGRES_HOST" "$GDK_POSTGRES_PORT"
      pg_reachable=true
    elif nc -z 127.0.0.1 "$GDK_POSTGRES_PORT" >/dev/null 2>&1; then
      printf "[ok] PostgreSQL reachable on localhost:%s\n" "$GDK_POSTGRES_PORT"
      pg_reachable=true
    else
      printf "[fail] PostgreSQL not reachable (checked socket %s and TCP localhost:%s)\n" "$GDK_POSTGRES_HOST" "$GDK_POSTGRES_PORT"
      failures=$((failures + 1))
    fi

    if [[ "$pg_reachable" == "true" ]]; then
      local wal_level
      wal_level="$(psql -h "$GDK_POSTGRES_HOST" -p "$GDK_POSTGRES_PORT" -U "$USER" -d gitlabhq_development -tAc "SHOW wal_level" 2>/dev/null || true)"
      if [[ "$wal_level" == "logical" ]]; then
        printf "[ok] PostgreSQL wal_level is 'logical' (required for Siphon CDC)\n"
      elif [[ -n "$wal_level" ]]; then
        printf "[fail] PostgreSQL wal_level is '%s', must be 'logical' for Siphon CDC\n" "$wal_level"
        printf "  Edit %s/postgresql/data/postgresql.conf and set: wal_level = logical\n" "$GDK_ROOT"
        printf "  Then restart PostgreSQL: gdk restart postgresql\n"
        failures=$((failures + 1))
      fi
    fi

    # GitLab: GDK may serve via HTTPS + nginx, so check the derived URL
    # instead of assuming a raw TCP port.
    local gitlab_host="${GDK_HOSTNAME}"
    local gitlab_port="${GDK_PORT}"
    if nc -z "$gitlab_host" "$gitlab_port" >/dev/null 2>&1; then
      printf "[ok] GitLab reachable on %s:%s\n" "$gitlab_host" "$gitlab_port"
    else
      printf "[fail] GitLab not reachable on %s:%s — GKG requires GitLab for JWT auth\n" "$gitlab_host" "$gitlab_port"
      failures=$((failures + 1))
    fi

    if [[ -n "$GITALY_TCP_ADDR" ]]; then
      local gitaly_host="${GITALY_TCP_ADDR%:*}"
      local gitaly_port="${GITALY_TCP_ADDR##*:}"
      nc -z "$gitaly_host" "$gitaly_port" >/dev/null 2>&1 && printf "[ok] Gitaly reachable on %s\n" "$GITALY_TCP_ADDR" || printf "[warn] Gitaly not reachable on %s\n" "$GITALY_TCP_ADDR"
    else
      printf "[warn] Gitaly TCP listen_addr is not configured; code indexing may require enabling it in gitaly.config.toml\n"
    fi
  fi

  printf "\nDerived config:\n"
  print_env

  if [[ "$failures" -gt 0 ]]; then
    printf "\n%d prerequisite check(s) failed.\n" "$failures"
    return 1
  fi
}

print_env() {
  cat <<EOF
GDK_ROOT=$GDK_ROOT
ENV_FILE=${REPO_ROOT}/.env
GDK_CLICKHOUSE_HTTP_PORT=$GDK_CLICKHOUSE_HTTP_PORT
GDK_CLICKHOUSE_TCP_PORT=$GDK_CLICKHOUSE_TCP_PORT
GDK_POSTGRES_HOST=$GDK_POSTGRES_HOST
GDK_POSTGRES_PORT=$GDK_POSTGRES_PORT
GDK_GITLAB_URL=$GDK_GITLAB_URL
GITALY_TCP_ADDR=${GITALY_TCP_ADDR:-<not configured>}
WEB_HTTP=$WEB_HTTP
WEB_GRPC=$WEB_GRPC
IDX_HEALTH=$IDX_HEALTH
GKG_NATS__URL=$GKG_NATS__URL
GKG_DATALAKE__URL=$GKG_DATALAKE__URL
GKG_DATALAKE__DATABASE=$GKG_DATALAKE__DATABASE
GKG_GRAPH__URL=$GKG_GRAPH__URL
GKG_GRAPH__DATABASE=$GKG_GRAPH__DATABASE
GKG_GITLAB__BASE_URL=$GKG_GITLAB__BASE_URL
GKG_SCHEDULE__TASKS__CODE_INDEXING_TASK__EVENTS_STREAM_NAME=$GKG_SIPHON_STREAM_NAME
GKG_SCHEDULE__TASKS__NAMESPACE_CODE_BACKFILL__EVENTS_STREAM_NAME=$GKG_SIPHON_STREAM_NAME
EOF
}

apply_schema() {
  clickhouse client --host 127.0.0.1 --port "$GDK_CLICKHOUSE_TCP_PORT" --query "CREATE DATABASE IF NOT EXISTS \`$GKG_GRAPH__DATABASE\`"

  python3 - <<'PY' | while IFS= read -r stmt; do
from pathlib import Path
sql = Path("config/graph.sql").read_text()
parts = []
for line in sql.splitlines():
    stripped = line.split("--", 1)[0].strip()
    if stripped:
        parts.append(stripped)
joined = " ".join(parts)
for stmt in joined.split(";"):
    stmt = stmt.strip()
    if stmt:
        print(stmt + ";")
PY
    clickhouse client --host 127.0.0.1 --port "$GDK_CLICKHOUSE_TCP_PORT" --database "$GKG_GRAPH__DATABASE" --query "$stmt"
  done
}

run_mode() {
  local mode="$1"
  shift
  exec cargo run -p gkg-server -- --mode="$mode"
}

case "${1:-webserver}" in
  check)
    run_checks
    ;;
  env)
    print_env
    ;;
  setup)
    apply_schema
    ;;
  webserver)
    run_mode webserver
    ;;
  indexer)
    run_mode indexer
    ;;
  dispatcher)
    run_mode dispatch-indexing
    ;;
  healthcheck)
    export GKG_HEALTH_CHECK__BIND_ADDRESS="$GKG_HEALTHCHECK_BIND_ADDRESS"
    run_mode health-check
    ;;
  *)
    printf "Usage: %s {check|env|setup|webserver|indexer|dispatcher|healthcheck}\n" "$(basename "$0")"
    exit 1
    ;;
esac
