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
  python3 - "$GDK_DEFAULT_YML" "$GDK_YML" "$expr" <<'PY'
import sys
from pathlib import Path

default_path = Path(sys.argv[1])
local_path = Path(sys.argv[2])
expr = sys.argv[3].split('.')

import yaml

def load(path):
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text())
    return data or {}

def lookup(data, keys):
    cur = data
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur

value = lookup(load(local_path), expr)
if value is None:
    value = lookup(load(default_path), expr)

if value is None:
    sys.exit(1)

if isinstance(value, bool):
    print(str(value).lower())
else:
    print(value)
PY
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
EOF
  exit 1
fi

GDK_ROOT="${GDK_ROOT:-${GDK_DIR}}"
GDK_ROOT="${GDK_ROOT/#\~/$HOME}"
GDK_YML="$GDK_ROOT/gdk.yml"
GDK_DEFAULT_YML="$GDK_ROOT/gdk.example.yml"
GITLAB_ROOT="${GITLAB_ROOT:-$GDK_ROOT/gitlab}"

DEV_DIR="${REPO_ROOT}/.dev/native"
LOG_DIR="${DEV_DIR}/logs"
PID_DIR="${DEV_DIR}/pids"

WEB1_HTTP="127.0.0.1:${GKG_SERVER_PORT_1:-8090}"
WEB1_GRPC="127.0.0.1:${GKG_SERVER_GRPC_PORT_1:-50054}"
WEB2_HTTP="127.0.0.1:${GKG_SERVER_PORT_2:-8091}"
WEB2_GRPC="127.0.0.1:${GKG_SERVER_GRPC_PORT_2:-50055}"
IDX1_HEALTH="127.0.0.1:${GKG_INDEXER_PORT_1:-4202}"
IDX2_HEALTH="127.0.0.1:${GKG_INDEXER_PORT_2:-4203}"
GKG_HEALTHCHECK_BIND_ADDRESS="${GKG_HEALTHCHECK_BIND_ADDRESS:-127.0.0.1:4201}"

GDK_CLICKHOUSE_HTTP_PORT="$(parse_gdk_value clickhouse.http_port 2>/dev/null || echo 8123)"
GDK_CLICKHOUSE_TCP_PORT="$(parse_gdk_value clickhouse.tcp_port 2>/dev/null || echo 9001)"
GDK_POSTGRES_PORT="$(parse_gdk_value postgresql.port 2>/dev/null || echo 5432)"
GDK_GITLAB_PORT="$(parse_gdk_value gitlab.rails.port 2>/dev/null || echo 3000)"
GKG_NATS__URL="${GKG_NATS__URL:-nats://127.0.0.1:4222}"
GKG_DATALAKE__URL="${GKG_DATALAKE__URL:-http://127.0.0.1:${GDK_CLICKHOUSE_HTTP_PORT}}"
GKG_DATALAKE__DATABASE="${GKG_DATALAKE__DATABASE:-gitlab_clickhouse_development}"
GKG_DATALAKE__USERNAME="${GKG_DATALAKE__USERNAME:-default}"
GKG_GRAPH__URL="${GKG_GRAPH__URL:-http://127.0.0.1:${GDK_CLICKHOUSE_HTTP_PORT}}"
GKG_GRAPH__DATABASE="${GKG_GRAPH__DATABASE:-gkg-development}"
GKG_GRAPH__USERNAME="${GKG_GRAPH__USERNAME:-default}"
GKG_GITLAB__BASE_URL="${GKG_GITLAB__BASE_URL:-http://127.0.0.1:${GDK_GITLAB_PORT}}"
GKG_SIPHON_STREAM_NAME="${GKG_SIPHON_STREAM_NAME:-siphon_stream_main_db}"
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
    nc -z 127.0.0.1 "$GDK_POSTGRES_PORT" >/dev/null 2>&1 && printf "[ok] PostgreSQL reachable on localhost:%s\n" "$GDK_POSTGRES_PORT" || { printf "[fail] PostgreSQL not reachable on localhost:%s — GDK and Siphon require PostgreSQL running\n" "$GDK_POSTGRES_PORT"; failures=$((failures + 1)); }
    nc -z 127.0.0.1 "$GDK_GITLAB_PORT" >/dev/null 2>&1 && printf "[ok] GitLab reachable on localhost:%s\n" "$GDK_GITLAB_PORT" || { printf "[warn] GitLab not reachable on localhost:%s\n" "$GDK_GITLAB_PORT"; failures=$((failures + 1)); }
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
GDK_POSTGRES_PORT=$GDK_POSTGRES_PORT
GDK_GITLAB_PORT=$GDK_GITLAB_PORT
GITALY_TCP_ADDR=${GITALY_TCP_ADDR:-<not configured>}
WEB1_HTTP=$WEB1_HTTP
WEB1_GRPC=$WEB1_GRPC
WEB2_HTTP=$WEB2_HTTP
WEB2_GRPC=$WEB2_GRPC
IDX1_HEALTH=$IDX1_HEALTH
IDX2_HEALTH=$IDX2_HEALTH
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

ensure_dirs() {
  mkdir -p "$LOG_DIR" "$PID_DIR"
}

is_running() {
  local pid_file="$1"
  [[ -f "$pid_file" ]] || return 1
  local pid
  pid="$(cat "$pid_file")"
  kill -0 "$pid" 2>/dev/null
}

start_process() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"
  local log_file="$LOG_DIR/$name.log"
  shift

  if is_running "$pid_file"; then
    printf "[skip] %s already running (pid %s)\n" "$name" "$(cat "$pid_file")"
    return
  fi

  rm -f "$pid_file"
  : > "$log_file"
  (
    stdbuf -oL -eL "$@" 2>&1 | sed -u "s/^/[$name] /"
  ) >> "$log_file" &
  echo $! > "$pid_file"
  printf "[ok] started %s (pid %s)\n" "$name" "$(cat "$pid_file")"
}

stop_process() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"
  if is_running "$pid_file"; then
    local pid
    pid="$(cat "$pid_file")"
    kill "$pid" 2>/dev/null || true
    rm -f "$pid_file"
    printf "[ok] stopped %s\n" "$name"
  else
    rm -f "$pid_file"
    printf "[skip] %s not running\n" "$name"
  fi
}

stream_logs() {
  ensure_dirs
  touch "$LOG_DIR"/*.log 2>/dev/null || true
  exec tail -n +1 -F "$LOG_DIR"/*.log
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
  exec env "$@" cargo run -p gkg-server -- --mode="$mode"
}

start_ha() {
  run_checks
  ensure_dirs

  start_process web-1 env \
    GKG_BIND_ADDRESS="$WEB1_HTTP" \
    GKG_GRPC_BIND_ADDRESS="$WEB1_GRPC" \
    GKG_METRICS__PROMETHEUS__ENABLED="$GKG_ENABLE_METRICS" \
    GKG_METRICS__PROMETHEUS__PORT="${GKG_METRICS_PORT_1:-9100}" \
    cargo run -p gkg-server -- --mode=webserver

  start_process web-2 env \
    GKG_BIND_ADDRESS="$WEB2_HTTP" \
    GKG_GRPC_BIND_ADDRESS="$WEB2_GRPC" \
    GKG_METRICS__PROMETHEUS__ENABLED="$GKG_ENABLE_METRICS" \
    GKG_METRICS__PROMETHEUS__PORT="${GKG_METRICS_PORT_2:-9101}" \
    cargo run -p gkg-server -- --mode=webserver

  start_process indexer-1 env \
    GKG_INDEXER_HEALTH_BIND_ADDRESS="$IDX1_HEALTH" \
    GKG_NATS__CONSUMER_NAME="${GKG_INDEXER_CONSUMER_1:-gkg-indexer-1}" \
    GKG_METRICS__PROMETHEUS__ENABLED="$GKG_ENABLE_METRICS" \
    GKG_METRICS__PROMETHEUS__PORT="${GKG_INDEXER_METRICS_PORT_1:-9200}" \
    cargo run -p gkg-server -- --mode=indexer

  start_process indexer-2 env \
    GKG_INDEXER_HEALTH_BIND_ADDRESS="$IDX2_HEALTH" \
    GKG_NATS__CONSUMER_NAME="${GKG_INDEXER_CONSUMER_2:-gkg-indexer-2}" \
    GKG_METRICS__PROMETHEUS__ENABLED="$GKG_ENABLE_METRICS" \
    GKG_METRICS__PROMETHEUS__PORT="${GKG_INDEXER_METRICS_PORT_2:-9201}" \
    cargo run -p gkg-server -- --mode=indexer

  cat <<EOF

Lightweight HA dev environment started.

Webservers:
  - http://$WEB1_HTTP
  - http://$WEB2_HTTP

Indexers:
  - health $IDX1_HEALTH
  - health $IDX2_HEALTH

Logs:
  mise run dev:logs

Stop:
  mise run dev:stop
EOF

  stream_logs
}

stop_ha() {
  stop_process web-1
  stop_process web-2
  stop_process indexer-1
  stop_process indexer-2
}

restart_ha() {
  stop_ha
  start_ha
}

case "${1:-webserver}" in
  start)
    start_ha
    ;;
  stop)
    stop_ha
    ;;
  restart)
    restart_ha
    ;;
  check)
    run_checks
    ;;
  env)
    print_env
    ;;
  logs)
    stream_logs
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
    printf "Usage: %s {start|stop|restart|check|env|logs|setup|webserver|indexer|dispatcher|healthcheck}\n" "$(basename "$0")"
    exit 1
    ;;
esac
