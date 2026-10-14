#!/usr/bin/env bash
# Native-process Orbit development against a Caproni cluster.
#
# Usage: scripts/dev/orbit-native-dev.sh {check|config|connect|setup|webserver|indexer|dispatcher|healthcheck}
#
# The cluster comes from a gitlab-caproni checkout (CAPRONI_DIR) with
# fragments/caproni.orbit.yaml enabled. Every cluster call goes through
# `caproni kubectl` run from that directory, so this script always talks to
# the cluster that checkout owns.
#
# `connect` holds the session state the modes need: port-forwards for
# ClickHouse and NATS, the in-cluster Orbit scaled to zero so only local code
# consumes NATS, and an EndpointSlice that routes the gkg-webserver Service to
# this host so GitLab Rails reaches the local webserver. Ctrl+C undoes all of it.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [[ -z "${ORBIT_DEV_SKIP_ENV:-}" && -f "$REPO_ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$REPO_ROOT/.env"
  set +a
fi

# CAPRONI_DIR_RESOLVED is the mise [env] fallback, so the mise tasks work without a .env file.
CAPRONI_DIR="${CAPRONI_DIR:-${CAPRONI_DIR_RESOLVED:-$HOME/gitlab-caproni}}"
CAPRONI_DIR="${CAPRONI_DIR/#\~/$HOME}"

DEV_OVERLAY="${DEV_OVERLAY_OVERRIDE:-$REPO_ROOT/config/dev.yaml}"
LOCAL_OVERLAY="${LOCAL_OVERLAY_OVERRIDE:-$REPO_ROOT/config/dev.local.yaml}"
GENERATED_DIR="${GENERATED_DIR_OVERRIDE:-$REPO_ROOT/.dev}"
STATE_FILE="$GENERATED_DIR/connect-state"

# Fixed Caproni names. The databases and users live in config/dev.yaml.
GITLAB_NS="gitlab"
STACK_NS="gitlab-dev-stack"
ORBIT_SELECTOR="app.kubernetes.io/name=gkg,app.kubernetes.io/instance=gkg"
SECRET_NAME="gitlab-dev-stack-gkg-secrets"
CLICKHOUSE_SVC="gitlab-dev-stack-clickhouse"
NATS_SVC="gitlab-dev-stack-nats"
CLICKHOUSE_LOCAL_PORT=8123
NATS_LOCAL_PORT=4222
WEBSERVER_SVC="gkg-webserver"
WEBSERVER_GRPC_PORT=50054
ENDPOINTSLICE_NAME="gkg-webserver-orbit-native-dev"

# Filled by load_cluster_values; tests set them directly.
CLICKHOUSE_URL="http://127.0.0.1:${CLICKHOUSE_LOCAL_PORT}"
CLICKHOUSE_PASSWORD=""
GITLAB_BASE_URL=""
JWT_VERIFYING_KEY=""
JWT_SIGNING_KEY=""

cap() { mise -C "$CAPRONI_DIR" exec -- caproni "$@"; }
kc() { cap kubectl "$@"; }
# A tool from the gitlab-caproni toolchain (colima, docker-cli) with that checkout's versions.
captool() { mise -C "$CAPRONI_DIR" exec -- "$@"; }

secret_value() {
  kc -n "$GITLAB_NS" get secret "$SECRET_NAME" -o "jsonpath={.data.$1}" | base64 -d
}

cluster_hostname() {
  cap config print 2>/dev/null | yq '.cluster.network.hostname'
}

cluster_driver() {
  local driver
  driver="$(cap config print 2>/dev/null | yq '.cluster.driver')"
  if [[ "$driver" == "auto" || -z "$driver" || "$driver" == "null" ]]; then
    if [[ "$(uname -s)" == "Darwin" ]]; then driver=colima; else driver=k3d; fi
  fi
  echo "$driver"
}

load_cluster_values() {
  GITLAB_BASE_URL="http://$(cluster_hostname)"
  CLICKHOUSE_PASSWORD="$(secret_value graph-password)"
  JWT_VERIFYING_KEY="$(secret_value gitlab-jwt-verifying-key)"
  JWT_SIGNING_KEY="$(secret_value gitlab-jwt-signing-key)"
}

yaml_str() {
  local escaped
  escaped="$(printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g')"
  printf '"%s"' "$escaped"
}

cluster_overlay_yaml() {
  echo "datalake:"
  echo "  url: $(yaml_str "$CLICKHOUSE_URL")"
  echo "  password: $(yaml_str "$CLICKHOUSE_PASSWORD")"
  echo "graph:"
  echo "  url: $(yaml_str "$CLICKHOUSE_URL")"
  echo "  password: $(yaml_str "$CLICKHOUSE_PASSWORD")"
  echo "gitlab:"
  echo "  base_url: $(yaml_str "$GITLAB_BASE_URL")"
  echo "  jwt:"
  echo "    verifying_key: $(yaml_str "$JWT_VERIFYING_KEY")"
  echo "    signing_key: $(yaml_str "$JWT_SIGNING_KEY")"
}

# The only value that differs between the processes `mise run dev` starts side by side. Every mode
# binds this address for the labkit probe server, so each one needs its own even with metrics off.
mode_overlay_yaml() {
  case "$1" in
    webserver) printf 'probe_server:\n  bind_address: "127.0.0.1:9100"\n' ;;
    indexer) printf 'probe_server:\n  bind_address: "127.0.0.1:9200"\n' ;;
    dispatch-indexing) printf 'probe_server:\n  bind_address: "127.0.0.1:9300"\n' ;;
    health-check) printf 'probe_server:\n  bind_address: "127.0.0.1:9400"\n' ;;
    *) ;;
  esac
}

# Writes .dev/<mode>.yaml and prints its path.
write_mode_overlay() {
  local mode="$1"
  local target="$GENERATED_DIR/$mode.yaml"
  local cluster_part mode_part tmp
  mkdir -p "$GENERATED_DIR"
  cluster_part="$(mktemp "$GENERATED_DIR/cluster.XXXXXX")"
  mode_part="$(mktemp "$GENERATED_DIR/mode.XXXXXX")"
  tmp="$(mktemp "$target.XXXXXX")"
  cluster_overlay_yaml >"$cluster_part"
  mode_overlay_yaml "$mode" >"$mode_part"
  local parts=("$DEV_OVERLAY" "$cluster_part")
  if [[ -s "$mode_part" ]]; then
    parts+=("$mode_part")
  fi
  if [[ -f "$LOCAL_OVERLAY" ]]; then
    parts+=("$LOCAL_OVERLAY")
  fi
  # yq's status is checked explicitly: callers run this inside $(...), where errexit does not
  # apply on bash 3.2, and a header-only overlay would send the server off on embedded defaults.
  echo "# Generated by scripts/dev/orbit-native-dev.sh; regenerated on every start. Edit config/dev.local.yaml instead." >"$tmp"
  if ! yq eval-all '(. as $item ireduce ({}; . * $item)) | ... comments=""' "${parts[@]}" >>"$tmp"; then
    rm -f "$cluster_part" "$mode_part" "$tmp"
    printf 'ERROR: could not merge %s; see the yq error above\n' "${parts[*]}" >&2
    return 1
  fi
  rm -f "$cluster_part" "$mode_part"
  chmod 600 "$tmp"
  mv "$tmp" "$target"
  echo "$target"
}

port_open() { nc -z 127.0.0.1 "$1" >/dev/null 2>&1; }

wait_for_port() { # port, what, seconds, [hint]
  local port="$1" what="$2" seconds="${3:-60}" i
  local hint="${4:-Is \`mise run dev:connect\` running in another terminal?}"
  for ((i = 0; i < seconds; i++)); do
    port_open "$port" && return 0
    if ((i == 0)); then
      printf 'Waiting for %s on 127.0.0.1:%s. %s\n' "$what" "$port" "$hint" >&2
    fi
    sleep 1
  done
  printf 'ERROR: %s never appeared on 127.0.0.1:%s\n' "$what" "$port" >&2
  return 1
}

# The Caproni cluster (colima profile or k3d cluster) name. Caproni's kube context is
# `colima-<name>` or `k3d-<name>`, which also covers the directory-hash naming strategy.
# CAPRONI_CLUSTER_NAME overrides.
cluster_name() {
  if [[ -n "${CAPRONI_CLUSTER_NAME:-}" ]]; then echo "$CAPRONI_CLUSTER_NAME"; return; fi
  kc config current-context 2>/dev/null | sed -e 's/^colima-//' -e 's/^k3d-//'
}

# The host's IP as the cluster sees it, for the EndpointSlice. ORBIT_DEV_HOST_IP overrides.
host_ip() {
  if [[ -n "${ORBIT_DEV_HOST_IP:-}" ]]; then echo "$ORBIT_DEV_HOST_IP"; return; fi
  local cluster ip=""
  cluster="$(cluster_name)"
  case "$(cluster_driver)" in
    colima)
      # colima writes host.docker.internal into the VM's /etc/hosts; the default route is the
      # lima gateway, which also reaches the host.
      ip="$(captool colima ssh -p "$cluster" -- sh -c "awk '/host.docker.internal/ {print \$1; exit}' /etc/hosts; ip route | awk '/^default/ {print \$3; exit}'" 2>/dev/null | head -1)"
      ;;
    k3d)
      ip="$(captool docker exec "k3d-${cluster}-server-0" sh -c "getent hosts host.k3d.internal | awk '{print \$1}'" 2>/dev/null)"
      ;;
  esac
  [[ -n "$ip" ]] || { echo "ERROR: could not determine the host IP as seen from the cluster; set ORBIT_DEV_HOST_IP" >&2; return 1; }
  echo "$ip"
}

endpointslice_yaml() { # host ip
  cat <<EOF
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: $ENDPOINTSLICE_NAME
  namespace: $GITLAB_NS
  labels:
    kubernetes.io/service-name: $WEBSERVER_SVC
    endpointslice.kubernetes.io/managed-by: orbit-native-dev
addressType: IPv4
ports:
  - name: grpc
    protocol: TCP
    port: $WEBSERVER_GRPC_PORT
endpoints:
  - addresses: ["$1"]
    conditions:
      ready: true
EOF
}

# State file lines are `<deployment>=<replicas>`. A zero is never restored: it means a previous
# session was killed before it could restore, and restoring zero would park Orbit for good.
restore_from_state_file() {
  local file="$1" name replicas
  [[ -f "$file" ]] || return 0
  while IFS='=' read -r name replicas; do
    [[ -n "$name" ]] || continue
    if [[ "$replicas" == "0" || -z "$replicas" ]]; then
      printf '[warn] %s was already at 0 replicas when this session started; restoring to 1. Check `kubectl -n %s get deploy`.\n' "$name" "$GITLAB_NS" >&2
      replicas=1
    fi
    kc -n "$GITLAB_NS" scale "deploy/$name" --replicas="$replicas" >/dev/null
  done <"$file"
  rm -f "$file"
}

run_connect() {
  mkdir -p "$GENERATED_DIR"
  local pf_pids=()
  cleanup() {
    printf '\nRestoring the cluster...\n'
    local pid
    for pid in "${pf_pids[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true; done
    kc -n "$GITLAB_NS" delete endpointslice "$ENDPOINTSLICE_NAME" --ignore-not-found >/dev/null
    restore_from_state_file "$STATE_FILE"
    printf 'Done.\n'
  }
  trap cleanup EXIT INT TERM

  if [[ -f "$STATE_FILE" ]]; then
    printf '[warn] a previous session left %s behind; restoring it first\n' "$STATE_FILE"
    restore_from_state_file "$STATE_FILE"
  fi

  printf 'Recording Orbit replica counts...\n'
  kc -n "$GITLAB_NS" get deploy -l "$ORBIT_SELECTOR" \
    -o jsonpath='{range .items[*]}{.metadata.name}={.spec.replicas}{"\n"}{end}' >"$STATE_FILE"
  cat "$STATE_FILE"

  printf 'Scaling the in-cluster Orbit to 0 so only local code consumes NATS...\n'
  kc -n "$GITLAB_NS" scale deploy -l "$ORBIT_SELECTOR" --replicas=0 >/dev/null

  local ip
  ip="$(host_ip)"
  printf 'Routing Service %s to this host (%s:%s)...\n' "$WEBSERVER_SVC" "$ip" "$WEBSERVER_GRPC_PORT"
  endpointslice_yaml "$ip" | kc apply -f - >/dev/null

  printf 'Port-forwarding ClickHouse (%s) and NATS (%s)...\n' "$CLICKHOUSE_LOCAL_PORT" "$NATS_LOCAL_PORT"
  kc -n "$STACK_NS" port-forward "svc/$CLICKHOUSE_SVC" "$CLICKHOUSE_LOCAL_PORT:8123" >"$GENERATED_DIR/pf-clickhouse.log" 2>&1 &
  pf_pids+=("$!")
  kc -n "$STACK_NS" port-forward "svc/$NATS_SVC" "$NATS_LOCAL_PORT:4222" >"$GENERATED_DIR/pf-nats.log" 2>&1 &
  pf_pids+=("$!")
  wait_for_port "$CLICKHOUSE_LOCAL_PORT" ClickHouse 30
  wait_for_port "$NATS_LOCAL_PORT" NATS 30

  printf 'Connected. Ctrl+C restores the cluster.\n'
  wait
}

# Runs a command with a ClickHouse port-forward if none is open (dev:setup without dev:connect).
# The forward is torn down on every path, including failure: a RETURN trap would not run when
# errexit tears the shell down, and an orphaned forward would make the next dev:connect believe
# the port is its own.
with_clickhouse_forward() {
  if port_open "$CLICKHOUSE_LOCAL_PORT"; then
    "$@"
    return
  fi
  mkdir -p "$GENERATED_DIR"
  kc -n "$STACK_NS" port-forward "svc/$CLICKHOUSE_SVC" "$CLICKHOUSE_LOCAL_PORT:8123" >"$GENERATED_DIR/pf-clickhouse-setup.log" 2>&1 &
  local pid=$! rc=0
  wait_for_port "$CLICKHOUSE_LOCAL_PORT" ClickHouse "${FORWARD_WAIT_SECONDS:-30}" \
    "Opening a temporary port-forward; see $GENERATED_DIR/pf-clickhouse-setup.log if this stalls." || rc=$?
  if [[ "$rc" -eq 0 ]]; then
    "$@" || rc=$?
  fi
  pkill -P "$pid" 2>/dev/null || true
  kill "$pid" 2>/dev/null || true
  return "$rc"
}

# Reads the database from the same merged overlay the server loads, so a graph.database override
# in config/dev.local.yaml creates the schema where the processes will look for it.
apply_schema() {
  load_cluster_values
  local overlay graph_database
  overlay="$(write_mode_overlay setup)"
  graph_database="$(yq '.graph.database' "$overlay")"
  # Exported rather than passed through `env`, whose argv (password included) shows in `ps`.
  export CLICKHOUSE_URL CLICKHOUSE_PASSWORD
  export CLICKHOUSE_DATABASE="$graph_database"
  CLICKHOUSE_USER="$(yq '.graph.username' "$overlay")"
  export CLICKHOUSE_USER
  with_clickhouse_forward "$SCRIPT_DIR/apply-graph-schema.sh"
}

print_config() {
  cat <<EOF
CAPRONI_DIR=$CAPRONI_DIR
DEV_OVERLAY=$DEV_OVERLAY
GENERATED_OVERLAYS=$GENERATED_DIR/<mode>.yaml
LOCAL_OVERLAY=$LOCAL_OVERLAY$([[ -f "$LOCAL_OVERLAY" ]] || echo " (absent)")

Derived from the Caproni cluster:
datalake.url=$CLICKHOUSE_URL
graph.url=$CLICKHOUSE_URL
gitlab.base_url=$GITLAB_BASE_URL
jwt keys and ClickHouse password: secret $GITLAB_NS/$SECRET_NAME
EOF
}

run_checks() {
  local failures=0
  printf "Checking Caproni-connected native-process prerequisites...\n\n"

  local tool
  for tool in mise cargo yq nc curl python3; do
    if command -v "$tool" >/dev/null 2>&1; then
      printf "[ok] %s found: %s\n" "$tool" "$(command -v "$tool")"
    else
      printf "[fail] %s not found\n" "$tool"
      failures=$((failures + 1))
    fi
  done

  if [[ -f "$CAPRONI_DIR/caproni.yaml" ]]; then
    printf "[ok] CAPRONI_DIR is a gitlab-caproni checkout: %s\n" "$CAPRONI_DIR"
  else
    printf "[fail] CAPRONI_DIR has no caproni.yaml: %s\n  Set CAPRONI_DIR in .env to your gitlab-caproni checkout.\n" "$CAPRONI_DIR"
    return 1
  fi

  if cap status >/dev/null 2>&1; then
    printf "[ok] caproni cluster is running\n"
  else
    printf "[fail] caproni cluster is not running. From %s run: caproni up\n" "$CAPRONI_DIR"
    return 1
  fi

  local deploys
  deploys="$(kc -n "$GITLAB_NS" get deploy -l "$ORBIT_SELECTOR" -o name 2>/dev/null | wc -l | tr -d ' ')"
  if [[ "$deploys" -gt 0 ]]; then
    printf "[ok] Orbit fragment is deployed (%s deployments)\n" "$deploys"
  else
    printf "[fail] no Orbit deployments in namespace %s.\n  Enable fragments/caproni.orbit.yaml in caproni.local.yaml and run scripts/setup-orbit.sh in %s\n" "$GITLAB_NS" "$CAPRONI_DIR"
    return 1
  fi

  if kc -n "$GITLAB_NS" get secret "$SECRET_NAME" >/dev/null 2>&1; then
    printf "[ok] secret %s present\n" "$SECRET_NAME"
  else
    printf "[fail] secret %s missing in namespace %s\n" "$SECRET_NAME" "$GITLAB_NS"
    failures=$((failures + 1))
  fi

  load_cluster_values
  if curl -fsS -o /dev/null --max-time 10 "$GITLAB_BASE_URL/-/liveness"; then
    printf "[ok] GitLab reachable at %s\n" "$GITLAB_BASE_URL"
  else
    printf "[fail] GitLab not reachable at %s. Run 'caproni update-etc-hosts' in %s\n" "$GITLAB_BASE_URL" "$CAPRONI_DIR"
    failures=$((failures + 1))
  fi

  if [[ -f "$STATE_FILE" ]]; then
    printf "[warn] %s exists: a previous dev:connect did not restore the cluster. The next dev:connect repairs it.\n" "$STATE_FILE"
  fi

  printf "\nDerived config:\n"
  print_config

  if [[ "$failures" -gt 0 ]]; then
    printf "\n%d prerequisite check(s) failed.\n" "$failures"
    return 1
  fi
}

run_mode() {
  local mode="$1"
  wait_for_port "$CLICKHOUSE_LOCAL_PORT" ClickHouse 120
  wait_for_port "$NATS_LOCAL_PORT" NATS 120
  load_cluster_values
  local overlay
  overlay="$(write_mode_overlay "$mode")"
  exec cargo run -p orbit-server -- --mode="$mode" --config "$overlay"
}

main() {
  case "${1:-webserver}" in
    check) run_checks ;;
    config) load_cluster_values; print_config ;;
    connect) run_connect ;;
    setup) apply_schema ;;
    webserver) run_mode webserver ;;
    indexer) run_mode indexer ;;
    dispatcher) run_mode dispatch-indexing ;;
    healthcheck) run_mode health-check ;;
    *)
      printf "Usage: %s {check|config|connect|setup|webserver|indexer|dispatcher|healthcheck}\n" "$(basename "$0")"
      exit 1
      ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
