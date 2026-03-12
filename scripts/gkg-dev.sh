#!/usr/bin/env bash
# ============================================================================
# gkg-dev.sh — Start, stop, and check the GKG local development environment
# ============================================================================
#
# WHAT THIS SCRIPT MANAGES
# ========================
#
# The GKG (GitLab Knowledge Graph) local dev environment has three layers:
#
# 1. Kubernetes cluster (colima or minikube)
#    A local K8s cluster that hosts the GKG services. macOS developers
#    typically use Colima; Linux developers typically use minikube.
#
# 2. GDK (GitLab Development Kit)  [default: ~/gdk]
#    A local GitLab instance that provides the upstream data services
#    GKG depends on:
#      - PostgreSQL (port 5432)  — SDLC data source
#      - NATS (port 4222)        — CDC message queue (JetStream)
#      - ClickHouse (port 8123)  — datalake + graph database
#      - Gitaly (port 8075)      — Git repository access via gRPC
#      - Rails                   — authorization decisions
#    GDK must be configured with nats, siphon, and clickhouse enabled.
#    See docs/dev/local-development.md for GDK setup instructions.
#
# 3. Tilt  [runs from: ~/knowledge-base (this repo)]
#    Orchestrates the K8s deployments that make up GKG itself:
#      - gkg-webserver   — HTTP/gRPC API (forwarded to localhost:8090)
#      - gkg-indexer     — NATS consumer, builds the graph in ClickHouse
#      - gkg-dispatcher  — CronJob that triggers indexing runs
#      - gkg-health-check — monitors all services (localhost:4201)
#      - Observability stack (Prometheus, Grafana, Loki, Alloy)
#    Tilt needs both K8s and GDK running before it starts.
#
# CONFIGURATION
# =============
#
# User-specific settings live in .gkg-dev.conf (gitignored).
# Copy .gkg-dev.conf.example to .gkg-dev.conf and edit as needed.
#
# The config file is sourced as bash, so values are KEY=value pairs.
# See .gkg-dev.conf.example for all options and their defaults.
#
# USAGE
# =====
#
#   scripts/gkg-dev.sh check    — verify prerequisites are installed
#   scripts/gkg-dev.sh start    — start all services
#   scripts/gkg-dev.sh stop     — stop all services
#   scripts/gkg-dev.sh status   — show what's running
#
# ============================================================================
set -euo pipefail

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Load user config (if present)
# ---------------------------------------------------------------------------
CONF_FILE="$REPO_ROOT/.gkg-dev.conf"
if [[ -f "$CONF_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$CONF_FILE"
fi

# ---------------------------------------------------------------------------
# Defaults — override any of these in .gkg-dev.conf
# ---------------------------------------------------------------------------

# Path to GDK (GitLab Development Kit) checkout.
GDK_ROOT="${GDK_ROOT:-$HOME/gdk}"

# Kubernetes runtime: "colima", "minikube", "docker-desktop", or "none"
# (set to "none" if you manage the cluster yourself).
# Auto-detected if not set.
K8S_RUNTIME="${K8S_RUNTIME:-auto}"

# Colima resource allocation (ignored for other runtimes).
COLIMA_CPUS="${COLIMA_CPUS:-4}"
COLIMA_MEMORY="${COLIMA_MEMORY:-12}"

# minikube resource allocation (ignored for other runtimes).
MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"
MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-12288}"

# Whether tilt up streams logs to stdout (true) or runs in the background (false).
# Either way you can access the Tilt UI at http://localhost:10350.
TILT_STREAM="${TILT_STREAM:-true}"

# ---------------------------------------------------------------------------
# Well-known service ports — change these if your GDK or Helm config differs
# ---------------------------------------------------------------------------
PORT_NATS="${PORT_NATS:-4222}"
PORT_TILT="${PORT_TILT:-10350}"
PORT_GKG_WEBSERVER="${PORT_GKG_WEBSERVER:-8090}"
PORT_HEALTH_CHECK="${PORT_HEALTH_CHECK:-4201}"
PORT_GRAFANA="${PORT_GRAFANA:-30300}"

# ---------------------------------------------------------------------------
# Colours (disabled if not a terminal)
# ---------------------------------------------------------------------------
if [[ -t 1 ]]; then
    RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
    BLUE='\033[0;34m'; BOLD='\033[1m'; RESET='\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; BLUE=''; BOLD=''; RESET=''
fi

info()  { printf "${BLUE}[info]${RESET}  %s\n" "$*"; }
ok()    { printf "${GREEN}[ok]${RESET}    %s\n" "$*"; }
warn()  { printf "${YELLOW}[warn]${RESET}  %s\n" "$*"; }
fail()  { printf "${RED}[fail]${RESET}  %s\n" "$*"; }

# ---------------------------------------------------------------------------
# GDK command — gdk is a Ruby binstub that lives inside the GDK directory.
# It requires Ruby on PATH (typically provided by mise) and must be run
# from within the GDK directory.
# ---------------------------------------------------------------------------
run_gdk() {
    if command -v gdk &>/dev/null; then
        (cd "$GDK_ROOT" && gdk "$@")
    elif [[ -x "$GDK_ROOT/bin/gdk" ]]; then
        # bin/gdk is a bash wrapper that calls gem/bin/gdk (Ruby).
        # Ruby is managed by mise — use 'mise exec' to run within the
        # GDK directory's tool context (requires 'mise trust' in GDK_ROOT).
        if command -v mise &>/dev/null; then
            (cd "$GDK_ROOT" && mise exec -- bin/gdk "$@")
        else
            (cd "$GDK_ROOT" && bin/gdk "$@")
        fi
    else
        fail "Cannot find gdk executable — check GDK_ROOT ($GDK_ROOT)"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# K8s runtime detection
# ---------------------------------------------------------------------------
detect_k8s_runtime() {
    if [[ "$K8S_RUNTIME" != "auto" ]]; then
        echo "$K8S_RUNTIME"
        return
    fi

    case "$(uname -s)" in
        Darwin)
            if command -v colima &>/dev/null; then
                echo "colima"
            elif command -v minikube &>/dev/null; then
                echo "minikube"
            else
                echo "unknown"
            fi
            ;;
        Linux)
            if command -v minikube &>/dev/null; then
                echo "minikube"
            elif command -v colima &>/dev/null; then
                echo "colima"
            else
                echo "unknown"
            fi
            ;;
        *)
            echo "unknown"
            ;;
    esac
}

# ---------------------------------------------------------------------------
# Port checks (cross-platform)
# ---------------------------------------------------------------------------
check_port() {
    local host="$1" port="$2"
    if command -v nc &>/dev/null; then
        # macOS nc uses -G for connect timeout; Linux uses -w
        if [[ "$(uname -s)" == "Darwin" ]]; then
            nc -z -G 2 "$host" "$port" 2>/dev/null
        else
            nc -z -w 2 "$host" "$port" 2>/dev/null
        fi
    elif command -v bash &>/dev/null; then
        # Fallback: bash /dev/tcp
        (echo >/dev/tcp/"$host"/"$port") 2>/dev/null
    else
        return 1
    fi
}

# ---------------------------------------------------------------------------
# k8s_running — check if the K8s cluster is reachable
# ---------------------------------------------------------------------------
k8s_running() {
    kubectl cluster-info &>/dev/null 2>&1
}

# ---------------------------------------------------------------------------
# COMMAND: check
# ---------------------------------------------------------------------------
cmd_check() {
    local errors=0

    printf "\n${BOLD}Checking prerequisites...${RESET}\n\n"

    # -- Required CLI tools --------------------------------------------------
    printf "${BOLD}CLI tools:${RESET}\n"
    local tools=("kubectl" "helm" "tilt" "mise" "cargo" "docker")
    for tool in "${tools[@]}"; do
        if command -v "$tool" &>/dev/null; then
            ok "$tool found: $(command -v "$tool")"
        else
            fail "$tool not found — install it before continuing"
            errors=$((errors + 1))
        fi
    done

    # -- K8s runtime ----------------------------------------------------------
    printf "\n${BOLD}Kubernetes runtime:${RESET}\n"
    local runtime
    runtime=$(detect_k8s_runtime)
    case "$runtime" in
        colima)   ok "Using Colima ($(colima --version 2>/dev/null || echo 'version unknown'))" ;;
        minikube) ok "Using minikube ($(minikube version --short 2>/dev/null || echo 'version unknown'))" ;;
        docker-desktop) ok "Using Docker Desktop K8s" ;;
        none)     info "K8S_RUNTIME=none — you manage the cluster yourself" ;;
        *)
            fail "No supported K8s runtime found (install colima or minikube)"
            errors=$((errors + 1))
            ;;
    esac

    # -- GDK directory -------------------------------------------------------
    printf "\n${BOLD}GDK (GitLab Development Kit):${RESET}\n"
    if [[ -d "$GDK_ROOT" ]]; then
        ok "GDK_ROOT exists: $GDK_ROOT"
        # Check that gdk is runnable (global, binstub, or via bundler)
        if command -v gdk &>/dev/null; then
            ok "gdk found on PATH: $(command -v gdk)"
        elif [[ -x "$GDK_ROOT/bin/gdk" ]]; then
            ok "gdk found: $GDK_ROOT/bin/gdk"
        elif [[ -f "$GDK_ROOT/Gemfile" ]]; then
            ok "gdk available via 'bundle exec' in $GDK_ROOT"
        else
            fail "Cannot find gdk executable in $GDK_ROOT"
            warn "Ensure GDK is installed: https://gitlab.com/gitlab-org/gitlab-development-kit"
            errors=$((errors + 1))
        fi
        # Check mise trust for GDK directory (required for mise exec to work)
        if command -v mise &>/dev/null && [[ -f "$GDK_ROOT/.mise.toml" || -f "$GDK_ROOT/mise.toml" ]]; then
            if mise config ls --no-header 2>/dev/null | grep -q "$GDK_ROOT"; then
                ok "mise trusts $GDK_ROOT"
            else
                warn "GDK mise config may not be trusted — run: cd $GDK_ROOT && mise trust"
            fi
        fi
    else
        fail "GDK_ROOT not found: $GDK_ROOT"
        warn "Set GDK_ROOT in .gkg-dev.conf if your GDK is elsewhere"
        errors=$((errors + 1))
    fi

    # Check GDK services are configured
    if [[ -f "$GDK_ROOT/gdk.yml" ]]; then
        for svc in nats siphon clickhouse; do
            if grep -q "${svc}:" "$GDK_ROOT/gdk.yml" 2>/dev/null; then
                ok "gdk.yml references $svc"
            else
                warn "gdk.yml may not have $svc configured — check docs/dev/local-development.md"
            fi
        done
    else
        warn "gdk.yml not found at $GDK_ROOT/gdk.yml — cannot verify service config"
    fi

    # Check GDK service health (if GDK is running)
    local gdk_status
    if gdk_status=$(run_gdk status 2>/dev/null); then
        gdk_status=$(echo "$gdk_status" | sed 's/\x1b\[[0-9;]*m//g')
        local down_services
        down_services=$(echo "$gdk_status" | grep -E "down \(want up\)" | sed 's/.*| *//;s/ *|$//' || true)
        if [[ -n "$down_services" ]]; then
            warn "Some GDK services are down but want to be up:"
            while IFS= read -r svc; do
                warn "  $svc"
            done <<< "$down_services"
            warn "Try: cd $GDK_ROOT && gdk restart"
        else
            local down_count
            down_count=$(echo "$gdk_status" | grep -c "| down " || true)
            local up_count
            up_count=$(echo "$gdk_status" | grep -c "| up " || true)
            if [[ $up_count -gt 0 ]]; then
                ok "GDK services: $up_count up, $down_count down"
            else
                info "GDK is not running (start it with 'gkg-dev.sh start')"
            fi
        fi
    fi

    # -- Tilt secrets ---------------------------------------------------------
    printf "\n${BOLD}Tilt configuration:${RESET}\n"
    if [[ -f "$REPO_ROOT/.tilt-secrets" ]]; then
        ok ".tilt-secrets exists"
    else
        fail ".tilt-secrets not found — copy from docs/dev/local-development.md"
        warn "Create $REPO_ROOT/.tilt-secrets with POSTGRES_PASSWORD, CLICKHOUSE_PASSWORD, GKG_JWT_SECRET"
        errors=$((errors + 1))
    fi

    # -- User config ----------------------------------------------------------
    printf "\n${BOLD}Script configuration:${RESET}\n"
    if [[ -f "$CONF_FILE" ]]; then
        ok ".gkg-dev.conf loaded"
        info "  GDK_ROOT=$GDK_ROOT"
        info "  K8S_RUNTIME=$K8S_RUNTIME (resolved: $(detect_k8s_runtime))"
    else
        warn ".gkg-dev.conf not found — using defaults (GDK_ROOT=$GDK_ROOT)"
        info "Copy .gkg-dev.conf.example to .gkg-dev.conf to customise"
    fi

    # -- PostgreSQL WAL level (if GDK is available) --------------------------
    printf "\n${BOLD}PostgreSQL (WAL level):${RESET}\n"
    local pg_conf="$GDK_ROOT/postgresql/data/postgresql.conf"
    if [[ -f "$pg_conf" ]]; then
        if grep -q "^wal_level.*=.*logical" "$pg_conf" 2>/dev/null; then
            ok "wal_level = logical in postgresql.conf"
        else
            warn "wal_level may not be set to 'logical' — Siphon CDC requires this"
            warn "Edit $pg_conf and set: wal_level = logical"
        fi
    else
        info "Cannot check postgresql.conf (GDK may not be initialised yet)"
    fi

    # -- Gitaly network listener (if GDK is available) ----------------------
    printf "\n${BOLD}Gitaly (network access):${RESET}\n"
    local gitaly_conf="$GDK_ROOT/gitaly/gitaly.config.toml"
    if [[ -f "$gitaly_conf" ]]; then
        if grep -q "listen_addr" "$gitaly_conf" 2>/dev/null; then
            ok "Gitaly has listen_addr configured"
        else
            warn "Gitaly may only listen on Unix socket — code indexing needs network access"
            warn "Add listen_addr = '0.0.0.0:8075' to $gitaly_conf"
        fi
    else
        info "Cannot check gitaly config (GDK may not be initialised yet)"
    fi

    # -- Summary --------------------------------------------------------------
    printf "\n"
    if [[ $errors -eq 0 ]]; then
        printf "${GREEN}${BOLD}All checks passed.${RESET}\n"
    else
        printf "${RED}${BOLD}$errors check(s) failed — fix the issues above before starting.${RESET}\n"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# COMMAND: start
# ---------------------------------------------------------------------------
cmd_start() {
    printf "\n${BOLD}Starting GKG dev environment...${RESET}\n\n"

    # -- 1. K8s cluster -------------------------------------------------------
    local runtime
    runtime=$(detect_k8s_runtime)
    case "$runtime" in
        colima)
            if colima status 2>/dev/null | grep -q Running; then
                ok "Colima already running"
            else
                info "Starting Colima (${COLIMA_CPUS} CPUs, ${COLIMA_MEMORY}GB RAM, K8s enabled)..."
                colima start --kubernetes --cpu "$COLIMA_CPUS" --memory "$COLIMA_MEMORY"
                ok "Colima started"
            fi
            ;;
        minikube)
            if minikube status 2>/dev/null | grep -q Running; then
                ok "minikube already running"
            else
                info "Starting minikube (${MINIKUBE_CPUS} CPUs, ${MINIKUBE_MEMORY}MB RAM)..."
                minikube start --cpus="$MINIKUBE_CPUS" --memory="$MINIKUBE_MEMORY"
                ok "minikube started"
            fi
            ;;
        docker-desktop)
            if k8s_running; then
                ok "Docker Desktop K8s is running"
            else
                fail "Docker Desktop K8s is not running — enable it in Docker Desktop settings"
                return 1
            fi
            ;;
        none)
            if k8s_running; then
                ok "K8s cluster is reachable (self-managed)"
            else
                fail "K8s cluster is not reachable — start it manually"
                return 1
            fi
            ;;
        *)
            fail "No supported K8s runtime found. Run 'check' first."
            return 1
            ;;
    esac

    # -- 2. GDK ---------------------------------------------------------------
    info "Starting GDK..."
    run_gdk start
    ok "GDK started"

    # Wait briefly for services to bind
    info "Waiting for GDK services to be ready..."
    local retries=0
    while ! check_port localhost "$PORT_NATS" && [[ $retries -lt 30 ]]; do
        sleep 1
        retries=$((retries + 1))
    done

    if check_port localhost "$PORT_NATS"; then
        ok "NATS is accepting connections (port $PORT_NATS)"
    else
        warn "NATS not responding on port $PORT_NATS after 30s — Tilt may have issues"
    fi

    # -- 3. Tilt ---------------------------------------------------------------
    start_tilt
}

# ---------------------------------------------------------------------------
# start_tilt — shared by start and restart
# ---------------------------------------------------------------------------
start_tilt() {
    if curl -sf "http://localhost:$PORT_TILT" >/dev/null 2>&1; then
        ok "Tilt is already running at http://localhost:$PORT_TILT"
        info "Run 'scripts/gkg-dev.sh restart' to restart it"
        return
    fi

    info "Starting Tilt..."
    if [[ "$TILT_STREAM" == "true" ]]; then
        info "Tilt will stream logs below (Ctrl+C to detach — services keep running)"
        info "Tilt UI: http://localhost:$PORT_TILT"
        (cd "$REPO_ROOT" && tilt up)
    else
        (cd "$REPO_ROOT" && tilt up &>/dev/null &)
        ok "Tilt started in background"
    fi

    printf "\n${BOLD}GKG dev environment is up:${RESET}\n"
    info "  Tilt UI:        http://localhost:$PORT_TILT"
    info "  GKG Webserver:  http://localhost:$PORT_GKG_WEBSERVER"
    info "  Grafana:        http://localhost:$PORT_GRAFANA (admin/admin)"
    info "  Health Check:   http://localhost:$PORT_HEALTH_CHECK"
}

# ---------------------------------------------------------------------------
# COMMAND: restart
# ---------------------------------------------------------------------------
cmd_restart() {
    printf "\n${BOLD}Restarting GKG dev environment...${RESET}\n\n"

    # -- 1. Tilt ---------------------------------------------------------------
    if curl -sf "http://localhost:$PORT_TILT" >/dev/null 2>&1 || pgrep -f "tilt up" >/dev/null 2>&1; then
        if k8s_running; then
            info "Stopping Tilt (this takes a few seconds)..."
            (cd "$REPO_ROOT" && tilt down) || warn "tilt down had errors"
        fi
        if pgrep -f "tilt up" >/dev/null 2>&1; then
            info "Stopping Tilt process..."
            pkill -f "tilt up" 2>/dev/null || true
        fi
        ok "Tilt stopped"
    else
        info "Tilt is not running — skipping"
    fi

    # -- 2. GDK (restart clears bad states where some services are stuck) ------
    info "Restarting GDK..."
    run_gdk restart
    ok "GDK restarted"

    # Wait for services to bind
    info "Waiting for GDK services to be ready..."
    local retries=0
    while ! check_port localhost "$PORT_NATS" && [[ $retries -lt 30 ]]; do
        sleep 1
        retries=$((retries + 1))
    done

    if check_port localhost "$PORT_NATS"; then
        ok "NATS is accepting connections (port $PORT_NATS)"
    else
        warn "NATS not responding on port $PORT_NATS after 30s — Tilt may have issues"
    fi

    # -- 3. Tilt ---------------------------------------------------------------
    start_tilt
}

# ---------------------------------------------------------------------------
# COMMAND: stop
# ---------------------------------------------------------------------------
cmd_stop() {
    printf "\n${BOLD}Stopping GKG dev environment...${RESET}\n\n"

    # -- 1. Tilt (stop first — needs K8s to clean up resources) ---------------
    if curl -sf "http://localhost:$PORT_TILT" >/dev/null 2>&1 || pgrep -f "tilt up" >/dev/null 2>&1; then
        if k8s_running; then
            info "Stopping Tilt (this takes a few seconds)..."
            (cd "$REPO_ROOT" && tilt down) || warn "tilt down had errors"
        fi
        # tilt down removes K8s resources but doesn't stop the tilt up process —
        # tilt up is a long-lived watcher (like a dev server). Sending SIGTERM
        # is equivalent to Ctrl+C in the terminal where it's running.
        if pgrep -f "tilt up" >/dev/null 2>&1; then
            info "Stopping Tilt process (SIGTERM)..."
            pkill -f "tilt up" 2>/dev/null || true
        fi
        ok "Tilt stopped"
    else
        info "Tilt is not running — skipping"
    fi

    # -- 2. GDK ---------------------------------------------------------------
    info "Stopping GDK..."
    run_gdk stop || warn "gdk stop had errors"
    ok "GDK stopped"

    # -- 3. K8s cluster -------------------------------------------------------
    local runtime
    runtime=$(detect_k8s_runtime)
    case "$runtime" in
        colima)
            info "Stopping Colima..."
            colima stop 2>/dev/null || true
            ok "Colima stopped"
            ;;
        minikube)
            info "Stopping minikube..."
            minikube stop 2>/dev/null || true
            ok "minikube stopped"
            ;;
        docker-desktop)
            info "Docker Desktop K8s left running (stop it from Docker Desktop settings)"
            ;;
        none)
            info "K8s cluster left running (self-managed)"
            ;;
    esac

    printf "\n${GREEN}${BOLD}GKG dev environment stopped.${RESET}\n"
}

# ---------------------------------------------------------------------------
# COMMAND: status
# ---------------------------------------------------------------------------
cmd_status() {
    printf "\n${BOLD}GKG dev environment status${RESET}\n"

    # -- K8s cluster ----------------------------------------------------------
    printf "\n${BOLD}Kubernetes cluster:${RESET}\n"
    local runtime
    runtime=$(detect_k8s_runtime)
    case "$runtime" in
        colima)
            if colima status 2>/dev/null | grep -q Running; then
                ok "Colima is running"
            else
                fail "Colima is not running"
            fi
            ;;
        minikube)
            if minikube status 2>/dev/null | grep -q Running; then
                ok "minikube is running"
            else
                fail "minikube is not running"
            fi
            ;;
        docker-desktop)
            if k8s_running; then ok "Docker Desktop K8s is running"
            else fail "Docker Desktop K8s is not running"; fi
            ;;
        none)
            if k8s_running; then ok "K8s cluster is reachable"
            else fail "K8s cluster is not reachable"; fi
            ;;
    esac

    # -- GDK services ---------------------------------------------------------
    # Use gdk status rather than port checks — some GDK services (PostgreSQL,
    # Gitaly) listen on Unix sockets, not TCP.
    printf "\n${BOLD}GDK services:${RESET}\n"
    local gdk_status
    if gdk_status=$(run_gdk status 2>/dev/null); then
        # Strip ANSI colour codes — some GDK versions output them even when piped
        gdk_status=$(echo "$gdk_status" | sed 's/\x1b\[[0-9;]*m//g')
        local up_services down_services stuck_services
        up_services=$(echo "$gdk_status" | grep "| up " | sed 's/.*| *up [^|]*| *//;s/ *|$//' || true)
        stuck_services=$(echo "$gdk_status" | grep "down (want up)" | sed 's/.*| *down[^|]*| *//;s/ *|$//' || true)
        down_services=$(echo "$gdk_status" | grep "| down " | grep -v "want up" | sed 's/.*| *down[^|]*| *//;s/ *|$//' || true)

        if [[ -n "$up_services" ]]; then
            while IFS= read -r svc; do
                ok "$svc"
            done <<< "$up_services"
        fi
        if [[ -n "$stuck_services" ]]; then
            while IFS= read -r svc; do
                fail "$svc — down (want up)"
            done <<< "$stuck_services"
        fi
        if [[ -n "$down_services" ]]; then
            while IFS= read -r svc; do
                warn "$svc — down"
            done <<< "$down_services"
        fi
    else
        fail "Cannot reach GDK — is it running? Try: cd $GDK_ROOT && gdk start"
    fi

    # -- Tilt / K8s pods -------------------------------------------------------
    printf "\n${BOLD}Tilt / GKG services:${RESET}\n"
    if pgrep -f "tilt up" >/dev/null 2>&1; then
        ok "Tilt is running (UI: http://localhost:$PORT_TILT)"
    else
        fail "Tilt is not running"
    fi

    if k8s_running; then
        local pod_status
        pod_status=$(kubectl get pods --no-headers 2>/dev/null || true)
        if [[ -n "$pod_status" ]]; then
            while IFS= read -r line; do
                local name status
                name=$(echo "$line" | awk '{print $1}')
                status=$(echo "$line" | awk '{print $3}')
                case "$status" in
                    Running)    ok "$name" ;;
                    Completed)  ok "$name (completed)" ;;
                    *)          fail "$name — $status" ;;
                esac
            done <<< "$pod_status"
        else
            warn "No pods found"
        fi
    else
        warn "K8s not reachable — cannot check pod status"
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
case "${1:-help}" in
    check)   cmd_check ;;
    start)   cmd_start ;;
    restart) cmd_restart ;;
    stop)    cmd_stop ;;
    status)  cmd_status ;;
    -h|--help|help)
        printf "Usage: %s {check|start|stop|restart|status}\n\n" "$(basename "$0")"
        printf "  check    Verify prerequisites are installed and configured\n"
        printf "  start    Start K8s cluster, GDK, and Tilt\n"
        printf "  restart  Restart GDK (clears bad states), then restart Tilt\n"
        printf "  stop     Stop Tilt, GDK, and K8s cluster\n"
        printf "  status   Show what is currently running\n"
        printf "\nConfiguration: copy .gkg-dev.conf.example to .gkg-dev.conf\n"
        ;;
    *)
        printf "${RED}Unknown command: %s${RESET}\n" "$1"
        printf "Usage: %s {check|start|stop|restart|status}\n" "$(basename "$0")"
        exit 1
        ;;
esac
