#!/usr/bin/env bash
# =============================================================================
# bootstrap-dev.sh — Start the full local GKG development environment
#
# Starts (in order, skipping anything already running):
#   1. Colima (with Kubernetes)
#   2. GDK services (postgresql, redis, clickhouse, nats, siphon, rails, etc.)
#   3. Tilt (builds + deploys GKG to k8s, runs in background)
#   4. kubectl port-forwards for gkg-webserver (HTTP :4200, gRPC :50051)
#
# Usage:
#   cd ~/Desktop/Code/gkg
#   ./scripts/bootstrap-dev.sh
#
# Options:
#   --no-tilt   Skip Tilt / port-forwards (useful when iterating on Rails only)
#   --no-gdk    Skip GDK start (if already running)
#
# Requirements:
#   - colima, kubectl, mise installed (brew install colima helm; mise install)
#   - ~/Desktop/Code/gdk exists and is configured
#   - .tilt-secrets exists in this repo (cp .tilt-secrets.example .tilt-secrets)
# =============================================================================

set -euo pipefail

GKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GDK_DIR=~/Desktop/Code/gdk
DEV_DIR="$GKG_DIR/.dev"
mkdir -p "$DEV_DIR"

TILT_LOG="$DEV_DIR/tilt.log"
PF_LOG="$DEV_DIR/port-forwards.log"

START_TILT=true
START_GDK=true

for arg in "$@"; do
  case $arg in
    --no-tilt) START_TILT=false ;;
    --no-gdk)  START_GDK=false  ;;
  esac
done

# ── Helpers ──────────────────────────────────────────────────────────────────

log()  { echo "  [bootstrap] $*"; }
ok()   { echo "  [bootstrap] ✓ $*"; }
info() { echo "  [bootstrap] → $*"; }
fail() { echo "  [bootstrap] ✗ $*" >&2; exit 1; }

wait_for() {
  local label="$1" cmd="$2" attempts="${3:-30}" delay="${4:-5}"
  info "Waiting for $label..."
  for i in $(seq 1 "$attempts"); do
    if eval "$cmd" &>/dev/null; then
      ok "$label is ready"
      return 0
    fi
    sleep "$delay"
  done
  fail "$label did not become ready after $((attempts * delay))s"
}

kill_stale_gkg_server() {
  local pids
  pids=$(pgrep -f "target/debug/gkg-server" 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    log "Killing stale local gkg-server process(es): $pids"
    kill -9 $pids 2>/dev/null || true
  fi
}

# ── Preflight checks ─────────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  GKG Dev Environment Bootstrap"
echo "══════════════════════════════════════════════════════════"
echo ""

[[ -d "$GDK_DIR" ]] || fail "GDK not found at $GDK_DIR"

if $START_TILT; then
  [[ -f "$GKG_DIR/.tilt-secrets" ]] || \
    fail ".tilt-secrets not found. Run: cp $GKG_DIR/.tilt-secrets.example $GKG_DIR/.tilt-secrets"
fi

# ── Step 1: Colima ───────────────────────────────────────────────────────────

log "Step 1: Colima"
if colima status 2>/dev/null | grep -q "Running"; then
  ok "Colima already running"
else
  info "Starting Colima with Kubernetes..."
  colima start --kubernetes
  wait_for "Kubernetes API" "kubectl cluster-info" 24 5
fi

# ── Step 2: GDK ──────────────────────────────────────────────────────────────

log "Step 2: GDK services"
if $START_GDK; then
  # Check if core services are already up
  if gdk status 2>/dev/null | grep -q "up "; then
    ok "GDK services appear to be running already"
  else
    info "Starting GDK..."
    cd "$GDK_DIR"
    gdk start
    wait_for "PostgreSQL" \
      "psql -h $GDK_DIR/postgresql -p 5432 -d gitlabhq_development -c 'SELECT 1' -q" \
      12 5
    wait_for "ClickHouse" \
      "/opt/homebrew/bin/clickhouse client --port 9001 -u default --query 'SELECT 1'" \
      12 5
  fi
else
  ok "Skipping GDK start (--no-gdk)"
fi

# ── Step 3: Kill any stale local gkg-server ───────────────────────────────────

log "Step 3: Check for stale local gkg-server"
kill_stale_gkg_server
ok "No stale gkg-server processes"

# ── Step 4: Tilt ─────────────────────────────────────────────────────────────

log "Step 4: Tilt"
if $START_TILT; then
  cd "$GKG_DIR"

  if kubectl get deployment gkg-webserver -n default &>/dev/null; then
    # Already deployed — check if pods are ready
    ready=$(kubectl get deployment gkg-webserver -n default \
      -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    if [[ "$ready" -ge 1 ]]; then
      ok "gkg-webserver already deployed and ready ($ready replicas)"
    else
      info "gkg-webserver deployment exists but pods not ready — Tilt may already be managing it"
    fi
  fi

  # Check if Tilt is already running (port 10350)
  if curl -s http://localhost:10350 &>/dev/null; then
    ok "Tilt UI already running at http://localhost:10350"
  else
    info "Starting Tilt in background (log: $TILT_LOG)..."
    nohup mise exec -- tilt up --stream >"$TILT_LOG" 2>&1 &
    TILT_PID=$!
    echo "$TILT_PID" > "$DEV_DIR/tilt.pid"
    ok "Tilt started (pid=$TILT_PID)"
    info "Waiting for gkg-webserver deployment to become ready..."
    wait_for "gkg-webserver pod" \
      "kubectl get deployment gkg-webserver -n default -o jsonpath='{.status.readyReplicas}' 2>/dev/null | grep -q '^[1-9]'" \
      36 10
  fi

  # ── Step 5: Port-forwards ─────────────────────────────────────────────────

  log "Step 5: Port-forwards"

  # Kill any existing port-forwards to gkg-webserver
  pkill -f "port-forward svc/gkg-webserver" 2>/dev/null || true
  sleep 1

  info "Starting port-forwards (log: $PF_LOG)..."
  kubectl port-forward svc/gkg-webserver 4200:8080  -n default >>"$PF_LOG" 2>&1 &
  PF1_PID=$!
  kubectl port-forward svc/gkg-webserver 50051:50051 -n default >>"$PF_LOG" 2>&1 &
  PF2_PID=$!

  echo "$PF1_PID $PF2_PID" > "$DEV_DIR/port-forwards.pid"

  wait_for "GKG HTTP health endpoint" \
    "curl -sf http://localhost:4200/health | grep -q status" \
    12 3

  # ── Step 6: Verify server is using the right database ────────────────────

  log "Step 6: Verify server config"
  db=$(kubectl logs deployment/gkg-webserver -n default 2>/dev/null \
    | grep "parsed ClickHouse config" \
    | tail -1 \
    | grep -o '"graph_database":"[^"]*"' \
    | cut -d'"' -f4 || echo "unknown")
  if [[ "$db" == "gkg-development" ]]; then
    ok "Server is using graph_database=gkg-development"
  else
    log "WARNING: could not confirm graph_database (got: '$db') — check kubectl logs"
  fi
else
  ok "Skipping Tilt / port-forwards (--no-tilt)"
fi

# ── Summary ──────────────────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  Bootstrap complete"
echo "══════════════════════════════════════════════════════════"
echo ""
echo "  GitLab     http://127.0.0.1:3000"
if $START_TILT; then
echo "  GKG HTTP   http://localhost:4200"
echo "  GKG gRPC   localhost:50051"
echo "  Tilt UI    http://localhost:10350"
echo ""
echo "  Tilt log:          $DEV_DIR/tilt.log"
echo "  Port-forward log:  $DEV_DIR/port-forwards.log"
fi
echo ""
echo "  Run E2E tests:"
echo "    cd ~/Desktop/Code/gdk/gitlab && bundle exec rails runner \\"
echo "      ~/Desktop/Code/angelo-orbit-work-specs/specs/e2e-knowledge-graph-testing/tests/redaction_test.rb"
echo ""
echo "  Teardown:"
echo "    $GKG_DIR/scripts/teardown-dev.sh"
echo ""
