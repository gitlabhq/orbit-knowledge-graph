#!/usr/bin/env bash
# =============================================================================
# teardown-dev.sh — Stop the local GKG development environment
#
# Stops (in reverse order):
#   1. kubectl port-forwards
#   2. Tilt
#   3. GDK services
#   4. Colima (optional — skipped by default to preserve k8s state)
#
# Usage:
#   cd ~/Desktop/Code/gkg
#   ./scripts/teardown-dev.sh
#
# Options:
#   --keep-colima   Leave Colima running (skips the slow VM boot on next bootstrap)
#   --no-gdk        Skip stopping GDK
# =============================================================================

set -euo pipefail

GKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GDK_DIR=~/Desktop/Code/gdk
DEV_DIR="$GKG_DIR/.dev"

STOP_COLIMA=true
STOP_GDK=true

for arg in "$@"; do
  case $arg in
    --keep-colima) STOP_COLIMA=false ;;
    --no-gdk)      STOP_GDK=false    ;;
  esac
done

log()  { echo "  [teardown] $*"; }
ok()   { echo "  [teardown] ✓ $*"; }
info() { echo "  [teardown] → $*"; }

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  GKG Dev Environment Teardown"
echo "══════════════════════════════════════════════════════════"
echo ""

# ── Port-forwards ─────────────────────────────────────────────────────────────

log "Stopping port-forwards..."
pkill -f "port-forward svc/gkg-webserver" 2>/dev/null && ok "Port-forwards stopped" || ok "No port-forwards running"

if [[ -f "$DEV_DIR/port-forwards.pid" ]]; then
  while read -r pid; do
    kill "$pid" 2>/dev/null || true
  done <<< "$(cat "$DEV_DIR/port-forwards.pid")"
  rm -f "$DEV_DIR/port-forwards.pid"
fi

# ── Tilt ─────────────────────────────────────────────────────────────────────

log "Stopping Tilt..."
if [[ -f "$DEV_DIR/tilt.pid" ]]; then
  TILT_PID=$(cat "$DEV_DIR/tilt.pid")
  kill "$TILT_PID" 2>/dev/null && ok "Tilt stopped (pid=$TILT_PID)" || ok "Tilt already stopped"
  rm -f "$DEV_DIR/tilt.pid"
else
  # Try to find and kill tilt by process name
  pkill -f "tilt up" 2>/dev/null && ok "Tilt stopped" || ok "Tilt not running"
fi

# Also kill any stale local gkg-server that may have been started manually
pkill -f "target/debug/gkg-server" 2>/dev/null && log "Killed stale local gkg-server" || true

# ── GDK ──────────────────────────────────────────────────────────────────────

if $STOP_GDK; then
  log "Stopping GDK..."
  cd "$GDK_DIR"
  gdk stop && ok "GDK stopped"
else
  ok "Skipping GDK stop (--no-gdk)"
fi

# ── Colima ───────────────────────────────────────────────────────────────────

if $STOP_COLIMA; then
  log "Stopping Colima..."
  colima stop && ok "Colima stopped"
else
  ok "Leaving Colima running (pass --keep-colima to preserve this behaviour)"
fi

echo ""
echo "══════════════════════════════════════════════════════════"
echo "  Teardown complete"
echo "══════════════════════════════════════════════════════════"
echo ""
