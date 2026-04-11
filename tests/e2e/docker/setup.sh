#!/usr/bin/env bash
# Phase 1: Configure GDK with HTTPS/nginx (no ClickHouse/NATS/Siphon).
# These services require a running init system which isn't available during
# Docker build. They'll be configured at container startup in the entrypoint.
set -euo pipefail

export MISE_PYTHON_GITHUB_ATTESTATIONS=false
eval "$(~/.local/bin/mise activate bash)"

cd /gitlab-gdk/gitlab-development-kit

# Phase 1: HTTPS + nginx only (no services that need to be running)
cp /tmp/gdk-phase1-overlay.yml gdk.yml
mise x -- gdk reconfigure

# Stop all services
mise x -- gdk stop || true

echo "Phase 1 setup complete (HTTPS/nginx configured)."
echo "ClickHouse, NATS, and Siphon will be configured at container startup."
