#!/usr/bin/env bash
# Configure GDK with HTTPS, ClickHouse, NATS, and Siphon.
# ClickHouse and NATS are pre-installed as system packages.
set -euo pipefail

export MISE_PYTHON_GITHUB_ATTESTATIONS=false
eval "$(~/.local/bin/mise activate bash)"

cd /gitlab-gdk/gitlab-development-kit

# Apply the full config
cp /tmp/gdk-e2e-overlay.yml gdk.yml

# Reconfigure GDK
mise x -- gdk reconfigure

# Stop everything (entrypoint starts services at runtime)
mise x -- gdk stop || true

echo "GDK e2e setup complete."
