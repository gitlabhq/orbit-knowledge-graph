#!/usr/bin/env bash
# Start GDK with all services, configure ClickHouse/NATS/Siphon, seed data.
set -euo pipefail

echo "=== gkg-e2e-base starting ==="

grep -q gdk.test /etc/hosts || echo "127.0.0.1 gdk.test" | sudo tee -a /etc/hosts

export MISE_PYTHON_GITHUB_ATTESTATIONS=false
eval "$(~/.local/bin/mise activate bash)"
cd /gitlab-gdk/gitlab-development-kit

# Phase 2: Apply full config with ClickHouse, NATS, Siphon.
# Services are available now because the container has a proper PID 1.
cp /tmp/gdk-e2e-overlay.yml gdk.yml
mise x -- gdk reconfigure
mise x -- gdk restart

echo "=== Waiting for Rails ==="
for i in $(seq 1 300); do
    if curl -sk https://gdk.test:3443/-/readiness 2>/dev/null | grep -q '"status":"ok"'; then
        echo "Rails ready after ${i}s"
        break
    fi
    [ "$i" -eq 300 ] && echo "WARNING: Rails not ready after 300s"
    sleep 1
done

# Seed data
echo "=== Seeding data ==="
cd /gitlab-gdk/gitlab-development-kit/gitlab
GITLAB_SIMULATE_SAAS=1 RAILS_ENV=development bundle exec rails runner /home/gdk/seed-data.rb || echo "WARNING: Seed errors (may be partial)"

echo "=== All services ready ==="
mise x -- gdk status
echo "  GitLab:     https://gdk.test:3443"
echo "  ClickHouse: http://localhost:8123"
echo "  NATS:       nats://localhost:4222"

# Keep container alive
sleep 720d
