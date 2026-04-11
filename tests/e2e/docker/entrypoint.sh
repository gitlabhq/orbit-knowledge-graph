#!/usr/bin/env bash
# Start all services via GDK's runit, seed data, then keep container alive.
set -euo pipefail

echo "=== gkg-e2e-base starting ==="

grep -q gdk.test /etc/hosts || echo "127.0.0.1 gdk.test" | sudo tee -a /etc/hosts

eval "$(~/.local/bin/mise activate bash)"
cd /gitlab-gdk/gitlab-development-kit
mise x -- gdk start

echo "=== Waiting for Rails ==="
for i in $(seq 1 300); do
    if curl -sk https://gdk.test:3443/-/readiness 2>/dev/null | grep -q '"status":"ok"'; then
        echo "Rails ready after ${i}s"
        break
    fi
    [ "$i" -eq 300 ] && echo "WARNING: Rails not ready after 300s"
    sleep 1
done

echo "=== Seeding data ==="
cd /gitlab-gdk/gitlab-development-kit/gitlab
GITLAB_SIMULATE_SAAS=1 RAILS_ENV=development bundle exec rails runner /home/gdk/seed-data.rb || echo "WARNING: Seed errors"

echo "=== All services ready ==="
cd /gitlab-gdk/gitlab-development-kit
mise x -- gdk status
echo "  GitLab:     https://gdk.test:3443"
echo "  ClickHouse: http://localhost:8123"
echo "  NATS:       nats://localhost:4222"

sleep 720d
