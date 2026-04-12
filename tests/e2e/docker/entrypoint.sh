#!/usr/bin/env bash
# Start all services via GDK's runit, seed data, then keep container alive.
# No set -e: the container must stay alive even if setup steps fail,
# so the main CI job can reach the services and debug.

echo "=== gkg-e2e-base starting ==="

grep -q gdk.test /etc/hosts || echo "127.0.0.1 gdk.test" | sudo tee -a /etc/hosts

# Increase ClickHouse memory limit (default 3GB is too low alongside GDK)
mkdir -p /gitlab-gdk/gitlab-development-kit/clickhouse/config.d
cat > /gitlab-gdk/gitlab-development-kit/clickhouse/config.d/e2e.xml <<'XML'
<clickhouse>
  <max_server_memory_usage>8000000000</max_server_memory_usage>
  <listen_host>0.0.0.0</listen_host>
</clickhouse>
XML

# Enable logical replication for Siphon CDC
PG_CONF="/gitlab-gdk/gitlab-development-kit/postgresql/data/postgresql.conf"
if ! grep -q "^wal_level = logical" "$PG_CONF" 2>/dev/null; then
    echo "wal_level = logical" >> "$PG_CONF"
fi

eval "$(~/.local/bin/mise activate bash)"
cd /gitlab-gdk/gitlab-development-kit

mise x -- gdk start

# Patch nginx to listen on 0.0.0.0 AFTER gdk start (which may regenerate configs)
sed -i 's/listen gdk\.test:/listen 0.0.0.0:/g' nginx/conf/nginx.conf
# Also patch workhorse to listen on 0.0.0.0
sed -i 's/listenAddr = "gdk\.test:/listenAddr = "0.0.0.0:/g' gitlab-workhorse/config.toml 2>/dev/null || true
# Reload nginx with the patched config
sv restart services/nginx || true

echo "=== Waiting for ClickHouse ==="
for i in $(seq 1 60); do
    if curl -s "http://127.0.0.1:8123/?query=SELECT+1" 2>/dev/null | grep -q "1"; then
        echo "ClickHouse ready after ${i}s"
        break
    fi
    sleep 1
done

echo "=== Setting up ClickHouse databases ==="
cd /gitlab-gdk/gitlab-development-kit/gitlab
curl -s "http://127.0.0.1:8123/" --data-binary "CREATE DATABASE IF NOT EXISTS gitlab_clickhouse_development"
curl -s "http://127.0.0.1:8123/" --data-binary "CREATE DATABASE IF NOT EXISTS gitlab_clickhouse_main_development"
cp config/click_house.yml.example config/click_house.yml
echo "=== Running ClickHouse migrations ==="
RAILS_ENV=development bundle exec rake gitlab:clickhouse:migrate 2>&1 || echo "WARNING: ClickHouse migration had errors"

echo "=== Waiting for Rails ==="
cd /gitlab-gdk/gitlab-development-kit
for i in $(seq 1 300); do
    if curl -sk https://gdk.test:3443/-/readiness 2>/dev/null | grep -q '"status":"ok"'; then
        echo "Rails ready after ${i}s"
        break
    fi
    [ "$i" -eq 300 ] && echo "WARNING: Rails not ready after 300s"
    sleep 1
done

# Restart siphon now that ClickHouse databases and wal_level are configured
mise x -- gdk restart siphon || true
sleep 5

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
