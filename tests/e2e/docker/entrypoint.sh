#!/usr/bin/env bash
# Start all services via GDK's runit, seed data, then keep container alive.
# No set -e: the container must stay alive even if setup steps fail.

echo "=== gkg-e2e-base starting ==="

# Remove memory limit for ClickHouse
mkdir -p /gitlab-gdk/gitlab-development-kit/clickhouse/config.d
cat > /gitlab-gdk/gitlab-development-kit/clickhouse/config.d/zz-e2e.xml <<'XML'
<clickhouse>
  <max_server_memory_usage>0</max_server_memory_usage>
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

# After GDK starts, fix ClickHouse to listen on all interfaces and restart it
echo "=== Fixing ClickHouse listen address ==="
CH_CONFIG="/gitlab-gdk/gitlab-development-kit/clickhouse/config.xml"
# Replace localhost-only listen_host with 0.0.0.0
sed -i '/<listen_host>::1<\/listen_host>/d' "$CH_CONFIG"
sed -i '/<listen_host>127\.0\.0\.1<\/listen_host>/d' "$CH_CONFIG"
sed -i 's|<!-- <listen_host>0\.0\.0\.0</listen_host> -->|<listen_host>0.0.0.0</listen_host>|' "$CH_CONFIG"
# If 0.0.0.0 wasn't in a comment, add it before </clickhouse>
grep -q '<listen_host>0.0.0.0</listen_host>' "$CH_CONFIG" || \
    sed -i 's|</clickhouse>|    <listen_host>0.0.0.0</listen_host>\n</clickhouse>|' "$CH_CONFIG"
# Verify
echo "ClickHouse listen_host entries:"
grep "listen_host" "$CH_CONFIG" | grep -v "<!--"
# Restart ClickHouse to apply
sv restart /gitlab-gdk/gitlab-development-kit/services/clickhouse
sleep 3

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
    if curl -s http://127.0.0.1:3000/-/readiness 2>/dev/null | grep -q '"status":"ok"'; then
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
echo "  GitLab:     http://0.0.0.0:3000"
echo "  ClickHouse: http://0.0.0.0:8123"
echo "  NATS:       nats://0.0.0.0:4222"

sleep 720d
