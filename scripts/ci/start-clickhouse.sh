#!/usr/bin/env bash
set -euo pipefail

dir="${CLICKHOUSE_DIR:-/tmp/clickhouse}"
password="${CLICKHOUSE_PASSWORD:-testpass}"

if [ "${1:-}" = wait ]; then
  for _ in $(seq 1 600); do
    curl -sf -u "default:$password" --data 'SELECT 1' http://127.0.0.1:8123/ >/dev/null && exit 0
    sleep 0.1
  done
  cat "$dir/start.log" "$dir/server.log" >&2
  exit 1
fi

mkdir -p "$dir"
exec > "$dir/start.log" 2>&1

export MISE_DISABLE_TOOLS="$(printf %s "${MISE_DISABLE_TOOLS:-}" | tr , "\n" | grep -vx clickhouse | paste -sd , -)"
mise install clickhouse
clickhouse="$(mise which clickhouse)"
"$clickhouse" --version | grep -q " ${CLICKHOUSE_VERSION:?}\." || { echo "mise clickhouse is not ${CLICKHOUSE_VERSION}"; "$clickhouse" --version; exit 1; }
mkdir -p "$dir/config.d" "$dir/data"
cat > "$dir/config.d/ci.xml" <<XML
<clickhouse>
  <path>$dir/data/</path>
  <tmp_path>$dir/data/tmp/</tmp_path>
  <user_files_path>$dir/data/user_files/</user_files_path>
  <users_config>$dir/users.xml</users_config>
  <listen_host>127.0.0.1</listen_host>
  <logger><level>warning</level><console>1</console></logger>
</clickhouse>
XML
cat > "$dir/users.xml" <<XML
<clickhouse>
  <profiles><default/></profiles>
  <quotas><default/></quotas>
  <users>
    <default>
      <password>$password</password>
      <networks><ip>::/0</ip></networks>
      <profile>default</profile>
      <quota>default</quota>
      <access_management>1</access_management>
    </default>
  </users>
</clickhouse>
XML
cd "$dir"
exec "$clickhouse" server > "$dir/server.log" 2>&1
