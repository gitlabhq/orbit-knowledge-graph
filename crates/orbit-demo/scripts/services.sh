#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

if ! vm_running; then
  colima_start
fi
for image in "$CLICKHOUSE_IMAGE" "$RELAY_IMAGE" "$RUSTFS_IMAGE"; do
  "${DOCKER[@]}" image inspect "$image" >/dev/null 2>&1 || {
    echo "$image is missing; run crates/orbit-demo/scripts/setup.sh first" >&2
    exit 1
  }
done

"${DOCKER[@]}" network inspect "$INTERNAL_NET" >/dev/null 2>&1 ||
  "${DOCKER[@]}" network create --internal --subnet "$INTERNAL_SUBNET" \
    -o com.docker.network.bridge.name="$INTERNAL_BRIDGE" "$INTERNAL_NET" >/dev/null
"${DOCKER[@]}" network inspect "$EDGE_NET" >/dev/null 2>&1 ||
  "${DOCKER[@]}" network create --subnet "$EDGE_SUBNET" \
    -o com.docker.network.bridge.name="$EDGE_BRIDGE" \
    -o com.docker.network.bridge.enable_ip_masquerade=false "$EDGE_NET" >/dev/null

vm sudo sh -s "$EDGE_BRIDGE" "$INTERNAL_BRIDGE" <"$ORBIT_DEMO_ROOT/scripts/vm-egress.sh"

"${DOCKER[@]}" rm -f "$CLICKHOUSE" "$RELAY" "$RUSTFS" "$S3_RELAY" >/dev/null 2>&1 || true
"${DOCKER[@]}" volume rm "$CLICKHOUSE_CONFIG_VOLUME" >/dev/null 2>&1 || true
COPYFILE_DISABLE=1 tar --no-xattrs -C "$ORBIT_DEMO_ROOT/clickhouse" -cf - config.d users.d |
  "${DOCKER[@]}" run --rm -i --network none --entrypoint tar \
    -v "$CLICKHOUSE_CONFIG_VOLUME:/config" "$CLICKHOUSE_IMAGE" -xf - -C /config

hardened=(--cap-drop ALL --security-opt no-new-privileges --read-only)
"${DOCKER[@]}" run -d --name "$CLICKHOUSE" --network "$INTERNAL_NET" "${hardened[@]}" \
  --user 101:101 --ulimit nofile=262144:262144 \
  --tmpfs /tmp:uid=101,gid=101 --tmpfs /var/log/clickhouse-server:uid=101,gid=101 \
  --mount "type=volume,src=$CLICKHOUSE_CONFIG_VOLUME,dst=/etc/clickhouse-server/config.d,volume-subpath=config.d,readonly" \
  --mount "type=volume,src=$CLICKHOUSE_CONFIG_VOLUME,dst=/etc/clickhouse-server/users.d,volume-subpath=users.d,readonly" \
  -v "$CLICKHOUSE_DATA_VOLUME:/var/lib/clickhouse" \
  --entrypoint /usr/bin/clickhouse-server "$CLICKHOUSE_IMAGE" \
  --config-file=/etc/clickhouse-server/config.xml >/dev/null

"${DOCKER[@]}" run -d --name "$RUSTFS" --network "$INTERNAL_NET" "${hardened[@]}" \
  --user 10001:10001 --tmpfs /tmp:uid=10001,gid=10001 --tmpfs /logs:uid=10001,gid=10001 \
  -v "$RUSTFS_DATA_VOLUME:/data" \
  -e RUSTFS_ACCESS_KEY="$S3_KEY" -e RUSTFS_SECRET_KEY="$S3_SECRET" \
  -e RUSTFS_CONSOLE_ENABLE=false -e RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS= \
  "$RUSTFS_IMAGE" >/dev/null

relay() {
  local name=$1 port=$2 target=$3
  "${DOCKER[@]}" create --name "$name" --network "$EDGE_NET" "${hardened[@]}" \
    --user 65534:65534 -p "127.0.0.1:$port:$port" "$RELAY_IMAGE" \
    "TCP-LISTEN:$port,fork,reuseaddr" "TCP:$target" >/dev/null
  "${DOCKER[@]}" network connect "$INTERNAL_NET" "$name"
  "${DOCKER[@]}" start "$name" >/dev/null
}
relay "$RELAY" "$RELAY_PORT" "$CLICKHOUSE:8123"
relay "$S3_RELAY" "$S3_PORT" "$RUSTFS:9000"

s3() {
  curl -s -m 3 -o /dev/null -w "%{http_code}" --aws-sigv4 "aws:amz:$S3_REGION:s3" \
    --user "$S3_KEY:$S3_SECRET" "$@" "http://127.0.0.1:$S3_PORT/$S3_BUCKET"
}

for _ in $(seq 1 60); do
  clickhouse=$(curl -s -m 2 "http://127.0.0.1:$RELAY_PORT/ping" 2>/dev/null || true)
  bucket=$(s3 -X PUT 2>/dev/null || true)
  if [[ "$clickhouse" == "Ok." && ("$bucket" == 200 || "$bucket" == 409) ]]; then
    assert_loopback_listener "$RELAY_PORT"
    assert_loopback_listener "$S3_PORT"
    echo "ClickHouse is up on 127.0.0.1:$RELAY_PORT and RustFS on 127.0.0.1:$S3_PORT, each behind a relay"
    exit 0
  fi
  sleep 1
done
echo "ClickHouse or RustFS did not answer through its relay (bucket answered $bucket)" >&2
"${DOCKER[@]}" logs --tail 20 "$CLICKHOUSE" >&2
"${DOCKER[@]}" logs --tail 20 "$RUSTFS" >&2
exit 1
