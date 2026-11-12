#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

if ! vm_running; then
  colima_start
fi
for image in "$CLICKHOUSE_IMAGE" "$RELAY_IMAGE"; do
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

"${DOCKER[@]}" rm -f "$CLICKHOUSE" "$RELAY" >/dev/null 2>&1 || true
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

"${DOCKER[@]}" create --name "$RELAY" --network "$EDGE_NET" "${hardened[@]}" \
  --user 65534:65534 -p "127.0.0.1:$RELAY_PORT:$RELAY_PORT" "$RELAY_IMAGE" \
  "TCP-LISTEN:$RELAY_PORT,fork,reuseaddr" "TCP:$CLICKHOUSE:8123" >/dev/null
"${DOCKER[@]}" network connect "$INTERNAL_NET" "$RELAY"
"${DOCKER[@]}" start "$RELAY" >/dev/null

for _ in $(seq 1 60); do
  if [[ "$(curl -s -m 2 "http://127.0.0.1:$RELAY_PORT/ping" 2>/dev/null)" == "Ok." ]]; then
    assert_loopback_listener "$RELAY_PORT"
    echo "ClickHouse is up behind the relay on 127.0.0.1:$RELAY_PORT"
    exit 0
  fi
  sleep 1
done
echo "ClickHouse did not answer through the relay" >&2
"${DOCKER[@]}" logs --tail 40 "$CLICKHOUSE" >&2
exit 1
