#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

failures=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1: $2"; failures=$((failures + 1)); }
expect_error() {
  local name=$1 pattern=$2 output=$3
  if grep -q -E "$pattern" <<<"$output"; then pass "$name"; else fail "$name" "$output"; fi
}
clickhouse_as() {
  curl -s -m 10 -u "$1" "http://127.0.0.1:$RELAY_PORT/" --data-binary "$2"
}
counter() {
  vm sudo iptables -L "$1" -v -x -n | awk -v m="$2" '$0 ~ m {sum += $1} END {print sum + 0}'
}

out=$(clickhouse_as demo:orbit-demo-read "SELECT * FROM url('http://example.com', 'RawBLOB')")
expect_error "grants: url() as the demo user is denied" "ACCESS_DENIED" "$out"
out=$(clickhouse_as demo:orbit-demo-read "SELECT * FROM remote('example.com', system.one)")
expect_error "grants and config: remote() as the demo user fails" "ACCESS_DENIED|UNACCEPTABLE_URL" "$out"
out=$(clickhouse_as demo:orbit-demo-read "INSERT INTO FUNCTION file('x.txt', 'RawBLOB') SELECT 'x'")
expect_error "grants: the demo user cannot write" "ACCESS_DENIED|READONLY" "$out"

probe=orbit-demo-probe
"${DOCKER[@]}" rm -f "$probe" >/dev/null 2>&1 || true
"${DOCKER[@]}" volume rm "$probe-users" >/dev/null 2>&1 || true
COPYFILE_DISABLE=1 tar --no-xattrs -C "$ORBIT_DEMO_ROOT/clickhouse/probe" -cf - users.d |
  "${DOCKER[@]}" run --rm -i --network none --entrypoint tar \
    -v "$probe-users:/config" "$CLICKHOUSE_IMAGE" -xf - -C /config
"${DOCKER[@]}" run -d --name "$probe" --network none --cap-drop ALL \
  --security-opt no-new-privileges --user 101:101 --tmpfs /var/lib/clickhouse:uid=101,gid=101 \
  --mount "type=volume,src=$CLICKHOUSE_CONFIG_VOLUME,dst=/etc/clickhouse-server/config.d,volume-subpath=config.d,readonly" \
  --mount "type=volume,src=$probe-users,dst=/etc/clickhouse-server/users.d,volume-subpath=users.d,readonly" \
  --entrypoint /usr/bin/clickhouse-server "$CLICKHOUSE_IMAGE" \
  --config-file=/etc/clickhouse-server/config.xml >/dev/null
out=""
for _ in $(seq 1 30); do
  out=$("${DOCKER[@]}" exec "$probe" clickhouse-client --user probe \
    -q "SELECT * FROM url('http://example.com', 'RawBLOB')" 2>&1 || true)
  grep -q -E "UNACCEPTABLE_URL|ACCESS_DENIED|DNS_ERROR" <<<"$out" && break
  sleep 1
done
"${DOCKER[@]}" rm -f "$probe" >/dev/null
"${DOCKER[@]}" volume rm "$probe-users" >/dev/null
expect_error "config: remote_url_allow_hosts refuses url() for a user granted everything" \
  "UNACCEPTABLE_URL" "$out"

limits=(--max_execution_time=10 --http_max_tries=1 --http_connection_timeout=3 --http_receive_timeout=3)
for target in "http://example.com" "http://93.184.215.14"; do
  out=$("${DOCKER[@]}" run --rm --network "$INTERNAL_NET" --cap-drop ALL \
    --security-opt no-new-privileges --entrypoint clickhouse "$CLICKHOUSE_IMAGE" local \
    "${limits[@]}" -q "SELECT * FROM url('$target', 'RawBLOB')" 2>&1 || true)
  expect_error "internal network: url($target) as the default user with no URL limits fails" \
    "DNS_ERROR|Host not found|Timeout|Network is unreachable|NetException|NETWORK_ERROR" "$out"
done

out=$("${DOCKER[@]}" exec "$CLICKHOUSE" getent hosts example.com 2>&1 && echo RESOLVED || true)
if grep -q RESOLVED <<<"$out"; then fail "internal DNS: example.com resolves" "$out"; else pass "internal DNS: example.com does not resolve from ClickHouse"; fi

before=$(counter DOCKER-USER "$EDGE_BRIDGE")
if "${DOCKER[@]}" exec "$RELAY" socat -u OPEN:/dev/null TCP:1.1.1.1:443,connect-timeout=5 2>/dev/null; then
  fail "DOCKER-USER: relay reached 1.1.1.1:443" "connected"
else
  after=$(counter DOCKER-USER "$EDGE_BRIDGE")
  if ((after > before)); then pass "DOCKER-USER: the relay's outbound connect timed out ($((after - before)) packets dropped)"; else fail "DOCKER-USER" "connect failed but the rule dropped nothing"; fi
fi

before=$(counter ORBIT-DEMO-OUT DROP)
if vm curl -s -m 5 -o /dev/null https://example.com 2>/dev/null; then fail "VM OUTPUT: curl example.com" "succeeded"; else pass "VM OUTPUT: curl -m 5 https://example.com fails"; fi
if vm curl -s -m 5 -o /dev/null https://1.1.1.1 2>/dev/null; then fail "VM OUTPUT: curl 1.1.1.1" "succeeded"; else
  after=$(counter ORBIT-DEMO-OUT DROP)
  if ((after > before)); then pass "VM OUTPUT: curl https://1.1.1.1 is dropped ($((after - before)) packets)"; else fail "VM OUTPUT" "curl failed but the rule dropped nothing"; fi
fi
masked=$( (vm systemctl is-enabled apt-daily.timer apt-daily-upgrade.timer motd-news.timer \
  ua-timer.timer systemd-timesyncd.service 2>/dev/null || true) | sort -u | tr '\n' ' ')
[[ "$masked" == "masked " ]] && pass "VM timers masked" || fail "VM timers masked" "$masked"

[[ "$(curl -s -m 5 "http://127.0.0.1:$RELAY_PORT/ping")" == "Ok." ]] &&
  pass "port forward works with the rules in place" || fail "port forward" "no answer"
for port in "$RELAY_PORT" "$@"; do
  if out=$(assert_loopback_listener "$port" 2>&1); then pass "lsof: $out"; else fail "lsof" "$out"; fi
done

echo "$failures failure(s)"
exit "$failures"
