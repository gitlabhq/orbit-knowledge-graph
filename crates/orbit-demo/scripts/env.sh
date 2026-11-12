set -euo pipefail

ORBIT_DEMO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE=orbit-demo
CONTEXT=colima-orbit-demo
DOCKER=(docker --context "$CONTEXT")
# The VM mounts nothing, so `colima ssh` must start from a directory the VM also has.
vm() {
  (cd / && colima ssh --profile "$PROFILE" -- "$@")
}

CLICKHOUSE_IMAGE=clickhouse/clickhouse-server@sha256:2c870d77ea76e955fb1c8dad536ad40a1236b6093db0c4966e22d21d4428982b
RELAY_IMAGE=alpine/socat@sha256:beb4a68d9e4fe6b0f21ea774a0fde6c31f580dde6368939ed70100c5385b015e

INTERNAL_NET=orbit-internal
INTERNAL_BRIDGE=orbit-int0
INTERNAL_SUBNET=172.30.0.0/24
EDGE_NET=orbit-edge
EDGE_BRIDGE=orbit-edge0
EDGE_SUBNET=172.30.1.0/24

CLICKHOUSE=orbit-demo-clickhouse
CLICKHOUSE_CONFIG_VOLUME=orbit-demo-clickhouse-config
CLICKHOUSE_DATA_VOLUME=orbit-demo-clickhouse-data
RELAY=orbit-demo-relay
RELAY_PORT=18123

colima_start() {
  colima start "$PROFILE" \
    --runtime docker --vm-type vz --arch aarch64 \
    --cpus 2 --memory 4 --disk 20 --root-disk 20 \
    --mount none --mount-type virtiofs --mount-inotify=false \
    --network-mode shared --network-host-addresses=false --port-forwarder ssh \
    --gateway-address 192.168.5.2 \
    --activate=false --ssh-agent=false --ssh-config=false --binfmt=false \
    --kubernetes=false --nested-virtualization=false --vz-rosetta=false \
    --save-config=true --template=false
}

vm_running() {
  colima status --profile "$PROFILE" >/dev/null 2>&1
}

assert_loopback_listener() {
  local port=$1 listeners
  listeners=$(lsof -iTCP:"$port" -sTCP:LISTEN -n -P 2>/dev/null | awk 'NR > 1 {print $9}' | sort -u)
  if [[ -z "$listeners" ]]; then
    echo "nothing listens on port $port" >&2
    return 1
  fi
  if grep -v -x "127.0.0.1:$port" <<<"$listeners" >/dev/null; then
    echo "port $port listens beyond 127.0.0.1: $listeners" >&2
    return 1
  fi
  echo "port $port: $listeners"
}
