#!/usr/bin/env bash
# Tests for scripts/dev/orbit-native-dev.sh that need no cluster: the overlay merge and the
# restore logic, with kubectl stubbed.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/../orbit-native-dev.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
failures=0

check() { # name, expected, actual
  if [[ "$2" == "$3" ]]; then printf '[ok] %s\n' "$1"; else printf '[fail] %s: expected %q, got %q\n' "$1" "$2" "$3"; failures=$((failures + 1)); fi
}

# shellcheck source=../orbit-native-dev.sh
ORBIT_DEV_SKIP_ENV=1 GENERATED_DIR_OVERRIDE="$TMP" \
  DEV_OVERLAY_OVERRIDE="$HERE/fixtures/dev.yaml" LOCAL_OVERLAY_OVERRIDE="$TMP/absent.yaml" \
  source "$SCRIPT"

# yaml_str_escapes: a key with a quote and a backslash survives into valid YAML.
CLICKHOUSE_PASSWORD='p"a\ss'
JWT_VERIFYING_KEY='k"e\y'
JWT_SIGNING_KEY='k"e\y'
GITLAB_BASE_URL="http://gitlab.caproni.test"
overlay="$(write_mode_overlay indexer)"
check "password round-trips" 'p"a\ss' "$(yq '.graph.password' "$overlay")"
check "jwt round-trips" 'k"e\y' "$(yq '.gitlab.jwt.verifying_key' "$overlay")"
check "base url" "http://gitlab.caproni.test" "$(yq '.gitlab.base_url' "$overlay")"
check "indexer probe port" "127.0.0.1:9200" "$(yq '.probe_server.bind_address' "$overlay")"
check "dev.yaml values kept" "gitlab_main" "$(yq '.datalake.database' "$overlay")"
check "overlay is private" "600" "$(stat -f '%Lp' "$overlay" 2>/dev/null || stat -c '%a' "$overlay")"

# local_overlay_wins: config/dev.local.yaml beats cluster-derived and dev.yaml values.
LOCAL_OVERLAY="$HERE/fixtures/dev.local.yaml"
overlay="$(write_mode_overlay webserver)"
check "local overlay wins" "gkg-sandbox" "$(yq '.graph.database' "$overlay")"
check "webserver probe port" "127.0.0.1:9100" "$(yq '.probe_server.bind_address' "$overlay")"

# restore_file_skips_zero: a recorded zero (killed session) restores to 1, others verbatim.
SCALED=()
kc() { SCALED+=("$*"); }
printf 'gkg-webserver=1\ngkg-indexer-sdlc=0\ngkg-dispatcher=2\n' >"$TMP/state"
restore_from_state_file "$TMP/state" 2>/dev/null
check "webserver restored" "-n gitlab scale deploy/gkg-webserver --replicas=1" "${SCALED[0]}"
check "zero becomes one" "-n gitlab scale deploy/gkg-indexer-sdlc --replicas=1" "${SCALED[1]}"
check "dispatcher restored" "-n gitlab scale deploy/gkg-dispatcher --replicas=2" "${SCALED[2]}"
check "state file removed" "no" "$([[ -f "$TMP/state" ]] && echo yes || echo no)"

# endpointslice carries the host ip and the service port name.
slice="$(endpointslice_yaml 192.168.106.1)"
check "slice address" "192.168.106.1" "$(yq '.endpoints[0].addresses[0]' <<<"$slice")"
check "slice port name" "grpc" "$(yq '.ports[0].name' <<<"$slice")"
check "slice service label" "gkg-webserver" "$(yq '.metadata.labels."kubernetes.io/service-name"' <<<"$slice")"

# host_ip honours the override without touching the cluster.
check "host ip override" "10.0.0.9" "$(ORBIT_DEV_HOST_IP=10.0.0.9 host_ip)"

if [[ "$failures" -gt 0 ]]; then printf '\n%d check(s) failed\n' "$failures"; exit 1; fi
printf '\nall checks passed\n'
