#!/usr/bin/env bash
# Tests for scripts/dev/caproni-edit-mode.sh that need no cluster and no mirrord.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/../caproni-edit-mode.sh"
FIXTURE="$HERE/fixtures/pod-config.yaml"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
failures=0

check() { # name, expected, actual
  if [[ "$2" == "$3" ]]; then printf '[ok] %s\n' "$1"; else printf '[fail] %s: expected %q, got %q\n' "$1" "$2" "$3"; failures=$((failures + 1)); fi
}

# shellcheck source=../caproni-edit-mode.sh
ORBIT_POD_CONFIG="$FIXTURE" GENERATED_DIR_OVERRIDE="$TMP" source "$SCRIPT"

check "webserver mode" "webserver" "$(edit_mode_for webserver)"
check "indexer-sdlc mode" "indexer" "$(edit_mode_for indexer-sdlc)"
check "indexer-code mode" "indexer" "$(edit_mode_for indexer-code)"
check "dispatcher mode" "dispatch-indexing" "$(edit_mode_for dispatcher)"
check "unknown key rejected" "1" "$(edit_mode_for bogus >/dev/null 2>&1; echo $?)"

check "webserver keeps the pod port" "9394" "$(probe_port_for webserver)"
check "ports are distinct" "4" "$(for k in webserver indexer-sdlc indexer-code dispatcher; do probe_port_for "$k"; done | sort -u | wc -l | tr -d ' ')"

overlay="$(write_overlay indexer-sdlc)"
check "overlay path" "$TMP/caproni-indexer-sdlc.yaml" "$overlay"
check "probe port rewritten" "0.0.0.0:9395" "$(yq '.probe_server.bind_address' "$overlay")"
check "other keys untouched" "gkg" "$(yq '.graph.database' "$overlay")"
check "grpc bind untouched" "0.0.0.0:50054" "$(yq '.grpc_bind_address' "$overlay")"
check "overlay is private" "600" "$(stat -f '%Lp' "$overlay" 2>/dev/null || stat -c '%a' "$overlay")"
# The server refuses a config where metrics.prometheus.port disagrees with the probe
# bind address, so the deprecated key must go when the port moves.
check "deprecated prometheus port removed" "null" "$(yq '.metrics.prometheus.port' "$overlay")"
check "prometheus stays enabled" "true" "$(yq '.metrics.prometheus.enabled' "$overlay")"

# malformed_pod_config_fails: a yq failure must not produce a header-only overlay.
printf 'probe_server: [unclosed\n' >"$TMP/bad.yaml"
rc=0; out="$(POD_CONFIG="$TMP/bad.yaml" write_overlay dispatcher 2>&1)" || rc=$?
check "malformed pod config fails" "1" "$rc"
check "no overlay left behind" "no" "$([[ -f "$TMP/caproni-dispatcher.yaml" ]] && echo yes || echo no)"

# missing_pod_config_errors: outside mirrord the pod config does not exist.
out="$(ORBIT_POD_CONFIG="$TMP/does-not-exist.yaml" bash "$SCRIPT" webserver 2>&1 || true)"
check "missing pod config names caproni run" "yes" "$(grep -q 'caproni run' <<<"$out" && echo yes || echo no)"

if [[ "$failures" -gt 0 ]]; then printf '\n%d check(s) failed\n' "$failures"; exit 1; fi
printf '\nall checks passed\n'
