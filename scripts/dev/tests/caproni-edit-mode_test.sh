#!/usr/bin/env bash
# Tests for scripts/dev/caproni-edit-mode.sh that need no cluster, no mirrord, and no cargo.
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

# A fake target/debug tree with a stand-in binary and two fips artifact directories.
FAKE_TARGET="$TMP/target/debug"
mkdir -p "$FAKE_TARGET/build/aws-lc-fips-sys-aaaa/out/build/artifacts" "$FAKE_TARGET/build/aws-lc-fips-sys-bbbb/out/build/artifacts"
FAKE_BINARY="$FAKE_TARGET/gkg-server"
printf 'not a real binary\n' >"$FAKE_BINARY"
chmod +x "$FAKE_BINARY"

# shellcheck source=../caproni-edit-mode.sh
ORBIT_POD_CONFIG="$FIXTURE" GENERATED_DIR_OVERRIDE="$TMP/.dev" ORBIT_BINARY_OVERRIDE="$FAKE_BINARY" source "$SCRIPT"

check "webserver mode" "webserver" "$(edit_mode_for webserver)"
check "indexer-sdlc mode" "indexer" "$(edit_mode_for indexer-sdlc)"
check "indexer-code mode" "indexer" "$(edit_mode_for indexer-code)"
check "dispatcher mode" "dispatch-indexing" "$(edit_mode_for dispatcher)"
check "unknown key rejected" "1" "$(edit_mode_for bogus >/dev/null 2>&1; echo $?)"

check "webserver keeps the chart port" "9394" "$(probe_port_for webserver)"
check "ports are distinct" "4" "$(for k in webserver indexer-sdlc indexer-code dispatcher; do probe_port_for "$k"; done | sort -u | wc -l | tr -d ' ')"

overlay="$(write_overlay indexer-sdlc)"
check "overlay path" "$TMP/.dev/caproni-indexer-sdlc.yaml" "$overlay"
check "probe port rewritten" "0.0.0.0:9395" "$(yq '.probe_server.bind_address' "$overlay")"
check "other keys untouched" "gkg" "$(yq '.graph.database' "$overlay")"
check "grpc bind untouched" "0.0.0.0:50054" "$(yq '.grpc_bind_address' "$overlay")"
check "overlay is private" "600" "$(stat -c '%a' "$overlay" 2>/dev/null || stat -f '%Lp' "$overlay")"
# The server refuses a config where metrics.prometheus.port disagrees with the probe
# bind address, so the deprecated key must go when the port moves.
check "deprecated prometheus port removed" "null" "$(yq '.metrics.prometheus.port' "$overlay")"
check "prometheus stays enabled" "true" "$(yq '.metrics.prometheus.enabled' "$overlay")"

# malformed_pod_config_fails: a yq failure must not produce a header-only overlay.
printf 'probe_server: [unclosed\n' >"$TMP/bad.yaml"
rc=0; out="$(POD_CONFIG="$TMP/bad.yaml" write_overlay dispatcher 2>&1)" || rc=$?
check "malformed pod config fails" "1" "$rc"
check "no overlay left behind" "no" "$([[ -f "$TMP/.dev/caproni-dispatcher.yaml" ]] && echo yes || echo no)"

# missing_pod_config_errors: outside mirrord the pod config does not exist.
out="$(ORBIT_POD_CONFIG="$TMP/does-not-exist.yaml" bash "$SCRIPT" webserver 2>&1 || true)"
check "missing pod config names caproni run" "yes" "$(grep -q 'caproni run' <<<"$out" && echo yes || echo no)"

# build_stamps_only_on_relink: the build step rewrites the stamp the mirrord-side processes
# watch only when cargo says it relinked gkg-server. A no-op build must not restart four
# servers, and a failed build must leave the running ones alone.
fake_cargo() { # fresh|fail -> prints what cargo's JSON stream would say
  case "$1" in
    fail) echo '{"reason":"build-finished","success":false}'; return 101 ;;
    *) printf '{"reason":"compiler-artifact","target":{"name":"orbit-server"},"fresh":false}\n{"reason":"compiler-artifact","target":{"name":"gkg-server"},"fresh":%s}\n{"reason":"build-finished","success":true}\n' "$1" ;;
  esac
}
cargo_build() { fake_cargo true; }
build >/dev/null
check "first build writes the stamp" "yes" "$([[ -s "$STAMP" ]] && echo yes || echo no)"
touch "$TMP/marker"; sleep 1
cargo_build() { fake_cargo true; }
build >/dev/null
check "no-op build leaves the stamp alone" "no" "$([[ "$STAMP" -nt "$TMP/marker" ]] && echo yes || echo no)"
cargo_build() { fake_cargo fail; }
rc=0; build >/dev/null 2>&1 || rc=$?
check "failed build reports failure" "1" "$rc"
check "failed build leaves the stamp alone" "no" "$([[ "$STAMP" -nt "$TMP/marker" ]] && echo yes || echo no)"
check "failed build leaves no message file" "0" "$(ls "$TMP"/.dev/cargo-messages.* 2>/dev/null | wc -l | tr -d ' ')"
cargo_build() { fake_cargo false; }
build >/dev/null
check "relinked binary refreshes the stamp" "yes" "$([[ "$STAMP" -nt "$TMP/marker" ]] && echo yes || echo no)"

# one_watcher_per_checkout: every reloader declares the build watcher, so whichever starts
# first owns the lock and the rest idle instead of racing cargo.
check "first watcher takes the lock" "0" "$(acquire_watch_lock >/dev/null 2>&1; echo $?)"
sleep 60 & live_pid=$!
echo "$live_pid" >"$WATCH_LOCK"
check "second watcher is refused while the first lives" "1" "$(acquire_watch_lock >/dev/null 2>&1; echo $?)"
kill "$live_pid" 2>/dev/null; wait "$live_pid" 2>/dev/null || true
echo 999999999 >"$WATCH_LOCK"
check "stale lock from a dead pid is taken over" "0" "$(acquire_watch_lock >/dev/null 2>&1; echo $?)"
# Caproni starts the four watchers in the same instant; exactly one may win.
rm -f "$WATCH_LOCK"
for i in 1 2 3 4; do
  (bash -c "sleep 0.3; ORBIT_POD_CONFIG='$FIXTURE' GENERATED_DIR_OVERRIDE='$TMP/.dev' source '$SCRIPT'; if acquire_watch_lock; then echo won; sleep 2; else echo lost; fi" >"$TMP/race.$i") &
done
wait
check "simultaneous watchers: one lock holder" "1" "$(cat "$TMP"/race.* | grep -c won)"

# watcher_shape: the build watcher must see a checkout under an ignored parent path and must
# not kill a running cargo when a second save lands.
watch_args="${WATCH_ARGS[*]}"
check "watcher ignores no gitignore" "yes" "$(grep -q -- '--ignore-nothing' <<<"$watch_args" && echo yes || echo no)"
check "watcher queues instead of restarting cargo" "yes" "$(grep -q -- '--on-busy-update=queue' <<<"$watch_args" && echo yes || echo no)"
check "watcher covers crates and config" "yes" "$(grep -q -- '-w crates' <<<"$watch_args" && grep -q -- '-w config' <<<"$watch_args" && echo yes || echo no)"

# launch_sets_library_path: a raw gkg-server needs the aws-lc-fips-sys artifacts on the
# loader path, which cargo run would have added for us.
# exec is shadowed by a function so the test sees the environment the launcher hands over.
# (macOS purges DYLD_* on exec of SIP-protected binaries such as /usr/bin/env, so a fake
# script binary cannot report it.)
out="$(exec() { printf 'dyld=%s\nld=%s\nargs=%s\n' "${DYLD_FALLBACK_LIBRARY_PATH:-}" "${LD_LIBRARY_PATH:-}" "$*"; }; launch indexer "$overlay")"
check "launcher passes mode and config" "args=$FAKE_BINARY --mode=indexer --config=$overlay" "$(grep '^args=' <<<"$out")"
check "launcher adds every fips artifacts dir (macOS)" "yes" "$(grep '^dyld=' <<<"$out" | grep -q 'aws-lc-fips-sys-aaaa/out/build/artifacts' && grep '^dyld=' <<<"$out" | grep -q 'aws-lc-fips-sys-bbbb/out/build/artifacts' && echo yes || echo no)"
check "launcher adds the fips artifacts dir (Linux)" "yes" "$(grep '^ld=' <<<"$out" | grep -q 'aws-lc-fips-sys-' && echo yes || echo no)"

# project_file: the reloaders must agree with the wrapper.
PROJECT="$HERE/../../../caproni-project.yaml"
matches_pattern() { # path -> yes if any read_only pattern of the first reloader matches
  local path="$1" pat
  while IFS= read -r pat; do [[ "$path" =~ $pat ]] && { echo yes; return; }; done < <(yq -r '.reloaders.gkg-webserver.mirror.filesystem.read_only[]' "$PROJECT")
  echo no
}
# The server checks that /etc/secrets itself exists before walking it, so the mirrord
# read-only pattern must match the directory path as well as the files under it.
check "pattern matches the secrets dir" "yes" "$(matches_pattern /etc/secrets)"
check "pattern matches a secret file" "yes" "$(matches_pattern /etc/secrets/gitlab/jwt/verifying_key)"
check "pattern matches the config dir" "yes" "$(matches_pattern /app/config)"
check "pattern matches the config file" "yes" "$(matches_pattern /app/config/config.yaml)"
check "pattern matches internal tls" "yes" "$(matches_pattern /etc/tls/tls.crt)"
check "pattern leaves the repo local" "no" "$(matches_pattern /Users/me/knowledge-graph/config/default.yaml)"
check "every reloader shares the patterns" "1" "$(yq -r '[.reloaders[].mirror.filesystem.read_only | join(",")] | unique | length' "$PROJECT")"
check "every reloader carries the local build watcher" "4" "$(yq -r '[.reloaders[].mirror.edit_mode_processes[] | select(.context == "local") | .command] | map(select(. == "scripts/dev/caproni-edit-mode.sh watch")) | length' "$PROJECT")"
check "no mirrord process runs cargo" "0" "$(yq -r '[.reloaders[].mirror.edit_mode_processes[] | select(.context != "local") | .command] | map(select(test("cargo|watch"))) | length' "$PROJECT")"
check "prepare hook builds through the wrapper" "yes" "$(yq -r '.lifecycle_hooks[] | select(.type == "edit_mode_prepare") | .script' "$PROJECT" | grep -q 'caproni-edit-mode.sh build' && echo yes || echo no)"

if [[ "$failures" -gt 0 ]]; then printf '\n%d check(s) failed\n' "$failures"; exit 1; fi
printf '\nall checks passed\n'
