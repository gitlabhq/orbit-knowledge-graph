#!/usr/bin/env bash
# Orchestrator for the orbit-perf CI job (see .gitlab/ci/orbit-perf.yml).
#
# Brings up gitlab-dev-stack + Orbit with caproni (config in
# scripts/ci/orbit-perf/caproni), seeds gkg's ClickHouse with an
# xtask-generated synthetic graph (bulk Parquet load, bypassing siphon/seed/
# indexing), and runs the gRPC load test (xtask loadtest) against gkg.
#
# Env (from the CI job): SYNTH_CONFIG, ROUNDS, CONCURRENCY, WARMUP_ROUNDS, SEED,
# LATENCY_CONCURRENCY, LATENCY_REQUESTS, LOAD_JOBS, GKG_IMAGE, GKG_IMAGE_TAG.
set -euo pipefail

ROOT="$(pwd)"                                   # knowledge-graph checkout (xtask lives here)
SYNTH_CONFIG="${SYNTH_CONFIG:-crates/xtask/simulator_small.yaml}"
ROUNDS="${ROUNDS:-5}"
CONCURRENCY="${CONCURRENCY:-20}"
WARMUP_ROUNDS="${WARMUP_ROUNDS:-1}"
SEED="${SEED:-42}"
LATENCY_CONCURRENCY="${LATENCY_CONCURRENCY:-1}"
LATENCY_REQUESTS="${LATENCY_REQUESTS:-4}"
LOAD_JOBS="${LOAD_JOBS:-4}"
# ClickHouse gets requests = limits after DDL; leaves room for the webserver (2 CPU / 8Gi) on 8 vCPU / 32 GB.
CH_CPU="${CH_CPU:-4}"
CH_MEMORY="${CH_MEMORY:-16Gi}"
CAPRONI_DIR="$ROOT/scripts/ci/orbit-perf/caproni"
GKG_SELECTOR="app.kubernetes.io/name=gkg,app.kubernetes.io/component=webserver"
DISPATCHER_SELECTOR="app.kubernetes.io/name=gkg,app.kubernetes.io/component=dispatcher"

log() { echo "==> $*" >&2; }
# Debug build, so it shares the compile cache with the lint jobs that also build xtask.
xtask() { mise exec -- cargo run -p xtask -- "$@"; }
cap() { mise -C "$CAPRONI_DIR" exec -- caproni -c "$CAPRONI_DIR/caproni.yaml" "$@"; }
kc()  { cap kubectl "$@"; }
chq() { cap kubectl -n gitlab-dev-stack exec -i gitlab-dev-stack-clickhouse-0 -c clickhouse -- clickhouse-client "$@"; }

# Runner hardware, so a shift in numbers can be matched to a different machine.
CPU_MODEL="$(grep -m1 '^model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//')" || true
CPU_COUNT="$(nproc 2>/dev/null)" || true
MACHINE_TYPE="$(curl -fsS --max-time 2 -H 'Metadata-Flavor: Google' \
  http://169.254.169.254/computeMetadata/v1/instance/machine-type 2>/dev/null)" || true
MACHINE_TYPE="${MACHINE_TYPE##*/}"
RUNNER_INFO="${MACHINE_TYPE:-unknown} · ${CPU_COUNT:-?} vCPU · ${CPU_MODEL:-unknown CPU}"
log "runner: ${RUNNER_INFO}"

# ---------------------------------------------------------------------------
# 1. Start the stack in the background; it does not depend on the synth data.
# ---------------------------------------------------------------------------
log "[1/4] bringing up gitlab-dev-stack + Orbit in the background"
mise -C "$CAPRONI_DIR" install
# CI tests the commit's own image; local runs keep the tag in values/gkg.yaml.
if [ -n "${GKG_IMAGE_TAG:-}" ]; then
  sed -i "s|^  tag: .*|  tag: \"${GKG_IMAGE_TAG}\"|" "$CAPRONI_DIR/values/gkg.yaml"
  log "     gkg image tag: ${GKG_IMAGE_TAG}"
fi
# Blocks until the image is pushed (built in parallel by the pipeline's docker-build job).
wait_for_image() {
  [ -n "${GKG_IMAGE_TAG:-}" ] || return 0
  # Log in like the other registry scripts, so an auth failure can't pass for "not pushed yet".
  if [ -n "${CI_REGISTRY_PASSWORD:-}" ]; then
    echo "$CI_REGISTRY_PASSWORD" | mise -C "$CAPRONI_DIR" exec -- docker login -u "$CI_REGISTRY_USER" --password-stdin "$CI_REGISTRY" >/dev/null \
      || log "     docker login failed, continuing without credentials (anonymous reads work)"
  fi
  log "     waiting for image ${GKG_IMAGE}:${GKG_IMAGE_TAG}"
  local start=$SECONDS
  for i in $(seq 1 90); do
    if mise -C "$CAPRONI_DIR" exec -- docker manifest inspect "${GKG_IMAGE}:${GKG_IMAGE_TAG}" >/dev/null 2>&1; then
      if [ "$i" -eq 1 ]; then
        log "     image already pushed"
      else
        log "     image pushed after waiting $(( SECONDS - start ))s"
      fi
      return 0
    fi
    [ $(( i % 6 )) -eq 0 ] && log "     still waiting for image ($(( SECONDS - start ))s so far)"
    sleep 10
  done
  log "     image not pushed after 15 min; did the pipeline's docker-build job fail or not run?"
  return 1
}
# Once the webserver is Ready (DDL done, schema active): stop background work and
# pin ClickHouse's CPU and memory. Runs before the bulk load, so the restart loses nothing.
quiet_cluster() {
  kc -n gitlab scale deploy/gkg-dispatcher --replicas=0
  local gone=0
  for _ in $(seq 1 60); do
    if [ -z "$(kc -n gitlab get pod -l "$DISPATCHER_SELECTOR" -o name)" ]; then gone=1; break; fi
    sleep 2
  done
  [ "$gone" = 1 ] || { echo "dispatcher pod still present after 120s" >&2; return 1; }
  kc -n gitlab-dev-stack patch statefulset gitlab-dev-stack-clickhouse --type=json -p "[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/resources\",\"value\":{\"requests\":{\"cpu\":\"${CH_CPU}\",\"memory\":\"${CH_MEMORY}\"},\"limits\":{\"cpu\":\"${CH_CPU}\",\"memory\":\"${CH_MEMORY}\"}}}]"
  kc -n gitlab-dev-stack rollout status statefulset/gitlab-dev-stack-clickhouse --timeout=600s
  # The schema watch runs over NATS, so the webserver should stay Ready; check anyway.
  kc wait -n gitlab --for=condition=Ready pod -l "$GKG_SELECTOR" --timeout=300s
  # Refreshable views would refresh mid-test; STOP VIEWS does not survive a restart, so it goes last.
  chq -q "SYSTEM STOP VIEWS" </dev/null
}
UP_LOG="$ROOT/caproni-up.log"
# fd 3 keeps the image-wait messages in the live job log; the rest goes to UP_LOG.
exec 3>&2
(
  wait_for_image 2>&3
  cap --debug up
  kc wait -n gitlab --for=condition=Ready pod -l "$GKG_SELECTOR" --timeout=600s
  quiet_cluster
  cap update-etc-hosts --ip "$(getent hosts docker | awk '{print $1}')"
) >"$UP_LOG" 2>&1 &
UP_PID=$!

# ---------------------------------------------------------------------------
# 2. Build xtask + generate the synthetic graph (Parquet).
# ---------------------------------------------------------------------------
log "[2/4] generating synthetic graph ($SYNTH_CONFIG)"
xtask synth generate -c "$SYNTH_CONFIG" --force
OUT_DIR="$(grep -E '^\s*output_dir:' "$SYNTH_CONFIG" | head -1 | awk '{print $2}' | tr -d '"'"'"'')"
OUT_DIR="${OUT_DIR:-gl_synthetic_data}"
ORG_DIR="$ROOT/$OUT_DIR/org_1"
[ -d "$ORG_DIR" ] || { echo "synth output not found at $ORG_DIR" >&2; exit 1; }
log "     synth output: $ORG_DIR ($(ls "$ORG_DIR" | tr '\n' ' ')) "

# Authoritative parquet-file -> ClickHouse table map, straight from the ontology
# the generator used. filename = node_type.lower()+'.parquet'; table =
# destination_table (explicit) or the gl_<name> default. edges.parquet -> gl_edge.
# Pure shell (the rust build image ships no python3).
MAP_FILE="$ROOT/.synth_table_map"
{
  echo "edges=gl_edge"
  find "$ROOT/config/ontology/nodes" -name '*.yaml' | while read -r f; do
    nt="$(grep -m1 -E '^node_type:' "$f" | awk '{print $2}')"
    [ -n "$nt" ] || continue
    dt="$(grep -m1 -E '^destination_table:' "$f" | awk '{print $2}')"
    lc="$(printf '%s' "$nt" | tr '[:upper:]' '[:lower:]')"
    echo "${lc}=${dt:-gl_${lc}}"
  done
} > "$MAP_FILE"

log "     waiting for the stack"
if ! wait "$UP_PID"; then
  echo "caproni bring-up failed; log follows:" >&2
  cat "$UP_LOG" >&2
  exit 1
fi
cat "$UP_LOG" >&2

# Digest of the image the webserver actually runs, so the report names exact bits.
GKG_IMAGE_ID="$(kc -n gitlab get pod -l "$GKG_SELECTOR" \
  -o jsonpath='{.items[0].status.containerStatuses[?(@.name=="gkg-webserver")].imageID}')" || true
GKG_IMAGE_DIGEST="${GKG_IMAGE_ID##*@}"
log "     gkg webserver image: ${GKG_IMAGE_ID:-unknown}"

# ---------------------------------------------------------------------------
# 3. Bulk-load the Parquet into gkg's versioned ClickHouse tables.
# ---------------------------------------------------------------------------
log "[3/4] loading synthetic graph into gkg ClickHouse"
# Sort by version number, not lexically: a plain DESC ranks v9 above v72.
PFX=""
for _ in $(seq 1 60); do
  PFX="$(chq -q "SELECT name FROM system.tables WHERE database='gkg' AND name LIKE '%gl_file' ORDER BY toInt32OrZero(extract(name, '^v([0-9]+)_')) DESC LIMIT 1" 2>/dev/null | sed 's/_gl_file$//' | tr -d '[:space:]')" || true
  [ -n "$PFX" ] && break
  sleep 2
done
[ -n "$PFX" ] || { echo "could not discover gkg table prefix" >&2; exit 1; }
log "     discovered gkg table prefix: $PFX"

INSERT_SETTINGS="input_format_skip_unknown_fields=1, input_format_parquet_allow_missing_columns=1"
FAILED="$ROOT/.synth_load_failed"
: > "$FAILED"

# Streams one Parquet into the pod's clickhouse-client over `exec -i` stdin
# (no kubectl cp / --file, which vary across versions). Missing columns
# (_version/_deleted/*_tags) fall back to their DDL defaults.
load_one() {
  local pq="$1" tbl="$2"
  if chq --database gkg --query "INSERT INTO \`${tbl}\` SETTINGS ${INSERT_SETTINGS} FORMAT Parquet" < "$pq"; then
    log "     loaded $(basename "$pq") -> gkg.$tbl"
  else
    echo "$tbl" >> "$FAILED"
  fi
}

# Largest first so the edge table does not start last; LOAD_JOBS inserts at a time.
# shellcheck disable=SC2012  # generated lowercase names; ls -S gives size order
while read -r pq; do
  stem="$(basename "$pq" .parquet)"
  suffix="$(grep -E "^${stem}=" "$MAP_FILE" | head -1 | cut -d= -f2)"
  if [ -z "$suffix" ]; then
    log "     skip $stem.parquet (no table mapping)"
    continue
  fi
  while [ "$(jobs -rp | wc -l)" -ge "$LOAD_JOBS" ]; do wait -n || true; done
  log "     $stem.parquet ($(du -h "$pq" | cut -f1)) -> gkg.${PFX}_${suffix}"
  load_one "$pq" "${PFX}_${suffix}" &
done < <(ls -S "$ORG_DIR"/*.parquet)
wait
if [ -s "$FAILED" ]; then
  echo "failed to load: $(tr '\n' ' ' < "$FAILED")" >&2
  exit 1
fi

# Merge every table down to its final parts, so background merges cannot run
# during the load test and every run reads the same part layout.
# OPTIMIZE can outlast clickhouse-client's default 300s receive timeout.
OPT_FAILED="$ROOT/.synth_optimize_failed"
: > "$OPT_FAILED"
optimize_one() {
  local tbl="$1"
  if chq --receive_timeout=1800 -q "OPTIMIZE TABLE gkg.\`${tbl}\` FINAL" </dev/null; then
    log "     optimized gkg.$tbl"
  else
    echo "$tbl" >> "$OPT_FAILED"
  fi
}
# Captured first so a failed lookup fails the job instead of optimizing nothing.
TABLES="$(chq -q "SELECT name FROM system.tables WHERE database='gkg' AND startsWith(name, '${PFX}_') AND engine LIKE '%MergeTree' ORDER BY total_bytes DESC" </dev/null)"
[ -n "$TABLES" ] || { echo "no gkg.${PFX}_* tables to optimize" >&2; exit 1; }
opt_start=$SECONDS
while read -r tbl; do
  [ -n "$tbl" ] || continue
  while [ "$(jobs -rp | wc -l)" -ge "$LOAD_JOBS" ]; do wait -n || true; done
  optimize_one "$tbl" &
done <<< "$TABLES"
wait
if [ -s "$OPT_FAILED" ]; then
  echo "failed to optimize: $(tr '\n' ' ' < "$OPT_FAILED")" >&2
  exit 1
fi
log "     OPTIMIZE FINAL done in $(( SECONDS - opt_start ))s"

# Settled = no merges running and the active part count unchanged across two polls.
settle_start=$SECONDS
prev_parts=""
settled=0
for _ in $(seq 1 120); do
  merges="$(chq -q "SELECT count() FROM system.merges WHERE database='gkg'" </dev/null | tr -d '[:space:]')" || merges=""
  parts="$(chq -q "SELECT count() FROM system.parts WHERE database='gkg' AND active" </dev/null | tr -d '[:space:]')" || parts=""
  if [ "$merges" = 0 ] && [ -n "$parts" ] && [ "$parts" = "$prev_parts" ]; then
    settled=1
    break
  fi
  prev_parts="$parts"
  sleep 5
done
if [ "$settled" != 1 ]; then
  echo "ClickHouse merges did not settle within 10 min (merges=${merges:-?}, active parts=${parts:-?})" >&2
  exit 1
fi
log "     merges settled after $(( SECONDS - settle_start ))s (${parts} active parts)"

# Dictionaries (e.g. traversal paths, HASHED over project/group) cache their
# source tables; reload them so lookups see the freshly loaded rows.
DICTS="$(chq -q "SELECT name FROM system.dictionaries WHERE database='gkg' AND startsWith(name, '${PFX}_') ORDER BY name" </dev/null)"
[ -n "$DICTS" ] || log "     warning: no gkg.${PFX}_* dictionaries to reload"
while read -r dict; do
  [ -n "$dict" ] || continue
  chq -q "SYSTEM RELOAD DICTIONARY gkg.\`${dict}\`" </dev/null
  log "     reloaded dictionary gkg.$dict"
done <<< "$DICTS"

log "     row counts:"
for t in gl_merge_request gl_note gl_edge gl_project gl_group gl_user; do
  n="$(chq -q "SELECT count() FROM gkg.\`${PFX}_${t}\`" 2>/dev/null | tr -d '[:space:]' || echo '?')"
  log "       ${PFX}_${t} = ${n}"
done

# ---------------------------------------------------------------------------
# 4. Port-forward gkg gRPC + run the gRPC load test (xtask loadtest).
# ---------------------------------------------------------------------------
log "[4/4] running gRPC load test (rounds=$ROUNDS concurrency=$CONCURRENCY warmup_rounds=$WARMUP_ROUNDS seed=$SEED latency_concurrency=$LATENCY_CONCURRENCY latency_requests=$LATENCY_REQUESTS)"

kc -n gitlab port-forward svc/gkg-webserver 50054:50054 >/tmp/gkg-pf.log 2>&1 &
PF_PID=$!
# ClickHouse HTTP for the load test's server-side query stats; best effort, not checked.
kc -n gitlab-dev-stack port-forward svc/gitlab-dev-stack-clickhouse 8123:8123 >/tmp/ch-pf.log 2>&1 &
CH_PF_PID=$!
trap 'kill "$PF_PID" "$CH_PF_PID" 2>/dev/null || true' EXIT

# Fail fast rather than load-testing a dead endpoint.
ready=0
for _ in $(seq 1 30); do
  if ! kill -0 "$PF_PID" 2>/dev/null; then
    echo "port-forward exited early; log follows:" >&2
    cat /tmp/gkg-pf.log >&2
    exit 1
  fi
  if (exec 3<>/dev/tcp/127.0.0.1/50054) 2>/dev/null; then
    exec 3>&-
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != 1 ]; then
  echo "gRPC port 50054 never became reachable after 30s; log follows:" >&2
  cat /tmp/gkg-pf.log >&2
  exit 1
fi

export GKG_JWT_SECRET
GKG_JWT_SECRET="$(kc -n gitlab get secret gitlab-dev-stack-gkg-secrets -o jsonpath='{.data.gitlab-jwt-signing-key}' | base64 -d)"
[ -n "$GKG_JWT_SECRET" ] || { echo "could not read gkg JWT signing key" >&2; exit 1; }

# The image's only ClickHouse user (it replaces `default`); it reads system.query_log and can SYSTEM FLUSH LOGS.
export ORBIT_PERF_CLICKHOUSE_URL=http://127.0.0.1:8123 ORBIT_PERF_CLICKHOUSE_USER=gitlab-dev-stack ORBIT_PERF_CLICKHOUSE_PASSWORD
ORBIT_PERF_CLICKHOUSE_PASSWORD="$(kc -n gitlab get secret gitlab-dev-stack-gkg-secrets -o jsonpath='{.data.graph-password}' | base64 -d)" \
  || { ORBIT_PERF_CLICKHOUSE_PASSWORD=""; log "     could not read the ClickHouse password; server-side stats may be missing"; }

REPORT="$ROOT/loadtest-results.md"
{
  echo "## Orbit perf: gRPC load test"
  echo
  echo "Commit \`${CI_COMMIT_SHORT_SHA:-local}\` · image \`${GKG_IMAGE_TAG:-dev}\` (\`${GKG_IMAGE_DIGEST:-unknown}\`) · [job](${CI_JOB_URL:-}) · synth \`${SYNTH_CONFIG}\`"
  echo
  echo "Runner: ${RUNNER_INFO}"
  echo
} > "$REPORT"
xtask loadtest \
  --endpoint http://127.0.0.1:50054 \
  --rounds "$ROUNDS" \
  --concurrency "$CONCURRENCY" \
  --warmup-rounds "$WARMUP_ROUNDS" \
  --seed "$SEED" \
  --latency-concurrency "$LATENCY_CONCURRENCY" \
  --latency-requests "$LATENCY_REQUESTS" \
  | tee -a "$REPORT"

log "done. results in loadtest-results.md"
