#!/usr/bin/env bash
# PoC orchestrator for the orbit-perf-poc CI job (see .gitlab/ci/orbit-perf-poc.yml).
#
# Brings up a real caproni + Orbit stack, seeds gkg's ClickHouse with an
# xtask-generated synthetic graph (bulk Parquet load, bypassing siphon/seed/
# indexing), and runs the direct gRPC load test against gkg.
#
# Env (from the CI job): SYNTH_CONFIG, CAPRONI_REF, ROUNDS, CONCURRENCY.
set -euo pipefail

ROOT="$(pwd)"                                   # knowledge-graph checkout (xtask lives here)
SYNTH_CONFIG="${SYNTH_CONFIG:-crates/xtask/simulator_small.yaml}"
CAPRONI_REF="${CAPRONI_REF:-main}"
ROUNDS="${ROUNDS:-5}"
CONCURRENCY="${CONCURRENCY:-20}"
CAPRONI_REPO="https://gitlab-ci-token:${CI_JOB_TOKEN:-}@gitlab.com/gitlab-org/gitlab-caproni.git"
LOADTEST_REPO="https://gitlab.com/gitlab-org/orbit/experiments/load-testing.git"

log() { echo "==> $*" >&2; }

# ---------------------------------------------------------------------------
# 1. Build xtask + generate the synthetic graph (Parquet). Runs in this repo,
#    using the mise-managed rust toolchain already installed by before_script.
# ---------------------------------------------------------------------------
log "[1/4] generating synthetic graph ($SYNTH_CONFIG)"
mise exec -- cargo xtask synth generate -c "$SYNTH_CONFIG" --force
OUT_DIR="$(grep -E '^\s*output_dir:' "$SYNTH_CONFIG" | head -1 | awk '{print $2}' | tr -d '"'"'"'')"
OUT_DIR="${OUT_DIR:-gl_synthetic_data}"
ORG_DIR="$ROOT/$OUT_DIR/org_1"
[ -d "$ORG_DIR" ] || { echo "synth output not found at $ORG_DIR" >&2; exit 1; }
log "     synth output: $ORG_DIR ($(ls "$ORG_DIR" | tr '\n' ' ')) "

# Authoritative parquet-file -> ClickHouse table map, straight from the ontology
# the generator used. filename = node_type.lower()+'.parquet'; table =
# destination_table (explicit) or the gl_<name> default. edges.parquet -> gl_edge.
MAP_FILE="$ROOT/.synth_table_map"
python3 - "$ROOT/config/ontology/nodes" "$MAP_FILE" <<'PY'
import glob, os, re, sys
nodes_dir, out = sys.argv[1], sys.argv[2]
rows = ["edges=gl_edge"]
for f in glob.glob(os.path.join(nodes_dir, "**", "*.yaml"), recursive=True):
    txt = open(f).read()
    nt = re.search(r'^node_type:\s*(\S+)', txt, re.M)
    if not nt:
        continue
    name = nt.group(1)
    dt = re.search(r'^destination_table:\s*(\S+)', txt, re.M)
    table = dt.group(1) if dt else "gl_" + name.lower()
    rows.append(f"{name.lower()}={table}")
open(out, "w").write("\n".join(rows) + "\n")
PY

# ---------------------------------------------------------------------------
# 2. Bring up caproni + Orbit (real stack). Reuses caproni's proven recipe; no
#    license / siphon / seed (the synthetic bulk-load replaces all of that).
# ---------------------------------------------------------------------------
log "[2/4] bringing up caproni + Orbit (ref=$CAPRONI_REF)"
rm -rf "$ROOT/_caproni"
git clone --depth 1 -b "$CAPRONI_REF" "$CAPRONI_REPO" "$ROOT/_caproni"
(
  cd "$ROOT/_caproni"
  mise install
  eval "$(mise activate bash --shims)"
  scripts/fetch-ch-scheme.sh
  printf 'extends: [fragments/caproni.orbit.yaml]\n' >> caproni.local.yaml
  caproni --debug up
  caproni kubectl wait -n gitlab --for=condition=Ready --selector="app=webservice" pods --timeout=600s
  caproni update-etc-hosts --ip "$(getent hosts docker | awk '{print $1}')"
)

# Helpers that run caproni from the clone.
CAP="cd $ROOT/_caproni && eval \"\$(mise activate bash --shims)\" &&"
kc()  { bash -c "$CAP caproni kubectl \"\$@\"" _ "$@"; }
chq() { bash -c "$CAP caproni kubectl -n gitlab-dev-stack exec -i gitlab-dev-stack-clickhouse-0 -c clickhouse -- clickhouse-client \"\$@\"" _ "$@"; }

# ---------------------------------------------------------------------------
# 3. Bulk-load the Parquet into gkg's versioned ClickHouse tables.
# ---------------------------------------------------------------------------
log "[3/4] loading synthetic graph into gkg ClickHouse"
PFX="$(chq -q "SELECT name FROM system.tables WHERE database='gkg' AND name LIKE '%gl_file' ORDER BY name DESC LIMIT 1" | sed 's/_gl_file$//' | tr -d '[:space:]')"
[ -n "$PFX" ] || { echo "could not discover gkg table prefix" >&2; exit 1; }
log "     discovered gkg table prefix: $PFX"

INSERT_SETTINGS="input_format_skip_unknown_fields=1, input_format_parquet_allow_missing_columns=1"

# Stream each Parquet into the pod's clickhouse-client over `exec -i` stdin
# (no kubectl cp / --file, which vary across versions). Missing columns
# (_version/_deleted/*_tags) fall back to their DDL defaults.
for pq in "$ORG_DIR"/*.parquet; do
  stem="$(basename "$pq" .parquet)"
  suffix="$(grep -E "^${stem}=" "$MAP_FILE" | head -1 | cut -d= -f2)"
  if [ -z "$suffix" ]; then
    log "     skip $stem.parquet (no table mapping)"
    continue
  fi
  tbl="${PFX}_${suffix}"
  log "     $stem.parquet ($(du -h "$pq" | cut -f1)) -> gkg.$tbl"
  chq --database gkg --query "INSERT INTO \`${tbl}\` SETTINGS ${INSERT_SETTINGS} FORMAT Parquet" < "$pq"
done

# Traversal-path dictionaries are HASHED over the project/group tables; reload
# them so traversals resolve against the freshly loaded rows.
chq -q "SYSTEM RELOAD DICTIONARY gkg.\`${PFX}_gl_project_traversal_paths_dict\`" || true
chq -q "SYSTEM RELOAD DICTIONARY gkg.\`${PFX}_gl_group_traversal_paths_dict\`" || true

log "     row counts:"
for t in gl_merge_request gl_note gl_edge gl_project gl_group gl_user; do
  n="$(chq -q "SELECT count() FROM gkg.\`${PFX}_${t}\`" 2>/dev/null | tr -d '[:space:]' || echo '?')"
  log "       ${PFX}_${t} = ${n}"
done

# ---------------------------------------------------------------------------
# 4. Port-forward gkg gRPC + run the direct gRPC load test.
# ---------------------------------------------------------------------------
log "[4/4] running gRPC load test (rounds=$ROUNDS concurrency=$CONCURRENCY)"
rm -rf "$ROOT/_loadtest"
git clone --depth 1 "$LOADTEST_REPO" "$ROOT/_loadtest"
# Use the vendored 0.83.1-adapted test (kept in this repo so the PoC is
# self-contained and doesn't depend on any unmerged caproni branch).
cp "$ROOT/scripts/ci/orbit-perf/direct_grpc_load_test.py" "$ROOT/_loadtest/src/grpc/direct_grpc_load_test.py"

bash -c "$CAP caproni kubectl -n gitlab port-forward svc/gkg-webserver 50054:50054" >/tmp/gkg-pf.log 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  (exec 3<>/dev/tcp/127.0.0.1/50054) 2>/dev/null && { exec 3>&-; break; }
  sleep 1
done

export GKG_JWT_SECRET
GKG_JWT_SECRET="$(kc -n gitlab get secret gitlab-dev-stack-gkg-secrets -o jsonpath='{.data.gitlab-jwt-signing-key}' | base64 -d)"
[ -n "$GKG_JWT_SECRET" ] || { echo "could not read gkg JWT signing key" >&2; exit 1; }

(
  cd "$ROOT/_loadtest"
  mise trust
  mise install
  mise run install
  mise run grpc:python -- --endpoint 127.0.0.1:50054 --rounds "$ROUNDS" --concurrency "$CONCURRENCY"
) | tee "$ROOT/loadtest-results.md"

log "done. results in loadtest-results.md"
