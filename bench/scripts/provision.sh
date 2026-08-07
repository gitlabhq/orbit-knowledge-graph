#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Provisioning tier=${TIER} run=${RUN_ID}"
mkdir -p "${BENCH_DIR}/results/${RUN_ID}"

CH_NS="${E2E_CH_NAMESPACE:-ra-clickhouse}"

# --- 1. Clean up previous e2e namespaces for this run ---
log "Cleaning up previous e2e-${RUN_ID}-* namespaces"
for ns in $($KC get ns -o name 2>/dev/null | grep "e2e-${RUN_ID}-" | sed 's|namespace/||'); do
  log "  deleting ${ns}"
  $KC delete ns "${ns}" --wait=false 2>/dev/null || true
done
# Wait for them to actually disappear before redeploying.
for ns in $($KC get ns -o name 2>/dev/null | grep "e2e-${RUN_ID}-" | sed 's|namespace/||'); do
  $KC wait --for=delete "ns/${ns}" --timeout=120s 2>/dev/null || true
done

# --- 2. Standalone ClickHouse (PVC-backed, survives across runs) ---
CH_PASSWORD=$(openssl rand -hex 24)

log "Deploying standalone ClickHouse in ${CH_NS}"
sed -e "s|\${CH_STORAGE}|$(tier '.clickhouse.storage')|g" \
    -e "s|\${CH_CPU}|$(tier '.clickhouse.cpu')|g" \
    -e "s|\${CH_MEMORY}|$(tier '.clickhouse.memory')|g" \
    -e "s|\${CH_PASSWORD}|${CH_PASSWORD}|g" \
  < "${BENCH_DIR}/manifests/standalone-ch.yaml" \
  | $KC apply -f -

$KC rollout status -n "${CH_NS}" statefulset/clickhouse --timeout=120s
log "  ClickHouse ready"

# --- 3. Create databases and users ---
# All passwords are hex (no special chars that break sed or SQL quoting).
export E2E_CH_SIPHON_PASS=$(openssl rand -hex 24)
export E2E_CH_GITLAB_PASS=$(openssl rand -hex 24)
export E2E_CH_DATALAKE_PASS=$(openssl rand -hex 24)
export E2E_CH_GRAPH_PASS=$(openssl rand -hex 24)
export E2E_CH_GRAPH_READ_PASS=$(openssl rand -hex 24)

SETUP_SQL=$(sed \
  -e "s|\${GRAPH_DB}|gkg|g" \
  -e "s|\${DATALAKE_DB}|datalake|g" \
  -e "s|\${GKG_WRITER_PASSWORD}|${E2E_CH_GRAPH_PASS}|g" \
  -e "s|\${GKG_READER_PASSWORD}|${E2E_CH_GRAPH_READ_PASS}|g" \
  -e "s|\${GKG_SIPHON_READER_PASSWORD}|${E2E_CH_DATALAKE_PASS}|g" \
  "${REPO_ROOT}/config/clickhouse-setup.sql")

$KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASSWORD}" --multiquery --query "
    DROP DATABASE IF EXISTS datalake;
    DROP DATABASE IF EXISTS gkg;
    CREATE DATABASE datalake;
    CREATE DATABASE gkg;
    DROP USER IF EXISTS siphon;
    DROP USER IF EXISTS gitlab;
    DROP USER IF EXISTS gkg_writer;
    DROP USER IF EXISTS gkg_reader;
    DROP USER IF EXISTS gkg_siphon_reader;
    CREATE USER siphon IDENTIFIED BY '${E2E_CH_SIPHON_PASS}';
    CREATE USER gitlab IDENTIFIED BY '${E2E_CH_GITLAB_PASS}';
    GRANT ALL ON datalake.* TO siphon;
    GRANT ALL ON datalake.* TO gitlab;
    ${SETUP_SQL}
  "
log "  Databases and users created (clean slate)"

# Store the default password for the import job.
$KC create secret generic ra-ch-credentials -n "${CH_NS}" \
  --from-literal=default-password="${CH_PASSWORD}" \
  --dry-run=client -o yaml | $KC apply -f -

# --- 4. Render tier overlay ---
log "Rendering tier overlay"
"${BENCH_DIR}/scripts/render-tier.sh" > "/tmp/ra-${RUN_ID}-tier-values.yaml"

# --- 5. Deploy e2e stack pointing at our standalone CH ---
log "Deploying e2e stack (GitLab takes ~5 min, Siphon follows)"
export E2E_SHA="${RUN_ID}"
export E2E_CH_HOST="clickhouse.${CH_NS}.svc.cluster.local"
export E2E_CH_NAMESPACE="${CH_NS}"
export E2E_CH_DEFAULT_PASS="${CH_PASSWORD}"
export E2E_EXTRA_VALUES="/tmp/ra-${RUN_ID}-tier-values.yaml"

"${E2E_DIR}/scripts/setup.sh" || log "WARN: setup.sh exited non-zero (license activation may have failed, non-fatal)"

# --- 6. Import datalake dump ---
log "Importing datalake dump"
"${BENCH_DIR}/scripts/import-dump-job.sh"

# --- 7. Reset dispatcher checkpoints to epoch so it re-indexes all dump data ---
# The dump preserves original _siphon_watermark timestamps. Without this
# the dispatcher's cursor starts at now() and never sees the old data.
log "Resetting dispatcher checkpoints to epoch"
CKPT=$($KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASSWORD}" -d gkg \
  --query "SELECT name FROM system.tables WHERE database='gkg' AND name LIKE '%_checkpoint' AND name NOT LIKE '%code_indexing%' LIMIT 1" 2>/dev/null)
if [[ -n "${CKPT}" ]]; then
  $KC exec -n "${CH_NS}" clickhouse-0 -- \
    clickhouse-client --password "${CH_PASSWORD}" -d gkg --multiquery \
    --query "TRUNCATE TABLE ${CKPT};
      INSERT INTO ${CKPT} (key, watermark) VALUES
        ('dispatch.sdlc.namespace.changes', '1970-01-01 00:00:00'),
        ('dispatch.sdlc.namespace.sweep', '1970-01-01 00:00:00')"
  log "  Set ${CKPT} to epoch"
fi
$KC rollout restart -n "e2e-${RUN_ID}-gkg" deploy/gkg-dispatcher deploy/gkg-indexer-default
$KC rollout status -n "e2e-${RUN_ID}-gkg" deploy/gkg-dispatcher --timeout=120s
log "  Dispatcher and indexer restarted"

# --- 8. Validate ---
log "Running validation gate"
"${BENCH_DIR}/scripts/validate.sh"

log "Provision complete"
