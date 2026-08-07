#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Provisioning tier=${TIER} run=${RUN_ID}"
mkdir -p "${BENCH_DIR}/results/${RUN_ID}"

# --- 1. Standalone ClickHouse (PVC-backed, survives across runs) ---
CH_NS="${E2E_CH_NAMESPACE:-ra-clickhouse}"
CH_PASSWORD=$(openssl rand -base64 24)

log "Deploying standalone ClickHouse in ${CH_NS}"
sed -e "s|\${CH_STORAGE}|$(tier '.clickhouse.storage')|g" \
    -e "s|\${CH_CPU}|$(tier '.clickhouse.cpu')|g" \
    -e "s|\${CH_MEMORY}|$(tier '.clickhouse.memory')|g" \
    -e "s|\${CH_PASSWORD}|${CH_PASSWORD}|g" \
  < "${BENCH_DIR}/manifests/standalone-ch.yaml" \
  | $KC apply -f -

$KC rollout status -n "${CH_NS}" statefulset/clickhouse --timeout=120s
log "  ClickHouse ready"

# --- 2. Create databases and users for the e2e stack ---
E2E_CH_SIPHON_PASS=$(openssl rand -base64 24)
E2E_CH_GITLAB_PASS=$(openssl rand -base64 24)
E2E_CH_DATALAKE_PASS=$(openssl rand -base64 24)
E2E_CH_GRAPH_PASS=$(openssl rand -base64 24)
E2E_CH_GRAPH_READ_PASS=$(openssl rand -base64 24)

SETUP_SQL=$(sed \
  -e "s/\${GRAPH_DB}/gkg/g" \
  -e "s/\${DATALAKE_DB}/datalake/g" \
  -e "s/\${GKG_WRITER_PASSWORD}/${E2E_CH_GRAPH_PASS}/g" \
  -e "s/\${GKG_READER_PASSWORD}/${E2E_CH_GRAPH_READ_PASS}/g" \
  -e "s/\${GKG_SIPHON_READER_PASSWORD}/${E2E_CH_DATALAKE_PASS}/g" \
  "${GKG_ROOT}/config/clickhouse-setup.sql")

$KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASSWORD}" --multiquery --query "
    CREATE DATABASE IF NOT EXISTS datalake;
    CREATE DATABASE IF NOT EXISTS gkg;
    CREATE USER IF NOT EXISTS siphon IDENTIFIED BY '${E2E_CH_SIPHON_PASS}';
    CREATE USER IF NOT EXISTS gitlab IDENTIFIED BY '${E2E_CH_GITLAB_PASS}';
    GRANT ALL ON datalake.* TO siphon;
    GRANT ALL ON datalake.* TO gitlab;
    ${SETUP_SQL}
  "
log "  Databases and users created"

# --- 3. Render tier overlay ---
log "Rendering tier overlay"
"${BENCH_DIR}/scripts/render-tier.sh" > "/tmp/ra-${RUN_ID}-tier-values.yaml"

# --- 4. Deploy e2e stack pointing at our standalone CH ---
log "Deploying e2e stack"
CH_HOST="clickhouse.${CH_NS}.svc.cluster.local"

export E2E_CH_HOST="${CH_HOST}"
export E2E_CH_NAMESPACE="${CH_NS}"
export E2E_CH_DEFAULT_PASS="${CH_PASSWORD}"
export E2E_CH_SIPHON_PASS
export E2E_CH_GITLAB_PASS
export E2E_CH_DATALAKE_PASS
export E2E_CH_GRAPH_PASS
export E2E_CH_GRAPH_READ_PASS
export E2E_EXTRA_VALUES="/tmp/ra-${RUN_ID}-tier-values.yaml"

"${E2E_DIR}/scripts/setup.sh"

# --- 5. Validate ---
log "Running validation gate"
"${BENCH_DIR}/scripts/validate.sh"

log "Provision complete"
