#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Provisioning tier=${TIER} run=${RUN_ID}"

mkdir -p "${BENCH_DIR}/results/${RUN_ID}"

# --- 1. Standalone ClickHouse (PVC-backed, survives across runs) ---
CH_NS="ra-clickhouse"
CH_PASSWORD=$(openssl rand -base64 24)

log "Deploying standalone ClickHouse"
sed -e "s/\${CH_STORAGE}/$(tier '.clickhouse.storage')/g" \
    -e "s/\${CH_CPU}/$(tier '.clickhouse.cpu')/g" \
    -e "s/\${CH_MEMORY}/$(tier '.clickhouse.memory')/g" \
    -e "s/\${CH_PASSWORD}/${CH_PASSWORD}/g" \
  < "${BENCH_DIR}/manifests/standalone-ch.yaml" \
  | $KC apply -f -

$KC rollout status -n "${CH_NS}" statefulset/clickhouse --timeout=120s
log "  ClickHouse ready"

# Create databases and Siphon users (mirrors e2e chart initdb).
SIPHON_PASS=$(openssl rand -base64 24)
GITLAB_PASS=$(openssl rand -base64 24)
$KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASSWORD}" --multiquery --query "
    CREATE DATABASE IF NOT EXISTS datalake;
    CREATE DATABASE IF NOT EXISTS gkg;
    CREATE USER IF NOT EXISTS siphon IDENTIFIED BY '${SIPHON_PASS}';
    CREATE USER IF NOT EXISTS gitlab IDENTIFIED BY '${GITLAB_PASS}';
    GRANT ALL ON datalake.* TO siphon;
    GRANT ALL ON datalake.* TO gitlab;
  "

# --- 2. Siphon one-shot: create datalake tables + dictionaries ---
log "Running Siphon consumer one-shot to create datalake schemas"

# sync-cdc-tables.sh already ran during setup.sh and produced cdc-consumer.yaml.
# We use the same consumer config but point it at our standalone CH.
SIPHON_IMAGE=$(yq -r '.siphon.image.repository + ":" + .siphon.image.tag' "${E2E_DIR}/config/versions.yaml")
CONSUMER_CONFIG="${E2E_DIR}/config/cdc-consumer.yaml"

if [[ ! -f "${CONSUMER_CONFIG}" ]]; then
  log "  Generating CDC consumer config"
  "${E2E_DIR}/scripts/sync-cdc-tables.sh"
fi

# Build a minimal siphon config that only creates tables (no PG, no NATS).
SIPHON_CFG="/tmp/ra-siphon-config.yaml"
cat > "${SIPHON_CFG}" << SIPHONCFG
application_identifier: ra_bench_schema_init
clickhouse:
  host: clickhouse.${CH_NS}.svc.cluster.local
  port: 9000
  ssl: false
  username: siphon
  password: "${SIPHON_PASS}"
  database: datalake
  insert_batch_size: 10000
  max_open_conns: 5
  max_idle_conns: 3
$(cat "${CONSUMER_CONFIG}")
SIPHONCFG

$KC create configmap ra-siphon-init-config -n "${CH_NS}" \
  --from-file=config.yaml="${SIPHON_CFG}" \
  --dry-run=client -o yaml | $KC apply -f -
rm -f "${SIPHON_CFG}"

# Run the consumer. It creates tables at startup, then we kill it after 30s.
$KC delete pod ra-siphon-init -n "${CH_NS}" --ignore-not-found 2>/dev/null
cat <<EOF | $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ra-siphon-init
  namespace: ${CH_NS}
spec:
  restartPolicy: Never
  containers:
    - name: siphon
      image: ${SIPHON_IMAGE}
      command: ["/app/siphon", "consumer", "--config=/etc/config/config.yaml", "--log-level=info"]
      volumeMounts:
        - name: config
          mountPath: /etc/config
      resources:
        requests: { cpu: 250m, memory: 512Mi }
        limits: { memory: 1Gi }
  volumes:
    - name: config
      configMap:
        name: ra-siphon-init-config
EOF

log "  Waiting for Siphon to create tables (30s)..."
sleep 30
# The consumer will crash (no NATS/PG) but table creation happens at init.
$KC delete pod ra-siphon-init -n "${CH_NS}" --ignore-not-found 2>/dev/null

# Verify tables were created.
TABLE_COUNT=$($KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASSWORD}" \
  --query "SELECT count() FROM system.tables WHERE database='datalake' AND engine LIKE '%MergeTree%'" 2>/dev/null)
log "  Datalake tables created: ${TABLE_COUNT}"

# --- 3. Patch dictionaries ---
# patch-ch-dicts.sh expects NS_CH and uses clickhouse-0 pod name (StatefulSet).
log "Patching dictionaries"
NS_CH="${CH_NS}" "${E2E_DIR}/scripts/patch-ch-dicts.sh" || log "  Dict patch skipped (may not exist yet)"

# --- 4. GKG ClickHouse setup (users, grants) ---
log "Running GKG ClickHouse setup"
GKG_WRITER_PASS=$(openssl rand -base64 24)
GKG_READER_PASS=$(openssl rand -base64 24)
GKG_SIPHON_READER_PASS=$(openssl rand -base64 24)

SETUP_SQL=$(sed \
  -e "s/\${GRAPH_DB}/gkg/g" \
  -e "s/\${DATALAKE_DB}/datalake/g" \
  -e "s/\${GKG_WRITER_PASSWORD}/${GKG_WRITER_PASS}/g" \
  -e "s/\${GKG_READER_PASSWORD}/${GKG_READER_PASS}/g" \
  -e "s/\${GKG_SIPHON_READER_PASSWORD}/${GKG_SIPHON_READER_PASS}/g" \
  "${GKG_ROOT}/config/clickhouse-setup.sql")

$KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASSWORD}" --multiquery --query "${SETUP_SQL}"
log "  GKG users and grants created"

# Store credentials for the GKG helm values and the import Job.
$KC create secret generic ra-ch-credentials -n "${CH_NS}" \
  --from-literal=default-password="${CH_PASSWORD}" \
  --from-literal=gkg-writer-password="${GKG_WRITER_PASS}" \
  --from-literal=gkg-reader-password="${GKG_READER_PASS}" \
  --from-literal=gkg-siphon-reader-password="${GKG_SIPHON_READER_PASS}" \
  --dry-run=client -o yaml | $KC apply -f -

# --- 5. Node pool (if requested) ---
if [[ "${RA_EPHEMERAL_CLUSTER:-}" == "1" ]]; then
  log "Creating ephemeral cluster"
  "${BENCH_DIR}/scripts/create-cluster.sh"
  KCTX=$(cat "${BENCH_DIR}/results/${RUN_ID}/kctx")
  export KCTX
  KC="kubectl --context=${KCTX}"
  export KC
elif [[ "${RA_SKIP_POOL:-}" != "1" ]]; then
  MACHINE=$(tier ".nodes.machine")
  COUNT=$(tier ".nodes.count")
  POOL_NAME="ra-${RUN_ID}-${TIER}"

  log "Creating tainted node pool ${POOL_NAME} (${MACHINE} x${COUNT})"
  gcloud container node-pools create "${POOL_NAME}" \
    --cluster e2e-harness --zone us-central1-a \
    --machine-type "${MACHINE}" --num-nodes "${COUNT}" \
    --node-taints "ra-run=${RUN_ID}:NoSchedule" \
    --node-labels "ra-run=${RUN_ID}" \
    --labels "ttl-hours=12,run-id=${RUN_ID}" \
    --project gl-knowledgegraph-prj-f2eec59d \
    --quiet
else
  log "Skipping node pool creation (RA_SKIP_POOL=1)"
fi

# --- 6. Render tier overlay pointing datalake at standalone CH ---
log "Rendering tier overlay"
"${BENCH_DIR}/scripts/render-tier.sh" > "/tmp/ra-${RUN_ID}-tier-values.yaml"

# Append datalake override to point GKG at our standalone CH.
cat >> "/tmp/ra-${RUN_ID}-tier-values.yaml" <<EOF

datalake:
  url: "http://clickhouse.${CH_NS}.svc.cluster.local:8123"
  database: datalake
  username: gkg_siphon_reader
  password: "${GKG_SIPHON_READER_PASS}"
EOF

# --- 7. Deploy the rest of the stack ---
log "Deploying stack via e2e setup"
export E2E_SHA="${RUN_ID}"
export E2E_EXTRA_VALUES="/tmp/ra-${RUN_ID}-tier-values.yaml"
export E2E_CH_PERSISTENCE="true"
export E2E_CH_DISK_SIZE="$(tier '.clickhouse.storage')"
export E2E_CH_CPU="$(tier '.clickhouse.cpu')"
export E2E_CH_MEMORY="$(tier '.clickhouse.memory')"
export E2E_CH_CPU_LIMIT="$(tier '.clickhouse.cpu')"
export E2E_CH_MEMORY_LIMIT="$(tier '.clickhouse.memory')"
"${E2E_DIR}/scripts/setup.sh"

# --- 8. Validate ---
log "Running validation gate"
"${BENCH_DIR}/scripts/validate.sh"

log "Provision complete"
