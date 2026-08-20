#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Provisioning tier=${TIER} run=${RUN_ID}"
mkdir -p "${BENCH_DIR}/results/${RUN_ID}"

CH_NS="${E2E_CH_NAMESPACE:-ra-ch-${RUN_ID}}"

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

# --- 2. Dedicated node pool for CH (optional) ---
CH_NODE_SELECTOR=""
CH_TOLERATIONS=""
if [[ "${RA_DEDICATED_POOL:-}" == "1" ]]; then
  IFS=_ read -r _ GKE_PROJECT GKE_ZONE GKE_CLUSTER <<< "${KCTX}"
  POOL_NAME="ra-${RUN_ID}"
  log "Creating dedicated pool ${POOL_NAME}"
  if ! gcloud container node-pools describe "${POOL_NAME}" \
      --cluster "${GKE_CLUSTER}" --project "${GKE_PROJECT}" --zone "${GKE_ZONE}" \
      >/dev/null 2>&1; then
    gcloud container node-pools create "${POOL_NAME}" \
      --cluster "${GKE_CLUSTER}" --project "${GKE_PROJECT}" --zone "${GKE_ZONE}" \
      --machine-type "$(tier '.nodes.machine')" --num-nodes 1 \
      --node-taints "ra-run=${RUN_ID}:NoSchedule" \
      --node-labels "ra-run=${RUN_ID}" --quiet
  else
    log "  Pool ${POOL_NAME} already exists, reusing"
  fi
  CH_NODE_SELECTOR="      nodeSelector:
        ra-run: \"${RUN_ID}\""
  CH_TOLERATIONS="      tolerations:
        - key: ra-run
          operator: Equal
          value: \"${RUN_ID}\"
          effect: NoSchedule"
fi

# --- 3. Standalone ClickHouse (PVC-backed, survives across runs) ---
CH_PASSWORD=$(openssl rand -hex 24)

PVC_DATA_SOURCE=""
if [[ -n "${RA_DATALAKE_SNAPSHOT:-}" ]]; then
  # VolumeSnapshots are namespace-scoped. To restore across namespaces we
  # create a pre-provisioned VolumeSnapshotContent + VolumeSnapshot pair
  # that references the same underlying GCE disk snapshot.
  ORIG_CONTENT=$($KC get volumesnapshot "${RA_DATALAKE_SNAPSHOT}" \
    -n "${RA_DATALAKE_SNAPSHOT_NS:-ra-clickhouse}" \
    -o jsonpath='{.status.boundVolumeSnapshotContentName}')
  export SNAP_HANDLE=$($KC get volumesnapshotcontent "${ORIG_CONTENT}" \
    -o jsonpath='{.status.snapshotHandle}')
  export LOCAL_SNAP="datalake-${RUN_ID}"
  export LOCAL_CONTENT="datalake-content-${RUN_ID}"
  export CH_NAMESPACE="${CH_NS}"
  $KC create ns "${CH_NS}" --dry-run=client -o yaml | $KC apply -f -
  envsubst < "${BENCH_DIR}/manifests/snapshot-restore.yaml" | $KC apply -f -
  log "  Waiting for snapshot ${LOCAL_SNAP} to bind"
  $KC wait -n "${CH_NS}" volumesnapshot/"${LOCAL_SNAP}" \
    --for=jsonpath='{.status.readyToUse}'=true --timeout=60s
  PVC_DATA_SOURCE="        dataSource:
          name: ${LOCAL_SNAP}
          kind: VolumeSnapshot
          apiGroup: snapshot.storage.k8s.io"
fi

# StatefulSet volumeClaimTemplates are immutable; delete before re-applying.
$KC delete statefulset clickhouse -n "${CH_NS}" --cascade=orphan 2>/dev/null || true

log "Deploying standalone ClickHouse in ${CH_NS}"
export CH_NAMESPACE="${CH_NS}" CH_PASSWORD CH_NODE_SELECTOR CH_TOLERATIONS PVC_DATA_SOURCE
export CH_IMAGE="$(bench '.images.clickhouse')"
export CH_STORAGE="$(tier '.clickhouse.storage')"
export CH_CPU="$(tier '.clickhouse.cpu')"
export CH_MEMORY="$(tier '.clickhouse.memory')"
envsubst < "${BENCH_DIR}/manifests/standalone-ch.yaml" | $KC apply -f -

$KC rollout status -n "${CH_NS}" statefulset/clickhouse --timeout=120s
log "  ClickHouse ready"

# --- 4. Create databases and users ---
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

DROP_DATALAKE="DROP DATABASE IF EXISTS datalake; CREATE DATABASE datalake;"
if [[ -n "${RA_DATALAKE_SNAPSHOT:-}" ]]; then
  DROP_DATALAKE=""
fi

$KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASSWORD}" --multiquery --query "
    ${DROP_DATALAKE}
    DROP DATABASE IF EXISTS gkg;
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

# Set TTLs on system log tables to prevent disk exhaustion.
log "Setting TTLs on system log tables"
for tbl in text_log trace_log query_log metric_log asynchronous_metric_log processors_profile_log part_log; do
  $KC exec -n "${CH_NS}" clickhouse-0 -- \
    clickhouse-client --password "${CH_PASSWORD}" -q \
    "ALTER TABLE system.${tbl} MODIFY TTL event_date + INTERVAL 1 DAY SETTINGS mutations_sync=0" 2>/dev/null || true
done
# Truncate any existing bloat from snapshot restores.
$KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASSWORD}" --multiquery -q "
    TRUNCATE TABLE IF EXISTS system.text_log;
    TRUNCATE TABLE IF EXISTS system.processors_profile_log;
    TRUNCATE TABLE IF EXISTS system.trace_log;
  " 2>/dev/null || true

# Store the default password for the import job.
$KC create secret generic ra-ch-credentials -n "${CH_NS}" \
  --from-literal=default-password="${CH_PASSWORD}" \
  --dry-run=client -o yaml | $KC apply -f -

# --- 5. Render tier overlay ---
log "Rendering tier overlay"
"${BENCH_DIR}/scripts/render-tier.sh" > "/tmp/ra-${RUN_ID}-tier-values.yaml"

# --- 6. Deploy e2e stack pointing at our standalone CH ---
log "Deploying e2e stack (GitLab takes ~5 min, Siphon follows)"
export E2E_SHA="${RUN_ID}"
export E2E_CH_HOST="clickhouse.${CH_NS}.svc.cluster.local"
export E2E_CH_NAMESPACE="${CH_NS}"
export E2E_CH_DEFAULT_PASS="${CH_PASSWORD}"
export E2E_EXTRA_VALUES="/tmp/ra-${RUN_ID}-tier-values.yaml"

"${E2E_DIR}/scripts/setup.sh" || log "WARN: setup.sh exited non-zero (license activation may have failed, non-fatal)"

# --- 7. Import datalake dump (or restore from snapshot) ---
if [[ -n "${RA_DATALAKE_SNAPSHOT:-}" ]]; then
  log "Skipping import: PVC restored from snapshot ${RA_DATALAKE_SNAPSHOT}"
else
  log "Importing datalake dump"
  CH_NS="${CH_NS}" "${BENCH_DIR}/scripts/import-dump-job.sh"
fi

# --- 8. Reset dispatcher checkpoints to epoch so it re-indexes all dump data ---
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

# --- 9. Enable GMP metrics scraping for GKG pods ---
log "Creating GMP PodMonitoring resources"
export GKG_NAMESPACE="e2e-${RUN_ID}-gkg"
for comp in dispatcher indexer webserver; do
  COMPONENT="${comp}" envsubst < "${BENCH_DIR}/manifests/pod-monitoring.yaml" | $KC apply -f -
done
log "  GMP scraping GKG pods every 15s"

# --- 10. Validate ---
log "Running validation gate"
"${BENCH_DIR}/scripts/validate.sh"

log "Provision complete"
