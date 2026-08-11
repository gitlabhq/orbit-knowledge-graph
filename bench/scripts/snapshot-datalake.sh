#!/usr/bin/env bash
# Snapshot a loaded CH PVC for fast cloning on future runs.
# Usage: KCTX=... RUN_ID=bench5 bash bench/scripts/snapshot-datalake.sh
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

: "${CH_NS:=ra-ch-${RUN_ID}}"
: "${SNAPSHOT_NAME:=golden-${DUMP_PREFIX:-datalake}}"

log "Snapshotting PVC data-clickhouse-0 in ${CH_NS} as ${SNAPSHOT_NAME}"

cat <<EOF | $KC apply -f -
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: ${SNAPSHOT_NAME}
  namespace: ${CH_NS}
spec:
  source:
    persistentVolumeClaimName: data-clickhouse-0
EOF

log "Waiting for snapshot to be ready..."
$KC wait -n "${CH_NS}" volumesnapshot/"${SNAPSHOT_NAME}" \
  --for=jsonpath='{.status.readyToUse}'=true --timeout=600s

log "Snapshot ${SNAPSHOT_NAME} ready."
