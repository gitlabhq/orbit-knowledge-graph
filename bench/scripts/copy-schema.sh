#!/usr/bin/env bash
# Copy datalake table schemas and dictionaries from the e2e ClickHouse
# (where Siphon created them) to the standalone bench ClickHouse.
# Runs as a Pod inside the cluster using clickhouse-client.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

SRC_NS="e2e-${RUN_ID}-clickhouse"
DST_NS="ra-clickhouse"

SRC_PASS=$($KC get statefulset clickhouse -n "${SRC_NS}" \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLICKHOUSE_PASSWORD")].value}')
DST_PASS=$($KC get statefulset clickhouse -n "${DST_NS}" \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLICKHOUSE_PASSWORD")].value}')

log "Copying datalake schemas from ${SRC_NS} to ${DST_NS}"

$KC delete pod ra-schema-copy -n "${DST_NS}" --ignore-not-found 2>/dev/null

cat > /tmp/ra-schema-copy.sh <<COPYEOF
#!/bin/bash
set -euo pipefail

SRC_HOST="clickhouse.${SRC_NS}.svc.cluster.local"
DST_HOST="clickhouse.${DST_NS}.svc.cluster.local"
SRC_PASS="${SRC_PASS}"
DST_PASS="${DST_PASS}"

src() { clickhouse-client --host "\$SRC_HOST" --password "\$SRC_PASS" "\$@"; }
dst() { clickhouse-client --host "\$DST_HOST" --password "\$DST_PASS" "\$@"; }

echo "==> Copying dictionaries"
for dict in \$(src --query "SELECT name FROM system.dictionaries WHERE database='datalake' FORMAT TSV"); do
  DDL=\$(src --query "SHOW CREATE DICTIONARY datalake.\${dict} FORMAT TSVRaw")
  DDL=\$(echo "\$DDL" | sed "s/PASSWORD '\\[HIDDEN\\]'/PASSWORD '\${DST_PASS}'/g; s/USER 'siphon'/USER 'default'/g; s/USER 'gitlab'/USER 'default'/g")
  echo "\$DDL" | dst --multiquery && echo "  \${dict}: OK" || echo "  \${dict}: FAILED"
done

echo "==> Copying table schemas"
for tbl in \$(src --query "SELECT name FROM system.tables WHERE database='datalake' AND engine LIKE '%MergeTree%' FORMAT TSV"); do
  DDL=\$(src --query "SHOW CREATE TABLE datalake.\${tbl} FORMAT TSVRaw")
  echo "\$DDL" | dst --multiquery 2>/dev/null && echo "  \${tbl}: OK" || echo "  \${tbl}: FAILED"
done

echo "==> Result: \$(dst --query "SELECT count() FROM system.tables WHERE database='datalake' AND engine LIKE '%MergeTree%'") tables"
echo "==> DONE"
COPYEOF

$KC create configmap ra-schema-copy-script -n "${DST_NS}" \
  --from-file=copy.sh=/tmp/ra-schema-copy.sh \
  --dry-run=client -o yaml | $KC apply -f -
rm -f /tmp/ra-schema-copy.sh

cat <<EOF | $KC apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ra-schema-copy
  namespace: ${DST_NS}
spec:
  restartPolicy: Never
  containers:
    - name: copy
      image: clickhouse/clickhouse-server:26.6
      command: ["bash", "/scripts/copy.sh"]
      volumeMounts:
        - name: scripts
          mountPath: /scripts
  volumes:
    - name: scripts
      configMap:
        name: ra-schema-copy-script
EOF

log "Waiting for schema copy to complete..."
$KC wait -n "${DST_NS}" pod/ra-schema-copy --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s 2>/dev/null || {
  log "Schema copy pod did not succeed:"
  $KC logs ra-schema-copy -n "${DST_NS}" 2>/dev/null | grep -E "FAILED|Error" | head -5
}

TABLE_COUNT=$($KC exec -n "${DST_NS}" clickhouse-0 -- \
  clickhouse-client --password "${DST_PASS}" \
  --query "SELECT count() FROM system.tables WHERE database='datalake' AND engine LIKE '%MergeTree%'" 2>/dev/null)
log "Standalone CH datalake tables: ${TABLE_COUNT}"

$KC delete pod ra-schema-copy -n "${DST_NS}" --ignore-not-found 2>/dev/null
