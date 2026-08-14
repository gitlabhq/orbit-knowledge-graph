#!/usr/bin/env bash
# Dump project IDs and full paths from the datalake for code corpus fetching.
# Output: TSV to stdout (project_id \t full_path)
# Usage: KCTX=... RUN_ID=bench7 bash bench/scripts/dump-project-list.sh > projects.tsv
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CH_NS="${E2E_CH_NAMESPACE:-ra-ch-${RUN_ID}}"

$KC exec -n "${CH_NS}" clickhouse-0 -- clickhouse-client -q "
SELECT
    p.id AS project_id,
    r.path AS full_path
FROM gkg.v88_gl_project p FINAL
JOIN datalake.siphon_routes r FINAL ON r.source_id = p.id AND r.source_type = 'Project'
WHERE NOT p._deleted AND NOT r._siphon_deleted
ORDER BY p.id
FORMAT TSV
"
