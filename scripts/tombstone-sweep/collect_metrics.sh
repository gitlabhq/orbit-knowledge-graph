#!/usr/bin/env bash
# Pulls authoritative duration/read/memory per probe phase from
# system.query_log across all replicas, keyed by the tsp:<tier>:<table>:<phase>
# log_comment tags. Reads through orbit-chq (sql-console), which is the only
# user able to see query_log.
set -u
SINCE=${1:-6}
ORBIT_CHQ_TIMEOUT=110 orbit-chq orbit-prod "
SELECT Settings['log_comment'] AS tag,
       max(query_duration_ms) AS duration_ms,
       max(read_rows) AS read_rows,
       formatReadableSize(max(read_bytes)) AS read,
       formatReadableSize(max(memory_usage)) AS peak_mem,
       max(event_time) AS finished
FROM clusterAllReplicas(default, 'system.query_log')
WHERE event_time > now() - INTERVAL $SINCE HOUR
  AND type = 'QueryFinish'
  AND Settings['log_comment'] LIKE 'tsp:%'
  AND Settings['log_comment'] NOT LIKE '%:verify'
  AND Settings['log_comment'] NOT LIKE '%:drop'
GROUP BY tag
ORDER BY tag" 2>/dev/null
