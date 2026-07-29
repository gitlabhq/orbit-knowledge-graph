#!/usr/bin/env bash
# Same shipped config as the other tables, plus an explicit still_matching check.
SRC=$1
C=$(dirname "$0")/chw
R() { timeout ${2:-900} $C <<< "$1" 2>&1 | tail -1; }
V() { R "$1" "${2:-900}" | grep -o '[0-9][0-9]*' | head -1; }
SK=$(timeout 60 $C <<< "SELECT sorting_key FROM system.tables WHERE database='gkg' AND name='$SRC' FORMAT TSVRaw" 2>/dev/null | tail -1)
R "DROP TABLE IF EXISTS gkg.sweep_probe_k" 60 >/dev/null
R "DROP TABLE IF EXISTS gkg.sweep_probe_t" 60 >/dev/null
R "CREATE TABLE gkg.sweep_probe_t CLONE AS gkg.$SRC" 300 >/dev/null
ROWS=$(V "SELECT count() AS n FROM gkg.sweep_probe_t FORMAT JSONEachRow")

T0=$(date +%s)
R "CREATE TABLE gkg.sweep_probe_k ENGINE = MergeTree ORDER BY ($SK) AS
   SELECT $SK FROM (SELECT $SK, _deleted FROM gkg.sweep_probe_t
     WHERE _version > now() - INTERVAL 7 DAY AND _version <= now()
     ORDER BY $SK, _version DESC LIMIT 1 BY $SK) WHERE _deleted
   SETTINGS max_memory_usage=8000000000, max_bytes_before_external_sort=2000000000,
            optimize_read_in_order=1, max_execution_time=3000" 3000 >/dev/null
T1=$(date +%s)
KEYS=$(V "SELECT count() AS n FROM gkg.sweep_probe_k FORMAT JSONEachRow")

R "DELETE FROM gkg.sweep_probe_t WHERE ($SK) IN (SELECT $SK FROM gkg.sweep_probe_k)
   SETTINGS lightweight_deletes_sync=1, max_execution_time=3000" 3000 >/dev/null
PREV=-1; STABLE=0
for i in $(seq 1 60); do
  N=$(V "SELECT count() AS n FROM gkg.sweep_probe_t FORMAT JSONEachRow" 300)
  [ "$N" = "$PREV" ] && STABLE=$((STABLE+1)) || STABLE=0
  PREV=$N
  [ $STABLE -ge 2 ] && break
  sleep 20
done
T2=$(date +%s)

# the check I skipped last time: nothing may remain that matches the key set
LEFT=$(V "SELECT count() AS n FROM gkg.sweep_probe_t WHERE ($SK) IN (SELECT $SK FROM gkg.sweep_probe_k) SETTINGS max_execution_time=2400 FORMAT JSONEachRow" 2500)
echo "$SRC|rows=${ROWS:-?}|keys=${KEYS:-?}|build=$((T1-T0))s|delete+settle=$((T2-T1))s|removed=$(( ${ROWS:-0} - ${PREV:-0} ))|still_matching=${LEFT:-UNMEASURED}" >> ${RESULTS:-/tmp/tombstone-sweep-results.txt}
R "DROP TABLE IF EXISTS gkg.sweep_probe_k" 60 >/dev/null
R "DROP TABLE IF EXISTS gkg.sweep_probe_t" 60 >/dev/null
