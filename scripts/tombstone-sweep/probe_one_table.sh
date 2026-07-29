#!/usr/bin/env bash
# Validates the maintenance.table_cleanup sweep statements against production
# data, inside the gkg_probe scratch database. Production tables are read,
# never written.
#
#   probe_one_table.sh <v85_table> fast   # 1/9970/ slice (or full copy for
#                                         # global tables), all-time window:
#                                         # proves statement shape + row-level
#                                         # accounting on every schema in seconds
#   probe_one_table.sh <v85_table> full   # zero-copy CLONE, shipped 7-day
#                                         # window: proves cost + completion on
#                                         # the real volume
#
# Every statement carries log_comment='tsp:<tier>:<table>:<phase>' so duration,
# read volume, and peak memory survive in system.query_log even if this process
# dies. Results append to $RESULTS as JSON lines.
set -u
SRC=$1
TIER=$2
CHW=$(dirname "$0")/chw
RESULTS=${RESULTS:-/tmp/tombstone-sweep/results.jsonl}
DB=gkg_probe
T="$DB.${TIER}_${SRC}"
K="$DB.${TIER}_${SRC}_keys"

# Shipped TableCleanupConfig defaults (crates/gkg-server-config/src/engine.rs)
MAX_MEM=8000000000
EXT_SORT=$((MAX_MEM / 4))
STMT_TIMEOUT=5400
POLL=5

run() { # run <phase> <client_timeout> <<< sql ; echoes last line of output
  local phase=$1 tmo=$2 sql out rc
  sql=$(cat)
  out=$(CHW_MAX_TIME="$tmo" timeout $((tmo + 30)) "$CHW" <<< "$sql" 2>&1)
  rc=$?
  if [ $rc -ne 0 ]; then
    # A killed/timed-out transport must not read as an empty success.
    echo "TRANSPORT_ERROR rc=$rc: $(tail -1 <<< "$out")"
    return
  fi
  tail -1 <<< "$out"
}
val() { grep -o '[0-9][0-9]*' <<< "$1" | head -1; }
fail() {
  echo "{\"table\":\"$SRC\",\"tier\":\"$TIER\",\"status\":\"fail\",\"phase\":\"$1\",\"error\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1][:500]))' "$2")}" >> "$RESULTS"
  echo "FAIL $SRC $TIER at $1: $2" >&2
  exit 1
}

SK=$(run meta 60 <<< "SELECT sorting_key FROM system.tables WHERE database='gkg' AND name='$SRC' FORMAT TSVRaw")
[ -z "$SK" ] && fail meta "no sorting_key for $SRC"

if [ "$TIER" = fast ]; then
  WINDOW_START="toDateTime64('1970-01-01 00:00:00.000000', 6)"
else
  WINDOW_START="now64(6) - INTERVAL 7 DAY"
fi
WINDOW_END="now64(6)"

run drop 120 <<< "DROP TABLE IF EXISTS $T SETTINGS log_comment='tsp:$TIER:$SRC:drop'" >/dev/null
run drop 120 <<< "DROP TABLE IF EXISTS $K SETTINGS log_comment='tsp:$TIER:$SRC:drop'" >/dev/null

TS="$DB.${TIER}_${SRC}_sample_ts"
RC="$DB.${TIER}_${SRC}_sample_rc"

T_CLONE0=$(date +%s)
if [ "$TIER" = fast ]; then
  run drop 120 <<< "DROP TABLE IF EXISTS $TS" >/dev/null
  run drop 120 <<< "DROP TABLE IF EXISTS $RC" >/dev/null
  OUT=$(run create 300 <<< "CREATE TABLE $T AS gkg.$SRC")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail create "$OUT";; esac
  # Replacing merges collapse duplicate versions and corrupt row-delta
  # accounting; freeze merge scheduling on the probe (mutations still run).
  OUT=$(run freeze 120 <<< "ALTER TABLE $T MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail freeze_merges "$OUT";; esac
  if [[ "$SK" == traversal_path* ]]; then
    SLICE="startsWith(traversal_path, '1/9970/')"
  else
    SLICE="1"
  fi
  OUT=$(run slice 900 <<< "INSERT INTO $T SELECT * FROM gkg.$SRC WHERE $SLICE LIMIT 5000000 SETTINGS max_execution_time=800, log_comment='tsp:$TIER:$SRC:slice'")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail slice "$OUT";; esac
  SLICED=$(val "$(run count 300 <<< "SELECT count() AS n FROM $T FORMAT JSONEachRow")")
  if [ "${SLICED:-0}" = 0 ]; then
    OUT=$(run slice 900 <<< "INSERT INTO $T SELECT * FROM gkg.$SRC LIMIT 100000 SETTINGS max_execution_time=800, log_comment='tsp:$TIER:$SRC:slice_fallback'")
    case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail slice_fallback "$OUT";; esac
  fi

  # Synthetic scenarios so every table exercises the full correctness triad:
  # $TS keys get a newest tombstone (every row must go), $RC keys get a
  # tombstone superseded by a newer live row (nothing of theirs may go).
  OUT=$(run sample 300 <<< "CREATE TABLE $TS ENGINE = MergeTree ORDER BY ($SK) AS SELECT DISTINCT $SK FROM $T ORDER BY ($SK) LIMIT 50")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail sample_ts "$OUT";; esac
  OUT=$(run sample 300 <<< "CREATE TABLE $RC ENGINE = MergeTree ORDER BY ($SK) AS SELECT DISTINCT $SK FROM $T WHERE NOT _deleted AND ($SK) NOT IN (SELECT $SK FROM $TS) ORDER BY ($SK) DESC LIMIT 50")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail sample_rc "$OUT";; esac
  OUT=$(run inject 300 <<< "INSERT INTO $T SELECT * REPLACE (1 AS _deleted, now64(6) AS _version) FROM $T WHERE ($SK) IN (SELECT $SK FROM $TS)")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail inject_ts "$OUT";; esac
  OUT=$(run inject 300 <<< "INSERT INTO $T SELECT * REPLACE (1 AS _deleted, now64(6) - INTERVAL 1 HOUR AS _version) FROM $T WHERE ($SK) IN (SELECT $SK FROM $RC)")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail inject_rc_tombstone "$OUT";; esac
  OUT=$(run inject 300 <<< "INSERT INTO $T SELECT * REPLACE (0 AS _deleted, now64(6) AS _version) FROM $T WHERE ($SK) IN (SELECT $SK FROM $RC) AND NOT _deleted")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail inject_rc_live "$OUT";; esac
else
  # Cross-database CLONE AS trips "Tables have different secondary indices" on
  # index-bearing tables (26.4); its documented two-step expansion works.
  OUT=$(run clone 300 <<< "CREATE TABLE $T AS gkg.$SRC")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail clone "$OUT";; esac
  OUT=$(run clone 1800 <<< "ALTER TABLE $T ATTACH PARTITION ALL FROM gkg.$SRC SETTINGS log_comment='tsp:$TIER:$SRC:clone'")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail clone_attach "$OUT";; esac
  OUT=$(run freeze 120 <<< "ALTER TABLE $T MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1")
  case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail freeze_merges "$OUT";; esac
fi
T_CLONE1=$(date +%s)

ROWS_BEFORE=$(val "$(run count 900 <<< "SELECT count() AS n FROM $T SETTINGS log_comment='tsp:$TIER:$SRC:count_before' FORMAT JSONEachRow")")

# Phase 1: materialise the tombstoned key set (exact shipped statement shape,
# build_tombstoned_keys_table_sql in table_cleanup.rs)
T_BUILD0=$(date +%s)
OUT=$(run build $STMT_TIMEOUT <<EOF
CREATE TABLE $K ENGINE = MergeTree ORDER BY ($SK) AS
SELECT $SK FROM (
  SELECT $SK, _deleted FROM $T
  WHERE _version > $WINDOW_START AND _version <= $WINDOW_END
  ORDER BY $SK, _version DESC
  LIMIT 1 BY $SK
) WHERE _deleted
SETTINGS max_memory_usage = $MAX_MEM, max_bytes_before_external_sort = $EXT_SORT,
         optimize_read_in_order = 1, max_execution_time = $STMT_TIMEOUT,
         log_comment = 'tsp:$TIER:$SRC:build_keys'
EOF
)
case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail build_keys "$OUT";; esac
T_BUILD1=$(date +%s)
KEYS=$(val "$(run count 300 <<< "SELECT count() AS n FROM $K FORMAT JSONEachRow")")

TRIAD=""
if [ "$TIER" = fast ]; then
  TS_MISSED=$(val "$(run assert 300 <<< "SELECT count() AS n FROM $TS WHERE ($SK) NOT IN (SELECT $SK FROM $K) FORMAT JSONEachRow")")
  RC_LEAKED=$(val "$(run assert 300 <<< "SELECT count() AS n FROM $K WHERE ($SK) IN (SELECT $SK FROM $RC) FORMAT JSONEachRow")")
  RC_COUNT=$(val "$(run assert 300 <<< "SELECT count() AS n FROM $RC FORMAT JSONEachRow")")
  TRIAD=",\"ts_missed\":${TS_MISSED:-null},\"rc_leaked\":${RC_LEAKED:-null},\"rc_keys\":${RC_COUNT:-null}"
fi

EXPECTED=$(val "$(run expected 900 <<< "SELECT count() AS n FROM $T WHERE ($SK) IN (SELECT $SK FROM $K) SETTINGS use_index_for_in_with_subqueries_max_values=1000000, max_execution_time=800, log_comment='tsp:$TIER:$SRC:expected' FORMAT JSONEachRow")")

# Phase 2: the delete (build_delete_tombstoned_keys_sql)
T_DEL0=$(date +%s)
# sync=0: a synchronous wait holds a silent HTTP connection that the Cloud
# path idles out after ~20 min (curl exit 52); the mutation itself survives
# client disconnects, so submit async and let the row-count poll below be the
# completion signal.
OUT=$(run delete 600 <<EOF
DELETE FROM $T WHERE ($SK) IN (SELECT $SK FROM $K)
SETTINGS ${EXTRA_DELETE_SETTINGS:+$EXTRA_DELETE_SETTINGS, }lightweight_deletes_sync = 0, max_execution_time = $STMT_TIMEOUT,
         log_comment = 'tsp:$TIER:$SRC:delete'
EOF
)
case "$OUT" in *Exception*|*DB::*|*TRANSPORT_ERROR*) fail delete "$OUT";; esac
T_DEL1=$(date +%s)

# Phase 3: completion check (build_count_remaining_tombstoned_rows_sql), polled
# exactly like wait_until_tombstoned_keys_are_gone
DEADLINE=$((T_DEL1 + STMT_TIMEOUT))
STILL=-1
while :; do
  STILL=$(val "$(run verify 2500 <<< "SELECT count() AS remaining FROM $T WHERE ($SK) IN (SELECT $SK FROM $K) SETTINGS use_index_for_in_with_subqueries_max_values=1000000, max_execution_time=2400, log_comment='tsp:$TIER:$SRC:verify' FORMAT JSONEachRow")")
  [ "${STILL:-x}" = 0 ] && break
  [ "$(date +%s)" -ge "$DEADLINE" ] && fail verify "still_matching=$STILL after ${STMT_TIMEOUT}s"
  sleep $POLL
done
T_VERIFY=$(date +%s)

ROWS_AFTER=$(val "$(run count 900 <<< "SELECT count() AS n FROM $T SETTINGS log_comment='tsp:$TIER:$SRC:count_after' FORMAT JSONEachRow")")
REMOVED=$((ROWS_BEFORE - ROWS_AFTER))

STATUS=ok
[ "$REMOVED" -ne "${EXPECTED:-0}" ] && STATUS=accounting_mismatch
if [ "$TIER" = fast ]; then
  RC_SURVIVORS=$(val "$(run assert 300 <<< "SELECT count() AS n FROM $T WHERE ($SK) IN (SELECT $SK FROM $RC) FORMAT JSONEachRow")")
  TRIAD="$TRIAD,\"rc_survivors\":${RC_SURVIVORS:-null}"
  [ "${TS_MISSED:-1}" != 0 ] && STATUS=tombstoned_key_missed
  [ "${RC_LEAKED:-1}" != 0 ] && STATUS=recreated_key_deleted
  [ "${RC_COUNT:-0}" != 0 ] && [ "${RC_SURVIVORS:-0}" = 0 ] && STATUS=recreated_rows_lost
fi

echo "{\"table\":\"$SRC\",\"tier\":\"$TIER\",\"status\":\"$STATUS\",\"rows_before\":$ROWS_BEFORE,\"keys\":${KEYS:-null},\"expected\":${EXPECTED:-null},\"removed\":$REMOVED,\"still_matching\":$STILL$TRIAD,\"clone_s\":$((T_CLONE1 - T_CLONE0)),\"build_s\":$((T_BUILD1 - T_BUILD0)),\"delete_s\":$((T_DEL1 - T_DEL0)),\"settle_s\":$((T_VERIFY - T_DEL1))}" >> "$RESULTS"

if [ "$STATUS" = ok ]; then
  for probe_table in "$T" "$K" "$TS" "$RC"; do
    run drop 300 <<< "DROP TABLE IF EXISTS $probe_table" >/dev/null
  done
else
  echo "kept $T / $K / $TS / $RC for inspection (status=$STATUS)" >&2
  exit 1
fi
