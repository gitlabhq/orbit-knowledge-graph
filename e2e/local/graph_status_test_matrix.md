# GetGraphStatus E2E Test Matrix

Every cell in the matrix must be verifiable via `GET /api/v4/orbit/graph_status`
(top-level REST) or `grpcurl ... GetGraphStatus` (direct gRPC). Each row is one
concrete, executable scenario.

## Response shape (for reference)

```json
{
  "state": "pending" | "indexing" | "idle",
  "initial_backfill_done": bool,
  "updated_at": "<RFC3339>" | "",
  "stale": bool,
  "domains": [{"name": "...", "items": [{"name": "...", "status": "pending"|"completed", "count": N}]}],
  "edge_counts": {"<REL_KIND>": N, ...},
  "sdlc": {"last_completed_at", "last_started_at", "last_duration_ms", "cycle_count", "last_error"} | null,
  "code":  {"projects_indexed", "projects_total", "last_indexed_at"} | null
}
```

## Matrix: state × scope × outcome

| # | Scenario | Scope | Expected `state` | `initial_backfill_done` | `stale` | `updated_at` | `domains[].items[].status` | `edge_counts` | `sdlc.cycle_count` | `sdlc.last_error` | `code.projects_*` | KV keys present |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **1** | Newly-created namespace, KG enabled, before dispatcher fires | top-group | `pending` | `false` | `true` | `""` | all `pending` | empty `{}` | `null` or absent | `null` or absent | 0 / 0 | none |
| **2** | Namespace mid-ETL cycle (dispatcher has fired, indexer running) | top-group | `indexing` | prev or `false` | depends on prev writes | `<recent>` | prev values | prev values | prev cycle | prev error | prev | `meta.<ns>` with state=indexing |
| **3** | Namespace after first successful ETL cycle (has data) | top-group | `idle` | `true` | `false` | `<recent>` | mix: entities with data `completed`, empty `pending` | non-empty | `1` | `""` | total ≥ 0 | `meta.<ns>`, `counts.<tp>` |
| **4** | Same namespace N cycles later | top-group | `idle` | `true` | `false` | `<recent>` | same | stable counts ± HLL error | `N` | `""` | same | same + updated_at refreshed |
| **5** | Namespace cycle that errored (ETL failure) | top-group | `idle` | preserves prior (monotonic) | `false` | `<recent>` | from last successful cycle | same | increments | `"<error msg>"` | preserved | `meta.<ns>` with last_error set |
| **6** | Subgroup lookup under an indexed top-group | subgroup | `idle` | `true` (inherited from top-group meta) | `false` | `<recent>` | subset of top-group's entities | subset | top-group's cycle | top-group's | top-group's | `counts.<subgroup_tp>` present |
| **7** | Subgroup of a namespace never dispatched | subgroup | `pending` | `false` | `true` | `""` | all `pending` | empty | `null` | `null` | 0/0 | no `counts.<tp>` |
| **8** | Project-scoped lookup — code indexing complete | project | `idle` | `true` | `false` | `<recent>` | domain `source_code` items `completed` (File, Directory, Definition, Branch) | CODE edges (`CONTAINS`, `DEFINES`, `IMPORTS`, `CALLS`) present | top-group's cycle | `""` | `projects_indexed ≥ 1`, `projects_total ≥ 1`, `last_indexed_at` set | `code.<project_id>`, `counts.<tp>` |
| **9** | Project-scoped lookup — code indexing NOT yet run | project | `idle` (from SDLC) | `true` | `false` | `<recent>` | `source_code` items all `pending` | no CODE edges | top-group's cycle | `""` | `projects_total ≥ 1`, `projects_indexed = 0` | no `code.<project_id>` |
| **10** | Project of a project under a non-enabled namespace | project | `pending` | `false` | `true` | `""` | all `pending` | empty | — | — | 0/0 | none |
| **11** | Traversal path stale (no recent writes, older than `staleness_threshold_secs`) | any | last known | last known | `true` | `<old>` | last values | last values | — | — | — | keys present but old |
| **12** | Zero-row re-dispatch (no new data since last cycle) | top-group | `idle` | `true` (preserved) | `false` | `<refreshed>` | **same** values as last cycle | **same** | `cycle_count++` | `""` | same | counts keys NOT rewritten (skip), meta refreshed |
| **13** | Namespace after deletion is requested | top-group | `pending` | `false` | `true` | `""` | all `pending` | empty | — | — | 0/0 | none (KV cleanup ran) |
| **14** | Namespace after deletion re-enabled before the handler drained | top-group | same as before | same | same | same | same | same | same | same | same | keys survive (re-enabled skip path) |

## Validation sub-matrix: monotonic + transitions

| # | Check | How |
|---|---|---|
| **M1** | `initial_backfill_done` never regresses from `true` to `false` | Poll same namespace across 3+ cycles; assert all show `true` once observed |
| **M2** | `sdlc.cycle_count` monotonic | Poll twice ≥ 90s apart; assert second ≥ first |
| **M3** | `state` transitions `pending → indexing → idle` on first run | Create fresh group, poll rapidly for 300s, capture sequence |
| **M4** | `state` transitions `idle → indexing → idle` on subsequent runs | Wait for first idle, then poll during the next dispatcher cycle |
| **M5** | `updated_at` strictly non-decreasing | Poll across cycles, parse RFC3339, assert each ≥ previous |
| **M6** | `last_started_at ≤ last_completed_at` always | For any non-empty sdlc, parse both timestamps |

## Validation sub-matrix: 4xx boundary conditions

| # | Request | Expected HTTP | Body shape |
|---|---|---|---|
| **B1** | `GET /graph_status` (no params) | 400 | `{"error": "...missing..."}` |
| **B2** | `GET /graph_status?namespace_id=1&project_id=2` (multiple) | 400 | `{"error": "...exactly one..."}` |
| **B3** | `GET /graph_status?namespace_id=99999999` (nonexistent) | 404 | `{"message": "...Not Found"}` |
| **B4** | `GET /graph_status?project_id=99999999` | 404 | same |
| **B5** | `GET /graph_status?full_path=does/not/exist` | 404 | same |
| **B6** | No auth header | 401 | `{"message": "401 Unauthorized"}` |
| **B7** | PAT with wrong scope | 403 | forbidden |
| **B8** | Personal namespace id (user namespace, not group) | 404 | leaks nothing about existence |
| **B9** | Private group the user does not belong to | 404 | same as nonexistent to prevent existence leak |
| **B10** | NATS unreachable at webserver boot | 503 (`UNAVAILABLE`) on subsequent calls | `"graph status not available (NATS not configured)"` |
| **B11** | Malformed JSON in KV (corrupted snapshot) | 500 (`Internal`) | generic error, no serde detail leakage |

## Scope-specific data requirements

For scenarios **3, 4, 6, 8, 12** to be meaningful, the test namespace needs:

| Requirement | Why |
|---|---|
| ≥ 1 root group | Has `Group` entity to count |
| ≥ 1 subgroup | Scenario 6 |
| ≥ 1 project under the subgroup | Scenarios 8, 9 |
| ≥ 1 commit pushed to default branch | Scenario 8 (code entities) |
| KG enabled on root namespace | Dispatcher will pick it up |
| Siphon has replicated the `knowledge_graph_enabled_namespaces` row | Dispatcher sees it in ClickHouse |

## Exact commands per row

### Row 1 (pending, fresh namespace)

```bash
GDK_TOKEN=$(cat ~/.gdk_token)
# Create new group
GID=$(curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" -X POST \
  "https://gdk.test:3443/api/v4/groups" \
  -d "name=matrix-pending-$$&path=matrix-pending-$$&visibility=public" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')

# Enable KG (returns 200)
curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" -X PUT \
  "https://gdk.test:3443/api/v4/admin/knowledge_graph/namespaces/$GID"

# Immediately call graph_status (before Siphon + dispatcher run)
curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" \
  "https://gdk.test:3443/api/v4/orbit/graph_status?namespace_id=$GID" \
  | python3 -m json.tool
# Expect: state=pending, initial_backfill_done=false, stale=true
```

### Row 3 (idle top-group with data)

```bash
GDK_TOKEN=$(cat ~/.gdk_token)
# Use an already-indexed namespace (e.g. id=22 "Commit451")
curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" \
  "https://gdk.test:3443/api/v4/orbit/graph_status?namespace_id=22" \
  | python3 -m json.tool
# Expect: state=idle, initial_backfill_done=true, stale=false,
#         domains non-empty with some COMPLETED items,
#         edge_counts non-empty, sdlc.cycle_count ≥ 1
```

### Row 8 (project with code entities)

```bash
GDK_TOKEN=$(cat ~/.gdk_token)
# Pick a project under an indexed namespace
curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" \
  "https://gdk.test:3443/api/v4/orbit/graph_status?project_id=<PID>" \
  | python3 -c '
import json,sys
d=json.load(sys.stdin)
sc=[it for dom in d["domains"] if dom["name"]=="source_code" for it in dom["items"]]
print("source_code items:")
for it in sc: print(f"  {it[\"name\"]}={it[\"count\"]} ({it[\"status\"]})")
print(f"code.projects_indexed={d[\"code\"][\"projects_indexed\"]} total={d[\"code\"][\"projects_total\"]}")
print(f"edge_counts has code edges: {any(k in d[\"edge_counts\"] for k in [\"CONTAINS\",\"DEFINES\",\"IMPORTS\",\"CALLS\"])}")
'
```

### Row 12 (zero-row skip, meta refreshes but counts unchanged)

```bash
GDK_TOKEN=$(cat ~/.gdk_token)
# Read once
R1=$(curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" \
  "https://gdk.test:3443/api/v4/orbit/graph_status?namespace_id=22")
C1=$(echo "$R1" | python3 -c 'import json,sys;print(json.load(sys.stdin)["sdlc"]["cycle_count"])')
U1=$(echo "$R1" | python3 -c 'import json,sys;print(json.load(sys.stdin)["updated_at"])')
T1=$(echo "$R1" | python3 -c 'import json,sys;print(sum(i["count"] for dom in json.load(sys.stdin)["domains"] for i in dom["items"]))')

sleep 120  # at least one dispatcher cycle

R2=$(curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" \
  "https://gdk.test:3443/api/v4/orbit/graph_status?namespace_id=22")
C2=$(echo "$R2" | python3 -c 'import json,sys;print(json.load(sys.stdin)["sdlc"]["cycle_count"])')
U2=$(echo "$R2" | python3 -c 'import json,sys;print(json.load(sys.stdin)["updated_at"])')
T2=$(echo "$R2" | python3 -c 'import json,sys;print(sum(i["count"] for dom in json.load(sys.stdin)["domains"] for i in dom["items"]))')

# Expect: C2 > C1, U2 > U1, T2 == T1 (zero-row skip preserves counts)
echo "cycles: $C1 -> $C2 (must increment)"
echo "updated_at: $U1 -> $U2 (must advance)"
echo "total entities: $T1 -> $T2 (must match if no new data)"
```

### Row 13 (namespace deletion KV cleanup)

```bash
GDK_TOKEN=$(cat ~/.gdk_token)
# Create, enable, wait for indexing, then disable
GID=$(curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" -X POST \
  "https://gdk.test:3443/api/v4/groups" \
  -d "name=matrix-del-$$&path=matrix-del-$$&visibility=public" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" -X PUT \
  "https://gdk.test:3443/api/v4/admin/knowledge_graph/namespaces/$GID"
# wait for idle
sleep 180
# verify KV keys exist via nats CLI
nats --server localhost:4222 kv get indexing_progress "meta.$GID"

# disable KG (triggers deletion handler on next cycle)
curl -sk -H "PRIVATE-TOKEN: $GDK_TOKEN" -X DELETE \
  "https://gdk.test:3443/api/v4/admin/knowledge_graph/namespaces/$GID"
sleep 300  # deletion runs on daily cron; in tests may need manual trigger

# verify KV keys gone
nats --server localhost:4222 kv get indexing_progress "meta.$GID"
# Expect: not found
```

## Execution order

Run in this order to maximize signal per cycle:

1. **B1-B11** (boundary conditions) — fast, no indexer dependency
2. **1** (pending) — needs fresh namespace
3. **3, 4** (idle top-group) — use existing indexed namespaces (ns 22 "Commit451")
4. **6** (subgroup) — needs a namespace with an actual subgroup
5. **8, 9** (project scope) — needs indexed project; scenario 8 needs code indexing to have run
6. **M1-M6** (monotonic checks) — longer-running polls, can overlap with others
7. **12** (zero-row skip) — 2-minute gap required
8. **13, 14** (deletion) — slow, run last

## Pass criteria

- All shape assertions hold
- `state` value matches expected column for every row
- No HTTP 5xx (server error) except **B10, B11** intentional
- Monotonic invariants M1-M6 never violated across polls
- No KV key leakage after namespace deletion
- `code.projects_indexed ≤ code.projects_total` always
- `updated_at` parses as valid RFC3339 when non-empty
