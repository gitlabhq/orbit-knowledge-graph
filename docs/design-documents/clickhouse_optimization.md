# ClickHouse Index & Query Optimization Plan

Research synthesis from deep analysis of the ClickHouse documentation, ClickHouse source code, GKG codebase (schema, query engine, ontology), and the Kùzu GDBMS white paper on graph query optimization.

Verified against ClickHouse source (`MergeTreeWhereOptimizer`, `KeyCondition`, `optimizeUseNormalProjection`) and GKG query engine codegen.

## Current State Summary

- **24 graph tables**, all `ReplacingMergeTree(_version, _deleted)` with `allow_experimental_replacing_merge_with_cleanup = 1`
- **Zero skipping indexes** anywhere in the schema
- **Zero partitioning** on any table
- **5 projections** total (only on code indexing tables and `code_indexing_checkpoint`)
- **No FINAL** in query engine output, no query-level settings
- **PREWHERE is automatic** — ClickHouse auto-promotes `startsWith(traversal_path, ...)` to PREWHERE since `traversal_path` is the first sorting key column (verified in `MergeTreeWhereOptimizer.cpp`)
- **OFFSET-based pagination** (no cursor/keyset pagination)
- All node tables (except `gl_user`) use ORDER BY `(traversal_path, id)`
- `gl_edge` ORDER BY `(traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind)`, PRIMARY KEY prefix `(traversal_path, source_id, source_kind, relationship_kind)`
- Every query has `startsWith(traversal_path, ...)` security filter on every `gl_*` table (except `gl_user`)
- **`startsWith()` IS converted to a range scan** on the primary key (`KeyCondition.cpp` registers it in `atom_map`), so the primary key is already optimal for security filters

---

## Recommendations (Priority Order)

### P0: High Impact, Low Effort

#### 1. Reverse Edge Index for Incoming Traversals

**Problem:** The edge table's PRIMARY KEY starts with `(traversal_path, source_id, ...)`. Incoming/reverse traversals JOIN on `target_id`, which is NOT in the primary key prefix. This forces a full scan within the `traversal_path` range for every incoming edge lookup. Bidirectional neighbor queries use OR conditions that defeat index usage entirely.

**Solution: Projection (simplest, no code changes, works with FINAL)**

```sql
ALTER TABLE gl_edge ADD PROJECTION reverse_lookup (
    SELECT *
    ORDER BY (traversal_path, target_id, target_kind, relationship_kind, source_id, source_kind)
);
ALTER TABLE gl_edge MATERIALIZE PROJECTION reverse_lookup;
ALTER TABLE gl_edge MODIFY SETTING deduplicate_merge_projection_mode = 'rebuild';
```

ClickHouse's query analyzer automatically selects the projection with fewest granules. No query engine changes needed.

**Verified:** Projections ARE compatible with FINAL in modern ClickHouse. The `optimizeUseNormalProjection.cpp` optimizer does NOT check `is_final` — projection selection runs regardless. The earlier claim about FINAL incompatibility was incorrect (verified against ClickHouse source).

**Alternative (only if independent tuning is needed):** A reverse edge table via MaterializedView gives separate control over index_granularity, skipping indexes, and settings, but requires explicit query routing in `lower.rs`. Use only if the projection approach proves insufficient.

**Tradeoffs:**
- Doubles `gl_edge` storage (full data copy in alternate sort order)
- Automatic query routing — no code changes needed
- `deduplicate_merge_projection_mode = 'rebuild'` ensures projection stays consistent during ReplacingMergeTree merges (already used on code tables)

**Impact:** Eliminates full scans for ~50% of edge lookups (all incoming traversals).

#### 2. Skipping Indexes on High-Filter Columns

**Problem:** Zero secondary indexes exist. Queries frequently filter on columns like `state`, `severity`, `relationship_kind`, `draft`, `confidential` that are NOT in the primary key. ClickHouse must scan all granules within the `traversal_path` range to evaluate these filters.

**Solution:** Add targeted skipping indexes:

```sql
-- NOTE: gl_edge skip indexes are largely redundant if recommendation #1 (reverse projection) is in place.
-- relationship_kind is already at position 4 in the PRIMARY KEY — usable for outgoing traversals.
-- The reverse projection covers incoming traversals. Only add these if the projection is not deployed:
--
-- ALTER TABLE gl_edge ADD INDEX idx_rel_kind relationship_kind TYPE set(100) GRANULARITY 1;
-- ALTER TABLE gl_edge ADD INDEX idx_target_id target_id TYPE bloom_filter(0.01) GRANULARITY 1;
-- ALTER TABLE gl_edge ADD INDEX idx_src_kind source_kind TYPE set(30) GRANULARITY 1;
-- ALTER TABLE gl_edge ADD INDEX idx_tgt_kind target_kind TYPE set(30) GRANULARITY 1;

-- Node table indexes — these ARE valuable regardless of other recommendations:

-- gl_merge_request: 'state' is the most filtered column across all benchmark queries
ALTER TABLE gl_merge_request ADD INDEX idx_state state TYPE set(10) GRANULARITY 1;
ALTER TABLE gl_merge_request MATERIALIZE INDEX idx_state;

-- gl_merge_request: draft boolean
ALTER TABLE gl_merge_request ADD INDEX idx_draft draft TYPE minmax GRANULARITY 1;
ALTER TABLE gl_merge_request MATERIALIZE INDEX idx_draft;

-- gl_pipeline: status filtered frequently
ALTER TABLE gl_pipeline ADD INDEX idx_status status TYPE set(20) GRANULARITY 1;
ALTER TABLE gl_pipeline MATERIALIZE INDEX idx_status;

-- gl_vulnerability: severity is a key filter
ALTER TABLE gl_vulnerability ADD INDEX idx_severity severity TYPE set(10) GRANULARITY 1;
ALTER TABLE gl_vulnerability MATERIALIZE INDEX idx_severity;

-- gl_work_item: state filtered often
ALTER TABLE gl_work_item ADD INDEX idx_state state TYPE set(10) GRANULARITY 1;
ALTER TABLE gl_work_item MATERIALIZE INDEX idx_state;
```

**Index type selection rationale:**
- `set(N)` for low-cardinality enum/string columns (state, severity, kind) — stores exact values per granule block, supports ALL filter functions. N should be >= number of distinct values.
- `minmax` for booleans and range-queried numerics — stores min/max per granule block, near-zero overhead.
- `bloom_filter` would be used for high-cardinality columns with equality checks, but none of the frequently-filtered columns are high-cardinality.

**Impact:** Reduces granule reads by skipping blocks where the filtered value doesn't exist. Most effective when the filtered value is selective (e.g., `state = 'merged'` on a table where most rows are `opened`).

#### 3. LowCardinality Column Types on Graph Tables

**Problem:** All String columns in graph tables are plain `String`. Enum-like columns (`state`, `source_kind`, `target_kind`, `relationship_kind`, `severity`, `visibility_level`, `user_type`, `source`) have low cardinality (<100 distinct values) but pay full String storage and comparison costs.

**Solution:** Change column types to `LowCardinality(String)`:

```sql
-- gl_edge (highest impact — most queried table)
ALTER TABLE gl_edge MODIFY COLUMN source_kind LowCardinality(String);
ALTER TABLE gl_edge MODIFY COLUMN target_kind LowCardinality(String);
ALTER TABLE gl_edge MODIFY COLUMN relationship_kind LowCardinality(String);

-- gl_merge_request
ALTER TABLE gl_merge_request MODIFY COLUMN state LowCardinality(String);

-- gl_pipeline
ALTER TABLE gl_pipeline MODIFY COLUMN status LowCardinality(String);
ALTER TABLE gl_pipeline MODIFY COLUMN source LowCardinality(String);

-- gl_vulnerability
ALTER TABLE gl_vulnerability MODIFY COLUMN severity LowCardinality(String);

-- gl_project
ALTER TABLE gl_project MODIFY COLUMN visibility_level LowCardinality(String);

-- gl_user
ALTER TABLE gl_user MODIFY COLUMN state LowCardinality(String);
ALTER TABLE gl_user MODIFY COLUMN user_type LowCardinality(String);
```

**How it helps:** LowCardinality uses dictionary encoding — strings are replaced with integer IDs internally. This improves:
- Compression ratio (smaller on disk)
- Filter performance (integer comparison vs string comparison)
- JOIN performance (smaller hash table entries)
- Effective when < 10,000 distinct values (our columns have < 100)

**Impact:** ~10-30% improvement in query speed on filtered columns, significant storage reduction. Zero query engine changes needed — ClickHouse handles LowCardinality transparently.

#### 4. Enable Query Condition Cache

**Problem:** Repeated queries with the same `startsWith(traversal_path, ...)` filter scan the same granules every time.

**Solution:** Enable at the server/session level:
```sql
SET use_query_condition_cache = 1;
```

Or in ClickHouse server config:
```xml
<query_condition_cache_size>104857600</query_condition_cache_size> <!-- 100MB -->
```

**How it helps:** Remembers per-filter, per-granule whether any row matched (1 bit per entry). On repeat queries with the same traversal_path filter, granules known to have zero matches are skipped entirely. 100MB covers ~6.8 trillion granule evaluations.

**Requirements:**
- Data should be mostly immutable (fits our model — CDC upserts are infrequent relative to reads)
- Filters should be selective (traversal_path filters are highly selective)
- Same filters repeated (security context reuse across user sessions)
- **Requires `allow_experimental_analyzer = 1`** (verified in ClickHouse source: `QueryPlanOptimizationSettings.cpp:159` — the cache is gated on the new analyzer being enabled)

**Impact:** Near-free speedup for repeated access patterns. No code changes needed.

---

### P1: High Impact, Medium Effort

#### 5. Semijoin Pre-filtering for Multi-hop Queries (Kùzu SIP-inspired)

**Concept from Kùzu:** Sideways Information Passing (SIP) — pass filters from one side of a join to reduce scans on the other. Kùzu's ASP-Join accumulates probe tuples, builds a semijoin filter, then uses it to restrict the build-side scan.

**Problem in GKG:** Multi-hop UNION ALL queries scan the full `gl_edge` table in each arm. For `max_hops=3`, that's 1+2+3 = 6 edge table scans. When the starting node set is selective (specific `node_ids` or filtered by `state`), most edge rows are irrelevant.

**Solution:** Pre-materialize the starting node IDs in a CTE and use them to restrict edge scans:

```sql
-- Current (no pre-filtering):
SELECT ... FROM gl_user AS u
INNER JOIN (
  SELECT e1.source_id AS start_id, e1.target_id AS end_id, 1 AS depth
  FROM gl_edge e1 WHERE e1.relationship_kind = {p0:String}
  UNION ALL
  SELECT e1.source_id, e2.target_id, 2
  FROM gl_edge e1 JOIN gl_edge e2 ON e1.target_id = e2.source_id ...
) AS hop_e0 ON u.id = hop_e0.start_id
WHERE startsWith(u.traversal_path, {p1:String}) AND u.state = {p2:String}

-- With SIP pre-filtering:
WITH source_ids AS (
  SELECT id FROM gl_user WHERE startsWith(traversal_path, {p1:String}) AND state = {p2:String}
)
SELECT ... FROM gl_user AS u
INNER JOIN (
  SELECT e1.source_id AS start_id, e1.target_id AS end_id, 1 AS depth
  FROM gl_edge e1
  WHERE e1.source_id IN (SELECT id FROM source_ids) AND e1.relationship_kind = {p0:String}
  UNION ALL
  SELECT e1.source_id, e2.target_id, 2
  FROM gl_edge e1 JOIN gl_edge e2 ON e1.target_id = e2.source_id
  WHERE e1.source_id IN (SELECT id FROM source_ids) ...
) AS hop_e0 ON u.id = hop_e0.start_id
```

**When to apply:** Only when the source node set is selective (has filters or `node_ids`). For broad queries (e.g., "all users"), the IN list becomes large and adds overhead. The query engine should check whether the starting node has filters before applying this optimization.

**Implementation:** In `lower.rs:build_hop_union_all()`, when the source node has filters or `node_ids`, emit a CTE collecting the filtered IDs and inject `source_id IN (SELECT id FROM ...)` into each UNION ALL arm.

**Impact:** Reduces edge table reads proportionally to source selectivity. For point queries (single `node_id`), this converts each edge scan from a range scan to a point lookup.

#### 6. Keyset Pagination (Replace OFFSET)

**Problem:** `LIMIT N OFFSET M` forces ClickHouse to read and discard M rows. For `OFFSET 900`, it reads 930 rows to return 30.

**Solution:** Implement cursor-based/keyset pagination using the sort key:

```sql
-- Instead of: ORDER BY mr.created_at DESC LIMIT 30 OFFSET 60
-- Use:        WHERE mr.created_at < {last_seen_created_at} ORDER BY mr.created_at DESC LIMIT 30
```

**Implementation:** The query engine would return a cursor (the last row's sort key values) alongside results. The next page request includes this cursor as a filter. For compound sorts (e.g., `created_at DESC, id DESC`), the cursor contains both values:
```sql
WHERE (mr.created_at, mr.id) < ({cursor_created_at}, {cursor_id})
ORDER BY mr.created_at DESC, mr.id DESC
LIMIT 30
```

**Impact:** Every page reads exactly `LIMIT` rows regardless of depth. Eliminates O(N) overhead for deep pagination.

#### 7. Recursive CTE for Multi-hop (Replace UNION ALL Unroll)

**Problem:** Multi-hop queries unroll into `max_hops` UNION ALL arms with `1+2+...+max_hops` total edge table scans (6 for max_hops=3). Each arm independently self-joins the edge table.

**Solution:** Replace the unrolled UNION ALL with a recursive CTE that builds incrementally:

```sql
-- Current: 6 edge scans for 3-hop
(SELECT ... FROM gl_edge e1 ...)  -- 1 scan
UNION ALL
(SELECT ... FROM gl_edge e1 JOIN gl_edge e2 ...)  -- 2 scans
UNION ALL
(SELECT ... FROM gl_edge e1 JOIN gl_edge e2 JOIN gl_edge e3 ...)  -- 3 scans

-- Recursive CTE: 3 edge scans for 3-hop (one per recursion level)
WITH RECURSIVE hops AS (
  SELECT source_id AS start_id, target_id AS end_id, 1 AS depth, ...
  FROM gl_edge WHERE relationship_kind IN (...) AND startsWith(traversal_path, ...)
  UNION ALL
  SELECT h.start_id, e.target_id, h.depth + 1, ...
  FROM hops h JOIN gl_edge e ON h.end_id = e.source_id
  WHERE h.depth < {max_hops} AND e.relationship_kind IN (...)
)
SELECT * FROM hops
```

**Feasibility:** The path-finding query type already uses recursive CTEs successfully (`lower.rs:200-407`), proving the pattern works in GKG's ClickHouse setup. The multi-hop pattern can reuse the same infrastructure.

**Impact:** Reduces edge table scans from O(n²) to O(n) where n = max_hops. For max_hops=3: 6 → 3 scans.

#### 8. Partitioning Strategy for FINAL Performance

**Problem:** No tables are partitioned. If FINAL is ever added to the query engine (for correctness), un-partitioned tables force single-threaded FINAL processing across all data.

**Solution:** Partition graph tables by a function of `traversal_path` that maps to namespace:

Partition graph tables by a function of `traversal_path` that maps to the root namespace:

```sql
-- Option A: Partition by top-level namespace ID extracted from traversal_path
-- Works if number of top-level namespaces stays under ~1000
PARTITION BY toUInt64(extract(traversal_path, '^\d+'))

-- Option B: Hash to fixed bucket count (safer for partition count control)
PARTITION BY sipHash64(substring(traversal_path, 1, position(traversal_path, '/') - 1)) % 256
```

**Why this unlocks parallel FINAL:** Since ClickHouse v23.12, `do_not_merge_across_partitions_select_final` is auto-derived when the partition key expression contains only columns from the primary key. Since `traversal_path` is the first column in ORDER BY for all node/edge tables, partitioning by a function of `traversal_path` qualifies. This means FINAL can run in parallel across partitions — one thread per partition instead of single-threaded across all data.

**Tradeoffs:**
- Too many partitions (>1000) degrades performance due to file descriptor overhead — monitor with `SELECT partition, count() FROM system.parts WHERE table='gl_edge' GROUP BY partition`
- Existing data would need to be re-inserted (partitioning can't be changed on existing data without table recreation)
- Merges only happen within the same partition — cross-partition parts are never merged
- `gl_user` cannot benefit (ORDER BY `id` only, no `traversal_path`)
- `DROP PARTITION` becomes available for efficient namespace deletion (vs current row-by-row approach)
- Option A enables true partition pruning for `startsWith(traversal_path, ...)` filters; Option B does not (hash destroys locality)

**Recommendation:** Start with `gl_edge` (largest table, most impacted by FINAL) and `gl_merge_request` (most queried node table). Use Option A if top-level namespace count is < 500; otherwise use Option B.

**Impact:** Enables parallel FINAL processing per partition if/when FINAL is added. Enables `DROP PARTITION` for efficient namespace deletion. Option A additionally enables partition pruning for security filters.

---

### P2: Medium Impact, Medium-High Effort

#### 9. Denormalize `project_id` into SDLC Entity Tables

**Problem:** "Find entities in project X" requires joining through `gl_edge` even though the relationship is 1:N and stable. This is the most common query pattern.

**Solution:** Add `project_id Int64` column to high-frequency SDLC tables:

```sql
ALTER TABLE gl_merge_request ADD COLUMN project_id Int64 DEFAULT 0;
ALTER TABLE gl_pipeline ADD COLUMN project_id Int64 DEFAULT 0;
ALTER TABLE gl_work_item ADD COLUMN project_id Int64 DEFAULT 0;
ALTER TABLE gl_vulnerability ADD COLUMN project_id Int64 DEFAULT 0;
```

The indexer would populate `project_id` during ETL (already available in the datalake source tables). The query engine could then skip the edge JOIN for project-scoped queries.

**Note:** The code graph tables (`gl_definition`, `gl_file`, `gl_directory`, `gl_imported_symbol`) already have `project_id` in their sort key — this approach extends the pattern to SDLC tables.

**Impact:** Eliminates one edge JOIN for the most common query pattern. Requires indexer and query engine changes.

#### 10. Dictionary for `gl_user` Lookups

**Problem:** `gl_user` is JOINed in ~10 of 29 benchmark queries. It's a global table (ORDER BY `id`, no traversal_path), making it ideal for in-memory dictionary lookup.

**Solution:**
```sql
CREATE DICTIONARY gl_user_dict (
    id Int64,
    username String,
    name Nullable(String),
    state String,
    avatar_url Nullable(String),
    user_type String
) PRIMARY KEY id
SOURCE(CLICKHOUSE(
    DB 'graph'
    QUERY 'SELECT id, username, name, state, avatar_url, user_type FROM gl_user FINAL'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);
```

Queries would use `dictGet` instead of JOIN:
```sql
-- Instead of: INNER JOIN gl_user AS u ON e.source_id = u.id
-- Use: dictGet('gl_user_dict', 'username', e.source_id) AS u_username
```

**Alternative: `join_algorithm = 'direct'`** — ClickHouse can use a Dictionary engine table as the right side of a JOIN transparently. This means we could keep the existing JOIN-based SQL generation but back the table with a dictionary, getting dictionary performance without rewriting the codegen:
```sql
CREATE TABLE gl_user_dict_table (...) ENGINE = Dictionary(gl_user_dict);
-- Then existing JOINs against this table use O(1) lookups automatically
```

**Tradeoffs:**
- `dictGet` approach requires codegen refactor (new TableRef variant for dictionary access)
- `direct` join algorithm approach requires no codegen changes but needs a Dictionary engine table
- Redaction pipeline needs adaptation (`_gkg_u_id` can be derived from the source_id directly)
- Dictionary refresh interval means user changes are delayed by 5-10 minutes (configurable via `LIFETIME`; can use `update_field` with `_version` for incremental refreshes)
- Dictionary source query needs `FINAL` to get deduplicated data from ReplacingMergeTree (acceptable — runs only at refresh time, not per-query)
- `gl_user` is exempt from traversal_path security (already in `SKIP_SECURITY_FILTER_TABLES`), so no security concerns
- Other node tables are NOT suitable for dictionaries due to security filter requirements
- Additional candidates for small reference tables: `gl_label`, `gl_milestone`, `gl_vulnerability_scanner`

**Impact:** Eliminates hash JOIN for user lookups. O(1) dictGet vs O(N) hash table build.

#### 11. Bidirectional Neighbor Query Refactor

**Problem:** Neighbor queries generate OR in the JOIN condition:
```sql
ON (u.id = e.source_id AND e.source_kind = 'User')
OR (u.id = e.target_id AND e.target_kind = 'User')
```
OR conditions in JOINs create separate hash tables per branch and prevent index usage.

**Solution:** Split into two queries combined with UNION ALL:
```sql
-- Outgoing neighbors (uses base table sort order)
SELECT e.target_id AS _gkg_neighbor_id, e.target_kind AS _gkg_neighbor_type, 1 AS _gkg_neighbor_is_outgoing
FROM gl_user AS u INNER JOIN gl_edge AS e ON u.id = e.source_id AND e.source_kind = 'User'
WHERE u.id = {p0:Int64}
UNION ALL
-- Incoming neighbors (uses reverse_lookup projection)
SELECT e.source_id, e.source_kind, 0
FROM gl_user AS u INNER JOIN gl_edge AS e ON u.id = e.target_id AND e.target_kind = 'User'
WHERE u.id = {p0:Int64}
LIMIT 10
```

**Impact:** Each branch uses the appropriate index/projection. Combined with recommendation #1 (reverse projection), both directions become index-assisted.

---

### P3: Investigated and Not Recommended

| Approach | Why Not |
|---|---|
| Explicit PREWHERE for `startsWith()` | **Unnecessary.** Verified in ClickHouse source (`MergeTreeWhereOptimizer.cpp`): `startsWith(traversal_path, ...)` is automatically promoted to PREWHERE because traversal_path is the first sorting key column and satisfies all viability checks (`isExpressionOverSortingKey`, `columnsSupportPrewhere`, `cannotBeMoved`). Adding explicit PREWHERE adds code complexity for zero performance gain. |
| Split `gl_edge` into per-type tables | High engineering cost (37+ tables, query engine rewrite for UNION ALL routing, path finding complexity). Skipping indexes on `relationship_kind` achieve 80% of the benefit at 5% of the cost. |
| AggregatingMergeTree / pre-computed aggregation | Materialized views trigger on INSERT only, not on ReplacingMergeTree deduplication/deletion. Pre-aggregated counts would never decrease, producing incorrect results for deleted/updated entities. |
| CollapsingMergeTree / VersionedCollapsingMergeTree | Requires sign-aware aggregation in every query and dual-row inserts in the indexer. ReplacingMergeTree is correct for our CDC upsert pattern. |
| Narrow/EAV table design | Destroys columnar compression, type safety, and SIMD performance. The ontology-driven wide table design is optimal for ClickHouse. |
| Materialized views for reverse edges | Projections (recommendation #1) achieve the same goal with simpler lifecycle, automatic consistency, and automatic query routing. Only use MV if independent tuning of the reverse table is needed. |
| Dictionaries for non-user node tables | Bypasses `traversal_path` security filtering and conflicts with the redaction pipeline. Only `gl_user` is viable. |
| Kùzu factorized processing in ClickHouse | ClickHouse is SQL-based; factorized vector representations cannot be expressed at the SQL level. The concept partially maps to avoiding unnecessary intermediate materialization via subqueries, but ClickHouse's query planner already handles this. |

---

### Query-Level Settings to Add

```sql
-- Force queries to use primary key (prevent accidental full scans in production)
SET force_primary_key = 1;

-- Optimize read order when ORDER BY matches sorting key
SET optimize_read_in_order = 1;  -- (default, but worth being explicit)

-- Memory safety for JOINs
SET max_bytes_in_join = 1073741824;  -- 1GB cap
SET join_overflow_mode = 'throw';

-- Query execution limits
SET max_execution_time = 30;  -- 30 second timeout
SET max_rows_to_read = 10000000;  -- 10M row cap
```

These should be set at the session/connection level in the ClickHouse client configuration.

---

### Diagnostic: EXPLAIN Usage

Before and after applying optimizations, validate with:
```sql
EXPLAIN PLAN indexes=1, projections=1
SELECT ...
```

Look for:
- **Parts before/after** at each index level (partition, primary key, skip index)
- **Granules before/after** — the key effectiveness metric
- **Projection** — confirms the reverse_lookup projection is being selected for incoming traversals
- **Search algorithm** — `binary search` or `generic exclusion search` on primary key

---

## Implementation Order

| Phase | Changes | Risk |
|---|---|---|
| **Phase 1 (DDL only)** | LowCardinality columns (#3), skipping indexes on node tables (#2), reverse projection on gl_edge (#1), query condition cache (#4), query-level settings | Zero query engine changes. Pure DDL + settings. Can be applied and rolled back independently. |
| **Phase 2 (Query engine)** | UNION ALL for neighbor queries (#11), SIP pre-filtering CTEs for multi-hop (#5), keyset pagination (#6) | Moderate codegen changes. Well-scoped to `lower.rs` and `codegen.rs`. |
| **Phase 3 (Query engine)** | Recursive CTE for multi-hop (#7), project_id denormalization (#9) | Recursive CTE replaces UNION ALL unroll in `lower.rs`. Denormalization requires indexer ETL + ontology changes. |
| **Phase 4 (Infrastructure)** | Partitioning strategy (#8), gl_user dictionary (#10) | Requires table recreation for partitioning, new dictionary lifecycle management. |

### Kùzu-Inspired Concepts Summary

| Kùzu Concept | ClickHouse Mapping | Status |
|---|---|---|
| Double-indexed edges (CSR forward + backward) | Reverse projection on gl_edge (#1) | Recommended — Phase 1 |
| Sideways information passing (SIP) | CTE pre-filtering for multi-hop (#5) | Recommended — Phase 2 |
| Sequential scan optimization | ClickHouse columnar + hash joins already avoid random I/O. `startsWith` auto-promoted to PREWHERE. | Already handled |
| Factorized processing | Cannot be expressed at SQL level. ClickHouse planner handles intermediate materialization. | Not applicable |
| Multiway WCO join for cycles | Path-finding CTE with `has()` cycle detection is efficient (O(4) bounded). | Already optimal |
| Morsel-driven parallelism | ClickHouse has native pipeline parallelism. Tune `max_threads` if needed. | Already handled |

### Verification Notes

These recommendations were verified against the ClickHouse source code:

- **`startsWith()` range conversion:** Confirmed in `KeyCondition.cpp:347` — `startsWith` is registered in `atom_map`, enabling primary key range analysis.
- **PREWHERE auto-promotion:** Confirmed in `MergeTreeWhereOptimizer.cpp:338-349` — `startsWith(traversal_path, ...)` passes all viability checks and is auto-promoted. Explicit PREWHERE is unnecessary.
- **Projection + FINAL compatibility:** Confirmed in `optimizeUseNormalProjection.cpp` — projection selection does NOT check `is_final`. Projections work with FINAL.
- **Query condition cache gating:** Confirmed in `QueryPlanOptimizationSettings.cpp:159` — requires `allow_experimental_analyzer = 1`.
- **`has()` cycle detection:** O(n) where n ≤ 4 (max_depth capped at 3 + start node). Negligible cost compared to edge joins.
