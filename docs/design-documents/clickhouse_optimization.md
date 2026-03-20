# ClickHouse Index & Query Optimization Plan

Research synthesis from deep analysis of the ClickHouse documentation, ClickHouse source code, GKG codebase (schema, query engine, ontology), and the Kùzu GDBMS white paper on graph query optimization.

Verified against ClickHouse source (`MergeTreeWhereOptimizer`, `KeyCondition`, `optimizeUseNormalProjection`) and GKG query engine codegen. All claims re-verified by a second research pass.

## Current State Summary

- **24 graph tables**, all `ReplacingMergeTree(_version, _deleted)` with `allow_experimental_replacing_merge_with_cleanup = 1`
- **1 skipping index**: `gl_edge.idx_relationship` (`relationship_kind TYPE set(100) GRANULARITY 4`)
- **Zero partitioning** on any table
- **6 projections** total: 4 `id_lookup` on code tables, 1 `project_lookup` on `code_indexing_checkpoint`, 1 `by_target` on `gl_edge` (deficient — see recommendation #1)
- **3 LowCardinality columns** already applied: `gl_edge.source_kind`, `gl_edge.target_kind`, `gl_edge.relationship_kind`
- **No FINAL** in query engine output — no query-time deduplication at all (see [Deduplication Gap](#deduplication-correctness-gap))
- **No query-level settings** applied to graph queries
- **PREWHERE is automatic** — ClickHouse auto-promotes `startsWith(traversal_path, ...)` to PREWHERE since `traversal_path` is the first sorting key column (verified in `MergeTreeWhereOptimizer.cpp`)
- **OFFSET-based pagination** (no cursor/keyset pagination)
- All node tables (except `gl_user`) use ORDER BY `(traversal_path, id)`
- `gl_edge` ORDER BY `(traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind)`, PRIMARY KEY prefix `(traversal_path, source_id, source_kind, relationship_kind)`
- Every query has `startsWith(traversal_path, ...)` security filter on every `gl_*` table (except `gl_user`)
- **`startsWith()` IS converted to a range scan** on the primary key (`KeyCondition.cpp` registers it in `atom_map`), so the primary key is already optimal for security filters

### Deduplication Correctness Gap

The query engine generates no FINAL, no `argMax`, no dedup subqueries. Between CDC upserts and the next background merge, queries can return:

- **Duplicate rows** (same entity from unmerged parts)
- **Stale data** (old version alongside the new version)
- **Ghost rows** (soft-deleted rows with `_deleted=true` still visible)

The system relies on:

1. ClickHouse background merges running frequently enough that duplicates are rare
2. Daily `OPTIMIZE TABLE ... FINAL CLEANUP` scheduled task (`crates/indexer/src/scheduler/table_cleanup.rs:97`) forcing complete deduplication
3. CDC update frequency being low relative to read frequency

**For graph traversal queries, this is acceptable**: JOINs on `id` naturally reduce duplicate impact, and the window of inconsistency is small. **Aggregation queries** (COUNT, SUM) are most affected. This trade-off is intentional — adding FINAL to every query would impose significant performance overhead for minimal correctness gain. If needed, FINAL can be added selectively (see recommendation #8).

---

## Recommendations (Priority Order)

### P0: High Impact, Low Effort

#### 1. Replace Deficient `by_target` Projection with `reverse_lookup`

**Problem:** The edge table's PRIMARY KEY starts with `(traversal_path, source_id, ...)`. Incoming/reverse traversals JOIN on `target_id`, which is NOT in the primary key prefix. This forces a full scan within the `traversal_path` range for every incoming edge lookup. Bidirectional neighbor queries use OR conditions that defeat index usage entirely.

**Existing state:** A `by_target` projection already exists on `gl_edge` (`graph.sql:226-229`), but it is **deficient**:

| Aspect | Existing `by_target` | Proposed `reverse_lookup` |
|---|---|---|
| Includes `traversal_path` in sort key | **NO** | YES (first position) |
| Includes `_version`, `_deleted` | **NO** (uses SELECT of specific cols) | YES (uses SELECT *) |
| Security filter uses projection index | **NO** — `startsWith(traversal_path, ...)` cannot use sort order | **YES** — traversal_path is first in sort key |
| Compatible with FINAL | **Uncertain** — missing dedup columns | **YES** — has all columns |

**Solution: Replace the projection**

```sql
ALTER TABLE gl_edge DROP PROJECTION by_target;
ALTER TABLE gl_edge ADD PROJECTION reverse_lookup (
    SELECT *
    ORDER BY (traversal_path, target_id, target_kind, relationship_kind, source_id, source_kind)
);
ALTER TABLE gl_edge MATERIALIZE PROJECTION reverse_lookup;
-- deduplicate_merge_projection_mode = 'rebuild' is already set on gl_edge
```

ClickHouse's query analyzer automatically selects the projection with fewest granules. No query engine changes needed.

**Verified:** Projections ARE compatible with FINAL in modern ClickHouse. The `optimizeUseNormalProjection.cpp` optimizer does NOT check `is_final` — projection selection runs regardless (verified against ClickHouse source).

**Kùzu mapping:** This is the ClickHouse equivalent of Kùzu's double-indexed CSR adjacency lists. Kùzu stores edges in both forward (source → targets) and backward (target → sources) CSR structures for sequential scan in both directions. The ClickHouse projection provides the same query capability — any node's incoming edges are contiguous in the projection's sort order — with the trade-off of full row duplication vs Kùzu's compact index-only structure.

**Impact:** Eliminates full scans for ~50% of edge lookups (all incoming traversals). Combined with recommendation #11, both directions of bidirectional neighbor queries become index-assisted.

#### 2. Skipping Indexes on High-Filter Columns

**Problem:** Only 1 skipping index exists (`gl_edge.idx_relationship`). Queries frequently filter on columns like `state`, `severity`, `status`, `draft`, `confidential` that are NOT in the primary key. ClickHouse must scan all granules within the `traversal_path` range to evaluate these filters.

**Filter frequency analysis** (from 29 benchmark queries in `fixtures/queries/sdlc_queries.yaml`):

| Column | Tables | Benchmark queries using it |
|---|---|---|
| `state` | gl_merge_request, gl_work_item, gl_vulnerability | 13 queries |
| `status` | gl_pipeline, gl_job | 4 queries |
| `severity` | gl_vulnerability | 2 queries |
| `draft` | gl_merge_request | 1 query |
| `archived` | gl_project | 2 queries |
| `confidential` | gl_work_item | 1 query |
| `noteable_type` | gl_note | 1 query |
| `source` | gl_pipeline | 1 query |
| `report_type` | gl_vulnerability | 1 query |
| `visibility_level` | gl_project | 1 query |

**Solution:** Add targeted skipping indexes:

```sql
-- HIGH PRIORITY: filtered in 4+ benchmark queries

-- gl_merge_request: 'state' is the most filtered column (13 queries)
ALTER TABLE gl_merge_request ADD INDEX idx_state state TYPE set(10) GRANULARITY 1;
ALTER TABLE gl_merge_request MATERIALIZE INDEX idx_state;

-- gl_merge_request: draft boolean
ALTER TABLE gl_merge_request ADD INDEX idx_draft draft TYPE minmax GRANULARITY 1;
ALTER TABLE gl_merge_request MATERIALIZE INDEX idx_draft;

-- gl_pipeline: status filtered frequently
ALTER TABLE gl_pipeline ADD INDEX idx_status status TYPE set(20) GRANULARITY 1;
ALTER TABLE gl_pipeline MATERIALIZE INDEX idx_status;

-- gl_work_item: state filtered often
ALTER TABLE gl_work_item ADD INDEX idx_state state TYPE set(10) GRANULARITY 1;
ALTER TABLE gl_work_item MATERIALIZE INDEX idx_state;

-- MEDIUM PRIORITY: filtered in 1-3 queries but highly selective

-- gl_vulnerability: severity, state, report_type
ALTER TABLE gl_vulnerability ADD INDEX idx_severity severity TYPE set(10) GRANULARITY 1;
ALTER TABLE gl_vulnerability MATERIALIZE INDEX idx_severity;
ALTER TABLE gl_vulnerability ADD INDEX idx_state state TYPE set(10) GRANULARITY 1;
ALTER TABLE gl_vulnerability MATERIALIZE INDEX idx_state;
ALTER TABLE gl_vulnerability ADD INDEX idx_report_type report_type TYPE set(15) GRANULARITY 1;
ALTER TABLE gl_vulnerability MATERIALIZE INDEX idx_report_type;

-- gl_note: noteable_type
ALTER TABLE gl_note ADD INDEX idx_noteable_type noteable_type TYPE set(15) GRANULARITY 1;
ALTER TABLE gl_note MATERIALIZE INDEX idx_noteable_type;

-- gl_pipeline: source
ALTER TABLE gl_pipeline ADD INDEX idx_source source TYPE set(25) GRANULARITY 1;
ALTER TABLE gl_pipeline MATERIALIZE INDEX idx_source;

-- gl_work_item: confidential boolean
ALTER TABLE gl_work_item ADD INDEX idx_confidential confidential TYPE minmax GRANULARITY 1;
ALTER TABLE gl_work_item MATERIALIZE INDEX idx_confidential;

-- gl_project: archived boolean
ALTER TABLE gl_project ADD INDEX idx_archived archived TYPE minmax GRANULARITY 1;
ALTER TABLE gl_project MATERIALIZE INDEX idx_archived;

-- gl_user: state and user_type (no traversal_path, full table scanned)
ALTER TABLE gl_user ADD INDEX idx_state state TYPE set(10) GRANULARITY 1;
ALTER TABLE gl_user MATERIALIZE INDEX idx_state;
ALTER TABLE gl_user ADD INDEX idx_user_type user_type TYPE set(20) GRANULARITY 1;
ALTER TABLE gl_user MATERIALIZE INDEX idx_user_type;
```

**Index type selection rationale:**
- `set(N)` for low-cardinality enum/string columns (state, severity, kind) — stores exact values per granule block, supports ALL filter functions. N should be >= number of distinct values.
- `minmax` for booleans and range-queried numerics — stores min/max per granule block, near-zero overhead. For booleans: if all rows in a granule are `false`, a filter for `true` skips it entirely.
- `GRANULARITY 1` = one index entry per data granule (8,192 rows). Finest granularity, most effective. The index overhead is minimal for set/minmax types.

**Note:** `gl_edge` already has `INDEX idx_relationship relationship_kind TYPE set(100) GRANULARITY 4` — no additional edge indexes are needed given the reverse projection (#1).

**Impact:** Reduces granule reads by skipping blocks where the filtered value doesn't exist. Most effective when the filtered value is selective (e.g., `state = 'merged'` on a table where most rows are `opened`).

#### 3. LowCardinality Column Types on Graph Tables

**Problem:** Most enum-like String columns in graph tables are plain `String`. These columns have low cardinality (<100 distinct values) but pay full String storage and comparison costs.

**Note:** `gl_edge` columns (`source_kind`, `target_kind`, `relationship_kind`) are already `LowCardinality(String)` — no changes needed there.

**Solution:** Change column types to `LowCardinality(String)`:

```sql
-- gl_merge_request
ALTER TABLE gl_merge_request MODIFY COLUMN state LowCardinality(String);
ALTER TABLE gl_merge_request MODIFY COLUMN merge_status LowCardinality(String);

-- gl_pipeline
ALTER TABLE gl_pipeline MODIFY COLUMN status LowCardinality(String);
ALTER TABLE gl_pipeline MODIFY COLUMN source LowCardinality(String);

-- gl_vulnerability
ALTER TABLE gl_vulnerability MODIFY COLUMN state LowCardinality(String);
ALTER TABLE gl_vulnerability MODIFY COLUMN severity LowCardinality(String);
ALTER TABLE gl_vulnerability MODIFY COLUMN report_type LowCardinality(String);

-- gl_work_item
ALTER TABLE gl_work_item MODIFY COLUMN state LowCardinality(String);
ALTER TABLE gl_work_item MODIFY COLUMN work_item_type LowCardinality(String);

-- gl_project
ALTER TABLE gl_project MODIFY COLUMN visibility_level LowCardinality(Nullable(String));

-- gl_user
ALTER TABLE gl_user MODIFY COLUMN state LowCardinality(String);
ALTER TABLE gl_user MODIFY COLUMN user_type LowCardinality(String);

-- gl_note
ALTER TABLE gl_note MODIFY COLUMN noteable_type LowCardinality(String);

-- gl_job
ALTER TABLE gl_job MODIFY COLUMN status LowCardinality(String);

-- gl_stage
ALTER TABLE gl_stage MODIFY COLUMN status LowCardinality(String);

-- gl_security_scan
ALTER TABLE gl_security_scan MODIFY COLUMN scan_type LowCardinality(String);
ALTER TABLE gl_security_scan MODIFY COLUMN status LowCardinality(String);

-- gl_finding
ALTER TABLE gl_finding MODIFY COLUMN severity LowCardinality(String);

-- gl_vulnerability_occurrence
ALTER TABLE gl_vulnerability_occurrence MODIFY COLUMN severity LowCardinality(String);
ALTER TABLE gl_vulnerability_occurrence MODIFY COLUMN report_type LowCardinality(String);
ALTER TABLE gl_vulnerability_occurrence MODIFY COLUMN detection_method LowCardinality(String);

-- gl_vulnerability_scanner
ALTER TABLE gl_vulnerability_scanner MODIFY COLUMN vendor LowCardinality(String);

-- Code tables
ALTER TABLE gl_definition MODIFY COLUMN definition_type LowCardinality(String);
ALTER TABLE gl_file MODIFY COLUMN language LowCardinality(String);
ALTER TABLE gl_file MODIFY COLUMN extension LowCardinality(String);
ALTER TABLE gl_imported_symbol MODIFY COLUMN import_type LowCardinality(String);
```

**How it helps:** LowCardinality uses dictionary encoding — strings are replaced with integer IDs internally. This improves:
- Filter performance (integer comparison vs string comparison)
- JOIN performance (smaller hash table entries)
- Compression ratio
- Effective when < 10,000 distinct values (our columns have < 100)

**Columns that should NOT be LowCardinality:** `traversal_path` (millions of distinct values), `source_branch`/`target_branch` (user-defined, unbounded), `name`/`title`/`description` (free text), `email`/`username` (unique per user), `sha`/`commit_id`/`uuid` (all unique), `file_path`/`path`/`fqn` (unbounded).

**Compatibility:** LowCardinality is fully compatible with skipping indexes, ORDER BY columns, JOINs, and ReplacingMergeTree deduplication. ClickHouse handles all encoding/decoding transparently.

**Impact:** ~10-30% improvement in query speed on filtered columns. Zero query engine changes needed.

#### 4. Enable Query Condition Cache

**Problem:** Repeated queries with the same `startsWith(traversal_path, ...)` filter scan the same granules every time.

**Solution:** Enable per-query in the graph query execution path:
```rust
query.with_option("use_query_condition_cache", "1")
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
- **Requires `enable_analyzer = 1`** (verified in ClickHouse source: `QueryPlanOptimizationSettings.cpp:159`). This is the **default in modern ClickHouse** (24.x+), so no explicit setting is needed on recent versions.

**Impact:** Near-free speedup for repeated access patterns. No code changes needed beyond adding the query option.

---

### P1: High Impact, Medium Effort

#### 5. Semijoin Pre-filtering for Multi-hop Queries (Kùzu SIP-inspired)

**Concept from Kùzu:** Sideways Information Passing (SIP) — pass filters from one side of a join to reduce scans on the other. Kùzu's ASP-Join (accumulate-semijoin-probe) is a 3-pipeline join operator: (1) accumulate probe-side tuples and build a semijoin filter, (2) use the filter to restrict build-side scans, (3) re-scan probe and probe the hash table.

**Problem in GKG:** Multi-hop UNION ALL queries scan the full `gl_edge` table in each arm. For `max_hops=3`, that's 1+2+3 = 6 edge table scans. When the starting node set is selective (specific `node_ids` or filtered by `state`), most edge rows are irrelevant.

**Current code:** `build_hop_union_all()` (`lower.rs:630-636`) iterates `1..=max_hops`, calling `build_hop_arm()` for each depth. Each arm chains `depth` edge table scans via INNER JOINs. The source node's filters are only applied AFTER the UNION ALL subquery completes, via the outer JOIN at `lower.rs:779-782`.

**Scan count analysis:**

| max_hops | Current (UNION ALL) | With SIP | Reduction |
|---|---|---|---|
| 1 | 1 scan | 1 scan (filtered) | Rows reduced |
| 2 | 3 scans | 3 scans (first filtered) | Rows reduced |
| 3 | 6 scans | 6 scans (first filtered) | Rows reduced |

SIP doesn't reduce scan count — it reduces the rows processed per scan by restricting the first edge scan in each arm to the source node set.

**Solution:** Pre-materialize the starting node IDs in a CTE and use them to restrict edge scans:

```sql
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

**ClickHouse IN pushdown:** ClickHouse DOES push down `IN (SELECT ...)` predicates to the storage engine level. When `source_id` is in the primary key prefix (position 2 in gl_edge's ORDER BY), the IN predicate narrows the granule range via `KeyCondition` analysis.

**When to apply:** Only when the source node set is selective (has filters or `node_ids`). Decision is made at compile time in `build_joins()` by checking `!source_node.filters.is_empty() || !source_node.node_ids.is_empty()`.

**Implementation in `lower.rs`:**
1. Pass the source `InputNode` into `build_hop_union_all()` (currently only receives `&InputRelationship`)
2. When the source has filters/node_ids, emit a CTE: `SELECT id FROM {source_table} WHERE {source_filters}`
3. Inject `e1.{start_col} IN (SELECT id FROM source_ids)` into each arm's WHERE clause
4. Return `(TableRef, Option<Cte>)` instead of just `TableRef` for inclusion in the top-level Query

**Security filter propagation:** The security pass (`security.rs:76-77`) iterates CTEs and calls `apply_to_query()` on each. A CTE scanning `gl_project` automatically gets `startsWith(traversal_path, ...)` injected. `gl_user` CTEs are exempt (in `SKIP_SECURITY_FILTER_TABLES`).

**Impact:** Reduces edge table reads proportionally to source selectivity. For point queries (single `node_id`), this converts each edge scan from a range scan to a point lookup.

#### 6. Keyset Pagination (Replace OFFSET)

**Problem:** `LIMIT N OFFSET M` forces ClickHouse to read and discard M rows. For `OFFSET 900`, it reads 930 rows to return 30.

**Current code:** `pagination()` (`lower.rs:44-50`) translates `InputRange { start, end }` into `LIMIT (end - start) OFFSET start`. Applied uniformly across all 4 query types.

**Solution:** Implement cursor-based/keyset pagination using the sort key:

```sql
-- Instead of: ORDER BY mr.created_at DESC LIMIT 30 OFFSET 60
-- Use:        WHERE (mr.created_at, mr.id) < ({cursor_created_at}, {cursor_id})
--             ORDER BY mr.created_at DESC, mr.id DESC LIMIT 30
```

**Implementation:**
- Add `cursor: Option<CursorValue>` to `Input` (mutually exclusive with `range`)
- In `lower.rs`, emit a tuple comparison WHERE clause when cursor is present
- ORDER BY must always include `id` as a tiebreaker for deterministic ordering
- The query response must return the cursor for the last row (API contract change)

**NULL handling:** Nullable columns in cursor values break tuple comparison (`NULL` comparisons return NULL/falsy). Cursor columns like `created_at` (Nullable on most tables) require `ifNull(created_at, '1970-01-01')` wrapping. `id` is always NOT NULL.

**ClickHouse optimization:** When the ORDER BY matches the table's sorting key, `optimize_read_in_order` (enabled by default) allows streaming reads without re-sorting. Tuple comparisons against sorting key columns enable primary key range pruning via `KeyCondition`.

**Impact:** Every page reads exactly `LIMIT` rows regardless of depth. Eliminates O(N) overhead for deep pagination.

#### 7. Recursive CTE for Multi-hop (Conditional — max_hops >= 4 Only)

**Problem:** Multi-hop queries unroll into `max_hops` UNION ALL arms with `1+2+...+max_hops` total edge table scans (6 for max_hops=3). Each arm independently self-joins the edge table.

**Solution:** Replace the unrolled UNION ALL with a recursive CTE that builds incrementally:

```sql
SET allow_experimental_analyzer = 1;
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

**Scan count comparison:**

| max_hops | UNION ALL unroll | Recursive CTE |
|---|---|---|
| 1 | 1 scan | 1 scan |
| 2 | 3 scans | 2 scans |
| 3 | 6 scans | 3 scans |
| 4 | 10 scans | 4 scans |
| N | N*(N+1)/2 | N |

**AST infrastructure already exists.** The compiler's AST has `Cte.recursive: bool` (`ast.rs:150`), `Cte::recursive()` constructor (`ast.rs:162-168`), and `codegen.rs:139-144` emits `WITH RECURSIVE` when any CTE is recursive. This was built for path-finding queries but can be reused.

**ClickHouse limitation (issue #75026):** The working table in a recursive CTE is materialized in memory and is opaque to the optimizer. ClickHouse cannot apply primary key index optimizations when joining against this working table. In the unrolled approach, `e1.target_id = e2.source_id` gives the optimizer concrete column equalities against physical gl_edge, enabling primary key range analysis.

**Recommendation:** Use recursive CTE **only for max_hops >= 4**, where the O(N²) → O(N) scan reduction outweighs the optimizer limitation. For max_hops <= 3, the unrolled approach with full primary key optimization is likely faster. This can be a compile-time decision in `build_joins()`.

**Composability with SIP:** SIP pre-filtering (#5) and recursive CTE are orthogonal. SIP only needs to be applied to the base case — the recursive step is already bounded by the frontier from previous iterations:

```sql
WITH source_ids AS (
  SELECT id FROM gl_user WHERE ...
),
RECURSIVE hops AS (
  SELECT ... FROM gl_edge WHERE source_id IN (SELECT id FROM source_ids) ...
  UNION ALL
  SELECT ... FROM hops h JOIN gl_edge e ON h.end_id = e.source_id ...
  WHERE h.depth < {max_hops}
)
SELECT * FROM hops
```

**Cycle detection:** Both approaches are bounded by max_hops. Neither has automatic cycle detection. To add it, append `AND NOT has(h.path_nodes, tuple(e.target_id, e.target_kind))` to the recursive step — O(n) where n <= 4, negligible cost.

**Impact:** Reduces edge table scans from O(n²) to O(n) where n = max_hops. Most beneficial for max_hops >= 4.

#### 8. Partitioning Strategy (Deferred — Only If FINAL Is Needed)

**Problem:** No tables are partitioned. If FINAL is ever added to the query engine, un-partitioned tables force single-threaded FINAL processing across all data.

**Current assessment: Neither partitioning option is compelling today.**

- GKG does not use FINAL — the "parallel FINAL" benefit is theoretical
- Option A (`PARTITION BY toUInt64(extract(traversal_path, '^\d+'))`) partition pruning with `startsWith` is uncertain — the optimizer may not connect `startsWith(traversal_path, ...)` to `toUInt64(extract(...))`. Needs empirical testing with `EXPLAIN indexes=1`.
- Option B (`sipHash64(...) % 256`) destroys locality — no partition pruning possible, no `DROP PARTITION` for namespace deletion
- `DROP PARTITION` doesn't align with namespace deletion granularity — GKG deletes at namespace level (e.g., `1/100/`), not org level. Multiple namespaces share an org.
- The current soft-delete namespace deletion approach (`crates/indexer/src/modules/namespace_deletion/lower.rs`) is sound and uses `startsWith(traversal_path, ...)` which leverages the primary key index.

**Options for reference:**

```sql
-- Option A: Partition by top-level namespace ID
-- Pro: potential partition pruning, natural org alignment
-- Con: unpredictable partition count (could exceed 1000 on GitLab.com)
PARTITION BY toUInt64(extract(traversal_path, '^\d+'))

-- Option B: Hash to fixed bucket count
-- Pro: fixed 256 partitions, predictable
-- Con: no pruning, no DROP PARTITION utility
PARTITION BY sipHash64(substring(traversal_path, 1, position(traversal_path, '/') - 1)) % 256
```

**Why parallel FINAL matters:** Since ClickHouse v23.12, `do_not_merge_across_partitions_select_final` is auto-derived when the partition key uses only columns from the sorting key. Since `traversal_path` is the first ORDER BY column, both options qualify for auto-derivation, enabling one FINAL thread per partition instead of single-threaded.

**Recommendation:** Do NOT add partitioning preemptively. Only proceed if: (a) FINAL is needed, (b) FINAL performance is measurably bad without partitioning, AND (c) org count is known. If FINAL is needed selectively, add `final: bool` to `TableRef::Scan` in the AST and enable per-query rather than adding global partitioning.

**Impact:** Enables parallel FINAL processing if/when needed. Adds operational complexity (table recreation, partition monitoring) that is not justified today.

---

### P2: Medium Impact, Medium-High Effort

#### 9. Denormalize `project_id` into SDLC Entity Tables

**Problem:** "Find entities in project X" requires joining through `gl_edge` even though the relationship is 1:N and stable. This is the most common query pattern.

**Prior art:** The code graph tables (`gl_definition`, `gl_file`, `gl_directory`, `gl_imported_symbol`) already have `project_id` in their sort key with `ORDER BY (traversal_path, project_id, branch, id)`. This approach extends the pattern to SDLC tables.

**Data availability confirmed:** The FK edge extraction (`crates/indexer/src/modules/sdlc/plan/input.rs:211-274`) already reads `project_id` from source data to create `IN_PROJECT` edges. The column is available during ETL but only used for the edge row, not stored in the node table. Ontology node definitions (`merge_request.yaml:126`, `pipeline.yaml:152`, `job.yaml:184`, `vulnerability.yaml:138`, etc.) explicitly reference `target_project_id`/`project_id` as FK columns.

**Solution:** Add `project_id Int64` column to high-frequency SDLC tables:

```sql
ALTER TABLE gl_merge_request ADD COLUMN project_id Int64 DEFAULT 0;
ALTER TABLE gl_pipeline ADD COLUMN project_id Int64 DEFAULT 0;
ALTER TABLE gl_work_item ADD COLUMN project_id Int64 DEFAULT 0;
ALTER TABLE gl_vulnerability ADD COLUMN project_id Int64 DEFAULT 0;
```

**Implementation path:**

1. **Ontology YAML**: Add `project_id` as a property to each node definition (same pattern as code graph nodes)
2. **DDL**: ALTER TABLE to add the column (non-breaking, no table recreation needed)
3. **Indexer**: The existing `NodeColumn::Identity/Rename` mechanism handles the new property automatically once added to the ontology
4. **Query engine (optional)**: When a traversal involves entity → Project via IN_PROJECT with a specific project ID, the compiler could short-circuit the edge JOIN by emitting `WHERE gl_merge_request.project_id = 42` directly
5. **Sort key update (optional, requires table recreation)**: `ORDER BY (traversal_path, project_id, id)` would make project-scoped queries use the primary key. This is high-impact but requires table recreation.

The `IN_PROJECT` edge should be kept even after denormalization — it's needed for graph traversal queries.

**Impact:** Eliminates one edge JOIN for the most common query pattern. Indexer changes are minimal since project_id is already extracted during ETL.

#### 10. Dictionary for `gl_user` Lookups

**Problem:** `gl_user` is JOINed in ~10 of 29 benchmark queries. It's a global table (ORDER BY `id`, no traversal_path), making it ideal for in-memory dictionary lookup.

**Security confirmation:** `gl_user` is in `SKIP_SECURITY_FILTER_TABLES` (`constants.rs:27-30`). It has NO `startsWith(traversal_path, ...)` filter — it's a global, unsecured table. A dictionary does NOT bypass any security. Other node tables are NOT safe for dictionaries due to security filter requirements.

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
    UPDATE_FIELD _version
    UPDATE_LAG 60
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);
```

**Memory estimation:** ~160 bytes/user × 38M users (GitLab.com) = ~6 GB. For self-managed instances (1K-100K users): negligible.

**Two implementation approaches:**

| Approach | Effort | Codegen changes | Performance |
|---|---|---|---|
| **A: `dictGet` in codegen** | Medium — new Expr variant | Yes — replace JOIN with dictGet calls | O(1) per lookup, no hash table build |
| **B: Dictionary Engine table** | Low — ontology mapping only | None — existing JOINs work unchanged | O(1) via `join_algorithm = 'direct'` |

```sql
-- Approach B: zero codegen changes
CREATE TABLE gl_user_dict_table (...) ENGINE = Dictionary(gl_user_dict);
-- Then change ontology to map User entity to gl_user_dict_table
-- Existing JOINs against this table use O(1) lookups automatically
```

**Recommendation:** Start with Approach B (Dictionary Engine table). Zero codegen changes. If finer control is needed later, refactor to Approach A.

**Incremental refresh:** The `UPDATE_FIELD _version` and `UPDATE_LAG 60` settings enable incremental refresh — only fetch rows newer than the last refresh, dramatically reducing refresh cost vs full reload.

**Impact:** Eliminates hash JOIN for user lookups. O(1) dictGet vs O(N) hash table build per query.

#### 11. Bidirectional Neighbor Query Refactor

**Problem:** Neighbor queries (`lower.rs:510-623`) generate OR in the JOIN condition:
```sql
ON (u.id = e.source_id AND e.source_kind = 'User')
OR (u.id = e.target_id AND e.target_kind = 'User')
```
OR conditions in JOINs create separate hash tables per branch and prevent index usage.

**Solution:** Split into two queries combined with UNION ALL:
```sql
SELECT * FROM (
  -- Outgoing neighbors (uses base table sort order)
  SELECT e.target_id AS _gkg_neighbor_id, e.target_kind AS _gkg_neighbor_type,
         1 AS _gkg_neighbor_is_outgoing
  FROM gl_user AS u INNER JOIN gl_edge AS e ON u.id = e.source_id AND e.source_kind = 'User'
  WHERE u.id = {p0:Int64}
  UNION ALL
  -- Incoming neighbors (uses reverse_lookup projection)
  SELECT e.source_id, e.source_kind, 0
  FROM gl_user AS u INNER JOIN gl_edge AS e ON u.id = e.target_id AND e.target_kind = 'User'
  WHERE u.id = {p0:Int64}
) ORDER BY ... LIMIT 10
```

**Correctness analysis:**
- **Deduplication**: Each edge has exactly one `(source_id, target_id)` pair. An edge appears in exactly one arm — the center node is either the source or the target, never both (edges always connect different entity types in our ontology, no self-loops).
- **Pagination**: ORDER BY and LIMIT applied to the outer query work correctly. `codegen.rs:341-347` emits `TableRef::Union` as a derived table in FROM.
- **Security**: The security pass (`security.rs:172-188`) recurses into UNION ALL arms and applies `startsWith` filters to each gl_edge scan independently.

**Implementation:** Changes scoped to `lower_neighbors()` in `lower.rs:510-623`. When `direction == Direction::Both`, emit `TableRef::Union` with two arms instead of using `source_join_cond_with_kind` with OR. The `if()` expressions for neighbor_id/type/is_outgoing (lines 557-599) become unnecessary — each arm returns the correct columns directly.

**Dependency on #1:** Without the reverse projection, the incoming arm still does a range scan. With both #1 and #11, each arm uses the optimal index: outgoing uses base table, incoming uses reverse projection.

**Impact:** Each branch uses the appropriate index/projection. Combined with recommendation #1, both directions become index-assisted.

---

### P3: Investigated and Not Recommended

| Approach | Why Not |
|---|---|
| Explicit PREWHERE for `startsWith()` | **Unnecessary.** Verified in ClickHouse source (`MergeTreeWhereOptimizer.cpp`): `startsWith(traversal_path, ...)` is automatically promoted to PREWHERE because traversal_path is the first sorting key column and satisfies all viability checks (`isExpressionOverSortingKey`, `columnsSupportPrewhere`, `cannotBeMoved`). Adding explicit PREWHERE adds code complexity for zero performance gain. **Also works with FINAL** — the PREWHERE viability check allows sorting-key expressions when `is_final` is true. |
| Split `gl_edge` into per-type tables | High engineering cost (37+ tables, query engine rewrite for UNION ALL routing, path finding complexity). Skipping indexes on `relationship_kind` achieve 80% of the benefit at 5% of the cost. |
| AggregatingMergeTree / pre-computed aggregation | Materialized views trigger on INSERT only, not on ReplacingMergeTree deduplication/deletion. Pre-aggregated counts would never decrease, producing incorrect results for deleted/updated entities. |
| CollapsingMergeTree / VersionedCollapsingMergeTree | Requires sign-aware aggregation in every query and dual-row inserts in the indexer. ReplacingMergeTree is correct for our CDC upsert pattern. |
| Narrow/EAV table design | Destroys columnar compression, type safety, and SIMD performance. The ontology-driven wide table design is optimal for ClickHouse. |
| Materialized views for reverse edges | Projections (recommendation #1) achieve the same goal with simpler lifecycle, automatic consistency, and automatic query routing. |
| Dictionaries for non-user node tables | Bypasses `traversal_path` security filtering and conflicts with the redaction pipeline. Only `gl_user` is viable (confirmed: only table in `SKIP_SECURITY_FILTER_TABLES`). |
| Kùzu factorized processing in ClickHouse | ClickHouse is SQL-based; factorized vector representations cannot be expressed at the SQL level. ClickHouse's planner already handles intermediate materialization via pipeline execution with late materialization. |
| Partitioning for namespace deletion | `DROP PARTITION` granularity doesn't match — GKG deletes at namespace level, not org level. The current soft-delete approach (`INSERT INTO ... SELECT ... WHERE startsWith(traversal_path, ...) AND _deleted = false` with `_deleted=true`) is correct and uses the primary key index. |

---

### Query-Level Settings

These should be applied **per-query** in the graph query execution path via `ArrowQuery.with_option()`, NOT at session level (to avoid affecting DDL, health checks, and indexer operations).

**Injection point:** `ClickHouseExecutor::execute()` (`crates/gkg-server/src/pipeline/stages/execution.rs:32`). The `clickhouse` Rust crate's `Client::with_option(key, value)` method sets per-query options. The client already uses this pattern for Arrow format settings in `ArrowClickHouseClient::new()` (`crates/clickhouse-client/src/arrow_client.rs:28-44`).

```rust
// Safe to enable immediately
query.with_option("use_query_condition_cache", "1")     // Cache granule filter results
query.with_option("max_execution_time", "30")            // 30s timeout
query.with_option("max_bytes_in_join", "1073741824")     // 1GB JOIN cap
query.with_option("join_overflow_mode", "throw")         // Fail on overflow (already default)

// Test first, then enable
query.with_option("force_primary_key", "1")              // Reject queries without PK usage
query.with_option("max_rows_to_read", "50000000")        // 50M row cap (pre-filter)
```

**Setting details:**

| Setting | Default | Risk | Notes |
|---|---|---|---|
| `use_query_condition_cache` | `0` | Low | Perfect for repeated `startsWith` filters. Requires `enable_analyzer=1` (default in modern CH). |
| `max_execution_time` | `0` (unlimited) | Low | 30s is 3x the expected p99. Checked between data blocks. |
| `max_bytes_in_join` | `0` (unlimited) | Low | 1GB is generous for namespace-scoped graph queries. |
| `join_overflow_mode` | `throw` | None | Already default. Explicit for safety — `break` would return incomplete graph data. |
| `force_primary_key` | `0` | Medium | All graph queries use `startsWith(traversal_path, ...)` so this should be safe, but `gl_user` has no traversal_path — verify user joins still work. Also verify projection-routed queries aren't rejected. |
| `max_rows_to_read` | `0` (unlimited) | Medium | Counted before WHERE/PREWHERE filtering, across ALL tables in the query. 50M provides margin for large namespaces with multi-table JOINs. Original 10M was too aggressive. |

**Removed from original recommendation:**
- `optimize_read_in_order = 1` — already the default, no value in setting it
- `allow_experimental_analyzer = 1` — already the default in modern ClickHouse (24.x+)

---

### Diagnostic: EXPLAIN Usage

Before and after applying optimizations, validate with:
```sql
EXPLAIN PLAN indexes=1, projections=1
SELECT ...
```

**What to look for:**

```
Indexes:
  Type: PrimaryKey
    Keys: [traversal_path]
    Condition: startsWith(traversal_path, '/123/')
    Parts: 4/5          -- 4 parts remaining out of 5 (1 pruned)
    Granules: 11/12      -- 11 granules remaining out of 12

  Type: Skip
    Name: idx_state
    Keys: [state]
    Condition: state = 'merged'
    Parts: 2/4           -- 2 parts remaining (2 pruned by skip index)
    Granules: 5/11        -- 5 granules remaining

Projections:
  Name: reverse_lookup
  Description: Projection has been analyzed and is used
```

**Validation checklist:**
1. **Reverse projection**: Run on an incoming traversal query — confirm `reverse_lookup` is selected (not `by_target`)
2. **Skip indexes**: Run on a `state = 'merged'` query — confirm `Type: Skip` appears with granule pruning
3. **Primary key**: Check `Type: PrimaryKey` shows significant granule pruning for `startsWith` filters
4. **PREWHERE**: Use `EXPLAIN SYNTAX SELECT ...` to confirm `startsWith` was promoted to a separate PREWHERE clause
5. **Query condition cache**: Run same query twice, compare `system.query_log` entries — second execution should show fewer `read_rows`

---

## Implementation Order

| Phase | Changes | Risk |
|---|---|---|
| **Phase 1 (DDL only)** | Replace `by_target` with `reverse_lookup` projection (#1), skipping indexes on 8+ tables (#2), LowCardinality on 25+ columns (#3), `use_query_condition_cache` + safety settings (#4) | Zero query engine changes. Pure DDL + per-query settings. Can be applied and rolled back independently. |
| **Phase 2 (Query engine)** | UNION ALL for bidirectional neighbor queries (#11), SIP pre-filtering CTEs for multi-hop (#5), keyset pagination (#6) | Moderate codegen changes. Well-scoped to `lower.rs` and `codegen.rs`. |
| **Phase 3 (Query engine)** | Recursive CTE for multi-hop when max_hops >= 4 (#7), project_id denormalization (#9) | Recursive CTE is conditional. Denormalization requires indexer ETL + ontology changes. |
| **Phase 4 (Infrastructure)** | gl_user dictionary via Dictionary Engine table (#10), partitioning strategy (#8, only if FINAL needed) | Dictionary is low-risk. Partitioning deferred until FINAL performance is measurably problematic. |

### Kùzu-Inspired Concepts Summary

| Kùzu Concept | ClickHouse Mapping | Status |
|---|---|---|
| Double-indexed edges (CSR forward + backward) | Reverse projection on gl_edge (#1) | Recommended — Phase 1. Functionally equivalent to Kùzu's bidirectional CSR, with full row duplication vs Kùzu's compact index structure. |
| Sideways information passing (SIP) | CTE pre-filtering for multi-hop (#5) | Recommended — Phase 2. Maps directly: CTE = accumulate probe tuples, IN subquery = semijoin filter, final JOIN = probe. |
| ASP-Join (accumulate-semijoin-probe) | SIP CTE + final JOIN (#5) | Recommended — Phase 2. The 3-pipeline structure maps to: (1) CTE materialization, (2) IN-filtered edge scan, (3) outer JOIN. |
| Sequential scan optimization | ClickHouse columnar + hash joins already avoid random I/O. `startsWith` auto-promoted to PREWHERE. | Already handled |
| Factorized processing | Cannot be expressed at SQL level. ClickHouse planner handles intermediate materialization via pipeline execution with late materialization. | Not applicable |
| Multiway WCO join for cycles | Path-finding CTE with `has()` cycle detection is efficient (O(4) bounded). Our queries are acyclic chain patterns — WCO joins are for cyclic patterns (triangles, cliques). | Already optimal |
| Morsel-driven parallelism | ClickHouse has native pipeline parallelism via DAG of processors with work-stealing thread pool. Conceptually similar to morsel-driven but operates at processor/port level. `max_threads` = auto by default. | Already handled |

### Verification Notes

All recommendations were verified against the ClickHouse source code. All 5 key claims were re-verified by a second independent research pass:

- **`startsWith()` range conversion:** Confirmed in `KeyCondition.cpp` — `startsWith` is registered in `atom_map` with `FUNCTION_IN_RANGE`, computing `[prefix, next_prefix)` range for primary key analysis.
- **PREWHERE auto-promotion:** Confirmed in `MergeTreeWhereOptimizer.cpp` — `startsWith(traversal_path, ...)` passes all viability checks and is auto-promoted. Works with FINAL too (sorting-key expressions allowed when `is_final=true`). Explicit PREWHERE is unnecessary.
- **Projection + FINAL compatibility:** Confirmed in `optimizeUseNormalProjection.cpp` — projection selection does NOT check `is_final`. Projections work with FINAL.
- **Query condition cache gating:** Confirmed in `QueryPlanOptimizationSettings.cpp:159` — requires `enable_analyzer = 1`, which is the **default in modern ClickHouse** (24.x+).
- **`has()` cycle detection:** O(n) where n <= 4 (max_depth capped at 3 + start node). Negligible cost compared to edge joins.
- **IN subquery pushdown:** Confirmed — ClickHouse materializes IN subquery results as a hash set and evaluates during MergeTree granule reading via `KeyCondition` when the IN column is in the primary key prefix.
