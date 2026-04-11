---
title: "GKG ADR 009: Indexing progress via NATS KV"
creation-date: "2026-04-10"
authors: [ "@michaelangeloio" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-04-10

## Context

Admins and automated systems currently have no lightweight way to answer basic
questions about indexing state: is indexing running, how far along is initial
backfill, what entity counts exist for a given namespace, or when did code
indexing last complete for a project. Today the only options are reading logs,
querying ClickHouse directly, or parsing checkpoint table internals.

We need an endpoint on the GKG webserver that Rails can proxy to expose
indexing progress. This endpoint serves four audiences:

1. **Namespace admins** checking rollout status after enabling Knowledge Graph.
2. **GitLab Rails** proxying progress to configuration UI pages.
3. **E2E test harnesses** polling for indexing completion before executing query
   assertions against the full GKG stack.
4. **Developer observability.** Developers enabling Knowledge Graph have no
   feedback loop today. They enable it, wait, and get nothing until they try a
   query. This endpoint closes that gap by surfacing what has been indexed and
   whether the system is healthy.

The previous design (issue #175) proposed deriving all state at query time from
ClickHouse checkpoint and graph tables. This ADR replaces that approach with a
NATS KV-backed progress store, where the indexer writes progress as a
side-effect of ETL and the webserver reads from NATS KV with no ClickHouse
dependency on the read path.

### Why move away from ClickHouse-derived progress

The ClickHouse-derived approach has three problems:

1. **`FINAL` cost at scale.** Checkpoint and graph count queries require
   `FINAL` to collapse `ReplacingMergeTree` versions. On large namespaces with
   many entity types, the webserver would run 15+ `FINAL` queries per request.
   These are cheap individually but add up under concurrent polling.

2. **Cross-concern coupling.** The webserver would need to understand checkpoint
   key formats (`ns.{id}.{plan}`), cursor semantics, and code checkpoint
   schemas. This leaks indexer internals into the query path.

3. **No backfill awareness.** Checkpoints track *where the indexer left off*,
   not *how much remains*. Deriving "75% complete" from a watermark requires
   knowing the total row count in the datalake, which means additional queries
   against source tables that the webserver should not access.

NATS KV solves these: the indexer owns the write side and can compute rich
progress snapshots post-ETL, while the webserver does a single KV read per
request.

## Decision

### Architecture

```plaintext
Indexer (DispatchIndexing + Indexer modes)
  |
  | Writes progress after each ETL cycle
  v
NATS KV bucket: "indexing_progress"
  |
  | Reads on gRPC request
  v
Webserver (gRPC endpoint)
  |
  | Proxied by Rails
  v
GitLab UI / E2E test harness
```

The indexer is the sole writer. The webserver is a reader. No cross-process
locking is needed.

### NATS KV bucket

**Bucket name:** `indexing_progress`

**Configuration:**

- History: 1 (only latest value needed)
- No TTL (entries persist until explicitly deleted by namespace deletion handler)
- Max value size: per-value default (-1, inherits server `max_payload` of 1MB).
  This is a **per-value** limit, not per-bucket. `MaxBytes` (total bucket
  storage) is a separate config that defaults to unlimited. Values are ~1-2KB
  for `counts` and `meta` keys, ~7-15KB for `code` keys with 50-100 branches.
  Even extreme cases (500 branches) produce ~72KB, well under the 1MB ceiling.

### Key schema

NATS KV maps keys to subjects where `.` is the token separator. We encode
traversal path segments as dot-separated tokens to leverage NATS subject
wildcard matching (`*` for one token, `>` for one-or-more).

| Key pattern | Example | Writer | Purpose |
|---|---|---|---|
| `counts.<tp_dots>` | `counts.1.9970` | SDLC namespace handler | Pre-aggregated SDLC + code node + edge counts for subtree |
| `counts.<tp_dots>` | `counts.1.9970.55154808` | SDLC namespace handler | Same, scoped to subgroup subtree |
| `code.<project_id>` | `code.12345` | Code indexing handler | Per-project code graph with per-branch breakdown |
| `meta.<namespace_id>` | `meta.9970` | SDLC namespace handler | Pipeline lifecycle state and operational metadata |

Where `<tp_dots>` is the traversal path with `/` replaced by `.` and trailing
slash removed. Example: traversal path `1/9970/55154808/` becomes key token
`1.9970.55154808`.

### Value schemas

#### Entity counts (`counts.<tp_dots>`)

Contains pre-aggregated subtree counts for **all** entity types: SDLC nodes,
code nodes, and edges. This gives a complete picture at any hierarchy level.

```json
{
  "updated_at": "2026-04-10T12:00:00Z",
  "nodes": {
    "Project": 150,
    "Group": 10,
    "MergeRequest": 3400,
    "WorkItem": 800,
    "Pipeline": 5000,
    "Vulnerability": 200,
    "File": 15000,
    "Definition": 42000,
    "ImportedSymbol": 9500,
    "Directory": 800,
    "Branch": 150
  },
  "edges": {
    "AUTHORED": 3000,
    "ASSIGNED_TO": 1200,
    "CONTAINS": 5550,
    "DEFINES": 42000,
    "IMPORTS": 9500,
    "CALLS": 8000,
    "CLOSES": 200,
    "RELATED_TO": 150
  }
}
```

Node keys are ontology entity names. Edge keys are ontology edge type names.
Both are driven by the ontology at runtime so new types appear automatically.
The ontology currently defines 24 node types and 40 edge types. At ~30 bytes
per entry in compact JSON, values are ~2KB.

Edge counts include cross-namespace edges (see
[Cross-namespace edge counting](#cross-namespace-edge-counting) below).

#### Code progress (`code.<project_id>`)

Per-project code graph detail with **per-branch breakdown**. Since code tables
are keyed by `(traversal_path, project_id, branch)` and the indexing pipeline
processes one branch at a time, counts are naturally per-branch. This structure
supports multi-branch indexing (the current default-branch-only behavior and
future multi-branch indexing).

```json
{
  "traversal_path": "1/9970/55154808/95754906/",
  "updated_at": "2026-04-10T11:30:00Z",
  "branches": {
    "main": {
      "commit": "abc123def",
      "indexed_at": "2026-04-10T11:30:00Z",
      "nodes": {
        "Branch": 1,
        "File": 500,
        "Directory": 50,
        "Definition": 2000,
        "ImportedSymbol": 1500
      },
      "edges": {
        "CONTAINS": 550,
        "DEFINES": 2000,
        "IMPORTS": 1500,
        "CALLS": 800
      }
    }
  }
}
```

The `traversal_path` field is embedded so the webserver can check the
requesting user's access prefixes before returning data.

#### Pipeline metadata (`meta.<namespace_id>`)

Tracks pipeline lifecycle and operational state.

```json
{
  "state": "idle",
  "initial_backfill_done": true,
  "updated_at": "2026-04-10T12:00:00Z",
  "sdlc": {
    "last_completed_at": "2026-04-10T11:55:00Z",
    "last_started_at": "2026-04-10T11:50:00Z",
    "last_duration_ms": 300,
    "cycle_count": 47,
    "last_error": null,
    "watermarks": {
      "Project": "2026-04-10T11:55:00Z",
      "MergeRequest": "2026-04-10T11:50:00Z"
    },
    "plans": {
      "Project": "completed",
      "MergeRequest": "in_progress",
      "WorkItem": "pending"
    }
  },
  "code": {
    "projects_indexed": 45,
    "projects_total": 150,
    "last_indexed_at": "2026-04-10T11:30:00Z"
  }
}
```

### State machine

The state model uses two independent axes:

**Axis 1: Lifecycle flag (monotonic, set once)**

`initial_backfill_done: false` transitions to `true` when all SDLC plans
complete their first full pass. It never reverts. This is the stable signal
that E2E tests and UI poll for.

**Axis 2: Operational state (reflects current activity)**

| State | Meaning |
|---|---|
| `pending` | Namespace enabled, no indexer has started processing yet |
| `indexing` | ETL cycle in progress (plans are running) |
| `idle` | ETL cycle finished, all plans completed, waiting for next dispatch |

State transitions:

```plaintext
                     first dispatch
  pending ──────────────────────────────> indexing
                                            │
                                            │ all plans complete
                                            v
                                          idle ──────> indexing (next dispatch)
                                            │               │
                                            │               │ all plans complete
                                            └───────────────┘
```

On the first cycle where all plans complete and `initial_backfill_done` is
false, the flag is set to true. On every subsequent cycle, the flag remains
true and the state oscillates between `indexing` and `idle`.

**Plan status derivation** (same logic as current checkpoint state machine):

| Checkpoint condition | Plan status |
|---|---|
| No checkpoint row for `ns.{id}.{plan}` | `pending` |
| Checkpoint with `cursor_values IS NOT NULL` | `in_progress` |
| Checkpoint with `cursor_values IS NULL` | `completed` |

**Why not a `completed` state?** An earlier version of this design used
`completed` as a state after the first full pass, transitioning to `indexing`
on the next cycle. This creates a race: if the indexer starts a second cycle
before a polling consumer observes `completed`, the consumer never sees it.
The `initial_backfill_done` flag solves this by being monotonic and always
observable regardless of polling timing.

### Hierarchy and aggregation

#### Problem

Not every user has access to the top-level group. A user with access only to
subgroup `1/9970/55154808/` needs counts scoped to that subtree. But the SDLC
indexer processes the entire top-level namespace in one ETL run.

#### Solution: pre-aggregate at write, O(1) reads

After each SDLC ETL run for namespace N, the indexer computes entity counts at
every group-level prefix in the hierarchy, then writes one KV entry per prefix.

The API accepts a `traversal_path` at any depth. A request for the top-level
namespace `1/9970/` reads key `counts.1.9970`. A request for a subgroup reads
`counts.1.9970.55154808`. A request for a specific project namespace reads
`counts.1.9970.55154808.95754906`. Each key contains the pre-aggregated
subtree total for all entities at or below that prefix.

**Write flow (post-ETL):**

1. Run one `UNION ALL` count query across all entity tables using `uniq(id)`
   instead of `count()`, grouped by full `traversal_path`:

   ```sql
   SELECT 'Project' AS entity, traversal_path, uniq(id) AS cnt
   FROM gl_project
   WHERE startsWith(traversal_path, {tp:String})
   GROUP BY traversal_path
   UNION ALL
   SELECT 'MergeRequest' AS entity, traversal_path, uniq(id) AS cnt
   FROM gl_merge_request
   WHERE startsWith(traversal_path, {tp:String})
   GROUP BY traversal_path
   UNION ALL
   -- ... all 24 node types (SDLC + code tables)
   ```

   **Why `uniq(id)` instead of `count()`.** Graph tables use
   `ReplacingMergeTree`, which deduplicates rows with the same primary key
   during background merges. Between merges, updated rows exist as multiple
   versions. `count()` counts every version, producing overcounts proportional
   to update volume. On staging with 16M edge rows, `count()` overcounts by
   49% overall and up to 300% for frequently updated edge types. `uniq(id)`
   uses HyperLogLog (HLL), a probabilistic set estimator: duplicate versions
   of the same entity have the same `id`, so HLL counts them once. HLL error
   is ~1-2% at high cardinalities, which is acceptable for a progress
   indicator and far better than the 49-300% overcount from `count()`.

   **Why no `NOT _deleted` filter.** Soft-deleted rows (`_deleted = true`) are
   physically removed during background merges when
   `allow_experimental_replacing_merge_with_cleanup = 1` is set (which it is
   on all graph tables). Between merges, the few surviving deleted rows add
   negligible error to HLL estimates. Omitting the filter avoids reading the
   `_deleted` column, which reduces I/O. The `exact_counts = true` path uses
   `FINAL` with `NOT _deleted` for users who need precise numbers.

   **Why no `FINAL`.** `FINAL` forces ClickHouse to read all raw rows and
   deduplicate them by primary key at query time. This is correct but
   expensive: on staging at 16M edges, `FINAL` reads 14.4M rows (620 MB) and
   takes 579ms vs 71ms without. At billions of edges, `FINAL` would take
   minutes. More critically, `FINAL` bypasses aggregate projections (see
   [below](#edge-count-projection)), negating the projection's performance
   benefit. `uniq()` achieves equivalent deduplication via HLL without `FINAL`.

2. Run one edge count query using the `node_edge_counts` projection:

   ```sql
   SELECT traversal_path, relationship_kind, sum(edge_cnt) AS cnt
   FROM (
       SELECT traversal_path, source_kind, target_kind, relationship_kind,
              uniq(source_id, target_id) AS edge_cnt
       FROM gl_edge
       WHERE startsWith(traversal_path, {tp:String})
       GROUP BY traversal_path, source_kind, target_kind, relationship_kind
   )
   GROUP BY traversal_path, relationship_kind
   ```

   The inner query matches the `node_edge_counts` projection's GROUP BY
   signature, so ClickHouse reads pre-aggregated projection data instead of
   raw edge rows. `uniq(source_id, target_id)` counts distinct edge pairs,
   naturally deduplicating RMT version duplicates. See
   [Edge count projection](#edge-count-projection) for details.

3. Run cross-namespace edge queries (see
   [below](#cross-namespace-edge-counting)).

4. In-memory rollup: for each row, split the traversal path and accumulate
   counts at every ancestor prefix:

   ```plaintext
   Row: traversal_path="1/9970/100/200/", entity="MergeRequest", count=45
   Adds 45 to:
     prefix "1.9970.100.200"   (leaf)
     prefix "1.9970.100"       (parent group)
     prefix "1.9970"           (top-level group)
   ```

5. Write one KV entry per distinct prefix with aggregated subtree counts.

This is 2 ClickHouse queries (one `UNION ALL` for nodes, one projection
query for edges) plus cross-namespace edge queries, plus in-memory
aggregation per ETL cycle. For a namespace with G groups, that produces
G KV puts. Typically G < 100.

**Read flow (webserver):**

A lookup at any hierarchy level is a single KV get. No scanning, no
aggregation on the read path.

| Scenario | Operation |
|---|---|
| Top-level namespace `1/9970/` | Read `counts.1.9970` |
| Subgroup `1/9970/55154808/` | Read `counts.1.9970.55154808` |
| Project namespace `1/9970/55154808/95754906/` | Read `counts.1.9970.55154808.95754906` |
| Disjoint access `1/9970/200/` + `1/9970/300/` | Read both keys, sum client-side |

The disjoint case is O(N) where N is the user's access prefix count, which is
typically < 5 after Rails' trie optimization.

### Cross-namespace edge counting

Most edges connect entities within the same namespace. The `gl_edge` table
stores a single `traversal_path` per edge row, assigned from the source
entity's namespace during ETL.

A small number of edge types can cross namespaces:

| Edge type | Scenario |
|---|---|
| `CLOSES` | MR in project A closes a WorkItem in project B |
| `FIXES` | MR in project A fixes a Vulnerability in project B |
| `RELATED_TO` | WorkItem in group X linked to WorkItem in group Y |

For these edges, the edge row's `traversal_path` reflects the source entity's
namespace. A simple `startsWith(traversal_path, TP)` count picks up edges
originating from the namespace but misses edges targeting entities in the
namespace from elsewhere.

**Dual-count approach:** count cross-namespace edges on both sides. After the
regular edge count query, run targeted queries for the cross-namespace edge
types using a join with the target entity table:

```sql
-- Cross-namespace edges targeting WorkItems in this namespace
SELECT w.traversal_path, e.relationship_kind, count() AS cnt
FROM gl_edge e
INNER JOIN gl_work_item w ON e.target_id = w.id
WHERE startsWith(w.traversal_path, {tp:String})
  AND NOT w._deleted
  AND NOT e._deleted
  AND e.relationship_kind IN ('CLOSES', 'RELATED_TO')
  AND NOT startsWith(e.traversal_path, {tp:String})
GROUP BY w.traversal_path, e.relationship_kind
```

```sql
-- Cross-namespace edges targeting Vulnerabilities in this namespace
SELECT v.traversal_path, e.relationship_kind, count() AS cnt
FROM gl_edge e
INNER JOIN gl_vulnerability v ON e.target_id = v.id
WHERE startsWith(v.traversal_path, {tp:String})
  AND NOT v._deleted
  AND NOT e._deleted
  AND e.relationship_kind = 'FIXES'
  AND NOT startsWith(e.traversal_path, {tp:String})
GROUP BY v.traversal_path, e.relationship_kind
```

The `by_target` projection on `gl_edge` (ordered by `target_id`) enables
efficient joins on `target_id`. These queries only run for the ~3
cross-namespace edge types and only after non-zero-row ETL runs.

The resulting counts are merged into the `counts.<tp>` values alongside the
regular edge counts. A single edge may appear in both the source and target
namespace's counts. This is intentional: each namespace's count reflects
"edges involving entities in my namespace."

### Project-level code lookups

The `code.<project_id>` key provides O(1) lookup by project ID. The webserver
receives a project ID, reads the key, checks the embedded `traversal_path`
against the user's access, and returns the per-branch breakdown.

### Edge count projection

The `gl_edge` table stores all graph relationships and is the largest table
(16M rows on staging, expected to grow to billions). Scanning it for counts
on every ETL cycle is expensive. A pre-aggregated projection eliminates this
cost.

**Projection DDL:**

```sql
ALTER TABLE gl_edge ADD PROJECTION node_edge_counts (
    SELECT
        traversal_path,
        source_kind,
        target_kind,
        relationship_kind,
        uniq(source_id),
        uniq(target_id),
        uniq(source_id, target_id)
    GROUP BY traversal_path, source_kind, target_kind, relationship_kind
);
```

**How it works.** ClickHouse builds the projection per data part at insert
time, computing one HLL sketch per distinct
`(traversal_path, source_kind, target_kind, relationship_kind)` group. When
a query's GROUP BY matches the projection's signature, ClickHouse reads the
pre-aggregated projection data instead of raw rows. HLL states from multiple
parts are merged during the query (HLL merge is a set union, which is
associative and handles cross-part deduplication correctly).

**Why `uniq(source_id, target_id)` for edge counts.** Each edge row has a
unique primary key `(traversal_path, source_id, relationship_kind, target_id,
source_kind, target_kind)`. Within each projection group (which already fixes
4 of those 6 columns), `(source_id, target_id)` uniquely identifies an edge.
Duplicate RMT versions of the same edge share the same `(source_id, target_id)`
pair, so `uniq(source_id, target_id)` deduplicates them via HLL without
needing `FINAL`.

**Why `FINAL` cannot be used with this projection.** `FINAL` forces
ClickHouse to read raw rows for deduplication. Aggregate projections store
HLL states that have lost per-row granularity, so ClickHouse cannot apply
RMT dedup to projection data. `EXPLAIN PLAN` confirms this: without `FINAL`,
the plan shows `ReadFromMergeTree (node_edge_counts)` (projection); with
`FINAL`, it shows `ReadFromMergeTree (gkg.gl_edge)` (raw table). This was
verified on both local and staging ClickHouse instances.

**Projection cardinality.** On staging, the projection contains 4,126
distinct groups for the largest namespace (373 traversal paths x ~40
relationship types with valid source/target kind combinations). This number
is bounded by the ontology, not by the raw edge count. At billions of edges,
the projection still contains ~4,000 rows per namespace.

**Deployment.** After adding the projection DDL to `graph.sql`, run
`ALTER TABLE gl_edge MATERIALIZE PROJECTION node_edge_counts` to build
projection data for existing parts. New inserts build projection data
automatically. Without materialization, ClickHouse falls back to raw table
scans for parts that lack projection data. On staging before materialization,
14/16 parts lacked the projection, causing 15,727 granule reads instead of
12.

### Performance

Post-ETL count queries add latency to the SDLC handler. Four strategies
keep this acceptable:

#### Use HLL (`uniq()`) instead of `count()` to avoid FINAL

All count queries use `uniq()` instead of `count()`. This eliminates the
need for `FINAL` while keeping error below ~1-2% (HLL). Without this, the
only way to get accurate counts is `FINAL`, which is prohibitively expensive
at scale (see above).

#### Use the edge projection to avoid scanning raw edge data

The `node_edge_counts` projection reduces edge count queries from scanning
millions of raw rows to reading ~4,000 pre-aggregated projection rows per
namespace. The node `UNION ALL` query still scans node tables directly, but
these are 10-100x smaller than `gl_edge`.

#### Skip counts when ETL processed zero rows

The pipeline already tracks `total_rows` per plan. If all plans processed
zero rows, skip the count queries entirely. The existing KV values are
already up-to-date.

#### Debounce count queries

Even with the projection, running count queries on every ETL cycle is
unnecessary when the dispatcher runs frequently (sub-minute intervals).
The handler skips count queries if the KV `counts` key was updated less than
30 seconds ago. At 500ms dispatch intervals, this eliminates ~98% of count
query invocations while keeping data fresh enough for UI polling.

#### Staging performance (X-ClickHouse-Summary)

Measured on staging against namespace `1/9970/` (16.4M raw edge rows, ~4M
node rows, 373 distinct traversal paths):

| Query | Rows read | Data read | Server time | Memory | Accuracy |
|---|---|---|---|---|---|
| Edge: `uniq(src,tgt)` via projection | 9,848 | 1.1 MB | 71ms | 121 MB | +0.1% |
| Edge: `count()` raw scan | 10,162,678 | 340 MB | 217ms | 130 MB | +49.4% |
| Edge: `FINAL` (ground truth) | 14,413,693 | 620 MB | 579ms | 735 MB | exact |
| Node: `uniq(id)` UNION ALL | 3,802,448 | 92 MB | 66ms | 158 MB | <1% |
| Node: `count()` UNION ALL | 3,802,448 | 63 MB | 51ms | 138 MB | 0-95% |

The projection reads **1,032x fewer rows** and **306x less data** than the
raw scan, and **1,464x fewer rows** than `FINAL`.

Combined post-ETL cost: **137ms server-side** (71ms edges + 66ms nodes).
With 30s debounce, this runs at most twice per minute per namespace.

#### Accuracy detail

Per-relationship-kind error for `uniq(source_id, target_id)` vs `FINAL` on
staging:

| Relationship | `count()` error | `uniq()` error |
|---|---|---|
| IN_PROJECT (2.1M edges) | +54.2% | -0.6% |
| HAS_STAGE (455K edges) | +171.4% | +0.6% |
| AUTHORED (664K edges) | +87.3% | +0.1% |
| CALLS (273K edges) | +4.2% | +1.8% |
| HAS_LABEL (106K edges) | +1.3% | 0% |
| CLOSES (11K edges) | 0% | 0% |
| MERGED_BY (3.5K edges) | 0% | 0% |

Low-cardinality edge types (< 1K) are exact. High-cardinality types have
~0.1-1.8% HLL error. The `count()` approach overcounts by 49-300% for edge
types that are re-written on every incremental run.

Node counts with `uniq(id)` are similarly accurate:

| Table | `count()` error | `uniq(id)` error |
|---|---|---|
| gl_note (567K) | +95.1% | +0.2% |
| gl_definition (393K) | +5.7% | +0.2% |
| gl_merge_request (31K) | +0.2% | 0% |
| gl_pipeline (160K) | 0% | -0.3% |

#### Impact analysis

- **Zero-row incremental runs:** no count overhead at all (skip).
- **Within debounce window:** no count overhead (skip).
- **Non-zero runs past debounce:** one node `UNION ALL` + one projection
  edge query + 2 cross-namespace join queries. ~137ms server-side, well
  within the 300s handler `ack_wait` timeout.
- **Checkpoint advancement:** `save_completed` happens per-plan before
  counts. Count query failures do not affect watermark progression.

Count queries run after all plans complete. Count query failures are logged
as warnings but do not fail the handler. Progress reporting is best-effort.

### NATS key scaling

With pre-aggregation at every group level, the key count per bucket is:

| Component | Keys per namespace | Example (100 namespaces) |
|---|---|---|
| `counts.<tp>` | 1 per distinct group-level prefix | ~10,000 |
| `code.<project_id>` | 1 per indexed project | ~20,000 |
| `meta.<ns_id>` | 1 per enabled namespace | ~100 |
| **Total** | | **~30,000** |

NATS JetStream stores KV entries as messages in a stream. 30K small messages
(1-15KB each) is trivial -- NATS is designed for millions of messages.
Total storage: ~150-500MB.

**No iteration on the hot path.** Reads are O(1) by exact key. Writes target
specific keys. The only time key enumeration occurs is during namespace
deletion, which is infrequent. The deletion handler constructs key names from
known data (traversal path prefixes from graph tables, project IDs from
`code_indexing_checkpoint`) rather than scanning all keys.

### Staleness

NATS KV is a derived cache. The source of truth for counts is ClickHouse. For
checkpoint state, the source of truth is the `checkpoint` table.

**Staleness bounds:**

| Data | Staleness bound | Why |
|---|---|---|
| Node/edge counts | One ETL interval (typically minutes) | Updated after each non-zero-row SDLC handler run |
| Code project counts | Updated on each code indexing run | Event-driven, near-real-time |
| Plan statuses | One ETL interval | Derived from checkpoints post-ETL |

**Failure modes:**

| Failure | Impact | Recovery |
|---|---|---|
| Indexer crashes after ClickHouse write, before KV update | KV stale until next ETL run | Next successful ETL run overwrites with fresh data |
| NATS restart without persistence | KV empty | Next ETL run for each namespace repopulates all keys |
| Indexer cannot reach NATS KV | KV not updated, indexing continues | Non-fatal; logged as warning. Next successful write recovers |

KV write failures must not fail the ETL pipeline. Progress reporting is
best-effort; the indexer's primary job is writing graph data to ClickHouse.

#### KV recovery

After a full KV loss, the indexer reconstructs state automatically on the next
ETL cycle. The handler reads checkpoints from ClickHouse (which survived the
NATS loss) and derives state:

- All checkpoint rows have non-epoch watermarks with no cursors:
  `state = "idle"`, `initial_backfill_done = true`
- Some rows missing or have cursors:
  `state = "indexing"`, `initial_backfill_done = false`
- No checkpoint rows at all:
  `state = "pending"`, `initial_backfill_done = false`

No special startup-time reconstruction logic is needed. Between KV loss and
the next ETL cycle, the webserver returns "not found" for progress queries.

**Freshness indicator:**

Every KV value includes `updated_at`. The webserver can compare this against
the current time and include a `stale: true` flag in the response if the value
is older than a configurable threshold (e.g., 2x the ETL interval).

### Testability

This design directly supports e2e testing where the full GKG stack (indexer +
webserver + ClickHouse + NATS) runs alongside Rails.

**E2E test flow:**

1. Test setup: enable a namespace, insert seed data into datalake tables.
2. Trigger: indexer's `DispatchIndexing` mode picks up the namespace and
   dispatches work.
3. Poll: test harness calls `GetNamespaceOverview` in a loop.
4. Assert: when `initial_backfill_done == true` and `state == "idle"`, the
   test proceeds to execute query assertions.

**Why this works reliably:**

- `initial_backfill_done` is a **monotonic flag**. Once set, it cannot be
  missed regardless of polling frequency. Unlike the previous `completed`
  state, there is no race between the indexer's next cycle and the test's poll.
- Reads hit NATS KV, not ClickHouse. No polling load on the database.
- The existing `NatsServices` trait with `kv_get`, `kv_put`, `kv_keys`
  supports `MockNatsServices` for unit testing the read path without NATS.
- Each namespace's progress is in its own keys. Concurrent test runs against
  different namespaces do not interfere.

**Complete "indexing done" check:**

```plaintext
meta.<ns_id>.initial_backfill_done == true
AND meta.<ns_id>.state == "idle"
AND meta.<ns_id>.code.projects_indexed == meta.<ns_id>.code.projects_total
```

### gRPC endpoint

A single endpoint, `GetNamespaceOverview`, replaces the existing
`GetGraphStats` endpoint. `GetGraphStats` currently has no callers in Rails
(the `GrpcClient` does not wrap it, no REST endpoint exists, and the frontend
`fetchGraphStats()` has no backend wired). It can be removed.

```protobuf
rpc GetNamespaceOverview(GetNamespaceOverviewRequest)
    returns (GetNamespaceOverviewResponse);

message GetNamespaceOverviewRequest {
  // Traversal path prefix (e.g., "1/9970/", "1/9970/55154808/").
  // Controls the scope: top-level namespace, subgroup, or project.
  string traversal_path = 1;

  // When true, entity counts come from live ClickHouse queries
  // instead of NATS KV. Slower but exact. Default: false.
  bool exact_counts = 2;
}

message GetNamespaceOverviewResponse {
  // Operational state: "pending", "indexing", "idle".
  // Empty when exact_counts=true (state lives only in KV).
  string state = 1;

  // When the KV cache was last written.
  string updated_at = 2;

  // Entity counts grouped by ontology domain.
  repeated OverviewDomain domains = 3;

  // Edge counts by relationship type.
  map<string, int64> edge_counts = 4;

  // SDLC pipeline progress (only from KV, not exact_counts).
  SdlcProgress sdlc = 5;

  // Code indexing overview.
  CodeOverview code = 6;

  // True when KV data is older than a staleness threshold.
  bool stale = 7;

  // True after all SDLC plans have completed at least one full pass.
  bool initial_backfill_done = 8;
}

message OverviewDomain {
  string name = 1;
  repeated OverviewItem items = 2;
}

message OverviewItem {
  string name = 1;
  int64 count = 2;
  // Plan status: "pending", "in_progress", "completed".
  // Empty when exact_counts=true or for code entities.
  string status = 3;
}

message SdlcProgress {
  string last_completed_at = 1;
  string last_started_at = 2;
  int64 last_duration_ms = 3;
  int64 cycle_count = 4;
  string last_error = 5;
  map<string, string> watermarks = 6;
}

message CodeOverview {
  int32 projects_indexed = 1;
  int32 projects_total = 2;
  string last_indexed_at = 3;
  repeated ProjectCodeOverview projects = 4;
}

message ProjectCodeOverview {
  int64 project_id = 1;
  string traversal_path = 2;
  string updated_at = 3;
  map<string, BranchCodeStats> branches = 4;
}

message BranchCodeStats {
  string commit = 1;
  string indexed_at = 2;
  map<string, int64> node_counts = 3;
  map<string, int64> edge_counts = 4;
}
```

The response shape is ontology-driven: `OverviewDomain` mirrors the existing
`GraphStatsDomain` grouping. Domain and entity names are derived from the
ontology at runtime.

When `exact_counts = false` (default), the endpoint reads from NATS KV.
When `exact_counts = true`, it runs live ClickHouse count queries (the same
`UNION ALL` approach as the current `GetGraphStats`). The response shape is
identical either way; only the `state`, `sdlc`, `code`, and
`initial_backfill_done` fields are empty when using exact counts since those
are KV-only concepts.

### Access control

The endpoint follows the same authorization pattern as the existing
`GetGraphStats`:

1. Extract JWT claims from the `Authorization: Bearer` header.
2. Call `authorize_traversal_path(&claims, &req.traversal_path)`, which
   checks that the requested path starts with an entry in the JWT's
   `group_traversal_ids`.
3. Return data scoped to the authorized traversal path.

No redaction exchange is needed. The endpoint returns aggregate counts and
status, not individual resources. This is consistent with the security doc's
note that "aggregations rely on Layers 1 and 2" (org filter + traversal ID
prefix).

### Namespace deletion

When a namespace is disabled, the `NamespaceDeletionHandler` already cleans up
graph data after 30 days. It should also delete all progress KV keys:

- `meta.<ns_id>`
- All `counts.<tp_dots>` keys matching the namespace's traversal path prefix
  (constructed from known hierarchy, not by scanning all keys)
- All `code.<project_id>` keys for projects under the namespace (resolved from
  `code_indexing_checkpoint`)

## Why not the alternatives

### Derive everything from ClickHouse at query time (previous design, issue #175)

This was the original proposal. It works but requires 15+ `FINAL` queries per
request, couples the webserver to checkpoint internals, and cannot express
backfill progress without querying datalake source tables. NATS KV moves the
computation to the write side where the indexer already has all the context.

### ClickHouse materialized status table

A dedicated `indexing_status` table updated by handlers on each ETL run.
Rejected because `ReplacingMergeTree` has no atomic increment. Concurrent
handlers writing to the same table create version conflicts. NATS KV avoids
this because each namespace's keys are written by a single handler instance
(the namespace handler holds the NATS work-queue message).

### Store at leaf level only, aggregate on read

Store counts only at the most granular traversal path (project namespace
level), and sum on read by prefix-scanning KV keys. For a namespace with 500
subgroups this means 500 key reads per API request. Pre-aggregation at write
makes reads O(1) at the cost of more KV puts per ETL cycle.

### Separate KV bucket per namespace

Would provide natural isolation but creates operational overhead: bucket
lifecycle management, monitoring per bucket, and NATS resource consumption
proportional to enabled namespaces. A single bucket with key prefixes is
simpler.

### Use `count()` without `FINAL` for count queries

The original version of this ADR proposed `count()` without `FINAL`, relying
on `allow_experimental_replacing_merge_with_cleanup` to keep overcount
"small." Staging data disproved this: `count()` overcounts by 49% overall
and up to 300% for edge types that are re-written every incremental cycle
(AUTHORED, IN_PROJECT, HAS_STAGE). The background merger cannot keep pace
with continuous CDC writes. `uniq()` solves this by deduplicating via HLL
with ~1-2% error instead of 49-300% overcount.

### Use `FINAL` with the edge projection

`FINAL` produces exact counts but bypasses aggregate projections entirely.
EXPLAIN PLAN confirms that `FROM gl_edge FINAL` reads from
`ReadFromMergeTree (gkg.gl_edge)` (raw table), not
`ReadFromMergeTree (node_edge_counts)` (projection). On staging, `FINAL`
reads 14.4M rows (620 MB, 579ms) vs 9,848 rows (1.1 MB, 71ms) for the
projection. At billions of edges, `FINAL` would take minutes.

### Ignore cross-namespace edges

Count edges only by the source entity's traversal path. This undercounts edges
for target namespaces. The cross-namespace edge types (`CLOSES`, `FIXES`,
`RELATED_TO`) can represent a significant number of relationships, and users
expect "edges in my namespace" to include both directions. The dual-count
approach adds 2 join queries per ETL cycle, which is acceptable.

## Consequences

### What improves

- **Read performance.** Progress lookups are a single NATS KV get instead of
  15+ ClickHouse queries with `FINAL`.
- **Separation of concerns.** The webserver does not need to understand
  checkpoint key formats, cursor semantics, or watermark interpretation.
- **Backfill visibility.** The `initial_backfill_done` flag and `state` field
  provide explicit lifecycle tracking without datalake queries.
- **E2E testability.** A monotonic `initial_backfill_done` flag enables
  reliable polling without race conditions.
- **Hierarchy-aware.** Pre-aggregated counts at every group level support
  lookups at top-level namespace, subgroup, or project scope.
- **Developer observability.** Developers get direct feedback on indexing
  status through the UI without needing infrastructure access.
- **Unified endpoint.** One RPC (`GetNamespaceOverview`) replaces two
  (`GetGraphStats` + `GetNamespaceIndexingProgress`), with an `exact_counts`
  flag for the rare case that precise ClickHouse counts are needed.
- **Complete entity coverage.** Namespace-level counts include both SDLC and
  code entities, giving a full picture without needing separate lookups.

### What gets harder

- **Additional write path.** The indexer writes to both ClickHouse and
  NATS KV after each ETL cycle. KV write failures must be non-fatal to avoid
  blocking indexing.
- **Eventual consistency.** KV values are stale by up to one ETL interval.
  Consumers must tolerate this (the `updated_at` field and `stale` flag help).
- **Cross-namespace edge complexity.** Dual-counting cross-namespace edges
  adds 2 join queries per ETL cycle and means the same edge appears in two
  namespace counts. This is correct but requires clear documentation.
- **KV bucket lifecycle.** The namespace deletion handler must clean up
  progress keys alongside graph data.
- **NATS dependency on read path.** The webserver needs a NATS client
  connection for progress reads. The architecture README already shows this
  connection but the code has not implemented it until now.

## References

- [Issue #175: Support getting namespace indexing status information](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/175)
- [ADR 005: PostgreSQL task table for code indexing triggers](005_code_indexing_task_table.md)
- [SDLC indexing design](../indexing/sdlc_indexing.md)
- [Code indexing design](../indexing/code_indexing.md)
- [Namespace deletion design](../indexing/namespace_deletion.md)
- [Security design](../security.md)
- [Observability design](../observability.md)
- [gRPC service definition](../../../crates/gkg-server/proto/gkg.proto)
- [Checkpoint store](../../../crates/indexer/src/checkpoint.rs)
- [Code checkpoint store](../../../crates/indexer/src/modules/code/checkpoint_store.rs)
- [NATS KV types](../../../crates/indexer/src/nats/kv_types.rs)
- [NatsServices trait](../../../crates/indexer/src/nats/services.rs)
- [Graph stats service](../../../crates/gkg-server/src/graph_stats/mod.rs)
- [gl_edge table schema](../../../config/graph.sql)
- [Snippet #5978783: query optimization research](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5978783)
