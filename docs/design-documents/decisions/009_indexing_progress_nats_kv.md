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
indexing progress. This endpoint serves three audiences:

1. **Namespace admins** checking rollout status after enabling Knowledge Graph.
2. **GitLab Rails** proxying progress to configuration UI pages.
3. **E2E test harnesses** polling for indexing completion before executing query
   assertions against the full GKG stack.

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

```
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
- Max value size: default (1MB, values are ~2-4KB)

### Key schema

NATS KV maps keys to subjects where `.` is the token separator. We encode
traversal path segments as dot-separated tokens to leverage NATS subject
wildcard matching (`*` for one token, `>` for one-or-more).

| Key pattern | Example | Writer | Purpose |
|---|---|---|---|
| `sdlc.<tp_dots>` | `sdlc.1.9970` | SDLC namespace handler | Pre-aggregated node + edge counts for subtree |
| `sdlc.<tp_dots>` | `sdlc.1.9970.55154808` | SDLC namespace handler | Same, scoped to subgroup subtree |
| `code.<project_id>` | `code.12345` | Code indexing handler | Per-project code graph status + counts |
| `meta.<namespace_id>` | `meta.9970` | SDLC namespace handler | Pipeline run metadata and watermarks |

Where `<tp_dots>` is the traversal path with `/` replaced by `.` and trailing
slash removed. Example: traversal path `1/9970/55154808/` becomes key token
`1.9970.55154808`.

### Value schemas

#### SDLC progress (`sdlc.<tp_dots>`)

```json
{
  "updated_at": "2026-04-10T12:00:00Z",
  "nodes": {
    "Project": 150,
    "Group": 10,
    "MergeRequest": 3400,
    "WorkItem": 800,
    "Pipeline": 5000,
    "Vulnerability": 200
  },
  "edges": {
    "AUTHORED": 3000,
    "ASSIGNED_TO": 1200,
    "CONTAINS": 550,
    "DEFINES": 2000
  }
}
```

Node keys are ontology entity names. Edge keys are ontology edge type names.
Both are driven by the ontology at runtime so new types appear automatically.
At ~14 node types and ~40 edge types with int64 counts, values are ~2KB.

#### Code progress (`code.<project_id>`)

```json
{
  "traversal_path": "1/9970/55154808/95754906/",
  "updated_at": "2026-04-10T11:30:00Z",
  "branches": {
    "main": {
      "commit": "abc123def",
      "indexed_at": "2026-04-10T11:30:00Z"
    }
  },
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
```

The `traversal_path` field is embedded for authorization: the webserver checks
the requesting user's access prefixes before returning data.

#### Pipeline metadata (`meta.<namespace_id>`)

```json
{
  "state": "indexing",
  "enabled_at": "2026-04-09T10:00:00Z",
  "updated_at": "2026-04-10T12:00:00Z",
  "sdlc": {
    "last_completed_at": "2026-04-10T11:55:00Z",
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

**State values:**

| State | Meaning |
|---|---|
| `pending` | Namespace enabled but no indexer has picked it up yet |
| `backfilling` | First ETL cycle in progress (watermark started from epoch) |
| `indexing` | Incremental indexing (at least one full pass completed) |
| `completed` | All SDLC plans have completed at least one full pass |

**Plan status derivation** (same logic as current checkpoint state machine):

| Checkpoint condition | Plan status |
|---|---|
| No checkpoint row for `ns.{id}.{plan}` | `pending` |
| Checkpoint with `cursor_values IS NOT NULL` | `in_progress` |
| Checkpoint with `cursor_values IS NULL` | `completed` |

### Hierarchy and aggregation strategy

#### Problem

Not every user has access to the top-level group. A user with access only to
subgroup `1/9970/55154808/` needs counts scoped to that subtree. But the SDLC
indexer processes the entire top-level namespace in one ETL run.

#### Solution: pre-aggregate at write, O(1) reads

After each SDLC ETL run for namespace N, the indexer computes entity counts at
every group-level prefix in the hierarchy, then writes one KV entry per prefix.

**Write flow:**

1. Run one COUNT query per entity type, grouped by full `traversal_path`:

   ```sql
   SELECT traversal_path, count() AS cnt
   FROM gl_project FINAL
   WHERE startsWith(traversal_path, {tp:String}) AND NOT _deleted
   GROUP BY traversal_path
   ```

2. Run one COUNT query for edges, grouped by `traversal_path` and `edge_type`:

   ```sql
   SELECT traversal_path, edge_type, count() AS cnt
   FROM gl_edge FINAL
   WHERE startsWith(traversal_path, {tp:String}) AND NOT _deleted
   GROUP BY traversal_path, edge_type
   ```

3. In-memory rollup: for each row, split the traversal path and accumulate
   counts at every ancestor prefix:

   ```
   Row: traversal_path="1/9970/100/200/", count=45 (MergeRequest)
   Adds 45 to:
     prefix "1.9970.100.200"   (leaf)
     prefix "1.9970.100"       (parent group)
     prefix "1.9970"           (top-level group)
   ```

4. Write one KV entry per distinct prefix with aggregated subtree counts.

This is E+1 ClickHouse queries (one per entity type, one for edges) plus
in-memory aggregation per ETL cycle. For a namespace with G groups, that
produces G KV puts. Typically G < 100.

**Read flow (webserver):**

A lookup at any hierarchy level is a single KV get. No scanning, no
aggregation on the read path.

| Scenario | Operation |
|---|---|
| User has access to `1/9970/` | Read `sdlc.1.9970` |
| User has access to `1/9970/55154808/` only | Read `sdlc.1.9970.55154808` |
| User has access to `1/9970/200/` and `1/9970/300/` but not `1/9970/` | Read both keys, sum client-side |

The third case is O(N) where N is the user's access prefix count, which is
typically < 5 after Rails' trie optimization.

### Project-level code lookups

The `code.<project_id>` key provides O(1) lookup by project ID. The webserver
receives a project ID, reads the key, checks the embedded `traversal_path`
against the user's access, and returns.

### Edge count tracking

Edges are tracked at two levels:

1. **Namespace level** (in `sdlc.<tp>` values): all edge types from `gl_edge`,
   covering both SDLC and code edges. Updated each SDLC ETL cycle.
2. **Project level** (in `code.<project_id>` values): code-specific edge counts
   (CONTAINS, DEFINES, IMPORTS, CALLS). Updated on each code indexing run.

The SDLC handler's post-ETL edge count query naturally picks up code edges
since they share `gl_edge`. Between SDLC runs, newly indexed code edges are
reflected in the project-level key immediately but appear in namespace-level
counts on the next SDLC cycle. This eventual consistency is acceptable for a
progress indicator.

### Handling indexing states

#### Initial backfill (namespace enabled for the first time)

1. `NamespaceDispatcher` publishes the first `NamespaceIndexingRequest`.
2. Before the indexer picks it up, the dispatcher writes `meta.<ns_id>` with
   `state: "pending"`.
3. When the SDLC handler starts processing, it updates `meta.<ns_id>` to
   `state: "backfilling"` (detected by watermark starting from epoch).
4. After each plan completes its first full pass, the handler updates the
   plan status in `meta.<ns_id>`.
5. After all plans complete one full pass, state transitions to `"completed"`.
6. On subsequent runs, state is `"indexing"` (incremental).

#### Code backfill dependency

Code indexing depends on the Project plan completing first (the
`NamespaceCodeBackfillDispatcher` needs `project_namespace_traversal_paths`
to be populated). The `meta` key's `code.projects_total` starts at 0 and is
updated once the Project plan completes and the dispatcher resolves all
projects.

#### Namespace deletion

When a namespace is disabled, the `NamespaceDeletionHandler` already cleans up
graph data after 30 days. It should also delete all KV keys for the namespace:

- `meta.<ns_id>`
- All `sdlc.<tp_dots>` keys matching the namespace's traversal path prefix
- All `code.<project_id>` keys for projects under the namespace (resolved from
  `code_indexing_checkpoint`)

### Staleness

NATS KV is a derived cache. The source of truth for counts is ClickHouse. For
checkpoint state, the source of truth is the `checkpoint` table.

**Staleness bounds:**

| Data | Staleness bound | Why |
|---|---|---|
| SDLC node/edge counts | One ETL interval (configurable, typically minutes) | Updated after each SDLC handler run |
| Code project counts | Updated on each code indexing run | Event-driven, near-real-time |
| Namespace-level code edge counts | One SDLC ETL interval | SDLC handler's edge query includes code edges |
| Plan statuses | One ETL interval | Derived from checkpoints post-ETL |

**Failure modes:**

| Failure | Impact | Recovery |
|---|---|---|
| Indexer crashes after ClickHouse write, before KV update | KV stale until next ETL run | Next successful ETL run overwrites with fresh data |
| NATS restart without persistence | KV empty | Next ETL run for each namespace repopulates all keys |
| Indexer cannot reach NATS KV | KV not updated, indexing continues | Non-fatal; logged as warning. Next successful write recovers |

KV write failures must not fail the ETL pipeline. Progress reporting is
best-effort; the indexer's primary job is writing graph data to ClickHouse.

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
3. Poll: test harness calls `GetNamespaceIndexingProgress` in a loop.
4. Assert: when `state == "completed"` and all plan statuses are `"completed"`,
   the test proceeds to execute query assertions.

**Why NATS KV makes this testable:**

- **No ClickHouse on the read path.** The webserver reads from NATS KV, which
  is already a required dependency. No additional ClickHouse client or query
  knowledge is needed in the webserver for progress.
- **Deterministic state transitions.** The `meta` key's state field follows a
  clear `pending -> backfilling -> completed -> indexing` progression. Tests
  can assert on specific transitions.
- **Mockable at the trait boundary.** The existing `NatsServices` trait already
  supports `kv_get`, `kv_put`, `kv_keys`. A `MockNatsServices` with in-memory
  state is sufficient for unit testing the progress read path without NATS.
- **Isolated per namespace.** Each namespace's progress is in its own keys.
  Concurrent test runs against different namespaces do not interfere.

**What the test harness needs to check for "indexing complete":**

```
meta.<ns_id>.state == "completed"
AND all meta.<ns_id>.sdlc.plans.* == "completed"
AND meta.<ns_id>.code.projects_indexed == meta.<ns_id>.code.projects_total
```

### gRPC endpoint

The endpoint is `GetNamespaceIndexingProgress`, added to the existing gRPC
service in `gkg.proto`. It reads from NATS KV only, no ClickHouse.

```protobuf
rpc GetNamespaceIndexingProgress(GetNamespaceIndexingProgressRequest)
    returns (GetNamespaceIndexingProgressResponse);

message GetNamespaceIndexingProgressRequest {
  string traversal_path = 1;
}

message GetNamespaceIndexingProgressResponse {
  string state = 1;
  string updated_at = 2;
  repeated ProgressDomain domains = 3;
  CodeProgress code = 4;
}

message ProgressDomain {
  string name = 1;
  repeated ProgressItem items = 2;
}

message ProgressItem {
  string name = 1;
  int64 count = 2;
  string status = 3;
}

message CodeProgress {
  int32 projects_indexed = 1;
  int32 projects_total = 2;
  repeated CodeProjectProgress projects = 3;
}

message CodeProjectProgress {
  int64 project_id = 1;
  string branch = 2;
  string commit = 3;
  string indexed_at = 4;
  map<string, int64> node_counts = 5;
  map<string, int64> edge_counts = 6;
}
```

The response shape is ontology-driven: `ProgressDomain` mirrors the existing
`GraphStatsDomain` grouping from `GetGraphStats`. Domain and entity names are
derived from the ontology at runtime.

### Relationship to `GetGraphStats`

`GetGraphStats` continues to exist as the authoritative, ClickHouse-backed
entity count endpoint. It works at any traversal path depth and returns exact
counts. `GetNamespaceIndexingProgress` is the lightweight, NATS KV-backed
progress endpoint focused on indexing lifecycle and status.

They share the same domain grouping (ontology-driven) but serve different
purposes and have different consistency guarantees.

## Why not the alternatives

### Derive everything from ClickHouse at query time (previous design, issue #175)

This was the original proposal. It works but requires 15+ `FINAL` queries per
request, couples the webserver to checkpoint internals, and cannot express
backfill progress without querying datalake source tables. NATS KV moves the
computation to the write side where the indexer already has all the context.

### ClickHouse materialized status table

A dedicated `indexing_status` table updated by handlers on each ETL run.
Rejected because `ReplacingMergeTree` has no atomic increment -- concurrent
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

## Consequences

### What improves

- **Read performance.** Progress lookups are a single NATS KV get instead of
  15+ ClickHouse queries with `FINAL`.
- **Separation of concerns.** The webserver does not need to understand
  checkpoint key formats, cursor semantics, or watermark interpretation.
- **Backfill visibility.** The `meta` key explicitly tracks indexing state
  (`pending`, `backfilling`, `completed`) which cannot be derived from
  checkpoints alone without datalake queries.
- **E2E testability.** A clear "indexing complete" signal enables automated
  test harnesses to poll and proceed deterministically.
- **Hierarchy-aware.** Pre-aggregated counts at every group level support
  users who only have access to a subtree.

### What gets harder

- **Additional write path.** The indexer now writes to both ClickHouse and
  NATS KV after each ETL cycle. KV write failures must be non-fatal to avoid
  blocking indexing.
- **Eventual consistency.** KV values are stale by up to one ETL interval.
  Consumers must tolerate this (the `updated_at` field helps).
- **KV bucket lifecycle.** The namespace deletion handler must clean up
  progress keys alongside graph data. Missed cleanup leaves orphaned keys
  (non-critical but messy).
- **NATS dependency on read path.** The webserver needs a NATS client
  connection for progress reads, adding NATS as a runtime dependency for the
  webserver mode (it currently only needs ClickHouse and gRPC to Rails).

## References

- [Issue #175: Support getting namespace indexing status information](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/175)
- [ADR 005: PostgreSQL task table for code indexing triggers](005_code_indexing_task_table.md)
- [SDLC indexing design](../indexing/sdlc_indexing.md)
- [Code indexing design](../indexing/code_indexing.md)
- [Namespace deletion design](../indexing/namespace_deletion.md)
- [gRPC service definition](../../../crates/gkg-server/proto/gkg.proto)
- [Checkpoint store](../../../crates/indexer/src/checkpoint.rs)
- [Code checkpoint store](../../../crates/indexer/src/modules/code/checkpoint_store.rs)
- [NATS KV types](../../../crates/indexer/src/nats/kv_types.rs)
- [NatsServices trait](../../../crates/indexer/src/nats/services.rs)
- [Graph stats service](../../../crates/gkg-server/src/graph_stats/mod.rs)
