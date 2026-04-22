---
title: "GKG ADR 009: Evolving GetGraphStats into GetGraphStatus"
creation-date: "2026-04-21"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-04-21

## Context

`GetGraphStats` returns entity counts grouped by ontology domain, scoped to a namespace via `traversal_path`. Consumers (Rails UI, Duo) use it to see what data exists in the knowledge graph for a given group or project.

Entity counts alone don't answer the questions users actually ask:

- "Is my project indexed yet?"
- "When was the last time indexing ran?"
- "How many of my group's projects have been code-indexed?"

This information lives in the indexer's checkpoint tables but isn't exposed through any RPC. We want to evolve `GetGraphStats` into `GetGraphStatus`, a single RPC that combines entity counts with indexing progress.

The GKG proto is the source of truth for the response schema. The Rails REST endpoint (`GET /api/v4/orbit/graph_status`) is a thin proxy that forwards to GKG. If the GKG response shape changes, Rails adjusts its mapping, not the other way around.

### Current state

- Accepts a raw `traversal_path` string.
- Runs a `UNION ALL` of `count()` queries across all node tables with a `traversal_path` column.
- Returns counts grouped by ontology domain (core, source_code, ci, plan, security, code_review).
- Does not distinguish between group and project scope.
- `count()` overcounts on `ReplacingMergeTree` tables between background merges.

## Decision

Four phases, each a standalone MR.

### Phase 1: Accurate counts with `uniq(id)`

The request keeps `traversal_path` as the only input:

```protobuf
message GetGraphStatsRequest {
  string traversal_path = 1;
}
```

The service looks up the traversal path in `gl_group` and `gl_project` to determine whether it belongs to a group or project. Either way, entity counts are returned for all node types under the traversal path using `startsWith(traversal_path, ...)`. Subgroups roll up: requesting a parent group counts everything under it. If neither table matches, the request fails with `not_found`.

Replace `count()` with `uniq(id)`. Graph tables use `ReplacingMergeTree`, which keeps multiple row versions between background merges. `count()` overcounts proportionally to update volume (observed 49% overall, up to 300% for frequently updated types on staging). `uniq(id)` uses HyperLogLog to count distinct entity IDs with ~1-2% error at high cardinalities — acceptable for a status indicator and far better than the current overcount.

### Phase 2: Indexing progress via NATS KV

The indexer writes indexing metadata to a NATS KV bucket (`indexing_progress`) after each run completes. Each key maps to a project or namespace:

- SDLC: keyed by top-level namespace ID (e.g., `sdlc.9970`)
- Code: keyed by project ID (e.g., `code.278964`)

The value is the same shape for both: `last_started_at`, `last_completed_at`, `last_duration_ms`, `last_error`. Overwritten on every run, so it always reflects the most recent attempt. A non-empty `last_error` means the last run failed.

Reads are O(1) lookups — no extra ClickHouse queries for indexing metadata. The `projects.indexed` / `projects.total_known` counts still come from ClickHouse since they require aggregation.

### Phase 3: Rename to GetGraphStatus, wire indexing metadata into the response

Rename the RPC to `GetGraphStatus`.

```protobuf
message GetGraphStatusRequest {
  string traversal_path = 1;
}
```

The response is flat: indexing metadata at the top level, a `projects` object, then `stats` (entity counts by ontology domain). Same fields regardless of scope.

The `projects` counts use `startsWith(traversal_path, ...)` on both `gl_project` (for `total_known`) and `code_indexing_checkpoint` (for `indexed`). Both tables are ordered by `traversal_path`, so prefix filtering is efficient.

### Phase 4: Response caching via NATS KV

Cache the full serialized response in a NATS KV bucket keyed by traversal path with a 60-second TTL. On a hit, return the cached response without touching ClickHouse. On a miss, run the queries, cache, and return.

This absorbs bursts from the UI or multiple consumers polling the same namespace without hammering ClickHouse. 60 seconds is short enough for reasonably fresh data.

## Examples

### Group scope

**Request:**

```json
{ "traversal_path": "9970/12345/" }
```

**Flow:**

1. Authorize the caller against the traversal path.
2. Look up the traversal path in `gl_group` and `gl_project`. Matches `gl_group` — group-scoped request.
3. If not a top-level group, resolve the top-level group from the traversal path. SDLC indexing runs at the top-level namespace, so indexing metadata comes from the top-level group's progress entry.
4. Read indexing progress from NATS KV (`sdlc.{namespace_id}`).
5. Count all entities under the traversal path using `uniq(id)` per entity type, grouped by domain.
6. Count projects known (`uniq(id)` on `gl_project`) vs projects indexed (`uniq(project_id)` on `code_indexing_checkpoint`) under the traversal path.

**Response:**

```json
{
  "last_started_at": "2026-04-10T11:50:00Z",
  "last_completed_at": "2026-04-10T11:55:00Z",
  "last_duration_ms": 300,
  "last_error": "",
  "projects": {
    "indexed": 45,
    "total_known": 150
  },
  "stats": [
    {
      "name": "core",
      "items": [
        { "name": "Project", "count": 150 },
        { "name": "Group", "count": 23 },
        { "name": "User", "count": 891 }
      ]
    },
    {
      "name": "code_review",
      "items": [
        { "name": "MergeRequest", "count": 8432 }
      ]
    },
    {
      "name": "ci",
      "items": [
        { "name": "Pipeline", "count": 12903 },
        { "name": "Job", "count": 51204 }
      ]
    },
    {
      "name": "plan",
      "items": [
        { "name": "WorkItem", "count": 3201 },
        { "name": "Milestone", "count": 87 }
      ]
    }
  ]
}
```

For a subgroup, indexing metadata comes from the top-level group. Entity counts and the `projects` ratio are scoped to the subgroup's traversal path.

### Project scope

**Request:**

```json
{ "traversal_path": "9970/12345/278964/" }
```

**Flow:**

1. Authorize the caller against the traversal path.
2. Look up the traversal path in `gl_group` and `gl_project`. Matches `gl_project` — project-scoped request.
3. Read indexing progress from NATS KV (`code.{project_id}`).
4. Count all entities under the project using `uniq(id)` per entity type, grouped by domain.

**Response:**

```json
{
  "last_started_at": "2026-04-10T11:30:00Z",
  "last_completed_at": "2026-04-10T11:30:05Z",
  "last_duration_ms": 5000,
  "last_error": "",
  "projects": {
    "indexed": 1,
    "total_known": 1
  },
  "stats": [
    {
      "name": "core",
      "items": [
        { "name": "Project", "count": 1 }
      ]
    },
    {
      "name": "source_code",
      "items": [
        { "name": "Branch", "count": 3 },
        { "name": "File", "count": 500 },
        { "name": "Directory", "count": 50 },
        { "name": "Definition", "count": 2000 },
        { "name": "ImportedSymbol", "count": 1500 }
      ]
    },
    {
      "name": "code_review",
      "items": [
        { "name": "MergeRequest", "count": 47 }
      ]
    },
    {
      "name": "ci",
      "items": [
        { "name": "Pipeline", "count": 312 },
        { "name": "Job", "count": 1580 }
      ]
    },
    {
      "name": "plan",
      "items": [
        { "name": "WorkItem", "count": 89 }
      ]
    }
  ]
}
```

### Data sources

| Response field | Source | Scope |
|---|---|---|
| `last_started_at`, `last_completed_at`, `last_duration_ms`, `last_error` | NATS KV `indexing_progress`, key `sdlc.{top_level_namespace_id}` (group) or `code.{project_id}` (project) | Both |
| `projects.total_known` | `uniq(id)` on `gl_project`, `startsWith(traversal_path, ...)` | Both (1 for project scope) |
| `projects.indexed` | `uniq(project_id)` on `code_indexing_checkpoint`, `startsWith(traversal_path, ...)` | Both (1 or 0 for project scope) |
| `stats[].items[].count` | `uniq(id)` per node table, `startsWith(traversal_path, ...)` | Both |

## Why the code/SDLC split

| Property | Code indexing | SDLC indexing |
|---|---|---|
| Trigger | Push to default branch (discrete task) | CDC stream (continuous) |
| Granularity | Per-project, per-branch | Per-namespace, per-entity-type |
| Checkpoint | `code_indexing_checkpoint` (last_commit, indexed_at) | `checkpoint` (watermark, cursor) |
| "Is it done?" | Yes: checkpoint exists with recent `indexed_at` | Not really: CDC is ongoing, freshness is the metric |
| Scope | Project-level | Namespace-level |

A project can be "fully code-indexed" while SDLC data is always streaming. The split lets consumers show a completion indicator for code and a freshness indicator for SDLC rather than a misleading unified status.

## Alternatives considered

### Separate `GetIndexingStatus` RPC

Two RPCs that consumers must call and correlate. Low entity counts might mean "not indexed yet" rather than "empty namespace." A single RPC avoids that ambiguity.

### `FINAL` for exact counts

`FINAL` forces ClickHouse to deduplicate at query time. On staging with 16M edge rows, `FINAL` reads 14.4M rows (620 MB) and takes 579ms vs 71ms without. At production scale this would take minutes. `uniq(id)` gets equivalent deduplication via HyperLogLog without the cost.

### HTTP GET for CDN caching

A bodyless GET could benefit from CDN caching, but the initial implementation is gRPC-only. Worth revisiting when the endpoint is exposed directly to the UI.

## Consequences

What improves:

- Single RPC for "what data exists" and "how fresh is it."
- Indexing coverage ratio (projects indexed vs known) is the metric operators actually want.
- `uniq(id)` eliminates overcounting from `ReplacingMergeTree` row duplication.
- Response caching via NATS KV protects ClickHouse from repeated calls.

What gets harder:

- The endpoint runs multiple queries per request (entity counts + NATS lookups) instead of one.
- Subgroup requests need to resolve the top-level group for SDLC indexing metadata.

## References

- [Code indexing design document](../indexing/code_indexing.md)
- [SDLC indexing design document](../indexing/sdlc_indexing.md)
- [ADR 005: PostgreSQL task table for code indexing triggers](005_code_indexing_task_table.md)
- [Security and authorization design](../security.md)
- [Rails MR !231381: Add GET /api/v4/orbit/graph_status endpoint](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/231381)
