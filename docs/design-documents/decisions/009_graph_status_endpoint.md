---
title: "GKG ADR 009: Evolving GetGraphStats into GetGraphStatus"
creation-date: "2026-04-21"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

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

The request includes a `scope` field so Rails tells GKG whether this is a group or project request. Rails already knows the scope from the request context, so there is no reason for GKG to spend extra ClickHouse queries looking it up:

```protobuf
message GetGraphStatsRequest {
  string traversal_path = 1;
  SourceType source_type = 2;
}

enum SourceType {
  SOURCE_TYPE_GROUP = 0;
  SOURCE_TYPE_PROJECT = 1;
}
```

Entity counts are returned for all node types under the traversal path using `startsWith(traversal_path, ...)`. Subgroups roll up: requesting a parent group counts everything under it.

Replace `count()` with `uniq(id)`. Graph tables use `ReplacingMergeTree`, which keeps multiple row versions between background merges. `count()` overcounts proportionally to update volume (observed 49% overall, up to 300% for frequently updated types on staging). `uniq(id)` uses HyperLogLog to count distinct entity IDs with ~1-2% error at high cardinalities — acceptable for a status indicator and far better than the current overcount.

#### Per-entity access control

Entity counts must respect the caller's access level. A user who cannot see vulnerabilities in the query pipeline must not see vulnerability counts in the stats response.

[!987](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/987) adds `required_role` to ontology nodes (e.g., `security_manager` for Vulnerability) and tags each traversal path in the JWT with the user's `access_level` on that path. The compiler's security pass calls `SecurityContext::paths_at_least(min_role)` to drop paths where the user's role is below the entity's floor.

The stats endpoint uses the same check. Before including a node type in the `UNION ALL`, skip it if the user has no traversal path that meets the entity's `required_access_level`:

```rust
.filter(|node| {
    let min_role = node.required_access_level();
    !security_context.paths_at_least(min_role).is_empty()
})
```

Entities the user cannot access at any path are excluded from the query entirely. A Reporter-only user sees zero Vulnerability rows in query results and zero Vulnerability counts in stats.

### Phase 2: Indexing progress via NATS KV

The indexer writes indexing metadata to a NATS KV bucket (`indexing_progress`) after each run completes. Each key maps to a project or namespace:

- SDLC: keyed by top-level namespace ID (e.g., `sdlc.9970`)
- Code: keyed by project ID (e.g., `code.278964`)

The value is the same shape for both: `last_started_at`, `last_completed_at`, `last_duration_ms`, `last_error`. Overwritten on every run, so it always reflects the most recent attempt. A non-empty `last_error` means the last run failed.

Reads are O(1) lookups — no extra ClickHouse queries for indexing metadata. The `projects.indexed` / `projects.total_known` counts still come from ClickHouse since they require aggregation.

Schema migrations trigger a full re-index, but the previous progress entry stays valid until the re-index completes. The data is stale but still accurate for the old schema version, so the endpoint keeps serving it rather than showing nothing.

### Phase 3: Rename to GetGraphStatus, wire indexing metadata into the response

Rename the RPC to `GetGraphStatus`.

```protobuf
message GetGraphStatusRequest {
  string traversal_path = 1;
  SourceType source_type = 2;
}
```

The response is flat: indexing metadata at the top level, a `projects` object, then `stats` (entity counts by ontology domain). Same fields regardless of scope.

The `projects` counts use `startsWith(traversal_path, ...)` on both `gl_project` (for `total_known`) and `code_indexing_checkpoint` (for `indexed`). Both tables are ordered by `traversal_path`, so prefix filtering is efficient.

### Phase 4: Response caching via NATS KV

Cache the full serialized response in a NATS KV bucket keyed by traversal path with a 60-second TTL. On a hit, return the cached response without touching ClickHouse. On a miss, run the queries, cache, and return.

The indexer invalidates the cached entry for the relevant traversal path after each indexing run, so consumers see fresh data immediately after indexing completes rather than waiting for the TTL to expire. The 60-second TTL is a fallback for bursts between indexing runs.

## Examples

### Group scope

**Request:**

```json
{ "traversal_path": "9970/12345/" }
```

**Flow:**

1. Authorize the caller against the traversal path.
2. Scope is `GROUP` (provided by Rails).
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
2. Source type is `PROJECT` (provided by Rails).
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

| Response field | Source |
|---|---|
| `last_started_at`, `last_completed_at`, `last_duration_ms`, `last_error` | NATS KV `indexing_progress`, key `sdlc.{top_level_namespace_id}` (group) or `code.{project_id}` (project) |
| `projects.total_known` | `uniq(id)` on `gl_project`, `startsWith(traversal_path, ...)` |
| `projects.indexed` | `uniq(project_id)` on `code_indexing_checkpoint`, `startsWith(traversal_path, ...)` |
| `stats[].items[].count` | `uniq(id)` per node table, `startsWith(traversal_path, ...)` |

## Rails endpoint

`GET /api/v4/orbit/graph_status` is the REST surface that consumers (Rails UI, Duo) call. It is a thin proxy: Rails resolves the namespace, builds a JWT with the caller's per-path access levels, and forwards to GKG's `GetGraphStatus` gRPC. GKG owns the response schema.

Based on the initial implementation in [Rails MR !231381](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/231381), adapted to the current design.

### Request

Callers identify the scope with exactly one of three parameters:

| Parameter | Type | Description |
|---|---|---|
| `namespace_id` | Integer | Group ID |
| `project_id` | Integer | Project ID |
| `full_path` | String | Full path of a group or project (e.g., `gitlab-org/gitlab`) |

The endpoint resolves the parameter to a namespace, checks `read_group` permission, and derives the `traversal_path` and `source_type` for the gRPC call.

### Namespace resolution

```ruby
def resolve_graph_status_namespace(params)
  namespace = if params[:namespace_id]
                Group.find_by_id(params[:namespace_id])
              elsif params[:project_id]
                Project.find_by_id(params[:project_id])&.namespace
              elsif params[:full_path]
                routable = Routable.find_by_full_path(params[:full_path])
                routable.is_a?(Project) ? routable.namespace : routable
              end

  namespace.is_a?(Group) ? namespace : nil
end
```

For projects, the namespace is the parent group. The `source_type` sent to GKG is `SOURCE_TYPE_PROJECT` when the caller passed `project_id`, `SOURCE_TYPE_GROUP` otherwise.

### gRPC call

The gRPC client builds the request and forwards it to GKG. The JWT (built by `request_kwargs`) carries the caller's `group_traversal_ids` with per-path `access_level`. GKG uses these for traversal-path scoping and per-entity role filtering.

```ruby
def get_graph_status(user:, source_type:, traversal_path:, timeout: DEFAULT_TIMEOUT)
  request = Gkg::V1::GetGraphStatusRequest.new(
    traversal_path: traversal_path,
    source_type: source_type
  )
  kwargs = request_kwargs(user: user, source_type: source_type, timeout: timeout)

  response = stub.get_graph_status(request, **kwargs)
  map_graph_status_response(response)
end
```

### Response mapping

GKG returns the protobuf response; Rails maps it to JSON for the REST API:

```ruby
def map_graph_status_response(response)
  {
    last_started_at: response.last_started_at,
    last_completed_at: response.last_completed_at,
    last_duration_ms: response.last_duration_ms,
    last_error: response.last_error.presence,
    projects: {
      indexed: response.projects.indexed,
      total_known: response.projects.total_known
    },
    stats: response.domains.map do |domain|
      {
        name: domain.name,
        items: domain.items.map { |item| { name: item.name, count: item.count } }
      }
    end
  }
end
```

### Authorization

- Route-level: `permissions: :read_knowledge_graph` with `boundary_type: :user`.
- Namespace-level: `can?(current_user, :read_group, namespace)` before calling GKG.
- Entity-level: handled by GKG via per-entity role scoping (see Phase 1). The JWT carries per-path access levels; GKG filters entity counts based on the caller's role. Rails does not need to know which entity types require elevated access.

### Error handling

| Condition | HTTP status |
|---|---|
| No lookup parameter provided | 400 |
| Namespace not found or not accessible | 404 |
| GKG unreachable | 503 |
| GKG authorization error | 403 |

## Alternatives considered

### Separate `GetIndexingStatus` RPC

Two RPCs that consumers must call and correlate. Low entity counts might mean "not indexed yet" rather than "empty namespace." A single RPC avoids that ambiguity.

### `FINAL` for exact counts

`FINAL` forces ClickHouse to deduplicate at query time. On staging with 16M edge rows, `FINAL` reads 14.4M rows (620 MB) and takes 579ms vs 71ms without. At production scale this would take minutes. `uniq(id)` gets equivalent deduplication via HyperLogLog without the cost.

### Pre-compute stats at index time

The indexer could write pre-aggregated entity counts to NATS KV alongside the indexing progress, making the entire response an O(1) read. Rejected because it adds complexity to the indexer for an endpoint that doesn't get enough traffic to justify it. If that changes, this is a natural next step after Phase 4.

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
- [MR !987: Per-entity role scoping for aggregation targets](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/987)
- [Rails MR !231381: Add GET /api/v4/orbit/graph_status endpoint](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/231381)
