---
title: "GKG ADR 004: Unified Query Response Schema"
creation-date: "2026-03-11"
authors: [ "@michaelangeloio", "@michaelusa", "@jgdoyon1", "@bohdanpk" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-03-11

## Context

### The problem

The GKG server returns flat tabular JSON rows. Graph topology is encoded in column naming conventions: a user node is `u_username`, an edge is `e0_type`, a neighbor is `_gkg_neighbor_id`. Each of the five query types (search, traversal, aggregation, path_finding, neighbors) produces a different output shape. There is no shared structure.

Internal `_gkg_*` columns leak into the output. The frontend has an entire file, `graph_transform.js`, whose only job is reconstructing nodes and edges from these flat rows. It relies on 11 hardcoded assumptions:

- Alias detection by scanning for `_type` suffixes
- Hardcoded neighbor keys (`_gkg_neighbor_id`, `_gkg_neighbor_type`, `_gkg_relationship_type`)
- Label priority heuristics (`name || title || username || full_path || id`)
- Per-query-type dispatch logic
- Column name pattern matching for entity extraction

There is no metadata envelope. The frontend cannot tell what query type produced a result, what types the columns are, or how many rows came back without inspecting the data. The ontology defines `label_field` and `primary_key` per entity, but query results don't reference them, so the frontend maintains a parallel mapping.

### What other databases do

We looked at response formats from Neo4j, Kuzu, ArangoDB, TigerGraph, Dgraph, AGE, Memgraph, and SurrealDB.

- Neo4j had a graph sidecar (returning both `row` and `graph` sections). They deprecated it in Query API v2 because every property was fully duplicated between the two sections. Bolt drivers and Neo4j Browser now extract graph topology client-side.
- Every database we checked converges on the same pattern: return structured inline objects, let the client extract graph entities.
- TigerGraph separates identity from data cleanly: `{ v_id, v_type, attributes: {} }`.
- ArangoDB paths use `{ vertices: [], edges: [] }`, close to what our PathFinding already does.
- All databases use the same response envelope for aggregated and non-aggregated queries. The cell content changes, not the structure.

### How we got here

1. **Graph sidecar alongside tabular rows** (Snippets [5965027](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965027), [5965036](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965036)). Rejected. Neo4j's `AggregatingWriter` copies every property twice with no reference mechanism. They dropped it in Query API v2 (5.19+). We dropped it for the same reason.

2. **Tabular rows with column descriptors**. Each row was a map of alias keys to node objects, with column descriptors and edge specs. PathFinding and Neighbors didn't fit: paths are variable-length sequences, neighbors have dynamic entity types. Both needed per-query-type dispatch. A user appearing in 50 traversal rows still had properties repeated 50 times.

3. **Graph-native nodes+edges**. Adopted after discussion between JG, Angelo, and Michael. JG pointed out that for table display, you group entities by type and show each type in its own stacked table. Edges feed graph visualizations directly. The shape stays the same across all query types.

## Decision

### Two layers

**Layer 1: Ontology (cached, fetched once)**

Entity types, property definitions, label fields, domains, styles, edge definitions. The frontend caches this on first load and builds lookup maps:

```plaintext
ontology.nodes["User"].label_field  -> "username"
ontology.nodes["User"].domain       -> "core"
ontology.nodes["User"].style        -> { color: "#10B981", size: 32 }
```

This already exists via `GET /api/v4/orbit/schema` / `GetGraphSchema` gRPC (see [ADR 003](003_api_design.md)).

**Layer 2: Query response (per-request)**

Typed metadata goes in the proto envelope. The JSON payload is always `{ query_type, nodes, edges }` with an optional `columns` array for aggregation queries.

Target proto shape (changes pending in !479; current proto uses `QueryMetadata` with `query_type`, `raw_query_strings`, `row_count` only):

```protobuf
message ExecuteQueryResult {
  string result_json = 1;               // JSON: { query_type, nodes, edges, columns? }
  string query_type = 2;               // "search", "traversal", etc.
  repeated string raw_query_strings = 3; // SQL through pipeline stages
  int32 row_count = 4;
}
```

Node `id` is always a JSON integer (ClickHouse Int64). All entity primary keys in the ontology are integer-typed. `raw_query_strings` is returned in non-production environments only; production deployments gate it behind a debug flag.

### Examples by query type

Every query returns `{ query_type, nodes, edges }`. Aggregation queries additionally include `columns`. The content varies, the base shape does not.

#### Search

Nodes only, no edges.

```json
{
  "query_type": "search",
  "nodes": [
    { "type": "User", "id": 1, "username": "alice", "name": "Alice", "state": "active" },
    { "type": "User", "id": 2, "username": "bob", "name": "Bob", "state": "active" }
  ],
  "edges": []
}
```

Table view shows one Users table. Graph view shows unconnected nodes.

#### Traversal (single-hop)

Nodes are deduplicated. Edges are instance-level.

```json
{
  "query_type": "traversal",
  "nodes": [
    { "type": "User", "id": 1, "username": "alice", "name": "Alice", "state": "active" },
    { "type": "User", "id": 2, "username": "bob", "name": "Bob", "state": "active" },
    { "type": "Project", "id": 101, "name": "Alpha", "full_path": "gitlab-org/alpha" },
    { "type": "Project", "id": 102, "name": "Beta", "full_path": "gitlab-org/beta" }
  ],
  "edges": [
    { "from": "User", "from_id": 1, "to": "Project", "to_id": 101, "type": "MEMBER_OF" },
    { "from": "User", "from_id": 1, "to": "Project", "to_id": 102, "type": "MEMBER_OF" },
    { "from": "User", "from_id": 2, "to": "Project", "to_id": 101, "type": "MEMBER_OF" }
  ]
}
```

User:1 appears once even though it has 2 edges. Table view stacks a Users table above a Projects table. Graph view gets 4 nodes and 3 edges directly.

#### Traversal (variable-length, max_hops > 1)

Edges carry a `depth` field.

```json
{
  "query_type": "traversal",
  "nodes": [
    { "type": "User", "id": 1, "username": "alice" },
    { "type": "Project", "id": 101, "name": "Alpha" },
    { "type": "Project", "id": 102, "name": "Beta" },
    { "type": "Project", "id": 103, "name": "Gamma" }
  ],
  "edges": [
    { "from": "User", "from_id": 1, "to": "Project", "to_id": 101, "type": "MEMBER_OF", "depth": 1 },
    { "from": "User", "from_id": 1, "to": "Project", "to_id": 102, "type": "MEMBER_OF", "depth": 2 },
    { "from": "User", "from_id": 1, "to": "Project", "to_id": 103, "type": "MEMBER_OF", "depth": 3 }
  ]
}
```

#### Aggregation (grouped)

Computed values are inlined on the group-by nodes. `columns` describes each aggregate so the consumer can distinguish computed values from entity properties. The frontend uses `query_type` to determine display mode (table vs graph).

```json
{
  "query_type": "aggregation",
  "nodes": [
    { "type": "Project", "id": 101, "name": "Alpha", "mr_count": 15, "avg_mr": 42.7 },
    { "type": "Project", "id": 102, "name": "Beta", "mr_count": 8, "avg_mr": 23.1 }
  ],
  "edges": [],
  "columns": [
    { "name": "mr_count", "function": "count", "target": "m" },
    { "name": "avg_mr", "function": "avg", "target": "m", "property": "id" }
  ]
}

#### Aggregation (ungrouped / scalar)

When no `group_by` is specified, the SQL returns only aggregate values with no entity columns. There are no nodes to carry the values, so `columns` holds both the metadata and the computed `value` directly. `nodes` is empty.

```json
{
  "query_type": "aggregation",
  "nodes": [],
  "edges": [],
  "columns": [
    { "name": "total", "function": "count", "target": "p", "value": 42 },
    { "name": "avg_size", "function": "avg", "target": "p", "property": "size", "value": 128.5 }
  ]
}
```

#### Path finding

Edges carry `path_id` and `step`. Nodes are deduplicated across paths.

```json
{
  "query_type": "path_finding",
  "nodes": [
    { "type": "User", "id": 1, "username": "alice" },
    { "type": "MergeRequest", "id": 42, "title": "Fix bug" },
    { "type": "Note", "id": 55, "title": "Design doc" },
    { "type": "Project", "id": 200, "name": "Omega" }
  ],
  "edges": [
    { "from": "User", "from_id": 1, "to": "MergeRequest", "to_id": 42, "type": "AUTHORED", "path_id": 0, "step": 0 },
    { "from": "MergeRequest", "from_id": 42, "to": "Project", "to_id": 200, "type": "IN_PROJECT", "path_id": 0, "step": 1 },
    { "from": "User", "from_id": 1, "to": "Note", "to_id": 55, "type": "AUTHORED", "path_id": 1, "step": 0 },
    { "from": "Note", "from_id": 55, "to": "Project", "to_id": 200, "type": "CONTAINS", "path_id": 1, "step": 1 }
  ]
}
```

Two paths from User:1 to Project:200. Both endpoints appear once in `nodes`. The graph view renders everything. Table view can group by `path_id` and sort by `step`.

#### Neighbors

Center node plus its neighbors. Edge direction matches the ontology.

```json
{
  "query_type": "neighbors",
  "nodes": [
    { "type": "Project", "id": 101, "name": "Alpha", "full_path": "gitlab-org/alpha" },
    { "type": "MergeRequest", "id": 42, "title": "Fix bug", "state": "merged" },
    { "type": "User", "id": 1, "username": "alice", "name": "Alice" },
    { "type": "File", "id": 500, "path": "app/controllers/sessions_controller.rb" }
  ],
  "edges": [
    { "from": "MergeRequest", "from_id": 42, "to": "Project", "to_id": 101, "type": "IN_PROJECT" },
    { "from": "User", "from_id": 1, "to": "Project", "to_id": 101, "type": "MEMBER_OF" },
    { "from": "Project", "from_id": 101, "to": "File", "to_id": 500, "type": "CONTAINS" }
  ]
}
```

The center node (Project:101) is just another node in the list. Neighbor types are mixed because they come from dynamic hydration. Graph view shows a star topology.

### Shapes

**Node:** flat object with `type`, `id`, and properties inline. No wrapper.

```json
{ "type": "User", "id": 42, "username": "alice", "name": "Alice Smith", "state": "active" }
```

The frontend builds composite IDs (`"User:42"`) for deduplication. Property names match the ontology, so the frontend can look up data types from the cached schema. For aggregation queries, computed values (like `mr_count`) are inlined as additional properties on the node.

**Column:** describes a computed aggregation value.

```json
{ "name": "mr_count", "function": "count", "target": "m" }
```

Optional fields: `target` (node alias being aggregated), `property` (field being aggregated, absent for plain `count`), `value` (the computed result, present only for ungrouped aggregations where `nodes` is empty). Present for all aggregation queries so the consumer can distinguish computed values from entity properties and display correct table headers.

**Edge:** two nodes connected by type and ID.

```json
{ "from": "User", "from_id": 1, "to": "Project", "to_id": 101, "type": "MEMBER_OF" }
```

Optional fields: `depth` (variable-length traversals), `path_id` + `step` (path finding).

### Design principles

1. Nodes are deduplicated. Each entity appears once.
2. Edges are instance-level. Each edge connects two specific nodes by `type`+`id`.
3. One shape for all query types. Search, traversal, aggregation, path_finding, neighbors all produce `{ query_type, nodes, edges, pagination }`. Aggregation queries additionally include `columns` to describe the computed values.
4. No internal columns leak. The formatter strips `_gkg_*` prefixes.
5. Metadata in proto, data in JSON. `query_type`, `raw_query_strings`, `row_count`, `pagination` are typed proto fields. The JSON includes `pagination` when a cursor was requested.
6. No redaction info exposed. Authorization is applied server-side. The consumer only sees what they are allowed to see.
7. Ontology is cached. Display metadata (labels, styles, descriptions) comes from the schema, not the response.
8. `id` and `type` are always included on nodes, even if the user didn't select them.
9. Pagination uses an agent-driven cursor model (`{ offset, page_size }`) that slices the authorized (post-redaction) result set. `PaginationInfo { has_more, total_rows }` is returned in both the proto metadata and the JSON body.

### Display hint

The frontend picks a default view from `query_type`:

| Query type | Default view |
|------------|-------------|
| `search`, `aggregation` | Table |
| `traversal`, `path_finding`, `neighbors` | Graph |

The user can always switch.

### Implementation

`GraphFormatter` in Rust replaces `RawRowFormatter` as the default. `GoonFormatter` handles LLM output (GOON/TOON format). `ResultContext` was extended with `EdgeMeta` to carry edge column metadata through the pipeline. A JSON Schema at `crates/gkg-server/schemas/query_response.json` is the shared contract between server and frontend. On the frontend side, `graph_transform.js` goes away entirely, replaced by ~30 lines of `buildGraphData()` that passes nodes and edges straight to Three.js.

## Consequences

**What improves:**

- The frontend stops reverse-engineering topology from column names.
- One parsing path for all 5 query types. No per-type dispatch.
- A user appearing in 50 rows becomes 1 node object. No data repetition.
- The JSON Schema prevents server/frontend drift.
- Graph data goes straight to Three.js with no extraction step.
- Tables are stacked per entity type, with columns from the ontology.

**What gets harder:**

- Breaking change to `result_json`. Rails passes the JSON through without parsing, so no Rails changes are needed, but any consumer that parses the JSON will need to handle the new shape.
- Single-entity search responses are slightly larger due to the envelope overhead.
- Adding a new query type means adding a new extractor in `GraphFormatter`.

## References

- Issue: #243 (Establish GKG Response Format)
- MR: !479
- Snippets: [5965027](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965027) (Michael U.), [5965036](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965036) (Angelo), [5965394](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965394) (Combined research)
- [ADR 003](003_api_design.md)
- MR !411 note on proto sync decisions
- JSON Schema: `crates/gkg-server/schemas/query_response.json`
