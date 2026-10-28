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

The Orbit server returned flat tabular JSON rows.
Column naming conventions encoded the graph topology.
A prefix on a column name marked a node alias, an edge, or a neighbor.
Each of the five query types produced a different output shape, with no shared structure.

Internal columns leaked into the output.
The frontend had a full module that only rebuilt nodes and edges from the flat rows.
That module depended on hardcoded assumptions:

- Alias detection from column name suffixes
- Fixed column names for neighbor IDs, neighbor types, and relationship types
- A label priority list across common properties
- Dispatch logic for each query type
- Column name pattern matching to extract entities

The response had no metadata envelope.
The frontend could not tell the query type, the column types, or the row count unless it scanned the data.
The ontology defines a label field and a primary key for each entity, but query results did not refer to them.
So the frontend kept a parallel mapping.

We compared the response formats of Neo4j, Kuzu, ArangoDB, TigerGraph, Dgraph, AGE, Memgraph, and SurrealDB:

- Neo4j returned a graph section next to the row section, and then deprecated it in Query API v2.
  The two sections duplicated every property.
  Neo4j drivers and Neo4j Browser now extract the graph topology on the client.
- Every database we checked returns structured inline objects and lets the client extract graph entities.
- TigerGraph keeps identity apart from data, with an ID, a type, and an attributes object.
- ArangoDB returns a path as a list of vertices and a list of edges, close to our path finding output.
- All of them use one envelope for aggregated and non-aggregated queries.
  The cell content changes, and the structure does not.

We evaluated three designs.
Two are under Why not the alternatives.
The team adopted the third, a graph-native shape, after discussion.
For a table view, the frontend groups entities by type and shows one stacked table per type.
For a graph view, the edges feed the visualization directly.

## Decision

**Every query returns one graph-native shape: a list of deduplicated nodes and a list of edges.**

Orbit splits the data into two layers.

The first layer is the ontology.
It holds entity types, property definitions, label fields, domains, styles, and edge definitions.
The frontend fetches it once from the schema API ([ADR 003](003_api_design.md)), caches it, and builds lookup maps.
Display metadata such as labels, styles, and descriptions comes from the ontology, not from the response.

The second layer is the response to each query.
Typed metadata goes in the gRPC envelope: the query type, the row count, the format name, and the format version.
The envelope also carries the compiled ClickHouse SQL for debugging.
Non-production environments return the SQL, and production gates it behind a debug flag.
The payload is structured JSON, or compact text for large language models (LLMs) as described in [ADR 012](012_goon_format.md).

The JSON payload holds the format version, the query type, the nodes, the edges, and pagination.
Aggregation queries also hold column descriptors, group column descriptors, and rows.
This illustrative single-hop traversal shows the base shape:

```json
{
  "format_version": "<version>",
  "query_type": "traversal",
  "nodes": [
    { "type": "User", "id": "1", "username": "sidney" },
    { "type": "Project", "id": "101", "name": "Alpha" },
    { "type": "Project", "id": "102", "name": "Beta" }
  ],
  "edges": [
    { "from": "User", "from_id": "1", "to": "Project", "to_id": "101", "type": "MEMBER_OF" },
    { "from": "User", "from_id": "1", "to": "Project", "to_id": "102", "type": "MEMBER_OF" }
  ]
}
```

A node is a flat object with its type, its ID, and its properties inline, with no wrapper.
Property names match the ontology, so the frontend reads data types from the cached schema.
The frontend joins the type and the ID into a composite key to deduplicate nodes.
An edge names its source type and ID, its target type and ID, and its relationship type.

Each query type fills the same shape:

- A lookup is a traversal with one node and no relationships, and it returns nodes with no edges.
- A traversal returns each node once and one edge for each connected pair.
- A variable-length traversal adds a depth to each edge.
- Path finding adds a path ID and a step to each edge, and it deduplicates nodes across paths.
- Neighbors returns the center node as one more node in the list, next to neighbors of mixed types.
- Aggregation keeps computed values in rows, never on graph nodes.

An aggregation column descriptor names the function, the target node alias, and an optional property.
A group column descriptor describes the group key, which is a node or a property.
A row for a node group nests the grouped entity under the group key, with the metric values next to it.
A row for a property group holds the bucket values and the metric values.
An aggregation with no group key returns one row of metric values and no nodes.

Node IDs are always JSON strings.
Hash-based code graph IDs often exceed the largest integer that JavaScript holds without precision loss.
Edge source and target IDs are strings too.
Entity primary keys stay integers inside the server.
Query input accepts IDs as integers or digit strings, so consumers can send IDs back without a cast.
Aggregation metric values keep their JSON numeric type.

The shape follows these rules:

- Each entity appears once in the node list.
- Each edge connects two specific nodes by type and ID.
- Every node carries its ID and type, even when the query did not select them.
- No internal columns appear in the output.
- No redaction information appears in the output, because authorization runs on the server.
- Pagination uses keyset cursors, and the server compiles the seek predicate into SQL.
- The response always carries pagination state: whether more results exist, whether the server truncated the result, and an optional next cursor.
- The JSON has no row count field, because the envelope row count holds the authorized row count of the page.

The frontend picks a default view from the query type, and the user can always switch:

| Query type | Default view |
|------------|--------------|
| Aggregation, single-node traversal | Table |
| Multi-node traversal, path finding, neighbors | Graph |

A graph formatter on the server replaces the raw row formatter as the default.
A JSON Schema is the shared contract between the server and the frontend.
The frontend drops its reconstruction module and passes nodes and edges straight to the Three.js renderer.

### Format versioning

**Every response carries a semantic version of its format.**

A major bump signals a breaking change to the shape.
A minor bump signals a new optional field.
A patch bump signals a fix to formatting.

The version appears in three places:

1. The top-level field of the JSON body.
1. The gRPC metadata, next to the format name.
1. The JSON Schema ID, whose suffix tracks the major version.

The continuous integration (CI) pipeline checks that the schema ID and the version agree.
CI also requires a strictly greater version for any change to the formatter code or the response schema.
Each formatter reports its own name and version, so the gRPC service sets the metadata without hardcoded values.
A stub formatter that delegates to another formatter reports no version.
The metadata then carries an empty string, so telemetry shows stub output.
The LLM text format gets its own version when its encoding ships ([ADR 012](012_goon_format.md)).

## Benefits of this approach

- The frontend no longer rebuilds topology from column names.
- All five query types use one parse path, with no dispatch for each type.
- A user that appears in 50 rows becomes one node object, with no repeated data.
- The JSON Schema prevents drift between the server and the frontend.
- Graph data goes to the renderer with no extraction step.
- Tables stack by entity type, with columns from the ontology.

## Consequences

- The JSON result is a breaking change.
  Rails passes the JSON through without a parse, so Rails needs no change.
  Any consumer that parses the JSON must handle the new shape.
- A single-entity traversal response is slightly larger because of the envelope.
- A new query type needs a new extractor in the graph formatter.

## Why not the alternatives

### A graph section next to tabular rows

The response would carry the tabular rows and a separate graph section.
Neo4j used this design and copied every property into both sections, with no reference between them.
Neo4j removed it in Query API v2, and we rejected it for the same reason.

### Tabular rows with column descriptors

Each row would map alias keys to node objects, with column descriptors and edge specifications.
Path finding and neighbors did not fit.
A path is a sequence of variable length, and neighbors have entity types that change per result.
Both needed dispatch for each query type.
A user in 50 traversal rows still had its properties repeated 50 times.

## References

- [Establish Orbit Response Format](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/243)
- [Unified response schema](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/479)
- [Align the gRPC contract for API design](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/411), with the note on protobuf sync decisions
- [Spike: unified response format for graph queries, by Michael U.](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965027)
- [Spike: unified query response, by Angelo](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965036)
- [Orbit response format: combined research](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965394)
- [ADR 003: Orbit API design](003_api_design.md)
- [ADR 012: GOON format](012_goon_format.md)
