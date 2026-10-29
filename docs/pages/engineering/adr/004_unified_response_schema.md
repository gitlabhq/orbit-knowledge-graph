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

Orbit returned query results as flat JSON rows, and the only sign of the graph shape was in the column names.
A prefix on a column name marked a node alias, an edge, or a neighbor.
Each query type returned a different shape, and internal columns leaked into the output.

So the frontend had a whole module that only rebuilt nodes and edges from the flat rows.
That module depended on fixed assumptions:

- Alias detection from column name suffixes
- Fixed column names for neighbor IDs, neighbor types, and relationship types
- A label priority list across common properties
- Dispatch logic for each query type
- Column name pattern matching to extract entities

The response also had no metadata, so the frontend scanned the data to learn the query type, column types, and row count.
The ontology, the schema of the graph, defines a label field and a primary key for each entity.
Query results did not refer to them, so the frontend kept its own copy of that mapping.

We compared the response formats of Neo4j, Kuzu, ArangoDB, TigerGraph, Dgraph, AGE, Memgraph, and SurrealDB:

- Neo4j returned a graph section next to the row section, with every property in both, and then deprecated it in Query API v2.
  Neo4j drivers and Neo4j Browser now find the graph shape on the client.
- Every database we checked returns structured inline objects and lets the client extract graph entities.
- TigerGraph keeps identity apart from data, with an ID, a type, and an attributes object.
- ArangoDB returns a path as a list of vertices and a list of edges, close to our path finding output.
- All of them use the same structure for aggregated and plain queries, and only the cell content changes.

We evaluated three designs, and after discussion the team chose a graph-native shape.
The other two are under Why not the alternatives.

## Decision

**Every query returns the same graph shape: a list of nodes, each listed once, and a list of edges.**

For a table view, the frontend groups the nodes by type and shows one table per type.
For a graph view, the edges go straight to the visualization.

The data comes in two layers.
The first layer is the ontology, with the entity types, properties, label fields, domains, styles, and edge definitions.
The frontend fetches it once from the schema API ([ADR 003](003_api_design.md)) and caches it.
Labels, styles, and descriptions come from the ontology instead of the response.

The second layer is the response to each query.
The gRPC envelope carries the query type, the row count, the format name, and the format version.
It also carries the generated ClickHouse SQL for debugging, which production returns only with a debug flag.
The payload is structured JSON, or compact text for large language models (LLMs) as [ADR 012](012_goon_format.md) describes.

The JSON payload holds the format version, the query type, the nodes, the edges, and pagination when the client requested a cursor.
Aggregation queries also hold column descriptions and rows.
For example, a one-hop traversal from a user to the projects they belong to returns this:

```plaintext
nodes:  User 1 (sidney), Project 101 (Alpha), Project 102 (Beta)
edges:  User 1 MEMBER_OF Project 101
        User 1 MEMBER_OF Project 102
```

A node is a flat object with its type, its ID, and its properties.
Property names match the ontology, so the frontend reads data types from the cached schema.
The type and the ID together identify a node, so the frontend uses them to merge duplicates.
An edge names its source type and ID, its target type and ID, and its relationship type.

Each query type fills the same shape:

- A lookup is a traversal with one node and no relationships, formerly the separate search query type, and it returns nodes with no edges.
- A traversal returns each node once and each edge between two nodes.
- A variable-length traversal adds a depth to each edge.
- Path finding adds a path ID and a step to each edge, and it deduplicates nodes across paths.
- Neighbors returns the center node as one more node in the list, next to neighbors of mixed types, and edge direction follows the ontology.
- Aggregation keeps computed values in rows, never on graph nodes.

An aggregation column description names the function, the node it counts, and an optional property.
A group column description names the group key, which is a node or a property.
A row for a node group nests the grouped entity under the group key, with the metric values next to it.
A row for a property group holds the group values and the metric values.
An aggregation with no group key returns one row of metric values and no nodes.

Node and edge IDs are always JSON strings, because code graph IDs are hashes that are often too large for a JavaScript number.
Inside Orbit, entity primary keys stay integers.
Query input accepts IDs as integers or strings, so clients can send IDs back as they got them.
Aggregation metric values stay JSON numbers.

The shape follows these rules:

- Each entity appears once in the node list.
- Each edge connects two specific nodes by type and ID.
- Every node carries its ID and type, even when the query did not select them.
- No internal columns appear in the output.
- No redaction information appears in the output, because authorization runs on the server.
- Pagination uses keyset cursors, and Orbit compiles the cursor into the SQL query.
- The response always says whether more results exist and whether Orbit cut the result short, with an optional next cursor.
- The JSON has no row count, because the envelope already carries the authorized row count of the page.

The frontend picks a default view from the query type, and the user can always switch:

| Query type | Default view |
|------------|--------------|
| Aggregation, single-node traversal | Table |
| Multi-node traversal, path finding, neighbors | Graph |

A graph formatter in Orbit replaces the raw row formatter as the default.
A JSON Schema is the shared contract between Orbit and the frontend.
The frontend drops its rebuild module and passes nodes and edges straight to the Three.js renderer.

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
CI also requires a higher version for any change to the formatter or the response schema.
Each formatter reports its own name and version, so the gRPC service does not need fixed values.
A placeholder formatter that hands off to another formatter reports an empty version, so telemetry can tell its output apart.
The LLM text format gets its own version when its encoding ships ([ADR 012](012_goon_format.md)).

## Benefits of this approach

- The frontend no longer rebuilds the graph from column names.
- All query types use one parser, with no special case for each type.
- A user that appears in 50 rows becomes one node object, with no repeated data.
- The JSON Schema prevents drift between the server and the frontend.
- Graph data goes to the renderer with no extraction step.
- Tables stack by entity type, with columns from the ontology.

## Consequences

- The JSON result is a breaking change.
  Rails passes the JSON through without reading it, so Rails needs no change, but any client that parses the JSON must handle the new shape.
- A single-entity traversal response is slightly larger because of the envelope.
- A new query type needs a new extractor in the graph formatter.

## Why not the alternatives

### A graph section next to tabular rows

The response would carry the tabular rows and a separate graph section.
Neo4j used this design and copied every property into both sections, with no reference between them.
Neo4j removed it in Query API v2, and we rejected it for the same reason.

### Tabular rows with column descriptors

Each row would map node aliases to node objects, with column and edge descriptions.
Path finding and neighbors did not fit, because a path has no fixed length and neighbors have different entity types in each result.
Both needed special handling for each query type.
A user in 50 traversal rows still had its properties repeated 50 times.

## References

- [Establish Orbit Response Format](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/243)
- [Unified response schema](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/479)
- [Align the gRPC contract for the API design](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/411), with the note on protobuf sync decisions
- [Spike: unified response format for graph queries, by Michael U.](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965027)
- [Spike: unified query response, by Angelo](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965036)
- [Orbit response format: combined research](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5965394)
- [ADR 003: Orbit API design (unified REST and GraphQL)](003_api_design.md)
- [ADR 012: GOON format](012_goon_format.md)
