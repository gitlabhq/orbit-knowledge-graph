# JSON Query DSL

## Overview

The JSON Query DSL is the default query language of Orbit Remote. It is built for LLMs that fill in a JSON schema.

1. An agent reads the graph schema and the DSL schema.
2. The agent fills in one JSON object that describes the query.
3. Orbit checks the object against the JSON schema, then against the ontology.
4. The shared compiler turns it into parameterized SQL.

One schema covers traversal, aggregation, path finding, and neighbors. Agents use one tool for every graph question, not one tool for each question type.

## Why JSON

- LLMs produce valid JSON against a schema more reliably than free-form query text.
- A JSON schema documents every field, type, and limit in one machine-readable file: `config/schemas/graph_query.schema.json`.
- Every value has a field, so values never mix with query structure. Nothing in the object can become SQL text.
- UI clients and services can build queries in code without a parser.

[GQL](orbit_query_frontend.md) is the text alternative. Both languages compile to the same input and return the same results.

## Query shape

A query names its type and the nodes it matches. The other fields depend on the type.

| Part | Purpose |
| --- | --- |
| `query_type` | One of `traversal`, `aggregation`, `path_finding`, or `neighbors` |
| `nodes` | The node selectors: an entity type, a variable name, and optional IDs, filters, and columns |
| `relationships` | The relationship selectors that connect the nodes, with a direction and a hop range |
| `aggregations` and `group_by` | The metrics and group keys of an aggregation |
| `path` and `neighbors` | The settings for path finding and neighbors |
| `limit` and ordering | The size and order of the result |
| `options` | Presentation settings that do not change what the query matches |

For example, this query returns the merged merge requests of each user:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User"},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"}
  ],
  "limit": 25,
  "order_by": "-mr.merged_at"
}
```

The [query language reference](../../source/remote/queries/query-language.md) documents every field, operator, and limit, with examples of each query type.

## Validation

Orbit checks a JSON query twice:

1. The JSON schema checks the structure, types, and limits.
2. The shared validator checks the same limits, then checks every entity, relationship, and property against the ontology.

The second check is the same one that GQL uses. Every JSON test therefore also tests the shared validator. A build script checks that the limits in the JSON schema match the limits in Rust.

## Schema discovery

Agents read the graph schema before they write a query. `get_graph_schema` returns the ontology as compact text, sized for an LLM context window. By default it lists node types by domain and relationship types with their endpoints. An agent expands the node types it needs to see their properties and relationships. The response never exposes the ClickHouse schema.

`get_query_dsl` returns the DSL schema. Its version is the `query_dsl` pin in `config/versions.yaml`, and its major version is part of the schema `$id`.

## Future of the DSL

GQL may replace the JSON Query DSL. The JSON frontend is one module with its own parse phase. Retiring it deletes that module and the schema file, and the shared phases do not change.
