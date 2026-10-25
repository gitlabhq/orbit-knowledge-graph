---
title: Query languages
description: The JSON Query DSL and GQL, and how both compile through one engine.
authors:
  - michaelusa
---

## What the query languages are

Orbit has two query languages. The JSON Query DSL is a JSON object that
follows a published schema, and it is the default. GQL is read-only text based
on openCypher 9. Both describe the same queries and return the same results.

Here is the same question in both languages: merged merge requests and the
users who wrote them.

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

```gql orbit-query
MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {state: 'merged'})
RETURN u.username, mr.title
LIMIT 10
```

## How it works

```mermaid
flowchart LR
    JSON[JSON query] --> JF[JSON frontend]
    GQL[GQL text] --> GF[GQL frontend]
    JF --> Input[Shared input]
    GF --> Input
    Input --> Engine[Graph query engine]
```

Each language has its own frontend. A frontend parses the query and turns it
into one shared input. From there on, the engine treats every query the same
way. The steps below follow a query through its frontend.

### 1. Rails picks the language

```mermaid
flowchart LR
    Flag[Per-user flag] --> Rails
    Rails -- language --> Webserver
    Webserver --> Frontend
```

A default-off feature flag in Rails picks the language for each user. With the
flag off, every surface accepts only JSON. With the flag on, the query
endpoint, named queries, and discovery all use GQL, and JSON queries fail.
Rails sets the language on each request. Callers can't choose it, and the
server never guesses it from the query.

### 2. The frontend parses the query

```mermaid
flowchart LR
    Object[JSON object] --> Schema[JSON schema check] --> Input[Shared input]
    Text[GQL text] --> Grammar --> Tree[Syntax tree] --> Lower --> Input
```

A JSON query is checked against its JSON schema, then read into the shared
input. A GQL query goes through a grammar adapted from the official openCypher
9 grammar. The frontend builds a typed syntax tree, then lowers it into the
shared input. Lowering checks the whole statement: the pattern must be one
connected tree, each variable must be unique, and the query type must be
clear.

A frontend never drops part of a query and runs the rest. Anything it can't
support fails with an error that names the problem.

### 3. Shared validation

```mermaid
flowchart LR
    Input[Shared input] --> Validator
    Ontology --> Validator
    Validator --> Checked[Checked input]
```

Both languages end in the same validator. It checks the query's limits, then
checks every entity, relationship, and property against the ontology. A JSON
query is checked twice, by its schema and by this validator, so every JSON
test also tests the shared validator.

### 4. Schema discovery

```mermaid
flowchart LR
    Caller -- discovery call --> Webserver
    Ontology --> Webserver
    Webserver -- compact schema --> Caller
```

Callers read the graph schema before they write a query. In JSON mode, they
call `get_graph_schema`. In GQL mode, they run `CALL db.schema()`. Both
answers come from the ontology, sized for an LLM context window, and never
show the ClickHouse tables underneath. A schema call runs no SQL.

### 5. Named queries

```mermaid
flowchart LR
    Name[Name and parameters] --> Server
    Server -- active language --> Frontend
    Frontend --> Engine
```

Some clients, like the Orbit dashboard, call queries by name instead of
writing them. Each named query is stored in both languages. The server picks
the spelling for the caller's active language and compiles it with that
language's frontend. Named queries follow the same rules as any other query.

## When things go wrong

- **The query is in the wrong language.** It's rejected. There is no fallback
  to the other language.
- **GQL uses syntax that Orbit doesn't support,** like `OPTIONAL MATCH` or
  `OR`. The error names the unsupported part.
- **A name doesn't match the ontology.** The error lists the valid names.
- **The query goes over a limit.** The error names the limit.
- **A named query no longer fits a new schema version.** The server leaves it
  out of that version and logs a warning.

## Why we built it this way

JSON came first because LLMs fill in a JSON schema more reliably than they
write free-form text. The schema also documents every field and limit in one
file that tools can read. GQL came next because agents already know Cypher,
and a short pattern costs fewer tokens than the same query in JSON.

Both languages lower into one shared input, so each capability exists once. A
new language is one new frontend. Retiring the JSON Query DSL means deleting
its frontend and its schema, and nothing after the frontend changes.

## Decisions

- [ADR 011: Agent command surface](../adr/011_agent_command_surface.md)
- [ADR 019: GQL response format](../adr/019_gql_response_format.md)

## Appendix

### Language principles

- Both languages lower to one input.
  A capability that exists in the input works in every language that can express it.
- The same query returns the same result in both languages.
  Tests check both spellings against one expectation.
- Query values never become SQL text.
  The compiler binds every value as a parameter, and every name must match the ontology.
- A frontend rejects what it does not support.
  It never drops part of a query and runs the rest.
- Errors are safe to show the caller.
  They name the problem without echoing the query text.
- Callers discover the graph from the ontology, never from the ClickHouse schema.
- Rails picks the language for each user.
  The server never guesses the language from the payload.

### Terms

- A **frontend** is the part of the compiler that parses one query language.
- The **input** is the language-neutral form of a query: node selectors, relationship selectors, filters, aggregations, ordering, and limits.
- A **node selector** names an entity type and a variable, with optional IDs, filters, and returned properties.
- A **relationship selector** connects two node selectors, with a relationship type, a direction, and a hop range.
- A **pattern** is the GQL form of node and relationship selectors, such as `(u:User)-[:AUTHORED]->(mr)`.

### The languages side by side

| | JSON Query DSL | GQL |
| --- | --- | --- |
| Form | A JSON object | One text statement |
| Built for | LLMs that fill in a JSON schema, and services that build queries in code | Agents and people who know Cypher |
| Query type | Stated in `query_type` | Inferred from the statement |
| Checks | JSON schema, then the shared validator | Grammar, then the shared validator |
| Schema discovery | `get_graph_schema` | `CALL db.schema()` |
| Frontend code | `passes/frontend/json_dsl/` | `passes/frontend/gql/` |

Both frontends live under `crates/query-engine/compiler/src/passes/frontend/`.

### Query types

Every query is one of four types.
JSON names the type, and GQL infers it from the statement.

| Query type | Answers | GQL form |
| --- | --- | --- |
| Traversal | Which nodes match this pattern? A single node with no relationships is a lookup. | Any other supported pattern |
| Aggregation | How many, how much, grouped by what? | An aggregate in `RETURN` |
| Path finding | What is the shortest path between these nodes? | A named `ANY SHORTEST` pattern |
| Neighbors | What is connected to this node? | One relationship to an unlabeled, unfiltered endpoint |

The [query language reference](../../../docs-gitlab-com/remote/queries/query-language.md) documents every field, operator, and limit.

### JSON Query DSL details

JSON is the default language.
It is designed for LLMs and services:

- LLMs produce valid JSON against a schema more reliably than free-form query text.
- One machine-readable file, `config/schemas/graph_query.schema.json`, documents every field, type, and limit.
- Every value has its own field, so values never mix with query structure.
- Services can build queries in code without a parser.

A query names its type and its nodes.
The other fields depend on the type:

| Part | Purpose |
| --- | --- |
| `query_type` | One of `traversal`, `aggregation`, `path_finding`, or `neighbors` |
| `nodes` | The node selectors |
| `relationships` | The relationship selectors that connect the nodes |
| `aggregations` and `group_by` | The metrics and group keys of an aggregation |
| `path` and `neighbors` | The settings for path finding and neighbors |
| `limit` and ordering | The size and order of the result |
| `options` | Presentation settings that do not change what the query matches |


The DSL has a version: the `query_dsl` pin in `config/versions.yaml`.
Its major version is part of the schema `$id`.
`get_query_dsl` returns the current schema.

### GQL details

GQL is the text language.
It is designed for agents that know Cypher:

- A short pattern costs fewer tokens than the same query in JSON.
- A pattern has the same shape as the graph it matches.
- One statement form covers queries and schema discovery.

GQL in Orbit is a restricted subset of openCypher 9 with a few extensions.
It reads and never writes.
It accepts one statement per request and has no query parameters, so callers keep untrusted values out of the query text.
It is not a Neo4j-compatible driver.
The grammar adapts productions from the official openCypher 9 M23 EBNF grammar, and the grammar module carries the Apache-2.0 license and the upstream attribution.

A statement has this shape:

```plaintext
MATCH pattern [WHERE predicates]
[MATCH pattern [WHERE predicates] ...]
RETURN projections
[ORDER BY key [ASC | DESC]]
[SKIP rows]
[LIMIT rows]
[DEBUG]
```


The pattern must form one connected tree.
Several `MATCH` clauses join into one pattern, and their `WHERE` predicates combine with `AND`.
`RETURN` selects what the graph response includes.
It is not a general table of expressions.

The GQL frontend works in two steps:

1. Parsing checks syntax: identifiers, strings, numbers, and the order of clauses.
1. Lowering checks the whole statement: unique variables, a connected pattern, the query type, and the projections.

Orbit adds these extensions to openCypher 9:

- Shortest paths use the ISO GQL prefix `ANY SHORTEST`, because openCypher 9 has no shortest-path syntax.
- `date_trunc` buckets dates in aggregations.
- Token predicates search text-indexed properties.
- `OFFSET` is a synonym for `SKIP`.
- `DEBUG` returns the compiled SQL to authorized users.

The frontend rejects writes, `OPTIONAL MATCH`, `WITH`, `UNION`, `OR`, cycles, and other syntax outside this shape.

### Validation

Both languages end in the same shared validator.
It checks the limits, then checks every entity, relationship, and property against the ontology.
The limits are Rust constants, and a build script checks that the JSON schema states the same limits.

A JSON query is checked twice: first by the JSON schema, then by the shared validator.
Every JSON test therefore also tests the shared validator.

### Adding or retiring a language

Every language-specific step lives in its frontend, so each language is one module:

- A new language adds a frontend that lowers to the shared input, and a compiler pipeline that starts with its parse phase.
- Retiring the JSON Query DSL deletes its frontend, its parse phase, and its schema file.
  The shared phases do not change.
- A check that reads only the input works for every language.
  For example, the relationship-direction check runs on GQL today, and the JSON pipelines can add it without frontend changes.

### Testing

Query scenarios state each query once in each language, and both spellings must return the same result.
A grammar fuzz target generates statements from the GQL grammar, and other fuzz targets send arbitrary input through both frontends.
[Testing](../testing/README.md) describes these layers.
