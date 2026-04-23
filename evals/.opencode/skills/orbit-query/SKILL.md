---
name: orbit-query
description: Query the GitLab Knowledge Graph via the Orbit REST API. Covers search, traversal, aggregation, neighbors, and path-finding queries using tools/orbit_query.py.
---

# Orbit Query Skill

The Orbit Knowledge Graph exposes a JSON query DSL via `POST /api/v4/orbit/query`. You interact with it through `python tools/orbit_query.py`.

## Step 1: Discover the schema (do this first)

```bash
# Get the query DSL schema (query format, operators, types)
python tools/orbit_query.py query-schema

# Get the ontology (entities, edges, properties) -- expand specific entities
python tools/orbit_query.py schema --expand User,Project,MergeRequest

# Get full ontology (all entities, no expansion)
python tools/orbit_query.py schema
```

## Step 2: Build and run queries

```bash
# Execute a query (JSON on stdin)
echo '<json>' | python tools/orbit_query.py query

# Execute from file
python tools/orbit_query.py query --file query.json

# Get LLM-friendly output format
echo '<json>' | python tools/orbit_query.py query --format llm
```

## Quick reference (examples)

These are examples. Always verify field names and edges against `query-schema` and `schema`.

### Search (find entities by filters)

```json
{"query_type":"search","node":{"id":"u","entity":"User","filters":{"username":"alice"}},"limit":10}
```

### Traversal (multi-hop joins)

Requires `nodes` (2+), `relationships` (1+). Use `type` for edge name, `from`/`to` for node IDs.

```json
{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":"alice"}},{"id":"mr","entity":"MergeRequest"}],"relationships":[{"type":"AUTHORED","from":"u","to":"mr"}],"limit":100}
```

2-hop (User -> MR -> Project):

```json
{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":"alice"}},{"id":"mr","entity":"MergeRequest"},{"id":"p","entity":"Project"}],"relationships":[{"type":"AUTHORED","from":"u","to":"mr"},{"type":"IN_PROJECT","from":"mr","to":"p"}],"limit":100}
```

### Aggregation

Requires `nodes` (1+), `aggregations` (1+). Each aggregation needs `function`, optional `target`, `group_by`, `property`, `alias`.

```json
{"query_type":"aggregation","nodes":[{"id":"mr","entity":"MergeRequest"},{"id":"p","entity":"Project"}],"relationships":[{"type":"IN_PROJECT","from":"mr","to":"p"}],"aggregations":[{"function":"count","target":"mr","group_by":"p","alias":"mr_count"}],"aggregation_sort":{"agg_index":0,"direction":"DESC"},"limit":20}
```

### Neighbors (all connected entities)

Use this to find everything connected to a node. Requires `node` (singular) and `neighbors` config.

```json
{"query_type":"neighbors","node":{"id":"mr","entity":"MergeRequest","filters":{"iid":100},"columns":"*"},"neighbors":{"node":"mr","direction":"both"},"limit":50}
```

### Path Finding

Requires `nodes` (2+) and `path` config with `type`, `from`, `to`, `max_depth`.

```json
{"query_type":"path_finding","nodes":[{"id":"a","entity":"User","filters":{"username":"alice"}},{"id":"b","entity":"User","filters":{"username":"bob"}}],"path":{"type":"shortest","from":"a","to":"b","max_depth":3},"limit":5}
```

## Response Format

Responses are wrapped in `result`:

```json
{
  "result": {
    "query_type": "search",
    "nodes": [{"type":"User","id":"1","username":"alice","name":"Alice","state":"active"}],
    "edges": []
  }
}
```

## Key Rules

- Entity names are PascalCase: `User`, `Project`, `MergeRequest`, `Issue`, `Pipeline`, `Group`
- Edge names are UPPER_SNAKE_CASE: `AUTHORED`, `IN_PROJECT`, `ASSIGNED`, `APPROVED`, `REVIEWED`
- Relationship selectors use `type` (not `edge`): `{"type":"AUTHORED","from":"u","to":"mr"}`
- Filter values: use strings for usernames, integers for IDs
- Always run `python tools/orbit_query.py query-schema` first to get the live DSL spec
- If a query returns 400, read the error message -- it tells you what's wrong
- Use `neighbors` query type (not manual traversals) when finding all connections to a node
