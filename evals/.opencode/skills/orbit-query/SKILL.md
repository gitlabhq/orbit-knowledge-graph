---
name: orbit-query
description: Query the GitLab Knowledge Graph via the Orbit REST API. Covers search, traversal, aggregation, neighbors, and path-finding queries using tools/orbit_query.py.
---

# Orbit Query Skill

The Orbit Knowledge Graph exposes a JSON query DSL via `POST /api/v4/orbit/query`. You interact with it through `tools/orbit_query.py`.

## Commands

```bash
# Get schema (do this first to discover entities, fields, and edges)
python tools/orbit_query.py schema --expand User,Project,MergeRequest

# Execute a query (JSON on stdin)
echo '<json>' | python tools/orbit_query.py query

# Execute from file
python tools/orbit_query.py query --file query.json
```

## Query Format

All queries are JSON piped to stdin. The top-level fields vary by query type.

### Search (find entities by filters)

```json
{"query_type":"search","node":{"id":"u","entity":"User","filters":{"username":"alice"}},"limit":10}
```

### Traversal (multi-hop joins)

```json
{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":"alice"}},{"id":"mr","entity":"MergeRequest"}],"relationships":[{"from":"u","to":"mr","edge":"AUTHORED"}],"limit":100}
```

2-hop (User -> MR -> Project):

```json
{"query_type":"traversal","nodes":[{"id":"u","entity":"User","filters":{"username":"alice"}},{"id":"mr","entity":"MergeRequest"},{"id":"p","entity":"Project"}],"relationships":[{"from":"u","to":"mr","edge":"AUTHORED"},{"from":"mr","to":"p","edge":"IN_PROJECT"}],"limit":100}
```

### Aggregation

```json
{"query_type":"aggregation","nodes":[{"id":"mr","entity":"MergeRequest"},{"id":"p","entity":"Project"}],"relationships":[{"from":"mr","to":"p","edge":"IN_PROJECT"}],"group_by":["p.name"],"aggregations":[{"function":"count","alias":"mr_count"}],"limit":20}
```

### Neighbors (all connected entities)

```json
{"query_type":"neighbors","node":{"id":"mr","entity":"MergeRequest","filters":{"iid":123}},"direction":"both","limit":50}
```

### Path Finding

```json
{"query_type":"path_finding","source":{"id":"a","entity":"User","filters":{"username":"alice"}},"target":{"id":"b","entity":"User","filters":{"username":"bob"}},"max_depth":3,"limit":5}
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
- The edge connecting MRs to Projects is `IN_PROJECT` (not `BELONGS_TO`)
- Filter values: use strings for usernames, integers for IDs
- Use `schema --expand <Entity>` to discover available filter fields and edges
- If a query returns 400, read the error message -- it tells you what's wrong
