# Orbit Query Skill

## Overview

The Orbit Knowledge Graph exposes a JSON query DSL via `POST /api/v4/orbit/query`. You interact with it through `tools/orbit_query.py`.

## Commands

```bash
# Execute a query (JSON on stdin)
echo '<json>' | python tools/orbit_query.py query

# Execute from file
python tools/orbit_query.py query --file query.json

# Get schema (discover entities and relationships)
python tools/orbit_query.py schema

# Get schema with expanded properties for specific nodes
python tools/orbit_query.py schema --expand User,Project,MergeRequest

# Check cluster health
python tools/orbit_query.py status
```

## Query Types

### 1. Search (find entities by filters)

```json
{
  "query_type": "search",
  "node": {
    "id": "u",
    "entity": "User",
    "filters": { "username": "alice" }
  },
  "limit": 10
}
```

### 2. Neighbors (find connected entities)

```json
{
  "query_type": "neighbors",
  "node": {
    "id": "mr",
    "entity": "MergeRequest",
    "filters": { "iid": 123, "project_id": 278964 }
  },
  "direction": "both",
  "limit": 50
}
```

Direction can be `"outgoing"`, `"incoming"`, or `"both"`.

### 3. Traversal (multi-hop joins)

```json
{
  "query_type": "traversal",
  "nodes": [
    { "id": "u", "entity": "User", "filters": { "username": "alice" } },
    { "id": "mr", "entity": "MergeRequest" }
  ],
  "relationships": [
    { "from": "u", "to": "mr", "edge": "AUTHORED" }
  ],
  "limit": 100
}
```

2-hop example (User -> MergeRequest -> Project):

```json
{
  "query_type": "traversal",
  "nodes": [
    { "id": "u", "entity": "User", "filters": { "username": "alice" } },
    { "id": "mr", "entity": "MergeRequest" },
    { "id": "p", "entity": "Project" }
  ],
  "relationships": [
    { "from": "u", "to": "mr", "edge": "AUTHORED" },
    { "from": "mr", "to": "p", "edge": "BELONGS_TO" }
  ],
  "limit": 100
}
```

### 4. Aggregation

```json
{
  "query_type": "aggregation",
  "nodes": [
    { "id": "mr", "entity": "MergeRequest" },
    { "id": "p", "entity": "Project" }
  ],
  "relationships": [
    { "from": "mr", "to": "p", "edge": "BELONGS_TO" }
  ],
  "group_by": ["p.name"],
  "aggregations": [
    { "function": "count", "alias": "mr_count" }
  ],
  "limit": 20
}
```

### 5. Path Finding

```json
{
  "query_type": "path_finding",
  "source": { "id": "a", "entity": "User", "filters": { "username": "alice" } },
  "target": { "id": "b", "entity": "User", "filters": { "username": "bob" } },
  "max_depth": 3,
  "limit": 5
}
```

## Response Format

All queries return:

```json
{
  "query_type": "search",
  "nodes": [
    { "type": "User", "id": "1", "username": "alice", "name": "Alice", "state": "active" }
  ],
  "edges": [],
  "row_count": 1
}
```

Traversal/path queries also populate `edges` with `from`, `from_id`, `to`, `to_id`, `type` fields.

## Tips

- Use `schema --expand <Entity>` to discover available filter fields
- Entity names are PascalCase: `User`, `Project`, `MergeRequest`, `Issue`, `Pipeline`, `Group`
- Edge names are UPPER_SNAKE_CASE: `AUTHORED`, `BELONGS_TO`, `ASSIGNED_TO`, `REVIEWED`
- Start with a simple search, then build up to traversals
- The `format=llm` option returns a compact text format that uses fewer tokens
