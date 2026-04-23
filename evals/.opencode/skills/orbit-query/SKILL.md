---
name: orbit-query
description: Query the GitLab Knowledge Graph via the Orbit REST API. Covers search, traversal, aggregation, neighbors, and path-finding queries using tools/orbit_query.py.
---

# Orbit Query Skill

The Orbit Knowledge Graph exposes a JSON query DSL via `POST /api/v4/orbit/query`. You interact with it through `python tools/orbit_query.py`.

**Important:** All commands run from the `evals/` directory. Do NOT `cd` to the repo root.

## Step 1: Discover the schema (do this first)

```bash
python tools/orbit_query.py query-schema
python tools/orbit_query.py schema --expand User,Project,MergeRequest
```

## Step 2: Build and run queries

```bash
# Pipe JSON on stdin
cat <<'EOF' | python tools/orbit_query.py query
{"query_type":"search","node":{"id":"u","entity":"User","filters":{"username":"root"}},"limit":10}
EOF

# Use --format llm for large results (compact, readable output)
cat <<'EOF' | python tools/orbit_query.py query --format llm
{"query_type":"neighbors","node":{"id":"mr","entity":"MergeRequest","filters":{"iid":100}},"neighbors":{"node":"mr","direction":"both"},"limit":50}
EOF
```

## Query patterns

### Search (find entities by filters)

```json
{"query_type":"search","node":{"id":"u","entity":"User","filters":{"username":"root"}},"limit":10}
```

### Traversal (multi-hop joins)

Requires `nodes` (2+), `relationships` (1+). Use `type` for the edge name.

User's MRs:
```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User", "filters": {"username": "root"}},
    {"id": "mr", "entity": "MergeRequest"}
  ],
  "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
  "limit": 100
}
```

User's open MRs in a specific project (3-node traversal with filters on multiple nodes):
```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "u", "entity": "User", "filters": {"username": "root"}},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}},
    {"id": "p", "entity": "Project", "node_ids": [278964]}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"},
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "limit": 100
}
```

Note: `node_ids` filters by the entity's primary ID (integer). Use `filters` for other properties like `username`, `state`, `iid`.

### Aggregation

```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "mr", "entity": "MergeRequest"},
    {"id": "p", "entity": "Project"}
  ],
  "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
  "aggregations": [{"function": "count", "target": "mr", "group_by": "p", "alias": "mr_count"}],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 20
}
```

### Neighbors (all connected entities)

Use this to find everything connected to a node. Do NOT manually traverse each edge type.

```json
{
  "query_type": "neighbors",
  "node": {"id": "mr", "entity": "MergeRequest", "filters": {"iid": 100}},
  "neighbors": {"node": "mr", "direction": "both"},
  "limit": 50
}
```

### Path Finding

```json
{
  "query_type": "path_finding",
  "nodes": [
    {"id": "a", "entity": "User", "filters": {"username": "alice"}},
    {"id": "b", "entity": "User", "filters": {"username": "bob"}}
  ],
  "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 3},
  "limit": 5
}
```

## Response format

```json
{
  "result": {
    "query_type": "search",
    "nodes": [{"type": "User", "id": "1", "username": "root"}],
    "edges": [{"type": "AUTHORED", "from_id": "1", "from_type": "User", "to_id": "99", "to_type": "MergeRequest"}]
  }
}
```

## Key rules

- Entity names are PascalCase: `User`, `Project`, `MergeRequest`, `Issue`, `Pipeline`, `Group`
- Edge names are UPPER_SNAKE_CASE: `AUTHORED`, `IN_PROJECT`, `ASSIGNED`, `APPROVED`, `REVIEWED`
- Relationship selectors use `type` (not `edge`): `{"type": "AUTHORED", "from": "u", "to": "mr"}`
- Filter values: strings for usernames/state, integers for IDs and iids
- `node_ids` takes an array of integers to filter by primary entity ID
- `filters` takes key-value pairs for property filters (username, state, iid, etc.)
- Use `--format llm` for large results instead of piping raw JSON through Python
- If a query returns empty results, verify the edge direction is correct by checking the schema
- If a query returns 400, read the error message and fix the query
- Use `neighbors` query type when finding all connections to a node
- The working directory is `evals/` — run `python tools/orbit_query.py`, not from the repo root
