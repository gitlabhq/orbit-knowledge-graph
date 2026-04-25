---
name: orbit-query
description: Query the GitLab Knowledge Graph via the Orbit REST API. All counting, grouping, filtering, and multi-hop logic should be pushed into the query DSL — never fetch raw data and post-process in Python.
---

# Orbit Query Skill

The Knowledge Graph query DSL compiles to ClickHouse SQL. It handles joins, aggregation, grouping, sorting, and filtering server-side. Your job is to express the question as a single query (or a small chain of queries), not to fetch raw data and process it yourself.

**All commands run from `evals/`. Never `cd` elsewhere.**

## Step 1: Discover the schema

```bash
python tools/orbit_query.py query-schema
python tools/orbit_query.py schema --expand User,Project,MergeRequest
```

## Step 2: Run queries

Output is toon format by default (compact text). Do NOT use `--format raw`.

```bash
cat <<'EOF' | python tools/orbit_query.py query
{"query_type":"search","node":{"id":"u","entity":"User","filters":{"username":"root"}},"limit":10}
EOF
```

## Query types

### Search — find entities by property filters

```json
{"query_type":"search","node":{"id":"u","entity":"User","filters":{"username":"root"}},"limit":10}
```

### Traversal — multi-hop joins in a single query

Chain up to 5 nodes with relationships. All filtering happens server-side on each node.

2-node:
```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "a", "entity": "EntityA", "filters": {"prop": "value"}},
    {"id": "b", "entity": "EntityB"}
  ],
  "relationships": [{"type": "EDGE_NAME", "from": "a", "to": "b"}],
  "limit": 100
}
```

3-node with mixed filters (`filters` for properties, `node_ids` for primary key):
```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "a", "entity": "EntityA", "filters": {"prop": "value"}},
    {"id": "b", "entity": "EntityB", "filters": {"state": "opened"}},
    {"id": "c", "entity": "EntityC", "node_ids": [12345]}
  ],
  "relationships": [
    {"type": "EDGE_AB", "from": "a", "to": "b"},
    {"type": "EDGE_BC", "from": "b", "to": "c"}
  ],
  "limit": 100
}
```

4-node chain:
```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "a", "entity": "EntityA"},
    {"id": "b", "entity": "EntityB"},
    {"id": "c", "entity": "EntityC"},
    {"id": "d", "entity": "EntityD", "node_ids": [12345]}
  ],
  "relationships": [
    {"type": "EDGE_AB", "from": "a", "to": "b"},
    {"type": "EDGE_BC", "from": "b", "to": "c"},
    {"type": "EDGE_CD", "from": "c", "to": "d"}
  ],
  "limit": 50
}
```

### Aggregation — counting, grouping, sorting server-side

For any question involving counts, top-N, or group-by, use aggregation. The server does the work.

Basic group-by count:
```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "a", "entity": "EntityA"},
    {"id": "b", "entity": "EntityB"}
  ],
  "relationships": [{"type": "EDGE_NAME", "from": "a", "to": "b"}],
  "aggregations": [{"function": "count", "target": "a", "group_by": "b", "alias": "total"}],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 20
}
```

3-node aggregation with filter (count A per C, where B has a specific state):
```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "a", "entity": "EntityA"},
    {"id": "b", "entity": "EntityB", "filters": {"status": "failed"}},
    {"id": "c", "entity": "EntityC", "node_ids": [12345]}
  ],
  "relationships": [
    {"type": "EDGE_AB", "from": "a", "to": "b"},
    {"type": "EDGE_BC", "from": "b", "to": "c"}
  ],
  "aggregations": [{"function": "count", "target": "b", "group_by": "a", "alias": "total"}],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 20
}
```

Key: `target` is what gets counted, `group_by` is what defines the groups. Both reference node IDs from the `nodes` array.

### Neighbors — fan-out to discover all connections

```json
{
  "query_type": "neighbors",
  "node": {"id": "x", "entity": "EntityX", "filters": {"iid": 100}},
  "neighbors": {"node": "x", "direction": "both"},
  "limit": 50
}
```

### Path Finding

```json
{
  "query_type": "path_finding",
  "nodes": [
    {"id": "a", "entity": "EntityA", "filters": {"prop": "value1"}},
    {"id": "b", "entity": "EntityB", "filters": {"prop": "value2"}}
  ],
  "path": {"type": "shortest", "from": "a", "to": "b", "max_depth": 3},
  "limit": 5
}
```

## Key rules

- Entity names: PascalCase (`User`, `MergeRequest`, `Pipeline`, `Finding`, `SecurityScan`)
- Edge names: UPPER_SNAKE_CASE (`AUTHORED`, `IN_PROJECT`, `HAS_HEAD_PIPELINE`, `SCANS`, `HAS_FINDING`)
- Relationship selectors use `type` (not `edge`): `{"type": "AUTHORED", "from": "u", "to": "mr"}`
- `node_ids`: integer array, filters by primary entity ID
- `filters`: key-value pairs for properties (username, state, iid, status)
- **Use aggregation queries for any counting or grouping. Never fetch raw rows and count in Python.**
- **Use multi-hop traversals for chains. Never query each hop separately and join in Python.**
- **Use the default toon output. Do NOT use `--format raw`.**
- If a query returns empty, verify edge direction in the schema
- If a query returns 400, read the error and fix the query
