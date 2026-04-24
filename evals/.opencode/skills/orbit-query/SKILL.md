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

## Query patterns

### Search

```json
{"query_type":"search","node":{"id":"u","entity":"User","filters":{"username":"root"}},"limit":10}
```

### Traversal (multi-hop joins)

All filtering happens in the query — do NOT search entities separately then join in Python.

2-node (User → MR):
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

3-node with filters on each (User → open MRs → specific Project):
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

4-node chain (Finding → SecurityScan → Pipeline → MergeRequest):
```json
{
  "query_type": "traversal",
  "nodes": [
    {"id": "f", "entity": "Finding"},
    {"id": "s", "entity": "SecurityScan"},
    {"id": "pl", "entity": "Pipeline"},
    {"id": "mr", "entity": "MergeRequest"}
  ],
  "relationships": [
    {"type": "HAS_FINDING", "from": "s", "to": "f"},
    {"type": "SCANS", "from": "s", "to": "pl"},
    {"type": "HAS_HEAD_PIPELINE", "from": "mr", "to": "pl"}
  ],
  "limit": 50
}
```

### Aggregation

Aggregation runs server-side. Push ALL counting, grouping, and sorting into the query. Never fetch raw rows and count in Python.

Count MRs per project (sorted):
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

Count MRs per label in a project:
```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "mr", "entity": "MergeRequest"},
    {"id": "l", "entity": "Label"},
    {"id": "p", "entity": "Project", "node_ids": [278964]}
  ],
  "relationships": [
    {"type": "HAS_LABEL", "from": "mr", "to": "l"},
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "aggregations": [{"function": "count", "target": "mr", "group_by": "l", "alias": "mr_count"}],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 10
}
```

Count failed pipelines per user in a project (3-hop aggregation):
```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "u", "entity": "User"},
    {"id": "mr", "entity": "MergeRequest"},
    {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
    {"id": "p", "entity": "Project", "node_ids": [278964]}
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"},
    {"type": "HAS_HEAD_PIPELINE", "from": "mr", "to": "pl"},
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "aggregations": [{"function": "count", "target": "pl", "group_by": "u", "alias": "failed_count"}],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 20
}
```

Count open review assignments per user:
```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "u", "entity": "User"},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}},
    {"id": "p", "entity": "Project", "node_ids": [278964]}
  ],
  "relationships": [
    {"type": "REVIEWER", "from": "u", "to": "mr"},
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "review_count"}],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 20
}
```

### Neighbors

Use this to discover all connections to a node. Do NOT manually query each edge type.

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
