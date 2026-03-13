# Integration Test Debugging Runbook

When a data_correctness integration test fails, follow this playbook **in order** before changing any test code.

## Step 1: Reproduce and capture the error

```bash
DOCKER_HOST="unix://$HOME/.colima/gkg/docker.sock" \
  cargo nextest run --all-features --test '*' -E 'test(data_correctness)' --retries 0
```

Record the exact panic message. Classify it:

| Panic pattern | Category |
|---|---|
| `node IDs mismatch` / `nodes in wrong order` / `expected N nodes, got M` | **Data shape** — the response has different nodes than expected |
| `unsatisfied assertion requirements` | **Enforcement** — test didn't call the required assertion methods |
| `Validation(...)` or `compile(...) unwrap on Err` | **Query rejected** — the query JSON is invalid per the query engine |
| `expected edge` / `should not exist` | **Edge mismatch** — edges present/absent unexpectedly |
| `did not satisfy predicate` | **Property mismatch** — node property has wrong value |

## Step 2: Generate the SQL (before touching any test code)

Use the `orbit query` CLI to compile the failing query and inspect the generated SQL. This determines whether the bug is in the **query engine** or in the **test assertions**.

```bash
cargo run --bin orbit -- query \
  --json '{"label": <PASTE_QUERY_JSON>}' \
  -t '1/100/'
```

For neighbors/path_finding queries that don't require `limit`, omit it. For queries requiring security context paths covering all seeded groups, use multiple `-t` flags:

```bash
-t '1/100/' -t '1/101/' -t '1/102/'
```

Read the generated SQL. Check:
- Which columns are in the SELECT? (seed node columns? neighbor columns? aggregation aliases?)
- What JOINs exist? (INNER JOIN gl_edge? subqueries?)
- What WHERE filters? (traversal_path, node_ids, filters)
- What ORDER BY / LIMIT / OFFSET?

## Step 3: Check the formatter (for neighbors, path_finding)

For dynamic query types (neighbors, path_finding), the graph formatter adds extra behavior beyond the SQL:

- **Neighbors** (`graph.rs:extract_neighbors`): adds BOTH the seed/center node AND each neighbor to the response nodes array. The seed appears exactly once. Edges connect seed to each neighbor.
- **Path finding** (`graph.rs:extract_path_finding`): adds all nodes along discovered paths. Empty paths = empty response.

Key reference locations:
- `crates/gkg-server/src/query_pipeline/formatters/graph.rs` — `extract_neighbors`, `extract_path_finding`
- `crates/query-engine/src/enforce.rs:129-181` — `enforce_return` registers which nodes appear in SELECT
- `crates/query-engine/src/lower.rs:413-526` — `lower_neighbors` SQL generation

Existing golden tests that confirm expected behavior:
- `crates/integration-tests/tests/server/graph_formatter.rs` — `neighbors_outgoing_exact`, `neighbors_incoming_exact`, `neighbors_both_exact` all assert the seed node IS present in response nodes.

## Step 4: Check the validator (for query rejected errors)

If the query was rejected at compile time:

```bash
# Check what the validator enforces
grep -n "check_" crates/query-engine/src/validate.rs
```

Common validation rules:
- `range` and `limit` may have mutual exclusion rules
- `order_by` must reference a declared node
- `aggregation_sort` requires `aggregations`
- `neighbors` requires `node_ids` on the seed node
- `path_finding` requires `path` config with `from`/`to`/`max_depth`

Read the validator error message carefully — it tells you which rule was violated.

## Step 5: Check the enforcement system (for unsatisfied requirements)

If the error is `unsatisfied assertion requirements`, the test used `ResponseView::for_query()` which derives requirements from the query AST. Each requirement must be satisfied by a corresponding assertion call.

| Requirement | Satisfied by |
|---|---|
| `OrderBy` | `assert_node_order` |
| `Filter { field }` | `assert_filter(entity, field, predicate)` |
| `NodeIds` | `node_ids()`, `assert_node_ids`, `assert_node_order` |
| `PathFinding` | `path_ids()` |
| `Aggregation` | `assert_node(entity, id, predicate)` checking an aggregate property |
| `Relationship { edge_type }` | `edges_of_type(type)`, `assert_edge_exists`, `assert_edge_set` |
| `Neighbors` | `assert_edge_exists`, `edges_of_type`, `assert_edge_set`, `assert_edge_count` |
| `AggregationSort` | `assert_node_order` |
| `Range` | `assert_node_count` |
| `Limit` | `assert_node_count` |

## Step 6: Verify seed data expectations

Before asserting specific counts or IDs, re-derive the expected result from the seed data topology. The seed data is documented at the top of `data_correctness.rs`.

Entity-specific notes:
- `gl_user` has **no `traversal_path` column**. User queries have no traversal_path WHERE clause — the security context doesn't filter users by path. All seeded users are always visible (before redaction).
- `gl_group`, `gl_project`, `gl_merge_request`, `gl_note` all have `traversal_path`. Queries against these tables include `startsWith(traversal_path, ...)` filters from the security context.
- Multi-hop traversals flatten edges: a 2-hop path `A→B→C` produces edge `(A, C)` in the response, not `(B, C)`. The `source_id`/`target_id` reflect the start and end of the hop chain.

Checklist:
- Count nodes per entity type that match the query filters
- For traversals: trace the JOIN path through edges, collect unique nodes
- For aggregation: compute the aggregate value per group manually
- For neighbors: include the seed node + all connected nodes via the specified edge types/direction
- For path_finding: trace all paths up to max_depth, verify each hop exists in gl_edge
- For redaction: remove unauthorized entity IDs, then recount

## Step 7: Decision tree

```
Is the SQL wrong? (Step 2)
  YES → bug in query-engine (lower.rs, enforce.rs, codegen.rs)
  NO  →
    Is the formatter wrong? (Step 3)
      YES → bug in graph formatter (graph.rs)
      NO  →
        Is the query rejected? (Step 4)
          YES → fix the query JSON in the test, or file a validator bug
          NO  →
            Is enforcement unsatisfied? (Step 5)
              YES → add the missing assertion call to the test
              NO  →
                Are the expected values wrong? (Step 6)
                  YES → fix the test assertions
                  NO  → actual data pipeline bug — investigate deeper
```

## Quick reference: orbit query examples

```bash
# Search with filter
cargo run --bin orbit -- query --json '{
  "search_active_users": {
    "query_type": "search",
    "node": {"id": "u", "entity": "User", "columns": ["username"],
             "filters": {"state": "active"}},
    "limit": 10
  }
}' -t '1/100/'

# Neighbors outgoing
cargo run --bin orbit -- query --json '{
  "user_neighbors": {
    "query_type": "neighbors",
    "node": {"id": "u", "entity": "User", "node_ids": [1]},
    "neighbors": {"node": "u", "direction": "outgoing"}
  }
}' -t '1/100/' -t '1/101/' -t '1/102/'

# Aggregation with sort
cargo run --bin orbit -- query --json '{
  "member_counts": {
    "query_type": "aggregation",
    "nodes": [{"id": "g", "entity": "Group"}, {"id": "u", "entity": "User"}],
    "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
    "aggregations": [{"function": "count", "target": "u", "group_by": "g", "alias": "cnt"}],
    "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
    "limit": 10
  }
}' -t '1/100/' -t '1/101/' -t '1/102/'

# Path finding
cargo run --bin orbit -- query --json '{
  "user_to_project": {
    "query_type": "path_finding",
    "nodes": [
      {"id": "s", "entity": "User", "node_ids": [1]},
      {"id": "e", "entity": "Project", "node_ids": [1000]}
    ],
    "path": {"type": "shortest", "from": "s", "to": "e", "max_depth": 3}
  }
}' -t '1/100/'
```
