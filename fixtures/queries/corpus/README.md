# Query corpus

A categorized set of GKG query-DSL queries used to exercise the query engine.
Each YAML file groups queries by origin suite; every entry is self-describing:

```yaml
mrs_open_in_project:
  desc: Open merge requests in a project
  category: traversal        # query shape: traversal | aggregation | time_series |
                             #   path_finding | neighbors | search | code_graph | cursor
  suite: sdlc                # origin grouping (one file per suite)
  expect: rows               # optional: rows (default) | empty | error
  query: |
    { ...inline GKG DSL JSON... }
  opencypher: |
    MATCH ...openCypher twin of the query...
```

`opencypher` is the same query in the openCypher frontend's dialect. The
`compiler::corpus` test in `crates/integration-tests/tests/compiler/` asserts
it compiles to byte-identical SQL, so every entry needs one; an entry the
frontend cannot spell carries `opencypher_skip: <reason>` instead. `$sample`
and `$sample:N` appear in a twin as the parameters `$sample` and `$sample_N`,
bound to the same `node_ids` the JSON side resolves to.
An `id` equality or `in` filter is spelled `{id: v}` / `x.id IN [...]` in the
twin and compared against the query with that filter folded into `node_ids`,
the form the frontend lowers it to.

`category` is the query *shape* and is independent of which file the entry lives
in. Two placeholder kinds keep the queries portable across environments:

| Placeholder | Meaning |
|---|---|
| `"$sample"` / `"$sample:N"` | a node's `node_ids`, bound to real ids at run time |
| `{{TOKEN}}` | a literal id / full_path, substituted from a caller-supplied map |

`expect` records intent:

| `expect` | meaning |
|---|---|
| `rows` *(default)* | returns at least one row against a data-rich namespace |
| `empty` | no usable rows at this scope: genuinely 0 rows (e.g. an edge type with no data in scope), or a heavy path-finding / deep multi-hop shape that is capped by resource limits |
| `error` | a deliberately invalid query that must fail to compile/run (negative control) |

## Validation

The `corpus_smoke` integration test (`crates/integration-tests/tests/server/corpus_smoke.rs`,
run as its own CI job) spins up ClickHouse with the ontology-generated graph
schema and runs every query here through compile + execute, asserting each one
runs without error (`expect: error` entries must fail). It does not check result
correctness, only that the queries stay runnable against the current schema.

Run locally with Docker: `mise test:integration:corpus`.

## `raw_sql_ab.yaml`

`raw_sql_ab.yaml` is a raw-SQL A/B suite (control vs. optimized variants of the
same workload), not DSL. The smoke test skips it.
