# Query corpus

A categorized set of Orbit GQL queries used to exercise the query engine.
Each YAML file groups queries by origin suite; every entry is self-describing:

```yaml
mrs_open_in_project:
  desc: Open merge requests in a project
  category: traversal        # query shape: traversal | aggregation | time_series |
                             #   path_finding | neighbors | search | code_graph
  suite: sdlc                # origin grouping (one file per suite)
  expect: rows               # optional: rows (default) | empty | error
  query: |
    MATCH (mr:MergeRequest {state: 'opened'})-[:IN_PROJECT]->(p:Project)
    WHERE p.id IN $sample
    RETURN *
    LIMIT 25
```

`category` is the query *shape* and is independent of which file the entry lives
in. Two placeholder kinds keep the queries portable across environments:

| Placeholder | Meaning |
|---|---|
| `x.id IN $sample` / `x.id IN $sample:N` | 1 or N ids of node `x`, bound to real ids at run time |
| `{{TOKEN}}` | a literal ID or `full_path`, substituted from a caller-supplied map |

`expect` records intent:

| `expect` | meaning |
|---|---|
| `rows` *(default)* | returns at least one row against a data-rich namespace |
| `empty` | no usable rows at this scope: genuinely 0 rows (e.g. an edge type with no data in scope), or a heavy path-finding / deep multi-hop shape that is capped by resource limits |
| `error` | a deliberately invalid query that must fail to compile/run (negative control) |

## Validation

The `corpus_smoke` test (`crates/integration-tests/tests/server/corpus_smoke.rs`)
runs in the `corpus-smoke-test` CI job. It starts ClickHouse with the graph schema
from the ontology. Then it compiles and runs three families of queries:

- every entry in this directory, as GQL. An `expect: error` entry must fail.
- every named query in `config/named_queries`, in JSON and in GQL.
- every `opencypher` and `json orbit-query` code block in the tracked Markdown files,
  every `language-opencypher` HTML code element, and every query in an `orbit query` line
  of a shell code block. The scan skips `docs-locale` and `CHANGELOG.md`.

The test also fails on `gql` or `cypher` code, on an `orbit query` line that it cannot read,
on Orbit query JSON in an unmarked `json` or shell code block, and on a code block with no
closing fence. It does not check result correctness.

Run it locally with Docker: `mise test:integration:corpus`. See
[Query corpus and executable docs](../../../docs/pages/engineering/testing/integration/scenarios.md#the-query-corpus-and-executable-docs).

## `raw_sql_ab.yaml`

`raw_sql_ab.yaml` is a raw-SQL A/B suite (control vs. optimized variants of the
same workload), not GQL. The smoke test skips it.
