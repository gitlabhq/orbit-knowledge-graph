---
title: Scenarios
description: How YAML query and indexer scenarios prove that Orbit returns and writes the right data.
order: 2
authors:
  - michaelusa
  - jgdoyon1
  - michaelangeloio
---

## Two kinds of scenario

Scenarios are the main proof of correctness, and both kinds run against a real ClickHouse:

| Scenario | Input | Runs | Checks |
| -------- | ----- | ---- | ------ |
| Query | Seed rows, the caller permissions, and a query | The full query path, from compilation to the formatted response | The response that a caller gets |
| Indexer | Data lake rows and the handlers to run | The real indexer handlers | The nodes, edges, and dispatched messages in the graph |

Each folder holds the scenarios of one behavior, such as `security/` or `merge_requests/`, and a filter can run one folder alone.

## Query scenarios

A query scenario sets the caller, gives the query, and states the result, as in this trimmed example:

```yaml
description: MR search scoped to 1/101/ returns only MRs in that namespace

config:
  security: { org_id: 1, paths: ["1/101/"] }

query:
  gql: |
    MATCH (mr:MergeRequest) WHERE mr.id >= 1 AND mr.id <= 10000 RETURN mr.title LIMIT 10

expect:
  node_count: 1
  nodes:
    MergeRequest:
      ids: [2002]
      absent: [2000, 2001]
```

`config.security` sets the paths that the caller can read, as the token from GitLab would.
Only one merge request is inside that path, so the other two must be absent.
A scenario can also set the redaction answers, add seed rows, load an ontology overlay, or run several steps in order.

The runner compiles each query the way the server does and runs it on ClickHouse.
Then it applies the redaction answers, hydrates and formats the response, and compares it with `expect`.
A scenario that writes gets its own clone of the database.
The runner collects every failure before it reports, so one run shows all the broken scenarios.

| Check | Use |
| ----- | --- |
| Nodes, edges, and groups | The data that the caller sees |
| Absent IDs and missing edges | The data that the caller must not see |
| Pages and repeated runs | Paging returns each row once, and the same query returns the same order |
| Generated SQL and index use | The compiler adds a filter, avoids a costly shape, and ClickHouse skips data outside the filter |
| Compile errors | The compiler rejects an invalid query |

**A scenario must check each query feature that it uses.**

If a scenario sorts or pages but does not check the order or the pages, the runner fails it.
A scenario cannot pass without proving anything.

ClickHouse can hold more than one version of a row until a merge runs.
A scenario can load each row as its own unmerged part.
The query must then remove the old versions itself and return only the latest one.

## Indexer scenarios

An indexer scenario seeds the data lake tables that Siphon fills in production, runs the handlers, and reads the graph:

```yaml
description: Groups produce OWNER edges and subgroup CONTAINS edges
scope: { namespace: 100 }

seed:
  namespaces:
    - { id: 100, traversal_path: "1/100/" }
    - { id: 101, parent_id: 100, traversal_path: "1/100/101/" }

expect:
  edges:
    - kind: OWNER
      from: User
      to: Group
      count: 2
      rows:
        - { target_id: 100, traversal_path: "1/100/" }
        - { target_id: 101, traversal_path: "1/100/101/" }
    - { kind: CONTAINS, from: Group, to: Group, traversal_path: "1/100/101/", count: 1 }
```

The graph must hold exactly these edges, with the right traversal path on each, so these tests also protect [row ownership](../../architecture/security/authorization.md#traversal-paths).
A scenario can also send change events from Siphon, to test incremental indexing.
It can run the scheduler to check which namespaces get work, and repeat seed, run, and check in steps.

Each scenario gets its own database, and the checks read merged rows, so a duplicate row from a retry does not change the result.
The handlers use an in-memory message broker, so a scenario needs only ClickHouse.
A schema check validates every scenario file in CI, so a typo in a key fails before the tests run.

## The query corpus and executable docs

The query corpus runs a curated set of real queries and every named query.
It also runs every marked query example in the user docs, the engineering pages, and the Orbit skill.
**A schema change that breaks a published example fails CI.**

The corpus also fails on a query in an unmarked code block, and on a code block that does not close.
