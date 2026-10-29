---
title: "GKG ADR 016: Column statistics for cost-based join ordering"
creation-date: "2026-06-05"
authors: [ "@michaelusa" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-06-05

## Context

The query compiler picks a join order from a fixed strategy for each query type and from coarse selectivity labels in the ontology.
Each label marks a filter as high or low selectivity.
Flat chains start from the edge table, and foreign key (FK) star queries start from the center node.

The labels hide large differences.
A string filter on a column with tens of thousands of distinct values gets the same label as one on a column with three.

Profiling showed the cost of this.
When the compiler replaced filter-only common table expressions (CTEs) with joins, rows read on code graph queries dropped by 70%.
That gain was luck, because the new shape happened to suit that query.
For a different filter combination, the old shape might have been better.
Without cardinality data, which counts rows and distinct values, the compiler cannot make this choice on purpose.

The compiler already supports several join strategies:

| Strategy | Entry point | When used |
|---|---|---|
| FK star | Center node | All hops use an FK to the same center |
| Flat chain | Edge table | Default for edge-based queries |
| Single node | Node table | No edges |

The gap is strategy selection.
With row counts and value distributions per column and namespace, the compiler can pick the entry point and join order with the smallest intermediate results.

## Decision

**The compiler orders joins by cost, with column statistics that ClickHouse maintains on insert.**

The compiler estimates the rows that each valid join order reads and picks the cheapest one.

### Query types affected

Join ordering applies to every query that joins two or more tables, so only bare single-node searches are out of scope.

| Query type | Tables joined | Current join strategy |
|---|---|---|
| Traversal, single hop | One edge and one or two nodes | Edge first, then join the nodes |
| Traversal, multiple hops | N edges and N+1 nodes | Edge first, then join each hop in sequence |
| Aggregation | Same as traversal, with grouping | Same as traversal |
| FK star | Center node and N target nodes, no edges | Center first through FK columns |
| Neighbors | One center node and one edge scan | Center first |
| Path finding | Two anchor nodes and recursive edge CTEs | Anchor CTEs, then edge expansion |

The planner is the plan pass of the compiler.
For a single-hop traversal, it starts from the edge table or from the more selective node.
For multiple hops, the direction of each hop matters.
For an FK star, the planner joins the smallest target first to narrow the result early.
For neighbors, the planner scans the center or the edges, based on center filters beyond the pinned IDs.
Path finding keeps its recursive structure.
Its anchor selection is still a join ordering choice: which endpoint expands first, and how to split forward and backward depth.

### Statistics

We keep three types of statistics, each in its own ClickHouse table that merges aggregate states:

| Statistic | Covers | Key |
|---|---|---|
| Value frequency | Booleans, enums, and strings with few distinct values | Table, column, traversal path, and value |
| Token frequency | Strings with more distinct values | Table, column, traversal path, and token |
| Equi-depth histogram | Integers, timestamps, dates, and floats | Table, column, traversal path, and bucket |

The ontology property types decide the category of each column at load time, so nobody lists columns by hand.
The statistics skip UUID columns, because their selectivity is always one row.
They also skip virtual columns and columns that cannot be filtered.

One materialized view per node table fires on insert and writes aggregate states to the statistics tables.
The view counts distinct entities per value or bucket with a HyperLogLog sketch, which gives an approximate distinct count.
The sketch ignores duplicate values, so the old row versions that node tables keep after an update do not inflate counts.

Deleted rows still contribute to the statistics.
We accept this error, because exact counts need a periodic batch job with a deduplicating scan.
For join ordering, the error is negligible.

Statistics are partitioned by traversal path, the namespace path stored on each row, so each namespace has its own statistics.
Entities without a traversal path, such as users and runners, use an empty partition key for global statistics.
The total row count of an entity comes from the histogram of its ID column.

A ClickHouse dictionary, an in-memory lookup table, sits over the statistics tables.
It refreshes every few minutes and answers lookups in less than a millisecond.

### Cardinality estimation

The planner estimates the rows that remain after filters:

```plaintext
estimated rows = total rows * product(selectivity of each filter)
```

Each filter type gets its selectivity from one statistic:

| Filter | Statistic | Selectivity |
|---|---|---|
| Equality | Value frequency | Count of the value divided by total rows |
| Set membership | Value frequency | Sum of the matching value counts divided by total rows |
| Range comparison | Histogram | Sum of the buckets in range divided by total rows |
| Substring, prefix, or suffix match | Token frequency | Count of the token divided by total rows, with a fixed default |
| Null check | Value frequency | Count of null values divided by total rows, with a fixed default |
| No filter | None | One |
| Pinned IDs | None | Number of pinned IDs, capped at total rows, divided by total rows |
| ID range | Histogram | Sum of the buckets in range divided by total rows |

The planner multiplies the selectivity values of filters on the same node.
This independence assumption is standard in PostgreSQL, MySQL, and other query planners.
Correlated filters, such as a city of Paris and a country of France, give overestimates.
Most filter columns in the Orbit ontology are independent.

The planner estimates join output with the FK join formula:

```plaintext
join output = left rows * right rows / max(distinct left keys, distinct right keys)
```

For an edge-to-node join, the key is the node ID on one side and the source or target ID on the edge.
The histogram table gives the number of distinct key values.

In this example query, the user is pinned and the merge requests have a branch filter:

```plaintext
Query: a traversal from a user with pinned node ID 1, through AUTHORED to merge requests on the main source branch, through IN_PROJECT to projects.

Statistics for one namespace:
  Merge requests: 82,000 rows, 4,200 distinct source branches
  AUTHORED edges from users: about 500,000 rows
  Projects: 200 rows

Ordering 1: User, edges, merge requests, edge, project
  1 user, about 6 edges, about 6 merge requests, 1 project lookup
  Total work: about 13 rows

Ordering 2: Merge requests, edges, user, edge, project
  About 20 merge requests, about 20 edges, 1 user, 1 project lookup
  Total work: about 42 rows

The planner picks ordering 1.
```

### Join order selection

The planner lists the join orderings that the declared ontology relationships allow.
It estimates the intermediate row count at each step and picks the ordering with the lowest estimated total.

The query language caps the number of nodes and relationships, so the search space is small.
Brute-force evaluation takes less than a microsecond, and no dynamic programming is necessary.
The compiler must be able to start a flat chain from either a node or an edge.

### Pipeline placement

The Orbit query pipeline runs these stages in order: security, compilation, the ClickHouse query, extraction, authorization, redaction, hydration, and output.
The stats fetch runs after security and before compilation.
At that point, Orbit has these inputs:

- The traversal paths that the user can access
- The parsed query, with its node entities and filters
- The ontology, which loads at startup

The compiler needs the statistics during its plan pass, before it emits SQL.
The fetch is one dictionary lookup, or a small batch, for the node tables of the query in the traversal paths of the user.
An in-process cache keeps results for a few minutes, keyed by table, column, and traversal path.
Most requests get their statistics from the cache with no ClickHouse round trip.
Orbit passes the statistics to the compiler as a map from table and column to the statistics of that column.

A statistics section in the schema configuration declares the table names and dictionary settings.
The same section sets the histogram bucket count, the top token count, the partition key, and the excluded columns.

## Benefits of this approach

Queries with several filtered nodes get the cheapest estimated join order instead of a fixed strategy.
The planner can skip narrowing CTEs when statistics show that a filter does not narrow much.
Measured numbers replace the high and low selectivity labels.
Code graph queries over files, definitions, or imported symbols benefit most.
Their tables are large, and their filter selectivity varies widely.

## Consequences

- Schema migrations must create and drop the statistics tables, views, and dictionary with the graph tables.
- Each node table gets one materialized view, and each view fires on every insert, which adds write amplification.
- Statistics for deleted entities stay slightly inflated until the HyperLogLog sketches are rebuilt.

## Why not the alternatives

### Table part metadata only

ClickHouse part metadata gives approximate global row counts with no new infrastructure.
It has no namespace scope, no distinct counts per column, and no value distributions.
Join ordering depends on filter selectivity as well as table size, so this is too coarse.

### Periodic batch job

A batch job could use deduplicating scans for exact statistics.
It adds a scheduler, failure handling, and staleness windows.
Materialized views are simpler and maintain themselves.

### Inline subquery at query time

Statistics computed before each query are too slow.
An exact distinct count over a deduplicated node table is itself a full table scan, which is the cost we want to avoid.

### One statistic type with distinct counts and row counts

This covers equality and set membership filters.
It cannot estimate range or text filter selectivity.
Three types cover all query language filters with little additional complexity.

## References

- [Cost-based join ordering using ClickHouse column statistics](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/826)
- [Add statistics schema configuration for cost-based query planning](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1555)
- [Replace filter-only CTE and IN with INNER JOIN for edge chains](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1533)
- [ClickHouse AggregatingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/aggregatingmergetree)
