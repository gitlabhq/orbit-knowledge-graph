---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use the GitLab Orbit query language to search and traverse the graph.
title: GitLab Orbit query language
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

Use the GitLab Orbit query language when you need GitLab data as a graph instead of a
flat API response. The language is a read-only subset of
[openCypher 9](https://opencypher.org/resources/). A query matches a graph
pattern of entities and relationships, then returns properties, counts, paths,
or neighbors.

```gql orbit-query
MATCH (u:User {username: 'root'})-[:AUTHORED]->(mr:MergeRequest {state: 'merged'})
RETURN u.username, mr.title
LIMIT 10
```

## Request envelope

When you submit a query with the REST API or `glab orbit query`, put the query
text in a top-level `query` field:

```json
{
  "query": "MATCH (mr:MergeRequest {id: 12345}) RETURN mr{.iid, .title, .state} LIMIT 1",
  "response_format": "llm"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `query` | Yes | The query text documented on this page. |
| `response_format` | No | `"llm"` (default; compact [TOON](https://github.com/toon-format/spec/blob/main/SPEC.md) text with one table per node type), `"raw"` (structured JSON), or `"gql"` (a graph pattern table). Use `"raw"` when piping output into `jq`. |

`glab orbit query` also accepts the query text as its argument, for example
`glab orbit query 'CALL db.schema()'`. Pass the envelope with `--file`.

## Discover the schema

Use `CALL db.schema()` to list every node and relationship type. To get the
properties and relationships of one node type, pass its name:

```plaintext
CALL db.schema()
CALL db.schema('MergeRequest')
```

Schema calls read ontology metadata only. They do not read graph data.

## Statement shape

A query is one statement:

```plaintext
MATCH pattern [WHERE predicates]
[MATCH pattern [WHERE predicates] ...]
RETURN projections
[ORDER BY key [ASC | DESC]]
[SKIP rows] [LIMIT rows]
[DEBUG]
```

- A pattern is a node, a chain of relationships, or comma-separated parts that
  form one connected tree. Disconnected parts and cycles are rejected.
- Give each node a label on its first use, for example `(mr:MergeRequest)`.
  Later parts can refer to the same variable, `(mr)`, without a label.
- Write relationship types after a colon: `-[:AUTHORED]->`. Without the colon,
  `-[AUTHORED]->` declares a variable and is rejected.
- Use `->` or `<-` between labeled nodes. An undirected relationship, `--`, is
  only for neighbor queries.
- Use bounded repeats for variable-length relationships: `*2` or `*1..3`.
  The maximum is three hops.
- Several `MATCH` clauses combine into one pattern, and their `WHERE`
  predicates combine with `AND`.
- `ORDER BY` accepts one sort key.
- Keywords are case insensitive. Names and values are case sensitive.

The shape of the pattern and the `RETURN` clause decide the kind of query:

| Pattern or projection | Query kind |
|-----------------------|------------|
| Named `ANY SHORTEST` pattern | Path finding |
| One relationship to an unlabeled, unfiltered endpoint | Neighbors |
| An aggregate function in `RETURN` | Aggregation |
| Any other pattern | Traversal |

## Selectivity

At least one node in a traversal or aggregation query must be selective. A node
is selective when it has an ID, a property filter, or an ID range with a span of
100,000 or less. `LIMIT` does not bound the scan, so
`MATCH (p:Project) RETURN p LIMIT 5` is rejected, and so is a node bounded only
by `NOT` over a list, text match, or group.

## Predicates

Write inline properties for equality, and `WHERE` for everything else:

```gql orbit-query
MATCH (mr:MergeRequest {state: 'merged'})
WHERE mr.created_at >= '2026-01-01' AND mr.created_at < '2026-02-01' AND mr.target_branch IN ['main', 'master']
RETURN mr.iid, mr.title
LIMIT 25
```

| Predicate | Use |
|-----------|-----|
| `=`, `<>`, `!=` | Equal or not equal to a value. |
| `<`, `<=`, `>`, `>=` | Numeric, date, or timestamp comparison. |
| `IN [...]` | Value is in a list. Maximum 100 values. |
| `CONTAINS 'x'` | String contains the value as a plain substring. |
| `STARTS WITH 'x'` | String starts with a prefix. |
| `ENDS WITH 'x'` | String ends with a suffix. |
| `IS NULL`, `IS NOT NULL` | Null check. |
| `token_match(n.p, 'word')` | Property contains the value as one whole word. |
| `all_tokens(n.p, 'words')` | Property contains every word in the value, in any order. |
| `any_tokens(n.p, 'words')` | Property contains at least one word in the value. |
| `type(r) = 'X'`, `type(r) IN [...]` | A one-hop relationship variable has one of the types. |

Combine predicates with `AND` and `NOT`, grouped with parentheses. Write
`NOT x.state IN [...]`, not `x.state NOT IN [...]`. `OR`, `XOR`, and arbitrary
expressions are not supported. A `type(r)` condition that can never match
rejects instead of returning zero rows. `date('2026-01-01')` and `datetime('...')` are accepted as
literals and compare like strings.

To identify nodes by ID, use an inline ID, an `IN` list, or a range:
`(u:User {id: 1})`, `WHERE u.id IN [1, 2]`, or
`WHERE u.id >= 1 AND u.id <= 100`. You can compare two properties of the same
or connected nodes: `WHERE mr.project_id = p.id`.

`CONTAINS`, `STARTS WITH`, `ENDS WITH`, and the token functions ignore ASCII
case: `CONTAINS 'Migration'` and `CONTAINS 'MIGRATION'` return the same rows.
The server (ClickHouse) folds ASCII letters only, so `STARTS WITH 'ÄRG'` does
not match `ärger`. Local queries (DuckDB) fold Unicode, so the same query does.
`=` and `IN` compare exact values, and so does every predicate on a sort-key
column such as `branch`, so the primary key keeps pruning.
The string predicates work only on string, enum, and UUID properties. Token
functions work only on text-indexed properties and reject other properties.

### Text-indexed properties

The following properties support `token_match`, `all_tokens`, and `any_tokens`.
Words are the runs of letters and digits in the property, so `main` matches
`src/main.rs` and `yml` matches `.gitlab-ci.yml`. On these properties
`CONTAINS`, `STARTS WITH`, and `ENDS WITH` prune with an ngram skip index and
the token functions prune with a text skip index. Use `CONTAINS` for a word
prefix (`migrat`) or a phrase with its words in order (`fix flaky`), and a token
function when word order does not matter.

<!-- The table below is generated from the ontology's `text` storage indexes. -->
<!-- Do not edit it by hand: run `mise run docs:query-language` and commit. CI fails on drift. -->
<!-- BEGIN GENERATED: text-indexed-properties -->

| Entity | Text-indexed properties |
|--------|------------------------|
| `Branch` | `name` |
| `ContainerRepository` | `name` |
| `Definition` | `file_path`, `fqn`, `name` |
| `Dependency` | `name` |
| `Deployment` | `ref` |
| `Directory` | `name`, `path` |
| `Environment` | `environment_type`, `name` |
| `File` | `name`, `path` |
| `Finding` | `description`, `name` |
| `Group` | `description`, `name` |
| `ImportedSymbol` | `file_path`, `import_path` |
| `Job` | `name`, `ref` |
| `Label` | `description`, `title` |
| `MergeRequest` | `description`, `source_branch`, `target_branch`, `title` |
| `MergeRequestDiff` | `head_commit_sha` |
| `MergeRequestDiffFile` | `new_path`, `old_path` |
| `Milestone` | `description`, `title` |
| `Note` | `note` |
| `Package` | `name` |
| `PackageFile` | `file_name` |
| `Pipeline` | `ref` |
| `Project` | `description`, `name` |
| `Runner` | `name` |
| `Stage` | `name` |
| `User` | `name`, `username` |
| `Vulnerability` | `description`, `title` |
| `VulnerabilityIdentifier` | `external_id`, `external_type`, `name` |
| `VulnerabilityOccurrence` | `description`, `name` |
| `VulnerabilityScanner` | `external_id`, `name` |
| `WorkItem` | `description`, `title` |

<!-- END GENERATED: text-indexed-properties -->

## Projections

`RETURN` controls which properties come back:

| Projection | Returns |
|------------|---------|
| `n` | The entity's default properties. |
| `n.title`, `n{.iid, .title}` | The named properties. Maximum 50 per node. |
| `properties(n)` | Every property you are allowed to read. |

GitLab Orbit always returns each node's ID and type, and the relationships that
connect the matched nodes. `AS` aliases are accepted on traversal projections,
and `ORDER BY` can name a property alias, but the alias does not rename the
output property.

## Columns and virtual columns

Most columns come from indexed graph tables in ClickHouse. Some columns are
virtual: GitLab Orbit fetches them from another service after the graph query returns.

Request virtual columns by name, for example `RETURN mr{.iid, .diff}`.
`properties(n)` on a path or neighbor result excludes virtual columns, because
they can require external service calls.

| Entity | Virtual column | What it returns |
|--------|----------------|-----------------|
| `MergeRequest` | `diff` | Full unified diff for the merge request. |
| `MergeRequestDiff` | `patch` | Full patch for one merge request diff snapshot. |
| `MergeRequestDiffFile` | `diff` | Per-file unified diff text. Returns `null` when `too_large` is `true`. |
| `File` | `content` | Raw source text of a file. |
| `Definition` | `content` | Source text for one indexed definition. |

The `content` column is for source code. For merge request diff text, use
`MergeRequest.diff`, `MergeRequestDiff.patch`, or `MergeRequestDiffFile.diff`.

### Text excerpts

Database-backed node strings are shortened to fit the page: the fewer rows a page can return, the longer each value may be, so a single-row lookup returns full text while a 1,000-row page returns short excerpts. Shortened values end with `" [truncated]"`; use the GitLab API for full text.

### Filtering on virtual columns

Virtual columns support `=`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`,
`IS NULL`, and `IS NOT NULL` in traversal queries, and only on nodes that pin
explicit IDs. The filter resolves the column
for those rows and compares in memory. Using a virtual column filter to
search rows you have not identified yet (content search) is not supported
and is rejected; find candidate rows with indexed filters first, then
filter the pinned rows by virtual column content.

You do not need to request the column to filter on it; a virtual column
that is filtered but not requested is resolved for the comparison and
omitted from the response.

Aggregation, neighbor, and path finding queries reject predicates on virtual
columns.

## Traversal examples

Fetch one merge request with its full diff:

```gql orbit-query
MATCH (mr:MergeRequest {id: 12345})
RETURN mr{.iid, .title, .state, .diff}
LIMIT 1
```

Fetch per-file diff content from diff snapshots:

```gql orbit-query
MATCH (mr:MergeRequest {id: 12345})-[:HAS_DIFF]->(snapshot:MergeRequestDiff)-[:HAS_FILE]->(file:MergeRequestDiffFile)
RETURN mr{.iid, .title, .state}, snapshot{.id, .state, .patch}, file{.new_path, .old_path, .too_large, .diff}
LIMIT 20
```

`HAS_DIFF` returns every diff snapshot the merge request ever had
(`MergeRequestDiff.merge_request_id` FK). `HAS_LATEST_DIFF` returns
only the most recent snapshot (`MergeRequest.latest_merge_request_diff_id`
FK) — useful for "what does the merge request look like right now", but
not for historical questions. For "every merge request that ever touched
a file", traverse `HAS_DIFF` over all snapshots. Using `HAS_LATEST_DIFF`
for historical-coverage questions can substantially undercount on
long-lived files: an MR that touched the file in an earlier revision
but not in its final diff is invisible through `HAS_LATEST_DIFF`.

`MergeRequestDiffFile.old_path` is the preferred column for file
lookups; `new_path` differs from `old_path` only on renames. Filtering
and grouping by `old_path` keeps the same row identity across an MR's
history. See the ontology field descriptions on
[`merge_request_diff_file.yaml`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/config/ontology/nodes/code_review/merge_request_diff_file.yaml).

Fetch source file content:

```gql orbit-query
MATCH (file:File)
WHERE file.path ENDS WITH 'app/models/project.rb'
RETURN file{.path, .language, .content}
LIMIT 5
```

Fetch the source text of a specific function or class definition. The `content`
column returns the raw source text of just that definition, not the full file.
Use `fqn` (fully-qualified name) for an exact match, or `name` with `contains`
for a broader search:

```gql orbit-query
MATCH (d:Definition)
WHERE d.fqn = 'Gitlab::Auth::authenticate'
RETURN d{.name, .fqn, .file_path, .start_line, .end_line, .content}
LIMIT 5
```

Find merged merge requests in a project:

```gql orbit-query
MATCH (project:Project {full_path: 'your-group/your-project'})<-[:IN_PROJECT]-(mr:MergeRequest {state: 'merged'})
RETURN project{.name, .full_path}, mr{.iid, .title, .state, .merged_at}
LIMIT 25
```

Find every pipeline that ran for one merge request. Always filter on
`source = 'merge_request_event'` to match what the merge request's
**Pipelines** tab shows:

```gql orbit-query
MATCH (p:Pipeline)
WHERE p.merge_request_id = 482908721 AND p.source = 'merge_request_event'
RETURN p{.id, .status, .source, .sha, .ref, .created_at}
ORDER BY p.created_at DESC
LIMIT 100
```

`merge_request_id` is the merge request's internal numeric `id`, not the
project-scoped `iid`. Look it up first with a `MergeRequest` query that
filters by `iid` and `project_id`, then use the `id` in the query above.

Both `Pipeline.merge_request_id` and the `(:MergeRequest)-[:TRIGGERED]->(:Pipeline)`
relationship link an MR to every CI pipeline spawned in its context,
including the downstream child pipelines (`source = "parent_pipeline"`)
that the top-level MR pipelines trigger. Without the
`source = 'merge_request_event'` predicate, the result over-counts by a large
factor on any MR that uses parent-child pipeline fan-out, and does not
match what the MR **Pipelines** tab shows. Apply the same filter when traversing
`(:MergeRequest)-[:TRIGGERED]->(:Pipeline)` in a multi-node query.

`(:MergeRequest)-[:HAS_HEAD_PIPELINE]->(:Pipeline)` is a different relationship. It
points to the single most recent pipeline running against the tip of the
merge request's source branch. Use it for "what is currently running", not
for pipeline history.

## Aggregation

An aggregate function in `RETURN` makes the query an aggregation. Every other
item in `RETURN` becomes a group key.

| Function | Argument | Supported property types |
|----------|----------|--------------------------|
| `count` | A node (count matching rows) or `n.property` (count non-null values) | Any |
| `sum` | `n.property` | Numeric only |
| `avg` | `n.property` | Numeric only |
| `min` | `n.property` | Numeric, boolean, enum, `Date`, or `DateTime` |
| `max` | `n.property` | Numeric, boolean, enum, `Date`, or `DateTime` |

Name each aggregate with `AS`, and sort by that name. Without `AS`, the output
column name is derived as `<function>_<node>` (`count_mr`) or
`<function>_<node>_<property>` (`avg_mr_merge_duration`).

| Group key | Example | Result value |
|-----------|---------|--------------|
| Node | `p` or `p{.full_path}` | A nested entity object in each row. |
| Property | `mr.state` | A scalar bucket value in each row. |
| Truncated date | `date_trunc('month', mr.created_at)` | The property truncated to the start of the unit. |

In aggregations, text pattern and ordering filters need their node grouped.

Property group columns are named `<node>_<property>` (`mr_state`), and truncated
keys append the unit (`mr_created_at_month`). Rename a group key with `AS`.
Duplicate output names are rejected.

Truncation units are `minute`, `hour`, `day`, `week`, `month`, `quarter`, and
`year`, and apply only to `Date` and `DateTime` properties. `minute` and `hour`
require IDs or a predicate on the truncated property to bound the number of
buckets. `sum` and `avg` reject `DateTime` properties. To aggregate over dates,
use `min` or `max`.

Property group keys must be ClickHouse-backed, filterable properties that you
are allowed to use. Virtual and unfilterable properties are rejected.

Count merged merge requests per project:

```gql orbit-query
MATCH (project:Project {full_path: 'your-group/your-project'})<-[:IN_PROJECT]-(mr:MergeRequest {state: 'merged'})
RETURN project, count(mr) AS merged_mrs
ORDER BY merged_mrs DESC
LIMIT 10
```

Count detected vulnerabilities by severity:

```gql orbit-query
MATCH (v:Vulnerability {state: 'detected'})
RETURN v.severity, count(v) AS vulnerability_count
ORDER BY vulnerability_count DESC
LIMIT 10
```

Aggregation responses are table-shaped. `columns` describes computed aggregate
values, `group_columns` describes grouping keys, and `rows` carries group values
plus metric values. Node-grouped rows store the grouped entity under the group
key. Property-grouped rows store the scalar bucket under the group key.

`count(*)`, `DISTINCT`, and `collect()` are not supported.

## Path finding

Name a shortest-path pattern and return the path variable:

```gql orbit-query
MATCH p = ANY SHORTEST (start:User {id: 1})-[:CREATOR|AUTHORED|IN_PROJECT*1..3]->(`end`:Project {id: 278964})
RETURN p
LIMIT 5
```

Both endpoints must be labeled and bounded by an ID, a property predicate, or an
ID range with a span of 500 or less. The path must be outgoing, start at one
hop, and go at most three hops. Path finding follows each relationship type only
in its defined direction, so choose endpoints and relationship types that form a
forward chain. `RETURN properties(p)` includes every readable ClickHouse-backed
property of the nodes on each path.

`end` is a reserved word, so the example escapes it with backticks.

## Neighbors

A relationship to an unlabeled endpoint with no predicates returns the
neighbors of a bounded center node:

```gql orbit-query
MATCH (mr:MergeRequest {id: 12345})-[:AUTHORED|IN_PROJECT|HAS_DIFF]-(neighbor)
RETURN mr, neighbor
LIMIT 25
```

Use `-->` for relationships that start at the center node, `<--` for
relationships that point at it, and `--` for both. Leave out the relationship
type to follow every type. The response lists the center node alongside its
neighbors, so leave the center out when counting neighbors.

`RETURN properties(neighbor)` includes every readable ClickHouse-backed property
of each neighbor. Virtual columns still require a traversal query that names
them.

## Pagination

Use `LIMIT` for up to 1,000 rows. The default is 30. Check `pagination.truncated`
in the response: when it is `true`, more matching rows exist.

Page with `SKIP` and `LIMIT`, as in openCypher. `OFFSET` is a synonym for
`SKIP`, and `SKIP` comes before `LIMIT`. Start at `SKIP 0` and add the page size
to `SKIP` for each next page, keeping the rest of the query the same, until
`pagination.has_more` is `false`:

```plaintext
MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest {state: 'merged'}) WHERE u.id = 1
RETURN mr.iid, mr.title ORDER BY mr.merged_at DESC SKIP 50 LIMIT 50
```

`SKIP` accepts up to 10,000. For deeper result sets, order by a unique key such
as `id` and filter past the last value from the previous page, for example
`WHERE n.id > 48211 ... ORDER BY n.id LIMIT 1000`. Each such page costs the same
regardless of depth.

A page can return fewer rows than `LIMIT`, or none, when some rows are hidden
from you. Keep going until `has_more` is `false`.

Pagination reads live data at request time; there is no snapshot. Each page
independently resolves the latest version of every row and filters out
soft-deleted rows. `SKIP` counts rows by position, so a row inserted or deleted
ahead of the current position between pages shifts later pages by one row: a
row can repeat or be missed. Rows whose sort key is NULL sort last and paginate
like any other row.

## Validation limits

GitLab Orbit rejects broad or ambiguous queries before it compiles SQL.

| Limit | Value |
|-------|-------|
| Query text | 32 KiB |
| Nodes per query | 5 |
| Relationships per query | 5 |
| Predicates per query | 16 |
| IDs in a list | 500 |
| Values in an `IN` list | 100 |
| Properties per node | 50 |
| Relationship types per relationship | 10 |
| Relationship hops and path depth | 3 |
| Rows per page | 1,000 |

Single-node queries also require selectivity. To inspect a broad entity, add a
predicate, provide IDs, or use a narrow ID range.

## Unsupported syntax

The language is read-only. It rejects mutations, `OPTIONAL MATCH`, `WITH`,
`UNION`, `UNWIND`, subqueries, `OR`, `XOR`, `DISTINCT`, `count(*)`, and
arbitrary expressions. Unsupported syntax returns an error that
names the problem instead of being ignored.

## Debug output

Add `DEBUG` to the end of a query to include the compiled ClickHouse SQL in the
response metadata when you are allowed to see it.
