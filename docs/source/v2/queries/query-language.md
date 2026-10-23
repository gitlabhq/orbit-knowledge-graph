---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query GitLab Orbit with GQL, a read-only, OpenCypher-like query language. Learn what GQL supports, the statement shape, filters, aggregations, pagination, and limits.
title: GitLab Orbit query language
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1258) in GitLab 19.4 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `orbit_gql_queries`. Disabled by default.
- Generally available in GitLab 19.5.

{{< /history >}}

GitLab Orbit accepts GQL, a read-only, OpenCypher-like query language that agents already know.
To send a query, see [run a query](_index.md#run-a-query).

## Supported syntax

GQL supports:

- `MATCH` with one connected pattern, and more than one `MATCH` clause in a query
- `WHERE` with `AND`, `NOT`, comparisons, `IN`, `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `IS NULL`, and `IS NOT NULL`
- `RETURN` with properties, nodes, `properties(n)`, `n{.property}`, and `AS` aliases
- The aggregate functions `count`, `sum`, `avg`, `min`, and `max`, and `date_trunc` for time periods
- `ORDER BY` with one sort key, `SKIP` (or `OFFSET`), and `LIMIT`
- Variable-length relationships up to three hops, and shortest paths with `ANY SHORTEST` or `SHORTEST 1`
- `type(r)`, and the text search functions `token_match`, `all_tokens`, and `any_tokens`
- `CALL db.schema()` and `DEBUG`

GQL does not support these forms, and rejects them:

- Write clauses, such as `CREATE`, `MERGE`, `SET`, and `DELETE`, and more than one statement
- `OPTIONAL MATCH`, `WITH`, `UNION`, `UNWIND`, `YIELD`, and subqueries
- Patterns that are not connected or that have a cycle
- `OR`, `XOR`, and `x NOT IN [...]` (write `NOT x IN [...]` instead)
- `DISTINCT`, `count(*)`, and arbitrary expressions
- More than one `ORDER BY` key, and `LIMIT` before `SKIP`
- Query parameters
- Other functions, such as `collect(x)` (the error names the function)

### Neighbors

To find every node that is connected to one node, write an undirected relationship, `--`, from a bounded node:

```gql orbit-query
MATCH (mr:MergeRequest {id: 482908721})--(n)
RETURN n
LIMIT 20
```

### Shortest paths

To find the shortest path between two bounded nodes, use `ANY SHORTEST`.
Name the relationship type and keep the hop range small.
A pattern with no relationship type, such as `-[*1..3]->`, follows every relationship and might time out.

```gql orbit-query
MATCH path = ANY SHORTEST (g:Group {id: 9970})-[:CONTAINS*1..2]->(p:Project {full_path: 'gitlab-org/gitlab'})
RETURN path
```

## Discover the schema

To list the properties and relationships of one node type, run:

```shell
glab orbit query "CALL db.schema('MergeRequest')"
```

`CALL db.schema()` lists every node type and relationship type.
Node names are case-sensitive. Issues, epics, tasks, and incidents are all the `WorkItem` node.
For the full list, see [indexed data](../schema.md).

## Statement shape

```plaintext
MATCH pattern [WHERE predicates]
[MATCH pattern [WHERE predicates] ...]
RETURN projections
[ORDER BY key [ASC | DESC]]
[SKIP rows]
[LIMIT rows]
[DEBUG]
```

- Each node needs a variable. Declare the label of each node, and any inline properties, where the node first appears:
  `(mr:MergeRequest {iid: 235291})`. Later parts of the query can reuse the bare variable.
- Write a relationship type after a colon: `-[:IN_PROJECT]->`.
  A lowercase name without a colon, such as `-[r]->`, is a variable that matches any relationship type.
  GitLab Orbit rejects an uppercase name without a colon, such as `-[IN_PROJECT]->`.
- To match one of several relationship types, separate them with `|`, for example `-[:AUTHORED|ASSIGNED]->`.
  You can name up to 10 types.
- Use `->` or `<-` between labeled nodes. Use undirected `--` only to find the neighbors of a node.
- All parts of the pattern must form one connected tree, with no cycles.

## Bounded nodes

Every query needs at least one node bounded by an ID or a literal property filter,
such as `{id: 278964}`, `{full_path: 'gitlab-org/gitlab'}`, or a `WHERE` comparison with a literal value.
`LIMIT` does not count, so GitLab Orbit rejects `MATCH (p:Project) RETURN p LIMIT 5`.

A neighbors query needs a bounded center node.
A shortest path query needs both endpoints bounded.

The examples use numeric IDs, such as `project_id: 278964`.
An `id` is unique in the instance.
An `iid`, such as the number of a merge request, is unique only in its project.
To find an ID, query by path first:

```gql orbit-query
MATCH (mr:MergeRequest {iid: 235291})-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
RETURN mr.id, p.id
LIMIT 1
```

On GitLab.com, this query returns `482908721` and `278964`, the IDs that the examples use.
It returns them only if you have the Reporter, Security Manager, Developer, Maintainer, or Owner role in the `gitlab-org` group.
Otherwise, this query and the examples that use `gitlab-org/gitlab` or these IDs return no rows.
To run the examples, query with the full path of one of your projects, and use the IDs that it returns.

## Filters

`WHERE` combines predicates with `AND` and `NOT`.
To exclude values, write `NOT x.state IN [...]`, not `x.state NOT IN [...]`.
These forms are supported:

- Comparisons: `=`, `<>` or `!=`, `<`, `<=`, `>`, `>=`. You can compare a property with a literal or with another property.
- Lists: `x.state IN ['opened', 'merged']`, with up to 100 values.
- Strings: `STARTS WITH`, `ENDS WITH`, and `CONTAINS`. The pattern needs at least three characters.
  Matching ignores ASCII case, except on the `branch` property of code nodes, such as `File` and `Definition`.
- Nulls: `IS NULL` and `IS NOT NULL`.

Values are literals.
For dates, use an ISO 8601 string, such as `'2026-01-31'` or `'2026-01-31T12:00:00Z'`.
You can wrap the string in `date()` or `datetime()`, but the result is the same.
Group predicates with parentheses.
Queries do not take parameters, so do not insert untrusted text into a query.

### Text search

`CONTAINS` is indexed on text-indexed properties, such as `MergeRequest.title` or `Definition.fqn`.
Use it for a word prefix, such as `'migrat'`, or for a phrase with its words in order, such as `'fix flaky'`.

When word order does not matter, use the token functions.
Words are runs of letters and digits, so `token_match(d.file_path, 'main')` matches `src/main.rs`.

- `token_match(property, 'token')` matches one whole word.
- `all_tokens(property, 'first second')` requires every word, in any order.
- `any_tokens(property, 'first second')` requires at least one of the words.

Properties without a text index reject the token functions.
This query finds merge requests with both words in the title, in any order:

```gql orbit-query
MATCH (mr:MergeRequest {project_id: 278964})
WHERE all_tokens(mr.title, 'database migration')
RETURN mr.iid, mr.title, mr.state
ORDER BY mr.created_at DESC
LIMIT 10
```

These properties have a text index:

<!-- The table below is generated from the ontology's `text` storage indexes. -->
<!-- Copy it from docs/source/remote/queries/query-language.md after `mise run docs:query-language`. That task does not update this v2 copy. -->
<!-- BEGIN GENERATED: text-indexed-properties -->

| Entity | Text-indexed properties |
|--------|-------------------------|
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

## Return values

- `RETURN mr.iid, mr.title` returns properties.
- `RETURN mr` returns the default properties of the node, for example `id`, `iid`, `title`, `state`,
  `source_branch`, and `target_branch` for `MergeRequest`. `properties(mr)` returns all of them.
  `mr{.iid, .title}` returns the listed properties.
- The response always includes the identity of each node and the metadata of each relationship.
- The aggregate functions are `count`, `sum`, `avg`, `min`, and `max`.
  The other items in `RETURN` become group keys.
  To sort by an aggregate, give it an alias: `RETURN mr.state, count(mr) AS mrs ORDER BY mrs DESC`.
- To group by a time period, use `date_trunc('<unit>', <property>)` as a group key on a date property.
  The units are `minute`, `hour`, `day`, `week`, `month`, `quarter`, and `year`.
  `minute` and `hour` also need a filter on the same property. Weeks start on Sunday.
- `type(r)` returns the type of a one-hop relationship variable in `WHERE`, `RETURN`, and `ORDER BY`,
  but not as an aggregation group key.

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
WHERE mr.created_at >= '2026-01-01T00:00:00Z'
RETURN date_trunc('month', mr.created_at) AS month, count(mr) AS mrs
ORDER BY month
LIMIT 12
```

## Pagination

To get results page by page, use `SKIP` and `LIMIT`.
`OFFSET` is the same as `SKIP`.
Put `SKIP` before `LIMIT`.
Start at `SKIP 0`.
For each next page, add the page size to `SKIP`:
`SKIP 0 LIMIT 20`, then `SKIP 20 LIMIT 20`, then `SKIP 40 LIMIT 20`.
Write `SKIP` on every page, also on the first page, so that all pages use the same sort order.
Do not change other parts of the query between pages.

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
WHERE mr.state = 'merged'
RETURN mr.iid, mr.title
ORDER BY mr.created_at DESC
SKIP 20
LIMIT 20
```

In the `raw` response format, `pagination.has_more` is `true` while more rows exist.
Stop when `pagination.has_more` is `false`.
A page can return fewer rows than `LIMIT`, or no rows, because GitLab removes the rows that you cannot access.
`SKIP` also counts these rows, so pages do not overlap.
When `pagination.truncated` is `true`, the result is incomplete.

`SKIP` accepts up to 10,000 rows.
To go deeper, sort by a unique property, such as `id`, and filter past the last value of the previous page:

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
WHERE mr.state = 'merged' AND mr.id > 482908721
RETURN mr.id, mr.iid, mr.title
ORDER BY mr.id
LIMIT 1000
```

Use the last `mr.id` of each page as the lower bound for the next page.

Each page reads the current data.
If a row is added or deleted before your position between two pages,
the later pages move by one row, so a row can repeat or be missing.

## Limits

- Each relationship spans at most three hops. `-[:CONTAINS*1..3]->` is a bounded variable-length hop.
- A query has at most five nodes and five relationships.
- `ORDER BY` takes one sort key.
- `LIMIT` accepts 1 to 1000 rows. The default is 30.
- `SKIP` accepts 0 to 10,000 rows.
- Each node returns at most 50 properties and filters on at most 500 IDs.
- The query text is at most 32 KiB.

Syntax errors report a line and column.

To see the SQL that GitLab Orbit runs, end the query with `DEBUG` and use the `raw` response format.
On GitLab.com, the response includes the SQL only for GitLab team members.
On GitLab Self-Managed and GitLab Dedicated, the response includes the SQL only for administrators.
For other users, GitLab ignores `DEBUG`.

## Related topics

- [Queries](_index.md)
- [GitLab Orbit API](https://docs.gitlab.com/api/orbit/)
- [Prompt examples](../prompt-examples.md)
