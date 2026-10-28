---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query GitLab Orbit with GQL, a read-only, openCypher-like query language. Learn what GQL supports, the statement shape, filters, aggregations, pagination, and limits.
title: GitLab Orbit query language
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/256465) in GitLab 19.5.

{{< /history >}}

GitLab Orbit accepts GQL, a read-only, openCypher-like query language that agents already know.
To send a query, see [run a query](_index.md#run-a-query).
The examples use the `gitlab-org/gitlab` project on GitLab.com.
To run them on your own projects, see [bounded nodes](#bounded-nodes).

## Supported syntax

GQL supports:

- `MATCH` with one connected pattern, and more than one `MATCH` clause in a query
- `WHERE` with `AND`, `NOT`, comparisons, `IN`, `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `IS NULL`, and `IS NOT NULL`
- `RETURN` with properties, nodes, `*`, `properties(n)`, `n{.property}`, and `AS` aliases
- The aggregate functions `count`, `count(DISTINCT x)`, `sum`, `avg`, `min`, and `max`, and `date_trunc` for time periods
- `ORDER BY` with one sort key, `SKIP` (or `OFFSET`), and `LIMIT`
- Variable-length relationships up to three hops, and shortest paths with `ANY SHORTEST` or `SHORTEST 1`
- `type(r)`, and the text search functions `token_match`, `all_tokens`, and `any_tokens`
- `CALL db.schema()` and `DEBUG`

GQL does not support these forms, and rejects them:

- Write clauses, such as `CREATE`, `MERGE`, `SET`, and `DELETE`, and more than one statement
- `OPTIONAL MATCH`, `WITH`, `UNION`, `UNWIND`, `YIELD`, and subqueries
- Patterns that are not connected or that have a cycle
- `OR`, `XOR`, and `x NOT IN [...]` (write `NOT x IN [...]` instead)
- `DISTINCT` outside `count()`, such as `RETURN DISTINCT` or `sum(DISTINCT x)`
- `count(*)` and arbitrary expressions
- More than one `ORDER BY` key
- Query parameters
- Other functions, such as `collect(x)` (the error names the function)

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
  Only the far node of a [neighbors query](#find-the-neighbors-of-a-node) has no label.
- Write a relationship type after a colon: `-[:IN_PROJECT]->`.
  A lowercase name without a colon, such as `-[r]->`, is a variable that matches any relationship type.
  GitLab Orbit rejects an uppercase name without a colon, such as `-[IN_PROJECT]->`.
- To match one of several relationship types, separate them with `|`, for example `-[:AUTHORED|ASSIGNED]->`.
  Relationship type names are case-sensitive.
- Use `->` or `<-` between labeled nodes. Use undirected `--` only to find the neighbors of a node.
- All parts of the pattern must form one connected tree, with no cycles.
  Each later part of the pattern must start from a node that an earlier part declares.

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
It returns them only if you have the Reporter, Security Manager, Developer, Maintainer, or Owner role for the `gitlab-org` group.
Otherwise, this query and the examples that use `gitlab-org/gitlab` or these IDs return no rows.
To run the examples, query with the full path of one of your projects, and use the IDs that it returns.

### Find the neighbors of a node

To find every node that is connected to one node, write an undirected relationship, `--`, from a [bounded node](#bounded-nodes):

```gql orbit-query
MATCH (mr:MergeRequest {id: 482908721})--(n)
RETURN n
LIMIT 20
```

The far node, `n`, takes no filters.

### Find the shortest path between two nodes

To find the shortest path between two [bounded nodes](#bounded-nodes), use `ANY SHORTEST`.
Name the relationship type, such as `-[:CONTAINS*1..2]->`, and keep the hop range small.
A shortest path with no relationship type, such as `-[*1..3]->`, fails.
The path uses `->`, starts at one hop, and is the only `MATCH` clause. `RETURN` takes only the path variable.
If the filter on an endpoint matches more than 500 nodes, the search starts from the 500 nodes with the lowest IDs.

```gql orbit-query
MATCH path = ANY SHORTEST (g:Group {id: 9970})-[:CONTAINS*1..2]->(p:Project {full_path: 'gitlab-org/gitlab'})
RETURN path
```

## Filters

`WHERE` combines predicates with `AND` and `NOT`.
To exclude values, write `NOT x.state IN [...]`, not `x.state NOT IN [...]`.
These forms are supported:

- Comparisons: `=`, `<>` or `!=`, `<`, `<=`, `>`, `>=`. You can compare a property with a literal.
  In a query without aggregation, you can also compare two node properties of the same type.
  A comparison between two properties does not [bound a node](#bounded-nodes).
- Lists: `x.state IN ['opened', 'merged']`, with up to 100 values, or up to 500 values in `x.id IN [...]`.
- Strings: `STARTS WITH`, `ENDS WITH`, and `CONTAINS`. The pattern needs at least three characters.
  Matching ignores ASCII case, except on the `branch` property of code nodes, such as `File` and `Definition`.
  `=` and `IN` are case-sensitive.
- Nulls: `IS NULL` and `IS NOT NULL`.

Values are literals.
A string is in single or double quotes.
Write a control character as an escape: `\n`, `\t`, `\r`, `\b`, `\f`, `\uXXXX`, or `\UXXXXXXXX`. Also escape `\\`, `\'`, and `\"`.
A string that contains a raw control character, such as a newline or a tab, fails.

For a timestamp property, such as `mr.created_at`, use a string in the format `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SS`,
with optional fractional seconds and a trailing `Z`, such as `'2026-01-31'` or `'2026-01-31T12:00:00Z'`.
A space can replace the `T`.
A date property, such as `WorkItem.due_date`, takes only `YYYY-MM-DD`.
A value with a time zone offset, such as `+02:00`, fails.
On a date or timestamp property, you can wrap the string in `date()` or `datetime()`, and the result is the same.
On other properties, the wrapper fails.
When you compare a date property with a timestamp property, the date counts as midnight.
Some timestamp properties, such as `mr.merged_at`, reject dates on or before `1970-01-01`.
To find rows without a value, use `IS NULL`.

An enum property, such as `mr.state`, accepts only its declared values with `=`, `<>`, and `IN`.
Enum values have no order, so `<`, `<=`, `>`, and `>=` fail.
An unknown value fails, and the error lists the valid values.

On a relationship, `source_kind` and `target_kind` must name the label of the matching endpoint node,
and `relationship_kind` must name a type in the pattern.
For example, `(mr:MergeRequest)-[r:IN_PROJECT]->(p:Project)` rejects `r.target_kind = 'Group'`.

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
The value needs at least three characters, and matching ignores ASCII case.
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

## Return values

- `RETURN mr.iid, mr.title` returns properties.
- `RETURN mr` returns the default properties of the node, for example `id`, `iid`, `title`, `state`,
  `source_branch`, and `target_branch` for `MergeRequest`. `properties(mr)` returns all properties of the node.
  `mr{.iid, .title}` returns the listed properties.
- `RETURN mr` and `properties(mr)` do not include properties for administrators, such as `User.is_admin`.
  To get one, name it, as in `RETURN u.is_admin`.
  For users who are not administrators, a query that names one fails.
- The response always includes the identity of each node and the metadata of each relationship.
- `type(r)` returns the type of a one-hop relationship variable in `WHERE`, `RETURN`, and `ORDER BY`,
  but not as an aggregation group key.
- `ORDER BY` sorts by a node property, a property alias, or `type(r)`. A relationship property fails.
  In an aggregation, `ORDER BY` sorts by an aggregate alias or a returned group key.
  Neighbors and shortest path queries do not accept `ORDER BY`.

### Aggregate results

- The aggregate functions are `count`, `sum`, `avg`, `min`, and `max`.
  The other items in `RETURN` become group keys.
  For users who are not administrators, an aggregation needs a node that belongs to a group or project,
  such as `Project` or `MergeRequest`. Neighbors and shortest path queries cannot aggregate.
  To sort by an aggregate, give it an alias: `RETURN mr.state, count(mr) AS mrs ORDER BY mrs DESC`.
  `sum` and `avg` need a numeric property. `min` and `max` need a number, boolean, date, or timestamp property.
  `DISTINCT` works only in `count()`, as in `count(DISTINCT mr.author_id)`.
  In an aggregation, a text match or a `<`, `<=`, `>`, or `>=` comparison on a free-text property, such as `mr.title`,
  needs that node in a group key: return the node, as in `RETURN mr, count(p)`, or one of its free-text properties,
  as in `RETURN mr.title, count(p)`. Other properties, such as `mr.iid`, do not count.
  Otherwise, the query fails with `text matching and ordering require grouping by "mr"`.
  `Project`, `Group`, `AgentSession`, `Branch`, `Commit`, and the code nodes
  `Definition`, `Directory`, `File`, and `ImportedSymbol` are exempt.
- To group by a time period, use `date_trunc('<unit>', <property>)` as a group key on a date or timestamp property.
  The units are `minute`, `hour`, `day`, `week`, `month`, `quarter`, and `year`.
  `minute` and `hour` also need a filter on the same property, or an `id =` or `id IN` filter on the same node.
  Weeks start on Sunday.

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
WHERE mr.created_at >= '2026-01-01T00:00:00Z'
RETURN date_trunc('month', mr.created_at) AS month, count(mr) AS mrs
ORDER BY month
LIMIT 12
```

## Pagination

To get results page by page, use `SKIP` and `LIMIT`, and add the page size to `SKIP` for each next page:
`SKIP 0 LIMIT 20`, then `SKIP 20 LIMIT 20`, then `SKIP 40 LIMIT 20`.
Write `SKIP` on every page, including `SKIP 0` on the first page, so that all pages use the same sort order.
Do not change other parts of the query between pages.

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
WHERE mr.state = 'merged'
RETURN mr.iid, mr.title
ORDER BY mr.created_at DESC
SKIP 20
LIMIT 20
```

In the `raw` response format (`--response-format raw` in the CLI), `pagination.has_more` is `true` while more rows exist.
Stop when `pagination.has_more` is `false`.
A page can return fewer rows than `LIMIT`, or no rows, because GitLab removes the rows that you cannot access.
`SKIP` also counts these rows, so pages do not overlap.
`pagination.truncated` has the same value as `pagination.has_more`.

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
- A query has at most five nodes. The pattern is a tree, so it has at most four relationships.
- A relationship names at most 10 types.
- A query has at most 16 conditions. Inline properties, such as `{project_id: 278964}`, count as conditions.
- Each node filters on at most 10 properties, and each relationship on at most 5.
  Each property takes at most 10 conditions.
- Parentheses, brackets, and braces nest at most 32 levels deep, and a condition has at most 32 `NOT`s.
- An aggregation has at most 10 aggregate functions and 4 group keys.
- `ORDER BY` takes one sort key.
- `LIMIT` accepts 1 to 1000 rows. The default is 30.
- `SKIP` accepts 0 to 10,000 rows.
- Each node returns at most 50 properties and filters on at most 500 IDs.
- Names, such as variables, aliases, labels, and properties, have at most 64 characters.
- Strings have at most 1024 characters.
- The query text is at most 32 KiB.

## Debug a query

Syntax errors report a line and column.

To see the SQL that GitLab Orbit runs, end the query with `DEBUG` and use the `raw` response format (`--response-format raw`).
On GitLab.com, the response includes the SQL only for GitLab team members.
On GitLab Self-Managed and GitLab Dedicated, the response includes the SQL only for administrators.
For other users, GitLab ignores `DEBUG`.

## Related topics

- [Queries](_index.md)
- [GitLab Orbit API](https://docs.gitlab.com/api/orbit/)
- [Prompt examples](../prompt-examples.md)
