---
redirect_to: 'query-language.md'
remove_date: '2027-01-20'
---

<!-- markdownlint-disable -->

This document was moved to [another location](query-language.md).

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/256465) in GitLab 19.4 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `orbit_gql_queries`. Disabled by default.
- Generally available in GitLab 19.5.

{{< /history >}}

GitLab Orbit accepts a read-only subset of [openCypher 9](https://opencypher.org/), which agents already know.
It replaces the deprecated JSON query domain-specific language (DSL). To send a query, see [run a query](_index.md#run-a-query).
Code examples in this documentation carry the label `gql`, the internal name of this query language.

## Discover the schema

To list the properties and relationships of one node type, run:

```shell
glab orbit query "CALL db.schema('MergeRequest')"
```

`CALL db.schema()` lists every node type and relationship type.
Node names are case-sensitive. Issues, epics, tasks, and incidents are all the `WorkItem` node.
For the full list, see [What GitLab Orbit indexes](../schema.md).

## Statement shape

```plaintext
MATCH pattern [WHERE predicates]
[MATCH pattern [WHERE predicates] ...]
RETURN projections
[ORDER BY key [ASC | DESC]]
[LIMIT rows | PAGE rows [AFTER 'token']]
```

- Each node needs a variable. Declare the label of each node, and any inline properties, where the node first appears:
  `(mr:MergeRequest {iid: 235291})`. Later parts of the query can reuse the bare variable.
- Write a relationship type after a colon: `-[:IN_PROJECT]->`.
  A lowercase name without a colon, such as `-[r]->`, is a variable that matches any relationship type.
  An uppercase name without a colon, such as `-[IN_PROJECT]->`, is rejected.
- To match one of several relationship types, separate them with `|`, for example `-[:AUTHORED|ASSIGNED]->`.
  You can name up to 10 types.
- Use `->` or `<-` between labeled nodes. Use undirected `--` only to find the neighbors of a node.
- All parts of the pattern must form one connected tree, with no cycles.

## Anchor at least one node

Every query needs at least one node that is bounded by an ID or a literal property filter,
such as `{id: 278964}`, `{full_path: 'gitlab-org/gitlab'}`, or a `WHERE` comparison with a literal value.
`LIMIT` does not count, so `MATCH (p:Project) RETURN p LIMIT 5` is rejected.

A neighbors query needs a bounded center node. A shortest path query needs both endpoints bounded.

The examples on this page use numeric IDs, such as `project_id: 278964`. An `id` is unique in the instance.
An `iid`, such as the number of a merge request, is unique only in its project. To find an ID, query by path first:

```gql orbit-query
MATCH (mr:MergeRequest {iid: 235291})-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
RETURN mr.id, p.id
LIMIT 1
```

On GitLab.com, this query returns `482908721` and `278964`, the IDs that the examples use.
It returns them only if you have at least the Reporter role in the `gitlab-org` group.
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

### Search text

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
<!-- Do not edit it by hand: run `mise run docs:generate` and commit. The `docs-generated-check` pre-commit hook fails on drift. -->
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
  `sum` and `avg` need a numeric property. `min` and `max` need a number, boolean, enum, date, or timestamp property.
  In an aggregation, a text match or a `<`, `<=`, `>`, or `>=` comparison on a free-text property, such as `mr.title`,
  needs that node in a group key. `Project`, `Group`, and code nodes are exempt.
- To group by a time period, use `date_trunc('<unit>', <property>)` as a group key on a date property.
  The units are `minute`, `hour`, `day`, `week`, `month`, `quarter`, and `year`.
  `minute` and `hour` also need a filter on the same property. Weeks start on Sunday.

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
WHERE mr.created_at >= '2026-01-01T00:00:00Z'
RETURN date_trunc('month', mr.created_at) AS month, count(mr) AS mrs
ORDER BY month
LIMIT 12
```

## Pagination

`PAGE rows` replaces `LIMIT` and returns `next_cursor` while more rows remain.
To get the next page, run the same query with `PAGE rows AFTER '<next_cursor>'`.
The cursor belongs to the query text, so do not change anything else between pages.

In the `raw` response format, the cursor is `pagination.next_cursor`.
When `pagination.truncated` is `true`, the result is incomplete.

## Limits

- Each relationship spans at most three hops. `-[:CONTAINS*1..3]->` is a bounded variable-length hop.
- A query has at most five nodes and five relationships.
- `ORDER BY` takes one sort key.
- `LIMIT` and `PAGE` accept 1 to 1000 rows.
- Each node returns at most 50 properties and filters on at most 500 IDs.
- The query text is at most 32 KiB.

These forms are not supported and are rejected:
write clauses, more than one statement, `OPTIONAL MATCH`, `WITH`, `UNION`, `UNWIND`, `YIELD`, subqueries,
`OR`, `x NOT IN [...]`, `DISTINCT`, `count(*)`, arbitrary expressions, and `SKIP` or other offset pagination.
`type(r)` returns the type of a one-hop relationship variable in `WHERE`, `RETURN`, and `ORDER BY`, but not as an aggregation group key. Other functions, such as `collect(x)`, are rejected with an error that names the function.

Syntax errors report a line and column.

To see the SQL that GitLab Orbit runs, end the query with `DEBUG` and use the `raw` response format.
On GitLab.com, the response includes the SQL only for GitLab team members.
On GitLab Self-Managed and GitLab Dedicated, it includes the SQL only for administrators. For other users, GitLab ignores `DEBUG`.

## Examples

### Count merge requests by state

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
RETURN mr.state, count(mr) AS mrs
ORDER BY mrs DESC
LIMIT 10
```

### List open work items in a project

```gql orbit-query
MATCH (wi:WorkItem)-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
WHERE wi.state = 'opened'
RETURN wi.iid, wi.title, wi.work_item_type
ORDER BY wi.created_at DESC
LIMIT 10
```

### Find the merge requests that changed a file

`HAS_DIFF` covers every diff version of a merge request.
Anchor the file on `old_path` and `project_id`, so the same path in another project does not match.

```gql orbit-query
MATCH (mr:MergeRequest)-[:HAS_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(f:MergeRequestDiffFile {project_id: 278964, old_path: 'app/services/base_service.rb'})
RETURN mr.id, mr.iid, mr.title, count(d) AS snapshots
ORDER BY snapshots DESC
LIMIT 100
```

### Find the subclasses of a class

`EXTENDS` points from the child to the parent. This query includes descendants up to three hops away.

```gql orbit-query
MATCH (child:Definition)-[:EXTENDS*1..3]->(parent:Definition {fqn: 'ApplicationRecord', project_id: 278964, branch: 'master'})
RETURN child.name, child.fqn, child.file_path
LIMIT 1000
```

### Find everything connected to one node

```gql orbit-query
MATCH (mr:MergeRequest {id: 482908721})--(n)
RETURN n
LIMIT 20
```

### Find the shortest path between two nodes

Name the relationship type and keep the hop range small.
An untyped `-[*1..3]->` follows every relationship and might time out.

```gql orbit-query
MATCH path = ANY SHORTEST (g:Group {id: 9970})-[:CONTAINS*1..2]->(p:Project {full_path: 'gitlab-org/gitlab'})
RETURN path
```

For more queries by goal, see [use cases](../use-cases.md).

## Related topics

- [Queries](_index.md)
- [REST API](api.md)
- [What GitLab Orbit indexes](../schema.md)
