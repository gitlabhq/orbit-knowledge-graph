# Orbit GQL reference

Read-only graph queries in GQL text, based on openCypher. Pass each query
inline, for example `glab orbit query "MATCH ... RETURN ..."`. See
[`SKILL.md`](../SKILL.md#running-a-query) for input forms.

## Discover the schema first

```shell
glab orbit query "CALL db.schema('MergeRequest')"
```

`CALL db.schema('Node')` returns one node's properties and its incoming and
outgoing relationship types. `CALL db.schema()` lists every node and
relationship type; call it at most once per session. Node names are case
sensitive. Issues, epics, tasks, and incidents are all the `WorkItem` node.

## Statement shape

```plaintext
MATCH pattern [WHERE predicates]
[MATCH pattern [WHERE predicates] ...]
RETURN projections
[ORDER BY key [ASC | DESC]]
[LIMIT rows | PAGE rows [AFTER 'token']]
```

- Declare each node's label, and any inline properties, where it first appears:
  `(mr:MergeRequest {iid: 235291})`. Later parts can reuse the bare variable.
- Write a relationship type after a colon: `-[:IN_PROJECT]->`. A lowercase
  name without a colon, such as `-[r]->`, is a variable that matches any
  relationship type. An uppercase name without a colon, such as
  `-[IN_PROJECT]->`, rejects.
- Use `->` or `<-` between labeled nodes. Undirected `--` is for neighbors only.
- All parts of the pattern must form one connected tree, with no cycles.
- Traversals are capped at three hops. `-[:CONTAINS*1..3]->` is a bounded
  variable-length hop.
- `ORDER BY` takes one sort key.
- `LIMIT` and `PAGE` accept 1 to 1000 rows.

## Selectivity

Every query needs at least one node bounded by an ID or a literal property
filter. Examples are `{id: 278964}`, `{full_path: 'gitlab-org/gitlab'}`, and a
`WHERE` comparison with a literal value. `LIMIT` does not count, so
`MATCH (p:Project) RETURN p LIMIT 5` rejects. A neighbors query needs a bounded
center node, and a shortest path needs both endpoints bounded.

## Predicates

`WHERE` combines predicates with `AND`. Supported forms:

- Comparisons: `=`, `<>` or `!=`, `<`, `<=`, `>`, `>=`.
- Lists: `x.state IN ['opened', 'merged']`.
- Strings: `STARTS WITH`, `ENDS WITH`, `CONTAINS`. The pattern needs at least
  3 characters.
- Nulls: `IS NULL`, `IS NOT NULL`.

Values are literals. There are no query parameters, so never splice untrusted
text into a query.

## Projections

- `RETURN mr.iid, mr.title` selects properties.
- `RETURN mr` selects the node's default columns; `properties(mr)` selects all.
- The response always includes node identity and relationship metadata.
- Aggregates are `count`, `sum`, `avg`, `min`, and `max`. Other return items
  become group keys. Alias a metric to sort by it:
  `RETURN mr.state, count(mr) AS mrs ORDER BY mrs DESC`.

## Pagination

`PAGE rows` replaces `LIMIT` and returns `next_cursor` while more rows remain.
Continue with `PAGE rows AFTER 'next_cursor'`. The cursor binds to the query
text, so change nothing else between pages.

If the response reports `pagination.truncated` (the default `llm` output prints
`truncated:true`), more rows matched than were returned. Raise the limit,
narrow the filters, or page with `PAGE`. Do not present a truncated result as
the full answer.

## Token search

`token_match(property, 'token')`, `all_tokens(property, 'tokens ...')`, and
`any_tokens(property, 'tokens ...')` match whole tokens through a text index.
They are faster than `CONTAINS` on large tables. They work in `WHERE`, combined
with `AND`, only on text-indexed properties. Other properties reject with a
validation error. Token values need at least 3 characters. These are the most
useful indexed properties:

| Node | Properties |
|---|---|
| `Definition` | `name`, `fqn`, `file_path` |
| `File` | `name`, `path` |
| `MergeRequest` | `title`, `description`, `source_branch`, `target_branch` |
| `MergeRequestDiffFile` | `old_path`, `new_path` |
| `WorkItem` | `title`, `description` |
| `Note` | `note` |
| `Pipeline` | `ref` |
| `User` | `name`, `username` |

Tokens match whole words, not substrings, so use `CONTAINS` for a fragment of
a word. A token predicate counts as a literal filter, so it can anchor a node.

## Not supported

Mutations, multiple statements, `OPTIONAL MATCH`, `WITH`, `UNION`, `UNWIND`,
subqueries, `OR`, general `NOT`, `DISTINCT`, `count(*)`, arbitrary expressions,
and offset pagination all reject. Syntax errors report a line and column.

## Recipes

### Look up a project's numeric ID

```gql orbit-query
MATCH (p:Project {full_path: 'gitlab-org/gitlab'})
RETURN p.id, p.full_path
LIMIT 1
```

For the repository you are in, `glab api projects/:fullpath | jq -r '.id'`
needs no query.

### Look up a merge request by IID

```gql orbit-query
MATCH (mr:MergeRequest {iid: 235291})-[:IN_PROJECT]->(p:Project {full_path: 'gitlab-org/gitlab'})
RETURN mr.iid, mr.title, mr.state
LIMIT 1
```

### Count merge requests per state in a project

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 77960826})
RETURN mr.state, count(mr) AS mrs
ORDER BY mrs DESC
LIMIT 10
```

### Pipelines that ran for one merge request

```gql orbit-query
MATCH (pl:Pipeline)
WHERE pl.merge_request_id = 482908721 AND pl.source = 'merge_request_event'
RETURN pl.id, pl.status, pl.ref
ORDER BY pl.created_at DESC
LIMIT 10
```

### Open work items in a project

```gql orbit-query
MATCH (wi:WorkItem)-[:IN_PROJECT]->(p:Project {id: 77960826})
WHERE wi.state = 'opened'
RETURN wi.iid, wi.title, wi.work_item_type
ORDER BY wi.created_at DESC
LIMIT 10
```

### Files a merge request touched

Use `HAS_DIFF`, which covers every diff snapshot. `HAS_LATEST_DIFF` covers only
the final one. Each file appears once per snapshot and there is no `DISTINCT`,
so group by the path:

```gql orbit-query
MATCH (mr:MergeRequest {id: 482908721})-[:HAS_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(f:MergeRequestDiffFile)
RETURN f.old_path, count(d) AS snapshots
ORDER BY snapshots DESC
LIMIT 50
```

`HAS_FILE` edges are sparsely populated. If this returns far fewer files than
the merge request changed, report the result as incomplete coverage.

### Merge requests that touched a file

This is the inverse of the recipe above: anchor on the file path and walk back
to the merge requests. Use `MergeRequestDiffFile.old_path`, which holds the path
before the change (equal to `new_path` unless the file was renamed). Each merge
request repeats once per diff snapshot, so group by it. Return `mr.project_id`
with `mr.iid`, because the same path exists in many projects:

```gql orbit-query
MATCH (mr:MergeRequest)-[:HAS_DIFF]->(d:MergeRequestDiff)-[:HAS_FILE]->(f:MergeRequestDiffFile {old_path: 'app/models/user.rb'})
RETURN mr.project_id, mr.iid, mr.title, mr.state, count(d) AS snapshots
ORDER BY snapshots DESC
LIMIT 50
```

Pick the edge by the question. "Which files did this merge request change?"
anchors on the merge request and walks `HAS_DIFF` then `HAS_FILE`. "Which merge
requests changed this file?" anchors on the file path and walks the same edges
backwards. The same sparse `HAS_FILE` coverage caveat applies.

The merge request that renamed a file to this path stores the path only in
`new_path`. To include it, rerun with `{new_path: 'app/models/user.rb'}` and
add `f.old_path` to `RETURN`, which gives the old name to query for history from
before the rename.

### Subclasses of a class

`EXTENDS` points from child to parent and collapses class extension, interface
implementation, and struct embedding. Use `Definition.fqn` for namespaced
parents. A bounded variable-length hop reaches indirect descendants:

```gql orbit-query
MATCH (child:Definition)-[:EXTENDS*1..3]->(parent:Definition {fqn: 'ApplicationRecord'})
RETURN child.name, child.fqn, child.file_path
LIMIT 100
```

### Callers of a function

`CALLS` points from the caller to the callee. Match `fqn` exactly as stored:
methods join the class with the language's separator, not Ruby's `Class#method`
notation. The separator is `::` for Ruby and Rust and `.` for Python and Java,
so `MergeRequests::RefreshService#execute` is stored as
`MergeRequests::RefreshService::execute`.

```gql orbit-query
MATCH (caller:Definition)-[:CALLS]->(callee:Definition {fqn: 'MergeRequests::RefreshService::execute'})
RETURN caller.name, caller.fqn, caller.file_path
LIMIT 100
```

An empty result usually means the `fqn` does not match. Check it first with
`token_match(d.name, 'execute')` on a `Definition`, or an `fqn STARTS WITH`
filter. Only after that, suspect that the language or project is not indexed.

### Merge requests whose title mentions any of several words

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 77960826})
WHERE any_tokens(mr.title, 'flaky retry') AND mr.state = 'merged'
RETURN mr.iid, mr.title
ORDER BY mr.created_at DESC
LIMIT 20
```

### Everything connected to one node

The far endpoint has a variable but no label.

```gql orbit-query
MATCH (mr:MergeRequest {id: 482908721})--(n)
RETURN n
LIMIT 20
```

### Merged merge requests by one author, a page at a time

```gql orbit-query
MATCH (u:User {username: 'root'})-[:AUTHORED]->(mr:MergeRequest)
WHERE mr.state = 'merged'
RETURN mr.iid, mr.title
ORDER BY mr.created_at DESC
PAGE 20
```

### Shortest path between two nodes

Name the relationship type and keep the hop range small. An untyped
`-[*1..3]->` follows every relationship and can time out.

```gql orbit-query
MATCH path = ANY SHORTEST (g:Group {id: 9970})-[:CONTAINS*1..2]->(p:Project {full_path: 'gitlab-org/gitlab'})
RETURN path
```

It returns one outgoing path per endpoint pair.

## Troubleshooting

### Empty result body

Usually the query matched no rows. Confirm with a known-good probe:

```shell
glab orbit query "MATCH (p:Project {full_path: 'gitlab-org/gitlab'}) RETURN p.id LIMIT 1"
```

If this returns a row, the connection works and your other query has no matches.

### Validation errors (HTTP 400, exit 1)

The query text did not parse or validate. Syntax errors report a line and
column. Common causes:

- No node has an ID or literal filter. Anchor at least one node; `LIMIT` does not count.
- `-[AUTHORED]->` instead of `-[:AUTHORED]->`. Without the colon the name is a variable.
- A relationship type that does not connect the two labels, or points the other way. The error names the valid direction.
- A node label repeated with a different label, or with inline properties twice.
- Unsupported syntax: `OR`, general `NOT`, `DISTINCT`, `count(*)`, `OPTIONAL MATCH`, `WITH`, or a second `ORDER BY` key.
- `PAGE ... AFTER` with a cursor from a different query. The cursor binds to the query text.

Fix: check node and relationship names with `CALL db.schema('Node')`.
