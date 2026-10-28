---
name: orbit-gql-migration
description: Converts and migrates legacy Orbit JSON DSL queries to Orbit GQL, then checks each converted query read-only against the graph. Legacy queries are objects with query_type, nodes, relationships, or aggregations. Use when a repo, runbook, script, doc, or agent skill contains them. Also use when a `glab orbit query --file` envelope or API body has an object `query`. Also use when a legacy JSON query fails with HTTP 400 compile_error. Do not use for writing new GQL queries (use the orbit skill).
version: 0.1.0
license: MIT
compatibility: Requires glab with the orbit commands and a GitLab instance with Orbit enabled.
metadata:
  audience: developers
  keywords: orbit, gql, migration, json, dsl, deprecated, glab
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit-gql-migration
---

# Migrate legacy Orbit JSON queries to GQL

The Orbit JSON query language is deprecated and removed. Convert each legacy
query to GQL (`glab orbit skills get orbit` has the syntax), run it, and keep
the GQL. This skill is a temporary migration aid and will be removed. Do not
copy it into long-lived tooling. Public API background:
<https://docs.gitlab.com/api/orbit/>.

## Step 0: confirm the instance accepts GQL

```shell
glab orbit query "CALL db.schema('Project')"
```

- A GQL syntax or compile error means the instance is too old for GQL. Stop and
  ask the administrator to upgrade GitLab and Orbit. Do not rewrite queries
  you cannot run.
- An auth error means the login is missing or expired. Fix it with
  `glab auth login`, then rerun. An HTTP 403 can instead mean the user has no
  Orbit access, and logging in again does not fix that. Ask the administrator.
- A network error or HTTP 429 is not a version problem. Wait and retry.

## Traps

- `neighbors` defaults to `direction: outgoing`. Map the direction you find,
  and write `->` when it is omitted. `-[...]-` adds the incoming neighbors and
  returns plausible extra rows.
- A node alias that is a GQL reserved word fails with "expected a variable" at
  the alias. The check is case-insensitive, so `End` fails too. Rename it. Likely ones: `end`, `desc`, `order`, `in`, `on`, `is`,
  `as`, `by`, `all`, `any`, `case`, `of`, `set`, `with`, `limit`, `match`.
  `start` is fine.
- The query lives in an envelope. Replace the object under `query` with a
  string of GQL and keep the other envelope fields. Never replace the whole
  file with bare GQL, because `--file` rejects it. A short query also runs
  without a file: `glab orbit query '<gql>'` takes the query text directly.
- Some constructs have no GQL form. See "Rejected constructs".

## Workflow

1. Find legacy queries: `rg -l '"query_type"\s*:'`, `--file` payloads,
   heredocs, API or MCP call bodies with `nodes` or `relationships`, and skills
   or prompts that embed them. Skip output files you wrote yourself.
2. Convert one query at a time with the mapping.
3. Validate it (below).
4. Report queries with no equivalent. Leave them for manual rework instead of
   guessing.

## Validate a conversion

Every step is read-only.

1. **Run it.** Use raw output so the shape is easy to inspect:

   ```shell
   glab orbit query --response-format raw "<gql>" > /tmp/out.json
   jq -c '{query_type, row_count, pagination: .result.pagination}' /tmp/out.json
   ```

   Exit 0 means it parsed and ran. HTTP 400 `compile_error` usually names the
   line and column. Fix it and rerun.
2. **Compare with what the old query returned.** If you saved old output,
   compare row count, property names, and the IDs of the first rows. If you
   did not, use an independent source for the same question, such as the REST
   API. Five newest merged MRs of a project:

   ```shell
   glab orbit query --response-format raw "MATCH (mr:MergeRequest {project_id: <id>, state: 'merged'}) RETURN mr.iid ORDER BY mr.created_at DESC LIMIT 5" \
     | jq -c '[.result.nodes[] | select(.type=="MergeRequest") | .iid]'
   glab api 'projects/<id>/merge_requests?state=merged&order_by=created_at&sort=desc&per_page=5' | jq -c '[.[].iid]'
   ```

   For counts, compare each group with the REST `X-Total` header
   (`glab api -i 'projects/<id>/merge_requests?state=opened&per_page=1'`).
   Small differences can come from indexing lag; large ones mean the
   conversion is wrong. The REST API omits `X-Total` above 10,000 rows. On very
   large projects the counts can also drift by a few percent. Use this check on
   small and medium projects.
3. **Check completeness.** `pagination.has_more` true means more rows exist
   than `LIMIT` returned. That is expected for a capped list. Compare only the
   rows you asked for.
4. **Run a control.** Change one filter value to a nonsense value and confirm
   the row count drops to 0. A query that returns the same rows either way is
   not filtering.
5. **Do not trust an empty result.** Probe with a row you know exists
   (`MATCH (n:Label {id: <known id>}) RETURN n LIMIT 1`).

## Mapping

| Legacy JSON construct | GQL |
| --- | --- |
| `{"query": {...}, ...}` envelope | `{"query": "<GQL text>", ...}`; keep other fields such as `response_format` |
| node without `columns` | `RETURN n` (default columns) |
| `options` (`dynamic_columns`, `include_debug_sql`) | drop it; it never changes semantics. Report `dynamic_columns` use |
| `id_property` | no known equivalent; report for manual rework |
| `query_type` | inferred from the pattern; drop it |
| `nodes[].id` / `entity` | `(id:Entity)`; rename ids that are GQL reserved words, e.g. `end` |
| `node_ids: [1]` | `{id: 1}` |
| `node_ids: [1,2,3]` | `WHERE n.id IN [1, 2, 3]` |
| `id_range {start,end}` | `WHERE n.id >= a AND n.id <= b` |
| `filters: {p: v}` / `eq` | inline `{p: v}` or `WHERE n.p = v` |
| `ne: v` | `WHERE n.p <> v` or `NOT n.p = v` |
| node-level `not: {p: {in: [...]}}` | `WHERE NOT n.p IN [...]` (not postfix `NOT IN`) |
| `gt/gte/lt/lte` | `> >= < <=` |
| `in` | `IN [...]` |
| `contains/starts_with/ends_with` | `CONTAINS / STARTS WITH / ENDS WITH` |
| `is_null: true` / `false` | `IS NULL` / `IS NOT NULL` |
| `token_match/all_tokens/any_tokens` | same-named functions in `WHERE` |
| `relationships[{type,from,to}]` | `(from)-[:TYPE]->(to)` |
| `type: [A,B]` | `-[:A\|B]->` |
| `hops: [1,2]` | `-[:TYPE*1..2]->`; max 3 in both |
| `direction: incoming` | `<-[:T]-` |
| `direction: both` | `-[:T]-` in a neighbors query only; rejected in a traversal, see below |
| relationship `filters` (single hop) | `[r:T {p: v}]` or `WHERE r.p = v` |
| `type: "*"` | `-->` |
| `columns: [a,b]` | `RETURN n.a, n.b` |
| `columns: "*"` | `RETURN properties(n)` |
| `order_by: "-n.p"` | `ORDER BY n.p DESC` (asc: no suffix) |
| `limit` | `LIMIT n`; default is 30 in both, write it anyway |
| `aggregations [{count: n, as: x}]` | `RETURN ..., count(n) AS x`; `sum/min/max/avg` likewise |
| `group_by: ["n.p"]` | the non-aggregate `RETURN` items |
| `group_by: ["u"]` (node) | `RETURN u{.id, .username}` |
| `group_by [{key, truncate, as}]` | `date_trunc('month', n.p) AS as` |
| `aggregation_sort: "-x"` | `ORDER BY x DESC` |
| `path_finding` `{from, to, max_depth, rel_types}` | `MATCH path = ANY SHORTEST (a:L {id: 1})-[:T\|U*1..max_depth]->(b:L {id: 2}) RETURN path`; GQL requires named types, so `rel_types` is mandatory |
| `neighbors` `{direction, rel_types}` | outgoing (default) `(c:L {id: 1})-[:A\|B]->(n)`, incoming `<-[:A\|B]-`, both `-[:A\|B]-`, then `RETURN c, n`; omitted `rel_types` is `(c)-->(n)` |

## Worked examples

Aggregation. Legacy:

```json
{"query": {"query_type": "aggregation",
  "nodes": [{"id": "mr", "entity": "MergeRequest"},
            {"id": "p", "entity": "Project", "node_ids": [278964]}],
  "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
  "group_by": ["mr.state"],
  "aggregations": [{"count": "mr", "as": "mrs"}],
  "aggregation_sort": "-mrs", "limit": 10}}
```

GQL:

```gql
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project {id: 278964})
RETURN mr.state, count(mr) AS mrs
ORDER BY mrs DESC
LIMIT 10
```

Neighbors with no `direction`, so outgoing. Legacy:

```json
{"query": {"query_type": "neighbors",
  "nodes": [{"id": "mr", "entity": "MergeRequest", "node_ids": [482908721]}],
  "neighbors": {"rel_types": ["AUTHORED", "IN_PROJECT"]}, "limit": 25}}
```

GQL:

```gql
MATCH (mr:MergeRequest {id: 482908721})-[:AUTHORED|IN_PROJECT]->(n)
RETURN mr, n
LIMIT 25
```

## Rejected constructs and workarounds

| Construct | What to do |
| --- | --- |
| `OR` between conditions | `IN [...]` for one property; otherwise run two queries |
| `OPTIONAL MATCH`, `WITH`, `UNION`, `UNWIND` | split into separate queries |
| `DISTINCT`, `count(*)` | group by the key; `count(node)` |
| a second `ORDER BY` key | sort the rows client side |
| no node anchored by an ID or literal filter | anchor one node; `LIMIT` does not count |
| `direction: both` on a traversal relationship | no equivalent. Run one query per direction, or a neighbors-style query |
| relationship `filters` on a variable-length hop | no equivalent (GQL rejects it). Any rewrite, such as a chain of filtered single-hop edges, changes the plan and result shape, so check its results by hand |
| postfix `n.p NOT IN [...]` | `NOT n.p IN [...]` |

## Pitfalls

- Negation exists in both languages: `ne` and the node-level `not` block map
  to `<>` and `NOT`. Keep semantics identical while migrating; improve after.
- Compare columns loosely: the legacy order followed node declaration order.
- Relationship directions follow the schema; the error names the valid one.
  `CALL db.schema('Node')` lists them.
- Fan-out on high-cardinality joins: prefer an aggregate.
- A node alias cannot be reused for a different label.
- Both languages cap at 16 predicates and 3 hops.
