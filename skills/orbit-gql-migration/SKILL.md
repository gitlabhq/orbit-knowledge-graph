---
name: orbit-gql-migration
description: Converts legacy Orbit JSON queries to Orbit GQL (read-only Cypher) and checks each result against your graph. Legacy queries are objects with query_type, nodes, relationships, or aggregations. Use when you find them in repos, runbooks, scripts, docs, or agent skills. Also use for a `glab orbit query --file` call with a JSON body, or a once-working query that fails with HTTP 400 compile_error. Skip for writing new queries (use the orbit skill).
version: 0.1.0
license: MIT
compatibility: Requires glab with the orbit commands and a GitLab instance with Orbit enabled in GQL mode.
metadata:
  audience: developers
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit-gql-migration
---

# Migrate legacy Orbit JSON queries to GQL

The Orbit JSON query language is deprecated and being removed. GitLab.com
already rejects it. This skill converts legacy JSON queries to GQL. It does not
teach the JSON language and never suggests it still works. It needs only the
`orbit` skill (`glab orbit skills get orbit`) for GQL syntax. The tables below
let you recognize each legacy construct without a JSON reference. Public API
background: <https://docs.gitlab.com/api/orbit/>.

This is a temporary migration aid. It will be removed once the JSON language
is gone from supported releases. Do not copy it into long-lived tooling.
Convert your queries once and keep the GQL.

## Step 0: confirm your instance speaks GQL

```shell
glab orbit query "CALL db.schema('Project')"
```

- Success: the instance is in GQL mode. Continue.
- An error, or the `orbit` skill's own text still shows JSON queries: your
  instance is not on GQL yet. Self-managed instances can run an older release,
  or have GQL query mode switched off. Converted queries would be rejected.
  Stop and tell the user to ask their administrator to enable GQL query mode or
  upgrade Orbit. Do not rewrite queries you cannot run.

## Workflow

1. Find legacy queries: `rg -l '"query_type"\s*:'`, code fences tagged
   `json orbit-query`, `--file` payloads or heredocs with `nodes` /
   `relationships`, and skills or prompts that embed them.
2. Convert one query at a time with the mapping table.
3. Run it (read-only) and check it with "Validate a conversion" below.
4. Report queries with no equivalent (see "Rejected constructs"); leave them
   for manual rework instead of guessing.

## Validate a conversion

Every step is read-only.

1. **Run it.** Use raw output so the shape is easy to inspect:

   ```shell
   glab orbit query --response-format raw "<gql>" > out.json
   jq -c '{query_type, row_count, pagination: .result.pagination}' out.json
   ```

   Exit 0 means it parsed and ran. HTTP 400 `compile_error` names the line and
   column; fix it and rerun.
2. **Compare with what the old query returned.** If you saved old output, compare
   row count, the property names, and the IDs of the first rows. If you did not,
   use an independent source for the same question, such as the REST API:

   ```shell
   # the five newest merged MRs of a project, by internal ID
   jq -c '[.result.nodes[] | select(.type=="MergeRequest") | .iid]' out.json
   glab api 'projects/<id>/merge_requests?state=merged&order_by=created_at&sort=desc&per_page=5' | jq -c '[.[].iid]'
   ```

   For counts, compare each group with the REST `X-Total` header
   (`glab api -i 'projects/<id>/merge_requests?state=opened&per_page=1'`).
   Small differences can come from indexing lag; large ones mean the
   conversion is wrong. The REST API omits `X-Total` above 10,000 rows.
   On very large projects the counts can also drift by a few percent. Use
   this check on small and medium projects.
3. **Check completeness.** `pagination.has_more` true means more rows exist than
   `LIMIT` returned. That is expected for a capped list; compare only the
   rows you asked for.
4. **Run a control.** Change one filter value to a nonsense value and confirm
   the row count drops to 0. A query that returns the same rows either way is
   not filtering.
5. **Do not trust an empty result.** Probe with a row you know exists
   (`MATCH (n:Label {id: <known id>}) RETURN n LIMIT 1`).

## Mapping

| Legacy JSON construct | GQL |
| --- | --- |
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
| `path_finding` | `MATCH path = ANY SHORTEST (a:L {id: 1})-[:T*1..N]->(b:L {id: 2}) RETURN path` |
| `neighbors` | `MATCH (c:L {id: 1})-[:A\|B]-(n) RETURN c, n` |

## Worked example

Legacy:

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
| node alias that is a GQL reserved word (`end`, `desc`, ...) | rename the alias |

## Pitfalls

- Negation exists in both languages: `ne` and the node-level `not` block map
  to `<>` and `NOT`. Keep semantics identical while migrating; improve after.
- Compare columns loosely: the legacy order followed node declaration order.
- Relationship directions follow the schema; the error names the valid one.
  `CALL db.schema('Node')` lists them.
- Fan-out on high-cardinality joins: prefer an aggregate.
- A node alias cannot be reused for a different label.
- Both languages cap at 16 predicates and 3 hops.
