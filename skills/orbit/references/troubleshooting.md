# Orbit skill troubleshooting

Common errors when using `glab api` against `/api/v4/orbit/*`, in rough order
of frequency. See [`SKILL.md`](../SKILL.md) for prerequisites.

## `HTTP 415: The provided content-type '' is not supported.`

**Cause:** `glab api --method POST` without an explicit `Content-Type` header.

**Fix:** add the header, or use the bundled wrapper:

```bash
glab api --method POST orbit/query \
  --header "Content-Type: application/json" \
  --input /tmp/q.json

# or (adjust path to wherever the skill is installed):
~/.config/opencode/skills/orbit/scripts/orbit-query /tmp/q.json
```

## `{"error":"404 Not Found"}`

**Cause 1 — feature flag off.** Orbit is gated behind the `knowledge_graph`
feature flag. If it's disabled for your user, every `/api/v4/orbit/*` endpoint
returns 404 regardless of input.

**Fix:** contact an admin to enable `knowledge_graph` for your user.

**Cause 2 — typo in the path.** The endpoint is `orbit/...`, not
`orbit_mcp/...`, `knowledge_graph/...`, or `projects/<id>/orbit/...`.
Orbit is top-level and user-scoped, not project-scoped.

## `HTTP 403 No Knowledge Graph enabled namespaces available`

**Cause:** Your user has the feature flag enabled but belongs to no top-level
group that has Orbit turned on.

**Fix:** an Owner of at least one top-level group you belong to must turn
Orbit on via **Orbit > Configuration** in the GitLab UI.

## `HTTP 401` / `HTTP 403` on all endpoints

**Cause:** Missing or expired `glab` authentication.

**Fix:**

```bash
glab auth status
glab auth login    # if expired
```

## `HTTP 429: Too Many Requests`

**Cause:** Rate limit (`orbit_query`) exceeded.

**Fix:** inspect the `Retry-After` response header and back off. For
agent-driven bulk work, reduce `limit`, add a short sleep between queries,
or batch via aggregation.

## Empty response body from `/orbit/query`

**Cause:** Usually normal — either the upstream gRPC returned no rows, or
output is being piped elsewhere. The Orbit API uses Workhorse streaming:
Rails returns a `Gitlab-Workhorse-Send-Data` header with an empty body, and
Workhorse substitutes the streamed gRPC response before it reaches `glab`.
You should see normal JSON output in your terminal.

**Verify with a known-good probe:**

```bash
cat > /tmp/q-min.json <<'JSON'
{"query": {"query_type": "traversal", "node": {"id": "p", "entity": "Project"}, "limit": 1},
 "response_format": "raw"}
JSON
glab api --method POST orbit/query \
  --header "Content-Type: application/json" \
  --input /tmp/q-min.json
```

If this returns a result, the connection works and your other query likely
has no matches.

## `HTTP 400` with validation errors

**Cause:** Query didn't match the DSL JSON Schema. Common causes:

- `query_type` is anything other than `traversal`, `aggregation`, `path_finding`,
  or `neighbors`. The enum is exactly those four. To search a single entity type,
  use `traversal` with a single `node`.
- Using `node` (singular) with `aggregation` / `path_finding`
  (they require `nodes`, plural).
- Using `nodes` (plural) with `neighbors` or single-node `traversal`
  (they require `node`, singular).
- Multi-node `traversal` (uses `nodes`) without at least 2 nodes and 1 relationship.
- `query_type: "aggregation"` without any `aggregations` entries.
- `max_hops` or `max_depth` > 3 (server-enforced ceiling).
- `cursor.offset + cursor.page_size > limit`.

**Fix:** validate against the live DSL schema, which is authoritative and
always current:

```bash
glab api orbit/tools | jq '.[] | select(.name=="query_graph") | .description' -r
```

The `description` field embeds the full JSON Schema (inside a `<toon>` block
for compact transport — still parseable). Full field reference in
[`query_language.md`](query_language.md).

## `{"code":"compile_error","message":"schema violation: traversal and aggregation queries require node_ids or filters on at least one node to avoid full edge table scans"}`

**Cause:** `traversal` or `aggregation` query has no `node_ids`, `id_range`, or
`filters` on any node. The server rejects unscoped queries to prevent full
edge-table scans.

**Fix:** add a `filters` clause on at least one node, or pre-resolve the
node IDs and pass them via `node_ids`:

```json
{"node": {"id": "p", "entity": "Project",
          "filters": {"full_path": {"op": "eq", "value": "gitlab-org/orbit/knowledge-graph"}}}}
```

## `{"code":"compile_error","message":"Query compilation failed."}` on a `path_finding` or `neighbors` query

**Cause 1 — unknown relationship type.** The DSL silently rejects edge names
that aren't in the ontology. `REFERENCES` is the most common guess and does
not exist. Source-code edges are: `CALLS`, `DEFINES`, `EXTENDS`, `IMPORTS`,
`ON_BRANCH`, `CONTAINS`. The closest "reference" analog is `IMPORTS`
(File → ImportedSymbol → Definition).

**Fix:** enumerate edges with `glab api orbit/schema | jq -r '.edges[].name'`
and use a valid name.

**Cause 2 — mismatched edge variant.** The compiler accepts edge variants the
ontology doesn't declare and either errors or silently returns 0 rows. The
most common: `IN_PROJECT(File→Project)` does not exist — only
`Branch → Project`. Files reach Project via
`File → ON_BRANCH → Branch → IN_PROJECT → Project`, or by filtering on the
stored `File.project_id` column directly.

**Fix:** check `glab api "orbit/schema?expand=File" | jq '.edges[] | select(.variants[]?.source_type=="File" or .variants[]?.target_type=="File")'`.

## `HTTP 502` / `HTTP 504 Gateway Timeout` on `orbit/query`

**Cause 1 — `path_finding` or `neighbors` on a dense center without
`rel_types`.** Definition / File / User / MergeRequest centers fan out
through every edge type and time out the gateway.

**Fix:** always set `rel_types` to a tight list when the source or target
node is one of the dense types above.

**Cause 2 — aggregation across a high-cardinality unscoped name.** Grouping
on `Definition.name = "compile"` (or similar names that match many defs
across branches) joins on every match and times out.

**Fix:** look up the matching Definition IDs first via a single-node
`traversal`, then pin them with `node_ids` in the aggregation. See
[`recipes.md`](recipes.md#two-step-pin-by-node_ids).

**Cause 3 — non-sargable string filter on `File.path`.** `ends_with` on
`path` cannot use the column index and full-scans the file table.

**Fix:** use `starts_with` with a rooted prefix (e.g. `crates/`), or filter
on `File.extension` / `File.name` instead.

**Cause 4 — transient gateway error.** A combined `state = "merged"` +
`merged_at >= ...` filter on MRs has been observed to 502 once and succeed
on retry without the date filter. Retry with backoff before changing the
query.

## Aggregation results show `sum(...)` and `avg(...)` that don't add up

**Cause:** when an aggregation includes per-row hydration columns
(`columns` on the `target` or `group_by` node) alongside `sum` / `avg`
aggregations on the same property, the per-row hydrated value is returned
in the same row as the aggregate. A row like
`{username, mrs: 175, avg_added: 2737, total_added: 21894}` looks like a
total over 175 MRs but `total_added` is the per-row hydrated value of the
representative MR, not the sum.

**Fix:** either drop hydration columns from the aggregation, or treat
non-aggregate columns as one representative row per group rather than as
totals. Verify by recomputing with a separate aggregation query.

## `path_finding` returns the same path 2–4× with different `path_id`s

**Cause:** edge tables retain multiple `_version` rows per logical edge.
The path enumerator walks each version separately, so a single 2-hop path
can surface as 4 path rows.

**Fix:** dedupe on the sequence of `node_id`s in the path; ignore the
`path_id` field for uniqueness. Same applies to duplicated rows in
`neighbors` results.

## `HTTP 503 Service Unavailable`

**Cause:** Upstream gRPC to the Orbit service failed. Usually transient.

**Fix:** retry with exponential backoff. If persistent, call
`glab api orbit/status` — if any component reports unhealthy, wait and
escalate in the team Slack channel.
