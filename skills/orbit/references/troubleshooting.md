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

## `HTTP 503 Service Unavailable`

**Cause:** Upstream gRPC to the Orbit service failed. Usually transient.

**Fix:** retry with exponential backoff. If persistent, call
`glab api orbit/status` — if any component reports unhealthy, wait and
escalate in the team Slack channel.
