# Orbit skill troubleshooting

Common errors when using `glab orbit remote`, organised by exit code. See
[`SKILL.md`](../SKILL.md) for prerequisites.

## CLI exit codes

| Exit | HTTP | Meaning                                                      |
|------|------|--------------------------------------------------------------|
| `0`  | 2xx  | Success.                                                     |
| `1`  | —    | Generic error (parse error, IO error, malformed body).       |
| `2`  | 404  | Orbit endpoint unavailable (typically: feature flag is off). |
| `3`  | 401  | Not authenticated.                                           |
| `4`  | 403  | Access denied (no Knowledge Graph enabled namespaces).       |
| `5`  | 429  | Rate limited.                                                |

`glab orbit remote query --format raw` is the easiest way to surface the full
JSON error payload when the exit code alone is not enough.

## Exit `2` — feature flag is off, or wrong subcommand

**Cause 1 — feature flag off.** Orbit is gated behind the `knowledge_graph`
feature flag. If it is disabled for your user, every endpoint returns 404.

**Fix:** contact an admin to enable `knowledge_graph` for your user.

**Cause 2 — wrong CLI path.** Make sure you are on `glab` v1.94.0+:

```shell
glab --version
glab orbit remote --help
```

If `glab orbit` is not recognised, upgrade `glab`.

## Exit `3` — not authenticated

**Cause:** Missing or expired `glab` auth.

**Fix:**

```shell
glab auth status
glab auth login    # if expired
```

## Exit `4` — `No Knowledge Graph enabled namespaces available`

**Cause:** Your user has the feature flag enabled but belongs to no top-level
group that has Orbit turned on.

**Fix:** an Owner of at least one top-level group you belong to must turn
Orbit on via **Orbit > Configuration** in the GitLab UI.

## Exit `5` — rate limited

**Cause:** Rate limit (`orbit_query`) exceeded.

**Fix:** back off and reduce churn. For agent-driven bulk work, lower `limit`,
add a short sleep between queries, or fold work into a single
aggregation/traversal query.

## Exit `1` — generic error

The most common causes:

- Malformed JSON request body (run `jq . /tmp/q.json` to validate).
- Unreachable hostname (check `glab auth status`).
- Network or TLS failure.

Re-run with `--format raw` and inspect stderr for details.

## Empty result body

**Cause:** Usually the query returned no rows. Confirm with a known-good probe:

Put the request body in `/tmp/q-min.json`:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "node": {
      "id": "p",
      "entity": "Project",
      "filters": {
        "full_path": {"op": "starts_with", "value": "gitlab-org/"}
      }
    },
    "limit": 1
  }
}
```

```shell
glab orbit remote query --format raw /tmp/q-min.json
```

If this returns a result, the connection works and your other query likely
has no matches.

## Validation errors (HTTP 400, exit `1`)

**Cause:** Query did not match the DSL JSON Schema. Common culprits:

- Using `node` (singular) with `aggregation` / `path_finding`
  (they require `nodes`, plural).
- Using `nodes` (plural) with `neighbors` or single-node `traversal`
  (they require `node`, singular).
- Multi-node `traversal` (uses `nodes`) without at least 2 nodes and 1 relationship.
- `query_type: "aggregation"` without any `aggregations` entries.
- `max_hops` or `max_depth` > 3 (server-enforced ceiling).
- `cursor.after` reused after changing the query (the token is bound to the exact query that issued it).
- `allowlist rejected` / `not valid under 'oneOf'` on a `columns` entry —
  the column name is not in the entity's allowlist. Run
  `glab orbit remote schema <Entity>` to get the valid column list.

**Fix:** validate against the live DSL schema, which is authoritative and
always current. Fetch it with `glab orbit remote dsl`:

```shell
glab orbit remote dsl
```

Full field reference in [`query_language.md`](query_language.md).

## Service unavailable

`glab orbit remote status` reports the GKG service health. On GitLab 19.1+
(after [!241580](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/241580))
the underlying `GET /api/v4/orbit/status` endpoint returns a nested wrapper —
`{ "user": { "available": … }, "system": { …health… } | null }` — and always
responds `200`; `system` is `null` when your user lacks Knowledge Graph access.
The CLI unwraps this for you: it prints the `system` health object (same output
as the old flat shape on pre-19.1 instances), and on `user.available == false`
(or a `404` when the feature flag is off) it exits `2`
([exit codes](#cli-exit-codes)). An exit `2` from `status` therefore means
"no Knowledge Graph access" — work through
[Exit `2`](#exit-2--feature-flag-is-off-or-wrong-subcommand).

If the printed `system` health shows any component unhealthy, retry with
exponential backoff. If persistent, escalate in the team Slack channel.

## Iteration budget rules

A single user question should resolve in at most 5 query attempts (see
[`SKILL.md`](../SKILL.md)). The supporting rules:

1. **Each retry must change something material.** Tweaking only `limit` or
   `columns` does not count as progress; changing `entity`, the relationship
   type, or a `filter` does.
2. **Validation errors (HTTP 400) count toward the budget.** Three consecutive
   validation errors on the same query shape means the shape is wrong — stop,
   re-read the relevant recipe, and pick a different shape.
3. **Empty results are not necessarily a failure.** Confirm with the
   [known-good probe](#empty-result-body) before assuming the query is wrong.
4. **When you give up, give up loudly.** Tell the user: "Orbit did not return
   an answer after 5 attempts. The query shapes I tried were: [...]. Suggested
   next steps: [...]." A clear give-up is more useful than silently inflating a
   partial result.

Cost grows linearly in attempts, both in CLI shell-out time and in agent
context. A hard cap is cheaper than an ambiguous answer.
