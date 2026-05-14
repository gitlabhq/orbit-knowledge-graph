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

```bash
glab --version
glab orbit remote --help
```

If `glab orbit` is not recognised, upgrade `glab`.

## Exit `3` — not authenticated

**Cause:** Missing or expired `glab` auth.

**Fix:**

```bash
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

```bash
cat > /tmp/q-min.json <<'JSON'
{"query": {"query_type": "traversal", "node": {"id": "p", "entity": "Project"}, "limit": 1}}
JSON
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
- `cursor.offset + cursor.page_size > limit`.

**Fix:** validate against the live DSL schema, which is authoritative and
always current. Fetch it from `/api/v4/orbit/schema/dsl`:

```bash
glab orbit remote dsl
```

Full field reference in [`query_language.md`](query_language.md).

## `invalid character '@'` (JSON parse error, not a CLI bug)

**Symptom.** `glab orbit remote query /tmp/q.json` returns
`invalid character '@' looking for beginning of value`, while
`jq . /tmp/q.json` appears to validate the same file.

**Cause.** `glab orbit remote query` does **not** preprocess the request
body — bytes inside JSON string literals (including `@` in email
addresses like `user@example.com`, Ruby `@instance_var` references, or
`@version` annotations) are forwarded to the API verbatim. The error is
a genuine JSON syntax failure from Go's `encoding/json`, almost always
one of:

- **A stray `@` outside a string literal.** Usually an unrendered
  template placeholder — e.g. `{"query": @variable}` or
  `{"value": @user_email}` left in the file by a shell or templating
  step. The `@` is not in quotes, so it is not valid JSON.
- **A UTF-8 BOM at the start of the file.** Some editors save files as
  "UTF-8 with BOM"; the first three bytes (`EF BB BF`) make the error
  surface as `invalid character 'ï'`, not `'@'`, but the failure mode
  is the same. `jq` silently tolerates a leading BOM, which is why the
  same file "validates with `jq`" but fails through `glab`.

**Fix.** Inspect the body for either condition before retrying:

```bash
# Catch stray `@` outside strings — should print nothing if the file
# is clean. Quoted `@`s inside strings are fine and will not match.
grep -nE '(^|[^"\\])@[A-Za-z_]' /tmp/q.json

# Strip a leading BOM if your editor added one.
sed -i '1s/^\xEF\xBB\xBF//' /tmp/q.json
```

Recent `glab` builds wrap this error with a hint pointing at the
offending byte and at `jq`, and strip a leading BOM before parsing.
Older builds surface the bare stdlib message; the underlying fix is
the same — clean the JSON body.

**Do not** route around this with `glab api orbit/query` — the body is
still invalid JSON and the API will reject it the same way. Fix the
file.

## Service unavailable

If `glab orbit remote status` shows any component unhealthy, retry with
exponential backoff. If persistent, escalate in the team Slack channel.
