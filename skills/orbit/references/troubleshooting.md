# Orbit skill troubleshooting

First-run setup and the common `glab orbit` errors, organised by exit code. See [`SKILL.md`](../SKILL.md) for usage.

## First-run setup

Read this when a command fails with "command not found", an auth error, or a feature-flag exit code. A working setup does not need these checks on every call.

1. glab v1.117.0 or later, with the Orbit commands:

   ```sh
   glab --version
   glab orbit help
   ```

   Install or upgrade glab with [the install instructions](https://gitlab.com/gitlab-org/cli#installation).

2. glab authenticated to GitLab:

   ```sh
   glab auth status
   glab auth login    # if not authenticated
   ```

3. Orbit Remote: the feature flag is on for your namespace:

   ```sh
   glab orbit graph-status --full-path GROUP_NAMESPACE
   ```

   Exit 0 means ready to query. Exit 2 means the flag is off; ask your GitLab admin.

4. Orbit Local: no server needed:

   ```sh
   glab orbit --install --yes
   glab orbit index /path/to/your/repo
   ```

   If `glab orbit` cannot find the managed binary, add `"$HOME/.config/glab-cli/bin/"` to `PATH`.

## CLI exit codes

| Exit | HTTP | Meaning                                                      |
|------|------|--------------------------------------------------------------|
| `0`  | 2xx  | Success.                                                     |
| `1`  | n/a  | Generic error (parse error, IO error, malformed body).       |
| `2`  | 404  | Orbit endpoint unavailable (typically: feature flag is off). |
| `3`  | 401  | Not authenticated.                                           |
| `4`  | 403  | Access denied (no Knowledge Graph enabled namespaces).       |
| `5`  | 429  | Rate limited.                                                |

`glab orbit query --response-format raw` prints the full JSON error payload when the exit code alone is not enough.

## Exit 2: feature flag off, or old glab

Cause 1: the `knowledge_graph` feature flag is off for your user, so every endpoint returns 404. Fix: ask an admin to enable it.

Cause 2: glab is older than v1.117.0 and does not know the flat `glab orbit` commands. Fix: check `glab --version` and upgrade.

## Exit 3: not authenticated

Cause: missing or expired glab auth. Fix:

```shell
glab auth status
glab auth login    # if expired
```

## Exit 4: no Knowledge Graph enabled namespaces

Cause: your user has the feature flag, but belongs to no top-level group with Orbit turned on. Fix: an Owner of one of your top-level groups turns Orbit on under **Orbit > Configuration** in the GitLab UI.

## Exit 5: rate limited

Cause: the `orbit_query` rate limit. Fix: back off. For bulk agent work, lower `limit`, add a short sleep between queries, or fold the work into one aggregation or traversal query.

## Exit 1: generic error

Common causes: a malformed JSON body (validate with `jq . /tmp/q.json`), an unreachable hostname (check `glab auth status`), or a network or TLS failure. Re-run with `--response-format raw` and read stderr.

## Empty result body

Usually the query matched no rows. Confirm with a known-good probe in `/tmp/q-min.json`:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "filters": {
        "full_path": {"starts_with": "gitlab-org/"}
      }
    }],
    "limit": 1
  }
}
```

```shell
glab orbit query --response-format raw /tmp/q-min.json
```

If this returns a row, the connection works and your other query has no matches.

## Validation errors (HTTP 400, exit 1)

The query did not match the DSL JSON Schema. Common causes:

- `node` (singular) instead of the `nodes` array. Wrap the selector: `"nodes": [{...}]`.
- `neighbors` or single-node `traversal` with more than one entry in `nodes`.
- Multi-node `traversal` without at least two nodes and one relationship.
- `aggregation` without any `aggregations` entries.
- `hops` upper bound or `max_depth` above 3, the server-enforced ceiling.
- `cursor.after` reused after the query changed. The token is bound to the exact query that issued it.
- `allowlist rejected` or `not valid under 'oneOf'` on a `columns` entry. The column is not in the entity's allowlist. Run `glab orbit ontology <Entity>` for the valid list.

Fix: validate against the live schema from `glab orbit dsl`. Full field reference in [`query_language.md`](query_language.md).

## Service unavailable

`glab orbit status` reports service health. It prints the `system` health object and exits 2 when your user has no Orbit access (see [Exit 2](#exit-2-feature-flag-off-or-old-glab)). On GitLab 19.1 and later the API wraps the response as `{ "user": {...}, "system": {...} | null }` and always returns 200. The CLI unwraps it for you.

If any component is unhealthy, retry with exponential backoff. If it persists, escalate in the team Slack channel.

## Iteration budget rules

Resolve a single user question in at most 5 query attempts (see [`SKILL.md`](../SKILL.md)). The supporting rules:

1. Each retry must change something material. Changing only `limit` or `columns` is not progress. Changing `entity`, the relationship type, or a `filter` is.
2. Validation errors (HTTP 400) count toward the budget. Three consecutive validation errors on one query shape mean the shape is wrong. Stop, re-read the relevant recipe, and pick a different shape.
3. Empty results are not always a failure. Confirm with the [known-good probe](#empty-result-body) before you assume the query is wrong.
4. When you give up, give up loudly. Tell the user: "Orbit did not return an answer after 5 attempts. The query shapes I tried were: [...]. Suggested next steps: [...]." A clear give-up is more useful than a silently inflated partial result.

Cost grows linearly in attempts, in CLI shell-out time and in agent context. A hard cap is cheaper than an ambiguous answer.
