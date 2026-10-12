---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Find and fix common errors in GitLab Orbit queries, the GitLab Orbit CLI, and the local graph.
title: Troubleshooting GitLab Orbit
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

<!-- TODO(GA): set the GA release and link the GA work item in the history line. -->
{{< history >}}

- GitLab Orbit Remote [introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- GitLab Orbit Remote [changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- GitLab Orbit Remote [enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- GitLab Orbit Remote [feature flag `knowledge_graph` removed](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5.
- GitLab Orbit Local [introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an experiment.
- GitLab Orbit Local [changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to beta in GitLab 19.1.
- Generally available in GitLab 19.x.

{{< /history >}}

## CLI install and update

### `glab orbit` is not recognized

`glab` shows an unknown command error for `orbit`, because `glab` is older than 1.117.
[Update `glab`](https://docs.gitlab.com/cli/), then run `glab orbit --install`.

### Error: `unrecognized subcommand`

```plaintext
error: unrecognized subcommand 'mcp'
```

Your `orbit` binary is older than the release that added the command.
Run `glab orbit --update`. If you installed `orbit` without `glab`, run the [installer](cli.md#other-install-methods) again.

### Error: glibc version not found

On Linux, `orbit` might fail to start:

```plaintext
orbit: /lib64/libc.so.6: version `GLIBC_2.28' not found (required by orbit)
```

The default build needs glibc 2.28 or later, for example RHEL 8, Debian 10, or Ubuntu 20.04.
Install the static musl build instead:

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --libc musl
```

The `@gitlab/orbit` npm package always uses the musl build on Linux.

## Turn on GitLab Orbit and connect your agent

<!-- vale orbit.StandaloneProductName = NO -->

### The Orbit settings page is missing

The **Orbit** settings page appears only for the Owner of a top-level group on GitLab.com.
If you are not an Owner, or you are in a subgroup, ask an Owner of the top-level group to turn on GitLab Orbit.
On GitLab Self-Managed and GitLab Dedicated, an administrator turns it on in the **Admin** area.

<!-- vale orbit.StandaloneProductName = YES -->

### Your agent does not use GitLab Orbit

The agent answers with its usual tools and never runs `glab orbit`.

- Check that the skill is installed in `~/.agents/skills/orbit`. For Claude Code, also check the
  `~/.claude/skills/orbit` link. For more information, see [GitLab Orbit skill](agents/_index.md#gitlab-orbit-skill).
- Start a new session of your agent. A session that started before setup does not load the skill.
- Start the prompt with `Using GitLab Orbit`.

To see what setup configured, run `glab orbit setup --dry-run`.

## Remote queries

{{< details >}}

- Tier: Premium, Ultimate
- Add-on: GitLab Orbit
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

### Exit codes 2 to 5

| Exit code | Message | Cause | Resolution |
|-----------|---------|-------|------------|
| `2` | `Orbit endpoint not available` | GitLab Orbit is not available on your instance. | Ask your administrator to [set up GitLab Orbit](self-managed/_index.md). |
| `2` | Ends with `not found`, such as `Project not found` or `Namespace not found` | The path or ID is wrong, or you are not a member. | Check the path or ID. |
| `3` | `not authenticated` | You are not signed in, or your token expired. | Run `glab auth login`. |
| `4` | `Orbit access denied` | No top-level group that you belong to has GitLab Orbit turned on. | Ask an Owner of the group to [turn on GitLab Orbit](quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group). |
| `5` | `rate limited` | You sent too many requests in a short time. | Wait for the time in the `Retry-After` header. Combine small queries into one aggregation. |

For all exit codes, see [exit codes](cli.md#exit-codes).

### Error: `no Orbit credential found`

`orbit` without `glab` exits with code 1 when it finds no token. To fix it, do one of these:

- Run the command through `glab orbit`, which passes your `glab` credential.
- Sign in with `glab auth login`.
- Set `GITLAB_TOKEN`. For an instance other than GitLab.com, also set `GITLAB_URL`.

### Error: `compile_error` for a JSON query

```plaintext
Orbit API error (HTTP 400): {"code":"compile_error","message":"schema violation: Orbit query syntax at line 1, column 1: expected Statement ..."}
```

The query uses the deprecated JSON query DSL. Rewrite it in the [Cypher query language](queries/cypher.md), for example:

```gql orbit-query
MATCH (p:Project {full_path: 'gitlab-org/gitlab'})
RETURN p.name, p.full_path
LIMIT 1
```

### Error: `GitLab credits exhausted`

The query fails with HTTP `402`, and `glab orbit query` exits with code 1.
The top-level group that GitLab Orbit bills has no GitLab Credits left, or its free trial ended.
Buy more [GitLab Credits](https://docs.gitlab.com/subscriptions/gitlab_credits/) for the group.

If the message is `Unable to verify GitLab credits`, GitLab could not check the GitLab Credits of the group.
Wait a few minutes, then send the query again.

### Error: `This group is not eligible for a GitLab Orbit trial`

The group does not meet the conditions for a trial, or already has the GitLab Orbit add-on.
To buy the add-on, [contact sales](https://about.gitlab.com/sales/). For more information, see [start a GitLab Orbit trial](trial.md).

### Error: `billing_namespace_required`

<!-- vale orbit.StandaloneProductName = NO -->

The query fails with HTTP `422` and `Select a billing namespace for Orbit to attribute usage.`
`glab orbit query` exits with code 1.

On GitLab.com, you are a member of more than one top-level group with GitLab Orbit turned on,
and you have not selected the group that GitLab Orbit bills.
In your preferences, select a group in **Billable Orbit namespace**.

<!-- vale orbit.StandaloneProductName = YES -->

### Results are missing recent data

The indexer has not finished indexing the group or project yet.
Check the progress, and send the query again when indexing is complete:

```shell
glab orbit graph-status --full-path <my-group>
```

`glab orbit status` shows only the health of the service, not indexing progress.

## Tokens and service accounts

{{< details >}}

- Tier: Premium, Ultimate
- Add-on: GitLab Orbit
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

These errors come from the REST API and the MCP endpoint. The CLI reports every HTTP `403` as exit code 4.

### Error: `insufficient_scope` on the MCP endpoint

The token has none of the `mcp_orbit`, `mcp`, or `read_api` scopes.
Create a token with the `mcp_orbit` or `read_api` scope, or sign in again to grant the `mcp_orbit` scope.

### Error: `403 Forbidden - No Orbit enabled namespaces available`

The service account does not have the Reporter role or higher in a group where GitLab Orbit is on.
Add the account with the Reporter role or higher to such a group.
For more information, see [service accounts](security.md#service-accounts).

### Error: `403 Forbidden` with no message

No group of the service account has the GitLab Orbit add-on or an active trial.
Add the account to a group where GitLab Orbit is on.
If you added the account recently, wait a few minutes, then send the query again.

### Results have no security data

The service account has the Reporter role, so GitLab removes security entities from results and aggregate counts.
Give the account the Security Manager role in the group.

## GitLab Duo

{{< details >}}

- Tier: Premium, Ultimate
- Add-on: GitLab Orbit
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

### Custom flow does not use GitLab Orbit

An agent in a custom flow does not call GitLab Orbit, and might say that it has no GitLab Orbit tools.

<!-- vale orbit.StandaloneProductName = NO -->

The flow configuration does not list the GitLab Orbit tools, or the user who triggered the flow
cleared **Other Foundational Agents** under **Orbit in GitLab Duo** in their preferences.
[Add the tools to the flow](agents/duo.md#add-the-tools-to-the-flow), and ask the user to select the option again.

<!-- vale orbit.StandaloneProductName = YES -->

## Local graph

These issues affect [GitLab Orbit Local](local/_index.md).

### Error: `no local graph found`

```plaintext
Error: no local graph found at /Users/you/.gitlab/orbit/graph.duckdb. Index a repository first (`orbit index` inside it, or the `index` MCP tool).
```

You did not index the repository yet, or the path that you gave with `--db` does not exist.
Earlier versions report `Table 'Definition' does not exist`.
Run `glab orbit index /path/to/your/repo`.

### Error: `current commit is not indexed`

`glab orbit repo-map` exits with code 1:

```plaintext
Error: current commit 1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b is not indexed in the local graph
       run:  glab orbit index .
```

`repo-map` does not index the checkout for you. Run `glab orbit index .`, then run `repo-map` again.

### Note: `not inside a git checkout`

Outside a Git checkout, `glab orbit sql` prints
`note: . is not inside a git checkout; querying every indexed commit (as with --all)`.
The note is not an error. The query runs on all indexed repositories.

### Error: `The local graph is busy`

```plaintext
The local graph is busy: orbit (PID 4242) is using it. Wait for it to finish, then try again.
```

Another `orbit` process holds the write lock on the graph file. The CLI tries again for about 25 seconds, then fails.
Earlier versions report `IO Error: Could not set lock on file`.
Wait for the other process to finish. If it does not finish, stop it with the PID from the error, for example `kill 4242`.

### `grep` prints nothing

`glab orbit grep` prints nothing and exits with code 1 when nothing matches, as `rg` does.
Search for a shorter term, or remove options that narrow the search.

### Files marked `(edited since index)`

The file differs from the last commit, so `grep` shows no definitions for it.
Commit the change. The next `grep` indexes the new commit.

### Warning: `ignoring unsupported flag`

`grep` does not support the option, such as `--limit` from earlier versions.
It drops the option and reads its value as a path, so it then fails with an error like `no such path in the repository: 5`.
Remove the option. To narrow the search, give paths, or use `-g`, `-t`, or `--kind`.

### Repositories missing after an update

The update changed the format of the local graph, and the next `index` rebuilt the graph file.
Index your repositories again.

## Related topics

- [CLI](cli.md)
- [GitLab Orbit Local](local/_index.md)
- [Security](security.md)
- [How GitLab Orbit works](how-it-works.md)
- [MCP](agents/mcp.md)
