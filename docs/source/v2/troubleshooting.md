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

{{< history >}}

- Remote graph:
  - [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
  - [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
  - [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
  - [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.
- Local graph:
  - [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an experiment.
  - [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to beta in GitLab 19.1.
  - Generally available in GitLab 19.5.

{{< /history >}}

## CLI install and update

### `glab orbit` is not recognized

`glab` shows an unknown command error for `orbit` or one of its commands, because your `glab` version is earlier than 1.115.
[Update `glab`](https://docs.gitlab.com/cli/), then run `glab orbit setup`.

### Error: `unrecognized subcommand`

```plaintext
error: unrecognized subcommand 'repo-map'
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

## GitLab Orbit setup and agent connection

<!-- vale orbit.StandaloneProductName = NO -->

### The Orbit settings page is missing

The **Orbit** settings page appears only for users with the Owner role for a top-level group.
If you do not have the Owner role, or you are in a subgroup, ask a user with the Owner role for the top-level group to turn on GitLab Orbit.
On GitLab Self-Managed and GitLab Dedicated, an administrator can also turn it on in the **Admin** area.

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
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

### `glab orbit` exits with code 2, 3, 4, or 5

| Exit code | Message | Cause | Resolution |
|-----------|---------|-------|------------|
| `2` | `Orbit endpoint not available` | GitLab Orbit is not available on your instance. | Ask your administrator to [set up GitLab Orbit](self-managed/_index.md). |
| `2` | Ends with `not found`, such as `Project not found` or `Namespace not found` | The path or ID is wrong, or you are not a member. | Check the path or ID. |
| `3` | `not authenticated` | You are not signed in, or your token expired. | Run `glab auth login`. |
| `4` | `Orbit access denied` | No top-level group that you belong to has GitLab Orbit turned on. | Ask a user with the Owner role for the group to [turn on GitLab Orbit](quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group). |
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

The query uses the deprecated JSON query domain-specific language (DSL). Rewrite it in [GQL](queries/query-language.md), for example:

```gql orbit-query
MATCH (p:Project {full_path: 'gitlab-org/gitlab'})
RETURN p.name, p.full_path
LIMIT 1
```

### Error: `GitLab credits exhausted`

The query fails with HTTP `402`, and `glab orbit query` exits with code 1.
The free trial of the top-level group ended. To keep using GitLab Orbit, [contact sales](https://about.gitlab.com/sales/).
For information about GitLab Orbit pricing and usage, see [GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/).

If the message is `Unable to verify GitLab credits`, GitLab could not check whether the group can run queries.
Wait a few minutes, then send the query again.

### Error: `This group is not eligible for a GitLab Orbit trial`

The group does not meet the conditions for a trial, or already has GitLab Orbit.
To get access to GitLab Orbit, [contact sales](https://about.gitlab.com/sales/). For more information, see [start a GitLab Orbit trial](trial.md).

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
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

These errors come from the REST API and the MCP endpoint. The CLI reports every HTTP `403` as exit code 4.

### Error: `insufficient_scope` on the MCP endpoint

The token has none of the `mcp_orbit`, `mcp`, `read_api`, `api`, or `ai_workflows` scopes.
Create a token with the `mcp_orbit` or `read_api` scope, or sign in again to grant the `mcp_orbit` scope.

### Error: `403 Forbidden` for a service account

A query from a service account might fail with `403 Forbidden - No Orbit enabled namespaces available`.

This issue occurs when the account does not have the Reporter role or higher in a group where
GitLab Orbit is turned on.

To resolve this issue, add the account to a group where GitLab Orbit is turned on,
with the Reporter role or higher.
For more information, see [service accounts](security.md#service-accounts).

### Error: `403 Forbidden` with no message

A query from a service account might fail with `403 Forbidden` and no other message.

This issue occurs when no group of the service account has GitLab Orbit available.

To resolve this issue, add the account to a group where GitLab Orbit is turned on.
If you added the account recently, wait a few minutes, then send the query again.

### Results have no security data

Results from a service account might not include security entities.

This issue occurs when the account has the Reporter role. GitLab removes security entities from results
and aggregate counts.

To resolve this issue, give the account the Security Manager role for the group.

## GitLab Duo

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

### Custom flow does not use GitLab Orbit

An agent in a custom flow does not call GitLab Orbit, and might say that it has no GitLab Orbit tools.

<!-- vale orbit.StandaloneProductName = NO -->

The flow configuration does not list the GitLab Orbit tools, or the user who triggered the flow
cleared **Other Foundational Agents** in the **GitLab Orbit** section of their preferences.
[Add the tools to the flow](agents/duo.md#use-gitlab-orbit-in-a-custom-flow), and ask the user to select the option again.

<!-- vale orbit.StandaloneProductName = YES -->

## Local graph

These issues affect the [local graph](cli.md#local-graph).

### Error: `no local graph found`

```plaintext
Error: no local graph found. Run `glab orbit index` first.
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
- [Local graph](cli.md#local-graph)
- [Security](security.md)
- [GitLab Orbit architecture](how-it-works.md)
- [MCP](agents/mcp.md)
