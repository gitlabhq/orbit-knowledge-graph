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
  - [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.
  - [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
  - [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
  - [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.
- Local graph:
  - [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an experiment.
  - [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to beta in GitLab 19.1.
  - Generally available in GitLab 19.5.

{{< /history >}}

When working with GitLab Orbit, you might encounter the following issues.

## CLI install and update

### Error: `unknown command` for `glab orbit`

You might get an error that states `unknown command` when you run `glab orbit` or one of its commands.

This issue occurs when your `glab` version is earlier than 1.115.

To resolve this issue, [update `glab`](https://docs.gitlab.com/cli/), then run `glab orbit setup`.

### Error: `unrecognized subcommand`

You might get an error that states:

```plaintext
error: unrecognized subcommand 'repo-map'
```

This issue occurs when your `orbit` binary is from a release earlier than the one that added the command.

To resolve this issue, run `glab orbit --update`.
If you installed `orbit` without `glab`, run the [installer](cli.md#other-install-methods) again.

### Error: `GLIBC_2.28' not found`

On Linux, `orbit` might fail to start with an error that states:

```plaintext
orbit: /lib64/libc.so.6: version `GLIBC_2.28' not found (required by orbit)
```

This issue occurs when the glibc version is earlier than 2.28.
The default build needs glibc 2.28 or later, for example RHEL 8, Debian 10, or Ubuntu 20.04.

To resolve this issue, install the static musl build:

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --libc musl
```

The `@gitlab/orbit` npm package always uses the musl build on Linux.

## GitLab Orbit setup and agent connection

<!-- vale orbit.StandaloneProductName = NO -->

### The Orbit settings page is missing

You might not see the **Orbit** settings page.
The page appears only for users with the Owner role for a top-level group.

This issue occurs when you do not have the Owner role, or when you are in a subgroup.

To resolve this issue, ask a user with the Owner role for the top-level group to turn on GitLab Orbit.
On GitLab Self-Managed and GitLab Dedicated, an administrator can also turn on GitLab Orbit in the **Admin** area.

<!-- vale orbit.StandaloneProductName = YES -->

### Your agent does not use GitLab Orbit

The agent answers with its usual tools and never runs `glab orbit`.

This issue occurs when the agent did not load the GitLab Orbit skill.

To resolve this issue:

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

`orbit` exits with code 1 with this error.

This issue occurs when you run `orbit` without `glab` and it finds no token.

To resolve this issue, do one of the following:

- Run the command through `glab orbit`, which passes your `glab` credential.
- Sign in with `glab auth login`.
- Set `GITLAB_TOKEN`. For an instance other than GitLab.com, also set `GITLAB_URL`.

### Error: `compile_error` for a JSON query

You might get an error that states:

```plaintext
Orbit API error (HTTP 400): {"code":"compile_error","message":"schema violation: Orbit query syntax at line 1, column 1: expected Statement ..."}
```

This issue occurs when the query uses the deprecated JSON query domain-specific language (DSL).

To resolve this issue, rewrite the query in [GQL](queries/query-language.md), for example:

```gql orbit-query
MATCH (p:Project {full_path: 'gitlab-org/gitlab'})
RETURN p.name, p.full_path
LIMIT 1
```

### Error: `GitLab credits exhausted`

The query fails with HTTP `402`, and `glab orbit query` exits with code 1.

This issue occurs when the free trial of the top-level group ended.

To resolve this issue, [contact sales](https://about.gitlab.com/sales/).
For more information, see [GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/).

If the message is `Unable to verify GitLab credits`, GitLab could not check whether the group can run queries.
The workaround is to wait a few minutes, then send the query again.

### Error: `This group is not eligible for a GitLab Orbit trial`

This issue occurs when the group does not meet the conditions for a trial, or already has GitLab Orbit.

To resolve this issue, contact sales.
For more information, see [start a GitLab Orbit trial](trial.md).

### Results are missing recent data

This issue occurs when the indexer has not finished indexing the group or project.

To resolve this issue, check the progress, and send the query again when indexing is complete:

```shell
glab orbit graph-status --full-path <my-group>
```

`glab orbit status` shows only the health of the service, not indexing progress.

## Tokens and service accounts

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

These errors come from the REST API and the Model Context Protocol (MCP) endpoint.
The CLI reports every HTTP `403` as exit code 4.

### Error: `insufficient_scope` on the MCP endpoint

This issue occurs when the token has none of the `mcp_orbit`, `mcp`, `read_api`, `api`, or `ai_workflows` scopes.

To resolve this issue, create a token with the `mcp_orbit` or `read_api` scope, or sign in again to grant the `mcp_orbit` scope.

### Error: `403 Forbidden` for a service account

A query from a service account might fail with `403 Forbidden - No Orbit enabled namespaces available`.

This issue occurs when the account does not have the Reporter, Security Manager, Developer, Maintainer,
or Owner role in a group where GitLab Orbit is turned on.

To resolve this issue, add the account to a group where GitLab Orbit is turned on, with one of these roles.
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

This issue occurs when the flow configuration does not list the GitLab Orbit tools, or when the user who triggered the flow
cleared **Other Foundational Agents** in the **GitLab Orbit** section of their preferences.

To resolve this issue, [add the tools to the flow](agents/duo.md#use-gitlab-orbit-in-a-custom-flow), and ask the user to select the option again.

<!-- vale orbit.StandaloneProductName = YES -->

## Local graph

These issues affect the [local graph](cli.md#local-graph).

### Error: `no local graph found`

You might get an error that states:

```plaintext
Error: no local graph found. Run `glab orbit index` first.
```

This issue occurs when the repository is not indexed, or when the path you gave with `--db` does not exist.
Earlier versions report `Table 'Definition' does not exist`.

To resolve this issue, run `glab orbit index <repository_path>`.

### Error: `current commit is not indexed`

`glab orbit repo-map` exits with code 1 with an error that states:

```plaintext
Error: current commit 1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b is not indexed in the local graph
       run:  glab orbit index .
```

This issue occurs when the local graph does not contain the current commit, because `repo-map` does not index the checkout for you.

To resolve this issue, run `glab orbit index .`, then run `repo-map` again.

### Error: `The local graph is busy`

You might get an error that states:

```plaintext
The local graph is busy: orbit (PID 4242) is using it. Wait for it to finish, then try again.
```

This issue occurs when another `orbit` process holds the write lock on the graph file.
The CLI tries again for about 25 seconds, then fails.
Earlier versions report `IO Error: Could not set lock on file`.

To resolve this issue, wait for the other process to finish.
If it does not finish, stop it with the process ID (PID) from the error, for example `kill 4242`.

### `grep` prints nothing

`glab orbit grep` prints nothing and exits with code 1.

This issue occurs when nothing matches, the same as with `rg`.

To resolve this issue, search for a shorter term, or remove options that narrow the search.

### Files marked `(edited since index)`

`grep` marks a file `(edited since index)` and shows no definitions for it.

This issue occurs when the file differs from the last commit.

To resolve this issue, commit the change.
The next `grep` indexes the new commit.

### Warning: `ignoring unsupported flag`

This issue occurs when you pass an option that `grep` does not support, such as `--limit` from earlier versions.
`grep` drops the option and reads its value as a path, so it then fails with an error like `no such path in the repository: 5`.

To resolve this issue, remove the option.
To narrow the search, give paths, or use `-g`, `-t`, or `--kind`.

### Repositories missing after an update

Repositories that you indexed before an update are missing from the local graph.

This issue occurs when the update changed the format of the local graph, and the next `index` rebuilt the graph file.

To resolve this issue, index your repositories again.

## Related topics

- [CLI](cli.md)
- [Security](security.md)
- [GitLab Orbit architecture](how-it-works.md)
- [MCP](agents/mcp.md)
