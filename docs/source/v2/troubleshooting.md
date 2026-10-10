---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Find and fix common errors in the GitLab Orbit CLI, the local graph, and server queries.
title: Troubleshooting GitLab Orbit
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Beta

{{< /details >}}

{{< history >}}

- GitLab Orbit Remote [introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- GitLab Orbit Remote [changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- GitLab Orbit Local [introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an experiment.
- GitLab Orbit Local [changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to beta in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

When you use GitLab Orbit, you might encounter the following issues.

## CLI install and update

### Error: `unrecognized subcommand`

You might get an error like this one.

```plaintext
error: unrecognized subcommand 'mcp'
```

This issue occurs when your `orbit` binary is older than the release that added the command.

To resolve this issue, update the binary:

```shell
glab orbit --update
```

If you installed `orbit` directly, run the installer again.
For more information, see [CLI](cli.md).

### Error: glibc version not found

On Linux, `orbit` might fail to start with an error like this one.

```plaintext
orbit: /lib64/libc.so.6: version `GLIBC_2.28' not found (required by orbit)
```

This issue occurs when you install the default glibc build on a distribution with an earlier glibc.
The glibc build needs glibc 2.28 or later, for example RHEL 8, Debian 10, or Ubuntu 20.04.

To resolve this issue, install the static musl build:

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --libc musl
```

You can also install the `@gitlab/orbit` npm package. It always uses the musl build on Linux.

## Local graph

### Error: `no local graph found`

You might get an error like this one.

```plaintext
Error: no local graph found at /Users/you/.gitlab/orbit/graph.duckdb. Index a repository first (`orbit index` inside it, or the `index` MCP tool).
```

This issue occurs when you did not index the repository yet,
or when the path that you gave with `--db` does not exist.
Earlier versions report this error as `Table 'Definition' does not exist`.

To resolve this issue, index the repository:

```shell
glab orbit index /path/to/your/repo
```

### Error: `current commit is not indexed`

`glab orbit repo-map` might exit with code 1 and an error like this one.

```plaintext
current commit 1a2b3c4 is not indexed in the local graph
```

This issue occurs when you did not index the commit that is checked out now.
`grep` and `context` index the checkout first, so they do not show this error.

To resolve this issue, index the checkout, then run `repo-map` again:

```shell
glab orbit index .
```

### Note: `not inside a git checkout`

When you run `glab orbit sql` outside a Git checkout, it prints
`note: . is not inside a git checkout; querying every indexed commit (as with --all)`.
This note is not an error. The query runs on all indexed repositories.

### Error: `The local graph is busy`

A command might pause, then fail with an error like this one.

```plaintext
The local graph is busy: orbit (PID 4242) is using it. Wait for it to finish, then try again.
```

Earlier versions report `IO Error: Could not set lock on file`.

This issue occurs when another `orbit` process holds the write lock on the DuckDB file.
The CLI tries again for a short time, then fails.

To resolve this issue, wait for the other process to finish.
If it does not finish, stop it with the PID from the error.

```shell
kill 4242
```

Then run your command again.

## Server queries

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
- Status: Beta

{{< /details >}}

Server commands such as `glab orbit query` exit with a code that tells you the cause.
For the full list, see [exit codes](cli.md#exit-codes).

### Error: `no Orbit credential found`

A server command might exit with code 1 and the message `no Orbit credential found`.

This issue occurs when you run `orbit` without `glab` and the CLI finds no token.

To resolve this issue, do one of these:

- Sign in with `glab auth login`.
- Set `GITLAB_TOKEN`. For an instance other than GitLab.com, also set `GITLAB_URL`.
- Run the command through `glab orbit`. It passes your `glab` credential to the binary.

### Error: `compile_error` for a JSON query

A JSON query might fail with an error like this one.

```plaintext
Orbit API error (HTTP 400): {"code":"compile_error","message":"schema violation: Orbit query syntax at line 1, column 1: expected Statement ..."}
```

This issue occurs when the per-user `orbit_gql_queries` feature flag is on for you.
With the flag on, the query endpoint accepts only GQL text and rejects JSON queries.

To resolve this issue, send the query as GQL text. For example:

```gql orbit-query
MATCH (p:Project {full_path: 'gitlab-org/gitlab'})
RETURN p.name, p.full_path
LIMIT 1
```

To use JSON queries again, ask your GitLab administrator to turn off the flag for you.
For more information, see [REST API](queries/api.md).

### Exit code 2: endpoint not available

`glab orbit` might exit with code 2 and the message `Orbit endpoint not available`.

This issue occurs when GitLab Orbit is not available on your GitLab instance.

To resolve this issue, ask your GitLab administrator to connect the instance to a GitLab Orbit server.
For more information, see [GitLab Orbit on GitLab Self-Managed](self-managed/_index.md).

If the message is `Project not found` or `Group not found`, the path or ID is wrong, or you are not a member.

### Exit code 3: not authenticated

`glab orbit` might exit with code 3 and the message `not authenticated`.

This issue occurs when you are not signed in with the GitLab CLI, or your token expired.

To resolve this issue, sign in again:

```shell
glab auth login
```

### Exit code 4: access denied

`glab orbit` might exit with code 4 and the message `Orbit access denied`.

This issue occurs when no top-level group that you belong to has GitLab Orbit turned on.

To resolve this issue, ask an Owner of the top-level group to
[turn on GitLab Orbit for the group](_index.md#turn-on-gitlab-orbit-for-your-group).

### Exit code 5: rate limited

`glab orbit` might exit with code 5 and the message `rate limited`.

This issue occurs when you send too many requests in a short time.

To resolve this issue, wait for the time in the `Retry-After` response header.
To send fewer requests, combine small queries into one aggregation query.

### Results are missing recent data

A server query might not return a project, a file, or a recent change.

This issue occurs when the indexer has not finished indexing the group or project yet.
The server graph shows your data as of the last index cycle.

To resolve this issue, check the indexing progress:

```shell
glab orbit graph-status --full-path <group_or_project_path>
```

Wait for indexing to finish, then send the query again.
`glab orbit status` shows only the health of the cluster. It does not show indexing progress.

## Tokens and service accounts

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
- Status: Beta

{{< /details >}}

### Error: `insufficient_scope` on the MCP endpoint

A connection to the MCP endpoint might fail with `insufficient_scope`.

This issue occurs when the personal access token or OAuth token has none of the
`mcp_orbit`, `mcp`, or `read_api` scopes.

To resolve this issue, create a token with the `mcp_orbit` or `read_api` scope,
or authenticate again to grant the `mcp_orbit` scope.

### Error: `403 Forbidden - No Orbit enabled namespaces available`

A query from a service account might fail with this error.

This issue occurs when the account does not have the Reporter role or higher in a group where GitLab Orbit is on.

To resolve this issue, add the account with the Reporter role or higher to a group where GitLab Orbit is on.
For more information, see [service accounts](security.md#service-accounts).

### Error: `403 Forbidden` with no message

A query from a service account might fail with `403 Forbidden` and no other message.

This issue occurs when no group of the account has a license for GitLab Orbit.

To resolve this issue, add the account to a group where GitLab Orbit is on.
The top-level group must have a Premium or Ultimate subscription.
If you added the account recently, GitLab can keep the earlier result for a few minutes.
Wait a few minutes, then send the query again.

### Results have no security data

Results for a service account might not include security entities.

This issue occurs when the account has the Reporter role.
GitLab removes security entities from the results and from aggregate counts.

To resolve this issue, give the account the Security Manager role in the group.

## GitLab Duo

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
- Status: Beta

{{< /details >}}

### Custom flow does not use GitLab Orbit

An agent in a custom flow runs but does not call GitLab Orbit.
The agent might say that it has no GitLab Orbit tools. The flow shows no error.

This issue occurs when the flow configuration does not list the GitLab Orbit tools.
It also occurs when the user who triggered the flow did not turn on GitLab Orbit.

<!-- vale orbit.StandaloneProductName = NO -->

To resolve this issue, add the tools to the flow `toolset`. Then ask the user to
select **Use Orbit in GitLab Duo** and **Other Foundational Agents** in their preferences.

<!-- vale orbit.StandaloneProductName = YES -->

For more information, see [GitLab Duo Agent Platform](agents/duo.md).

## Related topics

- [CLI](cli.md)
- [Security](security.md)
- [How GitLab Orbit works](how-it-works.md)
- [MCP](agents/mcp.md)
