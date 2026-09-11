---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Troubleshoot common errors in GitLab Orbit Local.
title: Troubleshoot GitLab Orbit Local
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

When working with GitLab Orbit Local, or the `orbit` binary directly, you might encounter the following issues.

## `no local graph found`

Symptoms:

```plaintext
Error: no local graph found at ~/.orbit/graph.duckdb. Run `orbit index` first.
```

Cause: The repository has not been indexed yet, or the `--db` path you
specified does not exist. On older versions of GitLab Orbit Local, this error was
reported as `Table 'Definition' does not exist`.

Resolution: Index the repository first:

```shell
glab orbit local index /path/to/your/repo
```

## `IO Error: Could not set lock on file`

Symptoms: A command appears to pause briefly, then fails with an error
containing `Could not set lock on file`.

Cause: Another `orbit` process is already running and holds the DuckDB
write lock. GitLab Orbit retries automatically with exponential backoff, but fails if
the lock is not released within the retry window.

Resolution: Wait for the other process to finish, or stop it:

```shell
pkill orbit
```

Then retry your command.

## `list_contains source_tags`

Symptoms: A query fails with an error containing `list_contains source_tags`.

Cause: A known bug triggered by certain filter combinations that include
the `source_tags` property.

Resolution: Remove any `source_tags` filter from your query and retry.

## `error: unrecognized subcommand 'mcp'`

Symptoms:

```plaintext
error: unrecognized subcommand 'mcp'
```

Cause: Your installed `orbit` binary predates the GitLab Orbit Local MCP
server.

Resolution: Update the managed binary, then start the stdio MCP server:

```shell
glab orbit --update
glab orbit local mcp serve
```

If you installed `orbit` directly, rerun the installer from the
[GitLab Orbit CLI instructions](access/cli.md#install).
