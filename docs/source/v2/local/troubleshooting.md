---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Troubleshoot common errors in GitLab Orbit Local.
title: Troubleshooting GitLab Orbit Local
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

When working with GitLab Orbit Local, or the `orbit` binary directly, you might encounter the
following issues.

## Error: `no local graph found`

You might get an error that states:

```plaintext
Error: no local graph found at ~/.gitlab/orbit/graph.duckdb. Run `orbit index` first.
```

This issue occurs when the repository is not indexed yet, or when the `--db` path you specified
does not exist.
On earlier versions of GitLab Orbit Local, this error was reported as
`Table 'Definition' does not exist`.

To resolve this issue, index the repository:

```shell
glab orbit index /path/to/your/repo
```

## Error: `Could not set lock on file`

A command might pause briefly, then fail with an error that contains
`IO Error: Could not set lock on file`.

This issue occurs when another `orbit` process is already running and holds the DuckDB write
lock.
GitLab Orbit retries automatically, but fails if the lock is not
released in the retry window.

To resolve this issue, wait for the other process to finish, or stop it:

```shell
pkill orbit
```

Then run your command again.

## Error: `list_contains source_tags`

A query might fail with an error that contains `list_contains source_tags`.

This issue occurs because of a known bug that certain filter combinations trigger, including the
`source_tags` property.

The workaround is to remove any `source_tags` filter from your query and run the query again.

## Error: `unrecognized subcommand 'mcp'`

You might get an error that states:

```plaintext
error: unrecognized subcommand 'mcp'
```

This issue occurs when your installed `orbit` binary version is older than the GitLab release
that introduced the `mcp` command.

To resolve this issue, update the managed binary, then start the stdio MCP server:

```shell
glab orbit --update
glab orbit mcp serve
```

If you installed `orbit` directly, run the installer again.
For more information, see the [GitLab Orbit CLI instructions](access/cli.md#install).
