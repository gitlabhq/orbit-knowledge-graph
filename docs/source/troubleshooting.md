---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Troubleshoot common errors in Orbit Local and Orbit Remote.
title: Troubleshoot GitLab Orbit
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/661) in GitLab 19.1.

{{< /history >}}

Use this page to troubleshoot errors you might encounter with
[Orbit Local](local/_index.md) or [Orbit Remote](remote/_index.md).

## Orbit Local

GitLab Orbit Local errors occur when running the `orbit` binary directly or through
`glab orbit local`.

### `no local graph found`

**Symptoms:**

```plaintext
Error: no local graph found at ~/.orbit/graph.duckdb. Run `orbit index` first.
```

**Cause:** The repository has not been indexed yet, or the `--db` path you
specified does not exist. On older versions of Orbit Local, this error was
reported as `Table 'Definition' does not exist`.

**Resolution:** Index the repository first:

```shell
glab orbit local index /path/to/your/repo
```

### `IO Error: Could not set lock on file`

**Symptoms:** A command appears to pause briefly, then fails with an error
containing `Could not set lock on file`.

**Cause:** Another `orbit` process is already running and holds the DuckDB
write lock. GitLab Orbit retries automatically with exponential backoff, but fails if
the lock is not released within the retry window.

**Resolution:** Wait for the other process to finish, or stop it:

```shell
pkill orbit
```

Then retry your command.

### `list_contains source_tags`

**Symptoms:** A query fails with an error containing `list_contains source_tags`.

**Cause:** A known bug triggered by certain filter combinations that include
the `source_tags` property.

**Resolution:** Remove any `source_tags` filter from your query and retry.

### `error: unrecognized subcommand 'mcp'`

**Symptoms:**

```plaintext
error: unrecognized subcommand 'mcp'
```

**Cause:** The `orbit mcp serve` subcommand is not yet implemented. MCP support
for Orbit Local is on the roadmap but is not available in the current release.

**Resolution:** Use one of the [supported access methods](local/_index.md).

## Orbit Remote

GitLab Orbit Remote errors occur when running `glab orbit remote` commands.
GitLab Orbit Remote requires GitLab Premium or Ultimate and the `knowledge_graph`
feature flag to be enabled on your instance.

### Exit code 2

**Symptoms:** `glab orbit remote` commands exit with code 2.

**Cause:** The `knowledge_graph` feature flag is not enabled for your
namespace or instance.

**Resolution:** Contact your GitLab administrator to enable the
`knowledge_graph` feature flag for your namespace.

### Exit code 3

**Symptoms:** `glab orbit remote` commands exit with code 3.

**Cause:** You are not authenticated with the GitLab CLI.

**Resolution:** Log in:

```shell
glab auth login
```

### `insufficient_scope` on the MCP endpoint

**Symptoms:** Connecting to the GitLab Orbit MCP endpoint fails with
`insufficient_scope`.

**Cause:** The personal access token or OAuth token does not include the
`mcp_orbit` scope. The `read_api` scope alone is not sufficient for the MCP
transport.

**Resolution:** Create a new token with the `mcp_orbit` scope, or
re-authenticate to grant the additional scope.
