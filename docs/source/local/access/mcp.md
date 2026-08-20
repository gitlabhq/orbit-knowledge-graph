---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect AI agents to your local graph with the GitLab Orbit Local MCP server.
title: GitLab Orbit Local MCP server
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/643) in GitLab 19.2 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

With the GitLab Orbit Local [Model Context Protocol](https://modelcontextprotocol.io/)
(MCP) server, you can securely connect AI tools and applications to your
local DuckDB graph. AI assistants like Claude Code, Codex, Cursor, and OpenCode
can then access your graph and write SQL queries
against it.

The GitLab Orbit Local MCP server is stateless, which means the server:

- Queries your local DuckDB graph, not a GitLab instance
- Does not cache results or persist query history
- Can respond to multiple clients independently

## Prerequisites

- Install either:
  - The [GitLab Orbit CLI](./cli.md) (`orbit`)
  - The [GitLab CLI](./glab.md) (`glab orbit`)

## Connect a client to the GitLab Orbit Local MCP server

The GitLab Orbit Local MCP server supports stdio transport. Arguments and commands
vary depending on the MCP client and your local environment.

### Connect Claude Code

Claude Code stores the MCP server configuration in one of three scopes. Choose the
scope that matches your use case.

| Scope | Availability | Stored in |
| ----- | ------------ | --------- |
| `local` (default) | You only, in the current project | `~/.claude.json` |
| `user` | You only, in all projects | `~/.claude.json` |
| `project` | Everyone who checks out the repository | `.mcp.json` in the repository root |

To add the MCP server for the current project, run:

{{< tabs >}}

{{< tab title="orbit" >}}

```shell
claude mcp add orbit-local -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="glab" >}}

```shell
claude mcp add orbit-local -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

To add the server for all of your projects, run:

{{< tabs >}}

{{< tab title="orbit" >}}

```shell
claude mcp add orbit-local --scope user -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="glab" >}}

```shell
claude mcp add orbit-local --scope user -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

To add the server for everyone who checks out the repository, run:

{{< tabs >}}

{{< tab title="orbit" >}}

```shell
claude mcp add orbit-local --scope project -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="glab" >}}

```shell
claude mcp add orbit-local --scope project -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

You can also edit the `.mcp.json` file directly:

{{< tabs >}}

{{< tab title="orbit" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="glab" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

To confirm the MCP server is connected, run:

```shell
claude mcp list
```

After you connect, [set up your AI assistant](./cli.md).

> [!note]
> For project-scoped commands, Claude Code asks for approval before
> creating the `mcp.json` file. Until you approve, `claude mcp list`
> shows the server as `Pending approval`.

### Connect Codex

Codex stores the MCP server configuration in `~/.codex/config.toml`. Codex
always configures the server for all of your projects.

To connect to Codex, run:

{{< tabs >}}

{{< tab title="orbit" >}}

```shell
codex mcp add orbit-local -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="glab" >}}

```shell
codex mcp add orbit-local -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

To confirm the server is registered, run:

```shell
codex mcp list
```

After you connect, [set up your AI assistant](./cli.md).

### Connect Cursor

Cursor reads the MCP server configuration from an `mcp.json` file. Choose the
scope that matches how widely you want the server available.

| Scope | Availability | Stored in |
| ----- | ------------ | --------- |
| Project | You only, in the current project | `.cursor/mcp.json` in the repository root |
| Global | You only, in all projects | `~/.cursor/mcp.json` |

To connect to Cursor, create or edit the `mcp.json` file for the scope you want:

{{< tabs >}}

{{< tab title="orbit" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "type": "stdio",
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="glab" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "type": "stdio",
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

To confirm the server is connected, run:

```shell
agent mcp list
```

### Connect OpenCode

Add the MCP server configuration to `opencode.json` in the repository root, or to
`~/.config/opencode/opencode.json` to use it in all of your projects:

{{< tabs >}}

{{< tab title="orbit" >}}

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["orbit", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="glab" >}}

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["glab", "orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

To confirm the server is connected, run:

```shell
opencode mcp list
```

After you connect, [set up your AI assistant](./cli.md).

### Connect other MCP clients

The GitLab Orbit Local MCP server does not expose a connection URL.
Clients that require a URL cannot connect to it.

{{< tabs >}}

{{< tab title="orbit" >}}

To connect using the GitLab Orbit CLI,
edit your client's MCP configuration file:

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="glab" >}}

To connect using the GitLab CLI:

1. Run `glab orbit local --install`. This command downloads the `orbit` binary.
1. Then, edit your client's MCP configuration file:

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

To confirm the server is connected, check that your client lists the `run_sql`,
`get_graph_schema`, and `index` tools.

## MCP tools

The GitLab Orbit Local MCP server provides a set of tools that interact
with your local DuckDB graph.

### `index`

Indexes a repository, or a directory of repositories, into the local DuckDB graph.

Example:

```plaintext
Index my checked out project.
```

### `get_graph_schema`

Fetches the schema. Includes table names, columns,
and data types present in the local DuckDB graph.

Example:

```plaintext
Use the `get_graph_schema` tool to show me what tables are in my local graph.
```

### `run_sql`

Executes read-only SQL against the local DuckDB graph.
Takes an array of statements and returns one JSON row array per statement,
at the same index.

A single `run_sql` call returns about 1 MB at most, counting all statements
in the call together. Larger results fail, and the agent retries with a
narrower query. If the agent does not recover, ask it for fewer results.

Example:

```plaintext
Show me the most used imports in this repository.
```