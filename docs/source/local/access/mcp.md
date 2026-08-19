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

With the GitLab Orbit Local [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server,
you can securely connect AI tools and applications to your local DuckDB graph. AI assistants
like Claude Desktop, Claude Code and OpenCode can then access your graph and write SQL queries against
it.

The GitLab Orbit Local MCP server is stateless, which means the server:

- Queries your local DuckDB graph, not a GitLab instance
- Does not cache results or persist query history
- Can respond to multiple clients independently

## Prerequisites

- Install the [GitLab Orbit CLI](./cli.md) (`orbit`), version 0.101.1 or later

## Connect a client to the GitLab Orbit Local MCP server

The GitLab Orbit Local MCP server supports stdio transport. Arguments and commands
vary depending on the MCP client and your local environment.

### Connect Claude Code

Claude Code stores the MCP server configuration in one of three scopes. Choose the
scope that matches how widely you want the server available.

| Scope | Availability | Stored in |
| ----- | ------------ | --------- |
| `local` (default) | You only, in the current project | `~/.claude.json` |
| `user` | You only, in all projects | `~/.claude.json` |
| `project` | Everyone who checks out the repository | `.mcp.json` in the repository root |

Prerequisites:

- If connecting to a shared repository, an `.mcp.json` file in your project root.

To add the server for the current project, run:

```shell
claude mcp add orbit-local -- orbit local mcp serve
```

To add the server for all of your projects, run:

```shell
claude mcp add orbit-local --scope user -- orbit local mcp serve
```

To add the server for everyone who checks out the repository, run:

```shell
claude mcp add orbit-local --scope project -- orbit local mcp serve
```

You can also edit the `.mcp.json` file directly:

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "orbit",
      "args": ["local", "mcp", "serve"]
    }
  }
}
```

### Connect Codex

```shell
codex mcp add orbit-local -- orbit mcp serve
```

### Connect OpenCode

Add to `opencode.json` (project or global):

```json
{
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["orbit", "mcp", "serve"],
      "enabled": true
    }
  }
}
```

### Connect other MCP clients

Any MCP client can connect by running `orbit mcp serve` (or
`glab orbit local mcp serve`) as a stdio server. For Cursor, use the
`.mcp.json` block above in `.cursor/mcp.json`.


## MCP tools

| Tool | Description |
|------|-------------|
| `run_sql` | Execute read-only SQL against the local DuckDB graph. Takes an array of statements; returns one JSON row array per statement, at the same index. |
| `get_graph_schema` | Fetch the schema: table names, columns, and data types present in the local DuckDB. |
| `index` | Index a repository (or a directory of repositories) into the local graph. |

The server is stateless: every tool call opens the DuckDB file on demand and
releases it before returning, so multiple editors can run one server process
each against the same graph.

Large `run_sql` results are rejected before serialisation (about 1 MB of
Arrow data) with an error asking the agent to add `LIMIT` or narrow the
projection, so a runaway `SELECT *` cannot freeze your editor.

## Using the tools

Once connected, instruct your AI agent to use GitLab Orbit directly.

Discover the schema:
> "Use `get_graph_schema` to show me what tables are in my local graph."

Find definitions by type:
> "Use GitLab Orbit to count the definitions in this repository by type, and list the
> ten largest classes."

Map a module:
> "Use GitLab Orbit to list every definition declared in `src/auth/` and show its
> kind."

The `_orbit_manifest` table lists the indexed repositories, so "what repos are
in my local graph?" is one `run_sql` call away.

## What's in the local graph

GitLab Orbit Local indexes code only: files, directories, definitions, and
imported symbols across all 11 supported languages. SDLC data (merge requests,
pipelines, users, vulnerabilities) is not available locally. That requires
[GitLab Orbit Remote](../../remote/_index.md).

## Billing

GitLab Orbit Local does not consume GitLab Credits. All local traffic stays on your
machine.
