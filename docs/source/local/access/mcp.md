---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Planned MCP server for Orbit Local. Not yet available.
title: Connect to Orbit Local via MCP
---

> [!warning]
> **Not yet available.** Orbit Local cannot run as an MCP server yet. The
> `orbit mcp serve` and `glab orbit local mcp serve` commands do not exist in
> any released version of the Orbit CLI. The configuration examples below
> describe the **planned** interface, not a feature you can use today. If you
> add this MCP config to your agent, it will fail silently: the server never
> starts and the agent cannot query anything.
>
> Until the MCP server ships, use the [workaround](#workaround-query-from-the-terminal)
> below. Track progress in
> [merge request !1377](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1377).

When it ships, Orbit Local will run as a stateless MCP server over stdio,
pointed at the local DuckDB graph instead of a GitLab instance. Unlike Orbit
Remote (which exposes a JSON query DSL), Orbit Local speaks raw DuckDB SQL -
agents compose SQL directly against the property graph tables.

## Prerequisites

- The Orbit CLI (`orbit`) is installed. See [Use the Orbit CLI directly](cli.md).
- A local repository has been indexed (`orbit index <path>` or
  `glab orbit local index <path>`).

## Workaround: query from the terminal

Until the MCP server ships, query your local graph directly from a terminal
with `glab orbit local`. This is the supported way to use Orbit Local today.

Run a raw read-only SQL query against the local DuckDB graph:

```shell
glab orbit local sql "SELECT name, definition_type FROM gl_definition LIMIT 10"
```

Describe the graph schema (table names, columns, and data types). Add `--raw`
for JSON instead of the default table view:

```shell
glab orbit local schema
```

You can paste the output of these commands into your AI agent as context. You
can also [install the Orbit skill manually](../../ai_coding_agents.md) today to
give the agent query recipes, SQL guidance, and troubleshooting.

## Planned interface

> [!note]
> Everything in this section describes the planned MCP server. None of these
> commands or config blocks work yet. They are preserved here as the
> specification for the contributor who implements the feature, and so you can
> see what is coming.

### Planned MCP tools

| Tool | Description |
|------|-------------|
| `run_sql` | Execute a read-only SQL query against the local DuckDB graph. Returns JSON rows. |
| `get_graph_schema` | Fetch the schema: table names, columns, and data types present in the local DuckDB. |
| `index` | Index a repository (or a directory of repositories) into the local graph. |

### Planned config: Claude Code

The planned interface will let you add the following to
`~/.claude/mcp_servers.json` or your project's `.claude/mcp_servers.json`:

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

Or, if you prefer to drive it through `glab`:

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

### Planned config: other MCP clients

Any MCP client will be able to connect by running `orbit mcp serve` (or
`glab orbit local mcp serve`). The server will speak MCP over stdio and expose
`run_sql`, `get_graph_schema`, and `index`.

### Planned usage

Once connected, you will instruct your AI agent to use Orbit directly.

Discover the schema:
> "Use `get_graph_schema` to show me what tables are in my local graph."

Find definitions by type:
> "Use Orbit to count the definitions in this repository by type, and list the
> ten largest classes."

Map a module:
> "Use Orbit to list every definition declared in `src/auth/` and show its
> kind."

## What's in the local graph

Orbit Local indexes code only: files, directories, definitions, and
imported symbols across all 11 supported languages. SDLC data (merge requests,
pipelines, users, vulnerabilities) is not available locally - that requires
[Orbit Remote](../../remote/_index.md).

## Billing

Orbit Local does not consume GitLab Credits. All local traffic stays on your
machine.
