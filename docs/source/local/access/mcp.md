---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect Claude Code, Codex, or any MCP-compatible AI agent to your local Orbit graph.
title: Connect to Orbit Local via MCP
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

> [!disclaimer]

Orbit Local can run as an MCP server, exposing the same two tools as Orbit
Remote (`query_graph`, `get_graph_schema`) but pointed at the local DuckDB
graph instead of a GitLab instance.

> [!note]
> The MCP server is experimental. Capabilities and config
> shape may change before GA.

## Prerequisites

- The `orbit` CLI is installed. See [Use the orbit CLI directly](cli.md).
- A local repository has been indexed (`orbit index <path>` or
  `glab orbit local index <path>`).

## MCP tools

| Tool | Description |
|------|-------------|
| `query_graph` | Execute a graph query using the Orbit query DSL against the local graph. |
| `get_graph_schema` | Fetch the schema: node types, properties, and relationship types present in the local graph. |

The contract is identical to Orbit Remote, so any skill or agent prompt that
works against Remote works against Local without changes.

> [!note]
> A planned `glab orbit setup` subcommand will install the Orbit skill and
> write the MCP config for you. Until it ships, configure your MCP client
> manually as shown below.

## Manual config: Claude Code

Add the following to `~/.claude/mcp_servers.json` or your project's
`.claude/mcp_servers.json`:

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

## Manual config: other MCP clients

Any MCP client can connect by running `orbit mcp serve` (or
`glab orbit local mcp serve`). The server speaks MCP over stdio and exposes
`query_graph` and `get_graph_schema`.

## Using the tools

Once connected, instruct your AI agent to use Orbit directly:

**Discover the schema:**
> "Use `get_graph_schema` to show me what node types are in my local graph."

**Find callers of a function:**
> "Use Orbit to find every file that imports `parseConfig` and the functions
> that call it."

**Map a module:**
> "Use Orbit to list every definition declared in `src/auth/` and show their
> kind."

The agent composes the JSON query DSL and calls `query_graph` on your behalf.

## What's in the local graph

Orbit Local indexes **code only**: files, directories, definitions, and
imported symbols across all 11 supported languages. SDLC data (merge requests,
pipelines, users, vulnerabilities) is not available locally - that requires
[Orbit Remote](../../remote/_index.md).

## Billing

Orbit Local does not consume GitLab Credits. All MCP traffic is local.
