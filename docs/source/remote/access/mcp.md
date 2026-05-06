---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect Claude Code, Codex, or any MCP-compatible AI agent to Orbit using the two MCP tools query_graph and get_graph_schema.
title: Connect via MCP
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

Orbit exposes two MCP tools that let any MCP-compatible AI agent query your GitLab
knowledge graph. Use this with Claude Code, OpenAI Codex, or any other tool that
supports the Model Context Protocol.

## Prerequisites

- Orbit is [enabled on your group](../get_started.md).
- You have a GitLab personal access token with `read_api` scope and access to the
  groups you want to query.

## MCP tools

| Tool | Description |
|------|-------------|
| `query_graph` | Execute a graph query using the Orbit query DSL. Returns typed results. |
| `get_graph_schema` | Fetch the current schema: all node types, their properties, and relationship types. |

## Configure Claude Code

Add the following to your Claude Code MCP configuration (`~/.claude/mcp_servers.json`
or your project's `.claude/mcp_servers.json`):

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "command": "glab",
      "args": ["mcp", "serve"],
      "env": {
        "GITLAB_TOKEN": "<your_personal_access_token>",
        "GITLAB_HOST": "https://gitlab.com"
      }
    }
  }
}
```

The `glab` CLI must be installed and authenticated. See the
[glab CLI documentation](https://docs.gitlab.com/cli/) for installation instructions.

## Configure other MCP clients

Any MCP client can connect to the Orbit MCP server by running `glab mcp serve`.
The server exposes `query_graph` and `get_graph_schema` over the MCP protocol.

## Billing

Queries through MCP consume **GitLab Credits**. Each query call to `query_graph`
uses credits from your GitLab subscription. `get_graph_schema` calls are free.

## Using the tools

Once connected, instruct your AI agent to use the Orbit tools directly:

**Discover the schema:**
> "Use `get_graph_schema` to show me what node types Orbit indexes."

**Run a query:**
> "Use `query_graph` to find the 10 projects with the most open merge requests in
> the `gitlab-org` group."

**Blast radius analysis:**
> "Use Orbit to find all files in this project that import `AuthService` directly
> or transitively."

**Onboarding:**
> "Use Orbit to map the key services in this group, their languages, and which
> projects they depend on."

The agent composes the JSON query DSL and calls `query_graph` on your behalf.
You can also pass raw JSON queries directly if you want precise control over results.

## Example: manual query_graph call

```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "aggregations": [
    {"function": "count", "target": "mr", "group_by": "p", "alias": "open_mrs"}
  ],
  "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
  "limit": 10
}
```
