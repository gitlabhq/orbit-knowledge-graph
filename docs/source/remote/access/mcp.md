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

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

<!-- -->

> [!disclaimer]

Orbit exposes two MCP tools that let any MCP-compatible AI agent query your GitLab
knowledge graph. Use this with Claude Code, OpenAI Codex, or any other tool that
supports the Model Context Protocol.

## Prerequisites

- Orbit is [enabled on your group](../getting-started.md).
- You're authenticated to GitLab. Run `glab auth login` (uses OAuth by default;
  personal access tokens with `read_api` scope also work).
- Your auth has access to the groups you want to query.

## MCP tools

| Tool | Description |
|------|-------------|
| `query_graph` | Execute a graph query using the Orbit query DSL. Returns typed results. |
| `get_graph_schema` | Fetch the current schema: all node types, their properties, and relationship types. |

## Connect your MCP client

Run:

```shell
glab orbit setup
```

That's it. `glab` detects your AI agent, installs the Orbit skill, and writes the
MCP server config. Authentication uses your existing `glab auth login` session -
no token to copy or paste.

Supported clients: Claude Code, OpenCode, Cursor, Codex, Gemini CLI. See
the [glab orbit page](glab.md) for flags and overrides.

If setup fails, run `glab auth status` to confirm you're authenticated, and check
that Orbit is enabled on at least one of your groups.

### Test it

In your AI agent, ask:

> "Use Orbit to list the 5 most recently updated projects in my group."

You should get typed results back with project names and paths. If you do, you're
connected.

### Manual configuration

For other clients, point them at `https://gitlab.com/api/v4/orbit/mcp`.

Some clients only support local stdio MCP servers. For those,
[`mcp-remote`](https://www.npmjs.com/package/mcp-remote) wraps the Orbit endpoint
as a local command:

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "command": "npx",
      "args": ["mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

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
