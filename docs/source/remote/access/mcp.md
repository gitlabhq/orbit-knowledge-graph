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
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

Orbit exposes two MCP tools that let any MCP-compatible AI agent query your GitLab
knowledge graph. Use this with Claude Code, OpenAI Codex, or any other tool that
supports the Model Context Protocol.

## Prerequisites

- Orbit is [enabled on your group](../getting-started.md).
- You're authenticated to GitLab. Run `glab auth login` (uses OAuth by default;
  personal access tokens with `read_api` scope also work).
- Your auth has access to the groups you want to query.
- If your MCP client connects directly over native HTTP (not through
  `mcp-remote`), its OAuth request must include the `mcp_orbit` scope. See the
  Gemini CLI example below.

## MCP tools

| Tool | Description |
|------|-------------|
| `query_graph` | Execute a graph query using the Orbit query DSL. Returns typed results. |
| `get_graph_schema` | Fetch the current schema: all node types, their properties, and relationship types. |

## Connect your MCP client

Configure your MCP client to point at `https://gitlab.com/api/v4/orbit/mcp`.

**Claude Code** supports the Orbit endpoint over the built-in HTTP transport.
Register it with one command:

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

The first `query_graph` or `get_graph_schema` call opens your browser to
authenticate with GitLab. No JSON config edit required.

> [!note]
> Claude Code connects directly over HTTP. Do not use `npx mcp-remote` with
> Claude Code — it wraps the endpoint in a stdio process that conflicts with
> the built-in transport and causes "Failed to connect" errors. Use the
> `claude mcp add --transport http` command shown above instead.

Some clients only support local stdio MCP servers. For those,
[`mcp-remote`](https://www.npmjs.com/package/mcp-remote) wraps the Orbit endpoint
as a local command.

**Cursor, Codex, and other JSON-config clients** — add to your agent's MCP config:

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

**opencode** — add to `~/.config/opencode/opencode.json`:

```json
{
  "mcp": {
    "gitlab-orbit": {
      "type": "local",
      "command": ["npx", "mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

> [!note]
> opencode requires `"type": "local"` and places command and arguments together
> in a single array. Using a separate `args` field or omitting `type` causes a
> `ConfigInvalidError`.

**Gemini CLI** — supports the Orbit endpoint over native HTTP transport. Add to
`~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "url": "https://gitlab.com/api/v4/orbit/mcp",
      "type": "http",
      "timeout": 5000,
      "oauth": {
        "enabled": true,
        "scopes": ["mcp_orbit"]
      }
    }
  }
}
```

You can also generate this with `gemini mcp add gitlab-orbit https://gitlab.com/api/v4/orbit/mcp -t http -s user`,
then add the `oauth.scopes` block by hand.

> [!note]
> Native HTTP MCP clients must request the `mcp_orbit` OAuth scope explicitly.
> Without `oauth.scopes: ["mcp_orbit"]`, authentication fails even if you're
> already signed in to GitLab elsewhere. If a client on native HTTP transport
> can't authenticate, add this scope to its MCP server config.
>
> Older Gemini CLI configs may use `httpUrl` instead of `url` + `type: "http"`.
> `httpUrl` still works but is deprecated; use `url` + `type` for new setups.

**Antigravity** — the Antigravity IDE and CLI read the same MCP config at
`~/.gemini/config/mcp_config.json`. Antigravity does not yet run the MCP OAuth
flow for remote servers (a native `serverUrl` entry sends `initialize` without
a token and fails with `Unauthorized`), so wrap the endpoint with `mcp-remote`:

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

> [!note]
> No `oauth` block is needed here. `mcp-remote` discovers the `mcp_orbit`
> scope from the endpoint's OAuth metadata and opens your browser to authorize
> on first use.

Authentication uses your existing `glab auth login` session - no token to copy or
paste. Supported clients: Claude Code, OpenCode, Cursor, Codex, Gemini CLI,
Antigravity.

> [!note]
> A planned `glab orbit setup` subcommand will install the Orbit skill and
> write this MCP config in one step. Until it ships, configure your MCP client
> manually as shown above.

You can also [install the Orbit skill manually](../../ai_coding_agents.md)
today to give the agent query recipes, DSL guidance, and troubleshooting.

### Test it

In your AI agent, ask:

> "Use Orbit to list the 5 most recently updated projects in my group."

You should get typed results back with project names and paths. If you do, you're
connected. If not, run `glab auth status` to confirm you're authenticated, and
check that Orbit is enabled on at least one of your groups.

## Billing

Queries through MCP consume GitLab Credits. Each query call to `query_graph`
uses credits from your GitLab subscription. `get_graph_schema` calls are free.

## Using the tools

Once connected, instruct your AI agent to use the Orbit tools directly:

Discover the schema:
> "Use `get_graph_schema` to show me what node types Orbit indexes."

Run a query:
> "Use `query_graph` to find the 10 projects with the most open merge requests in
> your group."

Blast radius analysis:
> "Use Orbit to find all files in this project that import `AuthService` directly
> or transitively."

Onboarding:
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
  "group_by": [{"kind": "node", "node": "p"}],
  "aggregations": [
    {"function": "count", "target": "mr", "alias": "open_mrs"}
  ],
  "aggregation_sort": {"column": "open_mrs", "direction": "DESC"},
  "limit": 10
}
```

## Troubleshooting

### "Failed to connect" in Claude Code

Claude Code has built-in HTTP MCP support. If you registered Orbit with
`npx mcp-remote` instead of `--transport http`, the `mcp-remote` wrapper
creates a local stdio process that conflicts with the native transport.

To fix, remove the broken registration and re-add with HTTP transport:

```shell
claude mcp remove gitlab-orbit
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

### "Needs authentication" on first use

This is expected. The first `query_graph` or `get_graph_schema` call opens
your browser to complete OAuth with GitLab. If the browser flow does not
trigger, verify your session:

```shell
glab auth status
```

If your session is expired, re-authenticate:

```shell
glab auth login
```

### Query errors after connecting

For query-time errors (validation failures, empty results, rate limits), see the
[Orbit skill documentation](../../ai_coding_agents.md), which includes DSL
guidance, query recipes, and exit-code diagnostics. Install the skill for
inline guidance:

```shell
glab skills install --global orbit
```
