---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect MCP clients to the local GitLab Orbit graph with orbit mcp serve, or to the server graph with the GitLab MCP endpoint.
title: GitLab Orbit MCP servers
---

GitLab Orbit has two [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) servers:

- The [local MCP server](#local-mcp-server) runs `orbit mcp serve` on your machine. It gives an agent SQL access to the local graph.
- The [GitLab MCP endpoint](#gitlab-mcp-endpoint) runs on GitLab.com. It gives an agent the server graph of your groups.

You can connect a client to one server or to both.

## Local MCP server

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/643) in GitLab 19.2 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

`orbit mcp serve` starts an MCP server over stdio.
AI agents like Claude Code, Codex, Cursor, and OpenCode can then read the local graph and run SQL on it.

The server is stateless:

- It reads the local graph, not a GitLab instance.
- It does not cache results or keep a query history.
- It answers more than one client, each one separately.

Prerequisites:

- The [GitLab Orbit CLI](../cli.md) is installed.

`glab orbit setup --mcp` writes the server configuration for Claude Code, Codex, and OpenCode.
For more information, see [connect AI agents](_index.md#set-up-your-agents).
The tabs show the manual configuration.

The examples use the server name `orbit`, the same name that `glab orbit setup` uses.
If you use `glab`, replace the command `orbit mcp serve` with `glab orbit mcp serve`.

### Connect a client to the local server

{{< tabs >}}

{{< tab title="Claude Code" >}}

Claude Code keeps the configuration in one of three scopes:

| Scope | Available to | Stored in |
|-------|--------------|-----------|
| `local` (default) | You, in the current project | `~/.claude.json` |
| `user` | You, in all projects | `~/.claude.json` |
| `project` | Everyone who checks out the repository | `.mcp.json` in the repository root |

To add the server, run one of these commands:

```shell
claude mcp add orbit -- orbit mcp serve
claude mcp add orbit --scope user -- orbit mcp serve
claude mcp add orbit --scope project -- orbit mcp serve
```

You can also edit `.mcp.json`:

```json
{
  "mcpServers": {
    "orbit": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

To check the connection, run `claude mcp list`.
For the `project` scope, Claude Code asks for approval before it uses `.mcp.json`.
Until you approve, `claude mcp list` shows the server as `Pending approval`.

{{< /tab >}}

{{< tab title="Codex" >}}

Codex keeps the configuration in `~/.codex/config.toml`, for all your projects:

```shell
codex mcp add orbit -- orbit mcp serve
```

To check the connection, run `codex mcp list`.

{{< /tab >}}

{{< tab title="Cursor" >}}

Cursor reads the configuration from `.cursor/mcp.json` in the repository root for one project,
or from `~/.cursor/mcp.json` for all your projects:

```json
{
  "mcpServers": {
    "orbit": {
      "type": "stdio",
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

To check the connection, run `agent mcp list`.

{{< /tab >}}

{{< tab title="OpenCode" >}}

Add the server to `opencode.json` in the repository root,
or to `~/.config/opencode/opencode.json` for all your projects:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "orbit": {
      "type": "local",
      "command": ["orbit", "mcp", "serve"]
    }
  }
}
```

To check the connection, run `opencode mcp list`.

{{< /tab >}}

{{< tab title="Other clients" >}}

The local server has no URL. A client that needs a URL cannot connect to it.

Add the server to the MCP configuration file of your client:

```json
{
  "mcpServers": {
    "orbit": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

If you use `glab`, run `glab orbit --install` first to download the binary.

To check the connection, make sure that your client lists the `run_sql`, `get_graph_schema`, and `index` tools.

{{< /tab >}}

{{< /tabs >}}

### Local server tools

| Tool | Description | Example prompt |
|------|-------------|----------------|
| `index` | Indexes a repository, or a directory of repositories, into the local graph. | "Index my checked-out project." |
| `get_graph_schema` | Returns the tables, columns, and data types of the local graph. | "Use `get_graph_schema` to show the tables in my local graph." |
| `run_sql` | Runs read-only SQL. It takes an array of statements and returns one array of JSON rows for each statement, in the same order. | "Show me the most used imports in this repository." |

One `run_sql` call returns about 1 MB at most, for all statements together.
A larger result fails, and the agent tries again with a narrower query.
If the agent does not recover, ask it for fewer results.

## GitLab MCP endpoint

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
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

The GitLab MCP endpoint is `https://gitlab.com/api/v4/orbit/mcp`.
It gives an MCP client two tools to find and run GitLab Orbit commands on the server graph.
`glab orbit setup --mcp` does not configure this endpoint. Configure it by hand.

Prerequisites:

- GitLab Orbit is [turned on for your group](../_index.md#turn-on-gitlab-orbit-for-your-group).
- You can access the groups that you want to query.
- Your client authenticates with OAuth. The first tool call opens your browser to sign in to GitLab.
  You can also use a [fine-grained personal access token](https://docs.gitlab.com/auth/tokens/fine_grained_access_tokens/)
  or a personal access token with the `read_api` scope.
- If your client connects over native HTTP, not through `mcp-remote`, its OAuth request must include the `mcp_orbit` scope.

### Connect a client to the endpoint

Some clients support only local stdio servers.
For these clients, [`mcp-remote`](https://www.npmjs.com/package/mcp-remote) wraps the endpoint as a local command.

{{< tabs >}}

{{< tab title="Claude Code" >}}

Claude Code connects to the endpoint over its built-in HTTP transport:

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

Do not use `npx mcp-remote` with Claude Code.
It starts a stdio process that conflicts with the built-in transport and causes `Failed to connect` errors.
To fix a registration that uses `mcp-remote`, remove it and add it again:

```shell
claude mcp remove gitlab-orbit
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

{{< /tab >}}

{{< tab title="Codex" >}}

```shell
codex mcp add gitlab-orbit -- npx mcp-remote https://gitlab.com/api/v4/orbit/mcp
```

{{< /tab >}}

{{< tab title="Cursor and other clients" >}}

Add the endpoint to the MCP configuration of your client, for example `~/.cursor/mcp.json`:

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

{{< /tab >}}

{{< tab title="OpenCode" >}}

Add the endpoint to `~/.config/opencode/opencode.json`:

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

OpenCode needs `"type": "local"` and one array for the command and its arguments.
A separate `args` field, or no `type`, causes a `ConfigInvalidError`.

{{< /tab >}}

{{< tab title="Gemini CLI" >}}

Gemini CLI connects over native HTTP. Add the endpoint to `~/.gemini/settings.json`:

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

You can also run `gemini mcp add gitlab-orbit https://gitlab.com/api/v4/orbit/mcp -t http -s user`,
then add the `oauth.scopes` block by hand.
Without `"scopes": ["mcp_orbit"]`, authentication fails, even if you are signed in to GitLab.

Older configurations use `httpUrl` in place of `url` and `type`.
`httpUrl` still works but is deprecated.

{{< /tab >}}

{{< tab title="Antigravity" >}}

The Antigravity IDE and CLI read `~/.gemini/config/mcp_config.json`.
Antigravity does not run the MCP OAuth flow for remote servers.
A native `serverUrl` entry sends `initialize` without a token and fails with `Unauthorized`.
Use `mcp-remote`:

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

Do not add an `oauth` block. `mcp-remote` gets the `mcp_orbit` scope from the OAuth metadata of the endpoint.

{{< /tab >}}

{{< /tabs >}}

### Test the connection

In your agent, ask:

```plaintext
Use GitLab Orbit to list the 5 most recently updated projects in my group.
```

The first call opens your browser to sign in. The client shows `Needs authentication` until you complete the sign-in.
After the sign-in, the agent returns project names and paths.
If the browser does not open, run `glab auth status` to check your session, and `glab auth login` to sign in again.
Also make sure that GitLab Orbit is turned on for one of your groups.
For query errors after you connect, see [troubleshooting](../troubleshooting.md).

### Endpoint tools

| Tool | Description |
|------|-------------|
| `list_commands` | Lists the GitLab Orbit commands with their descriptions and input schemas. |
| `invoke_command` | Runs a command by name with parameters, and returns typed results. |

`invoke_command` runs these commands:

| Command | Description |
|---------|-------------|
| `query_graph` | Runs a query in the JSON Query DSL, or read-only GQL text when GQL is turned on for the user. |
| `get_graph_schema` | Returns the node types, their properties, and the relationship types. |
| `get_query_dsl` | Returns the JSON DSL grammar of `query_graph` and its version. |
| `get_response_format` | Returns the response JSON Schema of `query_graph` and its version. |

The `orbit_gql_queries` feature flag in GitLab selects one query language for each user. It is off by default.
For the requirements, see the [query endpoint](../queries/api.md#query-endpoint).

- With the flag off, `list_commands` describes the JSON DSL, and `query_graph` accepts only JSON objects.
- With the flag on, `list_commands` describes only GQL, and `query_graph` accepts only strings.
  The command list does not show `get_query_dsl`, and DSL requests fail.
  Use `CALL db.schema()` to discover the graph.

A client cannot select the language.
Do not share discovery results across users or modes.

### Use the endpoint tools

After you connect, tell your agent to use the tools. For example:

```plaintext
Use list_commands to show the GitLab Orbit commands, then run get_graph_schema to show the node types.
Use query_graph to find the 10 projects with the most open merge requests in my group.
Use GitLab Orbit to find all files in this project that import AuthService directly or transitively.
Use GitLab Orbit to map the key services in this group, their languages, and the projects they depend on.
```

The agent writes the query and runs `query_graph` for you.
For precise control, give the agent a complete query.
For example, to call `invoke_command` with `{"command_name": "query_graph", "parameters": {"query": ...}}`, use this query:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "group_by": ["p"],
  "aggregations": [
    {"count": "mr", "as": "open_mrs"}
  ],
  "aggregation_sort": "-open_mrs",
  "limit": 10
}
```

To give the agent query recipes and the query language reference, install the [GitLab Orbit skill](_index.md#gitlab-orbit-skill).

### Billing

MCP queries do not consume GitLab Credits during the beta. For billing after general availability, see [billing](../how-it-works.md#billing).

## Related topics

- [Connect AI agents](_index.md)
- [GitLab Orbit in GitLab Duo](duo.md)
- [GitLab Orbit CLI](../cli.md)
- [Query the graph](../queries/_index.md)
- [Troubleshooting](../troubleshooting.md)
