---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect MCP clients to GitLab Orbit with the GitLab MCP endpoint, or to the local graph with the local MCP server.
title: GitLab Orbit MCP servers
---

GitLab Orbit has two [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) servers:

| Server | Gives your agent |
|--------|------------------|
| [GitLab MCP endpoint](#gitlab-mcp-endpoint) | The GitLab data and code of the groups where GitLab Orbit is turned on. |
| [Local MCP server](#local-mcp-server) | SQL access to the [local graph](../local/_index.md) on your machine. |

You can connect a client to one server or to both.
If your agent can run shell commands, use the [CLI and the GitLab Orbit skill](_index.md) instead.

## GitLab MCP endpoint

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.

{{< /history >}}

The endpoint is `https://gitlab.com/api/v4/orbit/mcp`.
On GitLab Self-Managed and GitLab Dedicated, replace `gitlab.com` with the hostname of your instance.

Prerequisites:

- GitLab Orbit is [turned on](../quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group) for a group that you can access.
- Your client signs in with OAuth. The first tool call opens your browser.
  A client that connects over native HTTP must request the `mcp_orbit` scope.
  You can also use a personal access token with the `read_api` scope,
  or a [fine-grained personal access token](https://docs.gitlab.com/auth/tokens/fine_grained_access_tokens/).

### Connect a client to the endpoint

`glab orbit setup --mcp` does not configure the endpoint. Add it to your client by hand.
Clients that support only local stdio servers connect through [`mcp-remote`](https://www.npmjs.com/package/mcp-remote),
which needs [Node.js](https://nodejs.org/).

{{< tabs >}}

{{< tab title="Claude Code" >}}

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

Do not use `npx mcp-remote` with Claude Code. It conflicts with the built-in HTTP transport and causes
`Failed to connect` errors. To fix a registration that uses `mcp-remote`, run
`claude mcp remove gitlab-orbit`, then add the endpoint again.

{{< /tab >}}

{{< tab title="Codex" >}}

```shell
codex mcp add gitlab-orbit -- npx mcp-remote https://gitlab.com/api/v4/orbit/mcp
```

{{< /tab >}}

{{< tab title="Gemini CLI" >}}

Add the endpoint to `~/.gemini/settings.json`:

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

Without `"scopes": ["mcp_orbit"]`, authentication fails, even when you are signed in to GitLab.

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
Otherwise, it fails with `ConfigInvalidError`.

{{< /tab >}}

{{< tab title="Cursor, Antigravity, and other clients" >}}

Add the endpoint to the MCP configuration of your client.
Cursor reads `~/.cursor/mcp.json`. Antigravity reads `~/.gemini/config/mcp_config.json`.

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

Antigravity does not run the MCP OAuth flow for remote servers, so it needs `mcp-remote`.
A native `serverUrl` entry sends no token and fails with `Unauthorized`.
Do not add an `oauth` block. `mcp-remote` gets the `mcp_orbit` scope from the endpoint.

{{< /tab >}}

{{< /tabs >}}

### Test the connection

In your agent, ask:

```plaintext
Use GitLab Orbit to list the 5 most recently updated projects in my group.
```

The first call opens your browser to sign in. Until you sign in, the client shows `Needs authentication`.
Then the agent returns project names and paths.

If the browser does not open, sign in again from your client.
Your client uses its own OAuth sign-in, not your `glab` session.
For example, remove the `gitlab-orbit` server from your client and add it again to restart the sign-in.
For other errors, see [troubleshooting](../troubleshooting.md).

### Endpoint tools

The endpoint has two tools. `list_commands` lists the GitLab Orbit commands with their input schemas.
`invoke_command` runs one of them by name:

| Command | Description |
|---------|-------------|
| `query_graph` | Runs a query in the [Cypher query language](../queries/cypher.md). |
| `get_graph_schema` | Returns the node types and relationship types, and the properties of the node types that you pass in `expand_nodes`. |
| `get_response_format` | Returns the response JSON Schema of `query_graph` and its version. |

To get the properties and relationships of a node type, pass its name in `expand_nodes`, for example `["MergeRequest"]`, or run
`CALL db.schema('MergeRequest')` with `query_graph`. The `get_query_dsl` command belongs to the deprecated JSON query domain-specific language (DSL).

Your agent writes the queries for you. For precise control, give it a complete query to run with
`{"command_name": "query_graph", "parameters": {"query": "<query>"}}`, for example:

```gql orbit-query
MATCH (mr:MergeRequest)-[:IN_PROJECT]->(p:Project)
WHERE mr.state = 'opened' AND p.full_path STARTS WITH 'my-group/'
RETURN p.name, p.full_path, count(mr) AS open_mrs
ORDER BY open_mrs DESC
LIMIT 10
```

For query recipes and the query language reference, install the [GitLab Orbit skill](_index.md#gitlab-orbit-skill).

## Local MCP server

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/643) in GitLab 19.2 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

`orbit mcp serve` starts an MCP server over stdio. It reads only the local graph,
keeps no cache or query history, and serves each client separately.
The server has no URL, so clients that need a URL cannot connect to it.

Prerequisites:

- The [GitLab Orbit CLI](../cli.md) is installed.

`glab orbit setup --mcp` registers the server for Claude Code, Codex, and OpenCode.
For more information, see [set up your agents](_index.md#set-up-your-agents).

### Connect a client to the local server

The examples use the server name `orbit` and the command `glab orbit mcp serve`, as `glab orbit setup` does.
`glab` must run the binary without a prompt, so first run `glab config set orbit_cli_auto_run true`.
`glab orbit setup` sets this option for you. If you installed the standalone binary, use `orbit mcp serve`.

{{< tabs >}}

{{< tab title="Claude Code" >}}

```shell
claude mcp add orbit -- glab orbit mcp serve
```

The server is then available to you in the current project.
Add `--scope user` for all your projects, or `--scope project` to write `.mcp.json` for everyone who
checks out the repository. Claude Code asks for approval before it uses a project `.mcp.json`.

To check the connection, run `claude mcp list`.

{{< /tab >}}

{{< tab title="Codex" >}}

```shell
codex mcp add orbit -- glab orbit mcp serve
```

Codex saves the server in `~/.codex/config.toml`, for all your projects.
To check the connection, run `codex mcp list`.

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
      "command": ["glab", "orbit", "mcp", "serve"]
    }
  }
}
```

To check the connection, run `opencode mcp list`.

{{< /tab >}}

{{< tab title="Cursor and other clients" >}}

Add the server to the MCP configuration of your client.
Cursor reads `.cursor/mcp.json` in the repository root, or `~/.cursor/mcp.json` for all your projects.

```json
{
  "mcpServers": {
    "orbit": {
      "command": "glab",
      "args": ["orbit", "mcp", "serve"]
    }
  }
}
```

For Cursor, also add `"type": "stdio"` to the `orbit` entry.

To check the connection, make sure that your client lists the `index`, `get_graph_schema`, and `run_sql` tools.
In Cursor, run `agent mcp list`.

{{< /tab >}}

{{< /tabs >}}

### Local server tools

| Tool | Description |
|------|-------------|
| `index` | Indexes a repository, or a directory of repositories, into the local graph. |
| `get_graph_schema` | Returns the tables, columns, and data types of the local graph. |
| `run_sql` | Runs read-only SQL statements and returns one array of JSON rows for each statement, in order. |

One `run_sql` call returns at most about 1 MB for all statements together.
If a result is larger, the call fails and the agent tries again with a narrower query.
If the agent does not recover, ask it for fewer results.

## Related topics

- [Connect your agent](_index.md)
- [GitLab Orbit Local](../local/_index.md)
- [GitLab Orbit in GitLab Duo](duo.md)
- [GitLab Orbit CLI](../cli.md)
- [Queries](../queries/_index.md)
- [Billing](../how-it-works.md#billing)
- [Troubleshooting](../troubleshooting.md)
