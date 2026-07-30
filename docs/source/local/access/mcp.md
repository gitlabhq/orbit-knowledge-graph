---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect Claude Code, Codex, OpenCode, or any MCP-compatible AI agent to your local GitLab Orbit graph.
title: Connect to GitLab Orbit Local via MCP
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/643) in GitLab 19.2 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

GitLab Orbit Local runs as a stateless MCP server over stdio, pointed at the local
DuckDB graph instead of a GitLab instance. Unlike GitLab Orbit Remote (which exposes
a JSON query DSL), GitLab Orbit Local speaks raw DuckDB SQL: agents compose SQL
directly against the property graph tables.

> [!note]
> The MCP server is experimental. Capabilities and config shape may change
> before GA.

## Prerequisites

- The GitLab Orbit CLI (`orbit`) is installed. See [Use the GitLab Orbit CLI directly](cli.md).
- A local repository has been indexed (`orbit index <path>` or
  `glab orbit local index <path>`). Agents can also index through the `index`
  MCP tool.

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

## Connect Claude Code

```shell
claude mcp add orbit-local -- orbit mcp serve
```

Or add the equivalent to your project's `.mcp.json`:

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

## Connect Codex

```shell
codex mcp add orbit-local -- orbit mcp serve
```

## Connect OpenCode

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

## Connect other MCP clients

Any MCP client can connect by running `orbit mcp serve` (or
`glab orbit local mcp serve`) as a stdio server. For Cursor, use the
`.mcp.json` block above in `.cursor/mcp.json`.

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
