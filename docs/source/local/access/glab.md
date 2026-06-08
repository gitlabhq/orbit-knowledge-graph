---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install, index, and query Orbit Local through the GitLab CLI with glab orbit local. The glab orbit setup, mcp serve, and status commands are planned.
title: Use Orbit Local with the GitLab CLI (`glab`)
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!disclaimer]

The [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) is the canonical way to install,
run, and integrate Orbit Local with your AI agent. `glab orbit local` mirrors
`glab orbit remote`, so the same patterns work whether you query the GitLab
instance or your local machine.

> [!note]
> `glab orbit local` ships today, in `glab` 1.94 or later. `glab orbit setup`,
> `glab orbit local mcp serve`, and `glab orbit local status` are planned and
> marked as such below.

Two top-level commands:

- `glab orbit local`: wraps the managed `orbit` binary to index and query the
  local graph. Available now.
- `glab orbit setup`: install the Orbit skill and point your AI agent at the
  local graph. Planned.

## Prerequisites

- `glab` 1.94 or later is installed.
- A local Git repository to index.

No GitLab account or network connection is required to use `glab orbit local`.

## Install

Install the managed `orbit` binary:

```shell
glab orbit local --install --yes
```

`glab` downloads the binary, verifies its checksum, and keeps it up to date.
Verify the install:

```shell
glab orbit local help
```

## Set up your AI agent

> [!note]
> `glab orbit setup` is planned, not yet shipped. Until it ships,
> [configure your MCP client manually](mcp.md#manual-config-claude-code).

Once shipped, `glab orbit setup` will install the Orbit skill and write the
MCP config in one command. It prompts for **Local** or **Remote** and
auto-detects your agent.

```shell
glab orbit setup
# Pick "Local" when prompted to point the MCP config at your local graph.
```

Supported agents: Claude Code, OpenCode, Cursor, Codex, Gemini CLI.

| Flag | Purpose |
|------|---------|
| `--agent=<name>` | Override auto-detection. |
| `--skill-only` | Install the skill files only; skip MCP config. |
| `--mcp-only` | Write MCP config only; skip skill install. |
| `--dry-run` | Print what would change without writing anything. |

The MCP config points at `orbit mcp serve` instead of the remote endpoint.
Your agent can call `query_graph` and `get_graph_schema` against the local
DuckDB graph.

You can also [install the Orbit skill manually](../../ai_coding_agents.md)
today with `glab skills install --global orbit`.

## Index a repository

```shell
glab orbit local index /path/to/your/repo
```

| Flag | Purpose |
|------|---------|
| `--threads` | Worker thread count. `0` (default) auto-detects from CPU cores. |
| `--stats` | Include detailed statistics in the JSON output. |
| `--verbose` | Verbose logging to stderr. |

## Run SQL against the graph

```shell
glab orbit local sql 'SELECT count(*) FROM gl_definition'
echo 'SELECT name FROM gl_definition LIMIT 3' | glab orbit local sql -
```

## Inspect the schema

```shell
glab orbit local schema
glab orbit local schema --raw
```

## Run as an MCP server

> [!note]
> `glab orbit local mcp serve` is planned, not yet shipped.

Once shipped, this command will expose the local graph to any MCP-compatible
AI agent:

```shell
glab orbit local mcp serve
```

It will serve `query_graph` and `get_graph_schema` over the MCP protocol
against `~/.orbit/graph.duckdb`. See [Connect via MCP](mcp.md) for the full
agent integration guide.

## List indexed repositories

> [!note]
> `glab orbit local status` is planned, not yet shipped.

Once shipped, this command will show which repositories are present in the
local graph, their indexing state, and the database path:

```shell
glab orbit local status
```

## Exit codes

`glab orbit local` maps errors to stable exit codes so scripts and agents can
branch on them.

| Status | Exit code | Meaning |
|--------|-----------|---------|
| Success | `0` | Command completed. |
| No graph | `2` | `~/.orbit/graph.duckdb` not found. Run `index` first. |
| Bad query | `4` | Query DSL failed validation or compilation. |
| Other | `1` | Unstructured error. Stderr contains details. |

## Billing

Orbit Local does not consume GitLab Credits. All processing is local.
