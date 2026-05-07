---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use the glab CLI to install Orbit Local, index repositories, and run queries against the local code graph.
title: Use Orbit Local with the glab CLI
---

{{< details >}}

- Tier: Free
- Offering: All tiers, self-managed and GitLab.com
- Status: Developer preview

{{< /details >}}

The [`glab` CLI](https://docs.gitlab.com/cli/) is the canonical way to install,
run, and integrate Orbit Local with your AI agent. `glab orbit local` mirrors
`glab orbit remote`, so the same patterns work whether you query the GitLab
instance or your local machine.

> [!note]
> `glab orbit local` is the planned packaging path for the developer preview.
> Until it ships, build from source - see [Use the orbit CLI directly](cli.md).

Three top-level commands:

- **`glab orbit setup`** - install the Orbit skill and point your AI
  agent at the local graph.
- **`glab orbit local`** - typed subcommands that wrap the `orbit` binary.
- **`glab orbit local mcp serve`** - run Orbit Local as an MCP server.

## Prerequisites

- `glab` is installed and authenticated:

  ```shell
  glab auth login
  ```

- A local Git repository to index.

No GitLab account or network connection is required to use `glab orbit local`
once the binary is installed.

## Set up your AI agent

Run `glab orbit setup` to install the Orbit skill and write the MCP config.
The command prompts you to pick **Local** or **Remote** and auto-detects
your agent.

```shell
glab orbit setup
# Pick "Local" when prompted to point the MCP config at your local graph.
```

Supported agents: Claude Code, OpenCode, Cursor, Codex, Gemini CLI, Duo CLI.

| Flag | Purpose |
|------|---------|
| `--agent=<name>` | Override auto-detection. |
| `--skill-only` | Install the skill files only; skip MCP config. |
| `--mcp-only` | Write MCP config only; skip skill install. |
| `--dry-run` | Print what would change without writing anything. |

The MCP config points at `orbit mcp serve` instead of the remote endpoint.
Your agent can call `query_graph` and `get_graph_schema` against the local
DuckDB graph.

## Index a repository

```shell
glab orbit local index /path/to/your/repo
```

| Flag | Purpose |
|------|---------|
| `--threads` | Worker thread count. `0` (default) auto-detects from CPU cores. |
| `--stats` | Include detailed statistics in the JSON output. |
| `--verbose` | Verbose logging to stderr. |

## Run a query

```shell
echo '{"query_type":"search","node":{"id":"d","entity":"Definition","columns":["name","kind"]},"limit":10}' \
  | glab orbit local query -
```

Pass a file path or `-` for stdin. The query language is identical to Orbit
Remote.

| Flag | Purpose |
|------|---------|
| `--format llm` | Compact text optimized for AI agent consumption. |
| `--format raw` | Structured JSON, suitable for piping to `jq`. |

## Inspect the schema

```shell
glab orbit local schema
glab orbit local schema --expand Definition File
glab orbit local schema --query
```

## Run as an MCP server

Expose the local graph to any MCP-compatible AI agent:

```shell
glab orbit local mcp serve
```

This serves `query_graph` and `get_graph_schema` over the MCP protocol against
`~/.orbit/graph.duckdb`. See [Connect via MCP](mcp.md) for the full agent
integration guide.

## List indexed repositories

```shell
glab orbit local status
```

Shows which repositories are present in the local graph, their indexing state,
and the database path.

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
