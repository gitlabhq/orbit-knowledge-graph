---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: The glab orbit local subcommands and glab orbit setup are planned for a future glab release. Until they ship, build from source and use the orbit binary directly.
title: Use Orbit Local with the GitLab CLI (`glab`)
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

The [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) is the canonical way to install,
run, and integrate Orbit Local with your AI agent. `glab orbit local` mirrors
`glab orbit remote`, so the same patterns work whether you query the GitLab
instance or your local machine.

> [!note]
> Both `glab orbit local` and `glab orbit setup` are planned for a future glab
> release. Every command on this page is the future shape, not the current one.
> Until they ship, build from source - see [use `orbit` directly](cli.md).

Two top-level commands (both planned, not yet shipped):

- `glab orbit setup`: install the Orbit skill and point your AI
  agent at the local graph.
- `glab orbit local`: typed subcommands that wrap the `orbit` binary.
  Includes `glab orbit local mcp serve` to run Orbit Local as an MCP server.

## Prerequisites

- `glab` is installed and authenticated:

  ```shell
  glab auth login
  ```

- A local Git repository to index.

No GitLab account or network connection is required to use `glab orbit local`
once the binary is installed.

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
