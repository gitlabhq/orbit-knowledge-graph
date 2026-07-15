---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install, index, and query GitLab Orbit Local through the GitLab CLI with glab orbit local and glab orbit setup.
title: Use GitLab Orbit Local with the GitLab CLI (`glab`)
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
> `glab orbit local` and `glab orbit setup` ship today, in `glab` 1.94 or later.

Two top-level commands:

- `glab orbit local`: wraps the managed `orbit` binary to index and query the
  local graph.
- `glab orbit setup`: guided onboarding that verifies access, installs the
  GitLab Orbit skill, and installs the local binary.

## Prerequisites

- `glab` 1.94 or later is installed.
- A local Git repository to index.

No GitLab account or network connection is required to use `glab orbit local`
once the binary is installed.

## Install

Install the managed `orbit` binary:

```shell
glab orbit local --install
```

`glab` downloads the binary, verifies its checksum, and keeps it up to date.
Verify the install:

```shell
glab orbit local help
```

## Set up your AI agent

`glab orbit setup` runs a guided onboarding: it verifies that GitLab Orbit is
reachable, installs the GitLab Orbit skill so AI coding agents can discover it, and
installs the local `orbit` binary.

```shell
glab orbit setup
```

| Flag | Purpose |
|------|---------|
| `--yes` | Accept every prompt (non-interactive). |
| `--global` | Install the skill at user scope (`~/.agents/skills/`) instead of the current repository. |
| `--path` | Install the skill to a specific directory. |
| `--skip-skill` | Skip the skill install step. |
| `--skip-local` | Skip the local binary install step. |
| `--upgrade` | Re-fetch the skill and update the binary in place. |

The skill drives the `orbit` binary directly. To connect an MCP client to the
local graph instead, see [Connect via MCP](mcp.md).

You can also [install the GitLab Orbit skill manually](../../ai_coding_agents.md)
with `glab skills install --global orbit`.

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

`glab orbit local schema` lists every table and column in the local DuckDB
graph:

```shell
glab orbit local schema
```

Pass table names as positional arguments to scope the output:

```shell
glab orbit local schema gl_definition              # scoped to one table
glab orbit local schema gl_definition gl_edge      # scoped to two tables
```

| Flag | Purpose |
|------|---------|
| `--raw` | Emit JSON instead of the default table view. |
| `--db` | Override the DuckDB path. Defaults to `~/.orbit/graph.duckdb`. |

## Run as an MCP server

Expose the local graph to any MCP-compatible AI agent:

```shell
glab orbit local mcp serve
```

It serves `run_sql`, `get_graph_schema`, and `index` over the MCP protocol
against `~/.orbit/graph.duckdb`. See [Connect via MCP](mcp.md) for the full
agent integration guide.

## Exit codes

`glab orbit local` returns `0` on success and a non-zero exit code on failure,
with details on stderr. Scripts and agents can branch on success or failure.

## Billing

Orbit Local does not consume GitLab Credits. All processing is local.
