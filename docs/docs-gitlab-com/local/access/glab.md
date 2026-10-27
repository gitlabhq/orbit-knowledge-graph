---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install, index, and query GitLab Orbit Local through the GitLab CLI with glab orbit and glab orbit setup.
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

The [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) is the canonical way to
install, run, and integrate GitLab Orbit with your AI agent. `glab orbit`
forwards one flat command tree to the managed `orbit` binary. The `workspace`
commands use the local graph, and commands such as `query` and `schema` use
the hosted graph. `grep` and `context` route on their own: they read local
code and, in a GitLab clone, also reach GitLab.

> [!note]
> The flat `glab orbit` command tree requires `glab` 1.117 or later.

`glab orbit setup` provides guided onboarding, installs the GitLab Orbit skill,
and configures supported AI agents.

## Prerequisites

- `glab` 1.117 or later is installed.
- A local Git repository to index.

The `workspace` commands require no GitLab account or network connection once
the binary is installed. `grep` and `context` work offline on local code. With
a GitLab sign-in, they also search the project or group behind `origin`.
Hosted commands (`query`, `status`, and `schema`) require an authenticated
GitLab account and network access.

## Install

Install the managed `orbit` binary:

```shell
glab orbit --install
```

`glab` downloads the binary, verifies its checksum, and keeps it up to date.
Verify the install:

```shell
glab orbit version
```

## Set up your AI agent

`glab orbit setup` configures AI coding agents to consult the graph: it writes a
managed section into each agent's instruction file and installs the GitLab Orbit
skill.

```shell
glab orbit setup
```

Run `glab orbit setup --help` for the full option list, including supported
agents and project or user scope.

The binary's own [`orbit setup`](cli.md#set-up-your-ai-agent) also detects
the installed agents, installs the skill, and registers the MCP server with
`--mcp`. `orbit uninstall` reverts it. To connect an MCP client by hand, see
[Connect via MCP](mcp.md).

You can also [install the GitLab Orbit skill manually](../../ai_coding_agents.md)
with `glab skills install --global orbit`.

## Index a repository

```shell
glab orbit workspace index /path/to/your/repo
```

| Flag | Purpose |
|------|---------|
| `--budget` | Seconds a language family may take before the run stops. No limit by default. |
| `--stats` | Include detailed statistics in the JSON output. |
| `--verbose` | Verbose logging to stderr. |

## Run SQL against the graph

```shell
glab orbit workspace sql 'SELECT count(*) FROM gl_definition'
echo 'SELECT name FROM gl_definition LIMIT 3' | glab orbit workspace sql -
```

Queries include all indexed revisions through one DuckDB client by default.
Use `--repo /path/to/repo` to select that checkout's current revision.
Use `--db /path/to/revision.duckdb` to select one database file.

| Flag | Purpose |
|------|---------|
| `--repo` | Query only the given checkout's current revision. |
| `--all` | Query every indexed repository and commit (the default). |
| `-F`, `--format` | Output format: `table` (default), `json`, `ndjson`, or `csv`. |
| `--db` | Read a specific revision database. Defaults to the indexed revision databases. |

## Inspect the schema

`glab orbit workspace schema` lists the graph tables and their columns. The
per-project search-index tables and the schema fingerprint are hidden; name one
explicitly to see it:

```shell
glab orbit workspace schema
```

Pass table names as positional arguments to scope the output:

```shell
glab orbit workspace schema gl_definition              # scoped to one table
glab orbit workspace schema gl_definition gl_edge      # scoped to two tables
```

| Flag | Purpose |
|------|---------|
| `--raw` | Emit JSON instead of the default table view. |
| `--db` | Override the DuckDB file path. Omit to use the local graph managed by GitLab Orbit. |

## Run as an MCP server

Expose the local graph to any MCP-compatible AI agent:

```shell
glab orbit workspace mcp serve
```

It serves `run_sql`, `get_graph_schema`, and `index` over the MCP protocol
against revision databases under `~/.gitlab/orbit/local/`. See [Connect via MCP](mcp.md) for the full
agent integration guide.

## Exit codes

`glab orbit` returns `0` on success and a non-zero exit code on failure,
with details on stderr. Scripts and agents can branch on success or failure.

## Billing

The `workspace` commands, and `grep` and `context` on local code, process data
locally and do not consume GitLab Credits.
Hosted commands require network access, and `query`
[consumes GitLab Credits](../../remote/access/glab.md#billing).
