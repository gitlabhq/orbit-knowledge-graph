---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Build and query a local code graph with the GitLab Orbit CLI (orbit) binary. No GitLab account or network connection required.
title: Use Orbit Local with the GitLab Orbit CLI (`orbit`)
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

The GitLab Orbit CLI (`orbit`) builds a code graph for any local repository and queries it
against a local DuckDB file. No GitLab connection required.

## Install

Install the standalone `orbit` binary with the one-line installer:

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

This adds `orbit` to your `PATH`. Open a new terminal, then verify the install:

```shell
orbit help
```

If you already use the GitLab CLI (`glab`), you can instead install a managed
binary with `glab orbit local --install`. That binary is invoked as
`glab orbit local <command>` rather than `orbit` directly - see
[Use Orbit Local with glab](glab.md).

### Build from source

To contribute to GitLab Orbit or run an unreleased build, compile the binary
yourself.

Prerequisites:

- [Rust toolchain](https://rustup.rs/) (stable)
- [`mise`](https://mise.jdx.dev/) for tool management

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

The compiled binary is at `target/release/orbit`. Add it to your `PATH` or
invoke it directly.

## Index a repository

```shell
orbit index /path/to/your/repo
```

GitLab Orbit parses the repository and writes a DuckDB graph to `~/.orbit/graph.duckdb`.
You can index multiple repositories. Each is scoped by project ID and branch
in the manifest table.

| Flag | Purpose |
|------|---------|
| `--threads` | Worker thread count. `0` (default) auto-detects from CPU cores. |
| `--stats` | Include detailed statistics in the JSON output. |
| `--verbose` | Verbose logging to stderr. |
| `--db` | Override the DuckDB file path (default: `~/.orbit/graph.duckdb`). |

## Inspect the schema

`orbit schema` lists every table and column in the local DuckDB graph:

```shell
orbit schema
```

Pass table names as positional arguments to scope the output:

```shell
orbit schema gl_definition              # scoped to one table
orbit schema gl_definition gl_edge      # scoped to two tables
```

| Flag | Purpose |
|------|---------|
| `--raw` | Emit JSON instead of the default table view. |
| `--db` | Override the DuckDB path. Defaults to `~/.orbit/graph.duckdb`. |

## Run SQL against the local graph

```shell
orbit sql 'SELECT count(*) FROM gl_definition'
orbit sql -F json 'SELECT name FROM gl_definition LIMIT 3'
echo 'SELECT 1+1' | orbit sql -
orbit sql --file query.sql
```

| Flag | Purpose |
|------|---------|
| `-F`, `--format` | `table` (default), `json`, `ndjson`, or `csv`. |
| `-f`, `--file` | Read the SQL from a file. |
| `--db` | Override the DuckDB path. Defaults to `~/.orbit/graph.duckdb`. |

## List indexed repositories

The graph can hold more than one repository. To see what it contains, run:

```shell
orbit list
orbit list -F json
```

Each row reports the repository path, branch, commit, indexing status, and
when it was last indexed:

```plaintext
+------------------------+--------+------------+---------+---------------------+
| repo_path              | branch | commit_sha | status  | last_indexed_at     |
+------------------------+--------+------------+---------+---------------------+
| /home/dev/workspace/kg | main   | 9606ae8... | indexed | 2026-05-18 10:14:02 |
| /tmp/cli-test          | main   | 654f3a6... | indexed | 2026-05-18 10:13:55 |
+------------------------+--------+------------+---------+---------------------+
```

| Flag | Purpose |
|------|---------|
| `-F`, `--format` | `table` (default), `json`, `ndjson`, or `csv`. |
| `--db` | Override the DuckDB path. Defaults to `~/.orbit/graph.duckdb`. |

If nothing has been indexed yet, `orbit list` exits `0`. The table view
prints nothing; structured formats emit valid empty output (`[]` for `json`,
no records for `ndjson`) so pipelines like `orbit list -F json | jq` keep
working.

## Run as an MCP server

Expose the local graph to any MCP-compatible AI agent over stdio:

```shell
orbit mcp serve
```

It serves `run_sql`, `get_graph_schema`, and `index` against
`~/.orbit/graph.duckdb`. See [Connect via MCP](mcp.md) for per-client config.

## Storage

The graph is stored at `~/.orbit/graph.duckdb`. Multiple repositories share
the same database. Delete the file to start over.

## Billing

Orbit Local does not consume GitLab Credits. All processing is local.

## What to try next

- [Connect via MCP](mcp.md) - connect Claude Code, Codex, and other agents to
  the local graph.
- [Use Orbit Local with glab](glab.md) - call the CLI through `glab orbit local`.
- [Schema reference](../../remote/schema.md) - available node types and properties.
- [Cookbook](../../remote/cookbook.md) - copy-paste queries for common use cases.
