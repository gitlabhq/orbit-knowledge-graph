---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Build and query a local code graph with the Orbit CLI binary. No GitLab account or network connection required.
title: Use Orbit Local with the Orbit CLI (`orbit`)
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

The Orbit CLI (`orbit`) builds a code graph for any local repository and queries it
against a local DuckDB file. No GitLab connection required.

> [!note]
> Orbit Local is experimental. Until packaged binaries ship,
> you must build from source. The packaged install path will be `glab orbit local`.

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (stable)
- [`mise`](https://mise.jdx.dev/) for tool management
- A local Git repository to index

## Install

Build from source:

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

Orbit parses the repository and writes a DuckDB graph to `~/.orbit/graph.duckdb`.
You can index multiple repositories. Each is scoped by project ID and branch
in the manifest table.

| Flag | Purpose |
|------|---------|
| `--threads` | Worker thread count. `0` (default) auto-detects from CPU cores. |
| `--stats` | Include detailed statistics in the JSON output. |
| `--verbose` | Verbose logging to stderr. |

## Query the graph

```shell
orbit query '{
  "query_type": "traversal",
  "node": {
    "id": "d",
    "entity": "Definition",
    "columns": ["name", "definition_type", "file_path"],
    "filters": { "definition_type": "Method" }
  },
  "limit": 20
}'
```

The query language is identical to Orbit Remote. See the
[query language reference](../../remote/queries/) for full syntax.

| Flag | Purpose |
|------|---------|
| `--raw` | Emit raw JSON instead of LLM-friendly text. |
| `--ontology` | Override the ontology directory. Defaults to embedded. |

## Inspect the schema

```shell
orbit schema --ontology
orbit schema --ontology --expand Definition File
orbit schema --query
```

`--ontology` describes the graph (entities, edges, properties).
`--query` describes the query DSL (how to write queries).

## Compile a query without running it

```shell
orbit compile --local '{
  "query_type": "traversal",
  "node": {
    "id": "f",
    "entity": "File",
    "columns": ["path"],
    "filters": {"language": "ruby"}
  },
  "limit": 5
}'
```

Useful for debugging query DSL or seeing the generated DuckDB SQL.

## Run raw SQL against the local graph

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

## Storage

The graph is stored at `~/.orbit/graph.duckdb`. Multiple repositories share
the same database. Delete the file to start over.

## Billing

Orbit Local does not consume GitLab Credits. All processing is local.

## What to try next

- [Connect via MCP](mcp.md) - expose the local graph to Claude Code or Codex.
- [Use Orbit Local with glab](glab.md) - call the CLI through `glab orbit local`.
- [Schema reference](../../remote/schema.md) - available node types and properties.
- [Cookbook](../../remote/cookbook.md) - copy-paste queries for common use cases.
