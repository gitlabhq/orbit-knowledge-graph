---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Build and try the local Orbit indexer from source.
title: Local Orbit indexer developer preview
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

The local Orbit indexer is a source-built developer preview. It lets you index a
local Git repository into DuckDB and run Orbit JSON queries from the command
line.

Use the local indexer when you want to:

- Try Orbit code graph queries before a packaged CLI is available.
- Test code indexing changes against a local repository.
- Compare local query behavior with the deployed Orbit service.

Do not use the local indexer as a replacement for the deployed Orbit service. It
does not include GitLab.com authorization, GitLab SDLC data replication, the
hosted dashboard, or the full MCP integration.

## Local and deployed Orbit compared

| Capability | Deployed Orbit service | Local Orbit indexer |
|------------|------------------------|---------------------|
| How you run it | GitLab.com service. | Source-built `orbit` binary. |
| Main storage | ClickHouse. | DuckDB under `~/.orbit/graph.duckdb`. |
| Data source | GitLab SDLC data and repository code. | Files in a local Git checkout. |
| Authorization | Delegated to GitLab Rails. | Local filesystem access only. |
| Query language | Orbit JSON query language. | Orbit JSON query language for local entities. |
| UI | Orbit dashboard. | Command line only. |
| MCP | Hosted Orbit MCP endpoint. | Not available in this preview. |

## Build the CLI

Prerequisites:

- [mise](https://mise.jdx.dev/) installed.
- A local checkout of the Orbit repository.

To build the local CLI:

1. Go to the Orbit repository:

   ```shell
   cd /path/to/knowledge-graph
   ```

1. Install tool versions:

   ```shell
   mise install
   ```

1. Build the `orbit` binary:

   ```shell
   mise build:cli
   ```

The compiled binary is available at `target/release/orbit`.

## Index a repository

To index a local Git repository:

```shell
./target/release/orbit index /path/to/repository
```

Orbit writes the graph database to `~/.orbit/graph.duckdb`. To use a different
data directory, set `ORBIT_DATA_DIR`:

```shell
ORBIT_DATA_DIR=/tmp/orbit ./target/release/orbit index /path/to/repository
```

The command prints a JSON summary with the indexed repository, graph counts, and
DuckDB path.

## Query the local graph

Run a query by passing a JSON query object:

```shell
./target/release/orbit query '{
  "query_type": "traversal",
  "node": {
    "id": "f",
    "entity": "File",
    "columns": ["relative_path", "language"]
  },
  "limit": 10
}'
```

To return raw graph JSON instead of the default agent-oriented output, add
`--raw`:

```shell
./target/release/orbit query --raw '{
  "query_type": "traversal",
  "node": {
    "id": "d",
    "entity": "Definition",
    "columns": ["name", "fqn", "file_path"]
  },
  "limit": 10
}'
```

## Inspect the local schema

The local graph includes only entities supported by the local DuckDB pipeline.
To inspect them:

```shell
./target/release/orbit schema --ontology
```

To include all deployed-service ontology entities for comparison:

```shell
./target/release/orbit schema --ontology --all
```

## Known preview limits

- The local indexer is built from source and is not distributed through GitLab
  CLI yet.
- It indexes local repository code, not the full GitLab SDLC graph.
- It stores all indexed repositories in one DuckDB database by default.
- It does not run a daemon or watch file changes.
- It does not expose a local MCP server in this preview.
