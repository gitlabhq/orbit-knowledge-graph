---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Build a code graph for a local repository and query it with the orbit CLI.
title: Get started with Orbit Local
---

{{< details >}}

- Tier: Free
- Offering: All tiers, self-managed and GitLab.com
- Status: Developer preview

{{< /details >}}

> [!note]
> Orbit Local is an early developer preview. It must be built from source and has
> no UI, no MCP integration, and no daemon process yet.

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (stable)
- [`mise`](https://mise.jdx.dev/) for tool management
- A local Git repository to index

## Step 1: Build the CLI

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

The compiled binary is at `target/release/orbit`.

## Step 2: Build the graph

```shell
./target/release/orbit index --path /path/to/your/repo
```

Orbit parses the repository and writes a DuckDB graph to `~/.orbit/graph.db`.

## Step 3: Query the graph

```shell
./target/release/orbit query --query '{
  "query_type": "search",
  "node": {
    "id": "d",
    "entity": "Definition",
    "columns": ["name", "kind", "file_path"],
    "filters": { "kind": "function" }
  },
  "limit": 20
}'
```

The query language is identical to Orbit Remote. See [Query language reference](../queries/) for full syntax.

## What to try next

- [What Orbit indexes](../indexing.md) — language and coverage scope
- [Schema reference](../schema.md) — available node types and properties
- [Cookbook](../cookbook.md) — copy-paste queries for common use cases
- [Get started with Orbit Remote](../remote/getting_started.md) — query your full GitLab instance
