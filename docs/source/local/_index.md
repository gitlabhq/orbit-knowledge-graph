---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: The orbit CLI — build and query a code graph on your own machine, no GitLab instance required.
title: Orbit Local
---

{{< details >}}

- Tier: Free
- Offering: All tiers, self-managed and GitLab.com
- Status: Developer preview

{{< /details >}}

> [!note]
> Orbit Local is an early developer preview. It must be built from source.

The `orbit` CLI runs entirely on your machine. Build a code graph for any local repository
and query it using the same query language as Orbit Remote. No GitLab account, no network
connection required.

- **For:** Any tier, self-managed, or offline use
- **Indexes:** Code only — files, definitions, cross-file references
- **Storage:** DuckDB (local file at `~/.orbit/graph.duckdb`)
- **Status:** Developer preview

[Get started with Orbit Local](getting-started.md)

## In this section

| Page | Description |
|---|---|
| [Get started](getting-started.md) | Build a graph and run your first query |
| [How it works](how-it-works.md) | Indexing pipeline, graph model, query execution |
