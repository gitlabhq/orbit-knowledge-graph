---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: The orbit CLI - build and query a code graph on your own machine, no GitLab instance required.
title: Orbit Local
---

{{< details >}}

- Tier: Free
- Offering: All tiers, self-managed and GitLab.com
- Status: Developer preview

{{< /details >}}

> [!note]
> Orbit Local is an early developer preview. Until packaged binaries ship,
> you must build from source.

Orbit Local runs entirely on your machine. Build a code graph for any local
repository and query it using the same query language as Orbit Remote. No
GitLab account, no network connection required.

- **For:** Any tier, self-managed, or offline use
- **Indexes:** Code only - files, definitions, cross-file references
- **Storage:** DuckDB (local file at `~/.orbit/graph.duckdb`)
- **Status:** Developer preview

[Get started with Orbit Local](getting-started.md)

## In this section

| Page | Description |
|---|---|
| [Get started](getting-started.md) | Pick an access method and run your first query |
| [How it works](how-it-works.md) | Indexing pipeline, graph model, query execution |
| [What Orbit Local indexes](indexing.md) | Code coverage, language support, scope |
| [Schema reference](schema.md) | The four node types in the local code graph |

## Access methods

| Method | Description |
|---|---|
| [orbit CLI](access/cli.md) | Run the `orbit` binary directly to index and query |
| [glab CLI](access/glab.md) | Drive Orbit Local through `glab orbit local` |
| [MCP](access/mcp.md) | Expose the local graph to Claude Code, Codex, and other agents |

Orbit Local does not consume GitLab Credits. All processing is local.
