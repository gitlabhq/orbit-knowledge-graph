---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Pick an access method and build your first local Orbit graph.
title: Get started with Orbit Local
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experimental

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 18.11.

{{< /history >}}

> [!disclaimer]

<!-- -->

> [!note]
> Orbit Local is experimental. Until packaged binaries ship,
> you must build from source.

Orbit Local runs on your machine. Pick the access method that matches how you
work, then run your first query.

## Pick an access method

| Method | Best for | Setup |
|---|---|---|
| [orbit CLI](access/cli.md) | Direct CLI use, scripting, indexing tasks | Build the binary from source |
| [glab CLI](access/glab.md) | Anyone already using `glab`; one-command AI agent setup | `glab orbit setup` (pick "Local" when prompted) |
| [MCP](access/mcp.md) | Claude Code, Codex, and other AI agents | `glab orbit setup` or manual MCP config |

The query language is identical across all three. Whatever you learn in one
transfers directly to the others, and to [Orbit Remote](../remote/_index.md).

## 60-second quickstart

> [!note]
> `glab orbit local` is the planned packaging path. Until it ships, use the
> `orbit` binary directly - see [Use the orbit CLI directly](access/cli.md).
> The shapes shown below match what `glab orbit local` will support.

Index a repository and inspect what Orbit found:

```shell
glab orbit local index /path/to/your/repo
glab orbit local schema
```

That builds a local DuckDB graph at `~/.orbit/graph.duckdb` and prints the
node types: `Definition`, `File`, `Directory`, `ImportedSymbol`.

Next:

- Run a real query: [Use Orbit Local with glab](access/glab.md).
- Wire it into your AI agent: `glab orbit setup` and pick "Local" when
  prompted. See [Connect via MCP](access/mcp.md).
- Learn the query DSL: [Query language reference](../remote/queries/).

## Billing

Orbit Local does not consume GitLab Credits. All processing is local.

## What to try next

- [What Orbit Local indexes](indexing.md) - language and coverage scope.
- [Schema reference](schema.md) - the four node types in the local graph.
- [Cookbook](../remote/cookbook.md) - copy-paste queries (code-only ones apply to Local).
- [Get started with Orbit Remote](../remote/getting-started.md) - query your full GitLab instance.
