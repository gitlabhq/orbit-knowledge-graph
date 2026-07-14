---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit Local - build and query a code graph on your own machine, no GitLab instance required.
title: Orbit Local
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

Orbit Local runs entirely on your machine. Build a code graph for any local
repository and query it using the same query language as Orbit Remote. No
GitLab account, no network connection required.

- Indexes: Code only, including files, definitions, cross-file references.
- Storage: DuckDB (local file at `~/.orbit/graph.duckdb`)

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
| [The Orbit CLI (`orbit`)](access/cli.md) | Run the `orbit` binary directly to index and query |
| [The GitLab CLI (`glab`)](access/glab.md) | Drive Orbit Local through `glab orbit local` |
| [MCP](access/mcp.md) | Expose the local graph to Claude Code, Codex, and other agents |

## Billing

Orbit Local does not consume GitLab Credits. All processing is local.
