---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit running on GitLab-hosted infrastructure
title: Orbit Remote
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

Orbit Remote runs on GitLab-hosted infrastructure. Enable it on a top-level group and it automatically indexes your entire SDLC and code - groups, projects, users, merge requests, pipelines, vulnerabilities, and source code - into a ClickHouse property graph.

- Indexes: Full SDLC + code graph
- Storage: ClickHouse (managed, no setup required)

[Get started with Orbit Remote](getting-started.md)

## In this section

| Page | Description |
|---|---|
| [Get started](getting-started.md) | Enable Orbit and run your first query |
| [How it works](how-it-works.md) | Indexing pipeline, graph model, query execution |
| [What Orbit indexes](indexing.md) | SDLC coverage, language support, indexing scope |
| [Security](security.md) | Roles required to query, the authorization model, and programmatic access |
| [Schema reference](schema.md) | All 28 node types across 6 domains |
| [Cookbook](cookbook.md) | Copy-paste queries for common use cases |
| [Query language](queries/) | Full query DSL reference |

## Access methods

| Method | Description |
|---|---|
| [GitLab Duo Agent Platform](access/duo.md) | Natural language questions via the GitLab UI |
| [MCP](access/mcp.md) | Connect Claude Code, Codex, and other agents |
| [The GitLab CLI (`glab`)](access/glab.md) | `glab orbit remote` for scripting and discovery (available in `glab` 1.94 or later) |
| [REST API](access/api.md) | Query from scripts, CI pipelines, or custom tooling |

## Billing

MCP and REST API queries consume GitLab Credits. GitLab Duo Agent Platform queries are zero-rated.
