---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use available tooling, like GitLab Duo Agent Platform, the GitLab Orbit MCP server, or the GitLab CLI, to query your GitLab Orbit Remote graph.
title: Connect your tools
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

GitLab Orbit Remote provides tools that you can use
to query your GitLab Orbit graph and connect
AI agents to your GitLab data.

## GitLab Orbit Remote with GitLab Duo Agent Platform

GitLab Duo Agent Platform agents use GitLab Orbit
automatically. Ask a question in GitLab Duo Chat and the
agent queries the graph when it needs relationships
across your projects, pipelines, and work items.

## GitLab Orbit Remote MCP server

Connect the GitLab Orbit MCP server to an MCP-compatible
AI client, like Claude Code or Codex. After you connect,
agents can discover the schema and query your graph.

## GitLab Orbit Remote with the GitLab CLI

Use `glab orbit` to discover the schema, run queries, and check
indexing progress from the command line.

## GitLab Orbit skill for AI coding agents

Install the GitLab Orbit skill to give AI coding agents
query recipes, query language guidance, and
troubleshooting help.

## GitLab Orbit Remote REST API

Query the graph directly from scripts, CI pipelines,
or custom tooling with the REST API.
