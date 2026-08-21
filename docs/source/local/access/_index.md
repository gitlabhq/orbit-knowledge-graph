---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use available tooling, like the GitLab Orbit CLI or GitLab Orbit Local MCP server, to interact with your local graph.
title: Connect your tools
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

GitLab Orbit Local provides tools that you can use
to access your local graph and interact with
coding agents.

## GitLab Orbit Local with the GitLab Orbit CLI

With the GitLab Orbit CLI, you can build a local graph
and query it without a connection to a GitLab instance.

You can also expose your graph to
AI agents with the GitLab Orbit Local MCP
server and configure AI coding assistants
to consult your graph.

## GitLab Orbit Local with the GitLab CLI

Extend the GitLab CLI to install and run GitLab Orbit CLI commands.
The GitLab CLI also manages updates for you and installs a
GitLab Orbit skill that updates agents with query recipes,
DSL guidance, and troubleshooting help.

## GitLab Orbit Local MCP server

Connect the GitLab Orbit Local MCP server to an AI client.
After you connect
to a client, you can configure agents to interact with your
local graph.

## GitLab Orbit Local skill for AI coding agents

Give AI coding agents the `orbit-local` skill, which
ships inside the GitLab Orbit CLI. The skill covers the
tables in your local graph, SQL recipes for querying them,
and the mistakes agents most often make.
