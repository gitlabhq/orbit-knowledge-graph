---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Enable Orbit Remote on GitLab.com and run your first query.
title: Get started with Orbit Remote
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experimental

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

## Prerequisites

- Owner role on the top-level group you want to enable Orbit on
- GitLab.com Premium or Ultimate plan

Orbit indexes top-level groups only. Subgroups and projects inherit indexing automatically.

## Step 1: Enable Orbit

1. On the left sidebar, expand **Your Work**.
1. Select **Orbit > Configuration**.
1. Find your top-level group in the **Indexes** list.
1. Toggle **Enable**.

Orbit begins indexing immediately. Initial indexing takes a few minutes for small groups
and up to 30 minutes for groups with thousands of projects.

Check indexing status at any time:

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

## Step 2: Run your first query

Orbit Remote exposes the same graph through three surfaces. Pick the one that matches who's querying:

| Method | Best for | Setup | Billing |
|---|---|---|---|
| **GitLab Duo Agent Platform** | End users in the GitLab UI | None | Zero-rated |
| **MCP** | Claude Code, Codex, other AI agents | One-time agent config | GitLab Credits |
| **REST API** | Scripts, dashboards, custom tooling | API token | GitLab Credits |

### GitLab Duo Agent Platform (no setup required)

Orbit is wired into the Developer Flow and the foundational agents - Planner agent, Security Analyst agent, Data Analyst agent, and CI Expert agent. They call Orbit's `query_graph` and `get_graph_schema` tools automatically when a question is best answered by traversing the graph. No tool selection or configuration required.

In the Developer Flow, ask a question that benefits from graph context, like:

> "What services would break if I rename the deploy_user method?"

GitLab Duo queries are zero-rated and do not consume GitLab Credits.

### MCP (Claude Code, Codex, other agents)

See [Use Orbit via MCP](access/mcp.md) for setup. Once configured, you have two tools: `query_graph` and `get_graph_schema`.

### REST API

Replace `your-group` with the top-level group path you enabled Orbit on. The `full_path` filter scopes the query so it passes Orbit's selectivity validation.

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{
    "query": {
      "query_type": "traversal",
      "node": {
        "id": "p",
        "entity": "Project",
        "columns": ["name", "full_path"],
        "filters": {
          "full_path": {"op": "starts_with", "value": "your-group/"}
        }
      },
      "limit": 10
    },
    "format": "raw"
  }' \
  "https://gitlab.com/api/v4/orbit/query"
```

## What to try next

- [What Orbit indexes](indexing.md) - understand coverage before writing queries
- [Schema reference](schema.md) - explore the 24 node types and their properties
- [Cookbook](cookbook.md) - copy-paste queries for common use cases
- [Get started with Orbit Local](../local/getting-started.md) - query a local repository offline
