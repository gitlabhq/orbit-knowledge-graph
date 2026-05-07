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
- Status: Experiment

{{< /details >}}

## Prerequisites

- Owner role on the top-level group you want to enable Orbit on
- GitLab.com Premium or Ultimate plan

Orbit indexes top-level groups only. Subgroups and projects inherit indexing automatically.

## Step 1: Enable Orbit

1. On the left sidebar, select **Search or go to** and find your top-level group.
1. Select **Settings > General**.
1. Expand **Orbit**.
1. Turn on the **Enable Orbit** toggle.
1. Select **Save changes**.

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
| **Duo Agent Platform** | End users in the GitLab UI | None | Zero-rated |
| **MCP** | Claude Code, Codex, other AI agents | One-time agent config | GitLab Credits |
| **REST API** | Scripts, dashboards, custom tooling | API token | GitLab Credits |

### Duo Agent Platform (no setup required)

1. On the left sidebar, select **GitLab Duo**.
1. Select **Orbit**.
1. Ask: "What are the most active projects in my group?"

Duo queries are zero-rated and do not consume GitLab Credits.

### MCP (Claude Code, Codex, other agents)

See [Use Orbit via MCP](../access/mcp.md) for setup. Once configured, you have two tools: `query_graph` and `get_graph_schema`.

### REST API

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{
    "query_type": "traversal",
    "node": {
      "id": "p",
      "entity": "Project",
      "columns": ["name", "full_path"]
    },
    "limit": 10
  }' \
  "https://gitlab.com/api/v4/orbit/query"
```

## What to try next

- [What Orbit indexes](../indexing.md) - understand coverage before writing queries
- [Schema reference](../schema.md) - explore the 24 node types and their properties
- [Cookbook](../cookbook.md) - copy-paste queries for common use cases
- [Get started with Orbit Local](../local/getting_started.md) - query a local repository offline
