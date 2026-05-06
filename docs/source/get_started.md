---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Enable Orbit for a group and run your first query in under five minutes.
title: Get started with Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

## Prerequisites

- You must be an Owner of the top-level group you want to enable Orbit on.
- Your group must be on GitLab.com on a Premium or Ultimate plan.

Orbit indexes top-level groups only. Subgroups and projects inherit indexing from
the top-level group automatically.

## Step 1: Enable Orbit

1. On the left sidebar, select **Search or go to** and find your top-level group.
1. Select **Settings > General**.
1. Expand **Orbit**.
1. Turn on the **Enable Orbit** toggle.
1. Select **Save changes**.

Orbit begins indexing your group immediately. Initial indexing takes a few minutes
for small groups and up to 30 minutes for groups with thousands of projects.

You can check indexing status at any time:

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

The response includes the indexed domain count, indexing progress, and last updated timestamp.

## Step 2: Run your first query

Choose how you want to query Orbit:

### Option A: Duo Agent Platform (no setup required)

If you have GitLab Duo Developer, the Orbit agent is available immediately.

1. On the left sidebar, select **GitLab Duo**.
1. Select **Orbit**.
1. Ask a question in natural language:
   - "What are the most active projects in my group?"
   - "Who reviewed the most merge requests last month?"
   - "Which pipelines are failing most often?"

Duo translates your question to a graph query and returns results. Queries through
Duo are zero-rated and do not consume GitLab Credits.

### Option B: MCP (Claude Code, Codex, or other MCP clients)

See [Connect via MCP](access/mcp.md) for setup instructions.

Once configured, you have two tools available: `query_graph` and `get_graph_schema`.

Ask your AI agent: "Use Orbit to show me the 10 most recently updated projects
in the gitlab-org group."

### Option C: REST API

Send a query directly to the API:

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

See [REST API reference](access/api.md) for full documentation.

## What to try next

- [What Orbit indexes](indexing.md): understand coverage before writing queries.
- [Schema reference](schema.md): explore the 24 node types and their properties.
- [Cookbook](cookbook.md): copy-paste queries for common use cases.
