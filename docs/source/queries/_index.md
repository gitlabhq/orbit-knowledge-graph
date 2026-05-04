---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query Orbit to explore connected GitLab data.
title: Queries
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

Orbit queries retrieve connected GitLab data from the knowledge graph. A query is
a JSON object that describes the nodes, relationships, filters, and result shape
you want.

Orbit supports these query types:

- `traversal`: find nodes and follow relationships between them.
- `aggregation`: group and count graph data.
- `path_finding`: find paths between nodes.
- `neighbors`: find nodes directly connected to a starting node.

Queries respect GitLab permissions. When you query Orbit through GitLab, you see
only data you can access in GitLab.

## Run a query in the dashboard

Prerequisites:

- Orbit is turned on for a top-level group.
- You have access to that group.

To run a query:

1. On the left sidebar, select **Search or go to**.
1. Select **Your work**.
1. Select **Orbit**.
1. Select **Explore**.
1. Select **Advanced query**.
1. Enter an Orbit JSON query.
1. Select **Execute query**.

Example query:

```json
{
  "query_type": "traversal",
  "node": {
    "id": "p",
    "entity": "Project",
    "columns": ["name", "full_path"]
  },
  "limit": 10
}
```

For all fields and examples, see [Orbit query language](query_language.md).

## Run a query with AI

GitLab Duo and MCP-compatible tools can write and run Orbit queries for you. Use
natural language prompts that ask for connected GitLab data.

Example prompts:

- "Show all open issues blocked by merge requests with failing pipelines."
- "Find vulnerabilities linked to merge requests merged in the last seven days."
- "List projects where `@alice` authored merge requests, grouped by project."
- "Find files changed in the most failed pipelines over the past month."
- "Show code definitions related to this project and their connected files."

For the MCP tool contract, see [Orbit MCP tools](mcp_tools.md).

## Run a local query

The source-built local Orbit indexer can run a subset of Orbit queries against a
local DuckDB graph. Local queries are for developer preview workflows and use
local repository code, not the deployed GitLab.com graph.

For details, see [Local Orbit indexer developer preview](../local_indexer.md).
