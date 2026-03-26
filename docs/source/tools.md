---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use Orbit MCP tools to query the knowledge graph and discover available entities and relationships.
title: Orbit MCP tools
---

{{< details >}}

- Tier: Ultimate
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

## `execute_query`

Query the knowledge graph and return matching nodes, relationships, and aggregations.

### Parameters

| Parameter | Type   | Required | Description |
|-----------|--------|----------|-------------|
| `query`   | object | Yes      | An Orbit query language object. |

### Example

```plaintext
Find merge requests merged in my-group/my-project in the last 7 days.
```

## `get_graph_schema`

Return the Orbit graph schema so agents can understand which entities, relationships, and properties are available.

### Parameters

| Parameter     | Type             | Required | Description |
|---------------|------------------|----------|-------------|
| `expand_nodes` | array of strings | No       | A list of nodes to fetch details for. If empty, returns the base graph schema. |

### Example

```plaintext
Help me understand the relationships between projects in my-group.
```
