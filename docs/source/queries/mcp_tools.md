---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use Orbit MCP tools to query the knowledge graph and discover available entities and relationships.
title: Orbit MCP tools
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

## `query_graph`

Query the knowledge graph and return matching nodes, relationships, and aggregations.

| Parameter | Type   | Required | Description |
|-----------|--------|----------|-------------|
| `query`   | object | Yes      | An Orbit [query language object](query_language.md). Valid query types: `traversal`, `aggregation`, `path_finding`, and `neighbors`. |

Example request:

```json
{
  "query": {
    "query_type": "traversal",
    "node": {
      "id": "p",
      "entity": "Project",
      "columns": ["name", "full_path"],
      "filters": {
        "name": { "op": "is_not_null" }
      }
    },
    "limit": 1
  }
}
```

Example response:

```json
{
  "result": {
    "format_version": "1.2.0",
    "query_type": "traversal",
    "nodes": [
      {
        "type": "Project",
        "id": "00000001",
        "name": "example-project",
        "full_path": "example-group/example-project"
      }
    ],
    "edges": []
  },
  "query_type": "traversal",
  "raw_query_strings": null,
  "row_count": 1
}
```

## `get_graph_schema`

Return the Orbit graph schema so agents can understand which entities, relationships, and properties are available.

| Parameter     | Type             | Required | Description |
|---------------|------------------|----------|-------------|
| `expand_nodes` | array of strings | No       | A list of nodes to fetch details for. If empty, returns the base graph schema. |

Example request:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "id": "1",
  "params": {
    "name": "get_graph_schema",
    "arguments": {
      "expand_nodes": ["Project"]
    }
  }
}
```

Example response:

```json
{
  "schema_version": "0.1",
  "domains": [
    {
      "name": "ci",
      "description": "Entities related to CI/CD pipelines, stages, jobs, deployments, environments, and runners.",
      "node_names": ["Deployment", "Environment", "Job", "Pipeline", "Runner", "Stage"]
    },
    {
      "name": "core",
      "description": "Entities which represent the structure of a GitLab instance.",
      "node_names": ["Group", "Note", "Project", "User"]
    }
  ],
  "nodes": [
    {
      "name": "MergeRequest",
      "domain": "code_review",
      "description": "A merge request for code review and merging changes into a target branch",
      "primary_key": "id",
      "label_field": "title"
    },
    {
      "name": "Project",
      "domain": "core",
      "description": "A GitLab project/repository",
      "primary_key": "id",
      "label_field": "name",
      "properties": [
        { "name": "id", "data_type": "Int", "nullable": false, "enum_values": [] },
        { "name": "full_path", "data_type": "String", "nullable": true, "enum_values": [] },
        { "name": "name", "data_type": "String", "nullable": true, "enum_values": [] },
        { "name": "visibility_level", "data_type": "Enum", "nullable": false, "enum_values": ["private", "internal", "public"] },
        { "name": "archived", "data_type": "Bool", "nullable": false, "enum_values": [] },
        { "name": "star_count", "data_type": "Int", "nullable": false, "enum_values": [] }
      ],
      "style": { "size": 40, "color": "#3B82F6" },
      "outgoing_edges": [],
      "incoming_edges": ["CONTAINS", "CREATOR", "IN_PROJECT", "MEMBER_OF"]
    },
    {
      "name": "User",
      "domain": "core",
      "description": "A GitLab user account",
      "primary_key": "id",
      "label_field": "username"
    }
  ],
  "edges": [
    {
      "name": "AUTHORED",
      "description": "Authorship relationship between users and entities",
      "variants": [
        { "source_type": "User", "target_type": "MergeRequest" },
        { "source_type": "User", "target_type": "WorkItem" }
      ]
    },
    {
      "name": "CREATOR",
      "description": "User created project",
      "variants": [
        { "source_type": "User", "target_type": "Project" }
      ]
    },
    {
      "name": "IN_PROJECT",
      "description": "Project association for entities",
      "variants": [
        { "source_type": "MergeRequest", "target_type": "Project" },
        { "source_type": "Pipeline", "target_type": "Project" }
      ]
    },
    {
      "name": "MEMBER_OF",
      "description": "A user is a member of a group or project",
      "variants": [
        { "source_type": "User", "target_type": "Group" },
        { "source_type": "User", "target_type": "Project" }
      ]
    }
  ]
}
```
