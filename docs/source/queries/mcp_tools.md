---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use Orbit MCP tools to query the knowledge graph, inspect schema, and check indexing status.
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

Orbit exposes MCP tools so AI agents can query GitLab graph data through a
structured tool contract.

Available tools:

- `query_graph`: run an Orbit JSON query.
- `get_graph_schema`: retrieve available nodes, relationships, and properties.
- `get_graph_status`: retrieve indexing status for a group or project.

## `query_graph`

Runs a query against the deployed Orbit service and returns matching nodes,
relationships, aggregations, or paths.

Parameters:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | object | Yes | An [Orbit query language](query_language.md) object. |
| `format` | string | No | Response format. Use `raw` for structured JSON or `llm` for agent-oriented text. Default is `llm`. |

Example request:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "id": "1",
  "params": {
    "name": "query_graph",
    "arguments": {
      "format": "raw",
      "query": {
        "query_type": "traversal",
        "node": {
          "id": "p",
          "entity": "Project",
          "columns": ["name", "full_path"]
        },
        "limit": 5
      }
    }
  }
}
```

Example response:

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "result": {
    "content": [
      {
        "type": "text",
        "text": {
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
          "row_count": 1
        }
      }
    ],
    "isError": false
  }
}
```

## `get_graph_schema`

Returns the Orbit graph schema so agents can see which entities,
relationships, and properties are available before writing a query.

Parameters:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `expand_nodes` | array of strings | No | Node names to expand. Use an empty array for the base schema. |
| `format` | string | No | Response format. Use `raw` or `llm`. Default is `llm`. |

Example request:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "id": "1",
  "params": {
    "name": "get_graph_schema",
    "arguments": {
      "format": "raw",
      "expand_nodes": ["Project"]
    }
  }
}
```

Example response:

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "result": {
    "content": [
      {
        "type": "text",
        "text": {
          "schema_version": "0.1",
          "domains": [
            {
              "name": "core",
              "description": "Entities that represent the structure of a GitLab instance.",
              "node_names": ["Group", "Project", "User"]
            }
          ],
          "nodes": [
            {
              "name": "Project",
              "domain": "core",
              "description": "A GitLab project or repository",
              "primary_key": "id",
              "label_field": "name"
            }
          ],
          "edges": [
            {
              "name": "IN_PROJECT",
              "description": "Project association for entities"
            }
          ]
        }
      }
    ],
    "isError": false
  }
}
```

## `get_graph_status`

Returns indexing progress and entity counts for a group or project.

Parameters:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `full_path` | string | Yes if `namespace_id` and `project_id` are not provided | Full path of a group or project. |
| `namespace_id` | integer | Yes if `full_path` and `project_id` are not provided | Group namespace ID. |
| `project_id` | integer | Yes if `full_path` and `namespace_id` are not provided | Project ID. |
| `format` | string | No | Response format. Use `raw` or `llm`. Default is `llm`. |

Provide exactly one of `full_path`, `namespace_id`, or `project_id`.

Example request:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "id": "1",
  "params": {
    "name": "get_graph_status",
    "arguments": {
      "format": "raw",
      "full_path": "example-group/example-project"
    }
  }
}
```

Example response:

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "result": {
    "content": [
      {
        "type": "text",
        "text": {
          "projects": {
            "indexed": 1,
            "total_known": 1
          },
          "domains": [
            {
              "name": "core",
              "items": [
                { "name": "Project", "count": 1 },
                { "name": "User", "count": 42 }
              ]
            }
          ],
          "indexing": {
            "state": "indexed",
            "last_started_at": "2026-04-29T12:00:00Z",
            "last_completed_at": "2026-04-29T12:02:00Z",
            "last_duration_ms": 120000,
            "last_error": null
          }
        }
      }
    ],
    "isError": false
  }
}
```
