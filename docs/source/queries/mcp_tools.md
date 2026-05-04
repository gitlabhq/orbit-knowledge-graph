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
The query DSL grammar is also exposed through `get_graph_schema` with
`include=["dsl"]` so MCP clients that truncate tool descriptions can still
discover it. The grammar stays inline on `query_graph` for one release cycle to
give existing consumers time to migrate; a follow-up MR strips it.

| Parameter | Type   | Required | Description |
|-----------|--------|----------|-------------|
| `query`   | object | Yes      | An [Orbit query language object](query_language.md). Valid query types: `traversal`, `aggregation`, `path_finding`, and `neighbors`. |
| `format`  | string | No       | `llm` (default) returns compact text. `raw` returns structured JSON. |

Example request:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "id": "1",
  "params": {
    "name": "query_graph",
    "arguments": {
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
          "raw_query_strings": null,
          "row_count": 1
        }
      }
    ],
    "isError": false
  }
}
```

## `get_graph_schema`

Return the Orbit graph schema so agents can understand which entities, relationships, and properties are available. Pass `include=["dsl"]` to also fetch the query DSL grammar, or `include=["response_format"]` for the formatter output JSON Schema, in the same call. This keeps the MCP tool surface small while still letting agents fetch ontology, query input, and query output shape together.

| Parameter      | Type             | Required | Description |
|----------------|------------------|----------|-------------|
| `expand_nodes` | array of strings | No       | A list of nodes to fetch details for. If empty, returns the base graph schema. Pass `["*"]` to expand every node. |
| `include`      | array of strings | No       | Extra blocks to merge into the response. Allowed values: `dsl` (the query input grammar) and `response_format` (the formatter output JSON Schema and its semver). |
| `format`       | string           | No       | `llm` (default) returns compact TOON. `raw` returns structured JSON. |

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
      }
    ],
    "isError": false
  }
}
```

### Fetching the DSL grammar and response format

The query input grammar and the formatter output JSON Schema both ride on
`get_graph_schema` via the `include` array, instead of as standalone tools.
This keeps the MCP tool surface to two entries while still letting agents
fetch ontology, input shape, and output shape in a single call.

Example request:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "id": "1",
  "params": {
    "name": "get_graph_schema",
    "arguments": {
      "include": ["dsl", "response_format"],
      "format": "raw"
    }
  }
}
```

Raw response payload (truncated):

```json
{
  "schema_version": "0.1",
  "domains": [...],
  "nodes": [...],
  "edges": [...],
  "dsl": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "GraphQueryAsJSON",
    "..." : "..."
  },
  "response_format": {
    "schema": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "title": "GKG unified query response",
      "..." : "..."
    },
    "version": "1.2.0"
  }
}
```

The `response_format.version` matches `config/RAW_OUTPUT_FORMAT_VERSION` and
the `format_version` field stamped on every query response.
