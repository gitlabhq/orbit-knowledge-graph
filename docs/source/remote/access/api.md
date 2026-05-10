---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the Orbit knowledge graph directly using the REST API. Reference for all four endpoints with authentication requirements and example requests.
title: REST API
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

<!-- -->

> [!disclaimer]

The Orbit REST API lets you query the knowledge graph directly from scripts,
CI pipelines, or custom tooling.

## Authentication

All endpoints require a GitLab personal access token with `read_api` scope,
passed as a Bearer token:

```shell
--header "Authorization: Bearer <your_token>"
```

Results are scoped to entities the token owner can access in GitLab.

## Billing

API calls consume GitLab Credits from your subscription. Each call to
`POST /api/v4/orbit/query` uses credits. The other endpoints are free.

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v4/orbit/query` | Execute a graph query |
| `GET` | `/api/v4/orbit/schema` | Fetch the current schema |
| `GET` | `/api/v4/orbit/status` | Check indexing status |
| `GET` | `/api/v4/orbit/tools` | List available MCP tool definitions |

## Query endpoint

Execute a graph query using the Orbit query DSL.

The request body contains:

- `query`: The Orbit query object.
- `format`: Optional response format. Use `raw` for structured JSON, or `llm`
  for compact text optimized for AI agents. Default: `llm`.

For example:

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{"query": <query_json>, "format": "raw"}' \
  "https://gitlab.com/api/v4/orbit/query"
```

See the [query language reference](../queries/query-language.md) for the full DSL.

### Example request

For example, a request to find projects with the most pipeline failures:

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{
    "query": {
      "query_type": "aggregation",
      "nodes": [
        {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
        {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
      ],
      "relationships": [
        {"type": "IN_PROJECT", "from": "pl", "to": "p"}
      ],
      "group_by": [{"kind": "node", "node": "p"}],
      "aggregations": [
        {
          "function": "count",
          "target": "pl",
          "alias": "failed_pipelines"
        }
      ],
      "aggregation_sort": {"column": "failed_pipelines", "direction": "DESC"},
      "limit": 10
    },
    "format": "raw"
  }' \
  "https://gitlab.com/api/v4/orbit/query"
```

An example response:

```json
{
  "result": {
    "format_version": "1.2.0",
    "query_type": "aggregation",
    "nodes": [
      {
        "type": "Project",
        "id": "1",
        "name": "payments-api",
        "full_path": "my-org/payments-api",
        "failed_pipelines": 47
      }
    ],
    "edges": [],
    "columns": [
      {
        "name": "failed_pipelines",
        "function": "count",
        "target": "pl"
      }
    ]
  },
  "query_type": "aggregation",
  "raw_query_strings": null,
  "row_count": 1
}
```

## Schema endpoint

Returns the current ontology: all node types, their properties and types,
and all relationship types.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/schema"
```

Use this to discover available entity types and properties before writing queries.

## Status endpoint

Returns the indexing status for groups where Orbit is enabled.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

An example response:

```json
{
  "status": "indexed",
  "domains": {
    "sdlc": {"indexed": true, "last_updated": "2026-05-05T14:22:00Z"},
    "code": {"indexed": true, "last_updated": "2026-05-05T14:18:00Z"}
  },
  "projects": {
    "total": 847,
    "indexed": 847
  }
}
```

## Tools endpoint

Returns the MCP tool definitions for `query_graph` and `get_graph_schema`
in a format compatible with MCP clients.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/tools"
```
