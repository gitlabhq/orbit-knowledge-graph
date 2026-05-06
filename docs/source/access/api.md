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

API calls consume **GitLab Credits** from your subscription. Each call to
`POST /api/v4/orbit/query` uses credits. The other endpoints are free.

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v4/orbit/query` | Execute a graph query |
| `GET` | `/api/v4/orbit/schema` | Fetch the current schema |
| `GET` | `/api/v4/orbit/status` | Check indexing status |
| `GET` | `/api/v4/orbit/tools` | List available MCP tool definitions |

## POST /api/v4/orbit/query

Execute a graph query using the Orbit query DSL.

**Request:**

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '<query_json>' \
  "https://gitlab.com/api/v4/orbit/query"
```

See the [query language reference](../queries/query_language.md) for the full DSL.

**Example - find projects with the most pipeline failures:**

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{
    "query_type": "aggregation",
    "nodes": [
      {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
      {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
    ],
    "relationships": [
      {"type": "HAS_PIPELINE", "from": "p", "to": "pl"}
    ],
    "aggregations": [
      {"function": "count", "target": "pl", "group_by": "p", "alias": "failed_pipelines"}
    ],
    "aggregation_sort": {"agg_index": 0, "direction": "DESC"},
    "limit": 10
  }' \
  "https://gitlab.com/api/v4/orbit/query"
```

**Response:**

```json
{
  "data": [
    {
      "p": {"name": "payments-api", "full_path": "my-org/payments-api"},
      "failed_pipelines": "47"
    },
    {
      "p": {"name": "auth-service", "full_path": "my-org/auth-service"},
      "failed_pipelines": "31"
    }
  ],
  "meta": {
    "total": 10,
    "cursor": null
  }
}
```

## GET /api/v4/orbit/schema

Returns the current ontology: all node types, their properties and types,
and all relationship types.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/schema"
```

Use this to discover available entity types and properties before writing queries.

## GET /api/v4/orbit/status

Returns the indexing status for groups where Orbit is enabled.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

**Example response:**

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

## GET /api/v4/orbit/tools

Returns the MCP tool definitions for `query_graph` and `get_graph_schema`
in a format compatible with MCP clients.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/tools"
```
