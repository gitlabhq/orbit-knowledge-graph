---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the GitLab Orbit graph directly using the REST API. Reference for all four endpoints with authentication requirements and example requests.
title: REST API
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

The GitLab Orbit REST API lets you query the graph directly from scripts,
CI pipelines, or custom tooling.

## Authentication

All endpoints require a GitLab personal access token with `read_api` scope,
passed as a Bearer token:

```shell
--header "Authorization: Bearer <your_token>"
```

Results are scoped to entities the token owner can access in GitLab.

## Billing

During the beta, API calls do not consume GitLab Credits.

When GitLab Orbit is generally available, each call to `POST /api/v4/orbit/query`
consumes GitLab Credits from your subscription. The other endpoints stay free.
Credit rates are published in
[GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/)
before charging begins.

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v4/orbit/query` | Execute a graph query |
| `GET` | `/api/v4/orbit/schema` | Fetch the current schema |
| `GET` | `/api/v4/orbit/status` | Check indexing status |
| `GET` | `/api/v4/orbit/tools` | List available MCP tool definitions |

## Query endpoint

Execute a graph query. The instance decides the query language: a JSON Query DSL object by default, or read-only GQL text when GitLab has enabled GQL for you.

The request body contains:

- `query`: A JSON Query DSL object, or a text string when GQL is enabled. A query whose shape does not match the enabled language is rejected.
- `response_format`: Optional response format. Use `raw` for structured JSON, or `llm`
  for compact text optimized for AI agents. Default: `raw`.

The GitLab Orbit CLI explicitly sends `llm` by default.

For example:

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{"query": <query_json>, "response_format": "raw"}' \
  "https://gitlab.com/api/v4/orbit/query"
```

See the [query language reference](../queries/query-language.md) for the full DSL.

The `orbit_gql_queries` feature flag in Rails selects the language. It is off by default, which keeps JSON.
The flag can target your user or a root group.
For the group gate, you need at least the Developer role in that group or one of its subgroups.
With the flag on, the query endpoint, the named-query catalog, and the dashboard editor all use GQL text.

To send read-only query text or inspect its ontology with the flag on:

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{"query":"MATCH (u:User {id: 1}) RETURN u.username LIMIT 1","response_format":"llm"}' \
  "https://gitlab.com/api/v4/orbit/query"

curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{"query":"CALL db.schema(\"MergeRequest\")","response_format":"raw"}' \
  "https://gitlab.com/api/v4/orbit/query"
```

The CLI accepts GQL text directly, without a language option:

```shell
glab orbit query 'CALL db.schema()'
glab orbit query 'MATCH (u:User {id: 1}) RETURN u'
```

An existing file path, `-`, or no argument still reads a JSON request envelope from a file or stdin.

The query text language, based on openCypher 9 syntax, is documented in the [GitLab Orbit query frontend](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/docs/design-documents/querying/orbit_query_frontend.md) design document.

### Example request

For example, a request to find projects with the most pipeline failures:

Put the request body in `request.json`:

```json orbit-query
{
  "query": {
    "query_type": "aggregation",
    "nodes": [
      {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
      {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
    ],
    "relationships": [
      {"type": "IN_PROJECT", "from": "pl", "to": "p"}
    ],
    "group_by": ["p"],
    "aggregations": [
      {
        "count": "pl",
        "as": "failed_pipelines"
      }
    ],
    "aggregation_sort": "-failed_pipelines",
    "limit": 10
  },
  "response_format": "raw"
}
```

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data @request.json \
  "https://gitlab.com/api/v4/orbit/query"
```

An example response:

```json
{
  "result": {
    "format_version": "2.0.0",
    "query_type": "aggregation",
    "nodes": [],
    "edges": [],
    "group_columns": [
      {
        "name": "p",
        "kind": "node",
        "node": "p",
        "entity": "Project"
      }
    ],
    "columns": [
      {
        "name": "failed_pipelines",
        "function": "count",
        "target": "pl"
      }
    ],
    "rows": [
      {
        "p": {
          "type": "Project",
          "id": "1",
          "properties": {
            "name": "payments-api",
            "full_path": "my-org/payments-api"
          }
        },
        "failed_pipelines": 47
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

Returns the indexing status for groups where GitLab Orbit is enabled.

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

Returns the MCP tool definitions for `list_commands` and `invoke_command`
in a format compatible with MCP clients.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/tools"
```
