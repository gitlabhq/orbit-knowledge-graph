---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the GitLab Orbit graph directly using the REST API. Reference for all four endpoints with authentication requirements and example requests.
title: GitLab Orbit REST API
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
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

All endpoints require a GitLab personal access token with `read_api` scope, or a
[fine-grained personal access token](../security.md#fine-grained-personal-access-tokens),
passed as a Bearer token:

```shell
--header "Authorization: Bearer <your_token>"
```

Results are scoped to entities the token owner can access in GitLab.

To query from a script or CI/CD job without a personal account, use a
[service account](../security.md#service-accounts).

## Billing

For how query calls consume GitLab Credits, see [billing](../how-it-works.md#billing).

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v4/orbit/query` | Run a graph query |
| `POST` | `/api/v4/orbit/query/:name` | Run a named query from the server catalog |
| `GET` | `/api/v4/orbit/query/templates` | List the named queries |
| `GET` | `/api/v4/orbit/schema` | Get the schema |
| `GET` | `/api/v4/orbit/schema/dsl` | Get the JSON Schema of the query DSL |
| `GET` | `/api/v4/orbit/schema/format` | Get guidance for the response formats |
| `GET` | `/api/v4/orbit/status` | Check your access and the cluster health |
| `GET` | `/api/v4/orbit/graph_status` | Check indexing progress for a group or project |
| `GET` | `/api/v4/orbit/skills` | List the agent skills on the server |
| `GET` | `/api/v4/orbit/skills/:name` | Get one agent skill |
| `GET` | `/api/v4/orbit/skills/:name/*path` | Get one file of an agent skill |
| `GET` | `/api/v4/orbit/tools` | List the MCP tool definitions |
| `GET` | `/api/v4/orbit/agent/commands` | List the agent commands |
| `POST` | `/api/v4/orbit/agent/commands/:name` | Run an agent command |

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

See the [query language reference](query-language.md) for the full DSL.

The per-user `orbit_gql_queries` feature flag in Rails selects the mode. It is off by default, which accepts only JSON objects.
With the flag on, the query endpoint, the named-query catalog, and the dashboard editor all use GQL text. JSON queries then reject, including requests from existing JSON callers.
There is no public language selector. Rails sets the protobuf language for GitLab Orbit to JSON or GQL. Raw or named query kind is separate; named queries render and compile in that selected language. Agents and public REST callers do not send a language selector.

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

To send a JSON request envelope, pass `--file <path>`, or `--file -` to read it from stdin.

The GQL text language uses an OpenCypher-like syntax. It is documented in the [GitLab Orbit query frontend](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/docs/design-documents/querying/orbit_query_frontend.md) design document.

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

Returns your access to GitLab Orbit and the health of the cluster.
It does not show indexing progress. For progress, use the
[graph status endpoint](#graph-status-endpoint).

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

An example response, with most components removed:

```json
{
  "user": {"available": true},
  "system": {
    "status": "healthy",
    "timestamp": "2026-10-10T17:23:55.413124334+00:00",
    "version": "0.137.0",
    "components": [
      {
        "name": "orbit-webserver",
        "status": "healthy",
        "replicas": {"ready": 3, "desired": 3}
      }
    ]
  }
}
```

The endpoint always returns `200`. When you have no access, `user.available` is `false`
and `system` is `null`.

## Graph status endpoint

Returns indexing progress and entity counts for one group or project.
Give exactly one of `namespace_id`, `project_id`, or `full_path`.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/graph_status?full_path=my-org/payments-api"
```

An example response, with most domains removed:

```json
{
  "projects": {"indexed": 1, "total_known": 1, "gaps": 0},
  "domains": [
    {
      "name": "code_review",
      "items": [
        {"name": "MergeRequest", "count": 2773},
        {"name": "MergeRequestDiff", "count": 16705},
        {"name": "MergeRequestDiffFile", "count": 141380}
      ]
    }
  ],
  "indexing": {
    "state": "indexed",
    "last_started_at": null,
    "last_completed_at": null,
    "last_duration_ms": null,
    "last_error": null
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

## Related topics

- [Queries](_index.md)
- [GitLab Orbit query language](query-language.md)
- [Security](../security.md)
