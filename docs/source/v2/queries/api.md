---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the GitLab Orbit graph with the REST API. Reference for the endpoints, authentication, and example requests.
title: GitLab Orbit REST API
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.

{{< /history >}}

Use the GitLab Orbit REST API to query the graph from scripts, scheduled reports, and custom tools.
For the GitLab API reference, see [GitLab Orbit API](https://docs.gitlab.com/api/orbit/).
For the GitLab Orbit data of the instance and of your groups in the GraphQL API, see
[`Query.orbit`](https://docs.gitlab.com/api/graphql/reference/#queryorbit).

## Authentication

Send a personal access token with the `read_api` scope, or a
[fine-grained personal access token](../security.md#fine-grained-personal-access-tokens), as a Bearer token:

```shell
--header "Authorization: Bearer <your_token>"
```

Results include only what the token owner can access in GitLab.
For automation without a personal account, use a [service account](../security.md#service-accounts).

## Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v4/orbit/query` | Run a graph query |
| `POST` | `/api/v4/orbit/query/:name` | Run a [named query](#named-queries) |
| `GET` | `/api/v4/orbit/query/templates` | List the named queries that take no parameters |
| `GET` | `/api/v4/orbit/schema` | Get the schema |
| `GET` | `/api/v4/orbit/schema/dsl` | Get the JSON Schema of the deprecated JSON query domain-specific language (DSL). Returns `404` when you use GQL. Use `CALL db.schema()` instead. |
| `GET` | `/api/v4/orbit/schema/format` | Get guidance for the response formats |
| `GET` | `/api/v4/orbit/status` | Check your access and the cluster health |
| `GET` | `/api/v4/orbit/graph_status` | Check indexing progress for a group or project |
| `GET` | `/api/v4/orbit/skills` | List the agent skills on the server |
| `GET` | `/api/v4/orbit/skills/:name` | Get one agent skill |
| `GET` | `/api/v4/orbit/skills/:name/*path` | Get one file of an agent skill |
| `GET` | `/api/v4/orbit/tools` | List the MCP tool definitions |
| `GET` | `/api/v4/orbit/agent/commands` | List the agent commands. The REST version of the MCP `list_commands` tool. |
| `POST` | `/api/v4/orbit/agent/commands/:name` | Run an agent command. The REST version of the MCP `invoke_command` tool. |

## Query endpoint

The request body contains:

- `query`: The query text in the [GitLab Orbit query language](query-language.md).
- `response_format`: Optional. Use `raw` for structured JSON, `llm` for compact
  [TOON](https://github.com/toon-format/spec/blob/main/SPEC.md) text for agents, or `gql` for a text table.
  Default: `raw`. The GitLab Orbit CLI sends `llm` by default.

### Example request

To find the projects in a group with the most failed pipelines, put this request body in `request.json`:

```json
{
  "query": "MATCH (pl:Pipeline)-[:IN_PROJECT]->(p:Project) WHERE pl.status = 'failed' AND p.full_path STARTS WITH 'my-group/' RETURN p.name, p.full_path, count(pl) AS failed_pipelines ORDER BY failed_pipelines DESC LIMIT 10",
  "response_format": "raw"
}
```

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data @request.json \
  --url "https://gitlab.com/api/v4/orbit/query"
```

An example response:

```json
{
  "result": {
    "format_version": "5.0.4",
    "query_type": "aggregation",
    "nodes": [],
    "edges": [],
    "columns": [
      {"name": "failed_pipelines", "function": "count", "target": "pl"}
    ],
    "group_columns": [
      {"name": "p_name", "kind": "property", "node": "p", "property": "name", "entity": "Project"},
      {"name": "p_full_path", "kind": "property", "node": "p", "property": "full_path", "entity": "Project"}
    ],
    "rows": [
      {"p_name": "payments-api", "p_full_path": "my-group/payments-api", "failed_pipelines": 47}
    ],
    "pagination": {"has_more": false, "truncated": false}
  },
  "query_type": "aggregation",
  "row_count": 1
}
```

## Named queries

The server has a catalog of named queries. To run one, send its parameters:

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{"parameters": {"entity": "Project", "field": "full_path", "text": "my-group/my-project"}, "response_format": "raw"}' \
  --url "https://gitlab.com/api/v4/orbit/query/search_nodes"
```

`GET /api/v4/orbit/query/templates` lists the named queries that take no parameters, with their query text.
No endpoint lists the named queries that take parameters.
For their names and parameters, see the [named query definitions](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/tree/main/config/named_queries).

## Schema endpoint

Returns all node types and relationship types. To also get the properties and relationships of node types, pass their names in `expand`, for example `?expand=MergeRequest,Project`, or `?expand=*` for every node type.
In a query, `CALL db.schema()` returns the same information.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/schema"
```

## Status endpoint

Returns your access to GitLab Orbit and the health of the service.
For indexing progress, use the [graph status endpoint](#graph-status-endpoint).

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
  "https://gitlab.com/api/v4/orbit/graph_status?full_path=my-group/payments-api"
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

Returns the MCP tool definitions of `list_commands` and `invoke_command`.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/tools"
```

## Related topics

- [Queries](_index.md)
- [Query language](query-language.md)
- [GitLab Orbit API reference](https://docs.gitlab.com/api/orbit/)
- [GraphQL API: `Query.orbit`](https://docs.gitlab.com/api/graphql/reference/#queryorbit)
- [Security](../security.md)
