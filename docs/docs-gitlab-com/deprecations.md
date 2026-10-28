---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: What changed in GitLab Orbit between the beta and general availability, and what to use instead.
title: GitLab Orbit deprecations and removals
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

Some GitLab Orbit commands, endpoints, and settings changed for general availability.
Move your scripts, agents, and integrations to the generally available release before you upgrade.

## Minimum versions

GitLab Orbit needs these versions or later:

| Component | Minimum version |
|-----------|-----------------|
| GitLab CLI (`glab`) | 1.126.0 |
| GitLab Orbit CLI (`orbit`) | 0.163.0 |
| GitLab Orbit Helm chart | 6.2.0 |
| GitLab Self-Managed | TBD |

If `glab` installed a GitLab Orbit CLI earlier than 0.163.0, `glab` 1.126.0 and later install the latest version.
To update the GitLab Orbit CLI, run `glab orbit --update`.

On GitLab.com and GitLab 19.6, `orbit schema` and `orbit status` fail in GitLab Orbit CLI 0.160.0 and earlier.

## GitLab Orbit Helm chart

{{< details >}}

- Offering: GitLab Self-Managed

{{< /details >}}

Before you upgrade the chart, read [Version compatibility](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts#version-compatibility) in the chart README.

Chart 4.0.0 renamed the chart from `gkg` to `orbit`.
The `gkg` chart gets no new releases.
Change your chart reference to `oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/orbit`.

Chart 5.0.0 needs GitLab Orbit 0.150.0 or later.
Chart 5.0.0 removes expanded data from log lines by default.
To keep the expanded data, set `metrics.expandedLogs.globalOverride: true`.

Chart 6.0.0 turns off Snowplow analytics by default.
To keep the analytics on, set `analytics.enabled: true`.

GitLab Orbit 0.143.0 removed the `/live` and `/ready` probe endpoints.
Charts earlier than 3.0.0 do not work with GitLab Orbit 0.143.0 or later.

## Query language

GitLab 19.6 removed the JSON query DSL.
GitLab.com removed it first.
GitLab 19.4 and 19.5 still accept JSON queries.
Write your queries in [GQL](queries/query-language.md).

To convert your JSON queries, tell your agent to load the `orbit-gql-migration` skill with `glab orbit skills get orbit-gql-migration`.

| JSON query DSL | GQL |
|----------------|-----|
| `query_type: "traversal"` or `"aggregation"` | Omit `query_type`. |
| `query_type: "neighbors"` | `(n:Label {id: 123})-[:TYPE]->(m)` |
| `query_type: "path_finding"` | `MATCH path = ANY SHORTEST (a)-[:TYPE*1..3]->(b) RETURN path` |
| `node_ids: [123]` | `(n:Label {id: 123})` |
| `node_ids: [123, 456]` | `WHERE n.id IN [123, 456]` |
| `filters.<property>: <value>` | `WHERE n.<property> = <value>` |
| `relationships` with `from` and `to` | `(a)-[:TYPE]->(b)` |
| `hops: [1, 3]` | `(a)-[:TYPE*1..3]->(b)` |
| `columns` | `RETURN n.<property>` |
| `order_by: "-n.<property>"` | `ORDER BY n.<property> DESC` |
| `limit` | `LIMIT <number>` |
| `aggregations` and `group_by` | `RETURN n.<property>, count(m) AS <alias>` |
| `aggregation_sort: "-<alias>"` | `ORDER BY <alias> DESC` |

## GitLab CLI commands

GitLab Orbit CLI 0.120.0 removed the `glab orbit remote` and `glab orbit local` commands.
`glab` passes every `glab orbit` command to the GitLab Orbit CLI.
`glab` 1.105.0 renamed the `--format` option of `glab orbit remote query` to `--response-format`.

| Removed command | Use instead |
|-----------------|-------------|
| `glab orbit remote query` | `glab orbit query` |
| `glab orbit remote query --format` | `glab orbit query --response-format` |
| `glab orbit remote schema` | `glab orbit schema` |
| `glab orbit remote status` | `glab orbit status` |
| `glab orbit remote graph-status` | `glab orbit status --full-path <path>` |
| `glab orbit remote tools` | No replacement |
| `glab orbit remote dsl` | No replacement |
| `glab orbit local <command>` | `glab orbit workspace <command>` |

## Agent skill

GitLab Orbit CLI 0.144.0 renamed the `orbit-cli` skill to `orbit`.
Skills from the beta use the removed `glab orbit remote` commands.

To replace the skill in your agents, run `glab orbit setup`.
Setup removes the old `orbit-cli` skill.
Setup keeps an `orbit-cli` skill with files that differ from the current release.
Delete that skill by hand.

## MCP endpoint tools

GitLab 19.3 removed three tools from the MCP endpoint at `/api/v4/orbit/mcp`.
A call to a removed tool returns an `Unknown tool` error.

| Removed tool | Use instead |
|--------------|-------------|
| `query_graph` | `invoke_command` with `command_name` set to `query_graph` |
| `get_graph_schema` | `invoke_command` with `command_name` set to `get_graph_schema` |
| `get_graph_status` | No replacement |

For example, the arguments of `invoke_command` for a query are:

```json
{"command_name": "query_graph", "parameters": {"query": "MATCH (p:Project {full_path: 'gitlab-org/gitlab'}) RETURN p.name"}}
```

## REST API endpoints

GitLab 19.6 removed these REST API endpoints.
GitLab.com removed them first.

| Removed endpoint | Use instead |
|------------------|-------------|
| `GET /api/v4/orbit/graph_status` | GraphQL `Group.orbit` or `Project.orbit` |
| `GET /api/v4/orbit/schema` | GraphQL `Query.orbit.schema` |
| `GET /api/v4/orbit/status` | GraphQL `Query.orbit.serviceStatus` |
| `GET /api/v4/orbit/query/templates` | GraphQL `Query.orbit.queryTemplates` |
| `GET /api/v4/orbit/tools` | MCP `tools/list` at `/api/v4/orbit/mcp` |
| `GET /api/v4/orbit/schema/format` | No replacement |
| `GET /api/v4/orbit/schema/dsl` | No replacement |

## GraphQL API fields

GitLab 19.6 removed these GraphQL fields and mutations.
GitLab.com removed them first.

| Removed field or mutation | Use instead |
|---------------------------|-------------|
| `Group.knowledgeGraphEnabled` | `Group.orbit.enabled` |
| `Group.knowledgeGraphAvailable` | `Group.userPermissions.updateKnowledgeGraphSetting` |
| `Group.orbitAccess` | `Group.orbit.access` |
| `Group.orbitTrial` | `Group.orbit.trial` |
| `Query.knowledgeGraphExcludedNamespaces` | `Query.orbit.excludedGroups` |
| `knowledgeGraphExcludedNamespaceCreate` | `orbitExcludedGroupCreate` |
| `knowledgeGraphExcludedNamespaceDestroy` | `orbitExcludedGroupDestroy` |

## GitLab Orbit CLI commands

GitLab Orbit CLI 0.160.0 groups the local graph commands under `orbit workspace`.
GitLab Orbit does not support SQL on the local graph.

| Command | Status | Use instead |
|---------|--------|-------------|
| `orbit index` | Deprecated | `orbit workspace index` |
| `orbit sql` | Deprecated | `orbit grep` or `orbit context` |
| `orbit list` | Deprecated | `orbit workspace list` |
| `orbit schema <table>` | Deprecated | No replacement |
| `orbit mcp serve` | Deprecated | `orbit workspace mcp serve` |
| `orbit graph-status` | Deprecated | `orbit status --full-path <path>` |
| `orbit ontology` | Deprecated | `orbit schema` |
| `orbit skill` | Deprecated | `orbit skills` |
| `orbit dsl` | Removed in 0.160.0 | No replacement |
| `orbit tools` | Removed in 0.160.0 | No replacement |
| `orbit repo-map` | Removed in 0.160.0 | No replacement |

In GitLab Orbit CLI 0.160.0 and later, `orbit schema` with no argument shows the graph schema of your GitLab instance.

## Output of `orbit status`

GitLab Orbit CLI 0.161.0 reads the indexing progress from the GitLab GraphQL API.
This change applies when you pass `--full-path`, `--namespace-id`, or `--project-id`.
The JSON output uses the GraphQL field names.

| Old field | New field |
|-----------|-----------|
| `projects.indexed` | `indexing.projects.indexed` |
| `projects.total_known` | `indexing.projects.totalKnown` |
| `projects.gaps` | `indexing.projects.gaps` |
| `sdlc_indexing` | `indexing.areas` |
| `code_indexing` | `indexing.areas` |
| `domains` | `itemCounts.domains` |
| `domains.items` | `itemCounts.domains.entities` |
| `domains.items.state` | Removed |
| `indexing.last_completed_at` | Removed |
| `indexing.last_error` | Removed |

The `indexing.state` values are uppercase, for example `READY`.

The `indexing.areas` list has one entry for each domain.

The `count` values are strings.

If your GitLab instance has no GraphQL `orbit` field, the CLI reads the REST API.
The output then keeps the old field names.

Without `--full-path`, `--namespace-id`, or `--project-id`, `orbit status` shows the GraphQL `serviceStatus` field.

## Output of `orbit schema`

GitLab Orbit CLI 0.161.0 reads the schema from the GitLab GraphQL API.
The JSON output uses the GraphQL field names.
The REST fallback of `orbit status` also applies to `orbit schema`.

| Old field | New field |
|-----------|-----------|
| `nodes.domain` | `nodes.domain.name` |
| `nodes.primary_key` | `nodes.primaryKey` |
| `nodes.label_field` | `nodes.labelProperty` |
| `nodes.properties` | `nodes.properties.nodes` |
| `nodes.properties.data_type` | `nodes.properties.nodes.dataType` |
| `nodes.properties.enum_values` | `nodes.properties.nodes.enumValues` |
| `edges.variants` | `edges.endpoints.nodes` |
| `edges.variants.source_type` | `edges.endpoints.nodes.source.name` |
| `edges.variants.target_type` | `edges.endpoints.nodes.target.name` |
| `schema_version` | Removed |
| `domains` | Removed |
| `nodes.style` | Removed |
| `nodes.outgoing_edges` | Removed |
| `nodes.incoming_edges` | Removed |

## GitLab Orbit server configuration

{{< details >}}

- Offering: GitLab Self-Managed

{{< /details >}}

GitLab Orbit 0.128.0 deprecated the `metrics.prometheus.port` setting.
Use `probe_server.bind_address` instead.

GitLab Orbit fails to start when the two settings point to different ports.
