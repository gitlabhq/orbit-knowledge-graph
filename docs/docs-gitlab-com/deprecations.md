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

A deprecated item still works.
A removed item no longer works.

## Minimum versions

GitLab Orbit needs these versions or later:

| Component | Minimum version |
|-----------|-----------------|
| GitLab CLI (`glab`) | TBD |
| GitLab Orbit CLI (`orbit`) | TBD |
| GitLab Orbit Helm chart | TBD |
| GitLab Self-Managed | TBD |

To update the GitLab Orbit CLI, run `glab orbit --update`.

Earlier versions of the GitLab Orbit CLI fail to show the indexing progress of a group or project.

## GitLab Orbit Helm chart

{{< details >}}

- Offering: GitLab Self-Managed

{{< /details >}}

Chart 4.0.0 renamed the chart from `gkg` to `orbit`.
The `gkg` chart gets no new releases.
Change your chart reference to `oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/orbit`.

To upgrade from chart 3.x to 4.0.0, follow [Upgrading to 4.0](https://gitlab.com/gitlab-org/orbit/orbit-helm-charts#upgrading-to-40) in the chart README.

GitLab Orbit 0.143.0 removed the `/live` and `/ready` probe endpoints.
Charts earlier than 3.0.0 do not work with GitLab Orbit 0.143.0 or later.

## GitLab CLI commands

`glab` 1.115 removed the `glab orbit remote` and `glab orbit local` commands.
`glab orbit` passes every command to the GitLab Orbit CLI.

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

## MCP endpoint tools

GitLab 19.3 removed two tools from the MCP endpoint at `/api/v4/orbit/mcp`.
A call to a removed tool returns an `Unknown tool` error.

| Removed tool | Use instead |
|--------------|-------------|
| `query_graph` | `invoke_command` with `command_name` set to `query_graph` |
| `get_graph_schema` | `invoke_command` with `command_name` set to `get_graph_schema` |

For example, the arguments of `invoke_command` for a query are:

```json
{"command_name": "query_graph", "parameters": {"query": "MATCH (p:Project) RETURN p.name LIMIT 5"}}
```

## REST API endpoints

GitLab 19.6 removed these REST API endpoints.
GitLab.com removed them first.

| Removed endpoint | Use instead |
|------------------|-------------|
| `GET /api/v4/orbit/graph_status` | The `orbit` field of a group or project in the GraphQL API |
| `GET /api/v4/orbit/schema/dsl` | No replacement |

## Query language

The JSON query DSL is deprecated.
Write new queries in [GQL](queries/query-language.md).

| JSON query DSL | GQL |
|----------------|-----|
| `query_type` | Omit it. |
| `node_ids: [123]` | `(n:Label {id: 123})` |
| `filters.<property>: <value>` | `WHERE n.<property> = <value>` |
| `relationships` with `from` and `to` | `(a)-[:TYPE]->(b)` |
| `columns` | `RETURN n.<property>` |
| `order_by: "-n.<property>"` | `ORDER BY n.<property> DESC` |
| `limit` | `LIMIT <number>` |
| `aggregations` and `group_by` | `RETURN n.<property>, count(m) AS <alias>` |

## GitLab Orbit CLI commands

GitLab Orbit CLI 0.160.0 groups the local graph commands under `orbit workspace`.

| Command | Status | Use instead |
|---------|--------|-------------|
| `orbit index` | Deprecated | `orbit workspace index` |
| `orbit sql` | Deprecated | `orbit workspace sql` |
| `orbit list` | Deprecated | `orbit workspace list` |
| `orbit schema <table>` | Deprecated | `orbit workspace schema <table>` |
| `orbit mcp serve` | Deprecated | `orbit workspace mcp serve` |
| `orbit graph-status` | Deprecated | `orbit status --full-path <path>` |
| `orbit ontology` | Deprecated | `orbit schema` |
| `orbit skill` | Deprecated | `orbit skills` |
| `orbit dsl` | Removed in 0.160.0 | No replacement |
| `orbit tools` | Removed in 0.160.0 | No replacement |

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

## Output of `orbit schema`

GitLab Orbit CLI 0.161.0 reads the schema from the GitLab GraphQL API.
The JSON output uses the GraphQL field names.

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
