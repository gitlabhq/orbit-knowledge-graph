---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query GitLab Orbit from the command line with glab orbit remote, available in glab 1.94 or later. The glab orbit setup helper is planned for a future glab release.
title: Use GitLab Orbit with the GitLab CLI (`glab`)
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

<!-- -->

> [!disclaimer]

The [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) is the canonical way to set up and
query GitLab Orbit from the command line.

`glab orbit` runs the managed `orbit` binary. It forwards each command to the
binary, which `glab` downloads, verifies, and keeps up to date for you. Run
`glab orbit remote <command> --help` for the binary's own command reference.

- `glab orbit remote`: query the GitLab Orbit Remote REST API. `glab` injects
  your GitLab credential automatically. Available in `glab` 1.94 or later.
- `glab orbit setup`: guided onboarding that installs the GitLab Orbit skill and
  configures your AI agent.

## Prerequisites

- GitLab Orbit is [enabled on your group](../getting-started.md).
- `glab` is installed and authenticated:

  ```shell
  glab auth login
  ```

- Your user has access to at least one top-level group with GitLab Orbit enabled.

## Set up your AI agent

`glab orbit setup` configures AI coding agents (Claude Code, OpenCode, Cursor,
Codex, Gemini CLI) to consult the graph, and installs the GitLab Orbit skill:

```shell
glab orbit setup
```

To connect an MCP client instead, [configure it manually](mcp.md#connect-your-mcp-client).

## Query GitLab Orbit from the command line

Use `glab orbit remote` to call the GitLab Orbit Remote API directly.
Useful for scripting, debugging, and exploring the schema before writing queries.
Requires `glab` 1.94 or later.

`glab` resolves your credential and passes it to the binary, so no extra
authentication step is needed. Use `--hostname` to target a specific GitLab
instance, and `--yes` to skip the one-time run confirmation in scripts.

| Subcommand | Endpoint | Purpose |
|------------|----------|---------|
| `glab orbit remote status` | `GET orbit/status` | Cluster health. |
| `glab orbit remote schema [node...]` | `GET orbit/schema` | Graph ontology. Positional args expand specific nodes. |
| `glab orbit remote dsl` | `GET orbit/schema/dsl` | Query DSL JSON Schema. The source of truth for the query body shape. |
| `glab orbit remote tools` | `GET orbit/tools` | MCP tool manifest with the full DSL JSON Schema. |
| `glab orbit remote query [file\|-]` | `POST orbit/query` | Run a query from a file or stdin. |
| `glab orbit remote graph-status` | `GET orbit/graph_status` | Indexing progress for a namespace, project, or full path. |

### Discover the schema

```shell
glab orbit remote status
glab orbit remote schema
glab orbit remote schema MergeRequest Project
glab orbit remote dsl
glab orbit remote tools
```

### Run a query

Replace `your-group` with your own group path. This query returns the first
five projects in that group:

Put the request body in `query.json`:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "filters": {
        "full_path": {"starts_with": "your-group/"}
      }
    }],
    "limit": 5
  }
}
```

```shell
glab orbit remote query query.json
```

The `--response-format` flag maps to the body's `response_format`:

- `--response-format llm` - compact text optimized for AI agent consumption.
- `--response-format raw` - structured JSON, suitable for piping to `jq`.

If `--response-format` is unset, the body's `response_format` wins, with `llm`
as the final fallback.

### Check indexing progress

Pass exactly one scope flag:

```shell
glab orbit remote graph-status --full-path your-group/your-project
glab orbit remote graph-status --namespace-id 24
glab orbit remote graph-status --project-id 2
```

## Exit codes

`glab orbit remote` maps HTTP errors to stable exit codes so scripts and agents
can branch on them without parsing stderr.

| Status | Exit code | Meaning |
|--------|-----------|---------|
| `200` | `0` | Success. |
| `404` | `2` | `knowledge_graph` feature flag is off, or path typo. |
| `401` | `3` | Missing or expired token. |
| `403` | `4` | No Knowledge Graph enabled namespaces available. |
| `429` | `5` | Rate limited. Inspect `Retry-After` and back off. |
| Other | `1` | Unstructured error. Response body, if any, is included. |

## Billing

`glab orbit remote query` consumes GitLab Credits the same way as MCP queries.
`status`, `schema`, `tools`, and `graph-status` calls are free.
