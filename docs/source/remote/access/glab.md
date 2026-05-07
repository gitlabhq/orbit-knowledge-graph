---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Set up Orbit for your AI agent and query it from the command line with the glab CLI. Use glab orbit setup to install the skill and MCP config in one command, and glab orbit remote to call the Orbit API directly.
title: Use Orbit with the glab CLI
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

The [`glab` CLI](https://docs.gitlab.com/cli/) is the canonical way to set up and
query Orbit from the command line.

Two top-level commands:

- **`glab orbit setup`** - one-command install of the Orbit skill and MCP config
  for your AI agent.
- **`glab orbit remote`** - typed subcommands that call the Orbit Remote REST API.

## Prerequisites

- Orbit is [enabled on your group](../getting-started.md).
- `glab` is installed and authenticated:

  ```shell
  glab auth login
  ```

- Your user has access to at least one top-level group with Orbit enabled.

## Set up your AI agent

Run `glab orbit setup` to install the Orbit skill and write the MCP config.
The command prompts you to pick **Local** or **Remote** and auto-detects
your agent.

```shell
glab orbit setup
# Pick "Remote" when prompted to point the MCP config at the GitLab instance.
```

Supported agents: Claude Code, OpenCode, Cursor, Codex, Gemini CLI, Duo CLI.

| Flag | Purpose |
|------|---------|
| `--agent=<name>` | Override auto-detection (`claude-code`, `cursor`, `codex`, `opencode`, `gemini`, `duo-cli`). |
| `--skill-only` | Install the skill files only; skip MCP config. |
| `--mcp-only` | Write MCP config only; skip skill install. |
| `--dry-run` | Print what would change without writing anything. |

The skill is fetched from
[`gitlab-org/orbit/knowledge-graph`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/tree/main/skills/orbit)
and dropped into your agent's skill convention path (for example,
`~/.claude/skills/orbit` for Claude Code). The MCP config points at
`<instance>/api/v4/orbit/mcp` so the agent can call `query_graph` and
`get_graph_schema` directly.

After setup, ask your agent:

> "Check the Orbit API status."

## Query Orbit from the command line

Use `glab orbit remote` (or the `r` alias) to call the Orbit Remote API directly.
Useful for scripting, debugging, and exploring the schema before writing queries.

| Subcommand | Endpoint | Purpose |
|------------|----------|---------|
| `glab orbit remote status` | `GET orbit/status` | Cluster health. |
| `glab orbit remote schema [node...]` | `GET orbit/schema` | Graph ontology. Positional args expand specific nodes. |
| `glab orbit remote tools` | `GET orbit/tools` | MCP tool manifest with the full DSL JSON Schema. |
| `glab orbit remote query [file\|-]` | `POST orbit/query` | Run a query from a file or stdin. |
| `glab orbit remote graph-status` | `GET orbit/graph_status` | Indexing progress for a namespace, project, or full path. |

### Discover the schema

```shell
glab orbit remote status
glab orbit remote schema
glab orbit remote schema MergeRequest Project
glab orbit remote tools
```

### Run a query

```shell
echo '{"query":{"query_type":"traversal","node":{"id":"p","entity":"Project","filters":{"full_path":{"op":"starts_with","value":"your-group/"}}},"limit":5}}' \
  | glab orbit remote query -
```

The `--format` flag maps to the body's `response_format`:

- `--format llm` - compact text optimized for AI agent consumption.
- `--format raw` - structured JSON, suitable for piping to `jq`.

If `--format` is unset, the body's `response_format` wins, with `llm` as the
final fallback.

### Check indexing progress

Pass exactly one scope flag:

```shell
glab orbit remote graph-status --full-path gitlab-org/gitlab
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

`glab orbit remote query` consumes **GitLab Credits** the same way as MCP queries.
`status`, `schema`, `tools`, and `graph-status` calls are free.
