---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Enable Orbit for a group and run your first query. Covers both the remote indexer (GitLab.com) and the local indexer developer preview.
title: Get started with Orbit
---

## Remote indexer

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

The remote indexer runs on GitLab-hosted infrastructure and is the primary path for
GitLab.com Premium and Ultimate customers.

### Prerequisites

- You must be an Owner of the top-level group you want to enable Orbit on.
- Your group must be on GitLab.com on a Premium or Ultimate plan.

Orbit indexes top-level groups only. Subgroups and projects inherit indexing automatically.

### Step 1: Enable Orbit

1. On the left sidebar, select **Search or go to** and find your top-level group.
1. Select **Settings > General**.
1. Expand **Orbit**.
1. Turn on the **Enable Orbit** toggle.
1. Select **Save changes**.

Orbit begins indexing your group immediately. Initial indexing takes a few minutes
for small groups and up to 30 minutes for groups with thousands of projects.

Check indexing status at any time:

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

### Use Orbit

| Access method | Description |
|---|---|
| [Duo Agent Platform](access/duo.md) | Ask questions in natural language via the GitLab UI |
| [MCP](access/mcp.md) | Connect Claude Code, Codex, and other agentic tools |
| [REST API](access/api.md) | Query from scripts, CI pipelines, or custom tooling |

MCP and REST API queries consume GitLab Credits. Duo Agent Platform queries are zero-rated.

### Step 2: Run your first query

**Duo Agent Platform (no setup required):**

If you have GitLab Duo Developer, the Orbit agent is available immediately.

1. On the left sidebar, select **GitLab Duo**.
1. Select **Orbit**.
1. Ask a question: "What are the most active projects in my group?"

Duo queries are zero-rated and do not consume GitLab Credits.

**MCP (Claude Code, Codex, other agents):**

See [Use Orbit via MCP](access/mcp.md) for setup. Once configured you have two tools:
`query_graph` and `get_graph_schema`.

**REST API:**

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{
    "query_type": "traversal",
    "node": {
      "id": "p",
      "entity": "Project",
      "columns": ["name", "full_path"]
    },
    "limit": 10
  }' \
  "https://gitlab.com/api/v4/orbit/query"
```

See [REST API reference](access/api.md) for full documentation.

## Local indexer

{{< details >}}

- Tier: Free
- Offering: All tiers, self-managed and GitLab.com
- Status: Developer preview

{{< /details >}}

> [!note]
> The local indexer is an early developer preview. It must be built from source and has
> no UI, no MCP integration, and no daemon process yet.

The local indexer lets you build and query an Orbit knowledge graph on your own machine
using DuckDB as the storage backend. It is the path for Community Edition users and teams
that cannot use the GitLab-hosted service.

### Prerequisites

- [Rust toolchain](https://rustup.rs/) (stable)
- [`mise`](https://mise.jdx.dev/) for tool management
- A local GitLab repository to index

### Step 1: Build the CLI

Clone the Orbit repository and build the CLI:

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

The compiled binary is placed in `target/release/orbit`.

### Step 2: Index a repository

Point the indexer at a local repository:

```shell
./target/release/orbit index --path /path/to/your/repo
```

Orbit parses the repository and writes a DuckDB graph file to `~/.orbit/graph.db` by default.

### Step 3: Query the graph

```shell
./target/release/orbit query --query '{
  "query_type": "traversal",
  "node": {
    "id": "f",
    "entity": "File",
    "columns": ["path", "language"]
  },
  "limit": 20
}'
```

### What the local indexer supports

- Code indexing: definitions, imports, cross-file references
- All [supported languages](indexing.md#supported-languages)
- Local DuckDB query execution

The local indexer does not index SDLC data (merge requests, pipelines, work items).
SDLC indexing requires a connected GitLab instance.

## What to try next

- [What Orbit indexes](indexing.md): understand coverage before writing queries.
- [Schema reference](schema.md): explore the 24 node types and their properties.
- [Cookbook](cookbook.md): copy-paste queries for common use cases.
