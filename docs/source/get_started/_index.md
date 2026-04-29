---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see the Technical Writing assignments in the GitLab handbook.
title: Get started with Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

Orbit indexes your GitLab groups, projects, pipelines, and code into a structured knowledge graph.
AI agents can query this graph to answer questions that require full workspace context: across repositories, pipelines, contributors, and dependencies.

**Time to complete:** About 5 minutes to enable and run your first query.

## Prerequisites

- GitLab Premium or Ultimate on GitLab.com
- Owner role on the top-level group you want to index
- For external agents: an MCP-compatible AI client (Claude Code, Codex, or similar)

## Step 1: Enable Orbit for a group

Enable Orbit on a top-level group to start indexing its data.

1. On the left sidebar, select **Search or go to** and find your group.
1. Select **Settings > [TW: confirm path]**.
1. Enable **Orbit knowledge graph indexing**.
1. Select **Save changes**.

Orbit begins indexing immediately. Indexing typically completes in seconds for most groups.
Subgroups and projects inherit the setting automatically. You don't need to enable them individually.

Orbit indexes the following data:

- Groups, projects, members, and merge requests
- Issues, pipelines, and CI/CD configuration
- Source code in Ruby, Python, Go, Java, Kotlin, TypeScript, JavaScript, Rust, and C#

## Step 2: Verify indexing

After enabling Orbit, confirm the knowledge graph is being populated.

1. Go to **[TW: confirm path]** in your group.

The Orbit configuration screen shows the current indexing status. Source code indexing may take a few minutes for large repositories.

If no data appears after five minutes, see [Troubleshooting](../orbit_troubleshooting/).

## Step 3: Access the knowledge graph

Orbit exposes the knowledge graph through two integration paths. Choose based on how your team works.

| | GitLab Duo Agent Platform | External agents (MCP) |
|---|---|---|
| **Best for** | Teams already using Duo | Claude Code, Codex, or custom agents |
| **Setup** | None, works automatically | Add MCP config to your agent |
| **Cost** | Included with your plan | Billed via GitLab Credits |

Both paths respect GitLab permissions. You only see data you already have access to in GitLab.

### Option A: GitLab Duo Agent Platform (included with your plan)

GitLab Duo Agent Platform automatically uses Orbit when it's enabled for a group. No additional setup is required.

1. Open a project in an indexed group.
1. Open GitLab Duo Agent Platform and start a conversation.

Duo can now answer questions that span your entire group. For example:

- _Which merge requests in this group have failing pipelines in the last 24 hours?_
- _Which services depend on `payments-service`?_
- _Show me issues linked to merge requests that introduced a critical vulnerability._

GitLab Duo Agent Platform respects GitLab permissions. It only surfaces data you already have access to.

### Option B: External agents via MCP

Connect Claude Code, Codex, or any MCP-compatible agent to Orbit using the `glab` CLI.

Run the setup command from your terminal:

```shell
glab orbit setup
```

This command auto-detects your coding agent (Claude Code, Cursor, VS Code, and others), installs the Orbit skills, and configures MCP automatically. To preview what it will do before applying, run:

```shell
glab orbit setup --dry-run
```

Once connected, your agent has access to two tools:

- **`query_graph`**: run a query against the Orbit knowledge graph
- **`get_graph_schema`**: retrieve the schema of available node types and fields

For manual configuration or advanced options, see [Connect an external agent with MCP](../queries/mcp_tools/).

## Next steps

- [Query language reference](../queries/query_language/): all available fields and filters
- [Schema reference](../schema/): node types across code, pipelines, security, and more
- [MCP tools reference](../queries/mcp_tools/): `query_graph` and `get_graph_schema` in detail
- [REST API](https://docs.gitlab.com/api/orbit/): query Orbit programmatically without MCP
- [Troubleshooting](../orbit_troubleshooting/): common indexing and query issues
