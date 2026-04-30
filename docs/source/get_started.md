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

Orbit indexes your GitLab data and code into a structured knowledge graph.
AI agents can query this graph to answer questions that require full workspace context.
Use Orbit to get answers to questions across repositories, pipelines, contributors, and dependencies.

## Step 1: Turn Orbit on or off for a group

Turn Orbit on for a top-level group to start indexing its data and add it to the knowledge graph.

Turn Orbit off to stop indexing and remove the group’s data from the graph.

Prerequisites:

- The Owner role for the group.

To turn Orbit on or off:

1. In the top bar, select **Search or go to** and find your group.
1. In the left sidebar, select **Settings** > **Orbit**.
1. Select **Get started**.
1. In the confirmation dialog, select **Turn on indexing**.

Orbit indexes your group and all of its subgroups and projects.

## Step 2: Verify indexing is complete

Next, make sure Orbit has correctly populated the knowledge graph:

1. In the top bar, select **Search or go to** > **Your work**.
1. In the **Explore** tab, make sure your groups appear in the knowledge graph.
1. Optional. Double-click a node to view its details.

You can also check the **Orbit configuration** page to understand the current indexing status.
Code indexing may take a few minutes for large repositories.

If no data appears after five minutes, see [troubleshooting Orbit](../orbit_troubleshooting/).

## Step 3: Query the knowledge graph

Orbit exposes the knowledge graph through two integration paths. Choose the best path for your team.

### Option 1: With GitLab Duo Agentic Chat

Agentic Chat automatically uses Orbit when it's enabled for a group. No additional setup is required.

1. In the top bar, select **Search or go to** and find your group.
1. On the GitLab Duo sidebar, select **Add new chat** ({{< icon name="pencil-square" >}}).

Duo can now answer questions that span your entire group. For example:

- "Which merge requests in this group have failing pipelines in the last 24 hours?"
- "Which services depend on `payments-service`?"
- "Show me issues linked to merge requests that introduced a critical vulnerability."

### Option 2: With MCP-compatible AI agents

Connect Claude Code, Codex, or any other MCP-compatible agent to Orbit using the GitLab CLI.

To connect to the MCP server:

- From the command line, run the setup command:

  ```shell
  glab orbit setup
  ```

  This command detects your agents, installs the Orbit skills, and configures MCP automatically.

  To preview the results of the command without applying any changes, run:

  ```shell
  glab orbit setup --dry-run
  ```

For a list of available MCP tools, see [Orbit MCP tools](queries/mcp_tools.md).
