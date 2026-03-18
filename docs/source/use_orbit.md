---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
title: Use Orbit
---

{{< details >}}

- Tier: Ultimate
- Offering: GitLab.com

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](../administration/feature_flags.md) named `knowledge_graph`. Disabled by default.
- Enabled on GitLab.com in GitLab 18.XX.

{{< /history >}}

{{< alert type="flag" >}}

The availability of this feature is controlled by a feature flag.
For more information, see the history.

{{< /alert >}}

## Turn Orbit on or off

Turn Orbit on for a top-level group to start indexing its data and add it to the knowledge graph.

Turn Orbit off to stop indexing and remove the group's data from the graph.

Prerequisites:

- You must have the Owner role for the group.

To turn Orbit on or off:

1. In the top bar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Configuration**.
1. Next to the top-level group you want to index, turn **Enable** on or off.

Orbit indexes your data in seconds.

When data is added, changed, or deleted, Orbit automatically updates the knowledge graph.
Updates to the graph can take several minutes.

## View the knowledge graph

Prerequisites:

- You must have the Reporter, Developer, Maintainer, or Owner role for a group or project to view its data.

To view the knowledge graph:

1. In the top bar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Data explorer**.

## Query the knowledge graph

Query the knowledge graph to understand relationships across your projects, power GitLab Duo agents, and expose context to external tools.

Prerequisites:

- Orbit must be turned on for a group or project.
- You must have the Reporter, Developer, Maintainer, or Owner role for a group or project to view its data.

## With GitLab Duo Agentic Chat

When Orbit is turned on, Agentic Chat automatically uses the knowledge graph as a data source to respond to prompts.

See [Use GitLab Duo Chat](https://docs.gitlab.com/user/gitlab_duo_chat/agentic_chat/#use-gitlab-duo-chat).

Example prompts:

- "List merged merge requests in the last 30 days for `my-project`, grouped by author."
- "Show all open issues that are blocked by merge requests with failing pipelines in `my-project`."
- "List services that directly depend on `payments-api` and show their last five deployments."
- "Find all vulnerabilities that are linked to merge requests merged in the last seven days in `my-group`, grouped by severity."
- "Show all projects where `@alice` has authored merge requests, with a count of merged vs open merge requests per project."
- "List the top 10 files in `my-group/my-project` that changed in the most failed pipelines over the past month."

## With an external AI agent

Use the Model Context Protocol (MCP) server to use external AI tools like Claude Code with Orbit.

To configure the Orbit MCP server:

- Follow the instructions in [connect a client to the GitLab MCP server](https://docs.gitlab.com/user/gitlab_duo/model_context_protocol/mcp_server/#connect-a-client-to-the-gitlab-mcp-server).

  To configure the MCP for only Orbit, use the URL `https://gitlab.com/api/v4/mcp_orbit`.

For a list of available tools, see [Orbit MCP tools](tools.md).

## With the UI

Write custom queries with the [Orbit query language](query_language.md), then execute them in the UI.

To query the knowledge graph:

1. In the top bar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Data explorer**.
1. In the query editor, enter a query.
1. Select **Execute query**.

Orbit displays the results of the query in the **Node explorer** and **Table** views.
