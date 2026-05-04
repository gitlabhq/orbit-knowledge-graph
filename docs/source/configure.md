---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see the Technical Writing assignments in the GitLab handbook.
description: Turn on Orbit, inspect indexed data, and connect AI tools.
title: Get started with Orbit
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

Use the Orbit dashboard to turn on indexing for top-level groups, inspect the
indexed graph, browse the schema, and connect AI tools.

## Before you begin

Prerequisites:

- The Owner role for a top-level group.
- A group on Premium or Ultimate.
- The `knowledge_graph` feature flag enabled for your user or instance.

Orbit indexes top-level groups. Subgroups and projects inherit indexing from the
top-level group.

## Turn on Orbit for a group

To turn on Orbit:

1. On the left sidebar, select **Search or go to**.
1. Select **Your work**.
1. Select **Orbit**.
1. In **Available groups**, find the top-level group you want to index.
1. Select **Get started**.
1. In the confirmation dialog, select **Turn on indexing**.

Orbit creates an enabled namespace record for the group. The deployed Orbit
service uses that record to schedule indexing work.

Initial indexing usually starts within a few minutes. Large groups and large
repositories can take longer.

## Check indexed content

After you turn on Orbit, use the dashboard to check what the graph contains.

To check indexed content:

1. On the left sidebar, select **Search or go to**.
1. Select **Your work**.
1. Select **Orbit**.
1. Select the group you turned on.
1. Review **Indexed content** for entity and relationship counts.
1. Optional. Filter by subgroup or project.

The indexed content view shows the latest graph status for the selected scope,
including entity counts grouped by domain.

## Explore the graph

Use the **Explore** tab to view and query the graph.

To explore the graph:

1. On the left sidebar, select **Search or go to**.
1. Select **Your work**.
1. Select **Orbit**.
1. Select **Explore**.
1. Use **Map** or **Table** view.
1. Optional. Select **2D** or **3D** map mode.
1. Optional. Select **Advanced query** to run an Orbit JSON query.

The map begins with enabled groups and expands to connected graph data. To view
more details, select a node.

## Browse the schema

Use the **Schema** tab to understand which objects and relationships are
available.

To browse the schema:

1. On the left sidebar, select **Search or go to**.
1. Select **Your work**.
1. Select **Orbit**.
1. Select **Schema**.
1. Search or filter by entity type.
1. Optional. Select an entity type to show matching instances in the graph.

The schema comes from the deployed Orbit service, so it reflects the ontology
available to your instance.

## Use Orbit with GitLab Duo

When Orbit is available for your user and group, GitLab Duo can use the knowledge
graph as a context source. Agentic Chat and other Duo agent experiences can ask
Orbit for connected GitLab data instead of relying only on the current page or
repository.

Try prompts such as:

- "Show me recent merge requests with failing pipelines."
- "What open vulnerabilities exist in my projects?"
- "Who merged the most this quarter?"
- "List pipelines that failed in the last week."

## Connect external AI tools

External tools connect to Orbit through the Orbit MCP endpoint.

Prerequisites:

- Install the [GitLab CLI](https://docs.gitlab.com/cli/).
- Turn on Orbit for at least one top-level group you can access.

To connect to the MCP server:

1. From the command line, run:

   ```shell
   glab orbit setup
   ```

1. Follow the prompts.

The setup command detects supported agents, installs Orbit skills, and configures
the MCP connection. To preview the changes without applying them, use:

```shell
glab orbit setup --dry-run
```

For the MCP tool contract, see [Orbit MCP tools](queries/mcp_tools.md).

## Turn off Orbit for a group

To turn off Orbit:

1. Go to the top-level group.
1. Select **Settings** > **Orbit**.
1. Select **Turn off indexing**.
1. Confirm the change.

Turning off Orbit stops indexing for the group. Orbit also removes the group's
data from the graph.
