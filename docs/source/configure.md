---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see the Technical Writing assignments in the GitLab handbook.
title: Configure Orbit
---

{{< details >}}

- Tier: Ultimate
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

## Turn Orbit on or off

Turn Orbit on for a top-level group to start indexing its data and add it to the knowledge graph.

Turn Orbit off to stop indexing and remove the group’s data from the graph.

Prerequisites:

- The Owner role for the top-level group.

To turn Orbit on or off:

1. In the top bar, select **Search or go to** and find your group.
1. In the left sidebar, select **Settings** > **Orbit**.
1. Select **Get started**.
1. In the confirmation dialog, select **Turn on indexing**.

Orbit indexes your group, and all its subgroups and projects.

The indexer job typically completes in seconds. For large
repositories, you might have to wait several minutes for the indexer
to finish.

## View the knowledge graph

View the knowledge graph when you want to:

- Verify that Orbit has correctly indexed your data.
- Visualize your software development lifecycle.

Prerequisites:

- Turn on Orbit for a group or project.
- The Reporter, Developer, Maintainer, or Owner role for the group or project.

To view the knowledge graph:

1. In the top bar, select **Search or go to** > **Your work**.
1. In the **Explore** tab, make sure your groups appear in the knowledge graph.
1. Optional. Double-click a node to view its details.

## Orbit with GitLab Duo

By default, the GitLab Duo Agent Platform uses the knowledge graph as
a data source to respond to improve results.

You can use Orbit with:

- GitLab Duo Agentic Chat
- Foundational agents
- Foundational flows

## Connect to the Orbit MCP server

Use the GitLab CLI to connect external AI tools like Claude Code to
Orbit.

Prerequisites:

- Install the [GitLab CLI](https://docs.gitlab.com/cli/).

To connect to the MCP server:

- From the command line, run the setup command:

  ```shell
  glab orbit setup
  ```

  This command detects your agents, installs the Orbit skills, and
  configures the MCP automatically.

  To preview the results of the command without applying changes, use the flag `--dry-run`.
