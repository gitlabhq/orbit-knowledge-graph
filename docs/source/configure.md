---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
title: Configure Orbit
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

1. On the left sidebar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Configuration**.
1. Next to the top-level group you want to index, turn **Enable** on or off.

Orbit indexes your data in seconds.

When data is added, changed, or deleted, Orbit automatically updates the knowledge graph.
Updates to the graph can take several minutes.

## View the knowledge graph

Use the UI to browse the graph of indexed groups, projects, and related objects.

Prerequisites:

- You must have the Reporter, Developer, Maintainer, or Owner role for a group or project to view its data.

To view the knowledge graph:

1. On the left sidebar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Node explorer**.
1. Optional. Select a node to view its details.
