---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Index your repositories and build a knowledge graph of your software development lifecycle.
title: Orbit
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

Orbit is a data analysis and observability engine for GitLab.
It indexes your groups, projects, and repositories, then analyzes
the relationships between them to build a knowledge graph of your
instance.

<!--- add a picture of the node explorer here --->

The knowledge graph is a structured, queryable map of your
entire software development lifecycle. Use it to understand how your
work is organized and how its parts relate to each other.

Orbit exposes the knowledge graph through a unified context API.
Explore the graph in the GitLab UI or query it with an AI tool like
GitLab Duo to bring full workspace context into your agentic AI
sessions.

You can use Orbit to get answers to questions like:

- Based on past reviews and file ownership, who should review this change?
- Have any vulnerabilities been found in this project, and are any unresolved?
- Which projects depend on this module or library?
- What work items are assigned to this user in these projects?
- Which projects do most pipeline failures come from?

## Turn Orbit on or off

Turn Orbit on for a top-level group to start indexing its data and add it to the knowledge graph.

Turn Orbit off to stop indexing and remove the group's data from the graph.

Prerequisites:

- You must have the Owner role for the group.

To turn Orbit on or off:

1. In the top bar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Configuration**.
1. Next to the top-level group you want to index, turn **Enable** on or off.

## View the knowledge graph

Use the data explorer to visualize your instance and verify that Orbit indexed your groups correctly.

Prerequisites:

- Orbit must be turned on for a group or project.
- You must have the Reporter, Developer, Maintainer, or Owner role for the group or project.

To view the knowledge graph:

1. In the top bar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Data explorer**.
1. Explore the knowledge graph:
   - In the **Node explorer** view, for details about a graph node, double-click the node.
   - In the **Table** view, to download your data as a CSV file, select **Download CSV**.

## Performance

The Orbit indexer runs in a separate Kubernetes cluster and does not
impact the performance of your instance. The indexer job completes in
seconds, even for large groups.

Changes to a group, project, or repository are reindexed automatically.
Reindexing typically completes a few minutes after a change.

## Coverage

Orbit indexes only the top-level groups where it is turned on.
Subgroups and projects inherit indexing from the top-level group.

Orbit indexes two types of data:

1. GitLab data includes the software development lifecycle objects that make up your instance:

   - Groups and projects
   - Users
   - Work items
   - Merge requests
   - Pipelines
   - Vulnerabilities and security findings

1. Code includes the content of your repositories:

   - Source files and directories
   - Function, class, and module definitions
   - Imports and cross-file references

   Code is indexed from only the default branch.

<!--- Re-add diagram --->

### Supported languages

Orbit supports code indexing for the following languages:

- Ruby
- Java
- Kotlin
- Python
- TypeScript
- JavaScript
- Rust
- C#

<!--- ## Billing and usage --->

## Feedback

Your feedback is valuable in helping us improve this feature.
Share your experience in [issue 592436](https://gitlab.com/gitlab-org/gitlab/-/work_items/592436).
