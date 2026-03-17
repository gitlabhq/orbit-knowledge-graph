---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
title: Orbit
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

Orbit indexes your groups, projects, and repositories, then analyzes
the relationships between them to build a knowledge graph of your
instance. The knowledge graph is a structured, queryable map of your
entire software development lifecycle. Use it to understand how your
work is organized and how its parts relate to each other.

Orbit exposes the knowledge graph through a unified context API.  You
can explore the graph in the GitLab UI or query it with the GitLab Duo
Agent Platform to bring full workspace context into your agentic AI
sessions.

You can use Orbit to get answers to questions like:

- Based on past reviews and file ownership, who should review this change?
- Have any vulnerabilities been found in this project, and are any unresolved?
- Which projects depend on this module or library?
- What work items are assigned to this user in these projects?

## Orbit workflow

Turn Orbit on for a top-level group to add its data to the knowledge graph.

Orbit ingests two categories of data:

1. GitLab data includes the objects that make up your instance:

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

   Orbit indexes code from only the default branch.

GitLab data and code-indexing tasks are streamed to a high-performance database
as change data capture (CDC) events. CDC events track every addition, update, and
deletion in the database, so the knowledge graph stays current automatically.

## Turn Orbit on or off

Turn Orbit on for a top-level group to add its data to the knowledge graph.
Turn it off to stop indexing and remove the group's data.

Prerequisites:

- You must have the Owner role for the group.

To turn Orbit on or off:

1. On the left sidebar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Configuration**.
1. Next to the top-level group you want to index, turn **Enable** on or off.

## Query the knowledge graph

## Feedback

Your feedback is valuable in helping us improve this feature. Share your experiences, suggestions, or issues in [issue 160](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/160).

<!---
## Troubleshooting
-->