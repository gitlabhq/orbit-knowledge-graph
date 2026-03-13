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

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.
- Enabled on GitLab.com in GitLab 18.XX.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.

Orbit is a service that builds a structured graph database, or knowledge graph, from your GitLab instance.
The service scans your workspace to index:

- GitLab objects, like groups, projects, work items, merge requests, and pipelines.
- Code objects, like files, functions, classes, and modules.

Orbit maps the relationships that connect each object, and stores this information in a knowledge graph.
View the knowledge graph in the UI to understand your entire software development lifecycle (SDLC) at a glance.
Or, connect an AI platform like the GitLab Duo Agent Platform to give your agents permission-aware access to SDLC data.

With Orbit, you can:

- Ask an AI agent to suggest optimal reviewers based on past contributions.
- Visualize hierarchies, like group membership or module dependencies.
- See all the merge requests, issues, and services related to a component.
- Generate up-to-date onboarding experiences for new developers.
- Surface past security findings, vulnerabilities, and incidents.

## Turn Orbit on or off

Turn Orbit on for a top-level group to add it to the knowledge graph.
To remove a group from the graph, turn Orbit off.

Prerequisites:

- You must have the Owner role for the group.

To start using Orbit:

1. In the top bar, select **Search or go to** > **Explore**.
1. In the left sidebar, select **Orbit** > **Configuration**.
1. Next to the top-level group you want to index, turn on **Enable**. To remove a group, turn the toggle off.

## Feedback

Your feedback is valuable in helping us improve this feature. Share your experiences, suggestions, or issues in [issue 160](https://gitlab.com/gitlab-org/rust/knowledge-graph/-/issues/160).
