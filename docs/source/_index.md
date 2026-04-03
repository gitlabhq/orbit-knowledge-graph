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

Orbit indexes your groups, projects, and repositories, then analyzes
the relationships between them to build a knowledge graph of your
instance. The knowledge graph is a structured, queryable map of your
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

## Feedback

Your feedback is valuable in helping us improve this feature.
Share your experiences in [issue 592436](https://gitlab.com/gitlab-org/gitlab/-/work_items/592436).
