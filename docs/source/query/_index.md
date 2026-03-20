---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the knowledge graph to understand relationships across your projects, power GitLab Duo agents, and expose context to external tools.
title: Query the knowledge graph
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

Query the knowledge graph to understand relationships across your projects, power GitLab Duo agents, and expose context to external tools.

Orbit respects role-based access control, so queries expose only data you have permission to see in GitLab.

## Prerequisites

- Orbit must be turned on for a group or project.
- You must have the Reporter, Developer, Maintainer, or Owner role in the group or project.

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

## With the UI

Write custom queries with the [Orbit query language](language.md), then execute them in the UI.

Prerequisites:

- Orbit must be turned on for your group or project.
- You must have the Reporter, Developer, Maintainer, or Owner role in the group or project.

To query the knowledge graph:

1. In the top bar, select **Search or go to** > **Your work**.
1. Select **Orbit** > **Data explorer**.
1. In the query editor, enter a query.
1. Select **Execute query**.

Orbit displays the results of the query in the **Node explorer** and **Table** views.
