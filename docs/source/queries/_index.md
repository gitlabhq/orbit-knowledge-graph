---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Query the Orbit knowledge graph to explore your GitLab instance.
title: Queries
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

Queries are the main way to work with the knowledge graph. A query is
a JSON object that defines what data to retrieve and how to structure
the results.

Queries respect role-based access control. When you query the
knowledge graph, you see only data you have permission to see in
GitLab.

You can query the graph directly by using the Orbit query language, or
have an AI agent like GitLab Duo write and run queries for you.

## Prerequisites

- Turn on Orbit for a group or project.
- The Reporter, Developer, Maintainer, or Owner role for the group or project.

## Run a query in the UI

Use the query editor to write and run queries in the UI.

To run a query:

1. In the top bar, select **Search or go to** > **Your work**.
1. Select **Orbit**, then select the **Explore** tab.
1. In the query editor, enter a query. For information about query syntax, see [Orbit query language](query_language.md).
1. Select **Execute query**.

## GitLab Duo Agentic Chat

When Orbit is turned on, Agentic Chat automatically uses the knowledge
graph as a data source to respond to prompts.

See [use GitLab Duo Chat](https://docs.gitlab.com/user/gitlab_duo_chat/agentic_chat/#use-gitlab-duo-chat).

## Connect to the Orbit MCP server

Use the Model Context Protocol (MCP) server to integrate external AI
tools like Claude Code with Orbit.

Use the [GitLab CLI](https://docs.gitlab.com/cli/) to configure Orbit in one command:

- In the command line, run:

  ```shell
  glab orbit setup
  ```

  To run the command without applying settings, you can apply the `--dry-run` option.

For a list of available MCP tools, see [Orbit MCP tools](tools.md).

## Example prompts

Use these example prompts with Agentic Chat or another AI agent:

- "List merged merge requests in the last 30 days for `my-project`, grouped by author."
- "Show all open issues that are blocked by merge requests with failing pipelines in `my-project`."
- "List services that directly depend on `payments-api` and show their last five deployments."
- "Find all vulnerabilities that are linked to merge requests merged in the last seven days in `my-group`, grouped by severity."
- "Show all projects where `@alice` has authored merge requests, with a count of merged vs open merge requests per project."
- "List the top 10 files in `my-group/my-project` that changed in the most failed pipelines over the past month."
