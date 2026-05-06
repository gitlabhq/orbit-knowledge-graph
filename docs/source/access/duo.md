---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use Orbit through GitLab Duo Agent Platform. Ask natural language questions about your SDLC and get answers grounded in your live GitLab data.
title: Use Orbit with Duo Agent Platform
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

The Orbit agent is available in GitLab Duo Agent Platform. Ask questions in natural language
and Duo translates them to Orbit graph queries, executes them, and returns answers grounded
in your live GitLab data.

## Prerequisites

- Orbit is [enabled on your group](../get_started.md).
- You have a GitLab Duo Developer seat.

## Access the Orbit agent

1. On the left sidebar, select **GitLab Duo**.
1. Select the **Orbit** agent.

## Billing

Queries through Duo Agent Platform are **zero-rated**. They do not consume GitLab Credits.

## Example prompts

Use these as a starting point. The Orbit agent handles the query translation automatically.

**Codebase exploration:**

- "What are the 10 most recently updated projects in my group?"
- "Which projects have the most open merge requests?"
- "Who are the top contributors to this project by merge requests merged?"

**Blast radius and impact:**

- "Which projects import the `payments-service` library?"
- "What files in this project depend on `UserAuthService`?"
- "If I deprecate this function, which other files reference it?"

**CI/CD and pipeline health:**

- "Which projects have the highest pipeline failure rate?"
- "What are the most common job failure reasons in this group?"
- "Which pipelines take the longest to run?"

**Security:**

- "Show me all critical and high severity open vulnerabilities in this group."
- "Which projects have unresolved vulnerabilities introduced in the last 30 days?"
- "What CVEs are present across my projects?"

**Planning and work items:**

- "How many open issues are assigned to each user in this group?"
- "Which milestones are overdue?"
- "What work items are blocking this epic?"

## Limitations

- The Orbit agent answers questions about data in groups where Orbit is enabled.
  It does not have access to groups you do not belong to.
- Complex multi-step queries may require follow-up questions to narrow the scope.
- Code content (file text, function bodies) is available but may not be returned
  by default for large results. Ask explicitly: "Show me the source of this function."
