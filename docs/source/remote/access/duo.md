---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use Orbit through GitLab Duo Agent Platform. Agents call Orbit's graph tools to ground their answers in your live GitLab data, across the GitLab Duo Agent, the Planner Agent, the Security Analyst Agent, the Data Analyst Agent, the CI Expert Agent, and the Duo Developer Flow.
title: Use Orbit with GitLab Duo Agent Platform
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experimental

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

<!-- -->

> [!disclaimer]

Orbit is integrated into GitLab Duo Agent Platform. DAP agents call Orbit's graph tools (`get_graph_schema`, `query_graph`) automatically when a question is best answered by traversing your SDLC graph - cross-project dependencies, blast radius, pipeline inheritance, vulnerability lineage, contributor patterns. When Orbit doesn't have the answer, the agent falls back to its existing tools.

## Prerequisites

- Orbit is [enabled on your group](../getting-started.md). Orbit requires GitLab.com Premium or Ultimate.
- You have access to GitLab Duo Agent Platform. See [GitLab Duo Agent Platform](https://docs.gitlab.com/user/duo_agent_platform/) for how to access it through your subscription or GitLab Credits.

## Where Orbit is available

Orbit is wired into the following GitLab Duo Agent Platform agents and flows:

| Agent or flow | When to use it |
|---|---|
| **GitLab Duo Agent** | General development assistant. Get help with code, planning, security, and project management. Calls Orbit when answers benefit from graph context. |
| **Planner Agent** | Issue and milestone planning. Ask about work item ownership, blockers, contributor load, milestone progress across projects. |
| **Security Analyst Agent** | Vulnerability triage. Ask about open vulnerabilities by severity, CVE coverage across the group, vulnerability introduction timelines. |
| **Data Analyst Agent** | SDLC analytics powered by GLQL. Ask about pipeline health, MR cycle time, contributor patterns, deployment frequency. |
| **CI Expert Agent** | Pipeline triage. Ask about job failure causes, pipeline inheritance, slowest jobs, frequently failing projects. |
| **Duo Developer Flow** | Turn a work item into a draft MR in the UI. Orbit grounds the agent's implementation in your live SDLC graph - dependencies, ownership, blast radius. |

When an agent uses Orbit to answer a question, the answer is grounded in your
live graph rather than the agent's general knowledge.

## Billing

Queries that GitLab Duo Agent Platform makes against Orbit on your behalf are
**zero-rated**. They do not consume GitLab Credits.

## Example prompts

Ask these in any of the surfaces above - the agent picks the right tool.

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

- Orbit only answers about groups where it is enabled and that you have access to.
- Complex multi-step questions may need a follow-up to narrow scope.
- Code content (file text, function bodies) is available but may not be returned
  by default for large results. Ask explicitly: "Show me the source of this function."
