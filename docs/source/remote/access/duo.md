---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use Orbit through GitLab Duo Agent Platform. Ask natural language questions about your SDLC and get answers grounded in your live GitLab data, across Duo Developer and the foundational agents.
title: Use Orbit with Duo Agent Platform
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

Orbit is integrated into GitLab Duo Agent Platform. Duo Developer and the foundational
agents call Orbit's graph tools (`get_graph_schema`, `query_graph`) directly when a
question is best answered by traversing your SDLC graph - cross-project dependencies,
blast radius, pipeline inheritance, vulnerability lineage, contributor patterns. When
Orbit doesn't have the answer, the agent falls back to its existing tools.

## Prerequisites

- Orbit is [enabled on your group](../getting-started.md).
- You have access to Duo Agent Platform through one of:
  - **Premium or Ultimate** subscription (includes monthly GitLab Credits: 12 per user on Premium, 24 per user on Ultimate)
  - **Duo Pro or Duo Enterprise** seats with GitLab Credits for DAP usage

## Where Orbit is available

Orbit is wired into the following Duo Agent Platform surfaces:

| Surface | When to use it |
|---------|----------------|
| **Duo Developer Flow** | In-IDE coding flow. Ask about cross-file references, who calls a function, blast radius for a change, dependencies between projects in your group. |
| **Duo CLI** | Terminal-based, editor-agnostic, scriptable. Same Orbit capabilities as Duo Developer Flow, available in headless and CI workflows. |
| **Planner agent** | Issue and milestone planning. Ask about work item ownership, blockers, contributor load, milestone progress across projects. |
| **Security Analyst agent** | Vulnerability triage. Ask about open vulnerabilities by severity, CVE coverage across the group, vulnerability introduction timelines. |
| **Data Analyst agent** | SDLC analytics. Ask about pipeline health, MR cycle time, contributor patterns, deployment frequency. |
| **CI Expert agent** | Pipeline triage. Ask about job failure causes, pipeline inheritance, slowest jobs, frequently failing projects. |

When an agent uses Orbit to answer a question, the response includes an inline
indicator so you know the answer is grounded in your live graph rather than the
agent's general knowledge.

## Billing

Queries that Duo Agent Platform makes against Orbit on your behalf are
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
