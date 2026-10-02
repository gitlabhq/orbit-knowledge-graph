---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use GitLab Orbit through GitLab Duo Agent Platform. Agents call GitLab Orbit's graph tools to ground their answers in your live GitLab data, across the GitLab Duo Agent, the Planner Agent, the Security Analyst Agent, the Data Analyst Agent, the CI Expert Agent, and the Developer Flow.
title: Use GitLab Orbit with GitLab Duo Agent Platform
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

GitLab Orbit is integrated into GitLab Duo Agent Platform. Agents call GitLab Orbit's command tools (`list_commands`, `invoke_command`) automatically, running commands such as `get_graph_schema` and `query_graph`, when a question is best answered by traversing your SDLC graph - cross-project dependencies, blast radius, pipeline inheritance, vulnerability lineage, contributor patterns. When GitLab Orbit doesn't have the answer, the agent falls back to its existing tools.

## Prerequisites

- GitLab Orbit is [enabled on your group](../getting-started.md).
- You have access to [GitLab Duo Agent Platform](https://docs.gitlab.com/user/duo_agent_platform/).

## Where GitLab Orbit is available

GitLab Orbit is wired into the following GitLab Duo Agent Platform agents and flows:

| Agent or flow | When to use it |
|---|---|
| GitLab Duo Agent | General development assistant. Get help with code, planning, security, and project management. Calls GitLab Orbit when answers benefit from graph context. |
| Planner Agent | Issue and milestone planning. Ask about work item ownership, blockers, contributor load, milestone progress across projects. |
| Security Analyst Agent | Vulnerability triage. Ask about open vulnerabilities by severity, CVE coverage across the group, vulnerability introduction timelines. |
| Data Analyst Agent | SDLC analytics powered by GLQL. Ask about pipeline health, MR cycle time, contributor patterns, deployment frequency. |
| CI Expert Agent | Pipeline triage. Ask about job failure causes, pipeline inheritance, slowest jobs, frequently failing projects. |
| Developer Flow | Turn a work item into a draft MR in the UI. GitLab Orbit grounds the agent's implementation in your live SDLC graph - dependencies, ownership, blast radius. |
| Custom flows | Your own flows. GitLab Orbit is available when the flow lists the GitLab Orbit tools. |

When an agent uses GitLab Orbit to answer a question, the answer is grounded in your
live graph rather than the agent's general knowledge.

## Turn on GitLab Orbit for custom flows

To turn on GitLab Orbit for custom flows:

<!-- vale orbit.StandaloneProductName = NO -->

1. In the upper-right corner, select your avatar.
1. Select **Preferences**.
1. Under **Orbit in GitLab Duo**, select the **Use Orbit in GitLab Duo**
   and **Other Foundational Agents** checkboxes.
1. Select **Save changes**.

<!-- vale orbit.StandaloneProductName = YES -->

> [!note]
> The **Custom Agents** setting does not apply to custom flows.

### Use GitLab Orbit in a custom flow

To use GitLab Orbit in a [custom flow](https://docs.gitlab.com/user/duo_agent_platform/flows/custom/),
you must add the GitLab Orbit tools to the flow configuration.
After you add the tools, you can write prompts that tell the agent to use GitLab Orbit.

Prerequisites:

- The Maintainer or Owner role for the project that manages the flow.
- Each user who triggers the flow must
  [turn on GitLab Orbit for custom flows](#turn-on-gitlab-orbit-for-custom-flows).

To use GitLab Orbit in a custom flow:

1. In the top bar, select **Search or go to** and find your group or project.
1. Select **AI** > **Flows**.
1. Select the flow you want to edit.
1. In the upper-right corner, select **Edit**.
1. Add the GitLab Orbit tools to the `toolset`
   of each agent component that needs them:

   ```yaml
   toolset:
     - orbit_list_commands
     - orbit_invoke_command
   ```

1. In the prompt, tell the agent when to use GitLab Orbit. For example,
   to find other projects that import the changed files.
1. Select **Save changes**.

To edit the flow in VS Code, see
[Edit a flow](https://docs.gitlab.com/user/duo_agent_platform/flows/custom/?tab=VS+Code#edit-a-flow).

## Billing

During the beta, queries that GitLab Duo Agent Platform makes against GitLab Orbit on
your behalf do not consume GitLab Credits.

When GitLab Orbit is generally available, these queries consume GitLab Credits. Credit
rates are published in
[GitLab Credits and usage billing](https://docs.gitlab.com/subscriptions/gitlab_credits/)
before charging begins.

## Example prompts

Ask these in any of the surfaces above - the agent picks the right tool.

Codebase exploration:

- "What are the 10 most recently updated projects in my group?"
- "Which projects have the most open merge requests?"
- "Who are the top contributors to this project by merge requests merged?"

Blast radius and impact:

- "Which projects import the `payments-service` library?"
- "What files in this project depend on `UserAuthService`?"
- "If I deprecate this function, which other files reference it?"

CI/CD and pipeline health:

- "Which projects have the highest pipeline failure rate?"
- "What are the most common job failure reasons in this group?"
- "Which pipelines take the longest to run?"

Security:

- "Show me all critical and high severity open vulnerabilities in this group."
- "Which projects have unresolved vulnerabilities introduced in the last 30 days?"
- "What CVEs are present across my projects?"

Planning and work items:

- "How many open issues are assigned to each user in this group?"
- "Which milestones are overdue?"
- "What work items are blocking this epic?"

## Limitations

- GitLab Orbit only answers about groups where it is enabled and that you have access to.
- Complex multi-step questions may need a follow-up to narrow scope.
- Code content (file text, function bodies) is available but may not be returned
  by default for large results. Ask explicitly: "Show me the source of this function."
- Code Review Flow does not use GitLab Orbit. To use GitLab Orbit in code review, use a
  [custom flow](#use-gitlab-orbit-in-a-custom-flow).
