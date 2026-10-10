---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use GitLab Orbit in GitLab Duo Agent Platform agents and flows to ground answers in your live GitLab data.
title: GitLab Orbit in GitLab Duo
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
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

GitLab Duo Agent Platform agents use the GitLab Orbit graph to answer questions about your live GitLab data.
You do not install anything. You turn on one setting.

Agents call the GitLab Orbit tools `list_commands` and `invoke_command` when the graph can answer the question.
Through these tools, they run commands such as `get_graph_schema` and `query_graph`.
The graph helps with cross-project dependencies, blast radius, pipeline inheritance, vulnerability history, and contributor patterns.
If the graph does not have the answer, the agent uses its other tools.

## Prerequisites

- GitLab Orbit is [turned on for your group](../_index.md#turn-on-gitlab-orbit-for-your-group).
- You have access to [GitLab Duo Agent Platform](https://docs.gitlab.com/user/duo_agent_platform/).

## Turn on GitLab Orbit in GitLab Duo

On GitLab.com, the setting is off by default. Each user turns it on.

<!-- vale orbit.StandaloneProductName = NO -->

1. In the upper-right corner, select your avatar.
1. Select **Preferences**.
1. Under **Orbit in GitLab Duo**, select the **Use Orbit in GitLab Duo** checkbox.
1. Select **Save changes**.

When you select **Use Orbit in GitLab Duo**, these options show. All of them are on by default:

| Option | Gives the GitLab Orbit tools to |
|--------|---------------------------------|
| **Agentic Chat** | Agentic chat. |
| **Orbit Agent** | The standalone Orbit agent in GitLab Duo. |
| **Other Foundational Agents** | Foundational agents outside chat, for example software development and security analyst agents. |
| **Custom Agents** | Custom agents that users build. |

When you clear **Use Orbit in GitLab Duo**, all GitLab Orbit features are off.

In GitLab Duo Chat, the **Orbit** control in the chat header shows if the graph is on.
Select it to change the **Agentic chat**, **Foundational agents**, and **Custom agents** options.

<!-- vale orbit.StandaloneProductName = YES -->

## Agents and flows that use GitLab Orbit

| Agent or flow | Use it for |
|---------------|------------|
| GitLab Duo Agent | General help with code, planning, security, and project management. |
| Planner Agent | Work item owners, blockers, contributor load, and milestone progress across projects. |
| Security Analyst Agent | Open vulnerabilities by severity, CVE coverage in the group, and when vulnerabilities were introduced. |
| Data Analyst Agent | SDLC analytics with GLQL: pipeline health, merge request cycle time, contributor patterns, and deployment frequency. |
| CI Expert Agent | Job failure causes, pipeline inheritance, slowest jobs, and projects that fail frequently. |
| Developer Flow | A draft merge request from a work item. The graph gives the agent dependencies, owners, and blast radius. |
| Custom flows | Your own flows, when the flow configuration lists the GitLab Orbit tools. |

For example, create a work item that asks to rename the `deploy_user` method.
The Developer Flow uses the graph to find every service that calls the method.
Then it drafts a merge request that updates each one.

Code Review Flow does not use GitLab Orbit.
To use the graph in code review, [add it to a custom flow](#use-gitlab-orbit-in-a-custom-flow).

## Use GitLab Orbit in a custom flow

A [custom flow](https://docs.gitlab.com/user/duo_agent_platform/flows/custom/) can use GitLab Orbit.
Turn on the setting, add the tools to the flow, then tell the agent when to use them.

### Turn on GitLab Orbit for custom flows

Each user who triggers the flow must [turn on GitLab Orbit in GitLab Duo](#turn-on-gitlab-orbit-in-gitlab-duo)
and keep **Other Foundational Agents** selected.
The **Custom Agents** option does not apply to custom flows.

### Add the tools to the flow

Prerequisites:

- You have the Maintainer or Owner role for the project that manages the flow.

To add the tools:

1. In the top bar, select **Search or go to** and find your group or project.
1. Select **AI** > **Flows**.
1. Select the flow you want to edit.
1. In the upper-right corner, select **Edit**.
1. Add the GitLab Orbit tools to the `toolset` of each agent component that needs them:

   ```yaml
   toolset:
     - orbit_list_commands
     - orbit_invoke_command
   ```

1. In the prompt, tell the agent when to use GitLab Orbit.
   For example, tell it to find other projects that import the changed files.
1. Select **Save changes**.

To edit the flow in VS Code, see
[Edit a flow](https://docs.gitlab.com/user/duo_agent_platform/flows/custom/?tab=VS+Code#edit-a-flow).

## Example prompts

Ask these questions in any agent or flow above. The agent selects the tool.

```plaintext
Which projects import the payments-service library?
If I deprecate this function, which other files reference it?
Which projects have the most open merge requests?
Who are the top contributors to this project by merged merge requests?
What are the most common job failure reasons in this group?
Which pipelines take the longest to run?
Show me all critical and high severity open vulnerabilities in this group.
Which projects have unresolved vulnerabilities introduced in the last 30 days?
Which milestones are overdue?
What work items are blocking this epic?
```

For more questions, see [use cases](../use-cases.md).

## Limitations

- GitLab Orbit answers only about groups that have it turned on and that you can access.
- A complex question with many steps can need a follow-up question to narrow the scope.
- For large results, the answer can leave out file text and function bodies.
  To get them, ask the agent to show the source of the function.

## Billing

GitLab Duo queries to GitLab Orbit do not consume GitLab Credits during the beta. For billing after general availability, see [billing](../how-it-works.md#billing).

## Related topics

- [Connect AI agents](_index.md)
- [MCP servers](mcp.md)
- [Use cases](../use-cases.md)
