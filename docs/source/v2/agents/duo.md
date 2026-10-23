---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use GitLab Orbit in GitLab Duo Agent Platform agents and flows to ground answers in your live GitLab data.
title: GitLab Orbit in GitLab Duo
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.
- GitLab Orbit in GitLab Duo [turned on by default](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/261361) in GitLab 19.6.

{{< /history >}}

GitLab Duo Agent Platform agents use GitLab Orbit to answer questions about your GitLab data, such as
cross-project dependencies, blast radius, pipeline inheritance, and vulnerability history.
You do not install anything.
Agents call the GitLab Orbit tools when the graph can answer a question, and their other tools when it cannot.

## Choose which agents use GitLab Orbit

GitLab Orbit is on in GitLab Duo by default for these users:

- On GitLab.com, each member of a top-level group where GitLab Orbit is turned on.
- On GitLab Self-Managed and GitLab Dedicated, all users.
  An administrator can turn off GitLab Orbit in GitLab Duo for the instance.
  For more information, see [administer GitLab Orbit](https://docs.gitlab.com/integration/orbit/).

Prerequisites:

- GitLab Orbit is [turned on for your group](../quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group).
- You have access to [GitLab Duo Agent Platform](https://docs.gitlab.com/user/duo_agent_platform/).

<!-- vale orbit.StandaloneProductName = NO -->

To choose which agents use GitLab Orbit:

1. In the upper-right corner, select your avatar.
1. Select **Preferences**.
1. On the **AI assistance** tab, in the **GitLab Orbit** section, select or clear the checkboxes.
   All of them are selected by default.

   | Checkbox | Agents that get the GitLab Orbit tools |
   |----------|----------------------------------------|
   | **Agentic Chat** | GitLab Duo Agentic Chat. |
   | **Orbit Agent** | The standalone GitLab Orbit agent in GitLab Duo. |
   | **Other Foundational Agents** | Foundational agents outside chat, for example the Security Analyst Agent. |
   | **Custom Agents** | Custom agents that users build. |

1. Select **Save changes**.
1. Open GitLab Duo Chat and confirm that the status dot next to **Orbit** in the chat header is green.

In the chat header, select **Orbit** to turn the **Agentic chat**, **Foundational agents**, and **Custom agents** toggles
on or off without leaving the chat.

<!-- vale orbit.StandaloneProductName = YES -->

## Agents and flows that use GitLab Orbit

| Agent or flow | Description |
|---------------|-------------|
| Default GitLab Duo agent | General help with code, planning, security, and project management. |
| Planner Agent | Work item owners, blockers, contributor load, and milestone progress across projects. |
| Security Analyst Agent | Open vulnerabilities by severity, CVE coverage in the group, and when vulnerabilities were introduced. |
| Data Analyst Agent | Software development lifecycle (SDLC) analytics, such as pipeline health, merge request cycle time, contributor patterns, and deployment frequency. |
| CI Expert Agent | Job failure causes, pipeline inheritance, slowest jobs, and projects that fail frequently. |
| Developer Flow | A draft merge request from a work item. The graph gives the agent dependencies, owners, and blast radius. |
| Custom flows | Your own flows, when the flow configuration lists the GitLab Orbit tools. |

For example, for a work item that asks to rename the `deploy_user` method, the Developer Flow finds every service
that calls the method and drafts a merge request that updates each one.

Code Review Flow does not use GitLab Orbit.
To use the graph in code review, [add GitLab Orbit to a custom flow](#use-gitlab-orbit-in-a-custom-flow).

## Use GitLab Orbit in a custom flow

To use GitLab Orbit in a [custom flow](https://docs.gitlab.com/user/duo_agent_platform/flows/custom/), add its tools to the flow.

Prerequisites:

- You have the Maintainer or Owner role for the project that manages the flow.
- Each user who triggers the flow keeps **Other Foundational Agents** selected in their
  [GitLab Orbit preferences](#choose-which-agents-use-gitlab-orbit).
  The **Custom Agents** option does not apply to flows.

To add the tools:

1. In the left sidebar, select **Search or go to** and find your group or project.
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

### Give a flow the local graph

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259938) in GitLab 19.6 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `duo_developer_orbit`. Disabled by default.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.

When a flow has a checkout of the project, the [GitLab Orbit CLI](../cli.md) installs the
[GitLab Orbit skill](_index.md#gitlab-orbit-skill) and indexes the checkout before the agent starts.
The agent can then search the code of the current branch with the [local graph](../cli.md#local-graph).
The skill also covers your GitLab data when **Other Foundational Agents** is selected
in the GitLab Orbit preferences of the user who triggers the flow.

Prerequisites:

- The flow does not set [`coding_environment: none`](https://docs.gitlab.com/user/duo_agent_platform/flows/custom_flows_schema/#coding_environment).
- The agent components that use the skill have the `run_command` tool, so they can run `glab orbit` commands.

## Example prompts

Ask these questions in any agent or flow that uses GitLab Orbit:

```plaintext
Which projects import the payments-service library?
What are the most common job failure reasons in this group?
Show me all critical and high severity open vulnerabilities in this group.
What work items are blocking this epic?
```

For more prompts, see [prompt examples](../prompt-examples.md).

## Known issues

- GitLab Orbit answers only about groups that have it turned on and that you can access.
- A complex question might need a follow-up question to narrow the scope.
- For large results, the answer might leave out source code. To get it, ask the agent to show the source.

## Related topics

- [Connect your agent](_index.md)
- [GitLab Orbit MCP server](mcp.md)
- [Prompt examples](../prompt-examples.md)
