---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Turn on GitLab Orbit for your group, connect your agent, and ask your first question about your merge requests, pipelines, and code.
title: GitLab Orbit quickstart
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

Turn on GitLab Orbit for your group, then set up your agent and ask your first question
about your GitLab data.

## Prerequisites

- Access to GitLab Orbit for the top-level group.
  To get access, start a [free trial on GitLab.com](trial.md), or [contact sales](https://about.gitlab.com/sales/).
- Reporter, Security Manager, Developer, Maintainer, or Owner role for the group.
- GitLab Duo, or an agent that [setup supports](agents/_index.md#setup-options), such as Claude Code, Codex, or OpenCode.
  For other agents, see the [GitLab Orbit MCP endpoint](agents/mcp.md).

## Step 1: Turn on GitLab Orbit

A user with the Owner role for the top-level group turns on GitLab Orbit in the [GitLab Orbit dashboard](dashboard.md).
If GitLab Orbit is already on for your group, go to [step 2](#step-2-connect-your-agent).
Otherwise, see [turn GitLab Orbit on or off for a group](dashboard.md#turn-gitlab-orbit-on-or-off-for-a-group).

The first index usually completes in a few minutes, and can take up to a few hours for large codebases.
Continue with the next steps while the index runs.

## Step 2: Connect your agent

GitLab Duo uses GitLab Orbit with no setup.
Other agents, such as Claude Code or Codex, need a one-time setup on your machine.

### GitLab Duo

<!-- vale orbit.StandaloneProductName = NO -->

GitLab Orbit is on by default in GitLab Duo.
To check, open GitLab Duo Chat and find **Orbit** in the chat header:

- If the status dot is green, GitLab Orbit is on. Go to [step 3](#step-3-ask-your-first-question).
- If the status dot is empty, select **Orbit**, then turn on the toggle for the agent you chat with.
  For more information, see [troubleshooting](troubleshooting.md#the-status-dot-next-to-orbit-is-not-green).

<!-- vale orbit.StandaloneProductName = YES -->

To choose which agents use GitLab Orbit, see [GitLab Orbit in GitLab Duo](agents/duo.md).

### Other agents

To set up an agent on your machine:

<!-- vale orbit.StandaloneProductName = NO -->

1. Install the [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) 1.126 or later.
   To check your version, run `glab version`.
1. Sign in to GitLab:

   ```shell
   glab auth login
   ```

   On GitLab Self-Managed or GitLab Dedicated, run `glab auth login --hostname <gitlab.example.com>`.
   To use `glab orbit` outside a clone of a project on your instance, also run
   `glab config set --global host <gitlab.example.com>`.

1. In a clone of a project in your group, start your agent and paste this prompt:

   ```plaintext
   Set up GitLab Orbit for this agent. In this repository, run
   `glab orbit --yes setup --yes`, then run `glab orbit graph-status` and
   show me the result. If a command fails, see
   https://docs.gitlab.com/orbit/troubleshooting/.
   ```

   The agent downloads the GitLab Orbit CLI, installs the GitLab Orbit skill for the agents
   that setup detects on your machine, and indexes the repository for code search.

<!-- vale orbit.StandaloneProductName = YES -->

To run setup yourself, see [connect your agent](agents/_index.md).

To check indexing progress, run `glab orbit graph-status` until the summary ends with `Ready.`

## Step 3: Ask your first question

In a clone of a project in your group, ask your agent:

```plaintext
Using GitLab Orbit, what does this project do, and how is it structured?
```

The agent writes a graph query, runs it, and answers with only the data that you can access.

For a guided first session, use an agent that runs shell commands, such as Claude Code or Codex.
Set it up as in [other agents](#other-agents), then paste this prompt:

```plaintext
Get started with GitLab Orbit: run `glab orbit skills get orbit-start`
and follow the skill that it prints.
```

The agent asks which project or group to use, answers a first question about it,
and offers to show how GitLab Orbit works.

GitLab Duo Chat cannot run shell commands.
In GitLab Duo Chat, or to ask your own question, use these prompts.
Replace `<my-group>` and `<my-group>/<my-project>` with the full paths of your group and project.

- Find the merge requests that a failing job blocks:

  ```plaintext
  Using GitLab Orbit, which open merge requests in <my-group>/<my-project> are blocked
  by a failed job? Include only merge requests updated in the last 7 days.
  Group them by job, and show who authored each one.
  ```

- Find the open vulnerabilities to fix first:

  ```plaintext
  Using GitLab Orbit, which projects in <my-group> have open critical or high
  vulnerabilities, and how many does each have?
  ```

- Check what a merge request affects before you approve it:

  ```plaintext
  Using GitLab Orbit, review merge request <!123> in <my-group>/<my-project>.
  Which callers of the code that it changes are outside the merge request?
  ```

For more prompts, see [prompt examples](prompt-examples.md).

## Next steps

- [Prompt examples](prompt-examples.md): prompts by goal
- [GitLab Orbit MCP endpoint](agents/mcp.md): connect agents that cannot run shell commands
- [Local graph](cli.md#local-graph): index and search code on your machine, with no GitLab account
- [Troubleshooting](troubleshooting.md): fix common errors
