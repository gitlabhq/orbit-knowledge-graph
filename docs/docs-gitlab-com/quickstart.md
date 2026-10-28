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
To try GitLab Orbit, start a [free trial on GitLab.com](trial.md), or [contact sales](https://about.gitlab.com/sales/).

## Prerequisites

- Reporter, Security Manager, Developer, Maintainer, or Owner role for the group.
- GitLab Duo, or an agent that [`glab orbit setup` supports](agents/_index.md#setup-options), such as Claude Code, Codex, or OpenCode.
  For other agents, use the Model Context Protocol (MCP) endpoint.
  For more information, see [GitLab Orbit MCP endpoint](agents/mcp.md).
- [GitLab CLI](https://docs.gitlab.com/cli/) 1.120.0 or later.

## Step 1: Turn on GitLab Orbit

A user with the Owner role for the top-level group turns on GitLab Orbit in the GitLab Orbit dashboard.
If GitLab Orbit is already on for your group, go to [step 2](#step-2-connect-your-agent).
Otherwise, see [turn GitLab Orbit on or off for a group](dashboard.md#turn-gitlab-orbit-on-or-off-for-a-group).

The first index usually completes in a few minutes, and can take up to a few hours for large codebases.
Continue with the next steps while the index runs.

## Step 2: Connect your agent

GitLab Duo is connected to GitLab Orbit by default.
Other agents, such as Claude Code or Codex, need a one-time setup on your machine.

### GitLab Duo

<!-- vale orbit.StandaloneProductName = NO -->

GitLab Orbit is on by default in GitLab Duo.
To check, open GitLab Duo Chat and find **Orbit** in the chat header:

- If the status dot is green, GitLab Orbit is on. Go to [step 3](#step-3-ask-your-first-question).
- If the status dot is empty, select **Orbit**, then turn on the toggle for the agent you chat with.
  For more information, see [troubleshooting](troubleshooting.md#the-status-dot-next-to-orbit-is-not-green).

<!-- vale orbit.StandaloneProductName = YES -->

To choose which agents use GitLab Orbit, see [GitLab Orbit in GitLab Duo](agents/duo.md#choose-which-agents-use-gitlab-orbit).

### Other agents

To set up an agent on your machine:

<!-- vale orbit.StandaloneProductName = NO -->

1. Sign in to the GitLab CLI:

   ```shell
   glab auth login
   ```

   On GitLab Self-Managed or GitLab Dedicated, sign in to your instance instead:

   ```shell
   glab auth login --hostname <gitlab.example.com>
   ```

   To use `glab orbit` outside a clone of a project on your instance, also set your instance as the default host:

   ```shell
   glab config set --global host <gitlab.example.com>
   ```

1. In a clone of a project in your group, start your agent and paste the following prompt:

   ```plaintext
   Set up GitLab Orbit for this agent. In this repository, run
   `glab orbit --yes setup --yes`, then run `glab orbit status --full-path`
   with this repository's GitLab project path and show me the result. If a command fails, see
   https://docs.gitlab.com/orbit/troubleshooting/.
   ```

   The agent downloads the GitLab Orbit CLI, installs the GitLab Orbit skill for the agents
   that `glab orbit setup` detects on your machine, and indexes the repository for code search.

<!-- vale orbit.StandaloneProductName = YES -->

To run `glab orbit setup` yourself, see [connect your agent to GitLab Orbit](agents/_index.md).

To check indexing progress, run the following command until the summary ends with `Ready.`:

```shell
glab orbit status --full-path <my-group/my-project>
```

## Step 3: Ask your first question

In a clone of a project in your group, ask your agent:

```plaintext
Using GitLab Orbit, what does this project do, and how is it structured?
```

The agent writes a graph query, runs it, and answers with only the data that you can access.

For a guided first session in an agent that runs shell commands, such as Claude Code or Codex,
paste this prompt instead:

```plaintext
Get started with GitLab Orbit: run `glab orbit skills get orbit-start`
and follow the skill that it prints.
```

Replace `<my-group>` and `<my-group>/<my-project>` with the full paths of your group and project.
To ask more questions in any agent, including GitLab Duo Chat, try the following prompts:

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

## Related topics

- [GitLab Orbit prompt examples](prompt-examples.md)
- [Local graph](cli.md#local-graph)
- [Troubleshooting GitLab Orbit](troubleshooting.md)
