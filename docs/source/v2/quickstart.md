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

## Step 1: Turn on GitLab Orbit for your group

A user with the Owner role for the top-level group turns on GitLab Orbit in the [GitLab Orbit dashboard](dashboard.md).
If GitLab Orbit is already on for your group, go to [step 2](#step-2-set-up-your-agent-to-use-gitlab-orbit).
Otherwise, see [turn GitLab Orbit on or off for a group](dashboard.md#turn-gitlab-orbit-on-or-off-for-a-group).

The first index usually completes in a few minutes, and can take up to a few hours for large codebases.
Continue with the next steps while the index runs.

## Step 2: Set up your agent to use GitLab Orbit

GitLab Duo uses GitLab Orbit automatically.
Other agents, such as Claude Code or Codex, need a one-time setup on your machine.

### Use GitLab Orbit in GitLab Duo

To ask your first question, open GitLab Duo Chat and ask:

```plaintext
Use GitLab Orbit to list the 5 most recently updated projects in my group.
```

For more prompts, see [prompt examples](prompt-examples.md).
To choose which agents use GitLab Orbit, see [GitLab Orbit in GitLab Duo](agents/duo.md).

### Use GitLab Orbit with your agent

Connect an agent on your machine to GitLab Orbit, then ask your first question.

To set up your agent:

<!-- vale orbit.StandaloneProductName = NO -->

1. Install the [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) 1.120 or later.
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

To ask your first question:

1. Start a new session of your agent in the repository and ask about your GitLab data:

   ```plaintext
   Using GitLab Orbit, which open merge requests in <my-group>/<my-project> are blocked
   by a failed job? Include only merge requests updated in the last 7 days.
   Group them by job, and show who authored each one.
   ```

   Replace `<my-group>/<my-project>` with the full path of your project.
   The agent writes a graph query, runs it with `glab orbit query`, and answers with only the data that you can access.

1. Ask about your code:

   ```plaintext
   Using GitLab Orbit, what does this project do, and how is it structured?
   ```

## Next steps

- [Prompt examples](prompt-examples.md): prompts by goal
- [GitLab Orbit MCP endpoint](agents/mcp.md): connect agents that cannot run shell commands
- [Local graph](cli.md#local-graph): index and search code on your machine, with no GitLab account
- [Troubleshooting](troubleshooting.md): fix common errors
