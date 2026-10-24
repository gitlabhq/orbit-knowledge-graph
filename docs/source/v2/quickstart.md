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
- GitLab Duo or an agent of your choice such as Claude Code, Codex, or OpenCode.

## Step 1: Turn on GitLab Orbit for your group

To check whether GitLab Orbit is already on for your group, open the **Status** tab of the [GitLab Orbit dashboard](dashboard.md).
If it is on, go to [step 2](#step-2-set-up-your-agent-to-use-gitlab-orbit).
If it is off and you do not have the role that this step needs, ask a user with the Owner role for the top-level group to turn it on.
On GitLab Self-Managed and GitLab Dedicated, an administrator can also turn it on.

{{< tabs >}}

{{< tab title="GitLab.com" >}}

Prerequisites:

- The Owner role for the top-level group.

To turn on GitLab Orbit:

1. In the top bar, select **Search or go to** and find your top-level group.
1. In the left sidebar, select **Settings** > **GitLab Orbit**.
1. Turn on the **Turn on GitLab Orbit** toggle.
1. In the **Turn on GitLab Orbit for `<my-group>`?** dialog, select **Turn on GitLab Orbit**.

The page shows **GitLab Orbit is indexing `<my-group>`**, then **GitLab Orbit is ready for `<my-group>`**.

{{< /tab >}}

{{< tab title="GitLab Self-Managed and GitLab Dedicated" >}}

Prerequisites:

- Administrator access.
- On GitLab Self-Managed, GitLab Orbit [set up for your instance](https://docs.gitlab.com/orbit/self-managed/).

To turn on GitLab Orbit:

1. In the upper-right corner, select **Admin**.
1. In the left sidebar, select **GitLab Orbit**.
1. Under **Top-level groups**, select your group.
1. Select **Actions** > **Turn on GitLab Orbit**.

To turn on GitLab Orbit for every top-level group, including new ones, turn on
**Turn on GitLab Orbit for all top-level groups automatically**.
For more information, see [turn on indexing for a group](https://docs.gitlab.com/orbit/self-managed/orbit-setup/#turn-on-indexing-for-a-group).

{{< /tab >}}

{{< /tabs >}}

The first index can take up to a few hours.
Continue with the next steps while the index runs.

## Step 2: Set up your agent to use GitLab Orbit

GitLab Duo uses GitLab Orbit automatically.
Other agents, such as Claude Code or Codex, need a one-time setup on your machine.

### Use GitLab Orbit in GitLab Duo

After you complete [step 1](#step-1-turn-on-gitlab-orbit-for-your-group),
GitLab Duo Agent Platform agents use GitLab Orbit by default to answer questions about your GitLab data.
You do not install anything.
To ask your first question, open GitLab Duo Chat and use one of the prompts at the end of this page.
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

To run setup yourself, or for other install methods, see [install the CLI](cli.md#install-the-cli).
For the setup options and what setup changes, see [connect your agent](agents/_index.md).

To check the progress of the first index from step 1, run `glab orbit graph-status` in the clone.
When indexing is complete, the summary ends with `Ready.`
Until then, answers can miss data.

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
- [GitLab MCP endpoint](agents/mcp.md): connect agents that cannot run shell commands
- [Local graph](cli.md#local-graph): index and search code on your machine, with no GitLab account
- [Troubleshooting](troubleshooting.md): fix common errors
