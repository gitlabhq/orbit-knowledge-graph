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

Turn on GitLab Orbit for your group, then choose one of two paths:
use GitLab Orbit in GitLab Duo, or connect your own coding agent and ask your first question.

## Prerequisites

- A top-level group with access to GitLab Orbit.
  To get access, [contact sales](https://about.gitlab.com/sales/).
  On GitLab.com, a group can also start a free trial in [step 1](#step-1-turn-on-gitlab-orbit-for-your-group).
- To query the graph, the Reporter, Security Manager, Developer, Maintainer, or Owner role for the group.
- To use your own coding agent, an agent on your machine, such as Claude Code, Codex, or OpenCode.

<!-- vale orbit.StandaloneProductName = NO -->

To check whether GitLab Orbit is on for your group, in the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.
Then select the **Group status** tab.
For more information, see [GitLab Orbit dashboard](dashboard.md).
If the page shows **Orbit is not turned on for your groups yet**, ask a user with the Owner role to turn on GitLab Orbit for the group.
If GitLab Orbit is already on for your group, skip [step 1](#step-1-turn-on-gitlab-orbit-for-your-group),
and go to [path 1](#path-1-use-gitlab-orbit-in-gitlab-duo) or [path 2](#path-2-use-gitlab-orbit-with-your-own-coding-agent).

## Step 1: Turn on GitLab Orbit for your group

{{< tabs >}}

{{< tab title="GitLab.com" >}}

Prerequisites:

- The Owner role for the top-level group.

To turn on GitLab Orbit:

1. In the top bar, select **Search or go to** and find your top-level group.
1. In the left sidebar, select **Settings** > **Orbit**.
1. Turn on the **Turn on Orbit** toggle.

The page shows **Orbit is indexing `<my-group>`**, then **Orbit is ready for `<my-group>`**.

You can also try GitLab Orbit with a free trial.
For more information, see [Start a free trial](trial.md).

{{< /tab >}}

{{< tab title="GitLab Self-Managed and GitLab Dedicated" >}}

Prerequisites:

- Administrator access.
- On GitLab Self-Managed, GitLab Orbit [set up for your instance](self-managed/_index.md).

To turn on GitLab Orbit:

1. In the upper-right corner, select **Admin**.
1. In the left sidebar, select **Orbit**.
1. Under **Top-level groups**, select your group.
1. Select **Actions** > **Turn on Orbit**.

To turn on GitLab Orbit for every top-level group, including new ones, turn on
**Turn on Orbit for all top-level groups automatically**.
For more information, see [turn on indexing for a group](self-managed/orbit-setup.md#turn-on-indexing-for-a-group).

{{< /tab >}}

{{< /tabs >}}

<!-- vale orbit.StandaloneProductName = YES -->

The first index can take up to a few hours.
Continue with the next steps while the index runs.

## Path 1: Use GitLab Orbit in GitLab Duo

GitLab Orbit is on in GitLab Duo by default.
After you complete [step 1](#step-1-turn-on-gitlab-orbit-for-your-group),
GitLab Duo Agent Platform agents use the graph to answer questions about your GitLab data.
You do not install anything.
To choose which agents use GitLab Orbit, see [GitLab Orbit in GitLab Duo](agents/duo.md).

## Path 2: Use GitLab Orbit with your own coding agent

Sign in to GitLab, set up your agent, and ask your first question.

### Set up GitLab Orbit with your agent

If your agent already runs on your machine, install `glab` and sign in with `glab auth login`.
Then paste this prompt, and the agent completes step 2 for you:

```plaintext
Install the GitLab Orbit CLI and connect it to this agent. Follow https://docs.gitlab.com/orbit/quickstart/
(the GitLab Orbit documentation index is https://docs.gitlab.com/llms.txt). Run `glab orbit setup`
in this repository. When you finish, run `glab orbit graph-status` and show me the result.
```

### Step 2: Sign in and set up your agent

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

1. Go to a Git repository on your machine.
   Any repository works.

   ```shell
   cd path/to/your/repo
   ```

1. Run setup.
   On first use, `glab` asks to download the GitLab Orbit CLI.
   In the list, keep the agents that setup found, and press <kbd>Enter</kbd>.

   ```shell
   glab orbit setup
   ```

<!-- vale orbit.StandaloneProductName = YES -->

Setup installs the GitLab Orbit skill for your agents, and indexes the repository for code search.
For other install methods, see [the GitLab Orbit CLI](cli.md#other-install-methods).
For the setup options and what setup changes, see [connect your agent](agents/_index.md).

### Step 3: Ask your first question

1. In a clone of a project in your group, check the indexing progress.
   When indexing is complete, the summary ends with `Ready.`

   ```shell
   glab orbit graph-status
   ```

1. Start a new session of your agent in the repository and ask about your GitLab data:

   ```plaintext
   Using GitLab Orbit, which projects in <my-group> had the most failed pipelines
   in the last 30 days? For the top three, show the failure reasons of the failed
   jobs and the merge requests with the most failed pipelines.
   ```

   The agent writes a graph query, runs it with `glab orbit query`, and answers with only the data that you can access.

1. Ask about your code:

   ```plaintext
   Using GitLab Orbit, what does this project do, and how is it structured?
   ```

## Next steps

- [Prompt examples](prompt-examples.md): prompts by goal
- [MCP server](agents/mcp.md): connect agents that cannot run shell commands
- [Local graph](cli.md#local-graph): index and search code on your machine, with no GitLab account
- [Troubleshooting](troubleshooting.md): fix common errors
