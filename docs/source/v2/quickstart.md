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

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) for GitLab Self-Managed in GitLab 19.2.2.
- [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.

{{< /history >}}

In this quickstart, you turn on GitLab Orbit for your group, connect your agent, and ask your first question.
To use GitLab Orbit only in GitLab Duo, complete step 1, then see [GitLab Orbit in GitLab Duo](agents/duo.md).

## Prerequisites

- A top-level group with a Premium or Ultimate subscription and the GitLab Orbit add-on.
  To buy the add-on, [contact sales](https://about.gitlab.com/sales/). On GitLab.com, a group can also start a free trial in step 1.
- The Owner role for the group to turn on GitLab Orbit.
  If GitLab Orbit is already on, the Reporter, Security Manager, Developer, Maintainer, or Owner role is enough. Start at step 2.
- An agent on your machine, such as Claude Code, Codex, or OpenCode.
- On GitLab Self-Managed, an administrator has [set up GitLab Orbit](self-managed/_index.md).

<!-- vale orbit.StandaloneProductName = NO -->

To check whether GitLab Orbit is on, in the top bar, select **Search or go to** > **Your work** > **GitLab Orbit**.
If the page shows **Orbit is not turned on for your groups yet**, ask an Owner of the group to turn it on.

## Step 1: Turn on GitLab Orbit for your group

To turn on GitLab Orbit:

1. In the top bar, select **Search or go to** and find your top-level group.
1. In the left sidebar, select **Settings** > **Orbit**.
1. If the page shows that the group is not eligible for a free Orbit trial, select **Contact sales**.
   Otherwise, turn on the **Turn on Orbit** toggle.
1. If the group starts a free trial, a dialog shows [what the trial includes](trial.md#what-the-trial-includes). Select **Turn on Orbit**.

The page shows **Orbit is indexing**, then **Orbit is ready**. The first index can take up to a few hours.
Continue with the next steps while it runs.

<!-- vale orbit.StandaloneProductName = YES -->

On GitLab Self-Managed and GitLab Dedicated, an administrator can also turn on indexing in the **Admin** area.
For more information, see [turn on indexing for a group](self-managed/orbit-setup.md#turn-on-indexing-for-a-group).

## Step 2: Install the CLI and sign in

<!-- vale orbit.StandaloneProductName = NO -->

1. Install the [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) 1.120 or later. To check your version, run `glab version`.
1. Install the GitLab Orbit CLI. `glab` keeps the binary in its own directory, so you run it as `glab orbit`.

   ```shell
   glab orbit --install
   ```

1. Sign in to GitLab. On GitLab Self-Managed or GitLab Dedicated, add `--hostname <gitlab.example.com>`,
   then run `glab config set --global host <gitlab.example.com>` so that `glab orbit` uses your instance in any directory.

   ```shell
   glab auth login
   ```

1. Check the indexing progress. When indexing is complete, the summary ends with `Ready.`

   ```shell
   glab orbit graph-status --full-path <my-group>
   ```

<!-- vale orbit.StandaloneProductName = YES -->

For other install methods, see [the GitLab Orbit CLI](cli.md#install-the-cli).

## Step 3: Connect your agent

Setup also indexes a repository on your machine for code search. Any repository works.

1. Go to a Git repository on your machine:

   ```shell
   cd path/to/your/repo
   ```

1. Set up your agents. In the list, keep the agents that setup found, and press <kbd>Enter</kbd>.

   ```shell
   glab orbit setup
   ```

1. Install the GitLab Orbit skill, so your agent can query your GitLab data:

   ```shell
   glab skills install --global orbit
   ```

1. If you use Claude Code, link the skill. Claude Code does not read `~/.agents/skills`.

   ```shell
   mkdir -p ~/.claude/skills && ln -s ~/.agents/skills/orbit ~/.claude/skills/orbit
   ```

For the setup options and what setup changes, see [Connect your agent](agents/_index.md).

## Step 4: Ask your first question

When `graph-status` shows `Ready.`, start a new session of your agent in the repository and ask about your GitLab data:

```plaintext
Using GitLab Orbit, which CI/CD jobs failed most often in <my-group> in the
last 30 days? For the top three, show the failure reasons and the merge
requests with the most failed pipelines.
```

The agent writes a graph query, runs it with `glab orbit query`, and answers with only the data that you can access.

Then ask about your code:

```plaintext
Using GitLab Orbit, what does this project do, and how is it structured?
```

For more prompts, see [use cases](use-cases.md).

## Next steps

- [Use cases](use-cases.md): prompts by goal.
- [GitLab Orbit in GitLab Duo](agents/duo.md): use GitLab Orbit in the GitLab UI.
- [MCP servers](agents/mcp.md): connect agents that cannot run shell commands.
- [GitLab Orbit Local](local/_index.md): index and search code on your machine, with no GitLab account.
- [Troubleshooting](troubleshooting.md): fix common errors.
