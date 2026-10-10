---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Give your AI agent a graph of your code and your GitLab data, then ask your first question in minutes.
title: GitLab Orbit
---

GitLab Orbit gives your AI agent a graph of your code and your GitLab SDLC data to query.
Ask it what breaks if you change a service, or which CI/CD jobs fail most.
The CLI builds the local code graph on your machine in seconds to minutes, with no GitLab account.

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. This feature is an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- [Introduced](https://gitlab.com/groups/gitlab-org/-/epics/22739) for GitLab Self-Managed in GitLab 19.2.2.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

## Step 1: Install the CLI

Install [`glab`](https://docs.gitlab.com/cli/) 1.117 or later, then run:

```shell
glab orbit --install
```

`glab` verifies the `orbit` binary and keeps it up to date.
For other install methods, see [the GitLab Orbit CLI](cli.md).

## Step 2: Connect your agents

Go to a Git repository and run setup:

```shell
cd path/to/your/repo
glab orbit setup
```

Setup finds the AI agents on your machine.
It gives each agent instructions, hooks, and the `orbit-cli` skill.
Then it indexes the repository. The output is similar to:

```plaintext
◇  Configured ───────────────────────────────╮
│  Claude Code   instructions, hooks, skill  │
│  Codex         instructions, skill         │
│  OpenCode      instructions, hooks, skill  │
├────────────────────────────────────────────╯
│
◇  demo  main @ 779e8c44
└  Done.
```

- To see the changes before setup writes them, add `--dry-run`.
- To set up one agent only, name it. For example, `glab orbit setup claude`.
- To remove the changes, run `glab orbit uninstall`.

## Step 3: Ask your first question

Check the graph from the terminal. Search for a function or class name from your code.
This example uses a small Python repository:

```shell
glab orbit grep authenticate
```

```plaintext
grep "authenticate" @ 779e8c44
exact: authenticate
  Definition:1200522285727076473  src.auth.authenticate  [Function]  src/auth.py:5-7  exact-name
  Definition:6119267860322992230  src.app.login  [Function]  src/app.py:3-4  body-only ×1
      4| return authenticate(request.user, request.password)
```

Then get its source and its callers:

```shell
glab orbit context Definition:1200522285727076473
```

```plaintext
Definition:1200522285727076473  src.auth.authenticate  [Function]  src/auth.py:5-7
5|def authenticate(user, password):
6|    store = SessionStore()
7|    return store.get(user)

Connections (3 indexed):
  <-- src.app.login  [calls]  Definition:6119267860322992230  (src/app.py:3)
  --> src.auth.SessionStore  [calls]  Definition:5672401777261482265  (src/auth.py:1)
  --> src.auth.SessionStore.get  [calls]  Definition:6248168060050738774  (:2)
```

Your agent runs the same commands for you. Open your agent in the repository and ask:

```plaintext
Using GitLab Orbit, what does this project do, and how is it structured?
```

To ask about merge requests, pipelines, or vulnerabilities, run `glab auth login`.
Your top-level group must have [GitLab Orbit turned on](#turn-on-gitlab-orbit-for-your-group).
Then ask:

```plaintext
Using GitLab Orbit, which CI/CD jobs failed most often in <my-group> in the
last 30 days? For the top three, show the failure reasons and the merge
requests with the most failed pipelines.
```

For more prompts, see [use cases](use-cases.md).
To ask in the GitLab UI instead, [turn on GitLab Orbit in GitLab Duo](agents/duo.md#turn-on-gitlab-orbit-in-gitlab-duo).

## Turn on GitLab Orbit for your group

GitLab Orbit indexes a top-level group with its subgroups and projects.

Prerequisites:

- The Owner role for the top-level group.
- A Premium or Ultimate subscription for the group.

To turn on GitLab Orbit on GitLab.com:

1. In the top bar, select **Search or go to** > **Your work**.
1. On the left sidebar, select **GitLab Orbit**.
1. In the upper-right corner, select **Configure**.
1. Select your top-level group.
1. In the dialog, select **Turn on indexing**.

The first index takes a few minutes, or up to 30 minutes for thousands of projects.
To check the progress:

```shell
glab orbit graph-status --full-path <my-group>
```

To query the graph, users need the roles in [security](security.md#roles).
On GitLab Self-Managed, see [turn on indexing for a group](self-managed/orbit-setup.md#turn-on-indexing-for-a-group).

## Next steps

Your agent uses the local code graph and the GitLab server graph. You do not choose a mode.

- [Use cases](use-cases.md): prompts by goal.
- [GitLab Orbit CLI](cli.md): install options and commands.
- [Connect AI agents](agents/_index.md): what setup changes.
- [Query the graph](queries/_index.md): the query language and the REST API.
- [What GitLab Orbit indexes](schema.md): entities and supported languages.
- [How GitLab Orbit works](how-it-works.md): the two graphs and freshness.
- [GitLab Orbit on GitLab Self-Managed](self-managed/_index.md): run it next to GitLab.
