---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Give AI coding agents the bundled orbit-local skill, with SQL recipes, repository-map guidance, and gotchas for querying your local GitLab Orbit graph.
title: Set up agents with the GitLab Orbit Local skill
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.2 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

The [`orbit-local` skill](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/tree/main/skills/orbit-local?ref_type=heads)
teaches coding agents how to interact
with your local graph. After running the skill,
agents learn how to find local tables, write SQL queries
against them, and how to avoid mistakes.

The skill ships inside the GitLab Orbit CLI (`orbit`) binary,
so it always matches the version you have installed.

This skill is not the same as the `orbit` skill for GitLab Orbit Remote.
Installing both skills is supported,
and each skill describes when to defer to the
other, so an agent can route a request to the right one.

For more information
about the `orbit` skill,
see [set up agents with the GitLab Orbit skill](../../ai_coding_agents.md).

## Prerequisites

- Install the [GitLab Orbit CLI (`orbit`)](./cli.md).
- Index a repository into the local graph.

No GitLab account or network connection is required.

### Install the skill for an agent

The skill has no installer. To make it discoverable to agents that read the
`~/.agents/skills/` directory, write each file to disk yourself:

```shell
mkdir -p ~/.agents/skills/orbit-local/references
orbit skill > ~/.agents/skills/orbit-local/SKILL.md
orbit skill references/cli.md > ~/.agents/skills/orbit-local/references/cli.md
orbit skill references/sql.md > ~/.agents/skills/orbit-local/references/sql.md
orbit skill references/repo_map.md > ~/.agents/skills/orbit-local/references/repo_map.md
```

To scope the skill to one project instead of your user account, write to
`.agents/skills/orbit-local/` in the project root.

### Update the skill

The skill is bundled in the CLI binary. To update the skill:

1. Update the CLI by re-running the
   [installation](../getting-started.md#install) method you used.
1. Re-run the agent installation commands to overwrite the files on disk.

Your agent then reads the guidance that matches your installed CLI version.

### Configure an agent to prefer the graph

The skill tells an agent how to query the graph.
It does not tell the agent to reach for
the graph first. To add that instruction to an agent's configuration, run:

```shell
orbit setup --local <ai_assistant>
```

For more information, see
[set up your AI assistant](cli.md#set-up-your-ai-assistant).
