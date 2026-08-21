---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Give AI coding agents the bundled orbit-local skill, with DuckDB SQL recipes, repository-map guidance, and gotchas for querying your local GitLab Orbit graph.
title: Set up AI coding agents with the GitLab Orbit Local skill
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.2 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

The `orbit-local` skill teaches AI coding agents to drive your local graph: which DuckDB
tables exist, how to write read-only SQL against them, and which mistakes to avoid.
It ships inside the GitLab Orbit CLI (`orbit`) binary, so it always matches the version
you have installed.

This skill is not the same as the `orbit` skill described in
[Set up AI coding agents with the GitLab Orbit skill](../../ai_coding_agents.md).
That skill targets GitLab Orbit Remote and composes JSON DSL queries. This one targets
GitLab Orbit Local and composes SQL.

## Prerequisites

- The GitLab Orbit CLI (`orbit`) is installed. See
  [Use GitLab Orbit Local with the GitLab Orbit CLI](cli.md).
- A repository is indexed into the local graph.

No GitLab account or network connection is required.

## What the skill contains

| File | Contents |
|------|----------|
| `SKILL.md` | The manifest: gotchas, the command surface, and a quick start. |
| `references/sql.md` | The DuckDB tables and paste-ready SQL recipes. |
| `references/repo_map.md` | The `orbit repo-map` workflow and its subcommands. |
| `references/cli.md` | Wrapper flags, config keys, and pass-through arguments. |

## Read the skill

Print the manifest:

```shell
orbit skill
```

Print one reference file by passing its path relative to the skill root:

```shell
orbit skill references/sql.md
```

Passing an unknown path returns an error that lists every available file.

When you print the manifest, the CLI appends a note explaining that the links in it point
at an on-disk skill tree that does not exist when the binary is your only copy, and that
you can fetch each reference with `orbit skill <path>`. Reference files are printed
without that note.

## Install the skill for an agent

The skill has no installer. To make it discoverable to agents that read the
`~/.agents/skills/` directory, write each file to disk yourself:

```shell
mkdir -p ~/.agents/skills/orbit-local/references
orbit skill > ~/.agents/skills/orbit-local/SKILL.md
orbit skill references/cli.md > ~/.agents/skills/orbit-local/references/cli.md
orbit skill references/sql.md > ~/.agents/skills/orbit-local/references/sql.md
orbit skill references/repo_map.md > ~/.agents/skills/orbit-local/references/repo_map.md
```

Once the manifest sits beside its `references/` directory, its relative links resolve.

To scope the skill to one project instead of your user account, write to
`.agents/skills/orbit-local/` in the project root.

## Keep the skill current

The skill is embedded when the binary is compiled, so `orbit skill` always reflects the
CLI you are running. A copy you exported to disk does not. After you upgrade the CLI,
run the export commands again so your agent reads current guidance.

## Choose between the two skills

| Use the `orbit-local` skill for | Use the `orbit` skill for |
|---------------------------------|---------------------------|
| The current checkout or working tree | Projects already indexed in GitLab |
| A branch that is not pushed or indexed remotely | Cross-project dependency and blast-radius analysis |
| Work that must stay offline | Merge request and contributor aggregation |

Installing both is supported. Each skill describes when to defer to the other, so an
agent can route a request to the right one.

## Configure an assistant to prefer the graph

The skill tells an agent how to query the graph. It does not tell the agent to reach for
the graph first. To add that instruction to an assistant's configuration, use
`orbit setup` with the `--local` flag, described in
[Set up your AI assistant](cli.md#set-up-your-ai-assistant).

The two are independent. Use `orbit setup` on its own, the skill on its own, or both.

## What to try next

- [Connect via MCP](mcp.md) - give agents live tool access to the local graph.
- [Use GitLab Orbit Local with the GitLab Orbit CLI](cli.md) - the full command reference.
- [Use GitLab Orbit Local with the GitLab CLI](glab.md) - run the CLI through `glab orbit local`.
