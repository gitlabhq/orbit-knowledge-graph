---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect your agent to GitLab Orbit with the GitLab Orbit CLI and the GitLab Orbit skill.
title: Connect your agent to GitLab Orbit
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment) in GitLab 19.0.
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) from experiment to beta in GitLab 19.1.
- Generally available in GitLab 19.5.

{{< /history >}}

The GitLab Orbit CLI connects the agents on your machine to GitLab Orbit.
One command, `glab orbit setup`, installs the CLI, tells your agents to search code with the
[local graph](../cli.md#local-graph), indexes the repository, and installs the GitLab Orbit skill.
To run it, see [install the CLI](../cli.md#install-the-cli).
For a short walkthrough, see the [quickstart](../quickstart.md).

The GitLab Orbit skill teaches your agent to query your GitLab data, such as merge requests, pipelines,
and vulnerabilities, and the local graph.
It gives the agent query recipes, the query language reference for your instance, and troubleshooting steps.
By default, setup installs a wrapper skill that loads the skill your GitLab instance serves,
so the skill always matches your instance.
If the CLI cannot get the skill from your instance, it uses the copy built into the CLI.

GitLab Duo Agent Platform has GitLab Orbit built in. For more information, see [GitLab Orbit in GitLab Duo](duo.md).
For agents that cannot run shell commands or prefer MCP, use the [GitLab MCP endpoint](mcp.md).

If you installed the standalone binary, run `orbit` in place of `glab orbit`.

## Setup options

Setup supports Claude Code (`claude`), Codex (`codex`), GitLab Duo CLI (`duo`), OpenCode (`opencode`), and Pi (`pi`).
By default, setup selects the agents it detects on your machine.

```shell
glab orbit setup [<agent>...] [options]
```

| Option | Description |
|--------|-------------|
| `<agent>` | Set up only these agents, for example `claude codex`. Setup adds a named agent even when it does not detect it. |
| `--all` | Set up every supported agent. |
| `--yes`, `-y` | Skip the list of agents. In scripts and other places without a terminal, also put `--yes` before `setup` to confirm the download: `glab orbit --yes setup --yes`. |
| `--skip <component>` | Leave out `instructions`, `hooks`, or `skill`. You can repeat it. |
| `--skill full` | Install the full skill files in place of the wrapper skill. |
| `--no-index` | Do not index the repository. |
| `--project` | Write to the current project in place of your user configuration. |
| `--dir <path>` | Write to the project in `<path>`. Implies `--project`. |
| `--dry-run` | Show the changes and write nothing. |
| `--verbose`, `-v` | List every file setup changes. |

## Changes setup makes

By default, setup writes to your user configuration, so the changes affect only you.

| Component | Agents | Changes |
|-----------|--------|---------|
| Instructions | All | A block between `<!-- orbit:setup:begin -->` and `<!-- orbit:setup:end -->` in the instruction file of the agent. The block tells the agent to search with `orbit grep` and to read code with `orbit context`. |
| Hooks | Claude Code, OpenCode | A reminder to use `orbit grep`. For Claude Code, the reminder comes when the agent runs `grep`, `rg`, `ack`, or `ag` in an indexed repository, and never blocks the call. For OpenCode, the reminder comes once each session. |
| Skill | Claude Code, Codex, GitLab Duo CLI, OpenCode | The `orbit` skill, in `~/.agents/skills/orbit`. When GitLab Duo CLI is one of the agents, setup writes the full skill files, and every agent that shares the directory loads them. For Claude Code, setup also links the skill in `~/.claude/skills/`, and allows the skill command in `settings.json` so that the skill loads without a prompt. |

Other agents that support the [Agent Skills specification](https://agentskills.io/specification), such as Gemini CLI,
can also load the skill from `~/.agents/skills/orbit`.

Setup follows these rules:

- Setup changes only the text between its markers and the entries it marked as its own.
- Before setup changes a file for the first time, it saves a copy as `<name>.orbit-backup`.
- Setup sets the `glab` option `orbit_cli_auto_run` to `true`, so the hooks run without a prompt.
- With `--project` or `--dir`, setup writes files such as `CLAUDE.md` and `AGENTS.md` in the project.
  Teams usually commit these files, so the change can reach your teammates.

## Remove the setup

To remove what setup wrote, including the `orbit` skill and the `~/.claude/skills/orbit` link, run:

```shell
glab orbit uninstall
```

The command leaves the rest of each file unchanged.
If you edited a file after setup, the command keeps the file and its backup.
It accepts the same agent names and the `--yes`, `--dry-run`, `--verbose`, `--project`, and `--dir` options as setup.

## Related topics

- [GitLab Orbit quickstart](../quickstart.md)
- [Prompt examples](../prompt-examples.md)
- [GitLab Orbit in GitLab Duo](duo.md)
- [GitLab Orbit MCP server](mcp.md)
- [GitLab Orbit CLI](../cli.md)
