---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Use GitLab Orbit in GitLab Duo, or connect any agent with the GitLab Orbit CLI or the GitLab Orbit MCP endpoint.
title: Connect your agent to GitLab Orbit
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

The GitLab Orbit CLI connects the agents on your machine to GitLab Orbit.
The GitLab Orbit skill teaches your agent to query your GitLab data, such as merge requests, pipelines,
and vulnerabilities, and the [local graph](../cli.md#local-graph).

GitLab Duo Agent Platform has GitLab Orbit built in. For more information, see [GitLab Orbit in GitLab Duo](duo.md).
For agents that cannot run shell commands or that prefer the Model Context Protocol (MCP), use the [GitLab Orbit MCP endpoint](mcp.md).
If your agent already connects to the GitLab MCP server, [add the GitLab Orbit tools](mcp.md#gitlab-orbit-tools-in-the-gitlab-mcp-server) to that connection.

## Set up your agent

Use the [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) to install the GitLab Orbit CLI and set up your agents.
For a short walkthrough, see the [quickstart](../quickstart.md).

Prerequisites:

- `glab` 1.120 or later. To check your version, run `glab version`.
- An agent that setup supports. For the list, see [setup options](#setup-options).

To set up your agent:

1. Go to a Git repository on your machine. Any repository works.
1. Run `glab orbit setup`.
   On first use, `glab` asks to download the GitLab Orbit CLI.
1. In the list of agents, keep or clear the agents that setup selected, then press <kbd>Enter</kbd>.

Setup installs the GitLab Orbit skill for your agents and indexes the repository for code search.
To check that the repository is indexed, run `glab orbit list`.
To load the skill, start a new session of your agent.
To update the setup, run the command again.

To run setup without a terminal, for example from an agent or a script, confirm both prompts with `--yes`:

```shell
glab orbit --yes setup --yes
```

The first `--yes` confirms the download. The second skips the list of agents.

To query your GitLab data, also [sign in with `glab`](../cli.md#query-the-remote-graph).

If you installed the [standalone binary](../cli.md#install-the-standalone-binary), run `orbit` in place of `glab orbit`.

## Setup options

Setup supports Claude Code (`claude`), Codex (`codex`), GitLab Duo (`duo`), OpenCode (`opencode`), and Pi (`pi`).
By default, setup selects the agents it detects on your machine.

```shell
glab orbit setup [<agent>...] [options]
```

| Option | Description |
|--------|-------------|
| `<agent>` | Set up only these agents, for example `claude codex`. Setup adds a named agent even when it does not detect it. |
| `--all` | Set up every supported agent. |
| `--yes`, `-y` | Skip the list of agents. |
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
| Skill | Claude Code, Codex, GitLab Duo, OpenCode | The `orbit` skill, in `~/.agents/skills/orbit`. For GitLab Duo, setup writes the full skill files. For Claude Code, setup also links the skill in `~/.claude/skills/`, and allows the skill command in `settings.json` so that the skill loads without a prompt. |

Other agents that support the [Agent Skills specification](https://agentskills.io/specification), such as Gemini CLI,
can also load the skill from `~/.agents/skills/orbit`.

By default, setup installs a wrapper skill that loads the skill your GitLab instance serves,
so the skill always matches your instance.
If the CLI cannot get the skill from your instance, it uses the copy built into the CLI.

Setup follows these rules:

- Setup changes only the text between its markers and the entries it marked as its own.
- Before setup changes a file for the first time, it saves a copy as `<name>.orbit-backup`.
- Setup sets the `glab` option `orbit_cli_auto_run` to `true` for the repository where you run it, so the hooks run without a prompt.
  To set it for every repository, run `glab config set orbit_cli_auto_run true --global`.
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
- [GitLab Orbit MCP endpoint](mcp.md)
- [GitLab Orbit CLI](../cli.md)
