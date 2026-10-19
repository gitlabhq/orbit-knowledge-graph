---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect your agent to GitLab Orbit with the GitLab Orbit CLI and the GitLab Orbit skill.
title: Connect your agent
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

<!-- TODO(GA): set the GA release and link the GA work item in the history line. -->

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- Generally available in GitLab 19.x.

{{< /history >}}

Connect the agents on your machine to GitLab Orbit with two commands. Run them in a Git repository:

```shell
glab orbit setup
glab skills install --global orbit
```

- `glab orbit setup` tells your agents to search code with the graph,
  and indexes the repository into the [local graph](../local/_index.md).
- `glab skills install --global orbit` installs the [GitLab Orbit skill](#gitlab-orbit-skill),
  which teaches your agent to query your GitLab data, such as merge requests, pipelines, and vulnerabilities.

GitLab Duo Agent Platform has GitLab Orbit built in. See [GitLab Orbit in GitLab Duo](duo.md).
For agents that cannot run shell commands, use an [MCP server](mcp.md).

This page uses `glab orbit`. If you installed the standalone binary, type `orbit` in place of `glab orbit`.

## Set up your agents

Prerequisites:

- The [GitLab Orbit CLI](../cli.md) is installed.

To set up your agents:

1. Go to a Git repository on your machine.
1. Run `glab orbit setup`.
1. In the list of agents, keep or clear the selection, then press <kbd>Enter</kbd>.
   The agents that setup detects on your machine are selected.

Setup supports Claude Code (`claude`), Codex (`codex`), GitLab Duo (`duo`), OpenCode (`opencode`), and Pi (`pi`).
To update the setup, run the command again.

Options:

- Name agents to set up only those, for example `glab orbit setup claude codex`.
  Setup adds a named agent even when it does not detect it. `--all` sets up every supported agent.
- `--yes` skips the list. Use it in scripts and other places without a terminal.
  Without it, setup stops with `stdin is not a terminal; pass --yes to proceed without a prompt`.
- `--mcp` also registers the [local MCP server](mcp.md#local-mcp-server).
- `--skip <component>` leaves out `instructions`, `hooks`, `skill`, or `mcp`. You can repeat it.
- `--no-index` skips indexing.
- `--dry-run` shows the changes and writes nothing. Add `--verbose` to list every file.

### What setup changes

By default, setup writes to your user configuration, so the changes affect only you.

| Component | What it adds | Agents |
|-----------|--------------|--------|
| Instructions | A block between `<!-- orbit:setup:begin -->` and `<!-- orbit:setup:end -->` in the instruction file of the agent. The block tells the agent to search with `orbit grep` and to read code with `orbit context`. | All |
| Hooks | A reminder to use `orbit grep`. For Claude Code, the reminder comes when the agent runs `grep`, `rg`, `ack`, or `ag` in an indexed repository, and never blocks the call. For OpenCode, it comes once each session. | Claude Code, OpenCode |
| Skill | The `orbit-cli` skill for the local graph, in `~/.agents/skills/`. Claude Code also gets a link in `~/.claude/skills/`. | Claude Code, Codex, OpenCode |
| MCP server | With `--mcp`, an `orbit` entry in the MCP configuration of the agent. | Claude Code, Codex, OpenCode |

- Setup changes only the text between its markers and the entries that it marked as its own.
- Before setup changes a file for the first time, it saves a copy as `<name>.orbit-backup`.
- `glab` asks for confirmation before it runs the GitLab Orbit binary. When you run setup through `glab`,
  setup sets the `glab` option `orbit_cli_auto_run` to `true`, so the hooks run without that prompt.
- To write to a project instead, use `--project` for the current directory, or `--dir <path>`.
  Teams usually commit files such as `CLAUDE.md`, `AGENTS.md`, and `.mcp.json`, so the change can reach your teammates.

## GitLab Orbit skill

The GitLab Orbit skill gives your agent query recipes, the reference for the query language of your instance,
and troubleshooting steps. It covers your GitLab data and the local graph.
The `orbit-cli` skill that setup installs covers only the local graph, so install both.

Prerequisites:

- You are signed in with `glab auth login`.
- GitLab Orbit is [turned on for your group](../quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group).

To install the skill for all your projects:

```shell
glab skills install --global orbit
```

Claude Code does not read `~/.agents/skills`. If you use Claude Code, also link the skill:

```shell
mkdir -p ~/.claude/skills && ln -s ~/.agents/skills/orbit ~/.claude/skills/orbit
```

- To install the skill for the current project only, leave out `--global`.
- To update the skill, run the install command again with `--force`.

### Use the wrapper skill

The `orbit-wrapper` skill loads the skill that your GitLab instance serves, so it always matches your instance and CLI.
The installed `orbit` skill is a copy that you update with `--force`.
Install it in place of the `orbit` skill. The `npx` command needs [Node.js](https://nodejs.org/):

```shell
npx skills add https://gitlab.com/gitlab-org/orbit/knowledge-graph --skill orbit-wrapper
```

Claude Code loads the wrapper only if it can run `glab orbit skills get orbit` without a prompt.
Allow the command in `~/.claude/settings.json`:

```json
{
  "permissions": {
    "allow": ["Bash(glab orbit skills get orbit)"]
  }
}
```

## Remove the setup

```shell
glab orbit uninstall
```

`glab orbit uninstall` removes what setup wrote and leaves the rest of each file unchanged.
If you edited a file after setup, it keeps the file and its backup.
It accepts the same agent names and the `--yes`, `--dry-run`, `--project`, and `--dir` options as setup.

To remove the GitLab Orbit skill, delete `~/.agents/skills/orbit` and the `~/.claude/skills/orbit` link.

## Related topics

- [GitLab Orbit quickstart](../quickstart.md)
- [Use cases](../use-cases.md)
- [GitLab Orbit in GitLab Duo](duo.md)
- [MCP servers](mcp.md)
- [GitLab Orbit CLI](../cli.md)
