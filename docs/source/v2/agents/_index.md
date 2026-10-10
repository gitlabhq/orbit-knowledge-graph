---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Configure AI coding agents to use the GitLab Orbit graph with glab orbit setup and the GitLab Orbit skill.
title: Connect AI agents
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

Connect your AI coding agent to the graph.
The agent then finds code and its callers with one command, not many file reads and text searches.

```shell
glab orbit setup
```

You can connect an agent in three ways:

- `glab orbit setup` adds instructions, hooks, and a skill to the agents on your machine.
- The [GitLab Orbit skill](#gitlab-orbit-skill) gives any agent query recipes and the query language reference.
- An [MCP server](mcp.md) gives an agent graph tools. Use it for agents that `glab orbit setup` does not support.

GitLab Duo Agent Platform has GitLab Orbit built in. See [GitLab Orbit in GitLab Duo](duo.md).

## Set up your agents

Prerequisites:

- The [GitLab Orbit CLI](../cli.md) is installed.

This page uses `glab orbit`. If you installed the standalone binary, type `orbit` in place of `glab orbit`.

To set up your agents:

1. Go to a repository on your machine.
1. Run the setup command:

   ```shell
   glab orbit setup
   ```

1. In the list of agents, keep or clear the selection, then press <kbd>Enter</kbd>.

The list shows the agents that setup detects on your machine, all selected.
Setup then indexes the current repository, so your agents have a graph to query.

`glab orbit setup` supports these agents: Claude Code (`claude`), Codex (`codex`), GitLab Duo (`duo`), OpenCode (`opencode`), and Pi (`pi`).

Options:

- Give agent names to select only those agents, for example `glab orbit setup claude codex`.
  A named agent is added even when setup does not detect it.
- `--all` configures every supported agent.
- `--yes` skips the list and applies the selection.
  Without a terminal, for example in a script or an agent, you must use `--yes`.
  Without it, setup stops with `stdin is not a terminal; pass --yes to proceed without a prompt`.
- `--mcp` also registers the local `orbit` MCP server. It is off by default.
- `--skip <component>` leaves out `instructions`, `hooks`, `skill`, or `mcp`. You can repeat it.
- `--no-index` skips the index step.
- `--dry-run` shows the changes and writes nothing. `--verbose` lists every file.

Run the command again at any time. It updates its changes in place.

### What setup changes

`glab orbit setup` changes files that belong to you. It runs only when you start it.
By default, it writes to your user configuration, so the change affects only you.

```shell
glab orbit setup --all --mcp --dry-run --yes
```

```plaintext
┌  Orbit setup (instructions, hooks, skill, mcp server)
│
◇  Plan ─────────────────────────────────────────────────╮
│  Claude Code   instructions, hooks, skill, mcp server  │
│  Codex         instructions, skill, mcp server         │
│  GitLab Duo    instructions                            │
│  OpenCode      instructions, hooks, skill, mcp server  │
│  Pi            instructions                            │
├────────────────────────────────────────────────────────╯
│
◇  Files in your user config ─────────────╮
│  instructions                           │
│    ~/.claude/CLAUDE.md                  │
│    ~/.codex/AGENTS.md                   │
│    ~/.config/opencode/AGENTS.md         │
│    ~/.gitlab/duo/AGENTS.md              │
│    ~/.pi/agent/AGENTS.md                │
│  hooks                                  │
│    ~/.claude/settings.json              │
│    ~/.config/opencode/opencode.json     │
│    ~/.config/opencode/plugins/orbit.js  │
│  skill                                  │
│    ~/.agents/skills/orbit-cli           │
│    ~/.claude/skills/orbit-cli (link)    │
│  mcp server                             │
│    ~/.claude.json                       │
│    ~/.codex/config.toml                 │
│    ~/.config/opencode/opencode.json     │
├─────────────────────────────────────────╯
│
└  Dry run: nothing written.
```

Each component does one job:

- **Instructions**: a block in the instruction file of the agent, between
  `<!-- orbit:setup:begin -->` and `<!-- orbit:setup:end -->`.
  The block tells the agent to use `orbit grep` and `orbit context` for code search.
  Setup does not change text outside the markers.
- **Hooks**: a reminder to use the graph when the agent searches or reads files.
  Claude Code gets a `PreToolUse` hook in `settings.json`.
  OpenCode gets a plugin file and a registration in `opencode.json`.
  Setup replaces or removes only the entries that it marked as its own.
- **Skill**: the `orbit-cli` skill in `.agents/skills/`.
  Claude Code does not read that directory, so it also gets a `.claude/skills/orbit-cli` link.
- **MCP server**: with `--mcp`, an `orbit` entry in the MCP configuration of the agent.
  Setup keeps your other servers and comments.

Before setup changes an existing file for the first time, it copies the file to `<name>.orbit-backup` in the same directory.
Setup never overwrites a backup, so the backup always holds your original file.
To get the original back, copy the backup by hand.

To write into a project, use `--project` for the current directory, or `--dir <path>` for a different directory.
Project files go to the repository root, for example `CLAUDE.md`, `AGENTS.md`, and `.mcp.json`.
Be careful: teams usually commit these files.
The change shows in `git status` and can get to your teammates.

If you do not want setup to change your files, add the same instruction block, MCP entry, and hooks by hand.

### Make Claude Code use the graph first

Claude Code sometimes ignores the graph and starts with a text search.
To prevent this, add `--graph-first`:

```shell
glab orbit setup claude --graph-first
```

With this option, the hook blocks the first search or file read in each Claude Code session.
The block message tells Claude Code to run `orbit grep` first.
Later calls get the usual reminder.
To override the option for one session, set `ORBIT_GRAPH_FIRST=1` or `ORBIT_GRAPH_FIRST=0`.

## Remove the setup

```shell
glab orbit uninstall
```

The list shows the detected agents that have a GitLab Orbit setup in that scope.
Give agent names to remove only those agents.
`glab orbit uninstall` removes the instruction block, hooks, MCP entry, and skill files that setup wrote.
It does not change the rest of each file.
If you edited a file after setup, the command keeps that file and its backup.
A backup is deleted when its file is the same as the original again.
`--yes`, `--dry-run`, `--project`, and `--dir` work as they do for `glab orbit setup`.

## GitLab Orbit skill

The GitLab Orbit skill gives AI coding agents structured help for the graph:

- **Query recipes**: queries for common questions, such as class inheritance, pipeline history, and grouped counts.
- **Query language reference**: the syntax of the enabled query language,
  the JSON Query DSL or the read-only OpenCypher-like GQL syntax.
- **Troubleshooting**: exit codes, steps for empty results, and common errors.
- **Repository maps**: how to use `repo-map` on a checkout.

The `orbit` skill covers the server graph and the local graph.
The `orbit-cli` skill that `glab orbit setup` installs covers only the local graph.

### Install the skill

Prerequisites:

- [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) 1.117 or later.
  If `glab skills` or `glab orbit` is not recognized, update `glab`.

To install the skill for all your projects, run:

```shell
glab skills install --global orbit
```

The skill goes into `~/.agents/skills/orbit`.
To install it for the current project only, run `glab skills install orbit`.
The skill then goes into `.agents/skills/orbit` in the project root.

Claude Code does not read `.agents/skills`. Link the skill into the Claude Code directory:

```shell
ln -s ../../.agents/skills/orbit ~/.claude/skills/orbit
```

### Update the skill

If the skill is installed, `glab` reports that `SKILL.md` exists.
To update the skill, run the install command again with `--force`:

```shell
glab skills install --global --force orbit
```

### Use the wrapper skill

The `orbit-wrapper` skill has no guidance of its own.
It runs `glab orbit skills get orbit` and loads the skill that your GitLab instance serves for your installed CLI.
Because of this, the skill always matches the binary.
It needs `glab` and the GitLab Orbit CLI.
Install it in place of the `orbit` skill, not with it.

To install it with [`npx skills`](https://github.com/vercel-labs/skills), run:

```shell
npx skills add https://gitlab.com/gitlab-org/orbit/knowledge-graph --skill orbit-wrapper
```

In the default permission mode, Claude Code cannot run the `!` command of the skill, and the skill does not load.
Allow the command in `~/.claude/settings.json`, or in `.claude/settings.json` of the project:

```json
{
  "permissions": {
    "allow": ["Bash(glab orbit skills get orbit)"]
  }
}
```

With this rule, the skill loads without a prompt.
Claude Code with `--dangerously-skip-permissions` also loads the skill.

## Related topics

- [MCP servers](mcp.md)
- [GitLab Orbit in GitLab Duo](duo.md)
- [GitLab Orbit CLI](../cli.md)
- [Use cases](../use-cases.md)
