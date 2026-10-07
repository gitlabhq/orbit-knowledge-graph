---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install the GitLab Orbit skill to give AI coding agents ready-to-use query recipes, DSL guidance, and troubleshooting for both GitLab Orbit Remote and GitLab Orbit Local.
title: Set up AI coding agents with the GitLab Orbit skill
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Beta

{{< /details >}}

The GitLab Orbit skill gives AI coding agents structured guidance for querying the
GitLab Orbit graph. It includes:

- **Query recipes** - paste-ready queries for common questions, such as
  class inheritance, pipeline history, and grouped counts.
- **Query language reference** - syntax guidance for the enabled query mode:
  JSON Query DSL or read-only openCypher 9-based syntax.
- **Troubleshooting** - exit codes, empty-result diagnostics, and common
  pitfalls.
- **Repository maps** - the local `repo-map` command for a checkout.
  JSON mode also includes a helper script for GitLab Orbit Remote.

The skill works with both [GitLab Orbit Remote](remote/_index.md) and
[GitLab Orbit Local](local/_index.md).

## Prerequisites

- [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) v1.117.0 or later. It
  forwards `glab orbit <command>` to the GitLab Orbit binary it installs. If
  `glab skills` or `glab orbit` is not recognized, update `glab` first.

## Install the skill

Install globally (available to every project):

```shell
glab skills install --global orbit
```

This installs the skill to `~/.agents/skills/orbit`.

Install for the current project only:

```shell
glab skills install orbit
```

This installs the skill to `.agents/skills/orbit` in the project root.

If the skill is already installed, `glab` reports that `SKILL.md` exists and
suggests `--force` to overwrite.

Claude Code does not scan `.agents/skills`. Link the skill into its own
directory so it can find it:

```shell
ln -s ../../.agents/skills/orbit ~/.claude/skills/orbit
```

[`orbit setup`](local/access/cli.md#set-up-your-ai-agent) writes the local
skill and this link for you.

## Update the GitLab Orbit skill

To update to the latest version, re-run the install command with `--force`:

```shell
glab skills install --global --force orbit
```

## Use the wrapper skill

The `orbit-wrapper` skill copies no guidance. It runs
`glab orbit skills get orbit` and loads the skill your GitLab instance serves,
composed with your installed GitLab Orbit CLI, so it can't drift from the binary.
It needs `glab` and the GitLab Orbit CLI installed. Install it instead of the `orbit`
skill, not alongside it. Install it with
[`npx skills`](https://github.com/vercel-labs/skills):

```shell
npx skills add https://gitlab.com/gitlab-org/orbit/knowledge-graph --skill orbit-wrapper
```

In Claude Code's default permission mode, Claude Code asks you to approve the
skill the first time it runs. The skill's `allowed-tools` covers its
`glab orbit skills get orbit` command, so approving the skill is enough. To skip
the prompt, for example in non-interactive `claude -p` runs, allow the skill in
`~/.claude/settings.json`, or in the project's `.claude/settings.json`:

```json
{
  "permissions": {
    "allow": ["Skill(orbit-wrapper)"]
  }
}
```

A plugin install names the skill `orbit:orbit-wrapper`.
