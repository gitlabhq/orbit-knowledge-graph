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

- **Query recipes** - paste-ready queries for common questions (blast
  radius, pipeline history, contributor patterns, class hierarchies, callers,
  and the merge requests that touched a file).
- **Query language reference** - the full query language so agents compose
  valid queries on the first attempt. GitLab serves the JSON Query DSL or GQL
  guidance, whichever query mode is enabled for you. Both cover token search
  and result truncation.
- **Troubleshooting** - exit codes and the named-query catalog for both modes.
  Empty-result and validation diagnostics are specific to each mode.
- **Repository map helpers** - `glab orbit repo-map` summarizes a local
  checkout. The remote repository map script is served only in JSON mode.

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
