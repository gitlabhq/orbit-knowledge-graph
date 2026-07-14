---
stage: Analytics
group: Knowledge Graph
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
GitLab Knowledge Graph. It includes:

- **Query recipes** - paste-ready JSON bodies for common questions (blast
  radius, pipeline history, contributor patterns).
- **DSL reference** - the full query language so agents compose valid queries
  on the first attempt.
- **Troubleshooting** - exit codes, empty-result diagnostics, and common
  pitfalls.
- **Repository map helpers** - scripts that summarize codebase structure from
  a local checkout or from Orbit Remote.

The skill works with both [Orbit Remote](remote/_index.md) and
[Orbit Local](local/_index.md).

## Prerequisites

- [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) v1.95.0 or later,
  which introduced `glab skills install`. If the subcommand is not recognized,
  update `glab` first.

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

## Update the GitLab Orbit skill

To update to the latest version, re-run the install command with `--force`:

```shell
glab skills install --global --force orbit
```
