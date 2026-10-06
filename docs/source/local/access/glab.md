---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Connect AI agents and MCP clients to GitLab Orbit Local with the GitLab CLI.
title: Use GitLab Orbit Local with the GitLab CLI (`glab`)
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

> [!disclaimer]

Install and run GitLab Orbit Local with the GitLab CLI (`glab`).

Use `glab orbit` to index local repositories and connect AI agents
and MCP clients to the code graph.

## Differences from the standalone binary

`glab orbit` and the standalone [`orbit` binary](cli.md) support the same commands.
When you use `glab orbit`, the following credential and installation setup applies:

- Installation and updates to the `orbit` binary are handled automatically.
- Your GitLab credentials are passed to the binary. You can then run
  [GitLab Orbit Remote commands](../../remote/access/glab.md) without authenticating again.

For more information about `glab`, see [GitLab CLI](https://docs.gitlab.com/cli/).

## Prerequisites

- `glab` 1.117.0 or later

## Set up GitLab Orbit Local with `glab`

Set up GitLab Orbit Local with `glab` to build a graph of your repository and connect your AI
agents to it.

To set up GitLab Orbit Local:

1. Install the managed binary:

   ```shell
   glab orbit --install
   ```

1. Verify the installation:

   ```shell
   glab orbit version
   ```

1. Index your repository:

   ```shell
   glab orbit index <path/to/repository>
   ```

1. Connect the AI agents on your machine to the graph:

   ```shell
   glab orbit setup
   ```

   The command writes instructions into each agent's instruction file and installs
   the GitLab Orbit skill.
   Before you run `glab orbit setup`, review [the files it changes](cli.md#what-it-changes).
   To undo the changes, run `glab orbit uninstall`.

1. Optional. To connect an MCP client by hand instead, start the MCP server with
   `glab orbit mcp serve`.
   For configuration for each client, see [connect through the MCP](mcp.md).

## Commands

`glab orbit <command>` behaves the same as `orbit <command>`.
Local commands run against the graph on your machine.
Hosted commands call GitLab Orbit Remote.

| Command type | Commands | Requirements |
|--------------|----------|--------------|
| Local | `index`, `grep`, `context`, `sql`, `schema`, `list`, `mcp`, `repo-map` | No GitLab account or network connection after installation. |
| Hosted | `query`, `status`, `ontology`, `dsl`, `tools`, `graph-status` | An authenticated GitLab account and network access. |

For help in the binary, run `glab orbit <command> --help`.

For information about GitLab Orbit commands that `glab` handles itself,
see the [`glab orbit` reference](https://docs.gitlab.com/cli/orbit/).
