---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Pick an access method and build your first local GitLab Orbit graph.
title: Get started with GitLab Orbit Local
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

GitLab Orbit Local runs on your machine. Install the `orbit` binary, pick the access
method that matches how you work, then run your first query.

## Install

Install the `orbit` binary directly with the one-line installer, or through
the GitLab CLI (`glab`) if you already use it.

On Linux, the installer uses the glibc archive by default and automatically
selects the fully static musl archive on musl-based distributions like Alpine.
To force the static Linux archive, pass `--libc musl`.

{{< tabs >}}

{{< tab title="macOS and Linux" >}}

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

To explicitly install the static musl binary (e.g. on a glibc system):

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --libc musl
```

Open a new terminal, then verify:

```shell
orbit help
```

{{< /tab >}}

{{< tab title="Windows" >}}

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 | iex
```

Open a new terminal, then verify:

```shell
orbit help
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab)" >}}

If you already have [`glab`](https://gitlab.com/gitlab-org/cli) installed:

```shell
glab orbit local --install
```

Verify:

```shell
glab orbit local help
```

See the [`glab orbit local` reference](https://docs.gitlab.com/cli/orbit/local/)
for details.

{{< /tab >}}

{{< /tabs >}}

## Pick an access method

| Method | Best for | Setup |
|---|---|---|
| [The GitLab Orbit CLI (`orbit`)](access/cli.md) | Direct CLI use, scripting, indexing tasks | One-line installer or `glab orbit local --install` |
| [The GitLab CLI (`glab`)](access/glab.md) | Anyone already using `glab` | `glab orbit local --install` |
| [MCP](access/mcp.md) | Claude Code, Codex, and other AI agents | `claude mcp add orbit-local -- orbit mcp serve` |

All three read the same local graph. GitLab Orbit Local is queried with DuckDB SQL;
the structured JSON query DSL is [GitLab Orbit Remote](../remote/_index.md) only.

## 60-second quickstart

> [!note]
> `glab orbit local` wraps the managed `orbit` binary. The binary downloads,
> is checksum-verified, and stays up to date on first use. Requires `glab`
> 1.94 or later. To run the binary directly instead, see
> [Use the `orbit` CLI directly](access/cli.md).

Index a repository and inspect what GitLab Orbit found:

```shell
glab orbit local index /path/to/your/repo
glab orbit local schema
```

That builds a local DuckDB graph at `~/.orbit/graph.duckdb` and prints every
table and column in it: `gl_definition`, `gl_file`, `gl_directory`,
`gl_imported_symbol`, `gl_edge`, and the `_orbit_manifest` bookkeeping table.

Next:

- Run a real query: [Use GitLab Orbit Local with glab](access/glab.md).
- Wire it into your AI agent: run `glab orbit setup` to install the GitLab Orbit
  skill, or [connect via MCP](access/mcp.md).
- Browse the table layout: [Schema reference](schema.md).

## Billing

GitLab Orbit Local does not consume GitLab Credits. All processing is local.

## What to try next

- [What GitLab Orbit Local indexes](indexing.md) - language and coverage scope.
- [Schema reference](schema.md) - the four node types in the local graph.
- [Cookbook](../remote/cookbook.md) - copy-paste queries (code-only ones apply to Local).
- [Get started with GitLab Orbit Remote](../remote/getting-started.md) - query your full GitLab instance.
