---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Build and query a local code graph with the GitLab Orbit CLI (orbit) binary. No GitLab account or network connection required.
title: Use GitLab Orbit Local with the GitLab Orbit CLI (`orbit`)
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

The GitLab Orbit CLI (`orbit`) builds a code graph for any local repository and queries it
against a local DuckDB file. No GitLab connection required.

## Install

Install the standalone `orbit` binary with the one-line installer:

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

This adds `orbit` to your `PATH`. Open a new terminal, then verify the install:

```shell
orbit help
```

You can also install from npm with `npm install -g @gitlab/orbit`.

If you already use the GitLab CLI (`glab`), you can instead install a managed
binary with `glab orbit local --install`. That binary is invoked as
`glab orbit local <command>` rather than `orbit` directly - see
[Use GitLab Orbit Local with glab](glab.md).

### Build from source

To contribute to GitLab Orbit or run an unreleased build, compile the binary
yourself.

Prerequisites:

- [Rust toolchain](https://rustup.rs/) (stable)
- [`mise`](https://mise.jdx.dev/) for tool management

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

The compiled binary is at `target/release/orbit`. Add it to your `PATH` or
invoke it directly.

## Index a repository

```shell
orbit index /path/to/your/repo
```

GitLab Orbit parses the repository and writes a DuckDB graph to `~/.orbit/graph.duckdb`.
You can index multiple repositories. Each is scoped by project ID and branch
in the manifest table.

| Flag | Purpose |
|------|---------|
| `--threads` | Worker thread count. `0` (default) auto-detects from CPU cores. |
| `--stats` | Include detailed statistics in the JSON output. |
| `--verbose` | Verbose logging to stderr. |
| `--db` | Override the DuckDB file path (default: `~/.orbit/graph.duckdb`). |

## Inspect the schema

`orbit schema` lists every table and column in the local DuckDB graph:

```shell
orbit schema
```

Pass table names as positional arguments to scope the output:

```shell
orbit schema gl_definition              # scoped to one table
orbit schema gl_definition gl_edge      # scoped to two tables
```

| Flag | Purpose |
|------|---------|
| `--raw` | Emit JSON instead of the default table view. |
| `--db` | Override the DuckDB path. Defaults to `~/.orbit/graph.duckdb`. |

## Run SQL against the local graph

```shell
orbit sql 'SELECT count(*) FROM gl_definition'
orbit sql -F json 'SELECT name FROM gl_definition LIMIT 3'
echo 'SELECT 1+1' | orbit sql -
orbit sql --file query.sql
```

| Flag | Purpose |
|------|---------|
| `-F`, `--format` | `table` (default), `json`, `ndjson`, or `csv`. |
| `-f`, `--file` | Read the SQL from a file. |
| `--db` | Override the DuckDB path. Defaults to `~/.orbit/graph.duckdb`. |

## List indexed repositories

The graph can hold more than one repository. To see what it contains, run:

```shell
orbit list
orbit list -F json
```

Each row reports the repository path, branch, commit, indexing status, when it
was last indexed, and an error message when the status is `error`:

```plaintext
+------------------------+--------+------------+---------+---------------------+---------------+
| repo_path              | branch | commit_sha | status  | last_indexed_at     | error_message |
+------------------------+--------+------------+---------+---------------------+---------------+
| /home/dev/workspace/kg | main   | 9606ae8... | indexed | 2026-05-18 10:14:02 |               |
| /tmp/cli-test          | main   | 654f3a6... | indexed | 2026-05-18 10:13:55 |               |
+------------------------+--------+------------+---------+---------------------+---------------+
```

A repository whose indexing fails is recorded with `status = error` and a
reason in `error_message`, so a failed or unindexable repository stays visible
here instead of silently disappearing.

| Flag | Purpose |
|------|---------|
| `-F`, `--format` | `table` (default), `json`, `ndjson`, or `csv`. |
| `--db` | Override the DuckDB path. Defaults to `~/.orbit/graph.duckdb`. |

If nothing has been indexed yet, `orbit list` exits `0`. The table view
prints nothing; structured formats emit valid empty output (`[]` for `json`,
no records for `ndjson`) so pipelines like `orbit list -F json | jq` keep
working.

## Run as an MCP server

Expose the local graph to any MCP-compatible AI agent over stdio:

```shell
orbit mcp serve
```

It serves `run_sql`, `get_graph_schema`, and `index` against
`~/.orbit/graph.duckdb`. See [Connect via MCP](mcp.md) for per-client config.

## Set up your AI assistant

`orbit setup` configures an AI coding assistant to consult the graph before it
reaches for grep. Name the assistants you want to configure:

```shell
orbit setup claude
```

Supported assistants are `claude`, `codex`, `opencode`, and `pi`. By default the
guidance points at the remote GitLab Orbit graph. To point it at your local
graph instead, pass `--local`:

```shell
orbit setup claude --local
```

### What it changes

This command modifies files that belong to you. It never runs on its own, only
when you invoke it.

For every assistant you name, `orbit setup`:

- Adds a block to that assistant's instruction file, such as `CLAUDE.md` or
  `AGENTS.md`. The block sits between `<!-- orbit:setup:begin -->` and
  `<!-- orbit:setup:end -->` markers, and anything outside those markers is left
  alone. Running the command again replaces the block in place instead of adding
  a second copy.
- Adds entries to that assistant's JSON configuration, where the assistant
  supports it. For Claude Code this is a `PreToolUse` hook in
  `settings.json`; for OpenCode it is a plugin file and its registration.
  Entries carry an `orbit` marker, and only marked entries are ever replaced or
  removed.

By default it writes to your user-global configuration, such as
`~/.claude/CLAUDE.md`. Pass `--project` to write into the current project
instead, or `--dir <path>` to target a specific project directory.

Before `orbit setup` modifies an existing file for the first time, it copies the
original to `<name>.orbit-backup` beside it. Restore that copy by hand if you
want the original back. Existing backups are never overwritten, so the copy
always holds the pre-`orbit` version.

Prefer `--project` with care: project instruction files are usually committed to
version control, so the change appears in `git status` and can reach your
teammates. User-global scope, the default, affects only you.

### Remove it

To undo the changes, run:

```shell
orbit setup claude --remove
```

This strips the marker-delimited block and the marked JSON entries, and leaves
the rest of each file untouched. If a file contained nothing but `orbit`
entries, it is deleted. Omit the assistant names to remove the setup for all of
them. Backup files are not deleted.

If you would rather not have `orbit setup` touch your files, skip it and add the
same instruction block and hooks by hand.

## Storage

The graph is stored at `~/.orbit/graph.duckdb`. Multiple repositories share
the same database. Delete the file to start over.

## Configure the CLI

`orbit config` reads and writes persisted settings in `~/.orbit/settings.json`.
A saved setting applies to every later run.

```shell
orbit config list                          # all settings and their saved values
orbit config get telemetry.enabled         # one setting
orbit config set telemetry.enabled false   # save a setting
```

| Setting | Values | Default | Purpose |
|---------|--------|---------|---------|
| `telemetry.enabled` | `true`, `false` | `true` | Whether the CLI sends usage telemetry. |

## Telemetry

The CLI sends usage events to the GitLab product analytics service so the team
can see how GitLab Orbit is used. Each event records which command ran, nothing more:
no repository content, file paths, or query text is sent. Telemetry is on by
default.

Turn it off with a saved setting, or with the environment variable in CI:

```shell
orbit config set telemetry.enabled false   # persists for every run
export ORBIT_TELEMETRY_ENABLED=false        # for CI or one shell
```

The environment variable overrides the saved setting.

| Variable | Purpose |
|----------|---------|
| `ORBIT_TELEMETRY_ENABLED` | `false` disables telemetry, `true` enables it. Overrides the saved setting. |
| `ORBIT_TELEMETRY_COLLECTOR_URL` | Send events to a different collector, for testing. Defaults to the GitLab collector. |

## Billing

GitLab Orbit Local does not consume GitLab Credits. All processing is local.

## What to try next

- [Connect via MCP](mcp.md) - connect Claude Code, Codex, and other agents to
  the local graph.
- [Use GitLab Orbit Local with glab](glab.md) - call the CLI through `glab orbit local`.
- [Schema reference](../../remote/schema.md) - available node types and properties.
- [Cookbook](../../remote/cookbook.md) - copy-paste queries for common use cases.
