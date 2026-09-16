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
against a local DuckDB file. Local source commands need no GitLab connection.
The same binary can read remote entity context from typed Ontology node references.

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
binary with `glab orbit --install`. That binary is invoked as
`glab orbit <command>` rather than `orbit` directly - see
[Use GitLab Orbit Local with glab](glab.md). `glab orbit` forwards every
command to the binary unchanged, so `glab orbit grep` runs `orbit grep`.

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

GitLab Orbit Local indexes the current working tree, including uncommitted source
files not excluded by `.gitignore`. It does not enumerate or check out other Git
branches.

The graph is stored in `~/.orbit/graph.duckdb` by default. Multiple checkout paths
can share one database, with each canonical checkout path determining its project
ID. Switching branches alone does not update the stored graph. Reindexing the same
checkout replaces its previous graph in that database with the current working-tree
contents. To retain graphs for multiple branches, index them from separate checkout
or worktree paths.

| Flag | Purpose |
|------|---------|
| `--threads` | Worker thread count. `0` (default) auto-detects from CPU cores. |
| `--stats` | Include detailed statistics in the JSON output. |
| `--verbose` | Verbose logging to stderr. |
| `--db` | Override the DuckDB file path (default: `~/.orbit/graph.duckdb`). |

## Read source or entity context

Use `context` with Definition references from `orbit grep`, or with one source
file in an indexed checkout:

```shell
orbit context Definition:481 'Definition[482]'
orbit context src/lib.rs
orbit context Definition:481 --tests
```

Definition targets include source and relationships. `--tests` includes test,
fixture, and generated connections. A file target prints its source and indexed
definitions without relationships. Use `--repo` to select a checkout, or `--db`
to override its local database.

Other Ontology node references select the remote context API:

```shell
orbit context MergeRequest:123 'Issue[999]'
glab orbit context 'MergeRequest[123]' Issue:999 --response-format json
```

Both `Type:ID` and `Type[ID]` work. Quote bracket references to prevent shell
expansion. Remote IDs are database IDs, not project-scoped IIDs such as `!123`
or `#999`. The CLI accepts `Issue` as shorthand for `WorkItem`: both Issue
spellings send `WorkItem[ID]` with the same database ID, without another API call.
Other nodes such as Project, User, and typed File references also route remotely.
Unknown node names and relationship names are rejected. Server support and
access for each node remain authoritative.

Remote context needs authentication and an instance that supports
`GET /api/v4/orbit/context`, but remote-only calls need no checkout or local database. `glab orbit`
forwards its credentials. Standalone `orbit` uses the existing credential
environment, `GITLAB_TOKEN`, or the glab credential helper.

The CLI sends one GET with repeated URL-encoded `refs[]` parameters. Remote
`--response-format` accepts `llm` (default) or `json`. JSON contains `version`
and `entities`, with each entity's `ref`, `type`, `id`, `found`, and `summary`
or `error: not_found`. Text starts with `orbit_context version=1.0.0 entities=...`.
The CLI prints bytes unchanged, including per-entity misses. It does not follow
links or redact fields locally. The separate `query` command keeps `raw|llm`.

Local and remote targets can be mixed:

```shell
orbit context Definition:481 'Issue[999]' Project:42 --repo . --tests
orbit context src/lib.rs 'File[8]' User:7 --repo . --response-format json
```

The local subset must contain Definition references OR one file, not both.
`--repo` and `--db` apply only to that subset; `--tests` remains definition-only.
Remote-only calls reject these unused local options. `--response-format` controls
only the remote portion; local-only calls retain their existing text output and
reject that flag. Local text is emitted first, then `--- Remote context ---` on
a separate line, then the unchanged remote bytes. Mixed stdout is composite
text even with remote JSON; only remote-only JSON is a whole machine-readable
JSON payload. Local ordering is unchanged, and remote refs retain their input
order in the batch.

Bare typed references take priority over same-named files, even with `--repo`.
Use `'./Issue[999]'` or an absolute path to select local file context. Local paths
resolve from the Git root, even when `--repo` points to a subdirectory.
Malformed references, unknown types, and invalid batches fail before telemetry,
credentials, requests, or local storage are opened. Either resolver failing exits
nonzero, without retrying on another backend. Local resolution runs first: a
local failure prevents the remote call; a remote failure can leave local text
and the separator on stdout. A context HTTP 404 can mean the endpoint is
unavailable; it does not prove that a feature flag is disabled.

Bare Definition references remain local, with no remote fallback on a miss.
Scoped remote Definition resolution requires a separate API contract and is not
implemented by this command.

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

Supported assistants are `claude`, `codex`, `opencode`, and `pi`. The guidance
tells the assistant to run `orbit grep` and `orbit context` before it greps or
reads raw source.

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
- [Use GitLab Orbit Local with glab](glab.md) - call the CLI through `glab orbit`.
- [Schema reference](../../remote/schema.md) - available node types and properties.
- [Cookbook](../../remote/cookbook.md) - copy-paste queries for common use cases.
