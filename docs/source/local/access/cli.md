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

Each checkout revision has its own database under `~/.gitlab/orbit/local/revisions/`.
Reindexing the same commit replaces its graph with the current working-tree contents.
Other indexed commits remain available.

| Flag | Purpose |
|------|---------|
| `--threads` | Worker thread count. `0` (default) auto-detects from CPU cores. |
| `--stats` | Include detailed statistics in the JSON output. |
| `--verbose` | Verbose logging to stderr. |
| `--db` | Write one checkout revision to a specific DuckDB file. A file cannot hold another revision. |

## Inspect the schema

`orbit schema` lists every table and column in the local DuckDB graph:

```shell
orbit schema
```

GitLab Orbit selects the database files automatically, so you do not need to locate or attach them.

Pass table names as positional arguments to scope the output:

```shell
orbit schema gl_definition              # scoped to one table
orbit schema gl_definition gl_edge      # scoped to two tables
```

| Flag | Purpose |
|------|---------|
| `--raw` | Emit JSON instead of the default table view. |
| `--db` | Override the DuckDB file path. Omit to use the local graph managed by GitLab Orbit. |

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
| `--db` | Read a specific revision database. Defaults to all indexed revision databases. |
| `--repo` | Query only the given checkout's current revision. |
| `--all` | Query all indexed revision databases (the default). |

## List indexed repositories

The graph can hold more than one repository. To see what it contains, run:

```shell
orbit list
orbit list -F json
```

Each row reports the repository path, branch, commit, database path, indexing status,
last index time, and any error message.

A repository whose indexing fails is recorded with `status = error` and a
reason in `error_message`, so a failed or unindexable repository stays visible
here instead of silently disappearing.

| Flag | Purpose |
|------|---------|
| `-F`, `--format` | `table` (default), `json`, `ndjson`, or `csv`. |
| `--db` | Read a specific manifest database. Defaults to `~/.gitlab/orbit/local/manifest.duckdb`. |

If nothing has been indexed yet, `orbit list` exits `0`. The table view
prints nothing; structured formats emit valid empty output (`[]` for `json`,
no records for `ndjson`) so pipelines like `orbit list -F json | jq` keep
working.

## Run as an MCP server

Expose the local graph to any MCP-compatible AI agent over stdio:

```shell
orbit mcp serve
```

It serves `run_sql`, `get_graph_schema`, and `index` against the revision databases
under `~/.gitlab/orbit/local/`. See [Connect via MCP](mcp.md) for per-client config.

## Set up your AI agent

`orbit setup` configures your AI coding agents to consult the graph before
they reach for grep. It detects the agents installed on your machine:

```shell
orbit setup
```

A picker lists the detected agents, all pre-selected. Press Enter to apply,
or pass `--yes` to skip the picker. Name an agent to add it even when it is
not detected, for example `orbit setup claude codex`. Add `--mcp` to also register
the `orbit` MCP server. Inside a Git repository, setup then indexes it so
your agents have a graph to query; `--no-index` skips that. Run
`orbit setup --help` for the other options. Supported agents are GitLab Duo,
Claude Code, Codex, OpenCode, and Pi.

### What it changes

This command modifies files that belong to you. It never runs on its own, only
when you invoke it.

For every agent it configures, `orbit setup`:

- Adds a block to that agent's instruction file, such as `CLAUDE.md` or
  `AGENTS.md`. The block sits between `<!-- orbit:setup:begin -->` and
  `<!-- orbit:setup:end -->` markers, and anything outside those markers is left
  alone. Running the command again replaces the block in place instead of adding
  a second copy.
- Installs one skill named `orbit` into `.agents/skills/`. Claude Code does not
  scan that directory, so it also gets a `.claude/skills/orbit` link. By default
  this is a thin wrapper that runs `orbit skills get orbit` when the agent loads
  it, so installing needs no network. `--skill full` writes the skill files
  themselves, for agents that cannot run a command when they load a skill.
  GitLab Duo CLI is one, so setup installs `full` whenever Duo is among the
  agents, and every agent that shares `.agents/skills` gets it. An explicit
  `--skill wrapper` overrides that and prints a warning. Without
  `--skill`, a later setup keeps the kind already installed and, for `full`, fetches the instance's skill again. `full` uses the
  skill your instance serves when credentials resolve and the instance answers,
  and the skill embedded in the binary otherwise. The directory name stays
  `orbit` in both cases. Setup records which agents use the directory in a
  `.orbit-managed` file beside it, not inside it, so under `glab orbit` the
  directory matches what `glab skills install` writes. A different `orbit` skill already there, or one whose files changed since
  setup wrote them, is renamed to `orbit.orbit-backup`, with its `SKILL.md` renamed to
  `SKILL.md.orbit-backup` so it stops loading. Setup also removes the
  `orbit-cli` skill and link that earlier releases installed, including the real
  copy they wrote where symlinks fail. A wrapper that
  `glab skills` wrote for the other launcher is kept as it is when that
  launcher's program is on `PATH`, so the two tools do not swap it back and
  forth. A lone wrapper without a `.orbit-managed` file is replaced or adopted
  instead of moved aside.
- For Claude Code, adds `Bash(orbit skills get orbit)` and
  `Bash(orbit skills get orbit *)` to `permissions.allow` in `settings.json`, so
  the wrapper skill loads without a permission prompt. Under `glab orbit` the
  rules read `Bash(glab orbit skills get orbit)` and
  `Bash(glab orbit skills get orbit *)`. When setup keeps a wrapper for the
  other launcher, it adds that launcher's rules too.
- With `--mcp`, adds the `orbit` MCP server to the agent's MCP configuration.
  Existing servers and comments are preserved.
- Adds entries to that agent's JSON configuration, where the agent
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
orbit uninstall
```

It lists the detected agents that have GitLab Orbit installed in that scope.
Name agents to target only those. It removes what `orbit setup` wrote and leaves
the rest of each file untouched. Setup records the skill directory it writes in a `.orbit-managed` file beside
it, and uninstall removes only directories listed there that still hold what setup wrote. A directory holding only a wrapper `SKILL.md` that differs from this release's only in its `version:` line, as `glab skills update` can leave it, counts as unchanged, and `.DS_Store` files are ignored. Any other edit makes it a changed directory. Agents share that
directory, so uninstalling one agent keeps it, and reports which agents still
use it, until the last of them is removed. It also removes the `orbit-cli`
skill that earlier releases installed, but only the files that match this release when
that directory has no marker. A `SKILL.md` there that differs from this release, edited or from an older
one, is renamed to `SKILL.md.orbit-backup`, so the old skill stops loading. Other files you edited after setup are kept, and
so are their backups. A backup goes away once its file is back to the original.
`--yes`, `--project`, and `--dir` work as they do for `orbit setup`.

If you would rather not have `orbit setup` touch your files, skip it and add the
same instruction block, MCP entry, and hooks by hand.

## Storage

Revision databases and a separate manifest are stored under `~/.gitlab/orbit/local/`.
Graph files use `revisions/<project-id>/<commit-sha>.duckdb`.
The `manifest.duckdb` file records their paths and status.
The canonical checkout path determines the project ID.
Reindexing replaces that commit's graph and preserves other revisions.
Use `orbit list -F json` to see database paths, or set `ORBIT_DATA_DIR` to change the workspace root. `orbit config` then reads and writes `$ORBIT_DATA_DIR/settings.json` too.

Queries wait up to five seconds for busy revisions before returning an error.
Workspace queries skip missing or incomplete revisions with a diagnostic.
Explicit `--db` queries fail for those files.
Tables combine by column name across revision schemas.

If you used the earlier shared `graph.duckdb`, run `orbit index` to populate the new layout.
The old file remains on disk.

## Configure the CLI

`orbit config` reads and writes persisted settings in `~/.gitlab/orbit/settings.json`.
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
can see how GitLab Orbit is used. Each event records which command or MCP tool
ran, whether it succeeded, its exit code and duration, the CLI version, and the
coding agent that ran it. `orbit setup` and `orbit uninstall` also record the agents they
found and changed, the components, and the outcome. No repository content, file paths, or query
text is sent. Telemetry is on by default.

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
