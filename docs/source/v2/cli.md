---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install the GitLab Orbit CLI, then query your GitLab data and code from the terminal.
title: GitLab Orbit CLI
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment) in GitLab 19.0.
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) from experiment to beta in GitLab 19.1.
- Commands that call the GitLab server:
  - [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment) in GitLab 18.10.
  - [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) from experiment to beta in GitLab 19.1.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5.

{{< /history >}}

The GitLab Orbit CLI (`orbit`) queries GitLab Orbit on your GitLab instance from the terminal.
Your agent runs the same commands for you.
The CLI also indexes the code on your machine into a [local graph](#local-graph).

## Install the CLI

You should use the [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) to install the GitLab Orbit CLI and connect your agents.
`glab` downloads and verifies the GitLab Orbit CLI, and tells you when a new version is available.

To install the CLI and set up your agents, see [set up your agent](agents/_index.md#set-up-your-agent).

`glab` handles these options itself:

| Option | Description |
|--------|-------------|
| `--yes` | Skip the confirmation prompts for this run. Put it before the command, for example `glab orbit --yes status`. |
| `--install` | Download the CLI, but do not run it. |
| `--update` | Update the CLI to the latest version. |

To skip the prompts on every run, set the `glab` options `orbit_cli_auto_download` and `orbit_cli_auto_run` to `true`:

```shell
glab config set --global orbit_cli_auto_download true
glab config set --global orbit_cli_auto_run true
```

When you run setup in a repository, it sets `orbit_cli_auto_run` only for that repository.

### Install the standalone binary

If you cannot use `glab`, install the standalone `orbit` binary.
The binary runs the same commands as `glab orbit`.
In the commands on this page, replace `glab orbit` with `orbit`, for example `orbit setup`.

- On macOS and Linux:

  ```shell
  curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
  ```

  To update, run the command with `--force`:

  ```shell
  curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --force
  ```

- On Windows:

  ```powershell
  irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 | iex
  ```

- With npm:

  ```shell
  npm install -g @gitlab/orbit
  ```

- By hand: download the archive for your platform from the
  [latest release](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases), and put `orbit` on your `PATH`.

## Command catalog

Remote commands query the remote graph on your GitLab instance.
They need a GitLab credential and GitLab Orbit turned on for your group.
Local commands use the local graph on your machine. They need no GitLab account and no network.

| Command | Graph | Purpose |
|---------|-------|---------|
| `query` | Remote | Run a query and stream the response. |
| `status` | Remote | Show the health of the service. |
| `ontology` | Remote | Show the node types, properties, and relationships. |
| `tools` | Remote | Show the Model Context Protocol (MCP) tool manifest. |
| `graph-status` | Remote | Show the indexing progress of a group or project. |
| `skills` | Remote when a GitLab credential is available, otherwise the built-in copy | List and print the agent skill files. |
| `index` | Local | Index a repository, or a directory of repositories. |
| `grep` | Local | Search every line of code, tests, configuration, and documentation, with the enclosing definitions. |
| `context` | Local | Show source, callers, and callees. |
| `repo-map` | Local | Print a compact repository map. |
| `sql` | Local | Run read-only SQL. |
| `schema` | Local | List the tables and columns. |
| `list` | Local | List the indexed repositories. |
| `setup`, `uninstall` | None | Add or remove the agent configuration. |
| `config` | None | Read and write saved settings. |
| `version` | None | Print the version. |

For all flags, run `glab orbit <command> --help`.

## Query the remote graph

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

Sign in once with `glab`, then run queries and check indexing progress from the terminal.

Prerequisites:

- GitLab Orbit is [turned on for your group](quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group).
- The Reporter, Security Manager, Developer, Maintainer, or Owner role for the group.

To sign in:

1. Run `glab auth login`.
   On GitLab Self-Managed or GitLab Dedicated, select your instance from the list, or add `--hostname <gitlab.example.com>`.
1. Optional. On GitLab Self-Managed or GitLab Dedicated, to use `glab orbit` outside a clone of a project on your instance,
   run `glab config set --global host <gitlab.example.com>`.

`glab orbit` passes your `glab` credential to the CLI on every run.
The standalone `orbit` binary uses a personal access token with the `read_api` scope in `GITLAB_TOKEN` if you set it, otherwise your `glab` credential.
On GitLab Self-Managed or GitLab Dedicated, also set `GITLAB_URL` to the URL of your instance.

### Run a query

To run a query, pass the query text:

```shell
glab orbit query "<query>"
```

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `<query>` | string | Yes, if you do not use `--file` | The query text in the [GitLab Orbit query language](queries/query-language.md). |
| `--file` | string | No | A file that holds the request body, or `-` for stdin. The body is a JSON object, such as `{"query": "MATCH ...", "response_format": "raw"}`. |
| `--response-format` | string | No | `llm` (default) for [TOON](https://github.com/toon-format/spec/blob/main/SPEC.md), a compact text format with one table for each node type and a table of relationships. `raw` for JSON. `gql` for a text table. |

This query returns five projects in a group. Replace `my-group` with the full path of your group:

```shell
glab orbit query "MATCH (p:Project) WHERE p.full_path STARTS WITH 'my-group/' RETURN p.full_path LIMIT 5"
```

To list the properties of node types, run `glab orbit ontology MergeRequest Project`.

### Check indexing progress

To check the indexing progress of a project, run this command in a clone of the project:

```shell
glab orbit graph-status
```

With no option, `graph-status` shows the project of the `origin` remote of the clone.
In a terminal, it prints a summary. In a pipe or a file, it prints JSON.
`status` shows only the health of the service.

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `--full-path` | string | No | The full path of a group or project, such as `my-group`. |
| `--namespace-id` | integer | No | The ID of a group. |
| `--project-id` | integer | No | The ID of a project. |
| `--response-format` | string | No | `raw` for JSON, or `llm` for compact text. |

Use only one of `--full-path`, `--namespace-id`, and `--project-id`.

## Local graph

The local graph holds the code on your machine, so your agent can find definitions, callers, and imports
in code that you committed but have not pushed. It works offline and needs no GitLab account.
For merge requests, pipelines, work items, and vulnerabilities, use the [remote graph](#query-the-remote-graph).

| Attribute | Local graph | Remote graph |
|-----------|-------------|--------------|
| Data | Code only | GitLab data and code |
| Code version | The checkout on your machine, including commits that you have not pushed | The default branch of each project |
| Refresh | You run `glab orbit index` again | Automatic |
| Query with | `grep`, `context`, `repo-map`, and `sql` | `query`, the MCP endpoint, the REST API, and GitLab Duo Agent Platform |

Your agent can use both graphs in the same conversation.

### Index a repository

`glab orbit setup` indexes the current repository for you. To index a repository yourself, run this command in it:

```shell
glab orbit index .
```

The CLI parses the files in the working tree, skips the files that `.gitignore` excludes,
and writes definitions, imports, and cross-file references to the graph.
The parser and [supported languages](schema.md#supported-languages) are the same as for the remote graph.

- When you pass the path of a directory, the CLI indexes each repository in the directory.
- Each indexed commit keeps its own graph. When you index the same commit again, the CLI re-indexes only the files that changed since the last run, including uncommitted edits.
- After you switch branches, `grep` and `context` index the new commit for you.
  Before you use `repo-map` or `sql` on the new commit, run `glab orbit index .`.

### Search and read code

`grep` searches every line of every file in the working tree, like `rg`, and adds definitions, callers, and callees from the graph:

```shell
glab orbit grep read_file
```

The output is similar to:

```plaintext
grep "read_file" @ 779e8c44: 3 lines in 2 files
Function read_file — src/utils.py:3-4 ←run
  3|def read_file(path):
  4|    return open(path).read()

  src/utils.py │ Function read_file:3-4 ←run :3 def read_file(path):
  src/main.py │ :1 from utils import read_file │ Method run:11-13 →hello,read_file :13         read_file('x')
```

- If the term names a definition, its source prints first.
  Then each file is one row, with each match under its enclosing definition.
  A definition shows as `Kind name:start-end`, with `←` callers and `→` callees.
- Plain words ignore case, `_`, `-`, and spaces, so `read_file` also matches `ReadFile`.
  Patterns with regular expression characters are regular expressions. For an exact string, use `-F`.
- Common `rg` options such as `-A`, `-B`, `-C`, `-w`, `-l`, `-c`, `-g`, and `-t` work.
  To filter by definition type, use `--kind`, such as `--kind Class,Method`.
- A file that differs from the last commit shows `(edited since index)` and no definitions.
  Commit the change to index it.

To read the source of a definition, and its callers and callees, pass its name to `context`:

```shell
glab orbit context read_file
```

`context` also takes fully qualified names, file paths, `path:start-end` ranges, and directories, several at a time.
If a name matches more than five definitions, `context` lists them so you can pick one.

To orient yourself in a repository, run `glab orbit repo-map`. The subcommands `tree`, `api`, `class`, `extends`,
and `imports` show more detail.

### Run SQL on the local graph

To run read-only SQL directly on the graph tables:

```shell
glab orbit sql -F json "SELECT count(*) AS n FROM gl_definition"
```

- By default, the query reads every indexed commit of every repository.
  To query only the indexed commit of a checkout, use `--repo <path>`, for example `--repo .`.
- To join across repositories, join the node tables to `_orbit_manifest` on `project_id`.
- `-F` accepts `table` (default), `json`, `ndjson`, and `csv`. `-f <file>` or `-` reads the SQL from a file or stdin.

To list the tables and columns, run `glab orbit schema`. To list the indexed repositories, run `glab orbit list`.

## Settings, storage, and telemetry

The CLI keeps its settings and the local graph in `~/.gitlab/orbit/`.
The local graph is one DuckDB file for each indexed commit under `local/revisions/`,
and a `local/manifest.duckdb` file that lists them.

- Local commands accept `--db <path>` for a different graph file.
- When an update of the CLI changes the format of the local graph, the next `index` rebuilds the graph file.
  Index your other repositories again after you update.

The CLI sends usage events to GitLab product analytics, such as the command, the result, the duration, and the CLI version.
It never sends repository content, file paths, or query text.
Telemetry is on by default. To turn it off, run:

```shell
glab orbit config set telemetry.enabled false
```

The CLI reads these environment variables:

| Variable | Purpose |
|----------|---------|
| `GITLAB_TOKEN`, `GITLAB_URL` | Token and instance for remote commands when you run the CLI without `glab`. Default URL: `https://gitlab.com`. Without `GITLAB_TOKEN`, the CLI gets the token and instance of your `glab` sign-in from `glab auth credential-helper`. |
| `ORBIT_DATA_DIR` | A directory for the settings and the local graph, instead of `~/.gitlab/orbit/`. |
| `ORBIT_TELEMETRY_ENABLED` | `false` or `true`. Overrides the saved telemetry setting, for example in a CI/CD job. |

## Exit codes

| Exit code | Meaning |
|-----------|---------|
| `0` | Success. |
| `1` | Other error, for example a missing credential, or HTTP `402`, `422`, or `503`. See stderr. `grep` also exits with `1`, with no error message, when nothing matches. |
| `2` | A command-line usage error, or HTTP `404`: the path does not exist, or the endpoint is not available. |
| `3` | HTTP `401`. The token is missing or expired. |
| `4` | HTTP `403`. No group with GitLab Orbit turned on is available to you. |
| `5` | HTTP `429`. Rate limited. Wait for `Retry-After`. |
| `130` | You canceled a prompt or indexing. |

## Related topics

- [`glab orbit` reference](https://docs.gitlab.com/cli/orbit/)
- [Connect your agent](agents/_index.md)
- [Query language](queries/query-language.md)
- [Troubleshooting](troubleshooting.md)
