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
- Commands that call the GitLab Orbit API:
  - [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment) in GitLab 18.10.
  - [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) from experiment to beta in GitLab 19.1.
- [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5.
- GitLab Orbit queries [changed](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/265010) to accept GQL by default in GitLab 19.4.4, 19.5.2, and 19.6.

{{< /history >}}

The GitLab Orbit CLI (`orbit`) queries GitLab Orbit from the terminal.
Your agent runs the same commands for you.

The CLI reads two sources:

- GitLab Orbit on your GitLab instance holds your GitLab data and the code on the default branch of each project.
- The [local graph](#local-graph) on your machine holds the code in your checkout, including commits that you have not pushed.

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

- With npm:

  ```shell
  npm install -g @gitlab/orbit
  ```

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

- By hand: download the archive for your platform from the
  [latest release](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases), and put `orbit` on your `PATH`.

## Command catalog

Commands that use the GitLab Orbit API need a GitLab credential and GitLab Orbit turned on for your group.
Commands that use only the local graph need no GitLab account and no network.
`grep` and `context` use the local graph, and also the GitLab Orbit API when a GitLab credential is available.

| Command | Uses | Purpose |
|---------|------|---------|
| `query` | GitLab Orbit API | Run a query and stream the response. |
| `status` | GitLab Orbit API | Show the health of the service, or the indexing progress of a group or project. |
| `schema` | GitLab Orbit API | Show the node types, properties, and relationships. |
| `skills get` | GitLab Orbit API when a GitLab credential is available, otherwise the built-in copy of `orbit` | Print the agent skill files, such as `orbit` and `orbit-start`. Only `orbit` has a built-in copy. |
| `grep` | Local graph and GitLab Orbit API | Search every line of code, tests, configuration, and documentation, with the enclosing definitions, and the merge requests, issues, and comments of your GitLab group. |
| `context` | Local graph and GitLab Orbit API | Show the source of definitions with their callers and tests, and read merge requests, issues, pipelines, and jobs. |
| `workspace index` | Local graph | Index a repository, or a directory of repositories. |
| `workspace list` | Local graph | List the indexed repositories. |
| `workspace mcp serve` | Local graph | Serve the local graph to agents over the Model Context Protocol (MCP). |
| `setup`, `uninstall` | None | Add or remove the agent configuration. |
| `config` | None | Read and write saved settings. |
| `version` | None | Print the version. |

For all flags, run `glab orbit <command> --help`.

## Query GitLab Orbit

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

Sign in once with `glab`, then run queries and check indexing progress from the terminal.

Prerequisites:

- GitLab Orbit is [turned on for your group](quickstart.md#step-1-turn-on-gitlab-orbit).
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

To list the properties of node types, run `glab orbit schema MergeRequest Project`.

### Check indexing progress

To check the indexing progress of a group or project, pass its full path:

```shell
glab orbit status --full-path <my-group/my-project>
```

Without `--full-path`, `--namespace-id`, or `--project-id`, `status` shows only the health of the service.
In a terminal, it prints a summary. In a pipe or a file, it prints JSON.
When indexing is complete, **SDLC data** and **Code** show `indexed`, and the summary ends with `Ready.`

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `--full-path` | string | No | The full path of a group or project, such as `my-group`. |
| `--namespace-id` | integer | No | The ID of a group. |
| `--project-id` | integer | No | The ID of a project. |
| `--response-format` | string | No | `raw` for JSON, or `llm` for compact text. |

Use only one of `--full-path`, `--namespace-id`, and `--project-id`.

## Local graph

The local graph is part of the GitLab Orbit CLI.
The local graph covers the code on your machine, which GitLab Orbit does not have.
Your agent can find definitions, callers, and imports in code that you committed but have not pushed.
The local graph works offline and needs no GitLab account.
For merge requests, pipelines, work items, and vulnerabilities, [query GitLab Orbit](#query-gitlab-orbit).

| Attribute | Local graph | GitLab Orbit |
|-----------|-------------|--------------|
| Data | Code only | GitLab data and code |
| Code version | The checkout on your machine, including commits that you have not pushed | The default branch of each project |
| Refresh | `grep` and `context` index new commits and edited files, or you run `glab orbit workspace index` | Automatic |
| Query with | `grep` and `context` | `query`, the MCP endpoint, the REST API, and GitLab Duo Agent Platform |

Your agent can use the local graph and GitLab Orbit in the same conversation.

### Index a repository

`glab orbit setup` indexes the current repository for you. To index a repository yourself, run this command in it:

```shell
glab orbit workspace index .
```

The CLI parses the files in the working tree, skips the files that `.gitignore` excludes,
and writes definitions, imports, and cross-file references to the graph.
The parser and [supported languages](schema.md#supported-languages) are the same as for GitLab Orbit.

- When you pass the path of a directory, the CLI indexes each repository in the directory.
- Each indexed commit keeps its own graph. When you index the same commit again, the CLI reindexes only the files that changed since the last run.
- After you switch branches or commit, `grep` and `context` index the new commit for you.
- To list the indexed repositories, run `glab orbit workspace list`.

`workspace sql`, `workspace schema`, and `workspace mcp serve` index the current repository the first time you use them in it,
even when you indexed other repositories.
They don't index a new commit of a repository that is already indexed. `grep`, `context`, or `workspace index` do.

#### Index in the background

By default, `workspace index` runs in the foreground.
If another index of the same repository is running, it waits for that index first.
To index without blocking a shell command that has a short timeout, use these options:

| Option | Behavior |
|--------|----------|
| `--async` | Start the index in the background and return when it holds its locks. Prints JSON with a `status`, a `pid`, and the `log` file path. `status` is `indexing` while the index runs, `indexed` if it finished before the command returned, or `queued` if another index of the repository is already running. Cannot be combined with `--wait`, `--timeout`, `--stats`, or `--verbose`. |
| `--wait` | Wait for a running index, then index in the foreground. Waiting is the default. |
| `--timeout <seconds>` | How long to wait for a running index or open readers before the command exits with code `75`. Default: `ORBIT_WAIT_TIMEOUT_MS` if set, else 600 seconds. |

With `--async`, if open readers hold the locks for up to 30 seconds, the command exits with code `75` and prints the `log` file path to stderr.
The background index keeps waiting, so you don't need to run it again.

Commands that read the local graph also wait for a running index, up to 10 minutes, then exit with code `75`.
To change the wait, set `ORBIT_WAIT_TIMEOUT_MS`.

### Search and read code

`grep` searches every line of every file in the working tree, like `rg`, and adds the enclosing definitions from the graph:

```shell
glab orbit grep read_file
```

The output is similar to:

```plaintext
grep "read_file" @ 6e979595: 3 lines in 2 files
Function read_file — src/utils.py:1-2
  1|def read_file(path):
  2|    return open(path).read()

src/utils.py
1-» Function read_file:1-2
1:def read_file(path):

src/main.py
1-» imports read_file (utils) → src/utils.py:1
1:from utils import read_file
8-» Method run:8-10
10:        read_file("x")
```

- If the term names a definition, its source prints first.
  Then each file prints its path once, with `line:text` for each match.
  A `line-» Kind name:start-end` line comes before the matches inside each definition, and import lines show where the name comes from.
- Plain words ignore case, `_`, and `-`, so `read_file` also matches `ReadFile`. Quote `a|b` to match either word.
  Patterns with regular expression characters are regular expressions. For an exact string, use `-F`.
- Common `rg` options such as `-A`, `-B`, `-C`, `-w`, `-l`, `-c`, `-g`, and `-t` work.
  To search definitions of one kind, start the query with the kind, such as `def:read_file` or `class:App`.
- `grep` and `context` index files that you edited after the last index.
  If indexing an edited file takes longer than two seconds or fails, `grep` marks the file `(edited since index)`
  and does not show its definitions. For more information, see [troubleshooting](troubleshooting.md#files-marked-edited-since-index).
- In a clone of a GitLab project, a search without paths also searches the merge requests, issues, and comments
  of the group of the `origin` remote, and prints them after the local results.
  To search only GitLab, start the query with `mr:`, `issue:`, `commit:`, or `comment:`.

To read the source of a definition and its callers, pass its name to `context`:

```shell
glab orbit context read_file
```

`context` also takes fully qualified names, file paths, `path:start-end` ranges, and directories, several at a time.
It also reads GitLab, in the project of the `origin` remote: `mr:42`, `issue:7`, `branch:<name>`, `pipeline:123`, `job:456`, or a GitLab URL.
With no target, it reads the merge request and failed CI/CD jobs of the current branch.

## Settings, storage, and telemetry

The CLI keeps its settings and the local graph in `~/.gitlab/orbit/`.
The local graph is one DuckDB file for each indexed commit under `local/revisions/`,
and a `local/manifest.duckdb` file that lists them.

- Commands that use the local graph accept `--db <path>` for a different graph file.
- When an update of the CLI changes the format of the local graph, the next `workspace index` rebuilds the graph file.
  Index your other repositories again after you update.

The CLI sends usage events, such as the command, the result, the duration, and the CLI version, to GitLab product analytics.
It never sends repository content, file paths, or query text.
Telemetry is on by default. To turn it off, run:

```shell
glab orbit config set telemetry.enabled false
```

The CLI reads these environment variables:

| Variable | Purpose |
|----------|---------|
| `GITLAB_TOKEN`, `GITLAB_URL` | Token and instance for the GitLab Orbit API when you run the CLI without `glab`. Default URL: `https://gitlab.com`. Without `GITLAB_TOKEN`, the CLI gets the token and instance of your `glab` sign-in from `glab auth credential-helper`. |
| `ORBIT_DATA_DIR` | A directory for the settings and the local graph, instead of `~/.gitlab/orbit/`. |
| `ORBIT_WAIT_TIMEOUT_MS` | How long, in milliseconds, a command waits for a running index before it exits with code `75`. Default: `600000` (10 minutes). |
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
| `75` | A running index did not finish in the wait time. Try again later, or raise the wait. |
| `130` | You canceled a prompt or indexing. |

## Related topics

- [`glab orbit` reference](https://docs.gitlab.com/cli/orbit/)
- [Connect your agent](agents/_index.md)
- [Query language](queries/query-language.md)
- [Troubleshooting](troubleshooting.md)
