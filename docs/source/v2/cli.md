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

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- Commands that call the GitLab server:
  - [Introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default.
  - [Changed](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
  - [Enabled on GitLab.com, GitLab Self-Managed, and GitLab Dedicated](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/245620) in GitLab 19.3.
  - [Generally available](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/259054) in GitLab 19.5. Feature flag `knowledge_graph` removed.
- Generally available in GitLab 19.5.

{{< /history >}}

The GitLab Orbit CLI (`orbit`) queries GitLab Orbit on your GitLab instance from the terminal.
Your agent runs the same commands for you.
The CLI also indexes the code on your machine into a [local graph](#local-graph).

## Install the GitLab Orbit CLI

Prerequisites:

- [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) 1.120 or later.

To install the CLI and set up your agents, run this command in a Git repository:

```shell
glab orbit setup
```

On first use, `glab` downloads and verifies the CLI. When a new version is available, `glab` tells you.
For the setup steps, see [connect your agent](agents/_index.md).

`glab` handles these options itself:

| Option | Description |
|--------|-------------|
| `--yes` | Skip the confirmation prompts for this run. Put it before the command, for example `glab orbit --yes status`. |
| `--install` | Download the CLI, but do not run it. |
| `--update` | Update the CLI to the latest version. |

To skip the prompts always, set the `glab` options `orbit_cli_auto_download` and `orbit_cli_auto_run` to `true`
with `glab config set`. Setup sets `orbit_cli_auto_run` for you.

### Other install methods

To install the standalone `orbit` binary on your `PATH`, use one of these methods.
Then open a new terminal and run `orbit help`. The standalone binary runs the same commands as `glab orbit`.

{{< tabs >}}

{{< tab title="macOS and Linux" >}}

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

The installer uses `~/.local/bin`. On Linux, the default glibc build needs glibc 2.28 or later.
The installer selects the static musl archive on musl distributions such as Alpine.
To force musl, end the command with `bash -s -- --libc musl`. To update, end it with `bash -s -- --force`.

{{< /tab >}}

{{< tab title="Windows" >}}

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 | iex
```

The installer uses `$env:LOCALAPPDATA\Programs\orbit` and your user `PATH`. It needs no administrator rights.
If your policy blocks remote scripts, install the signed `orbit.exe` by hand:

1. Download `orbit-cli-windows-x86_64.zip` and its `.sha256` file from the [latest release](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases).
1. Compare the hash with the `.sha256` file (letter case does not matter), and remove the Mark of the Web:

   ```powershell
   (Get-FileHash .\orbit-cli-windows-x86_64.zip -Algorithm SHA256).Hash
   Unblock-File .\orbit-cli-windows-x86_64.zip
   ```

1. Extract `orbit.exe` and add its directory to your user `PATH`:

   ```powershell
   Expand-Archive -Path .\orbit-cli-windows-x86_64.zip -DestinationPath .
   New-Item -ItemType Directory -Force -Path "$env:LOCALAPPDATA\Programs\orbit"
   Move-Item .\orbit.exe "$env:LOCALAPPDATA\Programs\orbit\orbit.exe"
   [Environment]::SetEnvironmentVariable("PATH", "$env:LOCALAPPDATA\Programs\orbit;$([Environment]::GetEnvironmentVariable('PATH', 'User'))", "User")
   ```

{{< /tab >}}

{{< tab title="npm" >}}

```shell
npm install -g @gitlab/orbit
```

On Linux, the package always uses the static musl binary, which runs on all distributions.

{{< /tab >}}

{{< /tabs >}}

## Command catalog

Remote commands query the remote graph on your GitLab instance.
They need a GitLab credential and GitLab Orbit [turned on for your group](quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group).
Local commands use the [local graph](#local-graph) on your machine. They need no GitLab account and no network.

| Command | Purpose | Graph |
|---------|---------|-------|
| `query` | Run a query and stream the response. | Remote |
| `status` | Show the health of the service. | Remote |
| `ontology` | Show the node types, properties, and relationships. | Remote |
| `dsl` | Show the JSON Schema of the deprecated JSON query domain-specific language (DSL). | Remote |
| `tools` | Show the MCP tool manifest. | Remote |
| `graph-status` | Show the indexing progress of a group or project. | Remote |
| `skills` | List and print the [agent skill](agents/_index.md#gitlab-orbit-skill) files. | Remote when a GitLab credential is available, otherwise the built-in copy |
| `index` | Index a repository, or a directory of repositories. | Local |
| `grep` | Search every line of code, tests, configuration, and documentation, with the enclosing definitions. | Local |
| `context` | Show source, callers, and callees. | Local |
| `repo-map` | Print a compact repository map. | Local |
| `sql` | Run read-only SQL. | Local |
| `schema` | List the tables and columns. | Local |
| `list` | List the indexed repositories. | Local |
| `setup`, `uninstall` | Add or remove the [agent configuration](agents/_index.md). | None |
| `config` | Read and write saved settings. | None |
| `version` | Print the version. | None |

For all flags, run `glab orbit <command> --help`.
To index, search, and run SQL on your machine, see [local graph](#local-graph).

## Query the remote graph

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

Prerequisites:

- GitLab Orbit is [turned on for your group](quickstart.md#step-1-turn-on-gitlab-orbit-for-your-group).

To sign in:

1. Run `glab auth login`.
   On GitLab Self-Managed or GitLab Dedicated, select your instance from the list, or add `--hostname <gitlab.example.com>`.
1. Optional. On GitLab Self-Managed or GitLab Dedicated, to use `glab orbit` outside a clone of a project on your instance,
   run `glab config set --global host <gitlab.example.com>`.

`glab orbit` gives your sign-in to the CLI on every run.

### Run a query

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

In a clone of a project in your group, run:

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

- To index each repository in a directory, give the path of the directory.
- Each indexed commit keeps its own graph. Indexing the same commit again replaces its graph.
- After you switch branches, `grep` and `context` index the new commit for you.
  Run `glab orbit index .` before you use `repo-map` or `sql` on it.

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

- If the term names a definition, its source prints first. Then each file is one row, with each match under its enclosing definition.
  A definition shows as `Kind name:start-end`, with `←` callers and `→` callees.
- Plain words ignore case, `_`, `-`, and spaces, so `read_file` also matches `ReadFile`.
  Patterns with regular expression characters are regular expressions. For an exact string, use `-F`.
- Common `rg` options such as `-A`, `-B`, `-C`, `-w`, `-l`, `-c`, `-g`, and `-t` work. To filter by definition type, use `--kind`, such as `--kind Class,Method`.
- A file that differs from the last commit shows `(edited since index)` and no definitions. Commit the change to index it.

To read the source of a definition, and its callers and callees, pass its name to `context`:

```shell
glab orbit context read_file
```

`context` also takes fully qualified names, file paths, `path:start-end` ranges, and directories, several at a time.
If a name matches more than five definitions, `context` lists them so you can pick one.

To orient yourself in a repository, run `glab orbit repo-map`. The subcommands `tree`, `api`, `class`, `extends`,
and `imports` show more detail.

### Run SQL on the local graph

```shell
glab orbit sql -F json "SELECT count(*) AS n FROM gl_definition"
```

The SQL is read-only and runs directly on the graph tables.

- By default, the query reads every indexed commit of every repository.
  To query only the indexed commit of a checkout, use `--repo <path>`, for example `--repo .`.
- To join across repositories, join the node tables to `_orbit_manifest` on `project_id`.
- `-F` accepts `table` (default), `json`, `ndjson`, and `csv`. `-f <file>` or `-` reads the SQL from a file or stdin.

To list the tables and columns, run `glab orbit schema`. To list the indexed repositories, run `glab orbit list`.

#### Local tables

| Table | Node type or content |
|-------|----------------------|
| `gl_directory` | `Directory` |
| `gl_file` | `File` |
| `gl_definition` | `Definition` |
| `gl_imported_symbol` | `ImportedSymbol` |
| `gl_edge` | All relationships, with `source_kind`, `relationship_kind`, and `target_kind` columns |
| `_orbit_manifest` | One row for each indexed commit of each repository, with its path (`repo_path`), branch, commit, and status |

The local graph uses these relationships: `CONTAINS`, `DEFINES`, `IMPORTS`, `CALLS`, and `EXTENDS`.
It has no `Branch`, `Project`, or other software development lifecycle (SDLC) nodes.

The local tables differ from the remote graph in these ways:

- `project_id` comes from the absolute path of the checkout. It is not a GitLab project ID.
- A `Definition` ID comes from the file path. The same function in two indexed repositories has two different IDs.
- `File.reason` tells why the indexer skipped a file or could not parse it, such as `skip_excluded_extension`.
- `Definition.definition_type` and `ImportedSymbol.import_type` values are specific to the language, such as `Class`, `Method`, or `NamedImport`.

### Storage and privacy

The CLI keeps the local graph in `~/.gitlab/orbit/local/`: one DuckDB file for each indexed commit under `revisions/`,
and a `manifest.duckdb` file that lists them.

- To keep the local graph in a different directory, set `ORBIT_DATA_DIR`.
- Local commands accept `--db <path>` for a different graph file.
- When an update of the CLI changes the format of the local graph, the next `index` rebuilds the graph file.
  Index your other repositories again after you update.

The local graph has no authorization layer. Anyone who can read its files can read all data in them.
Index and query commands send no code, file paths, or query text off your machine.
Each command sends one [telemetry](#settings-and-telemetry) event unless you turn telemetry off.

## Settings and telemetry

The CLI keeps its settings and the [local graph](#storage-and-privacy) in `~/.gitlab/orbit/`.

The CLI sends usage events to GitLab product analytics, such as the command, the result, the duration, and the CLI version.
It never sends repository content, file paths, or query text.
Telemetry is on by default. To turn it off, run:

```shell
glab orbit config set telemetry.enabled false
```

| Variable | Purpose |
|----------|---------|
| `GITLAB_TOKEN`, `GITLAB_URL` | Token and instance for remote commands when you run the CLI without `glab`. Default URL: `https://gitlab.com`. Without `GITLAB_TOKEN`, the CLI gets the token and instance of your `glab` sign-in from `glab auth credential-helper`. |
| `ORBIT_TELEMETRY_ENABLED` | `false` or `true`. Overrides the saved telemetry setting, for example in CI. |

## Exit codes

| Exit code | Meaning |
|-----------|---------|
| `0` | Success. |
| `1` | Other error, for example a missing credential, or HTTP `402`, `422`, or `503`. See stderr. `grep` also exits with `1`, and prints nothing, when nothing matches. |
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
