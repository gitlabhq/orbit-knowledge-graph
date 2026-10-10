---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Install the GitLab Orbit CLI, then index, search, and query code and GitLab data from the terminal.
title: GitLab Orbit CLI
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Beta

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- Commands that call the GitLab server [introduced](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) in GitLab 18.10 [with a feature flag](https://docs.gitlab.com/administration/feature_flags/) named `knowledge_graph`. Disabled by default. Changed to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.

{{< /history >}}

> [!flag]
> The availability of this feature is controlled by a feature flag.
> For more information, see the history.
> This feature is available for testing, but not ready for production use.

The GitLab Orbit CLI (`orbit`) indexes your code into a graph on your machine.
It also sends queries to the GitLab Orbit graph on your GitLab instance.
One binary does both.

## Install the CLI

Prerequisites:

- [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) 1.117 or later.

```shell
glab orbit --install
glab orbit version
```

`glab` downloads the binary, verifies its checksum, and prints the version.
To update the binary, run `glab orbit --update`.

`glab orbit` and `orbit` run the same binary with the same commands and flags.
`glab` also gives your `glab auth login` credential to the binary.
To skip the confirmation prompts of `glab`, put `--yes` before the command, for example `glab orbit --yes status`.
To skip them always, set the `glab` options `orbit_local_auto_download` and `orbit_local_auto_run`.

### Other install methods

To install the standalone `orbit` binary without `glab`, use one of these methods.
Then open a new terminal and run `orbit help`.

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

{{< tab title="Source" >}}

You need the stable [Rust toolchain](https://rustup.rs/) and [`mise`](https://mise.jdx.dev/):

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

The binary is at `target/release/orbit`.

{{< /tab >}}

{{< /tabs >}}

## Command catalog

Local commands use the graph file on your machine. They need no GitLab account and no network.
Server commands use the graph on your GitLab instance.
They need a GitLab credential and GitLab Orbit [turned on for your group](_index.md#turn-on-gitlab-orbit-for-your-group).

| Command | Purpose | Graph |
|---------|---------|-------|
| `index` | Index a repository, or a directory of repositories. | Local |
| `grep` | Search definition names, paths, and bodies. | Local |
| `context` | Show source, callers, and callees. | Local |
| `repo-map` | Print a compact repository map. | Local |
| `sql` | Run read-only SQL. | Local |
| `schema` | List the tables and columns. | Local |
| `list` | List the indexed repositories. | Local |
| `mcp` | Serve the local graph to AI agents over MCP. | Local |
| `query` | Run a query and stream the response. | Server |
| `status` | Show the health of the service. | Server |
| `ontology` | Show the node types, properties, and relationships. | Server |
| `dsl` | Show the JSON Schema of the query DSL. | Server |
| `tools` | Show the MCP tool manifest. | Server |
| `graph-status` | Show the indexing progress of a group or project. | Server |
| `skills` | List and print the [agent skill](agents/_index.md#gitlab-orbit-skill) files. | Server, or a built-in copy offline |
| `setup`, `uninstall` | Add or remove the [AI agent](agents/_index.md) configuration. | None |
| `config` | Read and write saved settings. | None |
| `version` | Print the version. | None |

For all flags, run `orbit <command> --help`. Local commands, except `mcp serve`, accept `--db <path>` for a different graph file.

## Index a repository

```shell
orbit index .
```

```plaintext
"graph": { "directories": 2, "files": 2, "definitions": 4, "imported_symbols": 1, "relationships": 14 },
"database_path": "/Users/you/.gitlab/orbit/graph.duckdb"
```

The CLI indexes the working tree, including uncommitted files, and skips files that `.gitignore` excludes.
It does not read other branches. A directory of repositories gives one graph for each repository.
When you index a checkout again, the new graph replaces the old graph.
For more information, see [how the local graph works](how-it-works.md).

## Search and read code

```shell
orbit grep AuthService
```

```plaintext
  Definition:4350498579292091568  src.auth.AuthService  [Class]  src/auth.py:1-6  exact-name
  Definition:4121712847655640147  src.auth.AuthService.login  [Method]  src/auth.py:2-3  name/path
  Definition:4748301037886056353  src.app.main  [Function]  src/app.py:3-5  body-only ×1
```

`orbit grep` is a full-text search, not a regular expression. Exact names come first.
Quote alternatives with `|`, for example `orbit grep 'login|check'`. Narrow with `--path`, `--kind`, and `--limit`.

`orbit context` takes `Definition:<id>` and `File:<id>` values, file paths, `path:start-end` ranges, and directories.
Both commands use the current checkout. If its commit is not in the graph, they index it first.

`orbit repo-map` prints an `overview` by default. The subcommands `tree`, `api`, `class`, `extends`, and `imports` show more detail.
It does not index for you. If the commit is not indexed, it exits with code `1`.

## Run SQL on the local graph

```shell
orbit sql -F json "SELECT count(*) AS n FROM gl_definition"
```

```plaintext
[{"n":4}]
```

Inside a checkout, the tables contain only its indexed commit, so you need no `project_id` or `commit_sha` filters.
Outside a checkout, the query reads every commit and prints a note on stderr.
Use `--repo <path>` for a different checkout, or `--all` for every repository.
`-F` accepts `table` (default), `json`, `ndjson`, and `csv`. `-f <file>` or `-` reads the SQL from a file or stdin.

`orbit schema gl_definition` shows one table. `orbit schema` hides the search-index tables until you name one.
For the table reference, see [what GitLab Orbit indexes](schema.md).

In `orbit list`, a repository that failed to index has the status `error` and a reason in `error_message`.
With nothing indexed, `orbit list -F json` prints `[]` and exits with code `0`.

## Query the server graph

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed
- Status: Beta

{{< /details >}}

Without `glab`, the binary reads `GITLAB_TOKEN`, or else uses `glab auth credential-helper`.
For GitLab Self-Managed, sign in with `glab auth login --hostname <host>` and run `glab orbit` in a clone from that host.
Without `glab`, set `GITLAB_URL=https://<host>` with `GITLAB_TOKEN`.

To see all properties of some nodes, run `glab orbit ontology MergeRequest Project`.

To run a query, put the request body in `query.json`. This query returns five projects in `your-group`:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "filters": {
        "full_path": {"starts_with": "your-group/"}
      }
    }],
    "limit": 5
  }
}
```

```shell
glab orbit query --file query.json
```

`--file -` reads stdin. `--response-format llm` gives compact text for agents, and `raw` gives JSON.
`gql` gives a graph pattern table, if your GitLab instance supports it.
The flag overrides `response_format` in the body. The default is `llm`.
For the syntax, see [query the graph](queries/_index.md).

For indexing progress, run `graph-status`. `status` does not show it.

```shell
glab orbit graph-status --full-path your-group/your-project
```

You can also use `--namespace-id` or `--project-id`. The output is JSON. Use `--response-format llm` for compact text.

## Settings, storage, and telemetry

The CLI keeps the graph and settings in `~/.gitlab/orbit/`. For more information, see [storage](how-it-works.md#storage).

The CLI sends usage events to GitLab product analytics.
An event has the command or MCP tool, the result, the exit code, the duration, the CLI version, and the AI agent.
`setup` and `uninstall` also send the agents and components they changed.
The CLI does not send repository content, file paths, or query text.
Telemetry is on by default. To turn it off, run `orbit config set telemetry.enabled false`. It is the only setting.

| Variable | Purpose |
|----------|---------|
| `GITLAB_TOKEN`, `GITLAB_URL` | Token and instance for server commands without `glab`. Default URL: `https://gitlab.com`. |
| `ORBIT_TELEMETRY_ENABLED` | `false` or `true`. Overrides the saved setting, for example in CI. |
| `ORBIT_TELEMETRY_COLLECTOR_URL` | A different event collector, for testing. |
| `ORBIT_GRAPH_FIRST` | `1` or `0`. Overrides the [`--graph-first`](agents/_index.md#make-claude-code-use-the-graph-first) setup option. |

## Exit codes

| Exit code | Meaning |
|-----------|---------|
| `0` | Success. |
| `1` | Other error, for example a missing credential. See stderr. |
| `2` | A command-line usage error, or HTTP `404`: the path does not exist, or the endpoint is not available. |
| `3` | HTTP `401`. The token is missing or expired. |
| `4` | HTTP `403`. No group with GitLab Orbit turned on is available to you. |
| `5` | HTTP `429`. Rate limited. Wait for `Retry-After`. |
| `130` | You canceled a prompt. |

## Billing

Local commands and, during the beta, `query` do not consume GitLab Credits. For billing after general availability, see [billing](how-it-works.md#billing).

## Related topics

- [`glab orbit` reference](https://docs.gitlab.com/cli/orbit/)
- [Connect AI agents](agents/_index.md)
- [MCP servers](agents/mcp.md)
- [Query the graph](queries/_index.md)
- [What GitLab Orbit indexes](schema.md)
- [How GitLab Orbit works](how-it-works.md)
- [Troubleshooting](troubleshooting.md)
