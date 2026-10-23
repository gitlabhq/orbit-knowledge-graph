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
The CLI also indexes the code on your machine into a [local graph](local/_index.md).

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
Local commands use the [local graph](local/_index.md) on your machine. They need no GitLab account and no network.

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
To index, search, and run SQL on your machine, see [the local graph](local/_index.md).

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

## Settings and telemetry

The CLI keeps its settings and the [local graph](local/_index.md#storage-and-privacy) in `~/.gitlab/orbit/`.

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
- [Local graph](local/_index.md)
- [Query language](queries/query-language.md)
- [Billing and credit usage](billing.md)
- [Troubleshooting](troubleshooting.md)
