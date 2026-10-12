---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Local indexes the code on your machine into a local graph. Your agent can search code that you have not pushed, with no GitLab account.
title: GitLab Orbit Local
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

<!-- TODO(GA): set the GA release and link the GA work item in the history line. -->
{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- Generally available in GitLab 19.x.

{{< /history >}}

GitLab Orbit Local indexes the code on your machine into a local graph, so your agent can find definitions,
callers, and imports in code that you committed but have not pushed. It works offline and needs no GitLab account.
For merge requests, pipelines, work items, and vulnerabilities, your agent queries the remote graph on GitLab.

This page uses `glab orbit`. If you installed the standalone binary, type `orbit` in place of `glab orbit`.

## Compare the local and remote graphs

| | Local graph | Remote graph |
|---|-------------|--------------|
| Data | Code only | GitLab data and code |
| Code version | The checkout on your machine, including commits that you have not pushed | The default branch of each project |
| Scope | Each repository that you index | Each top-level group where GitLab Orbit is turned on |
| Refresh | You run `glab orbit index` again | Automatic |
| Requires | The [GitLab Orbit CLI](../cli.md) | The GitLab Orbit add-on, and the [roles](../security.md#roles) to read the data |
| Query with | `grep`, `context`, `repo-map`, `sql`, and the local MCP server | `query`, the MCP endpoint, the REST API, and GitLab Duo Agent Platform |

Your agent can use both graphs in the same conversation.
[`glab orbit setup`](../agents/_index.md#set-up-your-agents) configures your agents for the local graph.

## Index a repository

Prerequisites:

- The [GitLab Orbit CLI](../cli.md#install-the-cli) is installed.

`glab orbit setup` indexes the current repository for you. To index a repository yourself, run this command in it:

```shell
glab orbit index .
```

The CLI parses the files in the working tree, skips the files that `.gitignore` excludes,
and writes definitions, imports, and cross-file references to the graph. A medium-sized repository indexes in seconds.
The parser and [supported languages](../schema.md#supported-languages) are the same as for the remote graph.

- To index each repository in a directory, give the path of the directory.
- Indexing a checkout again replaces its graph. After you switch branches, `grep` and `context` index the new commit
  for you. Run `glab orbit index .` before you use `repo-map` or `sql` on it.
- To keep graphs for more than one branch, index each branch from a separate checkout or worktree.
- In a terminal, `index` shows its progress. In a pipe or a file, it prints JSON statistics.

## Search and read code

`grep` searches every line of every file in the working tree, like `rg`, and adds the graph on top:

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
- Then each file is one row: the path, then each match under its enclosing definition, separated by `│`.
  A definition shows as `Kind name:start-end`, with `←` callers and `→` callees. `:3` is a line number.
- Plain words ignore case, `_`, `-`, and spaces, so `read_file` also matches `ReadFile`.
  Patterns with regular expression characters are regular expressions. For an exact string, use `-F`.
- To narrow the search, give paths, or use `-g`, `-t`, or `--kind`, such as `--kind Class,Method`.
  Common `rg` options such as `-A`, `-B`, `-C`, `-w`, `-l`, and `-c` work. Unsupported options are ignored with a warning.
- When nothing matches, `grep` prints nothing and exits with code `1`.
- A file that differs from the last commit shows `(edited since index)` and no definitions. Commit the change to index it.

To read the source of a definition, and its callers and callees, pass its name to `context`:

```shell
glab orbit context read_file
```

`context` also takes fully qualified names, `Definition:<id>` and `File:<id>` values, file paths,
`path:start-end` ranges, single lines such as `path:12`, and directories, several at a time.
If a name matches more than five definitions, `context` lists them so you can pick one.
`grep` and `context` index the current commit first if the graph does not have it.

To orient yourself in a repository, run `glab orbit repo-map`. The subcommands `tree`, `api`, `class`, `extends`,
and `imports` show more detail. `repo-map` does not index, and exits with code `1` if the commit is not indexed.

## Run SQL on the local graph

```shell
glab orbit sql -F json "SELECT count(*) AS n FROM gl_definition"
```

```plaintext
[{"n":4}]
```

The SQL is read-only and runs directly on the graph tables.

- Inside a checkout, the tables contain only its indexed commit. Outside a checkout, or if the commit is not indexed,
  the query reads every indexed commit and prints a note on stderr.
- Use `--repo <path>` for a different checkout, or `--all` for every repository.
- `-F` accepts `table` (default), `json`, `ndjson`, and `csv`. `-f <file>` or `-` reads the SQL from a file or stdin.

To list the tables and columns, run `glab orbit schema`, or `glab orbit schema gl_definition` for one table.
To list the indexed repositories, run `glab orbit list`. A repository that failed to index has the status `error`
and a reason in `error_message`.

### Local tables

Each node type has its own table. All relationships are in one edge table.

| Table | Node type or content |
|-------|----------------------|
| `gl_directory` | `Directory` |
| `gl_file` | `File` |
| `gl_definition` | `Definition` |
| `gl_imported_symbol` | `ImportedSymbol` |
| `gl_edge` | All relationships, with `source_kind`, `relationship_kind`, and `target_kind` columns |
| `_orbit_manifest` | One row for each indexed repository, with its branch, commit, and status |

The local graph uses these relationships: `CONTAINS`, `DEFINES`, `IMPORTS`, `CALLS`, and `EXTENDS`.
It has no `Branch`, `Project`, or SDLC nodes.

The local tables differ from the remote graph in these ways:

- `project_id` comes from the absolute path of the checkout. It is not a GitLab project ID.
- A `Definition` ID comes from the file path. The same function in two indexed repositories has two different IDs.
- `File.reason` tells why the indexer skipped a file or could not parse it, such as
  `skip_excluded_extension` or `fault_invalid_utf8`. It is empty for files that parsed correctly.
- `Definition.definition_type` and `ImportedSymbol.import_type` values are capitalized and
  specific to the language, such as `Class`, `Method`, `NamedImport`, or `CjsRequire`.
- `ImportedSymbol.identifier_name` is empty for wildcard and side-effect imports.

## Connect an agent over MCP

For agents that cannot run shell commands, `glab orbit mcp serve` serves the local graph over MCP.
For setup and tools, see [local MCP server](../agents/mcp.md#local-mcp-server).

## Storage and privacy

All indexed repositories share one DuckDB file at `~/.gitlab/orbit/graph.duckdb`.

- To keep the file in a different directory, set `ORBIT_DATA_DIR`.
- If `~/.gitlab/orbit` does not exist, the CLI moves an earlier `~/.orbit` directory to it.
- Local commands, except `mcp serve`, accept `--db <path>` for a different graph file.
- When an update of the CLI changes the format of the local graph, the next `index` rebuilds the graph file.
  Index your other repositories again after you update.

The local graph has no authorization layer. Anyone who can read the file can read all data in it.
Index and query commands send no request off your machine. The CLI uses the network only to install and update
the binary, fetch the GitLab Orbit skill, and send [telemetry](../cli.md#settings-and-telemetry).

## Billing

GitLab Orbit Local does not consume GitLab Credits. All processing is on your machine.

## Related topics

- [GitLab Orbit CLI](../cli.md)
- [Connect your agent](../agents/_index.md)
- [What GitLab Orbit indexes](../schema.md)
- [Troubleshooting the local graph](../troubleshooting.md#local-graph)
