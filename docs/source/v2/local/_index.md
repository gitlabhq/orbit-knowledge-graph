---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: The GitLab Orbit local graph holds the code on your machine. Your agent can search code that you have not pushed, with no GitLab account.
title: GitLab Orbit local graph
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated

{{< /details >}}

{{< history >}}

- [Introduced](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) in GitLab 19.0 as an [experiment](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Changed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) to [beta](https://docs.gitlab.com/policy/development_stages_support/#beta) in GitLab 19.1.
- Generally available in GitLab 19.5.

{{< /history >}}

The GitLab Orbit local graph holds the code on your machine, so your agent can find definitions,
callers, and imports in code that you committed but have not pushed. It works offline and needs no GitLab account.
For merge requests, pipelines, work items, and vulnerabilities, your agent queries the remote graph on GitLab.

If you installed the standalone binary, run `orbit` in place of `glab orbit`.

## Compare the local and remote graphs

| Attribute | Local graph | Remote graph |
|---|-------------|--------------|
| Data | Code only | GitLab data and code |
| Code version | The checkout on your machine, including commits that you have not pushed | The default branch of each project |
| Scope | Each repository that you index | Each top-level group where GitLab Orbit is turned on |
| Refresh | You run `glab orbit index` again | Automatic |
| Requires | The [GitLab Orbit CLI](../cli.md) | The GitLab Orbit add-on, and the [roles](../security.md#roles) to read the data |
| Query with | `grep`, `context`, `repo-map`, and `sql` | `query`, the MCP endpoint, the REST API, and GitLab Duo Agent Platform |

Your agent can use both graphs in the same conversation.
[`glab orbit setup`](../agents/_index.md#set-up-your-agents) configures your agents for the local graph.

## Index a repository

Prerequisites:

- The [GitLab Orbit CLI](../cli.md#install-the-gitlab-orbit-cli) is installed.

`glab orbit setup` indexes the current repository for you. To index a repository yourself, run this command in it:

```shell
glab orbit index .
```

The CLI parses the files in the working tree, skips the files that `.gitignore` excludes,
and writes definitions, imports, and cross-file references to the graph. A medium-sized repository indexes in seconds.
The parser and [supported languages](../schema.md#supported-languages) are the same as for the remote graph.

- To index each repository in a directory, give the path of the directory.
- Each indexed commit keeps its own graph. Indexing the same commit again replaces its graph. After you switch branches, `grep` and `context` index the new commit
  for you. Run `glab orbit index .` before you use `repo-map` or `sql` on it.
- In a terminal, `index` shows its progress. In a pipe or a file, it prints JSON statistics.

## Search and read code

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

- By default, and with `--all`, the query reads every indexed commit of every repository, including earlier commits of the current checkout.
- To query only the indexed commit of a checkout, use `--repo <path>`, for example `--repo .`.
- `-F` accepts `table` (default), `json`, `ndjson`, and `csv`. `-f <file>` or `-` reads the SQL from a file or stdin.

To join across repositories, run the query from any directory with `--all`, and join the node tables to
`_orbit_manifest` on `project_id`. This query lists the symbols that one indexed repository imports from another:

```shell
glab orbit sql --all "SELECT DISTINCT im.repo_path AS importing_repo, i.identifier_name AS symbol,
  dm.repo_path AS defining_repo, d.file_path AS defining_file, d.start_line AS defining_line
  FROM gl_imported_symbol i
  JOIN _orbit_manifest im ON im.project_id = i.project_id
  JOIN gl_definition d ON d.name = i.identifier_name
  JOIN _orbit_manifest dm ON dm.project_id = d.project_id
  WHERE dm.repo_path <> im.repo_path
  ORDER BY importing_repo, symbol"
```

The join matches on the symbol name. When several indexed repositories define the same name, narrow the query
on `im.repo_path` or `i.import_path`.

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
| `_orbit_manifest` | One row for each indexed commit of each repository, with its path (`repo_path`), branch, commit, and status |

The local graph uses these relationships: `CONTAINS`, `DEFINES`, `IMPORTS`, `CALLS`, and `EXTENDS`.
It has no `Branch`, `Project`, or other software development lifecycle (SDLC) nodes.

The local tables differ from the remote graph in these ways:

- `project_id` comes from the absolute path of the checkout. It is not a GitLab project ID.
- A `Definition` ID comes from the file path. The same function in two indexed repositories has two different IDs.
- `File.reason` tells why the indexer skipped a file or could not parse it, such as
  `skip_excluded_extension` or `fault_invalid_utf8`. It is empty for files that parsed correctly.
- `Definition.definition_type` and `ImportedSymbol.import_type` values are capitalized and
  specific to the language, such as `Class`, `Method`, `NamedImport`, or `CjsRequire`.
- `ImportedSymbol.identifier_name` is empty for wildcard and side-effect imports.

## Storage and privacy

The CLI keeps the local graph in `~/.gitlab/orbit/local/`: one DuckDB file for each indexed commit under `revisions/`, and a `manifest.duckdb` file that lists them.

- To keep the local graph in a different directory, set `ORBIT_DATA_DIR`.
- If `~/.gitlab/orbit` does not exist, the CLI moves an earlier `~/.orbit` directory to it.
- Local commands accept `--db <path>` for a different graph file.
- When an update of the CLI changes the format of the local graph, the next `index` rebuilds the graph file.
  Index your other repositories again after you update.

The local graph has no authorization layer. Anyone who can read its files can read all data in them.
Index and query commands send no code, file paths, or query text off your machine. Each command sends one
[telemetry](../cli.md#settings-and-telemetry) event unless you turn telemetry off. The CLI also uses the network to install and update the binary, and to fetch the GitLab Orbit skill.

## Related topics

- [GitLab Orbit CLI](../cli.md)
- [Connect your agent](../agents/_index.md)
- [Indexed data](../schema.md)
- [Billing and credit usage](../billing.md)
- [Troubleshooting the local graph](../troubleshooting.md#local-graph)
