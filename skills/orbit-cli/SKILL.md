---
name: orbit-cli
description: >
  Index and query a LOCAL checkout of a repository offline with the Orbit CLI
  (the `orbit` binary, run directly or via `glab orbit`). It builds a
  DuckDB property graph from the working tree. Use grep for definitions and
  relationships, context for source bodies, and read-only SQL for aggregations.
  Use when the request targets the current checkout, working tree, or a
  branch that is not pushed/indexed remotely, or is explicitly offline/local:
  index this repo locally, who calls X in my checkout, list definitions in a
  file, generate a repo map of a local checkout, run SQL over the local code
  graph, or serve the local graph over MCP. For queries against already-indexed
  production data in GitLab (a project such as gitlab-org/gitlab, cross-project
  blast radius, contributor or merge-request aggregation) use the `orbit` skill;
  for single-entity GitLab lookups or write operations use `glab`.
version: 0.5.7
license: MIT
metadata:
  audience: developers
  keywords: orbit, orbit-cli, orbit-local, knowledge-graph, code-graph, duckdb, sql, repo-map
  workflow: ai
---

# Orbit local CLI skill

Index and query a **local** copy of the GitLab Orbit graph. The local CLI
parses a checked-out repository into a DuckDB property
graph. **`grep`** finds definitions and relationships; **`context`** reads their
source bodies. Read-only SQL handles aggregations and complex queries.
Orbit Remote instead speaks the JSON DSL over gRPC. Use this skill for the
working tree; use the `orbit` skill for production data.

## Invocation

The binary is `orbit`. This skill writes commands as `orbit <subcommand>`. When
you reach it through glab, prefix with `glab orbit` and add `--yes` to skip the
download/run prompts in non-interactive shells:

```bash
orbit index .                  # bundled binary
glab orbit --yes index .       # same, via the glab wrapper
```

`glab orbit --install --yes` installs or updates the managed binary. Full
wrapper flags, config keys, and pass-through rules:
[`references/cli.md`](references/cli.md).

## Gotchas (read first)

- **`index` operates on Git repositories found under `PATH`.** Pointing it at a
  plain subdirectory that is not its own repo indexes nothing (no graph stats are
  printed at all). Pass a repository root.
- **Queries are SQL, not the DSL.** `orbit sql "SELECT …"` runs against DuckDB
  tables (`gl_definition`, `gl_edge`, `gl_file`, `gl_directory`,
  `gl_imported_symbol`). There is no `query_type`/`nodes`/`relationships` JSON
  here — that is Orbit Remote.
- **`definition_type` values are capitalized** (`Function`, `Method`,
  `AssociatedFunction`, `Struct`, `Field`, `Variant`, `Module`, `Constant`, …).
  Filtering `WHERE definition_type='function'`
  returns zero rows; use `'Function'`. Run `orbit schema gl_definition` when
  unsure of columns.
- **Relationships live in `gl_edge`**, keyed by `source_id`/`target_id` with
  `relationship_kind` in `DEFINES`, `CALLS`, `IMPORTS`, `CONTAINS`, `EXTENDS`.
  Join back to `gl_definition` on `id` to resolve names.
- **The graph is per-commit.** The node tables (`gl_definition`, `gl_file`,
  `gl_directory`, `gl_imported_symbol`) carry `commit_sha`; `gl_edge` does
  not - join back to a definition to scope edges to a commit. Re-run `index`
  after checking out a different commit. Default database is
  `~/.orbit/graph.duckdb` (override with `--db`).

## Command surface

| Command | Purpose |
|---|---|
| `orbit index <PATH> [--stats] [--db P]` | Parse repos under `PATH` into DuckDB; prints graph stats as JSON |
| `orbit grep [QUERY…] [--path P] [--kind K,K]` | Find definitions by name; queries with three or fewer matches include source automatically |
| `orbit grep FQN --related-to [--edge K] [--in] [--out]` | List connections, including uses through members |
| `orbit grep FQN --callers` / `--callees` | List incoming or outgoing calls |
| `orbit context [FQN…] [--file P] [--kind K,K]` | Read definition bodies by FQN, unique tail, or glob; `--file` alone prints a file overview |
| `orbit sql [QUERY] [-f FILE] [-F table\|json\|ndjson\|csv] [--all] [--repo P]` | Run read-only SQL scoped to the current checkout's commit; `-` reads from stdin, `--all` spans every indexed commit |
| `orbit schema [TABLE…] [--raw]` | Describe graph tables/columns (index-storage tables hidden); scope to table names to trim output |
| `orbit list [-F …]` | List indexed repositories, branch, commit, status |
| `orbit mcp serve` | Serve the local graph to MCP agents (`run_sql`, `get_graph_schema`, `index`) |
| `orbit repo-map <SUBCOMMAND> [--repo P] [--ext E]` | High-level, LLM-oriented repo map (`overview`, `tree`, `api`, `class`, `extends`, `imports`) |
| `orbit skill [PATH]` | Print the bundled, version-matched skill content; no arg prints `SKILL.md`, else a relative path like `references/sql.md` |

## Definitions and relationships

Search one concept per `grep`, including multiword identifiers like `rate limit`.
Inspect known targets directly with `context`; `--file` alone prints imports and
definition signatures. Reuse returned source from `grep` or `context` for edits
instead of reading it again with raw file tools. Stop exploring when the edit
point is clear; follow identifiers only for remaining questions and batch
independent lookups. Use raw reads for non-code or unreliable index coverage.

Search matches names and paths, not bodies. Neither those matches nor missing
graph relationships establish field reads, writes, or dataflow; inspect source.

```shell
orbit grep "rateLimit" --path src --kind Method,Function
orbit context "Type::method"
orbit context --file src/lib.rs
orbit grep "Type::method" --callers --path src --kind Method
orbit grep "Type::method" --callees
orbit grep "Type" --related-to --edge extends --in
```

Relationship selectors accept an FQN, a unique unqualified tail, or a glob.
Pass one positional target with the flag, or a target immediately after it.
An explicit flag target takes precedence over positional terms. Use one
relationship selector per call, without `--limit`.
`--path` and `--kind` filter connected results, not the target definition.
Connections from test, fixture, and generated files are counted but hidden
unless `--tests` is passed. Incoming lookups include uses through members.

`grep` includes source automatically when a query has three or fewer matches.
Broader results include a copyable `context` command for the top candidates.

`context` accepts several names or globs in one call. `--file` takes a
repo-relative or absolute path inside the checkout. `--file` alone prints an
overview: imports, definition signatures, and nested members, so a file can be
mapped before reading one body. With names, it restricts lookup to that file
and accepts bare names; `--kind` narrows the selection.

`grep` and `context` refresh changed and new source files on demand, removing
deleted files without reparsing unchanged files. Successful refreshes update
search results and definition ranges. Failed, unsupported, or unstable refreshes
keep the previous definitions; definition reads return the full current file with
`ranges=unverified`, and file overviews report that the outline is unavailable.
Test code is included.

File refresh invalidates all relationships for that project: it is not a full
semantic rebuild. Relationship lookups refuse incomplete results; SQL, MCP, and
repo maps warn about affected projects. Re-run `index` to rebuild relationships.
Do not treat missing connections as evidence that code is unrelated.

`--kind` is one comma-separated list (`Class,Method`); a quoted pipe list
(`"Class|Method"`) also works. It is not repeatable.

## Quick start

```shell
orbit index .                                   # index the current repo
orbit schema gl_definition gl_edge              # confirm columns before querying
orbit sql "SELECT definition_type, count(*) n FROM gl_definition GROUP BY 1 ORDER BY n DESC"
```

Paste-ready SQL for callers, definitions-in-file, subclasses, and imports:
[`references/sql.md`](references/sql.md).

## Repository map

For a hierarchical orientation pass over a local checkout (languages, structure,
key abstractions, per-file APIs) instead of ad-hoc SQL, use the native
`orbit repo-map` command:

```shell
orbit repo-map overview                 # start here
orbit repo-map tree crates              # types grouped by file under a subtree
orbit repo-map api crates/orbit-cli   # types + callables + signatures
```

It is scoped to the current commit; index first if the commit is not indexed.
Full workflow and subcommands: [`references/repo_map.md`](references/repo_map.md).

## References

| Topic | Location |
|---|---|
| CLI wrapper flags, config keys, pass-through args | [`references/cli.md`](references/cli.md) |
| DuckDB tables and paste-ready SQL recipes | [`references/sql.md`](references/sql.md) |
| Repository-map command (`orbit repo-map`) | [`references/repo_map.md`](references/repo_map.md) |
