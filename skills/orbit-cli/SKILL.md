---
name: orbit-cli
description: >
  Index and query a LOCAL checkout of a repository offline with the Orbit CLI
  (the `orbit` binary, run directly or via `glab orbit`). It builds a
  DuckDB property graph from the working tree. Use grep to find definitions,
  context for source or relationships, and read-only SQL for aggregations.
  Use when the request targets the current checkout, working tree, or a
  branch that is not pushed/indexed remotely, or is explicitly offline/local:
  index this repo locally, who calls X in my checkout, list definitions in a
  file, generate a repo map of a local checkout, run SQL over the local code
  graph, or serve the local graph over MCP. For queries against already-indexed
  production data in GitLab (a project such as gitlab-org/gitlab, cross-project
  blast radius, contributor or merge-request aggregation) use the `orbit` skill;
  for remote Ontology node context use `orbit context` with a database ID.
  For other single-entity GitLab lookups or write operations use `glab`.
version: 0.7.0
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

- **`index` operates on git repositories found under `PATH`.** Pointing it at a
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
| `orbit grep [QUERY…] [--path P] [--kind K,K]` | Find definitions and print source for the top three, or list definitions under a path |
| `orbit context <TARGET…>` | Read local Definition references or one file; other Ontology node references route remotely, including in mixed batches |
| `orbit sql [QUERY] [-f FILE] [-F table\|json\|ndjson\|csv] [--all] [--repo P]` | Run read-only SQL scoped to the current checkout's commit; `-` reads from stdin, `--all` spans every indexed commit |
| `orbit schema [TABLE…] [--raw]` | Describe graph tables/columns (index-storage tables hidden); scope to table names to trim output |
| `orbit list [-F …]` | List indexed repositories, branch, commit, status |
| `orbit mcp serve` | Serve the local graph to MCP agents (`run_sql`, `get_graph_schema`, `index`) |
| `orbit repo-map <SUBCOMMAND> [--repo P] [--ext E]` | High-level, LLM-oriented repo map (`overview`, `tree`, `api`, `class`, `extends`, `imports`) |
| `orbit skill [PATH]` | Print the bundled, version-matched skill content; no arg prints `SKILL.md`, else a relative path like `references/sql.md` |

## Definitions and relationships

```bash
orbit grep "rateLimit" --path src --kind Method,Function
orbit context Definition:481
orbit context src/lib.rs
orbit context Definition:481 --tests
```

`grep` searches Definition names, FQNs, file paths, and bodies. It returns
`Definition:<id>` references and source for its top three matches. Pass those
exact references to `context`; it does not resolve names,
FQNs, or globs. One existing repo-relative or absolute file path prints that
file's source and definitions once. Definition targets also show relationships
with direction and edge kind; file targets do not. `--tests` includes test,
fixture, and generated connections. If raw search is
needed to locate a file, return to `context <path>` to read it. Built-in Read
and shell reads are only for non-code or unavailable Orbit source. Never
truncate Orbit output.

`Definition[<id>]` also works locally. Quote bracket references in shells.
Bare typed references take priority over same-named files, even with `--repo`.
Use `./Issue[999]` or an absolute path to select local file context.
Local file paths resolve from the Git root selected by `--repo`.

`--kind` is one comma-separated list (`Class,Method`); a quoted pipe list
(`"Class|Method"`) also works. It is not repeatable.

## Remote entity context

```shell
orbit context MergeRequest:123 'Issue[999]'
orbit context 'MergeRequest[123]' Issue:999 --response-format json
```

Ontology node references other than bare Definition refs use the remote API,
including Project, User, and typed File refs. `Issue` normalizes to `WorkItem`
with the same database ID and no extra API call. IDs are database IDs, not
project-scoped IIDs. Unknown node names and relationship names fail preflight;
server support and access remain authoritative.

Mix remote refs with local definitions OR one file. `--repo` and `--db` apply
only to the local subset, and `--tests` is definition-only. Remote-only calls
need no checkout or database and reject these unused local flags. Bare Definition
refs stay local; scoped remote Definition resolution is not implemented.

Remote `--response-format` accepts `llm` (default) or `json`. The CLI prints
server bytes unchanged, including per-entity `found:false` results, without
following links. Local-only output rejects `--response-format`. Mixed stdout is
local text, a remote separator, then remote bytes: composite text even when the
remote portion is JSON. Remote-only JSON remains a whole JSON payload. Either
resolver failing exits nonzero without fallback; a remote failure can leave local
text and the separator on stdout. The separate `query` command uses `raw|llm`.

## Quick start

```bash
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

```bash
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
