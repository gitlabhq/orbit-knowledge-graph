---
name: orbit-local
description: >
  Index and query a LOCAL checkout of a repository offline with the Orbit local
  CLI (the `orbit` binary, run directly or via `glab orbit local`). It builds a
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
version: 0.5.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, orbit-local, knowledge-graph, code-graph, duckdb, sql, repo-map
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
you reach it through glab, prefix with `glab orbit local` and add `--yes` to
skip the download/run prompts in non-interactive shells:

```bash
orbit index .                     # bundled binary
glab orbit local --yes index .    # same, via the glab wrapper
```

`glab orbit local --install --yes` installs/updates the managed binary. Full
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
| `orbit grep [QUERY…] [--path P] [--kind K,K] [--body]` | Find definitions by name, or list definitions under a path; `--body` also prints the top three bodies |
| `orbit grep FQN --related-to [--edge K] [--in] [--out]` | List connections, including uses through members |
| `orbit grep FQN --callers` / `--callees` | List incoming or outgoing calls |
| `orbit context [FQN…] [--file P] [--kind K,K] [--outline]` | Read source bodies by FQN, unique tail, glob, or file; `--outline` prints signatures and members only |
| `orbit sql [QUERY] [-f FILE] [-F table\|json\|ndjson\|csv] [--all] [--repo P]` | Run read-only SQL scoped to the current checkout's commit; `-` reads from stdin, `--all` spans every indexed commit |
| `orbit schema [TABLE…] [--raw]` | Describe graph tables/columns (index-storage tables hidden); scope to table names to trim output |
| `orbit list [-F …]` | List indexed repositories, branch, commit, status |
| `orbit mcp serve` | Serve the local graph to MCP agents (`run_sql`, `get_graph_schema`, `index`) |
| `orbit repo-map <SUBCOMMAND> [--repo P] [--ext E]` | High-level, LLM-oriented repo map (`overview`, `tree`, `api`, `class`, `extends`, `imports`) |
| `orbit skill [PATH]` | Print the bundled, version-matched skill content; no arg prints `SKILL.md`, else a relative path like `references/sql.md` |

## Definitions and relationships

```bash
orbit grep "rateLimit" --path src --kind Method,Function
orbit context "Type::method"
orbit context --file src/lib.rs
orbit context "Type" --outline
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

`grep --body` prints the bodies of the top matches (at most three) in the
same call, for the common case where the first hit is the one you want.

`context` accepts several names or globs in one call. `--file` takes a
repo-relative or absolute path inside the checkout. `--file` alone reads
all definitions and the lines between them. With names, it restricts lookup
to that file and accepts bare names; `--kind` narrows the selection.
`--outline` replaces bodies with each definition's signature and its nested
members, so a large type or file can be mapped before reading one method.

`--kind` is one comma-separated list (`Class,Method`); a quoted pipe list
(`"Class|Method"`) also works. It is not repeatable.

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
orbit repo-map api crates/orbit-local   # types + callables + signatures
```

It is scoped to the current commit; index first if the commit is not indexed.
Full workflow and subcommands: [`references/repo_map.md`](references/repo_map.md).

## References

| Topic | Location |
|---|---|
| CLI wrapper flags, config keys, pass-through args | [`references/cli.md`](references/cli.md) |
| DuckDB tables and paste-ready SQL recipes | [`references/sql.md`](references/sql.md) |
| Repository-map command (`orbit repo-map`) | [`references/repo_map.md`](references/repo_map.md) |
