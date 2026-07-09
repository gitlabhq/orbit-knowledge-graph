---
name: orbit-local
description: >
  Index and query a LOCAL checkout of a repository offline with the Orbit local
  CLI (the `orbit` binary, run directly or via `glab orbit local`). It builds a
  DuckDB property graph from the working tree and you query it with read-only
  SQL. Use when the request targets the current checkout, working tree, or a
  branch that is not pushed/indexed remotely, or is explicitly offline/local:
  index this repo locally, who calls X in my checkout, list definitions in a
  file, generate a repo map of a local checkout, run SQL over the local code
  graph, or serve the local graph over MCP. For queries against already-indexed
  production data in GitLab (a project such as gitlab-org/gitlab, cross-project
  blast radius, contributor or merge-request aggregation) use the `orbit` skill;
  for single-entity GitLab lookups or write operations use `glab`.
version: 0.1.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, orbit-local, knowledge-graph, code-graph, duckdb, sql, repo-map
  workflow: ai
---

# Orbit local CLI skill

Index and query a **local** copy of the GitLab Knowledge Graph (product name
**Orbit**). The local CLI parses a checked-out repository into a DuckDB property
graph and answers questions with **read-only SQL** — a different surface from
Orbit Remote, which speaks the JSON DSL over gRPC. Use this skill for the
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
- **The graph is per-commit.** `gl_*` rows carry `commit_sha`; re-run `index`
  after checking out a different commit. Default database is
  `~/.orbit/graph.duckdb` (override with `--db`).

## Command surface

| Command | Purpose |
|---|---|
| `orbit index <PATH> [--stats] [--db P]` | Parse repos under `PATH` into DuckDB; prints graph stats as JSON |
| `orbit sql [QUERY] [-f FILE] [-F table\|json\|ndjson\|csv]` | Run read-only SQL; `-` reads from stdin |
| `orbit schema [TABLE…] [--raw]` | Describe tables/columns; scope to table names to trim output |
| `orbit list [-F …]` | List indexed repositories, branch, commit, status |
| `orbit mcp serve` | Serve the local graph to MCP agents (`run_sql`, `get_graph_schema`, `index`) |

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
key abstractions, per-file APIs) instead of ad-hoc SQL, use the bundled helper
(`scripts/repo_map.py`, path relative to this skill root). Full workflow and
subcommands: [`references/repo_map.md`](references/repo_map.md).

## References

| Topic | Location |
|---|---|
| CLI wrapper flags, config keys, pass-through args | [`references/cli.md`](references/cli.md) |
| DuckDB tables and paste-ready SQL recipes | [`references/sql.md`](references/sql.md) |
| Local repository-map helper | [`references/repo_map.md`](references/repo_map.md) |
