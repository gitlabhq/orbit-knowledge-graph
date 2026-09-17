---
name: orbit-cli
description: >
  Index and query a local checkout offline with the Orbit CLI (`orbit`, or
  `glab orbit`). Use grep to find definitions, context for source and
  relationships, SQL for aggregation, and repo-map for orientation. Use it
  for the working tree or an unpushed branch. For indexed GitLab data use the
  `orbit` skill. For single-entity lookups or writes use `glab`.
version: 0.7.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, orbit-cli, orbit-local, knowledge-graph, code-graph, duckdb, sql, repo-map
  workflow: ai
---

# Orbit local CLI skill

The local CLI parses a checked-out repository into a DuckDB property graph.
`grep` finds definitions. `context` reads their source and relationships.
Read-only SQL handles aggregations. `repo-map` gives a directory-level
orientation. Orbit Remote instead speaks the JSON DSL over gRPC. Use the
`orbit` skill for production data.

`orbit --help` lists the commands. `orbit <command> --help` documents every
flag. Read it before you guess a flag.

## Invocation

The binary is `orbit`. This skill writes commands as `orbit <subcommand>`.
Through glab, prefix with `glab orbit`. Add `--yes` in non-interactive shells
to skip the download and run prompts:

```shell
orbit index .                  # bundled binary
glab orbit --yes index .       # same, via the glab wrapper
```

Wrapper flags, config keys, and pass-through rules: [`references/cli.md`](references/cli.md).

## Find, then read

```shell
orbit grep "rate limit" --path src --kind Method,Function
orbit context Definition:<id> [Definition:<id>...]
orbit context src/lib.rs
```

`grep` returns `Definition:<id>` references. Pass them to `context`, which
does not resolve names, FQNs, or globs. Definition targets list connections
with edge kind: `<--` is incoming (callers), `-->` is outgoing (callees).
Connections from test, fixture, and generated files are counted but hidden.
The output names `--tests` when it hides some. Reuse returned source instead
of a raw file read, and never truncate Orbit output.

## Gotchas

- Queries are SQL, not the DSL. `orbit sql "SELECT …"` runs against
  `gl_definition`, `gl_edge`, `gl_file`, `gl_directory`, and
  `gl_imported_symbol`. The `query_type`/`nodes`/`relationships` JSON belongs
  to Orbit Remote.
- `definition_type` values are capitalized (`Function`, `Method`,
  `AssociatedFunction`, `Struct`, `Field`, `Variant`, `Module`, `Constant`, …).
  `WHERE definition_type='function'` returns zero rows. Run
  `orbit schema gl_definition` when unsure of columns.
- The graph is per-commit and holds every indexed checkout. `orbit sql`
  scopes to the current one. `--all` spans them all. Re-run `index` after
  checking out a different commit.

## Quick start

```shell
orbit index .                                   # index the current repo
orbit schema gl_definition gl_edge              # confirm columns before querying
orbit sql "SELECT definition_type, count(*) n FROM gl_definition GROUP BY 1 ORDER BY n DESC"
```

Paste-ready SQL for callers, definitions-in-file, subclasses, and imports:
[`references/sql.md`](references/sql.md).

## Repository map

For a hierarchical orientation pass over a local checkout, use
`orbit repo-map` instead of ad-hoc SQL:

```shell
orbit repo-map overview                 # start here
orbit repo-map tree crates              # types grouped by file under a subtree
orbit repo-map api crates/orbit-cli     # types + callables + signatures
```

Workflow and budget: [`references/repo_map.md`](references/repo_map.md).

## References

| Topic | Location |
|---|---|
| CLI wrapper flags, config keys, pass-through args | [`references/cli.md`](references/cli.md) |
| DuckDB tables and paste-ready SQL recipes | [`references/sql.md`](references/sql.md) |
| Repository-map workflow (`orbit repo-map`) | [`references/repo_map.md`](references/repo_map.md) |
