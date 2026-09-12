---
name: orbit-cli
description: >
  Index and query a local checkout offline with the Orbit CLI (`orbit`, or
  `glab orbit`). Use grep for definitions and callers, context for source
  bodies, SQL for aggregation, and repo-map for orientation. Use it for the
  working tree or an unpushed branch. For indexed GitLab data use the `orbit`
  skill. For single-entity lookups or writes use `glab`.
version: 0.6.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, orbit-cli, orbit-local, knowledge-graph, code-graph, duckdb, sql, repo-map
  workflow: ai
---

# Orbit local CLI skill

Index and query a local copy of the GitLab Orbit graph. The local CLI parses a
checked-out repository into a DuckDB property graph. `grep` finds definitions
and relationships. `context` reads their source bodies. Read-only SQL handles
aggregations. Orbit Remote instead speaks the JSON DSL over gRPC. Use this
skill for the working tree and the `orbit` skill for production data.

Every command documents its flags. Run `orbit <command> --help` before you
guess a flag.

## Invocation

The binary is `orbit`. This skill writes commands as `orbit <subcommand>`.
Through glab, prefix with `glab orbit`. Add `--yes` in non-interactive shells
to skip the download and run prompts:

```bash
orbit index .                  # bundled binary
glab orbit --yes index .       # same, via the glab wrapper
```

`glab orbit --install --yes` installs or updates the managed binary. Wrapper
flags, config keys, and pass-through rules: [`references/cli.md`](references/cli.md).

## Gotchas (read first)

- `index` operates on git repositories found under `PATH`. A plain
  subdirectory that is not its own repo indexes nothing and prints no graph
  stats. Pass a repository root.
- Queries are SQL, not the DSL. `orbit sql "SELECT …"` runs against DuckDB
  tables (`gl_definition`, `gl_edge`, `gl_file`, `gl_directory`,
  `gl_imported_symbol`). The `query_type`/`nodes`/`relationships` JSON belongs
  to Orbit Remote.
- `definition_type` values are capitalized (`Function`, `Method`,
  `AssociatedFunction`, `Struct`, `Field`, `Variant`, `Module`, `Constant`, …).
  `WHERE definition_type='function'` returns zero rows. Run
  `orbit schema gl_definition` when unsure of columns.
- Relationships live in `gl_edge`, keyed by `source_id`/`target_id` with
  `relationship_kind` in `DEFINES`, `CALLS`, `IMPORTS`, `CONTAINS`, `EXTENDS`.
  Join back to `gl_definition` on `id` to resolve names.
- The graph is per-commit. The node tables carry `commit_sha`; `gl_edge` does
  not, so join back to a definition to scope edges. Re-run `index` after
  checking out a different commit.
- The default database is `~/.orbit/graph.duckdb`. Override it with `--db`.

## Definitions and relationships

```bash
orbit grep "rateLimit" --path src --kind Method,Function
orbit grep "Type::method" --callers --path src --kind Method
orbit grep "Type::method" --callees
orbit grep "Type" --related-to --edge extends --in
orbit context "Type::method"
orbit context --file src/lib.rs
orbit context "Type" --outline
```

Relationship targets accept an FQN, a unique unqualified tail, or a glob.
`--path` and `--kind` filter the connected results, not the target.

## Quick start

```bash
orbit index .                                   # index the current repo
orbit schema gl_definition gl_edge              # confirm columns before querying
orbit sql "SELECT definition_type, count(*) n FROM gl_definition GROUP BY 1 ORDER BY n DESC"
```

Paste-ready SQL for callers, definitions-in-file, subclasses, and imports:
[`references/sql.md`](references/sql.md).

## Repository map

For a hierarchical orientation pass over a local checkout, use
`orbit repo-map` instead of ad-hoc SQL:

```bash
orbit repo-map overview                 # start here
orbit repo-map tree crates              # types grouped by file under a subtree
orbit repo-map api crates/orbit-cli     # types + callables + signatures
```

It is scoped to the current commit. Index first if the commit is not indexed.
Workflow and budget: [`references/repo_map.md`](references/repo_map.md).

## References

| Topic | Location |
|---|---|
| CLI wrapper flags, config keys, pass-through args | [`references/cli.md`](references/cli.md) |
| DuckDB tables and paste-ready SQL recipes | [`references/sql.md`](references/sql.md) |
| Repository-map workflow (`orbit repo-map`) | [`references/repo_map.md`](references/repo_map.md) |
