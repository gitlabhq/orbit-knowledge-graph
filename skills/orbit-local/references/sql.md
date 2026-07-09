# Orbit local SQL reference

The local graph is a DuckDB database (`~/.orbit/graph.duckdb` by default) that
you query with read-only SQL via `orbit sql` (or `glab orbit local --yes sql`).
Run `orbit schema [TABLE…]` to see live columns; the tables below are the ones
you query directly (`_orbit_manifest` is bookkeeping).

## Tables

| Table | Row = | Key columns |
|---|---|---|
| `gl_definition` | a defined symbol | `id`, `name`, `fqn`, `definition_type`, `file_path`, `start_line`, `end_line`, `commit_sha` |
| `gl_file` | an indexed file | `id`, `path`, `language`, `commit_sha` |
| `gl_directory` | a directory | `id`, `path`, `name` |
| `gl_imported_symbol` | an import occurrence | `id`, `identifier_name`, `import_path`, `file_path` |
| `gl_edge` | a relationship | `source_id`, `source_kind`, `relationship_kind`, `target_id`, `target_kind` |

`relationship_kind` values: `DEFINES`, `CALLS`, `IMPORTS`, `CONTAINS`,
`EXTENDS`. Edges are id-to-id — join `source_id`/`target_id` back to
`gl_definition.id` (or `gl_file.id`) to resolve names.

`definition_type` values are **capitalized** (`Function`, `Method`,
`AssociatedFunction`, `Struct`, `Field`, `Variant`, `Module`, `Constant`, …).
Lowercase filters return nothing.

## Recipes

Pass SQL as an argument, or `-` to read from stdin. `-F json|ndjson|csv`
switches output away from the default table.

Definition-type histogram:

```bash
orbit sql "SELECT definition_type, count(*) n FROM gl_definition GROUP BY 1 ORDER BY n DESC"
```

Definitions declared in one file:

```bash
orbit sql "SELECT definition_type, name, start_line FROM gl_definition
           WHERE file_path='crates/orbit-local/src/main.rs' ORDER BY start_line"
```

Who calls a function (`CALLS` edge, resolved to caller names):

```bash
orbit sql "SELECT s.name AS caller, s.file_path, s.start_line
           FROM gl_edge e
           JOIN gl_definition s ON e.source_id = s.id
           JOIN gl_definition t ON e.target_id = t.id
           WHERE e.relationship_kind='CALLS' AND t.name='run_sql'"
```

What a function calls (flip source/target):

```bash
orbit sql "SELECT DISTINCT t.name AS callee
           FROM gl_edge e
           JOIN gl_definition s ON e.source_id = s.id
           JOIN gl_definition t ON e.target_id = t.id
           WHERE e.relationship_kind='CALLS' AND s.name='main'"
```

Subtypes of a base type (`EXTENDS`):

```bash
orbit sql "SELECT s.name AS subtype, s.file_path
           FROM gl_edge e
           JOIN gl_definition s ON e.source_id = s.id
           JOIN gl_definition t ON e.target_id = t.id
           WHERE e.relationship_kind='EXTENDS' AND t.name='Visitor'"
```

Who imports a symbol:

```bash
orbit sql "SELECT DISTINCT file_path FROM gl_imported_symbol
           WHERE identifier_name LIKE '%Workspace%' ORDER BY file_path"
```

## Notes

- Node tables (`gl_definition`, `gl_file`, `gl_directory`, `gl_imported_symbol`)
  carry `commit_sha`; filter on it when a repository has been indexed at more
  than one commit, or re-`index` after checkout. `gl_edge` has no `commit_sha` -
  join back to a definition to scope edges to a commit.
- `orbit sql` is read-only; there is no write path into the graph other than
  `index`.
