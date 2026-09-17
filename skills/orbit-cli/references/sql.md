# Orbit local SQL reference

The local graph is a DuckDB database (`~/.orbit/graph.duckdb` by default) that
you query with read-only SQL via `orbit sql` (or `glab orbit --yes sql`).
Only `index` writes to it.
Run `orbit schema [TABLE…]` to see live columns. The tables below are the ones
you query directly (`_orbit_manifest` is bookkeeping).

## Tables

| Table | Row = | Key columns |
|---|---|---|
| `gl_definition` | a defined symbol | `id`, `name`, `fqn`, `definition_type`, `file_path`, `start_line`, `end_line`, `project_id`, `commit_sha` |
| `gl_file` | an indexed file | `id`, `path`, `language`, `commit_sha` |
| `gl_directory` | a directory | `id`, `path`, `name` |
| `gl_imported_symbol` | an import occurrence | `id`, `identifier_name`, `import_path`, `file_path`, `project_id` |
| `gl_edge` | a relationship | `source_id`, `source_kind`, `relationship_kind`, `target_id`, `target_kind` |
| `_orbit_manifest` | an indexed repository | `repo_path`, `project_id`, `branch`, `commit_sha`, `status` |

`relationship_kind` values: `DEFINES`, `CALLS`, `IMPORTS`, `CONTAINS`,
`EXTENDS`. Edges hold only identifiers. Join `source_id`/`target_id` back to
`gl_definition.id` (or `gl_file.id`) to resolve names.

## Recipes

Definition-type histogram:

```shell
orbit sql "SELECT definition_type, count(*) n FROM gl_definition GROUP BY 1 ORDER BY n DESC"
```

Definitions declared in one file:

```shell
orbit sql "SELECT definition_type, name, start_line FROM gl_definition
           WHERE file_path='crates/orbit-cli/src/main.rs' ORDER BY start_line"
```

Who calls a function (`CALLS` edge, resolved to caller names):

```shell
orbit sql "SELECT s.name AS caller, s.file_path, s.start_line
           FROM gl_edge e
           JOIN gl_definition s ON e.source_id = s.id
           JOIN gl_definition t ON e.target_id = t.id
           WHERE e.relationship_kind='CALLS' AND t.name='run_sql'"
```

What a function calls (flip source/target):

```shell
orbit sql "SELECT DISTINCT t.name AS callee
           FROM gl_edge e
           JOIN gl_definition s ON e.source_id = s.id
           JOIN gl_definition t ON e.target_id = t.id
           WHERE e.relationship_kind='CALLS' AND s.name='main'"
```

Subtypes of a base type (`EXTENDS`):

```shell
orbit sql "SELECT s.name AS subtype, s.file_path
           FROM gl_edge e
           JOIN gl_definition s ON e.source_id = s.id
           JOIN gl_definition t ON e.target_id = t.id
           WHERE e.relationship_kind='EXTENDS' AND t.name='Visitor'"
```

Who imports a symbol:

```shell
orbit sql "SELECT DISTINCT file_path FROM gl_imported_symbol
           WHERE identifier_name LIKE '%Workspace%' ORDER BY file_path"
```

Where an imported symbol is defined in another indexed repository:

```shell
orbit sql --all "SELECT im.repo_path AS importing_repo, i.identifier_name AS symbol,
                 dm.repo_path AS defining_repo, d.file_path AS defining_file,
                 d.start_line AS defining_line
                 FROM gl_imported_symbol i
                 JOIN _orbit_manifest im ON im.project_id = i.project_id
                 JOIN gl_definition d ON d.name = i.identifier_name
                 JOIN _orbit_manifest dm ON dm.project_id = d.project_id
                 WHERE dm.repo_path <> im.repo_path
                 ORDER BY importing_repo, symbol"
```

## Notes

- Only with `--all` or `--repo` do the `commit_sha` columns matter. `gl_edge`
  has none, so join back to a definition to scope edges by hand.
- Edges stay within one repository. A question that spans repositories is a
  join through `_orbit_manifest`, as in the last recipe. That table is never
  scoped, so the join needs `--all` to match anything. It matches on symbol
  name. Narrow on `im.repo_path` or `i.import_path` when several indexed
  repositories, or several worktrees of one, define the same name.
