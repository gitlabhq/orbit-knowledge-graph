# Orbit local SQL reference

`orbit workspace sql "QUERY"` runs SQL against the local DuckDB graph. `orbit workspace schema [TABLE…]` lists live columns.

## Tables

| Table | Row = | Key columns |
|---|---|---|
| `gl_definition` | a defined symbol | `id`, `name`, `fqn`, `definition_type`, `file_path`, `start_line`, `end_line`, `project_id`, `commit_sha` |
| `gl_file` | an indexed file | `id`, `path`, `language`, `commit_sha` |
| `gl_directory` | a directory | `id`, `path`, `name` |
| `gl_imported_symbol` | an import occurrence | `id`, `identifier_name`, `import_path`, `file_path`, `project_id` |
| `gl_edge` | a relationship | `source_id`, `source_kind`, `relationship_kind`, `target_id`, `target_kind` |
| `_orbit_manifest` | a checkout revision | `repo_path`, `project_id`, `branch`, `commit_sha`, `status`, `database_path` |

`relationship_kind`: `DEFINES`, `CALLS`, `IMPORTS`, `CONTAINS`, `EXTENDS`. Edges hold only IDs. Join `source_id`/`target_id` to `gl_definition.id` or `gl_file.id` to get names.

## Recipes

Definition-type histogram:

```shell
orbit workspace sql "SELECT definition_type, count(*) n FROM gl_definition GROUP BY 1 ORDER BY n DESC"
```

Definitions in one file:

```shell
orbit workspace sql "SELECT definition_type, name, start_line FROM gl_definition
           WHERE file_path='crates/orbit-cli/src/main.rs' ORDER BY start_line"
```

Callers of a function (`CALLS`):

```shell
orbit workspace sql "SELECT s.name AS caller, s.file_path, s.start_line
           FROM gl_edge e
           JOIN gl_definition s ON e.source_id = s.id
           JOIN gl_definition t ON e.target_id = t.id
           WHERE e.relationship_kind='CALLS' AND t.name='run_query'"
```

Callees of a function:

```shell
orbit workspace sql "SELECT DISTINCT t.name AS callee
           FROM gl_edge e
           JOIN gl_definition s ON e.source_id = s.id
           JOIN gl_definition t ON e.target_id = t.id
           WHERE e.relationship_kind='CALLS' AND s.name='main'"
```

Subtypes via `EXTENDS`:

```shell
orbit workspace sql "SELECT s.name AS subtype, s.file_path
           FROM gl_edge e
           JOIN gl_definition s ON e.source_id = s.id
           JOIN gl_definition t ON e.target_id = t.id
           WHERE e.relationship_kind='EXTENDS' AND t.name='Filter'"
```

Importers of a symbol:

```shell
orbit workspace sql "SELECT DISTINCT file_path FROM gl_imported_symbol
           WHERE identifier_name LIKE '%Workspace%' ORDER BY file_path"
```

Cross-repository symbol lookup (`--all`):

```shell
orbit workspace sql --all "SELECT im.repo_path AS importing_repo, i.identifier_name AS symbol,
                 dm.repo_path AS defining_repo, d.file_path AS defining_file
                 FROM gl_imported_symbol i
                  JOIN _orbit_manifest im ON im.project_id = i.project_id AND im.commit_sha = i.commit_sha
                 JOIN gl_definition d ON d.name = i.identifier_name
                  JOIN _orbit_manifest dm ON dm.project_id = d.project_id AND dm.commit_sha = d.commit_sha
                 WHERE dm.repo_path <> im.repo_path
                 ORDER BY importing_repo, symbol"
```

## Notes

- SQL includes all indexed revisions by default. Use `--repo .` to select the current checkout revision. `gl_edge` has no `commit_sha`, so join to a definition to scope edges.
- Edges stay inside one repository. Cross-repository questions join through `_orbit_manifest`. The recipe matches on symbol name, so narrow on `im.repo_path` and commit for duplicate names.
