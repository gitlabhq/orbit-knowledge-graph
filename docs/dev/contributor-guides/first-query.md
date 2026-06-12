# Write your first Orbit query

This tutorial takes you from a clean checkout to your first useful Orbit Local
queries. Orbit Local runs entirely on your machine, stores its graph in DuckDB,
and does not need Orbit Remote, Rails, ClickHouse, NATS, Docker, or a GitLab
account.

You need:

- `glab` 1.94 or later.
- Any local Git repository with source files.

The examples use `orbit` directly after installation. If you prefer to stay
inside `glab`, replace `orbit` with `glab orbit local` in each command.

The current Orbit Local command queries the local DuckDB graph with SQL. The
Orbit Remote Query DSL is linked at the end after the local graph concepts are
familiar.

## Install Orbit Local

Install the managed Orbit Local binary:

```shell
glab orbit local --install --yes
```

Verify that the binary is available:

```shell
orbit help
```

You should see commands for indexing a repository, inspecting the schema, and
querying the local DuckDB graph.

## Index a repository

Move into any Git repository, then index it:

```shell
cd /path/to/any/git/repo
orbit index .
```

Orbit parses the repository and writes graph rows to `~/.orbit/graph.duckdb`.
The command prints a JSON summary similar to:

```json
{
  "repository": "my-project",
  "graph": {
    "directories": 42,
    "files": 318,
    "definitions": 904,
    "imported_symbols": 211,
    "relationships": 1459
  },
  "processing": {
    "skipped_files": 0,
    "errored_files": 0
  }
}
```

The exact counts depend on the repository and supported languages in it.

## Inspect the schema

Before writing a query, inspect the graph tables:

```shell
orbit schema
```

Orbit Local indexes four code graph node types:

| Node type | Table | What it represents |
|---|---|---|
| `Directory` | `gl_directory` | A directory in the indexed repository. |
| `File` | `gl_file` | A source file. |
| `Definition` | `gl_definition` | A function, class, method, module, or other named symbol. |
| `ImportedSymbol` | `gl_imported_symbol` | An import or cross-file symbol reference. |

Relationships between those nodes are stored in `gl_edge`. Each relationship
row has a source node, a relationship kind, and a target node:

| Column | Meaning |
|---|---|
| `source_kind` / `source_id` | The node where the relationship starts. |
| `relationship_kind` | The relationship type, such as `CONTAINS` or `DEFINES`. |
| `target_kind` / `target_id` | The node where the relationship ends. |

## List files

Start with a small query that lists files from the indexed repository:

```shell
orbit sql 'SELECT path, language FROM gl_file ORDER BY path LIMIT 5'
```

Example output:

```plaintext
+------------------------+----------+
| path                   | language |
+------------------------+----------+
| Cargo.toml             | toml     |
| crates/app/src/main.rs | rust     |
| docs/index.md          | markdown |
+------------------------+----------+
```

This is the simplest graph query: select nodes of one type, `File`, and return
some properties. In SQL form, selecting a node type means reading from its
table.

## Add a filter

Now filter definitions by name:

```shell
orbit sql "SELECT file_path, name, definition_type, start_line FROM gl_definition WHERE lower(name) LIKE '%parse%' ORDER BY file_path, start_line LIMIT 10"
```

This looks for definitions whose name contains `parse`, then returns where each
definition appears. A repository might return rows like:

```plaintext
+---------------------------+--------------+-----------------+------------+
| file_path                 | name         | definition_type | start_line |
+---------------------------+--------------+-----------------+------------+
| crates/parser/src/lib.rs  | parse_query  | Function        | 42         |
| crates/parser/src/mod.rs  | parse_filter | Function        | 118        |
+---------------------------+--------------+-----------------+------------+
```

If your repository has no matching definitions, the command still succeeds and
prints an empty table.

## Follow a relationship

Relationships let you move from one node type to another. This query starts at
`File` nodes, follows outgoing `DEFINES` relationships, and returns the
definitions declared in each file:

```shell
orbit sql "SELECT f.path, d.name, d.definition_type, d.start_line FROM gl_file f JOIN gl_edge e ON e.source_kind = 'File' AND e.relationship_kind = 'DEFINES' AND e.target_kind = 'Definition' AND e.source_id = f.id JOIN gl_definition d ON d.id = e.target_id ORDER BY f.path, d.start_line LIMIT 5"
```

Example output:

```plaintext
+--------------------------+-------------+-----------------+------------+
| path                     | name        | definition_type | start_line |
+--------------------------+-------------+-----------------+------------+
| crates/app/src/main.rs   | main        | Function        | 12         |
| crates/app/src/query.rs  | QueryRunner | Struct          | 8          |
+--------------------------+-------------+-----------------+------------+
```

The graph part is the join through `gl_edge`:

- `source_kind = 'File'` means the relationship starts at a file.
- `relationship_kind = 'DEFINES'` means the file declares a definition.
- `target_kind = 'Definition'` means the relationship ends at a definition.

## Change the output format

The default `orbit sql` output is a table. Use `-F json` when you want a shape
that is easier to pipe into another tool:

```shell
orbit sql -F json 'SELECT path, language FROM gl_file ORDER BY path LIMIT 2'
```

Example output:

```json
[
  {
    "path": "Cargo.toml",
    "language": "toml"
  },
  {
    "path": "crates/app/src/main.rs",
    "language": "rust"
  }
]
```

Other formats are available:

```shell
orbit sql -F ndjson 'SELECT path FROM gl_file LIMIT 2'
orbit sql -F csv 'SELECT path, language FROM gl_file LIMIT 2'
```

## Try one more repository

Indexing another repository helps you see which parts of the output are
repository-specific and which parts come from the Orbit graph model:

```shell
cd /path/to/another/git/repo
orbit index .
orbit sql 'SELECT language, count(*) AS files FROM gl_file GROUP BY language ORDER BY files DESC LIMIT 10'
```

The local graph can contain multiple indexed repositories. Rows include
`project_id`, `branch`, and `commit_sha` columns when you need to distinguish
between them.

## Where to go next

- [Orbit Local schema reference](../../source/local/schema.md) for the four
  local node types and their properties.
- [Orbit Local access methods](../../source/local/getting-started.md) for
  direct `orbit`, `glab orbit local`, and planned MCP usage.
- [Orbit query language reference](../../source/remote/queries/query-language.md)
  for the Query DSL used by Orbit Remote and agent-facing graph queries.
- [Cookbook](../../source/remote/cookbook.md) for copy-paste query ideas.
- [Add a language to the code indexer](../adding-a-language.md) if you want to
  contribute parser coverage for more source languages.
