# Local Code Graph Queries with DuckDB

## Summary

Enable the `orbit` CLI to index local repositories into a DuckDB database and execute graph queries against it offline — no ClickHouse, no server, no network required.

## Motivation

Today `orbit index` builds an in-memory `GraphData` and discards it after printing stats. `orbit query` compiles JSON DSL to ClickHouse SQL but never executes it. There is no local query path. Developers should be able to index a repository and immediately query its code graph locally.

## Scope

### In scope

- **5 code graph tables**: `gl_directory`, `gl_file`, `gl_definition`, `gl_imported_symbol`, `gl_edge`
- **All 5 query types**: search, traversal, aggregation, path_finding, neighbors
- **DuckDB** as the embedded database (single file at `~/.orbit/indexes/<repo>/graph.duckdb`)
- **SQL dialect abstraction** in the query engine codegen layer
- **Local-mode compile path** that skips security context and redaction enforcement

### Out of scope

- SDLC data (MRs, pipelines, vulnerabilities, etc.)
- Multi-tenancy / traversal_path security filtering
- Rails-delegated redaction
- Hydration pipeline
- Server-mode DuckDB support

## Architecture

```plaintext
orbit index <path>
    ├── DirectoryFileSource (file discovery)
    ├── RepositoryIndexer (parse → analyze → GraphData)
    ├── GraphData::assign_node_ids()
    ├── ArrowConverter::convert_all() → RecordBatches
    └── DuckDbClient::insert_arrow() → ~/.orbit/indexes/<repo>/graph.duckdb

orbit query --local <path> --json '<query>'
    ├── compile_local(json, ontology, SqlDialect::DuckDb)
    │   ├── validate + parse + normalize (unchanged)
    │   ├── lower (unchanged — AST uses logical function names)
    │   ├── skip enforce_return (no redaction)
    │   ├── skip apply_security_context (no multi-tenancy)
    │   ├── skip check_ast (no security invariants)
    │   └── codegen(ast, dialect=DuckDb) → ParameterizedQuery
    ├── DuckDbClient::open(graph.duckdb)
    ├── DuckDbClient::query_arrow(sql, params) → Vec<RecordBatch>
    └── format results as JSON
```

## DuckDB DDL

```sql
CREATE TABLE IF NOT EXISTS gl_directory (
    id BIGINT PRIMARY KEY,
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_file (
    id BIGINT PRIMARY KEY,
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    extension VARCHAR,
    language VARCHAR,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_definition (
    id BIGINT PRIMARY KEY,
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    fqn VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    definition_type VARCHAR NOT NULL,
    start_line BIGINT,
    end_line BIGINT,
    start_byte BIGINT,
    end_byte BIGINT,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_imported_symbol (
    id BIGINT PRIMARY KEY,
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    project_id BIGINT NOT NULL,
    branch VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    import_type VARCHAR,
    import_path VARCHAR,
    identifier_name VARCHAR,
    identifier_alias VARCHAR,
    start_line BIGINT,
    end_line BIGINT,
    start_byte BIGINT,
    end_byte BIGINT,
    _version BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS gl_edge (
    traversal_path VARCHAR NOT NULL DEFAULT '0/',
    source_id BIGINT NOT NULL,
    source_kind VARCHAR NOT NULL,
    relationship_kind VARCHAR NOT NULL,
    target_id BIGINT NOT NULL,
    target_kind VARCHAR NOT NULL,
    _version BIGINT NOT NULL DEFAULT 0
);
```

Local mode uses `traversal_path = '0/'`, `project_id` derived from path hash, `branch = 'HEAD'`.

## SQL Dialect Abstraction

### Approach

Add a `SqlDialect` enum to the codegen layer. The AST remains unchanged — it uses logical function names. Codegen remaps them per dialect.

### Function mapping

| AST function name | ClickHouse emission | DuckDB emission |
|---|---|---|
| `startsWith` | `startsWith` | `starts_with` |
| `has` | `has` | `list_contains` |
| `array` | `array` | `list_value` |
| `arrayConcat` | `arrayConcat` | `list_concat` |
| `arrayResize` | `arrayResize` | `list_resize` |
| `tuple` | `tuple` | `struct_pack` |
| `if` | `if` | `CASE WHEN $1 THEN $2 ELSE $3 END` |

### Parameter placeholders

| Dialect | Format | Example |
|---|---|---|
| ClickHouse | `{pN:Type}` | `{p0:String}` |
| DuckDB | `$N` (1-indexed) | `$1` |

### SET statements

ClickHouse: emitted before query (`SET allow_experimental_analyzer = 1;`).
DuckDB: skipped entirely.

### IN with arrays

ClickHouse: `col IN {p0:Array(String)}` — single array parameter.
DuckDB: element-by-element expansion `col IN ($1, $2, $3)` — DuckDB does not support array arguments in IN.

## New crate: `duckdb-client`

```plaintext
crates/duckdb-client/
├── Cargo.toml
└── src/
    ├── lib.rs      (re-exports)
    ├── client.rs   (DuckDbClient)
    ├── schema.rs   (DDL constants)
    └── error.rs    (DuckDbError)
```

### Dependencies

- `duckdb = "1.10500.0"` (features: `bundled`)
- `arrow = "57.2.0"`

### Public API

```rust
pub struct DuckDbClient { conn: Connection }

impl DuckDbClient {
    pub fn open(path: &Path) -> Result<Self>;
    pub fn open_in_memory() -> Result<Self>;
    pub fn initialize_schema(&self) -> Result<()>;
    pub fn execute(&self, sql: &str) -> Result<()>;
    pub fn query_arrow(
        &self,
        sql: &str,
        params: &[(&str, &dyn duckdb::ToSql)],
    ) -> Result<Vec<RecordBatch>>;
    pub fn insert_arrow(&self, table: &str, batch: &RecordBatch) -> Result<()>;
}
```

## Query Engine Changes

### New public function: `compile_local`

```rust
pub fn compile_local(
    json_input: &str,
    ontology: &Ontology,
    dialect: SqlDialect,
) -> Result<CompiledQueryContext>
```

Pipeline: validate → parse → normalize → lower → codegen(dialect). No enforce, no security, no check.

### `SqlDialect` enum

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    ClickHouse,
    DuckDb,
}
```

### Codegen changes

`codegen()` gains a `dialect` argument. The `Context` struct stores the dialect and dispatches:

- `emit_param()`: `{pN:Type}` vs `$N`
- `emit_expr()` for `FuncCall`: remaps function names via `dialect.func_name()`
- `emit_query()`: skips SET statements for DuckDB
- Array IN values: always expand element-by-element for DuckDB

## CLI Changes

### `orbit index`

After building `GraphData`:

1. `graph_data.assign_node_ids(project_id, "HEAD")`
1. `ArrowConverter::new("0/", project_id, "HEAD", Utc::now()).convert_all(&graph_data)`
1. `DuckDbClient::open(~/.orbit/indexes/<repo>/graph.duckdb)`
1. `client.initialize_schema()`
1. Delete existing data for this project/branch, then insert new batches
1. Update manifest status to `Indexed`

### `orbit query`

New `--local <path>` flag:

1. Resolve repo path → `~/.orbit/indexes/<repo>/graph.duckdb`
1. Load ontology (full — the DSL validator needs it)
1. `compile_local(json, &ontology, SqlDialect::DuckDb)`
1. Open DuckDB, execute query, format results as JSON

## Testing

- Unit tests for DuckDB codegen (compare emitted SQL side-by-side with ClickHouse)
- Unit tests for `DuckDbClient` (in-memory DB, insert + query round-trip)
- Integration test: index a small fixture directory, run all 5 query types
- Existing query-engine tests must continue to pass unchanged
