# Schema management

## Overview

The GitLab Knowledge Graph schema evolves over time as new entities and relationships are added.
This document describes the implemented approach to schema version tracking and the
table-prefix-aware migration orchestrator.

## Schema Definition

The schema is defined by node and relationship types in the ontology (`config/ontology/`) and
materialized as ClickHouse DDL in `config/graph.sql`. The graph DDL creates property graph tables
(one per node type, e.g. `gl_user`, `gl_project`) in the graph ClickHouse database.

## Schema Version Tracking

### `config/SCHEMA_VERSION`

A plain text file at `config/SCHEMA_VERSION` holds the current schema version as a `u32` integer.
The binary reads this at compile time via `include_bytes!` and exposes it as:

```rust
pub const SCHEMA_VERSION: u32 = /* parsed from config/SCHEMA_VERSION */;
```

Version 0 is the initial (V0) schema — the unversioned table layout used since the service launched.

### `gkg_schema_version` control table

All service modes (Indexer, Webserver, DispatchIndexing) create this table on startup if it does
not exist. On a fresh install, the Indexer also creates all graph tables from the ontology DDL
generator and records the embedded version as active:

```sql
CREATE TABLE IF NOT EXISTS gkg_schema_version (
    version UInt32,
    status Enum8('active' = 1, 'migrating' = 2, 'retired' = 3, 'dropped' = 4),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY version
```

Key properties:

- Uses `FINAL` when reading to handle `ReplacingMergeTree` eventual consistency.
- The table itself is never prefixed or dropped — it is the single source of truth for the active
  version across all migrations.
- Implemented in `crates/indexer/src/schema_version.rs`.

### Table prefix derivation

Each schema version maps to a string prepended to graph table names:

| Version | Prefix | Example table |
|---------|--------|---------------|
| 0 | *(empty)* | `gl_user` |
| 1 | `v1_` | `v1_gl_user` |
| N | `vN_` | `vN_gl_user` |

Version 0 uses no prefix for backward compatibility. Functions `table_prefix(version)` and
`prefixed_table_name(table, version)` are provided in `schema_version.rs`.

Unprefixed names are stored in the ontology (the ontology validation enforces the `gl_` prefix
convention). The prefix is applied at the call site when constructing ClickHouse queries.

### Webserver prefix injection

The webserver reads the active schema version once on startup (no hot-swap; a pod restart is
required after a migration completes) and passes the derived prefix through the entire query
pipeline:

```text
startup
  → schema_version::init()           # creates gkg_schema_version if absent
  → schema_version::read_active_version()   # reads active row; None → defaults to v0
  → schema_version::table_prefix(version)   # "" for v0, "v1_" for v1, …
  → GrpcServer::new(…, table_prefix)
  → QueryPipelineService::new(…, table_prefix)
  → QueryPipelineContext { table_prefix }   # shared across all pipeline stages
  → CompilationStage calls compile(…, &ctx.table_prefix)
  → normalize() prepends prefix to every node table name and edge table name
  → lower() uses input.compiler.default_edge_table (already prefixed) instead of a
    compile-time constant
```

Fresh installs (no `gkg_schema_version` row found) default to version 0 (empty prefix) so that
unprefixed tables from before schema versioning was introduced continue to work.

The query compiler does not access ClickHouse metadata to discover which tables exist — the prefix
is injected top-down from the startup read and flows through without further I/O.

### Configuration

```yaml
schema:
  max_retained_versions: 2  # total table-sets to keep (default: 2, minimum: 2)
```

With the default of 2: after migrating to version N, the indexer keeps the N active tables and
the N−1 rollback target, and drops older table-sets automatically. The value is validated at
startup — values below 2 are rejected.

## CI and local enforcement

A CI job (`schema-version-check`, lint stage, MR-only) fails if `config/graph.sql`
or `config/ontology/` changes without a corresponding bump to `config/SCHEMA_VERSION`.
The same check runs as a lefthook pre-commit hook for immediate local feedback.
Local (DuckDB) DDL is generated from the ontology at runtime, so `config/ontology/`
changes automatically affect both ClickHouse and DuckDB schemas.

Non-schema ontology changes (descriptions, comments) can bypass the check by adding
`[skip schema-version-check]` to the MR description.

## Zero-downtime migration orchestrator

When the indexer starts, it compares the embedded `SCHEMA_VERSION` with the active version in
`gkg_schema_version`. If they differ, `schema_migration::run_if_needed()` runs the following
flow **before** the NATS engine starts consuming messages:

### Migration flow

1. **Acquire lock** — NATS KV `indexing_locks/schema_migration` (TTL-based). If another pod
   holds the lock, wait up to 5 minutes; the other pod is handling the migration.

2. **Re-check after lock** — Another pod may have completed the migration while this pod was
   waiting. If the active version now matches, skip.

3. **Drain** — The engine has not started; no in-flight NATS messages exist. This phase is a
   no-op today and is reserved for future dual-write scenarios.

4. **Create new-prefix tables** — Generate DDL from the ontology via
   `generate_graph_tables_with_prefix()` and execute `CREATE TABLE IF NOT EXISTS vN_<table>`
   for each graph table. The table list is derived from the ontology: node tables, edge tables,
   and auxiliary tables (`checkpoint`, `code_indexing_checkpoint`,
   `namespace_deletion_schedule`). Control tables (`gkg_schema_version`) are not prefixed.

5. **Mark migrating** — Insert the new version with status `migrating` in `gkg_schema_version`.
   The Webserver cutover (tracked in issue #441) switches reads to the new table-set.

6. **Release lock** — Allow other pods to proceed.

After the orchestrator returns, the indexer starts normally and writes all data to the new-prefix
tables. Because new-prefix checkpoints are empty, the dispatcher's normal namespace poll cycle
re-dispatches backfill work automatically — no explicit trigger is needed.

### Write path prefix enforcement

All indexer write paths use `prefixed_table_name(table, SCHEMA_VERSION)` at query construction
time:

| Module | Tables prefixed |
|--------|----------------|
| `checkpoint.rs` | `checkpoint` |
| `modules/code/checkpoint_store.rs` | `code_indexing_checkpoint` |
| `modules/code/config.rs` | All code-module node and edge tables (`gl_branch`, `gl_directory`, `gl_file`, `gl_definition`, `gl_imported_symbol`, edge table) |
| `modules/namespace_deletion/store.rs` | `checkpoint`, `code_indexing_checkpoint`, `namespace_deletion_schedule` |
| `modules/namespace_deletion/lower.rs` | All ontology node and edge tables |
| `modules/sdlc/plan/input.rs` | All SDLC node destination tables and the shared edge table |

Datalake tables (`siphon_*`) are never prefixed — only graph tables are.

### Observability

The metric `gkg_schema_migration_total` (counter) tracks migration phase outcomes:

| Label | Values |
|-------|--------|
| `phase` | `acquire_lock`, `drain`, `create_tables`, `mark_migrating`, `complete` |
| `result` | `success`, `failure`, `skipped` |

## Migration completion detection

After the indexer creates new-prefix tables and marks a version as `migrating`, the dispatcher's
normal namespace poll cycle re-indexes all enabled namespaces into the new tables. A scheduled
task (`MigrationCompletionChecker`) running in `DispatchIndexing` mode periodically checks
whether the migration is complete.

Implemented in `crates/indexer/src/migration_completion.rs`.

### Completion criteria

The checker compares checkpoint state in the new-prefix tables against the set of enabled
namespaces from the datalake. Both SDLC and code indexing must be complete:

1. **SDLC namespaces** — count distinct namespace IDs with checkpoint entries (keys matching
   `ns.<id>.*`) in the `vN_checkpoint` table, compared against the count of enabled namespaces
   in `siphon_knowledge_graph_enabled_namespaces`.
2. **Code indexing namespaces** — count distinct namespace IDs (extracted from the
   `traversal_path` column) in `vN_code_indexing_checkpoint`, compared against the same enabled
   namespace count.

When all enabled namespaces have checkpoint entries in both new-prefix tables, the migration is
considered complete.

#### Known trade-off: checkpoint-based validation

Completion is checkpoint-based, not row-count-based. A checkpoint entry proves the indexing
pipeline ran and committed for that scope, but does not validate that the output tables contain
the expected number of rows. This is the standard pattern for CDC/ETL systems: silent data-loss
bugs (e.g. an upstream source returning empty results) would not be caught by this check. Full
data correctness validation is deferred to staging E2E tests.

### Status transitions on completion

When completion is detected:

1. All previously `active` versions are marked `retired`.
2. The `migrating` version is marked `active`.
3. The `gkg_schema_migration_completed_total` counter is incremented.

Webserver pods must be restarted after the version transitions to pick up the new active prefix.

### Automatic cleanup via retention window

After completion detection, the checker enforces the `max_retained_versions` setting (default: 2).
Versions outside this window with status `retired` are cleaned up:

```text
Example with max_retained_versions=2, after migrating to v2:
  v2 → active  (keep)
  v1 → retired (keep — within window, rollback target)
  v0 → retired (OUTSIDE window → drop tables, mark "dropped")
```

Cleanup logic:

1. Read all versions from `gkg_schema_version` ordered by version descending.
2. Filter to non-dropped entries; keep the top `max_retained_versions`.
3. For each entry outside the window with status `retired`:
   a. Execute `DROP TABLE IF EXISTS <prefix><table>` for each graph table.
   b. Mark the version as `dropped` in `gkg_schema_version`.

### Safety guarantees

- Only tables for versions with status `retired` are dropped — never `active` or `migrating`.
- `DROP TABLE IF EXISTS` is idempotent — safe to retry on partial failures.
- The cleanup runs under the `schema_migration` NATS KV lock — no concurrent cleanup attempts.
- `DROP TABLE` uses async drop (no `SYNC` keyword) since table names are monotonically
  versioned and will never be reused.
- Within the retention window (default 2), the previous version's tables always exist for
  rollback.

### Configuration

The migration completion checker runs every 5 minutes by default:

```yaml
schedule:
  tasks:
    migration_completion:
      cron: "0 */5 * * * *"
```

### Observability

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `gkg_schema_migration_completed_total` | counter | — | Successful migration completions |
| `gkg_schema_cleanup_total` | counter | `version`, `result` | Table cleanup operations per version (`success` or `failure`) |

Table drop operations are logged at `info` level with the version and table name.

---

Breaking schema changes (column type changes, table restructuring) use new prefixed tables rather
than `ALTER TABLE`, avoiding ClickHouse data rewrites and table locks.
