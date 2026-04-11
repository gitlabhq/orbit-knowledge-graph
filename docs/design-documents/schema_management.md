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

### Configuration

```yaml
schema:
  max_retained_versions: 2  # total table-sets to keep (default: 2, minimum: 2)
```

With the default of 2: after migrating to version N, the indexer keeps the N active tables and
the N−1 rollback target, and drops older table-sets automatically. The value is validated at
startup — values below 2 are rejected.

## CI and local enforcement

A CI job (`schema-version-check`, lint stage, MR-only) fails if `config/graph.sql`,
`config/graph_local.sql`, or `config/ontology/` changes without a corresponding bump to
`config/SCHEMA_VERSION`. The same check runs as a lefthook pre-commit hook for immediate local
feedback.

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

### Subsequent migration steps

- **Issue #441** (query compiler): Switch Webserver reads to the new table-set.
- **Issue #442** (cleanup): Drop old table-sets beyond `max_retained_versions`.

Breaking schema changes (column type changes, table restructuring) use new prefixed tables rather
than `ALTER TABLE`, avoiding ClickHouse data rewrites and table locks.
