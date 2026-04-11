# Schema management

## Overview

The GitLab Knowledge Graph schema evolves over time as new entities and relationships are added.
This document describes the implemented approach to schema version tracking and the planned
zero-downtime migration strategy.

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
not exist and record the embedded version as active on a fresh install:

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

## Zero-downtime migration strategy (V0.5 and beyond)

The current implementation lays the foundation for zero-downtime schema migrations using table
prefixes. The planned migration flow (to be implemented in subsequent issues) is:

1. **Bump `config/SCHEMA_VERSION`** — CI enforces that schema-affecting changes are versioned.
2. **Create new table-set** — Indexer creates `vN_*` tables alongside existing ones.
3. **Dual-write** — Indexer writes to both the old and new table-sets during the migration window.
4. **Webserver cutover** — Webserver switches reads to the new table-set.
5. **Cleanup** — Old table-sets beyond `max_retained_versions` are dropped.

Breaking schema changes (column type changes, table restructuring) use new prefixed tables rather
than `ALTER TABLE`, avoiding ClickHouse data rewrites and table locks.
