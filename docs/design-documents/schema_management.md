# Schema management

## Overview

The GitLab Knowledge Graph schema evolves over time as we add new entities and relationships. This document describes the implemented schema management strategy, from schema definition through version tracking and migration.

This document covers:

1. **Schema Definition**: How node and edge types are defined.
2. **Schema Registration**: How the query engine discovers the current schema.
3. **Schema Version Tracking**: How the binary tracks and detects schema mismatches.
4. **Schema Evolution**: Strategies for applying schema changes.

## Schema Definition and Registration

The schema is defined by the node and relationship types detailed in the [Data Model](./data_model.md) document. The authoritative DDL lives in `config/graph.sql`. The ontology YAML in `config/ontology/nodes/` drives ETL, query validation, and redaction — new entity types start there, not in Rust.

The Graph Query Engine (`gkg-webserver`) uses the compiled ontology to understand available node types and their ClickHouse table mappings at startup, without runtime metadata queries.

## Schema Version Tracking (V0)

The V0 strategy uses a **drop-and-recreate** approach for schema migrations. A schema version is embedded in the GKG binary and persisted in ClickHouse. A periodic background check detects mismatches and coordinates the reset.

### Schema version constant

`SCHEMA_VERSION` (`crates/indexer/src/schema_version.rs`) is a manually bumped `u64` constant. Any MR that changes `config/graph.sql` must also bump this constant. A CI check (`scripts/check-schema-version.sh`) enforces this.

Why manual and not auto-computed: a hash of `graph.sql` would trigger on whitespace or comment changes that don't affect the actual schema. A manual version makes schema-changing commits explicit in the MR diff.

### Control table

The `gkg_schema_version` table persists the current schema version:

```sql
CREATE TABLE IF NOT EXISTS gkg_schema_version (
    version UInt64,
    applied_at DateTime64(6, 'UTC') DEFAULT now64(6),
    _version DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (version)
```

This table is created via `CREATE TABLE IF NOT EXISTS` at indexer startup, before any version checks. It is excluded from the drop-and-recreate set.

### Periodic mismatch detection

The version check runs as a periodic background task in Indexer mode (configurable via `schema_version_check.interval_secs`, default 30s).

**Check loop:**

1. Read persisted version from `gkg_schema_version FINAL`
2. Compare to embedded `SCHEMA_VERSION`
3. If they match: nothing to do (lightweight no-op — one SELECT)
4. If mismatch: count enabled namespaces in the datalake
5. If namespaces still enabled: log warning, continue running normally
6. If zero enabled namespaces: trigger schema reset (Issue 2)

**Operational workflows:**

- **Deploy first, disable later**: The indexer keeps running after deploy. It logs a warning each cycle about the mismatch. Once the admin disables namespaces, the next check cycle triggers the reset.
- **Disable first, deploy later**: When the new binary starts, the first check cycle sees mismatch + zero namespaces → immediate reset.
- **Fresh install**: No rows in `gkg_schema_version` → treated as mismatch. Fresh install has zero enabled namespaces → reset triggers immediately. For a fresh install this is equivalent to applying `graph.sql` (all `CREATE TABLE IF NOT EXISTS` are no-ops or first creates).

### Observability

- **Metric**: `gkg.schema.version.mismatch` gauge — 1 when mismatch detected, 0 otherwise
- **Logging**: `warn` each check cycle while mismatch persists and namespaces are enabled; `info` when mismatch is detected and when reset is triggered

## Schema Evolution and Migrations

As GitLab evolves, the Knowledge Graph schema must evolve with it. The migration strategy depends on the nature of the change.

### V0: Drop and recreate

For the initial rollout, schema changes use a drop-and-recreate strategy:

1. Bump `SCHEMA_VERSION` in the MR that changes `graph.sql`
2. Deploy the new binary
3. Disable all namespaces (via the `knowledge_graph_enabled_namespaces` table)
4. The indexer detects the mismatch and zero namespaces, drops GKG-owned tables, and re-applies `graph.sql`
5. Re-enable namespaces to trigger re-indexing

This is acceptable during early rollout when the data can be fully re-indexed from the datalake.

### ALTER TABLE

For additive, non-breaking changes like `ADD COLUMN`, `ALTER TABLE` is a metadata-only operation in ClickHouse — nearly instantaneous and non-blocking.

For complex or breaking changes (e.g. modifying a column's data type), ClickHouse must rewrite the affected column data. This can be long-running and resource-intensive.

### Shadow Column Migrations

The process for a breaking change follows the GitLab [zero-downtime migration process](https://docs.gitlab.com/ee/development/database/avoiding_downtime_in_migrations.html):

1. **Create Shadow Column**: Add a new column with the desired schema alongside the original.
2. **Backfill Historical Data**: Copy existing data in batches.
3. **Dual-write on insert**: Use the lower of migration start date and last indexing date.
4. **Atomic Column Swap**: Rename columns in a single `ALTER TABLE` with multiple `RENAME COLUMN` clauses.
5. **Clean Up**: Drop the old column in a subsequent deployment.
