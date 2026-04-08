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
6. If zero enabled namespaces: execute the drop-and-recreate schema reset

**Operational workflows:**

- **Deploy first, disable later**: The indexer keeps running after deploy. It logs a warning each cycle about the mismatch. Once the admin disables namespaces, the next check cycle triggers the reset.
- **Disable first, deploy later**: When the new binary starts, the first check cycle sees mismatch + zero namespaces → immediate reset.
- **Fresh install**: No rows in `gkg_schema_version` → treated as mismatch. Fresh install has zero enabled namespaces → reset triggers immediately. For a fresh install this is equivalent to applying `graph.sql` (all `CREATE TABLE IF NOT EXISTS` are no-ops or first creates).

### Drop-and-recreate reset

When the check loop finds mismatch + zero enabled namespaces, it executes a reset:

1. **Acquire lock**: Try to acquire the `schema_reset` key in the `indexing_locks` NATS KV bucket (120 s TTL). If another pod holds the lock, skip this cycle — the next check will re-examine and resume normally once the version matches.
2. **Drop GKG-owned tables**: Execute `DROP TABLE IF EXISTS … SYNC` for every table parsed from the embedded `config/graph.sql`. The `SYNC` suffix waits for completion before proceeding.
3. **Recreate tables**: Re-execute every statement in `config/graph.sql` (all `CREATE TABLE IF NOT EXISTS` — idempotent).
4. **Record version**: Insert the new `SCHEMA_VERSION` into `gkg_schema_version`.
5. **Release lock**.

**Table classification:**

- **GKG-owned** (droppable): all tables defined in `config/graph.sql` — `gl_*`, `checkpoint`, `code_indexing_checkpoint`, `namespace_deletion_schedule`
- **Excluded from drop**: `gkg_schema_version` — never dropped; survives resets
- **External** (never touched): `siphon_*` tables are not in `graph.sql` and are never dropped

Table names are parsed from the embedded `GRAPH_SCHEMA_SQL` constant at runtime, so the drop set stays automatically in sync with `graph.sql` without any hardcoded list.

**Concurrency safety:** `DROP TABLE IF EXISTS` and `CREATE TABLE IF NOT EXISTS` are both idempotent in ClickHouse. The NATS lock reduces redundant work during rolling deploys but is not required for correctness.

**Error handling:** If any step fails, the lock is released and the error is logged. The next detection cycle retries the full reset.

### Observability

- **Metric**: `gkg.schema.version.mismatch` gauge — 1 when mismatch detected, 0 otherwise
- **Metric**: `gkg.schema.version.check_loop_active` gauge — 1 while the check loop is running, 0 after it exits
- **Metric**: `gkg.schema.reset.total` counter — incremented on each reset attempt, labeled `result=success|failure`
- **Logging**: `warn` each check cycle while mismatch persists and namespaces are enabled; `warn` when reset begins; `info` when reset completes and version is recorded

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

**Re-indexing after reset**: No new re-indexing machinery is needed. The existing dispatch pipeline handles the post-reset state naturally because re-enabling a namespace is indistinguishable from enabling it for the first time:

- `NamespaceDispatcher` and `GlobalDispatcher` fire on their normal schedule. With empty `checkpoint` and graph tables, the handlers start from watermark epoch-zero and pick up all datalake rows.
- `NamespaceCodeBackfillDispatcher` consumes CDC insert events for the re-enabled namespaces and dispatches `CodeIndexingTaskRequest` per project.
- `NamespaceDeletionScheduler` starts from epoch-zero with an empty `namespace_deletion_schedule` table and rebuilds deletion schedules organically.

See the [V0 schema reset runbook](../dev/runbooks/v0_schema_reset.md) for the step-by-step procedure and troubleshooting guide.

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
