# Schema management

## Overview

The GitLab Knowledge Graph schema evolves over time as new entities and relationships are added.
This document describes the implemented approach to schema version tracking and the
table-prefix-aware migration orchestrator.

## Schema Definition

The schema is defined by node and relationship types in the ontology (`config/ontology/`) and
materialized as ClickHouse DDL in `config/graph.sql`. The graph DDL creates property graph tables
(one per node type, e.g. `gl_user`, `gl_project`) in the graph ClickHouse database. Ontology
storage metadata also owns table-level MergeTree settings, such as indexes, projections, primary
keys, and explicit `SETTINGS` entries that need to be emitted into the generated DDL.

## Schema Version Tracking

This file covers ClickHouse DDL versioning only. The query **response format** is
versioned separately as a semver in `config/RAW_OUTPUT_FORMAT_VERSION` and
enforced by `scripts/check-response-schema-version.sh` alongside
`scripts/check-schema-version.sh`. See [ADR 004](decisions/004_unified_response_schema.md)
for the response format contract.

### `config/SCHEMA_VERSION`

A plain text file at `config/SCHEMA_VERSION` holds the current schema version as a `u32` integer.
The binary reads this at compile time via `include_bytes!` and exposes it as:

```rust
pub const SCHEMA_VERSION: u32 = /* parsed from config/SCHEMA_VERSION */;
```

Version 0 is the initial (V0) schema — the unversioned table layout used since the service launched.

#### When to bump

Bump `config/SCHEMA_VERSION` for any ontology or DDL change that affects what is stored in
ClickHouse, not just table structure:

- **DDL shape changes**: new columns, type changes, index additions, engine changes.
- **Storage setting changes**: primary keys, sort keys, projections, index granularity, or
  table-level ClickHouse settings emitted into `CREATE TABLE ... SETTINGS`.
- **Edge type renames**: e.g. `MERGED_BY` → `MERGED`. The `gl_edge.relationship_kind` column
  stores these as string values, so old rows remain with the old name while the compiler emits
  the new name. Without a bump, affected edges are silently missing from query results.
- **ETL mapping changes**: column renames, enum value changes, FK rewiring. The ETL pipeline
  is fully ontology-driven (`PlanInput` is built from `&Ontology`), so these are always
  ontology YAML changes and the CI check catches them automatically.

Changes that do **not** require a bump: ontology description updates, comments, formatting,
documentation-only fields, or query-side-only changes (new filter operators, new query types).

### `gkg_schema_version` control table

The Indexer and DispatchIndexing modes create this table on startup if it does not exist;
the Webserver only reads from it (it runs as a read-only ClickHouse user). On a fresh install,
the Indexer also creates all graph tables from the ontology DDL generator and records the
embedded version as active:

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

The webserver pins the table prefix to its **embedded** `SCHEMA_VERSION` at startup. The prefix
flows through the query pipeline once and never changes for the lifetime of the process — a binary
upgrade is the only way to switch to a new prefix. The active version recorded in
`gkg_schema_version` is consulted only by the readiness gate (see below), not by the query path.

```plaintext
startup
  → schema_version::table_prefix(SCHEMA_VERSION)   # "" for v0, "v1_" for v1, …
  → GrpcServer::new(…, table_prefix)
  → QueryPipelineService::new(…, table_prefix)
  → QueryPipelineContext { table_prefix }          # shared across all pipeline stages
  → CompilationStage calls compile(…, &ctx.table_prefix)
  → normalize() prepends prefix to every node table name and edge table name
  → lower() uses input.compiler.default_edge_table (already prefixed) instead of a
    compile-time constant
```

The query compiler does not access ClickHouse metadata to discover which tables exist — the prefix
is injected top-down from the embedded version and flows through without further I/O.

### Webserver readiness gate

A background task (`SchemaWatcher`) polls `gkg_schema_version` every
`schema.version_poll_interval_secs` seconds (default `5`) and classifies the result against the
binary's embedded version:

| Database active version vs. binary | State | `/ready` response | Action |
|---|---|---|---|
| missing (no row yet) | `Pending` | `503` with `schema_pending` | keep polling |
| `<` binary | `Pending` | `503` with `schema_pending` | keep polling |
| `==` binary | `Ready` | existing checks (`200` if all healthy) | serve traffic |
| `>` binary | `Outdated` | `503` with `schema_outdated` | log error, cancel shutdown token, exit |

`/live` is never gated on the watcher — Kubernetes keeps the pod alive while it waits for the
indexer to promote the matching version. When the binary detects a newer active version than it
supports, the watcher cancels the shared `CancellationToken`, the gRPC and HTTP servers exit
their `tokio::select`, and the process returns. Kubernetes restarts the pod; if the operator
deployed the wrong (too-old) binary, `CrashLoopBackoff` surfaces the mistake instead of silently
serving the wrong schema.

Transient ClickHouse errors during a poll keep the previous state — the watcher does not
flap to `Pending` on a single failed read.

Implemented in `crates/gkg-server/src/schema_watcher.rs`.

#### Observability

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `gkg.webserver.schema.state` | observable gauge | `state` (`pending` \| `ready` \| `outdated`) | Value `1` for the active state, `0` otherwise |

### Configuration

```yaml
schema:
  max_retained_versions: 2          # total table-sets to keep (default: 2, minimum: 2)
  version_poll_interval_secs: 5     # webserver readiness-gate poll cadence (default: 5, minimum: 1)
```

With `max_retained_versions: 2`: after migrating to version N, the indexer keeps the N active
tables and the N−1 rollback target, and drops older table-sets automatically. The value is
validated at startup — values below 2 are rejected.

`version_poll_interval_secs` controls how often the webserver re-reads the active version from
`gkg_schema_version` to drive the readiness gate (see "Webserver readiness gate" below).

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
| `modules/sdlc/plan/input.rs` | All SDLC node destination tables and per-relationship edge tables (resolved from ontology) |

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

Webserver behavior on promotion is automatic: pods built for the new version flip to `Ready`
on the next poll, and pods built for an older version detect `active > embedded` and exit via
the `SchemaWatcher` shutdown path described above. No manual restart is required for either
fleet — Kubernetes recycles the old pods and routes traffic to the new ones once they pass
their readiness check.

### Automatic cleanup via retention window

After completion detection, the checker enforces the `max_retained_versions` setting (default: 2).
Versions outside this window with status `retired` are cleaned up:

```plaintext
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
