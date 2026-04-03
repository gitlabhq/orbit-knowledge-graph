# V1 — Migration registry

The smallest useful version of the framework: a Rust migration registry, a ClickHouse-backed ledger, a NATS KV migration lock, and support for additive DDL migrations only.

## Goal

Replace the manual `config/graph.sql` application process with an automated, version-tracked migration system that can apply additive schema changes (new tables, new columns, new projections) without human intervention during deployment.

## Components

### Migration trait

Migrations are defined in Rust and compiled into the binary. Each migration implements a trait:

```rust
#[async_trait]
pub trait Migration: Send + Sync {
    /// Monotonically increasing version number.
    fn version(&self) -> u64;

    /// Human-readable migration name (e.g., "add_gl_epic_table").
    fn name(&self) -> &str;

    /// The type of migration — determines which lifecycle phases apply.
    fn migration_type(&self) -> MigrationType;

    /// Apply the global schema change (DDL).
    async fn prepare(&self, ctx: &MigrationContext) -> Result<()>;
}

pub enum MigrationType {
    /// Additive DDL only — complete after prepare().
    Additive,
    /// Requires scoped reindexing (V2).
    Convergent,
    /// Cleanup of previously converged migration (V3).
    Finalization,
}
```

The `MigrationContext` provides a ClickHouse client for executing DDL statements. In V1, only `Additive` migrations are supported — `Convergent` and `Finalization` are defined in the enum for forward compatibility but rejected by the reconciler.

### Migration registry

A `MigrationRegistry` collects all migrations and validates ordering:

```rust
pub struct MigrationRegistry {
    migrations: Vec<Box<dyn Migration>>,
}

impl MigrationRegistry {
    pub fn new() -> Self { /* ... */ }

    /// Register a migration. Panics if version is not strictly increasing.
    pub fn register(&mut self, migration: Box<dyn Migration>) { /* ... */ }

    /// All registered migrations, ordered by version.
    pub fn migrations(&self) -> &[Box<dyn Migration>] { /* ... */ }
}
```

Migrations are registered in a central function (e.g., `fn build_migration_registry() -> MigrationRegistry`) that is called at indexer startup.

### Migration ledger (ClickHouse)

A `gkg_migrations` table records the lifecycle of each migration:

```sql
CREATE TABLE IF NOT EXISTS gkg_migrations (
    version        UInt64,
    name           String,
    migration_type LowCardinality(String),
    status         LowCardinality(String),
    started_at     Nullable(DateTime64(6, 'UTC')),
    completed_at   Nullable(DateTime64(6, 'UTC')),
    error_message  Nullable(String),
    retry_count    UInt32 DEFAULT 0,
    _version       DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (version)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;
```

**Status values in V1**: `pending`, `preparing`, `prepared`, `completed`, `failed`.

This table is bootstrapped by the reconciler using `CREATE TABLE IF NOT EXISTS` — the one piece of DDL that runs outside the migration framework. This is safe because `CREATE TABLE IF NOT EXISTS` is idempotent and metadata-only in ClickHouse.

### Migration lock (NATS KV)

The reconciler acquires an exclusive lock before advancing migration state, reusing the existing `LockService` trait and `indexing_locks` bucket:

| Key | Purpose | TTL |
|---|---|---|
| `migration.reconciler` | Exclusive lock for the reconciler loop | ~60s, refreshed periodically |

The lock uses TTL-based leasing. If the lock-holding instance dies, the lease expires and another eligible instance acquires it.

**Lock-holder eligibility.** After acquiring the lock, the instance compares its migration registry against the persisted ledger. If it cannot advance any migration (e.g., an older binary during rolling deployment), it releases the lock. This ensures the newest compatible binary drives migration progression during mixed-version windows.

### Migration reconciler

The reconciler is a background task within the `Indexer` mode process. It does **not** run in `DispatchIndexing` (run-once-and-exit) or `Webserver` (read-only).

```
loop {
    if !try_refresh_migration_lock() {
        // Another instance holds the lock, or lock acquisition failed.
        sleep(backoff_interval)
        continue
    }

    let registry = load_migration_registry()
    let ledger = read_migration_ledger_from_clickhouse()

    for migration in registry.migrations() {
        match ledger.status(migration.version()) {
            None | Some(Status::Pending) => {
                // New migration — record as pending, then start preparing
                upsert_migration_status(migration, Status::Preparing)
                match migration.prepare(&ctx).await {
                    Ok(()) => {
                        upsert_migration_status(migration, Status::Completed)
                    }
                    Err(e) => {
                        upsert_migration_status(migration, Status::Failed, error: e)
                        break // Stop at first failure — migrations are sequential
                    }
                }
            }
            Some(Status::Failed) => {
                // Retry: increment retry_count, re-attempt prepare()
                // Break after max retries to avoid infinite loops
            }
            Some(Status::Completed) => continue,
            _ => break, // Unexpected state — stop and log
        }
    }

    sleep(reconcile_interval)
}
```

Key behaviors:

- **Sequential execution.** Migrations are applied in version order. A failed migration blocks subsequent ones.
- **Idempotent DDL.** Migration `prepare()` implementations should use idempotent DDL (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ADD COLUMN IF NOT EXISTS`) so that retries are safe.
- **No startup coupling.** The reconciler starts as a background `tokio::spawn` task alongside the message processing engine. Pod readiness is not affected.
- **Failure isolation.** A failed migration is recorded with its error message and retry count. The reconciler retries on the next loop iteration up to a configurable maximum, then stops advancing until operator intervention.

### NATS KV schema version notification

After completing a migration, the reconciler writes the current schema version to a NATS KV key:

| Key | Value | Purpose |
|---|---|---|
| `migration.version` | Latest completed migration version (as string) | Webserver cache invalidation trigger |

The webserver can watch this key to know when to refresh any cached schema metadata. This is a lightweight notification mechanism — the webserver reads durable state from ClickHouse or the ontology, not from NATS KV.

## Example: adding a new node table

Suppose we need to add a `gl_epic` table. Today this requires:

1. Add the DDL to `config/graph.sql`.
2. Manually apply the DDL against production ClickHouse.
3. Add the ontology definition in `config/ontology/nodes/`.
4. Deploy the new code.

With V1, steps 1–2 become a migration:

```rust
pub struct AddGlEpicTable;

#[async_trait]
impl Migration for AddGlEpicTable {
    fn version(&self) -> u64 { 1 }
    fn name(&self) -> &str { "add_gl_epic_table" }
    fn migration_type(&self) -> MigrationType { MigrationType::Additive }

    async fn prepare(&self, ctx: &MigrationContext) -> Result<()> {
        ctx.execute_ddl("
            CREATE TABLE IF NOT EXISTS gl_epic (
                id Int64 CODEC(Delta(8), ZSTD(1)),
                title String DEFAULT '' CODEC(ZSTD(1)),
                -- ... columns ...
                traversal_path String DEFAULT '0/' CODEC(ZSTD(1)),
                _version DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(ZSTD(1)),
                _deleted Bool DEFAULT false,
                INDEX idx_id id TYPE bloom_filter(0.01) GRANULARITY 1,
                PROJECTION by_id (SELECT * ORDER BY id)
            ) ENGINE = ReplacingMergeTree(_version, _deleted)
            ORDER BY (traversal_path, id) PRIMARY KEY (traversal_path, id)
            SETTINGS index_granularity = 2048,
                     deduplicate_merge_projection_mode = 'rebuild',
                     allow_experimental_replacing_merge_with_cleanup = 1
        ").await
    }
}
```

The migration is registered in the registry, and on the next deployment the reconciler applies it automatically.

## Relationship to `config/graph.sql`

In V1, `config/graph.sql` continues to serve as:

- The **initial schema** for new ClickHouse instances (fresh deployments, integration tests, E2E pipelines).
- The **canonical reference** for the current full schema.

Migrations represent **deltas** from the schema defined in `config/graph.sql` at the time migration tracking begins. When a migration adds a table, the DDL should also be added to `graph.sql` so that fresh installations get the complete schema without replaying all migrations.

This is the same pattern used by Rails: `db/schema.rb` is the canonical schema, and `db/migrate/` contains the deltas.

## Acceptance criteria

1. **Migration trait and registry** — `Migration` trait defined, `MigrationRegistry` validates version ordering, central registration function exists.
2. **Migration ledger** — `gkg_migrations` table created automatically by the reconciler. Status transitions are recorded durably.
3. **Migration lock** — Reconciler acquires `migration.reconciler` lock via existing `LockService`. Only one reconciler instance runs at a time. Lock-holder eligibility is checked after acquisition.
4. **Reconciler loop** — Runs as a background task in `Indexer` mode. Applies pending additive migrations sequentially. Retries failed migrations up to a configurable maximum.
5. **NATS KV notification** — `migration.version` key updated after each completed migration.
6. **Idempotent DDL** — Migration `prepare()` uses idempotent DDL. Re-running a completed migration is a no-op.
7. **Integration tests** — Testcontainer-based tests verify: migration application, ledger updates, lock contention behavior, retry on failure.
8. **No startup coupling** — Pod readiness is unaffected by migration state. The reconciler starts asynchronously after the indexer is ready.
9. **Observability** — Metrics for: migrations applied, migrations failed, reconciler lock status, reconciler loop duration. Log lines for each phase transition.
