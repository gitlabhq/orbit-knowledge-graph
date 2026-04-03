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

**Status values in V1**: `pending`, `preparing`, `completed`, `failed`. The `prepared` status is not used in V1 — additive migrations go directly from `preparing` to `completed`. The `prepared` → `converging` transition is introduced in V2 for convergent migrations.

This table is bootstrapped by the reconciler using `CREATE TABLE IF NOT EXISTS` — the one piece of DDL that runs outside the migration framework. This is safe because `CREATE TABLE IF NOT EXISTS` is idempotent and metadata-only in ClickHouse.

### Control-plane table semantics

The `gkg_migrations` table (and `gkg_migration_scopes` in V2) use `ReplacingMergeTree`, which provides **eventual deduplication** rather than transactional row updates. This is workable for migration control state, but requires explicit conventions:

**Single-writer guarantee.** All writes to `gkg_migrations` are serialized through the migration reconciler, which holds an exclusive NATS KV lock. There is never concurrent write contention on these rows. Scope status updates in V2 may come from multiple workers, but each worker writes to a distinct `(migration_version, scope_kind, scope_key)` row — no two workers update the same scope simultaneously (guaranteed by the existing per-scope NATS locking pattern).

**Read semantics.** The reconciler reads current state using `SELECT ... FROM gkg_migrations FINAL ORDER BY version`. The `FINAL` modifier is acceptable here because:

- The table is tiny (tens to low hundreds of rows — one per migration).
- Reads happen once per reconciler loop iteration (every ~30s), not on the hot path.
- The reconciler is the only component that makes decisions based on this data.

For V2's `gkg_migration_scopes`, which may have more rows (thousands of scopes), the reconciler uses aggregate queries rather than full table scans:

```sql
-- Count non-converged scopes (efficient even without FINAL on large tables)
SELECT count() FROM gkg_migration_scopes FINAL
WHERE migration_version = {version:UInt64} AND status != 'converged';
```

If `FINAL` performance becomes a concern at scale, an `argMax`-style projection can be added:

```sql
PROJECTION latest_status (
    SELECT migration_version, scope_kind, scope_key,
           argMax(status, _version) AS status,
           argMax(retry_count, _version) AS retry_count
    GROUP BY migration_version, scope_kind, scope_key
)
```

**Version column semantics.** The `_version` column uses `now64(6)` (wall-clock microseconds). This is safe because:

- The reconciler is single-writer, so there are no concurrent conflicting writes to the same row.
- Wall-clock monotonicity within a single process is sufficient; cross-process ordering is guaranteed by the NATS lock (only one writer at a time).
- For V2 scope updates from workers, each worker updates a distinct row, so `_version` ordering only matters within a single scope's history.

**Manual intervention.** The operational procedures use direct `INSERT` statements to override migration state. Because `ReplacingMergeTree` keeps the row with the highest `_version`, a new insert with `now64(6)` will always supersede previous state after the next merge. Operators should verify the result with `SELECT ... FINAL` after insertion.

### Migration lock (NATS KV)

The reconciler acquires an exclusive lock before advancing migration state, reusing the existing `indexing_locks` NATS KV bucket:

| Key | Purpose | TTL |
|---|---|---|
| `migration.reconciler` | Exclusive lock for the reconciler loop | ~60s |

**Lock semantics.** The migration lock provides **leader election**, not strong mutual exclusion with fencing. This is sufficient because all lock-protected operations (DDL application, ledger writes) are designed to be **idempotent** — if a former lock-holder's stale operation completes after the lease expires and a new holder takes over, the duplicate operation is a no-op.

**Acquisition and refresh.** The current `LockService` trait supports only `try_acquire` (create-only with TTL) and `release` (delete). For the migration reconciler, we extend this with a **lease refresh** operation that updates the existing key's value with a new TTL using NATS KV's `update` API (compare-and-swap on the key revision). The reconciler:

1. Acquires the lock via `try_acquire("migration.reconciler", ttl=60s)`.
2. Stores the NATS KV key revision returned on successful create.
3. Before each migration step, refreshes the lease by calling `kv_put` with `expected_revision` (optimistic concurrency) and a new TTL. The existing `KvPutOptions` already supports `expected_revision`.
4. If the refresh fails with `RevisionMismatch`, another instance has taken over — the reconciler stops and re-enters the acquisition loop.

This means a reconciler that loses its lease **discovers this before its next DDL operation**, rather than racing blindly.

**Long DDL safety.** ClickHouse additive DDL (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ADD COLUMN IF NOT EXISTS`) is metadata-only and completes in milliseconds. The 60s TTL is more than sufficient. If a future migration requires longer operations, the reconciler should break them into sub-steps with lease refreshes between each step.

**No fencing tokens.** We do not implement fencing tokens (e.g., writing the lock revision into ClickHouse rows). This is a deliberate simplicity trade-off: the idempotency requirement on all DDL operations makes fencing unnecessary for V1. If V2/V3 introduce non-idempotent operations, fencing should be revisited.

**Lock-holder eligibility.** After acquiring the lock, the instance compares its migration registry against the persisted ledger. If it cannot advance any migration (e.g., an older binary during rolling deployment), it releases the lock. This ensures the newest compatible binary drives migration progression during mixed-version windows. "Compatible" means: the instance's registry contains at least one migration whose version is greater than the highest completed version in the ledger, or it contains the migration currently in `preparing`/`failed` state.

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

### Authoring contract

To prevent drift between `config/graph.sql` and the migration registry, the following rules apply:

1. **Every schema-changing MR must update both.** If a migration adds a table or column, the same MR must add the corresponding DDL to `config/graph.sql`. If only `graph.sql` is updated without a migration, existing installations will not receive the change.
2. **`graph.sql` is always the complete current schema.** A fresh install using only `graph.sql` must produce the same schema as an existing install that has replayed all migrations.
3. **Migrations must use idempotent DDL.** Because fresh installs apply `graph.sql` first (which already includes the migration's DDL), the migration's `prepare()` must be a no-op if the DDL was already applied. Use `IF NOT EXISTS` / `IF EXISTS` clauses.

### CI enforcement

A CI check validates that `config/graph.sql` and the migration registry are consistent:

- **Fresh-install compatibility test**: Apply `graph.sql` to an empty ClickHouse, then run all migrations — every migration should succeed (idempotent DDL means no-ops for already-applied DDL).
- **Migration-only compatibility test**: Apply only migrations (without `graph.sql`) to an empty ClickHouse — the resulting schema should match what `graph.sql` produces. This can be verified by comparing `SHOW CREATE TABLE` output for all graph tables.

These tests run as part of the existing integration test suite using testcontainers. The exact implementation (e.g., a dedicated test case in `integration-tests`) is a V1 implementation detail.

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
