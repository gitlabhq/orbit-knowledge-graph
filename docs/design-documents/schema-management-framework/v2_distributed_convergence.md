# V2 — Distributed convergence

Extends V1 to support migrations that require distributed data convergence — reindexing, backfilling, or rewriting data across many scopes before a migration can be considered complete.

## Goal

Enable schema changes that affect existing indexed data. After the global DDL is applied (V1), the system tracks which scopes (namespaces, projects, branches) still need to be brought to the new schema version, and coordinates the reindex/backfill work through the existing scheduler and worker pool infrastructure.

## Problem

Some schema changes cannot be completed by DDL alone:

- **Adding a required property** that must be backfilled from the datalake for all existing entities.
- **Changing a column type** via the shadow column pattern (add new column, backfill, swap).
- **Restructuring edge data** that requires reprocessing all relationships in a namespace.
- **Adding a new entity type** that requires scanning existing datalake data to populate it.

These changes need the indexer to reprocess data for every affected scope, which may take hours or days across a large instance.

## Components

### Convergence scope tracking

A `gkg_migration_scopes` table tracks per-scope progress:

```sql
CREATE TABLE IF NOT EXISTS gkg_migration_scopes (
    migration_version UInt64,
    scope_kind        LowCardinality(String),  -- 'namespace', 'project_branch'
    scope_key         String,                   -- traversal_path or 'project_id/branch'
    target_version    UInt64,
    current_version   UInt64 DEFAULT 0,
    status            LowCardinality(String),   -- 'stale', 'converging', 'converged', 'failed'
    last_attempt_at   Nullable(DateTime64(6, 'UTC')),
    error_message     Nullable(String),
    retry_count       UInt32 DEFAULT 0,
    _version          DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (migration_version, scope_kind, scope_key)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;
```

### Extended migration trait

V2 extends the `Migration` trait with convergence methods:

```rust
#[async_trait]
pub trait Migration: Send + Sync {
    // ... V1 methods ...

    /// What kind of scopes need convergence? None for additive migrations.
    fn convergence_scopes(&self) -> Option<ConvergenceScopeKind> {
        None
    }

    /// Given a scope, perform the convergence work (reindex, backfill, etc.).
    /// Called by the indexer worker when processing a convergence task.
    async fn converge_scope(
        &self,
        ctx: &MigrationContext,
        scope: &ConvergenceScope,
    ) -> Result<()> {
        Ok(())
    }
}

pub enum ConvergenceScopeKind {
    /// All namespaces (SDLC indexing scopes).
    Namespace,
    /// All project+branch combinations (code indexing scopes).
    ProjectBranch,
    /// Both SDLC and code scopes.
    All,
}

pub struct ConvergenceScope {
    pub kind: ConvergenceScopeKind,
    pub key: String,  // traversal_path or "project_id/branch"
}
```

### Reconciler extensions

The V1 reconciler loop is extended to handle the `converging` phase:

```
for migration in registry.migrations() {
    match ledger.status(migration.version()) {
        // ... V1 cases ...

        Some(Status::Prepared) if migration.convergence_scopes().is_some() => {
            // Declare convergence targets
            let scopes = discover_scopes(migration.convergence_scopes())
            insert_scope_records(migration.version(), scopes, status: Stale)
            upsert_migration_status(migration, Status::Converging)
        }

        Some(Status::Converging) => {
            let remaining = count_non_converged_scopes(migration.version())
            if remaining == 0 {
                upsert_migration_status(migration, Status::Converged)
            }
            // Otherwise, convergence work continues via scheduler
            break // Don't advance past a converging migration
        }

        Some(Status::Converged) => {
            // V3 handles finalization; in V2, converged → completed
            upsert_migration_status(migration, Status::Completed)
        }
    }
}
```

### Scope discovery

The reconciler discovers scopes by querying existing checkpoint tables:

- **Namespace scopes**: All distinct `traversal_path` values from `checkpoint` or from a set of representative node tables.
- **Project+branch scopes**: All entries in `code_indexing_checkpoint`.

New scopes that appear after convergence targets are declared (e.g., a new namespace is onboarded) are handled by the regular indexing path — they are indexed with the current schema version from the start and do not need convergence.

### Scheduler integration

The `DispatchIndexing` mode is extended with a new `ScheduledTask`:

- **`MigrationConvergenceDispatcher`** — queries `gkg_migration_scopes` for stale scopes, publishes targeted reindex/backfill requests to NATS JetStream.

These requests are consumed by the existing indexer worker pool. The handler for convergence work:

1. Calls `migration.converge_scope()` for the given scope.
2. On success, updates the scope record in `gkg_migration_scopes` to `converged`.
3. On failure, updates with `failed` status and error message.

This reuses the existing NATS JetStream → WorkerPool → Handler pipeline. Convergence tasks are just another type of work item.

### Produced-data schema versioning

To detect stale scopes and reason about mixed-version data, GKG should track the schema version that produced indexed data at the convergence scope level:

- For SDLC: the schema version used when a namespace was last fully indexed.
- For code: the schema version used when a project+branch was last indexed.

This can be added as a column to existing checkpoint tables or tracked in a dedicated table. The exact mechanism is a detailed design question for V2 implementation.

This is inspired by Zoekt, where indexed repositories carry a `schema_version` and stale scopes are detected by comparing against the current target.

## Example: changing a column type via shadow column

Suppose `gl_merge_request.state` needs to change from `LowCardinality(String)` to an `Enum8`. This requires the shadow column migration pattern from `schema_management.md`:

```rust
pub struct MrStateToEnum;

#[async_trait]
impl Migration for MrStateToEnum {
    fn version(&self) -> u64 { 5 }
    fn name(&self) -> &str { "mr_state_to_enum" }
    fn migration_type(&self) -> MigrationType { MigrationType::Convergent }

    async fn prepare(&self, ctx: &MigrationContext) -> Result<()> {
        // Phase 1: Add shadow column
        ctx.execute_ddl("
            ALTER TABLE gl_merge_request
            ADD COLUMN IF NOT EXISTS state_v2 Enum8(
                'opened' = 1, 'closed' = 2, 'merged' = 3, 'locked' = 4
            ) DEFAULT 'opened'
        ").await
    }

    fn convergence_scopes(&self) -> Option<ConvergenceScopeKind> {
        Some(ConvergenceScopeKind::Namespace)
    }

    async fn converge_scope(
        &self,
        ctx: &MigrationContext,
        scope: &ConvergenceScope,
    ) -> Result<()> {
        // Backfill state_v2 from state for all MRs in this namespace
        ctx.execute_dml("
            INSERT INTO gl_merge_request (id, traversal_path, state_v2, _version)
            SELECT id, traversal_path,
                   CAST(state AS Enum8('opened'=1,'closed'=2,'merged'=3,'locked'=4)),
                   now64(6)
            FROM gl_merge_request FINAL
            WHERE traversal_path LIKE {scope_prefix:String}
              AND state_v2 = 'opened'  -- default value = not yet backfilled
        ", params!{ scope_prefix: format!("{}%", scope.key) }).await
    }
}
```

The reconciler:

1. Applies the `ALTER TABLE ADD COLUMN` (prepare phase).
2. Discovers all namespaces and creates scope records.
3. The scheduler dispatches convergence work per namespace.
4. Workers backfill `state_v2` for each namespace.
5. When all scopes are converged, the migration is marked converged.
6. A subsequent V3 finalization migration would swap the columns and drop the old one.

## Compatibility during convergence

During the convergence window:

- **Writes** (indexer): Must dual-write to both old and new columns. The indexer's ETL handlers check whether a convergent migration is active and write to both columns.
- **Reads** (webserver): Must read from the old column until the migration is fully converged. The query compiler can check migration status to decide which column to use.
- **New scopes**: Namespaces/projects onboarded during convergence are indexed with the latest schema from the start — no backfill needed.

## Acceptance criteria

1. **Convergence scope table** — `gkg_migration_scopes` created automatically. Records per-scope status transitions.
2. **Extended migration trait** — `convergence_scopes()` and `converge_scope()` methods available.
3. **Reconciler convergence logic** — Declares targets, monitors progress, advances to converged when all scopes complete.
4. **Scheduler integration** — `MigrationConvergenceDispatcher` dispatches stale scopes as NATS messages. Existing worker pool processes them.
5. **Scope discovery** — Scopes discovered from checkpoint tables. New scopes during convergence are handled by regular indexing.
6. **Dual-write support** — Indexer handlers can detect active convergent migrations and dual-write.
7. **Query compatibility** — Webserver/compiler can check migration status and choose the correct column/table.
8. **Progress observability** — Metrics for: total scopes, converged scopes, failed scopes, convergence lag. Dashboard-friendly.
9. **Integration tests** — End-to-end test: migration with convergence, scope dispatch, worker convergence, migration completion.
