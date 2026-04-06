# V2 — Distributed convergence

> **Design maturity: directional.** This phase describes the target architecture for distributed convergence. The high-level model (scope tracking, scheduler integration, compatibility modes) is considered sound. However, several areas require a follow-up design pass before implementation: scope discovery completeness, the convergence correctness model (when exactly is a scope "converged"?), code vs SDLC interaction specifics, and duplicate-dispatch/retry semantics. These are tracked in the [open questions](README.md#still-open).

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

Tracking the schema version that produced indexed data is a **core requirement** for V2 — not an optional follow-up. Without it, the system cannot reliably detect stale scopes, reason about mixed-version data, or determine when convergence is complete.

**Design.** A `schema_version UInt64` column is added to the existing checkpoint tables:

```sql
-- SDLC checkpoint (existing table, new column)
ALTER TABLE checkpoint
ADD COLUMN IF NOT EXISTS schema_version UInt64 DEFAULT 0;

-- Code indexing checkpoint (existing table, new column)
ALTER TABLE code_indexing_checkpoint
ADD COLUMN IF NOT EXISTS schema_version UInt64 DEFAULT 0;
```

The indexer writes the current target schema version (highest completed migration version) into the checkpoint whenever it completes indexing work for a scope. This happens as part of the normal indexing write path — no separate write is needed.

**Stale scope detection.** A scope is stale when its checkpoint `schema_version` is less than the target version declared by a convergent migration. The reconciler discovers stale scopes by querying:

```sql
-- Find SDLC scopes that need convergence for migration version 5
SELECT DISTINCT key AS scope_key
FROM checkpoint FINAL
WHERE schema_version < {target_version:UInt64};

-- Find code scopes that need convergence
SELECT traversal_path, project_id, branch
FROM code_indexing_checkpoint FINAL
WHERE schema_version < {target_version:UInt64};
```

**New scopes.** Scopes that are first indexed after a migration is declared are written with the current target schema version from the start. They never appear as stale and require no convergence.

**Unit of correctness.** A scope is considered "converged" when **both** conditions are met:

1. The migration-specific `converge_scope()` backfill has completed (recorded in `gkg_migration_scopes`).
2. The scope's checkpoint `schema_version` has been updated to the target version.

The `converge_scope()` implementation is responsible for updating the checkpoint's `schema_version` as its final step. This ensures that a scope is only marked converged when the data has actually been produced at the target version.

This design is inspired by Zoekt, where indexed repositories carry a `schema_version` and stale scopes are detected by comparing against the current target.

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
        // Add shadow column with a Nullable type — NULL means "not yet backfilled"
        ctx.execute_ddl("
            ALTER TABLE gl_merge_request
            ADD COLUMN IF NOT EXISTS state_v2 Nullable(Enum8(
                'opened' = 1, 'closed' = 2, 'merged' = 3, 'locked' = 4
            ))
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
        // Backfill state_v2 from state for all MRs in this namespace.
        // Uses Nullable — NULL means not yet backfilled (avoids sentinel value confusion).
        // This operation is idempotent: re-running it for already-backfilled rows
        // produces the same result because ReplacingMergeTree deduplicates by
        // ORDER BY key, and the new _version supersedes the old row.
        ctx.execute_dml("
            INSERT INTO gl_merge_request (id, traversal_path, state_v2, _version)
            SELECT id, traversal_path,
                   CAST(state AS Enum8('opened'=1,'closed'=2,'merged'=3,'locked'=4)),
                   now64(6)
            FROM gl_merge_request FINAL
            WHERE traversal_path LIKE {scope_prefix:String}
              AND state_v2 IS NULL
        ", params!{ scope_prefix: format!("{}%", scope.key) }).await?;

        // Update the checkpoint schema_version to mark this scope as converged
        ctx.update_scope_schema_version(&scope, 5).await
    }
}
```

**Idempotence invariant.** Convergence operations must be safe to re-run. This example achieves idempotence through:

- **Nullable for "not yet backfilled"**: Using `Nullable` with `IS NULL` instead of a default sentinel value avoids confusing real data values (like `'opened'`) with "not yet migrated" state.
- **ReplacingMergeTree deduplication**: Re-inserting a row with the same ORDER BY key but a newer `_version` is a no-op after the next merge.
- **Scope-level tracking**: The `gkg_migration_scopes` table records whether a scope's convergence has been attempted, so the scheduler does not re-dispatch already-converged scopes.

The reconciler:

1. Applies the `ALTER TABLE ADD COLUMN` (prepare phase).
2. Discovers all namespaces and creates scope records.
3. The scheduler dispatches convergence work per namespace.
4. Workers backfill `state_v2` for each namespace and update the checkpoint `schema_version`.
5. When all scopes are converged, the migration is marked converged.
6. A subsequent V3 finalization migration would swap the columns and drop the old one.

## Runtime compatibility contract

During the convergence window, both writers (indexer) and readers (webserver) need to know what behavior is expected. The **authoritative signal** is the migration status in the `gkg_migrations` table.

### Compatibility modes

Each convergent migration defines a `CompatibilityMode` that the runtime consults:

```rust
pub enum CompatibilityMode {
    /// Migration not yet started — use old schema only
    Legacy,
    /// Migration in progress (preparing/converging) — dual-write, read old
    DualWrite,
    /// Migration converged — dual-write, can read new
    ReadNew,
    /// Migration finalized — use new schema only
    NewOnly,
}
```

The migration framework exposes a query interface for runtime components:

```rust
/// Returns the current compatibility mode for a given migration.
/// Cached in-memory, refreshed on NATS KV `migration.version` notification.
fn compatibility_mode(migration_version: u64) -> CompatibilityMode;
```

### Writer behavior (indexer)

The indexer's ETL handlers query the compatibility mode to determine write behavior:

| Mode | Writer behavior |
|---|---|
| `Legacy` | Write to old columns/tables only |
| `DualWrite` | Write to both old and new columns/tables |
| `ReadNew` | Write to both (maintaining backward compatibility until finalization) |
| `NewOnly` | Write to new columns/tables only (post-finalization) |

Dual-write is activated when the reconciler transitions a migration to `preparing` or `converging` status. The indexer does not need to know which specific scopes are converged — it always dual-writes while the migration is active.

### Reader behavior (webserver)

The query compiler checks the compatibility mode to determine which column or table to reference:

| Mode | Reader behavior |
|---|---|
| `Legacy` | Query old columns/tables |
| `DualWrite` | Query old columns/tables (new data is incomplete) |
| `ReadNew` | Query new columns/tables (all data is at target version) |
| `NewOnly` | Query new columns/tables only |

The webserver refreshes its cached compatibility state when it receives a NATS KV `migration.version` notification. Between refreshes, it operates on stale-but-safe state (reading from old columns when new columns are already available is always safe; reading from new columns before convergence completes is the only unsafe direction).

### New scopes

Namespaces/projects onboarded during convergence are indexed with the current target schema version from the start. The indexer dual-writes as usual, and the new scope's checkpoint `schema_version` is set to the target version immediately. No convergence backfill is needed for these scopes.

### Watermark and checkpoint interaction

The existing SDLC indexing pipeline uses cursor-based keyset pagination with watermarks stored in the `checkpoint` table (see [!446](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/446)). Code indexing uses `code_indexing_checkpoint` with `last_task_id` and `last_commit` (see [!564](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/564)). Migration convergence must interact correctly with these checkpoints.

**Key invariant**: convergence backfill must not disrupt normal indexing watermarks. The backfill and normal indexing serve different purposes:

- **Normal indexing** processes *new* CDC events from Siphon (SDLC) or new code pushes (code). Its watermark tracks "how far through the event stream have I processed?"
- **Convergence backfill** re-processes *existing* data that was indexed at an older schema version. It does not consume new events — it re-reads data that was already ingested and rewrites it to the new schema.

**SDLC convergence**: The backfill reads from the ClickHouse datalake (already-ingested Siphon data) and writes to graph tables. It does **not** reset or modify the SDLC handler's watermark in the `checkpoint` table. The watermark continues to track CDC stream position independently. The backfill tracks its own progress via the `gkg_migration_scopes` table and updates the checkpoint's `schema_version` column on completion.

**Code convergence**: Re-indexing a project+branch requires downloading the repository archive from Rails and re-running the code parser. This is the same operation as a normal code indexing task. The convergence dispatcher publishes `CodeIndexingTaskRequest` messages (the same message type as normal code indexing). The existing handler processes them, and on completion the `code_indexing_checkpoint` row is updated with the new `last_commit` and `schema_version`. This means code convergence **does** update the code checkpoint — but this is correct because it produces a fresh, complete index of the project at the new schema version.

**Dual-write and watermarks**: When the indexer dual-writes during a convergent migration, both old and new columns are written in the same `INSERT` batch. This does not affect watermark progression — the watermark advances normally because the same number of events are processed. The additional columns are simply extra data in the same write.

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
