# V3 — Finalization

Adds explicit support for cleaning up after convergent migrations: dropping deprecated columns, removing shadow structures, stopping dual-write, and other destructive changes that are only safe after full convergence.

## Goal

Provide a safe, policy-driven mechanism for completing the expand/migrate/contract lifecycle. Finalization is the "contract" step — it removes old structures and compatibility code that are no longer needed after a convergent migration has completed.

## Why a separate phase

Finalization is intentionally delayed and separated from convergence because:

1. **Rollback safety.** After convergence completes, operators may want a soak period before removing the old structures. If a problem is discovered, the old columns/tables are still available.
2. **Mixed-version windows.** During rolling deployments, older binaries may still be reading from old structures. Finalization should only happen after all pods are running the new binary.
3. **Destructive operations.** Dropping columns, renaming structures, and stopping dual-write are irreversible. They deserve explicit lifecycle tracking and observability.

## Components

### Extended migration trait

V3 adds the `finalize()` method to the migration trait:

```rust
#[async_trait]
pub trait Migration: Send + Sync {
    // ... V1 + V2 methods ...

    /// Cleanup after convergence. Only called after status reaches 'converged'.
    /// Examples: drop old columns, rename shadow columns, stop dual-write.
    async fn finalize(&self, ctx: &MigrationContext) -> Result<()> {
        Ok(())  // Default: no finalization needed
    }

    /// Whether finalization requires explicit operator approval.
    /// When true, the reconciler will not auto-finalize — it waits for
    /// an operator to mark the migration as ready for finalization.
    fn requires_manual_finalization(&self) -> bool {
        false
    }
}
```

### Finalization migrations

A finalization migration is a standalone migration that only performs cleanup. It references the convergent migration it cleans up after:

```rust
pub struct MrStateDropOldColumn;

#[async_trait]
impl Migration for MrStateDropOldColumn {
    fn version(&self) -> u64 { 6 }
    fn name(&self) -> &str { "mr_state_drop_old_column" }
    fn migration_type(&self) -> MigrationType { MigrationType::Finalization }

    /// Precondition: migration version 5 (mr_state_to_enum) must be converged.
    fn depends_on_converged(&self) -> Option<u64> {
        Some(5)
    }

    async fn prepare(&self, ctx: &MigrationContext) -> Result<()> {
        // Rename shadow column to canonical name, drop old column
        ctx.execute_ddl("
            ALTER TABLE gl_merge_request
                RENAME COLUMN state TO state_old,
                RENAME COLUMN state_v2 TO state
        ").await?;

        ctx.execute_ddl("
            ALTER TABLE gl_merge_request
                DROP COLUMN IF EXISTS state_old
        ").await
    }
}
```

### Reconciler finalization logic

**Standalone finalization migrations are the standard path.** This is the preferred model because it naturally creates a release boundary and soak window between convergence and cleanup. The finalization migration ships in a separate deployment from the convergent migration, giving operators explicit control over timing.

The reconciler checks the `depends_on_converged()` precondition before applying a finalization migration. If the referenced convergent migration is not yet fully converged, the finalization migration blocks:

```
Some(Status::Pending) if migration.migration_type() == Finalization => {
    let required = migration.depends_on_converged()
        .expect("finalization migrations must declare dependency");
    if ledger.status(required) != Some(Status::Converged | Status::Completed) {
        break  // Blocked — prerequisite not met
    }
    // Proceed with normal preparing → completed flow
}
```

**Inline finalization is available but discouraged.** For simple cases (e.g., dropping an unused index after an additive migration), a convergent migration may implement `finalize()` directly. When present, the reconciler calls it after convergence completes. However, because this removes the soak window, it should be reserved for low-risk operations and requires explicit opt-in via `fn has_inline_finalization(&self) -> bool`.

### Deployment-version precondition

A finalization migration is only safe when **all serving binaries understand the post-cutover schema**. The reconciler cannot verify this directly (it does not know what binary versions are running on other pods). Instead, the safety guarantee comes from the deployment model:

1. The finalization migration is added in a new release.
2. The release is deployed via rolling update — all pods are updated before the reconciler runs.
3. The reconciler applies the finalization migration only after acquiring the lock (which means a new-version pod is running).

For extra safety, a finalization migration can implement `requires_manual_finalization() -> bool` (defined on the `Migration` trait). When true, the reconciler waits for an operator to set an approval flag (a ClickHouse record in `gkg_migrations` with a dedicated `approved_for_finalization` status) before proceeding. This is recommended for destructive operations like column drops or table renames.

## Example: completing the MR state enum migration

Continuing the V2 example where `gl_merge_request.state` was migrated from `LowCardinality(String)` to `Enum8` via a shadow column:

| Migration | Version | Type | Action |
|---|---|---|---|
| `MrStateToEnum` | 5 | Convergent | Add `state_v2` column, backfill per namespace |
| `MrStateDropOldColumn` | 6 | Finalization | Rename `state_v2 → state`, drop old `state` |

The timeline:

1. **Deploy V5 code**: Indexer dual-writes `state` and `state_v2`. Webserver reads `state`.
2. **V5 converges**: All namespaces backfilled. Migration 5 status → `converged`.
3. **Deploy V6 code**: Contains the finalization migration. Indexer still dual-writes.
4. **V6 runs**: Reconciler checks that V5 is converged, then applies the column rename/drop. Migration 6 status → `completed`.
5. **Post-V6**: Indexer writes only to `state` (now the Enum8 column). Webserver reads `state` (now Enum8).

The finalization migration is a separate deployment step, giving operators a soak period between convergence and cleanup.

## Safety considerations

- **No automatic destructive DDL without convergence.** The reconciler never runs a finalization migration unless all convergence prerequisites are met.
- **Column drops are deferred.** Even in the finalization step, consider using a two-phase approach: first rename the old column (e.g., `state_old`), then drop it in a subsequent migration. This provides an additional rollback window.
- **Rollback during finalization.** If finalization fails, the migration is marked `failed` and the old structures remain. The reconciler retries, or the operator intervenes.
- **No implicit finalization.** A convergent migration does not auto-finalize unless the author explicitly implements `finalize()` or sets auto-finalize policy.

## Acceptance criteria

1. **Finalization trait method** — `finalize()` and `requires_manual_finalization()` available on the `Migration` trait with sensible defaults.
2. **Standalone finalization migrations** — `MigrationType::Finalization` with `depends_on_converged()` precondition checking.
3. **Manual finalization gate** — Operator approval mechanism (NATS KV key or ClickHouse record) that the reconciler checks before auto-finalizing.
4. **Lifecycle tracking** — `finalizing` status tracked in `gkg_migrations` with started/completed timestamps.
5. **Safety checks** — Reconciler refuses to finalize unless all convergence prerequisites are met. Logged clearly when finalization is blocked.
6. **Integration tests** — End-to-end test: convergent migration completes, finalization migration runs, old structures removed, data integrity verified.
7. **Documentation** — Migration author guide covering: when to use inline vs. standalone finalization, how to set finalization policy, rollback considerations.
