# Operational model

Cross-cutting concerns for the schema migration framework: failure handling, observability, deployment integration, and operational procedures.

## Failure handling

### Migration failure

When a migration's `prepare()`, `converge_scope()`, or `finalize()` fails:

1. The failure is recorded in `gkg_migrations` with status `failed`, the error message, and an incremented `retry_count`.
2. The reconciler retries on the next loop iteration, up to a configurable maximum (e.g., 5 retries).
3. After max retries, the reconciler stops advancing past the failed migration and emits an alert-level log and metric.
4. Operator intervention is required: fix the underlying issue, then either reset the migration status to `pending` (to retry from scratch) or manually mark it as `completed` (to skip).

**Key property**: because migration state is durable in ClickHouse and the reconciler is reconciliation-based, recovery is a continuation problem — the system picks up where it left off.

### Scope convergence failure (V2)

When a scope fails to converge:

1. The scope record in `gkg_migration_scopes` is updated to `failed` with the error message.
2. The scheduler continues dispatching other stale scopes — one scope failure does not block others.
3. Failed scopes are retried on subsequent scheduler runs, up to a per-scope retry limit.
4. The migration remains in `converging` status until all scopes are either `converged` or permanently failed.
5. If too many scopes fail (configurable threshold), the reconciler emits an alert and pauses dispatching.

**Failure classification** is an open design question (see [open questions](README.md#still-open)). A simple initial approach: all scope failures are treated as transient and retried up to a per-scope limit. After exhausting retries, a scope is marked `permanently_failed` and excluded from the convergence count. The migration can still complete if the remaining scopes converge, but the permanently failed scopes are surfaced in metrics and logs for operator attention.

### Reconciler crash

If the reconciler process crashes:

1. The NATS KV migration lock lease expires (TTL-based).
2. Another eligible indexer instance acquires the lock on its next reconciler loop iteration.
3. The new lock-holder reads the current migration state from ClickHouse and continues from the last durable state.
4. In-flight DDL that was not recorded as completed is re-attempted (this is why idempotent DDL is required).

### ClickHouse unavailability

If ClickHouse is temporarily unavailable:

1. The reconciler cannot read the ledger or apply DDL. It logs the error and retries on the next loop iteration.
2. The indexer's existing health check (`/ready`) will report ClickHouse as unhealthy, which prevents K8s from routing traffic.
3. Migration state is not corrupted — the reconciler simply cannot make progress until ClickHouse is back.

## Deployment integration

### Rolling deployments

The migration framework is designed for rolling deployments where old and new binary versions coexist:

1. **New binary deploys** with migrations registered in its registry.
2. **Old pods** continue running — they do not know about the new migrations but can operate against the existing schema.
3. **New pod acquires migration lock** and applies additive DDL (V1). The DDL is backward-compatible (new columns have defaults, new tables don't affect old queries).
4. **Old pods continue operating** against the updated schema. `CREATE TABLE IF NOT EXISTS` and `ADD COLUMN IF NOT EXISTS` are safe for old readers/writers.
5. **Convergent migrations** (V2) start dual-write in new pods. Old pods write to old columns only — this is acceptable because convergence backfills the new columns.
6. **All pods updated** — dual-write is now universal. Convergence proceeds.

**Compatibility determination.** The reconciler determines compatibility by comparing its migration registry against the persisted ledger:

- An instance is **eligible to drive migrations** if its registry contains the migration currently in a non-terminal state (`preparing`, `converging`, `failed`) or the next pending migration.
- An instance is **compatible but passive** if its registry does not contain any migrations beyond those already completed. It can safely operate (read/write graph data) but should not hold the migration lock.
- An instance is **incompatible** if the ledger contains completed migrations that are not in its registry (i.e., it is an older binary than expected). This should not happen in normal rolling deployments but would indicate a problematic rollback. The instance logs a warning and continues operating — it does not attempt to acquire the migration lock.

This means the framework supports **one migration version ahead** during rolling deployments. Multi-hop upgrades (skipping releases) are supported as long as each intermediate migration's DDL is idempotent and backward-compatible.

### Rollback

Rolling back the application binary is supported:

- **Additive migrations**: Old binary ignores new tables/columns. No harm.
- **Convergent migrations in progress**: Old binary writes to old columns only. Convergence pauses because the old binary does not hold the migration lock (eligibility check). When the new binary is re-deployed, convergence resumes.
- **After finalization**: Rollback is unsafe if destructive DDL was applied. This is why finalization is a separate, explicit step with optional manual approval.

**Reversing schema state is not automatic.** The migration model optimizes for forward-only expand/migrate/contract flows with safe pause/resume. Explicit rollback migrations can be defined for cases where a safe reversal path exists.

### Fresh installations

For a fresh ClickHouse instance (new deployment, integration test, E2E):

1. `config/graph.sql` is applied first — this creates the full current schema.
2. The reconciler starts and bootstraps the `gkg_migrations` table.
3. The reconciler compares the registry against the empty ledger.
4. For migrations whose DDL is already covered by `graph.sql` (e.g., `CREATE TABLE IF NOT EXISTS`), the idempotent DDL is a no-op and the migration is marked `completed`.
5. Any migrations that go beyond `graph.sql` (e.g., new columns added after the last `graph.sql` update) are applied normally.

This means `config/graph.sql` should always reflect the latest desired schema, and migrations should always use idempotent DDL.

## Observability

### Metrics

The migration framework should expose Prometheus metrics for monitoring and alerting:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `gkg_migration_status` | Gauge | `version`, `name`, `status` | Current status of each migration (encoded as int) |
| `gkg_migration_applied_total` | Counter | `version`, `name`, `result` | Migrations applied (success/failure) |
| `gkg_migration_prepare_duration_seconds` | Histogram | `version` | Time spent in prepare() |
| `gkg_migration_reconciler_lock_held` | Gauge | | 1 if this instance holds the migration lock |
| `gkg_migration_reconciler_loop_duration_seconds` | Histogram | | Duration of each reconciler loop iteration |
| `gkg_migration_scopes_total` | Gauge | `version`, `status` | Count of convergence scopes by status (V2) |
| `gkg_migration_scopes_converged_ratio` | Gauge | `version` | Fraction of scopes converged (V2) |
| `gkg_migration_finalization_pending` | Gauge | `version` | 1 if a migration is waiting for finalization approval (V3) |
| `gkg_migration_desired_version` | Gauge | | Highest migration version in the registry (target state) |
| `gkg_migration_current_version` | Gauge | | Highest completed migration version (actual state) |
| `gkg_migration_oldest_stale_scope_age_seconds` | Gauge | `version` | Age of the oldest unconverged scope (V2) |
| `gkg_migration_blocking_release` | Gauge | `version` | 1 if a migration is in a non-terminal state (blocks next release) |

### Logging

Structured log lines (via `tracing`) for each lifecycle transition:

```
INFO migration.reconciler: migration phase transition version=1 name="add_gl_epic_table" from="pending" to="preparing"
INFO migration.reconciler: migration completed version=1 name="add_gl_epic_table" duration_ms=42
WARN migration.reconciler: migration failed version=2 name="add_state_v2_column" error="timeout" retry_count=1
INFO migration.convergence: scope converged version=2 scope_kind="namespace" scope_key="123/" remaining=47
```

### Health endpoint extension

The `/ready` endpoint should **not** gate on migration state — pod readiness is infrastructure-level. However, a new informational endpoint could expose migration status:

| Endpoint | Response |
|---|---|
| `GET /migrations` | JSON array of migration statuses from `gkg_migrations` |
| `GET /migrations/:version` | Detailed status including scope convergence progress (V2) |

These are informational only — used by operators and dashboards, not by K8s probes.

## Operational procedures

### Inspecting migration state

```sql
-- Current migration status
SELECT version, name, status, started_at, completed_at, retry_count, error_message
FROM gkg_migrations FINAL
ORDER BY version;

-- Convergence progress for a specific migration (V2)
SELECT
    status,
    count() as scope_count
FROM gkg_migration_scopes FINAL
WHERE migration_version = 5
GROUP BY status;

-- Failed scopes (V2)
SELECT scope_kind, scope_key, error_message, retry_count, last_attempt_at
FROM gkg_migration_scopes FINAL
WHERE migration_version = 5 AND status = 'failed'
ORDER BY last_attempt_at DESC;
```

### Manual intervention

**Skip a failed migration** (use with caution):

```sql
INSERT INTO gkg_migrations (version, name, migration_type, status, completed_at)
VALUES (2, 'add_state_v2_column', 'additive', 'completed', now64(6));
```

**Reset a migration for retry**:

```sql
INSERT INTO gkg_migrations (version, name, migration_type, status, retry_count)
VALUES (2, 'add_state_v2_column', 'additive', 'pending', 0);
```

**Approve manual finalization** (V3):

The approval mechanism (NATS KV key or ClickHouse record) is a V3 implementation detail. The operator sets a flag that the reconciler checks before proceeding.

### Runbook checklist

For operators during deployments involving schema migrations:

1. **Pre-deploy**: Check current migration state (`SELECT ... FROM gkg_migrations FINAL`). Verify no failed migrations.
2. **Deploy**: Roll out new binary. The reconciler will acquire the lock and apply pending migrations.
3. **Monitor**: Watch migration metrics and logs. Confirm additive migrations complete quickly.
4. **Convergence** (V2): Monitor scope progress. Expected duration depends on data volume. Alert if stale scope count is not decreasing.
5. **Finalization** (V3): After convergence and soak period, approve finalization if manual approval is required. Monitor finalization completion.
6. **Post-deploy**: Verify all migrations are `completed`. Check application health.
