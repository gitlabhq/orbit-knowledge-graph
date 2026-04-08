# V0 schema reset runbook

Operations guide for performing a V0 drop-and-recreate schema migration. Follow this procedure when deploying a GKG release that changes `config/graph.sql` and bumps `SCHEMA_VERSION`.

## When this procedure applies

A V0 reset is required whenever:

- `SCHEMA_VERSION` in `crates/indexer/src/schema_version.rs` is bumped
- The corresponding `config/graph.sql` has changed (new column, table, or index)

The `gkg.schema.version.mismatch` metric will be `1` after the new binary is deployed. This is expected — the reset does not happen until all namespaces are disabled.

## Overview

The V0 strategy is drop-and-recreate. All GKG-owned graph tables are emptied, the new schema is applied, and then namespaces are re-enabled to trigger a full re-index via the existing pipeline. No new re-indexing machinery is needed — re-enabling a namespace is indistinguishable from enabling it for the first time.

```plaintext
deploy new binary
  → mismatch detected (versions differ)
    → disable all namespaces (Rails admin action)
      → indexer drops + recreates tables
        → re-enable namespaces (Rails admin action)
          → existing dispatch pipeline re-indexes all data
```

## Pre-flight checklist

Before deploying:

- [ ] Verify `SCHEMA_VERSION` is bumped in `crates/indexer/src/schema_version.rs`
- [ ] Verify `config/graph.sql` changes are correct and reviewed
- [ ] Estimate re-index duration (see [Duration estimates](#duration-estimates))
- [ ] Schedule a maintenance window if re-index duration is significant
- [ ] Notify stakeholders of the downtime window

## Step-by-step procedure

### 1. Deploy the new binary

Deploy the new GKG release using the standard deployment process.

After deployment, verify the new binary is running:

```plaintext
gkg.schema.version.mismatch = 1
```

The indexer continues running normally during the mismatch. All existing data remains queryable. New data continues to be indexed against the old schema.

### 2. Disable all namespaces

In the Rails admin console or via the API, delete all rows from `knowledge_graph_enabled_namespaces`. This signals to the indexer that it is safe to proceed with the reset.

```ruby
# Rails console
KnowledgeGraph::EnabledNamespace.delete_all
```

The indexer polls every 30 seconds (configurable via `schema_version_check.interval_secs`). On the next check cycle after namespaces reach zero:

```plaintext
gkg.schema.reset.total{result="success"} += 1
gkg.schema.version.mismatch → 0
```

Monitor the `gkg.schema.reset.total` counter and the `gkg.schema.version.mismatch` gauge.

### 3. Verify the reset completed

After the reset counter increments, confirm:

- The `gkg.schema.version.mismatch` gauge is `0`
- All graph tables are empty (spot-check `SELECT count() FROM gl_user`)
- The new schema version is recorded:

```sql
SELECT version FROM gkg_schema_version FINAL ORDER BY applied_at DESC LIMIT 1;
-- should return the new SCHEMA_VERSION
```

### 4. Re-enable namespaces

Re-enable namespaces via the Rails admin UI or API. This creates fresh Siphon CDC insert events for `knowledge_graph_enabled_namespaces`, which the existing dispatch pipeline handles naturally.

```ruby
# Rails console — re-enable specific namespaces
namespace_ids.each do |id|
  KnowledgeGraph::EnabledNamespace.create!(root_namespace_id: id)
end
```

### 5. Monitor re-indexing progress

The re-index is driven by the standard dispatch pipeline:

- **SDLC data**: `NamespaceDispatcher` and `GlobalDispatcher` publish indexing requests
- **Code data**: `NamespaceCodeBackfillDispatcher` picks up the re-enable CDC events

Key metrics during re-indexing:

| Metric | What it shows |
|--------|---------------|
| `gkg.sdlc.handler.requests_processed_total` | SDLC entities indexed |
| `gkg.sdlc.dispatcher.requests_published_total` | Namespace indexing requests sent |
| `gkg.code.handler.tasks_processed_total` | Code tasks completed |

Log queries:

```plaintext
# SDLC re-indexing progress
level=info message="handler completed" handler=namespace_handler

# Code re-indexing backfill triggered
level=info message="dispatched code indexing task requests" source=namespace_backfill
```

### 6. Verify re-indexing is complete

Run spot checks to confirm data is back in the graph tables:

```sql
-- Check user count matches datalake
SELECT count() FROM gl_user FINAL WHERE _deleted = false;
SELECT count() FROM siphon_users WHERE _siphon_deleted = false;

-- Check project count
SELECT count() FROM gl_project FINAL WHERE _deleted = false;
SELECT count() FROM siphon_projects WHERE _siphon_deleted = false;
```

When counts converge, re-indexing is complete.

## Duration estimates

Re-index duration depends on data volume and indexer throughput.

| Data volume | Estimated SDLC re-index | Estimated code re-index |
|-------------|------------------------|------------------------|
| Small (< 1M entities) | < 30 minutes | 1–4 hours |
| Medium (1–10M entities) | 1–4 hours | 4–24 hours |
| Large (> 10M entities) | 4–24 hours | 1–7 days |

Code re-indexing is slower because it involves fetching repository content from Gitaly for each project and branch.

## Troubleshooting

### Reset not triggering after namespaces disabled

**Symptom**: `gkg.schema.version.mismatch` remains `1` after all namespaces are disabled.

**Check**:

1. Confirm all namespace rows are actually deleted:

   ```sql
   SELECT count() FROM siphon_knowledge_graph_enabled_namespaces WHERE _siphon_deleted = false;
   -- should return 0
   ```

2. Check the indexer logs for errors from the schema version check loop:

   ```plaintext
   level=warn message="schema version check failed"
   ```

3. Verify the `gkg.schema.version.check_loop_active` gauge is `1` (loop is running).

**Resolution**: If the count query above returns 0 but the reset still isn't triggering, the Siphon replication may have a lag. Wait for the next check cycle (30 s by default).

### Re-indexing stalled

**Symptom**: Graph table counts are not growing after namespaces are re-enabled.

**Check**:

1. Verify the DispatchIndexing pod is running and the `NamespaceDispatcher` is firing:

   ```plaintext
   level=info message="dispatched namespace indexing requests" dispatched=N
   ```

2. Verify the Indexer pod is consuming messages:

   ```plaintext
   level=info message="handler completed" handler=namespace_handler
   ```

3. Check the NATS stream backlog:

   ```plaintext
   nats stream info GKG_INDEXER
   ```

**Resolution**: If dispatch is running but the indexer is not consuming, check for NATS connectivity errors or worker pool exhaustion (`gkg.engine.active_workers`).

### Lock contention during reset

**Symptom**: Log shows `schema reset lock held by another pod — skipping this cycle` repeatedly.

**Explanation**: This is expected during rolling deploys. One pod acquires the lock and performs the reset. Others skip their cycle. The lock TTL is 120 s.

**Action**: Wait two full check intervals (60 s). If the lock is never released, check for crashed pods holding a stale lock in the `indexing_locks` NATS KV bucket:

```shell
nats kv get indexing_locks schema_reset
nats kv del indexing_locks schema_reset  # manual release if pod crashed mid-reset
```

### Data missing after re-index

**Symptom**: Graph counts are lower than datalake counts after re-indexing completes.

**Possible causes**:

- Checkpoint watermarks were written during re-indexing so some rows were skipped as "already indexed"
- Namespace not re-enabled before re-indexing started

**Resolution**:

1. Clear the affected checkpoints to force a re-scan:

   ```sql
   -- Clear all namespace checkpoints for namespace 100
   INSERT INTO checkpoint (key, watermark, cursor_values, _deleted)
   SELECT key, argMax(watermark, _version), argMax(cursor_values, _version), true
   FROM checkpoint
   WHERE startsWith(key, 'ns.100.')
   GROUP BY key
   HAVING argMax(_deleted, _version) = false;
   ```

2. Wait for the next dispatch cycle — the handler will re-index from epoch-zero.
