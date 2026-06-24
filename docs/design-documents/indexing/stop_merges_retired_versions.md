# PRD: Stop background merges on retired schema versions at cutover

Status: ready for implementation. Owner: handoff to implementing agent.
Branch already created: `michaelusa/stop-merges-on-retire` (off `main`).

## Summary

When a schema migration completes and the old version is marked `retired`, the
old version's tables keep getting picked up by ClickHouse's background merge
scheduler. Those merges do no useful work (the retired version serves no queries
and receives no writes) but consume the cluster's limited big-merge capacity,
starving the new active version. Fix: when a version is retired at cutover, stop
background merges on its tables. Keep them readable for the rollback window;
existing retention logic still drops them later.

## Problem and evidence

The merge scheduler has no concept of "retired". It sees mergeable parts on any
table and merges them. Measured on prod (orbit-prod, ClickHouse Cloud,
SharedMergeTree, 10 replicas), captured 2026-06-09:

- v58 is `active` (serving queries), v57 is `retired`, v56 is `dropped`.
- v57 still occupies **297 parts / 476 GiB** on disk.
- A single **50 GiB merge on `v57_gl_ci_edge` ran for 7.8 hours** (~1.86 MiB/s,
  2.4B rows, vertical merge + projection rebuild) while it was the cluster's one
  in-flight big merge.
- v58 (the version that matters) had only 13 tiny CDC merges (132 KiB total)
  running at the same time.

So the big-merge slot was held ~8 hours by a retired version. The graph tables
themselves are otherwise healthy (single-digit to low-20s part counts); this is
purely wasted allocation, not a fragmentation problem.

Background context on why big merges are slow (so the implementer understands the
stakes): these tables carry ~9 projections each under
`deduplicate_merge_projection_mode='rebuild'`, so every merge re-materializes all
projections. That makes each big merge expensive and the merge pool precious.
Reserving it for the active version is the goal.

## Goals

- At cutover (old active to `retired`), stop background merges on the retired
  version's graph tables.
- Reversible: a rollback that re-promotes a retired version must resume merges.
- Best-effort: failing to stop merges must never block or fail promotion.
- Idempotent: also stop merges for any already-`retired`-not-`dropped` version on
  the next run, so this fixes the current v57 state, not only future cutovers.

## Non-goals

- Speeding up individual merges (the 1.86 MiB/s throughput / projection-rebuild
  cost is a separate concern).
- Changing the retention window or drop timing (existing `drop_version_tables`
  retention logic is unchanged).
- The v56 "dropped but 4 orphan parts / 5.21 GiB still on disk" issue is a
  separate cleanup bug; note it but do not fix it here.

## Where the change goes

`crates/indexer/src/schema/completion.rs`, in `MigrationCompletionChecker`. All
references below are by symbol because exact line numbers drift.

- The cutover happens in the `promote`-style method: a loop over
  `read_all_versions` marks any `status == "active" && version != migrating` as
  retired via `mark_version_retired(...)` (around line 233-243), then marks the
  migrating version active. Add the stop-merges call immediately after the
  `mark_version_retired` call for that entry.
- Mirror the existing `drop_version_tables(version)` method for table
  enumeration: it uses `table_prefix(version)` and
  `generate_graph_tables(&self.ontology)` to get `{prefix}{table_name}`. Add a
  sibling `stop_merges_for_version(version)` that enumerates the same graph
  tables (tables only; views and dictionaries do not merge) and issues the stop
  per table.
- For the idempotent path: in the retention/cleanup pass that iterates retired
  versions, ensure merges are stopped for any retired-not-dropped version too
  (cheap and idempotent: stopping merges on a table with merges already stopped
  is a no-op).

## The one thing that is easy to get wrong: `SYSTEM STOP MERGES` is node-local

`DROP TABLE` in `drop_version_tables` works with an unqualified table name and no
`ON CLUSTER` because it is metadata DDL that auto-replicates across the shared
catalog. **`SYSTEM STOP MERGES` is different: it is node-local runtime server
state, not replicated DDL.** The indexer connects through the load-balanced Cloud
endpoint, so a plain `SYSTEM STOP MERGES <table>` would stop merges on exactly
one of the 10 replicas and the other 9 would keep merging. This is the most
likely way to ship a fix that silently does nothing.

### Recommended approach (verify on staging/dev first)

Issue it cluster-wide:

```sql
SYSTEM STOP MERGES ON CLUSTER 'default' <prefixed_table>
```

The cluster name on this service is `default` (10 replicas), confirmed via
`system.clusters`. `ON CLUSTER` propagates the initiator's default database, the
same reason the existing unqualified `DROP` resolves correctly, so the table can
stay unqualified.

The corresponding rollback (resume) statement, for the un-retire path:

```sql
SYSTEM START MERGES ON CLUSTER 'default' <prefixed_table>
```

### Fallback approach if `ON CLUSTER` is restricted on this Cloud service

Some Cloud configurations restrict `ON CLUSTER`. If verification shows it does
not propagate, use a per-table `ALTER ... MODIFY SETTING`, which auto-replicates
like DDL (no `ON CLUSTER` needed) and is reversible:

```sql
ALTER TABLE <prefixed_table> MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1
```

This caps the maximum merge result size so the selector picks no merges.
Reverse by resetting the setting. Verify this specific setting is table-level
alterable on the deployed ClickHouse version before relying on it.

Pick one approach after verification and use it consistently for both
stop and resume.

## Rollback consideration

If the migration system ever re-promotes a retired version back to active
(rollback), that path must `SYSTEM START MERGES` (or reset the setting) for that
version's tables. Find the un-retire / rollback transition (if one exists today)
and add the resume there. If no rollback path exists yet, document that retired
tables have merges stopped so a future rollback knows to resume them.

## Code sketch

```rust
/// Stops background merges on a retired version's graph tables so the merge
/// pool is reserved for the active version. Retired tables receive no further
/// writes, so this only prevents the scheduler from grinding their existing
/// parts during the retention window; tables stay readable for rollback and
/// are reclaimed later by `drop_version_tables`.
///
/// `SYSTEM STOP MERGES` is node-local runtime state (unlike DROP TABLE, which
/// auto-replicates), so it must be issued ON CLUSTER to reach every replica
/// behind the load-balanced endpoint.
async fn stop_merges_for_version(&self, version: u32) -> Result<(), String> {
    let prefix = table_prefix(version);
    for t in generate_graph_tables(&self.ontology) {
        let prefixed = format!("{prefix}{}", t.name);
        let ddl = format!("SYSTEM STOP MERGES ON CLUSTER 'default' {prefixed}");
        self.graph
            .execute(&ddl)
            .await
            .map_err(|e| format!("STOP MERGES {prefixed}: {e}"))?;
    }
    Ok(())
}
```

Call site (best-effort, never blocks promotion):

```rust
mark_version_retired(&self.graph, entry.version).await.map_err(...)?;
if let Err(e) = self.stop_merges_for_version(entry.version).await {
    warn!(version = entry.version, error = %e, "failed to stop merges on retired version");
}
```

## Acceptance criteria

- After a migration cutover, the newly retired version's graph tables have
  background merges stopped, verified in a non-prod environment via
  `system.merges` showing no new merges for that version while the active
  version still merges.
- Promotion still succeeds if the stop call fails (best-effort; a warning is
  logged, no error propagates).
- The idempotent pass stops merges for an already-retired-not-dropped version.
- Existing drop/retention behavior is unchanged.
- A rollback path (if present) resumes merges.

## Testing

- Unit test: the cutover path invokes `stop_merges_for_version` for the retired
  version. Assert the generated statements cover every `generate_graph_tables`
  entry with the correct `v{N}_` prefix (mirror the existing `drop_version_tables`
  test style; see the test module at the bottom of `completion.rs`).
- Note for integration tests: `SYSTEM STOP MERGES ON CLUSTER` will not behave
  meaningfully in a single-node testcontainer, so validate statement generation
  and invocation with a mocked client rather than asserting merge behavior.

## Hard safety constraint

Do not run any cluster-mutating or operational command against prod (no
`SYSTEM STOP/START MERGES`, `OPTIMIZE`, `ALTER`, `DROP`, `INSERT`, anything
`ON CLUSTER`) by hand. The read-through console access is the read-only
`sql-console` user and must stay `SELECT`/`EXPLAIN`/`SHOW` only. This change is
code that the indexer executes in a real deploy. Verify Cloud `ON CLUSTER`
behavior in staging/dev or via the deployed indexer, never by manual prod
commands.

## References

- Schema version lifecycle: `migrating -> active -> retired -> dropped`, tracked
  in the `gkg_schema_version` ClickHouse table.
- `crates/indexer/src/schema/completion.rs`: `MigrationCompletionChecker`,
  the promote/cutover method, `drop_version_tables`, the retention/cleanup pass.
- `crates/indexer/src/schema/version.rs`: `mark_version_retired`,
  `mark_version_dropped`, `mark_version_active`, `read_all_versions`.
- Table enumeration helpers: `generate_graph_tables`, `table_prefix`
  (imported in `completion.rs` from `query_engine::compiler`).
- Engine on prod: `SharedReplacingMergeTree`, unpartitioned, sort key
  `(traversal_path, id)`; cluster `default` (10 replicas).
