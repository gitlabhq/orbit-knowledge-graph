# Universal Hydration: Open Concerns

## Performance: two roundtrips vs one

Every query (except aggregation) now requires at least two ClickHouse roundtrips:
the structural query, then one hydration query per entity type. ClickHouse point
lookups by primary key are fast, but network latency compounds. Mitigation: run
hydration queries in parallel per entity type via `tokio::join!` or `FuturesUnordered`.

**Status: validated.** Evaluated 29 queries against real ClickHouse (simulator data:
6937 nodes, 16128 edges). 27/29 compile and execute successfully. Average structural
query time ~37ms. Hydration roundtrips not yet parallelized but structural SQL is
confirmed working end-to-end. Two failures are pre-existing (query fixtures reference
a nonexistent `system` column on Note).

## Aggregation stays untouched

Aggregation queries need actual column values in the SQL for GROUP BY and aggregate
functions (COUNT, SUM, AVG). Hydration doesn't apply — the result is aggregate
values, not individual entity rows.

**Status: resolved.** Confirmed — `build_hydration_plan` returns `HydrationPlan::None`
for aggregation, and aggregation queries go through `compile()` without any SELECT
slimming. Aggregation queries execute correctly against ClickHouse.

## Indirect auth resolution for dynamic nodes with owner_entity

**Status: resolved.** Pre-auth hydration step fetches `auth_id_column` values from
entity tables, populates `auth_id_overrides` on `QueryResult`.

## Snapshot test volume

**Status: resolved.** All 104 query-engine + 115 gkg-server lib tests pass.

## Hydration query security context

**Status: resolved.** `HydrationStage` receives the security context and passes
it to `compile_with_columns()` for all query types.

## Edge metadata opt-in for PathFinding

**Status: resolved.** `InputPath.include_edges` field added (defaults to `false`).
JSON schema updated.

## Integration tests (Docker/ClickHouse)

The 56 redaction integration tests in `redaction_integration.rs` require Docker
with ClickHouse. All mechanical changes (`.sql` → `.structural.sql`, etc.) have
been applied. Assertions checking for edge columns in structural SQL have been
removed (edge columns come from hydration now).

**Status:** assertions updated; not yet run with Docker testcontainers. Separately,
the simulator evaluator confirmed 27/29 queries execute successfully against real
ClickHouse with generated data.

## Dynamic hydration merge format

**Status: resolved.** `merge_dynamic_properties` implemented:
- Neighbors: flat column merge (one neighbor per row)
- PathFinding: builds a `path_nodes` JSON array via `ColumnValue::Json`. Each
  element contains `id`, `type`, and hydrated properties with the `n_` alias
  prefix stripped. 5 unit tests covering the merge logic.
