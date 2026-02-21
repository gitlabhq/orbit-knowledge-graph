# Universal Hydration: Open Concerns

## Performance: two roundtrips vs one

Every query (except aggregation) now requires at least two ClickHouse roundtrips:
the structural query, then one hydration query per entity type. ClickHouse point
lookups by primary key are fast, but network latency compounds. Mitigation: run
hydration queries in parallel per entity type via `tokio::join!` or `FuturesUnordered`.

Benchmark before/after on representative traversal queries to quantify the overhead.

**Status:** architecture implemented; benchmarks not yet run.

## Aggregation stays untouched

Aggregation queries need actual column values in the SQL for GROUP BY and aggregate
functions (COUNT, SUM, AVG). Hydration doesn't apply — the result is aggregate
values, not individual entity rows. Confirm this assumption holds for all current
aggregation patterns and won't need revisiting.

**Status:** confirmed — `build_hydration_plan` returns `HydrationPlan::None` for
aggregation, and aggregation queries go through `compile()` without any SELECT
slimming.

## Indirect auth resolution for dynamic nodes with owner_entity

Dynamic nodes (from PathFinding paths and Neighbors results) have entity types
discovered at runtime. Some of these entities use indirect authorization — e.g., a
`Definition` node is authorized via its owning `Project`'s ID (`project_id`), not
its own `id`.

**Status: resolved.** Pre-auth hydration step added between ExtractionStage and
AuthorizationStage. For Dynamic hydration plans, `HydrationStage::resolve_auth_ids()`
fetches `auth_id_column` values from entity tables, populates `auth_id_overrides`
on `QueryResult`. Both `resource_checks()` and `apply_authorizations()` check
overrides first, then fall back to edge column resolution.

## Snapshot test volume

~150 query-engine snapshot tests assert on generated SQL. The structural SELECT
changes will invalidate most of them.

**Status: resolved.** All 104 query-engine lib tests and 109 gkg-server lib tests
updated and passing. Tests that checked column counts/aliases in structural SQL
were updated to check `_gkg_*` columns from `enforce_return()`. Hydration template
tests verify property columns appear in the hydration plan.

## Hydration query security context

Hydration queries are compiled through the full `query_engine::compile()` pipeline,
including security context injection. This is correct — hydration must respect
traversal_path filtering — but means the security context must be threaded through
to the hydration stage. Currently done for Neighbors; needs to work for all types.

**Status:** implemented. `HydrationStage` receives the security context and passes
it to `compile_with_columns()` when building both static and dynamic hydration
queries.

## Edge metadata opt-in for PathFinding

Making `_gkg_edges` optional in PathFinding queries requires a new DSL field
(`include_edges` or similar). This is a query DSL schema change that needs
ontology schema validation updates and client communication.

**Status: resolved.** `InputPath.include_edges` field added (defaults to `false`).
JSON schema updated with `include_edges` boolean in `PathConfig`. CTE conditionally
skips edge tuple accumulation when `include_edges` is false.

## Integration tests (Docker/ClickHouse)

The 56 redaction integration tests in `redaction_integration.rs` require Docker
with ClickHouse. All mechanical changes (`.sql` → `.structural.sql`, etc.) have
been applied. However, some tests may still fail at runtime because they assert
that edge columns (`e0_type`, `e0_src`) appear in structural SQL results for
traversal queries — those columns are now only available after hydration.

**Status:** mechanical replacements done; runtime validation pending (needs Docker).

## Dynamic hydration merge format

`merge_dynamic_properties` in `HydrationStage` has a placeholder for how to attach
hydrated properties to path rows containing multiple nodes. Path rows have arrays
of node IDs/types in `_gkg_path`; the merge needs to expand these into per-node
property maps or a list of enriched objects.

**Status:** implementation skeleton exists; final merge format not finalized.
