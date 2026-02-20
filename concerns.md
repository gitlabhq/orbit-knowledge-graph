# Universal Hydration: Open Concerns

## Performance: two roundtrips vs one

Every query (except aggregation) now requires at least two ClickHouse roundtrips:
the structural query, then one hydration query per entity type. ClickHouse point
lookups by primary key are fast, but network latency compounds. Mitigation: run
hydration queries in parallel per entity type via `tokio::join!` or `FuturesUnordered`.

Benchmark before/after on representative traversal queries to quantify the overhead.

## Aggregation stays untouched

Aggregation queries need actual column values in the SQL for GROUP BY and aggregate
functions (COUNT, SUM, AVG). Hydration doesn't apply — the result is aggregate
values, not individual entity rows. Confirm this assumption holds for all current
aggregation patterns and won't need revisiting.

## Indirect auth resolution for dynamic nodes with owner_entity

Dynamic nodes (from PathFinding paths and Neighbors results) have entity types
discovered at runtime. Some of these entities use indirect authorization — e.g., a
`Definition` node is authorized via its owning `Project`'s ID (`project_id`), not
its own `id`.

Today, this resolution uses edge columns (`e_src`, `e_dst`, `e_src_type`,
`e_dst_type`) present in the result row. With slim structural queries, these
columns may not be present. Need to decide on the resolution approach before
implementation. See design document for options.

## Snapshot test volume

~150 query-engine snapshot tests assert on generated SQL. The structural SELECT
changes will invalidate most of them. Plan for a bulk update pass and review
carefully to avoid regressions.

## Hydration query security context

Hydration queries are compiled through the full `query_engine::compile()` pipeline,
including security context injection. This is correct — hydration must respect
traversal_path filtering — but means the security context must be threaded through
to the hydration stage. Currently done for Neighbors; needs to work for all types.

## Edge metadata opt-in for PathFinding

Making `_gkg_edges` optional in PathFinding queries requires a new DSL field
(`include_edges` or similar). This is a query DSL schema change that needs
ontology schema validation updates and client communication.
