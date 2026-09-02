---
title: "GKG ADR 018: Synthetic projections for reverse edge reads"
creation-date: "2026-09-02"
authors: [ "@michaelusa" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-09-02

## Context

Every edge table sorts by `(traversal_path, relationship_kind, source_id, target_id, ...)` with a
primary key of `(traversal_path, relationship_kind, source_id)` (`config/ontology/schema.yaml`,
`settings.edge_tables`). A forward hop, anchored on `source_id`, prunes granules through the
primary key. A reverse hop, anchored on `target_id`, has only the `bloom_filter(0.0001)` skip
index on `target_id`, so it reads every granule in the `(traversal_path, relationship_kind)`
range that the bloom cannot exclude. On fat relationships that range is the problem itself:
`IN_PROJECT` holds 420M edges in a single namespace (#910) and `gl_ci_edge` holds 3.4B rows
(#859).

Native ClickHouse projections were the first answer. `by_source` and `by_target` reorder
projections shipped on every edge table and were removed in v58 (!1772, commit `6c6915739`)
after A/B measurement on orbit-prod with compiler-rendered SQL:

| Measurement (2026-06-15, orbit-prod) | Projections on | Projections off |
|---|---|---|
| Neighbors, 20 group ids, `gl_edge` | 10.1s, 154 granules, ~88 ms per granule | 3.1s, 579 granules, ~3.8 ms per granule |
| Pathfinding, project to project BFS | fails at 17.9s during projection analysis | 5.8s |
| Aggregation, FK-elided traversal, selective lookup | identical plan | identical plan |

The reorders lost because a projection is stored inside each parent part. A reverse lookup had to
probe a slice of every part, which on object storage is a scattered sequence of high-latency
reads, while the base scan coalesced one range per part. Merges paid too: with
`deduplicate_merge_projection_mode = 'rebuild'` every merge re-materialized about nine
projections, and one 50 GiB merge on `v57_gl_ci_edge` ran for 7.8 hours
([stop_merges_retired_versions.md](../indexing/stop_merges_retired_versions.md)).

Two later changes make native projections structurally unfit, not merely mis-tuned:

- Edge dedup moved to `FINAL` (!1895, #910). ClickHouse documents that "Projections are not
  supported in the `SELECT` statements with the `FINAL` modifier", so no edge read could use a
  projection today even if one existed.
- Maintenance now issues lightweight deletes (`edge_tombstone_collapse`) and
  `ALTER TABLE ... APPLY DELETED MASK` (`table_cleanup`). Projections tolerate those only through
  `lightweight_mutation_projection_mode`, whose default is `throw`.

The reverse-read gap remains open.

## Decision

A **synthetic projection** is a sibling `ReplacingMergeTree` table with the same columns as its
base edge table, a permuted sort key, and a `TO` materialized view on the base that keeps it
current. It is a real table, so it merges on its own schedule, owns its own part budget, serves
`FINAL`, and can be rebuilt from the base with one `INSERT ... SELECT`.

### Declared once, in the ontology

```yaml
settings:
  edge_tables:
    gl_edge:
      sort_key: [traversal_path, relationship_kind, source_id, target_id, source_kind, target_kind]
      reorders:
        - name: gl_edge_by_target
          sort_key: [traversal_path, relationship_kind, target_id, source_id, target_kind, source_kind]
          primary_key: [traversal_path, relationship_kind, target_id]
          # optional: restrict to reverse-hot kinds; omitted means every kind on the base
          relationship_kinds: [IN_PROJECT, CONTAINS, CLOSES]
```

The loader enforces:

- `sort_key` is a permutation of the base sort key. The `ReplacingMergeTree` dedup identity is
  the full sort-key tuple, so a permutation keeps the same identity and `FINAL` on the reorder
  returns exactly the latest row per edge that `FINAL` on the base returns.
- `primary_key` is a prefix of `sort_key`.
- `relationship_kinds`, when present, are kinds routed to the base table.

A reorder is registered as a graph table: it appears in `edge_tables()`-style enumerations,
`sort_key_for_table` answers for it, and `edge_table_config` exposes its base.

### DDL composed from the base

The DDL generator builds the reorder from the base's already-generated `CreateTable`
(`build_edge_table` in `crates/query-engine/compiler/src/passes/codegen/ddl/mod.rs`): same
columns, engine, settings, and partition expression, with three changes.

- `ORDER BY` and `PRIMARY KEY` are the reorder's.
- The codecs on the two ID columns swap. The base gives `Delta(8)` to `source_id` because it is
  sort-adjacent and `T64` to `target_id`; the reorder gives `Delta(8)` to `target_id` and `T64`
  to `source_id`.
- The bloom index on the new leading ID column is dropped, since the primary key covers it; the
  bloom on the other ID column stays.

One versioned materialized view per reorder:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS {gl_edge_by_target__mv}
TO {gl_edge_by_target}
AS SELECT * FROM {gl_edge} [WHERE relationship_kind IN (...)]
```

Both objects ride the existing versioned paths. `MaterializedViewDefinition` already supports
`to_table` and `{table}` placeholders; `generate_graph_materialized_views_with_prefix` versions
them; `migration.rs` creates views after tables; `migration_completion::ontology_known_names`
makes them GC-eligible.

### Write path: no indexer change

Every insert path already funnels through the base table: the SDLC pipeline writer, the
code-indexing `BufferedWriter`, tombstone rows, and backfills. The view fires for each block
and writes the same `_version` and `_deleted`. Async inserts trigger views at flush. The cost is
one extra sort and write per edge block: 2x write amplification and 2x storage for each
opted-in table, or less with `relationship_kinds`.

Two operations do not flow through a view and must cover reorders explicitly:

- **Lightweight deletes and deleted-mask application.** `edge_tombstone_collapse` and
  `table_cleanup` build their table lists from `ontology.edge_tables()`. Reorders join that
  enumeration, keyed by their own sort key, so the same `IN (key list)` delete and the same
  `APPLY DELETED MASK` run against them.
- **Partial flush failures.** The view runs inside the base insert, so a view failure fails the
  insert. The indexer pins `async_insert = 1` without `async_insert_deduplicate`
  (`crates/indexer/src/clickhouse/writer.rs`), so a durable retry re-delivers the block to both
  tables and the duplicate collapses under `ReplacingMergeTree`. Two gaps remain: fire-and-forget
  writes never see the error, and a flush can commit the base part before the view write fails.
  A scheduled reconcile closes both by re-issuing
  `INSERT INTO reorder SELECT * FROM base WHERE _version > watermark` for recent windows. It is
  idempotent for the same reason the retry is.

### Migration and backfill

A reorder is a versioned graph table, but it never needs a re-index. On a forward migration the
new version's reorder is populated from the new version's base after the base is cloned or
rebuilt: `INSERT INTO v<N>_gl_edge_by_target SELECT * FROM v<N>_gl_edge`. This adds a third
classification to ADR 017's `CloneFromActive` and `RebuildEmpty`: `DeriveFromBase`. Adding or
re-keying a reorder is therefore a cheap migration whose ledger scope is that of its base table.

### Compiler routing

The compiler already knows a hop's anchored column. `Direction::edge_columns()` returns
`target_id` first for incoming hops; the neighbors plan builds `incoming_tables` per direction;
`flat_chain` anchors interior hops with `cascade_anchor` on `curr_col`; pathfinding expands the
frontier per direction. The routing rule is:

> Scan the reorder when the hop's anchored column leads the reorder's primary key after
> `(traversal_path, relationship_kind)`, and the reorder covers the hop's relationship kinds.

Unanchored range scans stay on the base, where the two tables are equivalent. `normalize`
populates `table_sort_keys` for reorders so `FINAL` and `LIMIT BY` dedup keep working. Hydration
is untouched because it reads node tables. The plan pass rewrites `hop.edge_table`, mirroring
`bind_denormalized` on the denormalized join tables branch; lowering does not change because the
reorder has the base's columns.

### Why a sibling table compacts where a projection did not

ClickHouse describes a projection as "a subdirectory that stores an anonymous `MergeTree`
table's part" inside the parent part directory, merged "via its storage's merge routine" during
the parent's merge. Part boundaries are the parent's, merge timing is the parent's, and every
merge or lightweight delete rebuilds the projection. A reverse read therefore touches every
parent part, sorted by target only within each one.

A sibling table has its own parts, its own merge selection, and its own `parts_to_throw_insert`
budget. Its merges are plain six-column `ReplacingMergeTree` merges with no rebuild step, and
`OPTIMIZE ... FINAL CLEANUP` applies to it like any table. It settles into a few large parts
sorted by target, so a primary-key-anchored reverse read is one coalesced range per part. That
is the read shape that won the on/off measurement above.

### Rollout

Opt in per table, largest reverse-read pain first and largest storage last:

1. `gl_edge` with `relationship_kinds` limited to kinds whose reverse reads anchor on a hub:
   `IN_PROJECT` (everything in a project), `CONTAINS` (the parent of an entity), `CLOSES` (merge
   requests closing an issue).
2. `gl_code_edge` for `CALLS` and `IMPORTS` ("who calls or imports X"). The reorder key is
   `(traversal_path, relationship_kind, project_id, branch, target_id, source_id, ...)`.
3. `gl_ci_edge` only if step 1 shows the storage cost is acceptable at 3.4B rows.

## Measurement plan

Run before merge, on the bench cluster with compiler-rendered SQL, comparing `read_rows`,
`read_bytes`, and `ProfileEvents` rather than wall clock, which on ClickHouse Cloud swings with
object-storage concurrency.

1. **Compaction.** Create `gl_edge_by_target` for the active version, backfill with
   `INSERT ... SELECT`, and record active part count and bytes for base and reorder over 24 hours
   of ingest.
2. **Reads.** A/B routed versus unrouted on the corpus shapes that anchor on `target_id`:
   incoming neighbors of a project over `IN_PROJECT`, merge requests closing an issue, callers
   of a definition, pathfinding, and a multi-hop chain with a reverse interior hop.
3. **Writes.** The reference-architecture simulation (!2239) with the view on and off: insert
   latency, async-insert flush errors, parts created per minute.
4. **Consistency.** Count and `cityHash64` of `FINAL` rows per `(traversal_path,
   relationship_kind)` on base versus reorder, before and after a reconcile run.

Success is a read_rows reduction of at least 5x on anchored reverse reads with no wall
regression, insert p99 within 20 percent of baseline, and no query class regressing.

## Why not the alternatives

- **Restore native projections.** Measured loss on reads, incompatible with `FINAL`, rebuilt on
  every merge and lightweight delete.
- **Dual write at ETL time.** Touches every writer (SDLC pipeline, code `BufferedWriter`,
  tombstone paths, backfill), drifts on partial failure exactly like a view does, and cannot be
  rebuilt without a re-index. A view covers every insert path with no indexer code and rebuilds
  with one statement. Both pay the same 2x write cost.
- **Bloom only (status quo).** A `0.0001` bloom still reads the whole
  `(traversal_path, relationship_kind)` range minus true negatives; on fat relationships a reverse
  anchored read is a range scan.
- **Denormalized join tables** (`michaelusa/denormalized-join-tables`). They solve a different
  problem, node properties on the hop, and are complementary. Reorders reuse the same
  declaration, DDL composition, `TO` view, and plan-pass table rewrite. A denormalized table can
  itself declare a reorder.
- **Refreshable materialized view instead of an insert trigger.** Refresh lag on hot edge tables
  is unacceptable for reads; a refresh is the right shape only for the reconcile.

## Consequences

- Reverse adjacency becomes primary-key pruned while `FINAL` dedup keeps working, and base-table
  merges stay projection-free.
- The indexer does not change. The graph-shape fact lives in the ontology once, and ETL,
  maintenance, GC, and the compiler all read it from there.
- Opted-in tables double their edge storage and write amplification, bounded by
  `relationship_kinds`.
- Reorders are versioned tables, so GC, deleted-mask cleanup, tombstone collapse, and migration
  classification must enumerate them. Missing one leaves a silently stale reorder, which is why
  they enter through the existing `edge_tables()` enumeration rather than a parallel list.
- View insert failures create a new consistency surface that the reconcile task owns.
- The compiler gains a routing decision that must be measured per query class, as the projection
  removal was.

## Non-goals

- Reorders on node tables. Node-table projections measured as perf-neutral before removal.
- Aggregate projections such as per-kind counts. Separate concern with different storage.
- Partitioning. The reorder inherits whatever the base declares.

## References

- !1772 `perf(schema): remove all projections and tighten bloom filters` (commit `6c6915739`)
- !1895 `perf(compiler): dedup edges and filter-only nodes with FINAL`, issue #910
- Issue #859, expand FK elision (the `d3` reverse hop compiles to a 3x `gl_ci_edge` self-join)
- [ADR 017: clone-based non-blocking migrations](017_clone_based_non_blocking_migrations.md)
- [Stop merges on retired versions](../indexing/stop_merges_retired_versions.md), merge cost
  under projection rebuild
- ClickHouse docs: MergeTree projections (storage inside the part, `FINAL` limitation);
  `CREATE VIEW` (materialized view insert semantics, source-data changes not propagated)
- Sibling branch `michaelusa/denormalized-join-tables`: `crates/ontology/src/denormalized.rs`,
  `crates/query-engine/compiler/src/passes/codegen/ddl/denormalized.rs`, `bind_denormalized`
  in `passes/plan/edge_chain.rs`
- Code touch points: `crates/indexer/src/orchestrator/scheduled/edge_tombstone_collapse.rs`
  (`swept_edge_tables`), `table_cleanup.rs` (`list_graph_tables`),
  `crates/indexer/src/schema/migration.rs`, `passes/plan/neighbors.rs` (`incoming_tables`),
  `passes/shared.rs` (`resolve_edge_table`), `input.rs` (`Direction::edge_columns`)
