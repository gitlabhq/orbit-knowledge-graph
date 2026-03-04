# SDLC v2 Integration Plan

Goal: Merge v2 capabilities into v1 (`sdlc/`) directly. No parallel `sdlc_v2/` module.

Reference branch: `full-sdlc-v2-update` (58 files, +3647/-284 lines) contains the complete v2 implementation alongside v1. All MRs cherry-pick from this branch.

## Architecture Overview

v2 replaces v1's eager string-templated SQL with a structured pipeline:

```
Ontology YAML
  → from_ontology::build_plans()      # builds PipelinePlan per entity
    → AST (ast.rs: Query, Expr, TableRef)
    → codegen::emit_sql()             # SQL emitted per page
  → ExtractQuery                      # owns cursor state, generates paginated SQL
  → Pipeline::run()                   # cursor-paginated loop with position persistence
```

### Key v2 types

- **`ast::Query`** — minimal SQL AST: `select: Vec<SelectExpr>`, `from: TableRef`, `where_clause: Option<Expr>`, `order_by`, `limit`. `Expr::Raw` is the escape hatch for ClickHouse-specific fragments.
- **`ExtractQuery`** — wraps a `Query` + sort key columns + cursor values + batch size. Generates SQL via `to_sql()`, advances via `advance(&last_batch)`. Resumable from `IndexingPosition`.
- **`PipelinePlan`** — bundles an `ExtractQuery` + `Vec<TransformOutput>` (node + FK edge transforms) per entity.
- **`IndexingPosition`** — `{ watermark, cursor_values: Option<Vec<String>> }`. State machine: None = first run, Some = interrupted mid-page, None cursor = completed.
- **`PipelineContext`** — per-handler context: watermark, position key, base conditions.

### What changes per layer

| Layer | v1 (current) | v2 (target) |
|-------|-------------|-------------|
| ETL config | Monolithic `query:` string | Structured `select/from/where/watermark/deleted/order_by` |
| SQL generation | `prepare.rs` string templates | `from_ontology.rs` → AST → `codegen.rs` |
| Pipeline abstraction | `OntologyEntityPipeline` (eager SQL) | `PipelinePlan` + `ExtractQuery` (on-demand SQL) |
| Pagination | Streaming, no cursors | Keyset cursor (DNF clause), resumable |
| Position tracking | `watermark_store.rs` (scope-level) | `indexing_position.rs` (per-entity, `{scope}.{entity}`) |
| Handler logic | Fetch params → build pipeline → process | Build context → `pipeline.run(&plans, &context)` |

## Branch Strategy

Stacked branches, each branching from the previous:

```
main
 └─ mr1/ontology-structured-etl
     └─ mr2/pipeline-plan-abstraction
         └─ mr3/indexing-position-store
             └─ mr4/rewire-pipeline
```

As each MR merges to main, rebase the next branch onto main.

## MR Sequence

### MR 1: Ontology — structured ETL fields + `order_by`
- **Branch:** `mr1/ontology-structured-etl` from `main`
- **Status:** NOT STARTED (code exists on reference branch)
- **Risk:** Low, behavior-preserving
- **Touches existing behavior:** Yes, but `prepare.rs` adapts trivially
- **What changes:**
  - `EtlConfig::Query` decomposed: `select`, `from`, `where_clause`, `watermark`, `deleted`, `order_by`
  - `EtlConfig::Table` gains `order_by: Vec<String>`
  - `watermark()` and `deleted()` return non-Option (always present)
  - All ontology YAML migrated from monolithic `query:` to structured fields
- **Files:**
  - `fixtures/ontology/ontology.schema.json` — extend schema for new ETL fields
  - All `fixtures/ontology/nodes/**/*.yaml` and `edges/**/*.yaml` — migrate to structured format
  - `crates/ontology/src/etl.rs` — restructure `EtlConfig::Query`, add `order_by` to `Table`
  - `crates/ontology/src/entities.rs` — add `order_by` to `EdgeSourceEtlConfig`
  - `crates/ontology/src/lib.rs` — parse new YAML fields
  - `crates/indexer/src/modules/sdlc/prepare.rs` — adapt to build SQL from structured fields instead of raw query string
  - All v1 test fixtures — update to match new `EtlConfig` constructors

### MR 2: Add plan module — AST, codegen, PipelinePlan, ExtractQuery
- **Branch:** `mr2/pipeline-plan-abstraction` from MR1
- **Status:** NOT STARTED (code exists on reference branch)
- **Risk:** None — additive only
- **Touches existing behavior:** No
- **What changes:**
  - New `sdlc/plan/` submodule with the structured query pipeline
  - `build_plans(ontology, batch_size) -> PartitionedPlans` as the entry point
  - No existing code modified — purely additive
- **Files (all new, sourced from `sdlc_v2/plan/`):**
  - `crates/indexer/src/modules/sdlc/plan/mod.rs` — `ExtractQuery`, `PipelinePlan`, `TransformOutput`, `PartitionedPlans`
  - `crates/indexer/src/modules/sdlc/plan/ast.rs` — `Query`, `Expr` (Column/Raw/BinaryOp/IsNotNull/FuncCall/Cast), `SelectExpr`, `OrderExpr`, `TableRef`, `Op`
  - `crates/indexer/src/modules/sdlc/plan/codegen.rs` — `emit_sql(&Query) -> String`, expression/clause emitters
  - `crates/indexer/src/modules/sdlc/plan/from_ontology.rs` — `build_plans()`, `build_node_plan()`, `build_edge_etl_plan()`, `build_extract_query()`, `build_node_transform()`, `build_fk_edge_transform()`
- **Visibility change:** types scoped to `pub(in crate::modules::sdlc)` instead of `sdlc_v2`

### MR 3: Add IndexingPosition store
- **Branch:** `mr3/indexing-position-store` from MR2 (or MR1 — independent of MR2)
- **Status:** NOT STARTED (code exists on reference branch)
- **Risk:** None — additive only
- **Touches existing behavior:** No
- **What changes:**
  - New position store with per-entity cursor tracking
  - `sdlc_indexing_position` table: `(key String, watermark DateTime, cursor_values String, _version UInt64)`
  - `IndexingPositionStore` trait: `load()`, `save_progress()`, `save_completed()`
- **Files:**
  - NEW `crates/indexer/src/modules/sdlc/indexing_position.rs` — sourced from `sdlc_v2/indexing_position.rs`
  - `crates/indexer/tests/fixtures/siphon.sql` — add `sdlc_indexing_position` table DDL
  - Schema migration for the new table

### MR 4: Rewire pipeline + handlers + switchover
- **Branch:** `mr4/rewire-pipeline` from MR3
- **Status:** NOT STARTED (code exists on reference branch)
- **Risk:** Medium — this is the actual switchover
- **Touches existing behavior:** Yes
- **What changes:**
  - Pipeline rewritten with cursor-paginated loop (`Pipeline::run`)
  - Handlers simplified to build `PipelineContext` and delegate to `pipeline.run(&plans, &context)`
  - `prepare.rs` and `transform.rs` removed (logic lives in `plan/`)
  - `watermark_store.rs` removed (replaced by `indexing_position.rs`)
  - `sdlc_v2/` directory deleted entirely
- **Files modified:**
  - `sdlc/pipeline.rs` — cursor-paginated loop with position persistence
  - `sdlc/global_handler.rs` — simplify to `PipelineContext` + `pipeline.run()`
  - `sdlc/namespace_handler.rs` — same simplification
  - `sdlc/mod.rs` — use `build_plans()` from plan module, register simplified handlers
  - `integration-testkit/src/context.rs` — concat batches helper
- **Files removed:**
  - `sdlc/prepare.rs` (677 lines)
  - `sdlc/transform.rs` (397 lines)
  - `sdlc/watermark_store.rs` (144 lines)
  - Entire `sdlc_v2/` directory (~3000 lines)
- **Test changes:**
  - All integration queries use `FINAL` keyword (ClickHouse ReplacingMergeTree correctness)
  - Position keys change to `"{scope}.{entity_name}"` format
  - Watermark assertions switch from `global_indexing_watermark` to `sdlc_indexing_position`

## Key Design Decisions
- Stacked branches (linear: MR1 → MR2 → MR3 → MR4)
- Each MR must compile and pass tests independently
- MRs 2 and 3 are additive-only (no behavior changes)
- MR 4 is the actual switchover
- No `sdlc_v2/` module ever exists in main — everything lands directly in `sdlc/`
- v2 proven at scale: 34M rows, 880K rows/sec, 35 pages (see `PAGINATION_VALIDATION.md`)
- Net code reduction: v1 (3815 lines) → v2 (3000 lines), ~17% smaller despite more capability
