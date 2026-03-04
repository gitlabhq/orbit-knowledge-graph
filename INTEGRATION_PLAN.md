# SDLC v2 Integration Plan

Goal: Merge v2 capabilities into v1 (`sdlc/`) directly. No parallel `sdlc_v2/` module.

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
- **Status:** NOT STARTED
- **Risk:** Low
- **Touches existing behavior:** Yes, but `prepare.rs` adapts trivially
- **Files:**
  - `fixtures/ontology/ontology.schema.json` — add select/from/where/watermark/deleted/order_by
  - All `fixtures/ontology/nodes/**/*.yaml` and `edges/**/*.yaml` — migrate + add order_by
  - `crates/ontology/src/etl.rs` — restructure EtlConfig::Query, add order_by to Table
  - `crates/ontology/src/entities.rs` — add order_by to EdgeSourceEtlConfig
  - `crates/ontology/src/lib.rs` — parse new YAML fields
  - `crates/indexer/src/modules/sdlc/prepare.rs` — build SQL from structured fields
  - All v1 test fixtures — add order_by

### MR 2: Add PipelinePlan + ExtractQuery (additive, no behavior change)
- **Branch:** `mr2/pipeline-plan-abstraction` from MR1
- **Status:** NOT STARTED
- **Risk:** None — additive only
- **Touches existing behavior:** No
- **Files:**
  - NEW `crates/indexer/src/modules/sdlc/plan.rs` — ExtractQuery, PipelinePlan, build_plans()
  - Source: v2's `plan/mod.rs` + `plan/from_ontology.rs`

### MR 3: Add IndexingPosition store (additive, no behavior change)
- **Branch:** `mr3/indexing-position-store` from MR2 (or MR1, independent of MR2)
- **Status:** NOT STARTED
- **Risk:** None — additive only
- **Touches existing behavior:** No
- **Files:**
  - NEW `crates/indexer/src/modules/sdlc/indexing_position.rs`
  - `crates/indexer/tests/fixtures/siphon.sql` — add sdlc_indexing_position table
  - Schema migration for the new table

### MR 4: Rewire pipeline + handlers + switchover
- **Branch:** `mr4/rewire-pipeline` from MR3
- **Status:** NOT STARTED
- **Risk:** Medium — this is the actual switchover
- **Touches existing behavior:** Yes
- **Files:**
  - `sdlc/pipeline.rs` — cursor-paginated loop using PipelinePlan + ExtractQuery
  - `sdlc/global_handler.rs` — simplify to PipelineContext + pipeline.run()
  - `sdlc/namespace_handler.rs` — same simplification
  - `sdlc/mod.rs` — use build_plans() instead of OntologyEntityPipeline/OntologyEdgePipeline
  - Remove `prepare.rs`, `transform.rs` (logic now in plan.rs)
  - Integration tests — add FINAL to queries, use sdlc_indexing_position for watermarks
  - `integration-testkit/src/context.rs` — concat batches
  - Delete `sdlc_v2/` directory

## Key Design Decisions
- Stacked branches (linear: mr1 → mr2 → mr3 → mr4)
- Each MR must compile and pass tests independently
- MRs 2 and 3 are additive-only (no behavior changes)
- MR 4 is the actual switchover
- No `sdlc_v2/` module ever exists in main — everything lands directly in `sdlc/`
