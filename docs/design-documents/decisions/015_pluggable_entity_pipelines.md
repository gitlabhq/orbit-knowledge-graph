---
title: "GKG ADR 015: Pluggable transforms over a shared SDLC pipeline"
creation-date: "2026-06-03"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-06-03

## Context

The SDLC indexer's transform stage is SQL-only. Every entity flows through one
generic `EntityHandler` (`crates/indexer/src/modules/sdlc/handler/entity.rs:23`)
that owns a `Plan` and a `Pipeline` (`crates/indexer/src/modules/sdlc/pipeline.rs:74`).
The pipeline runs `Extractor → Transformer → Loader`, and the transform is a list
of SQL strings (`Transformation { sql, destination_table, dict_encode_columns }`,
`plan/mod.rs:206`) executed against an in-memory DataFusion `MemTable` named
`source_data` (`pipeline.rs:390-433`). The transform set is generated from
ontology YAML in `plan/lower.rs`.

This is right for entities whose graph shape is a row-wise projection of one
extracted batch. It is wrong for entities whose transform is **derived at runtime
and needs to query the datalake again mid-transform** — the driver being the
SystemNote handler (ADR 013), which parses GFM reference tokens out of free-text
note bodies, then resolves them to entity IDs with batched `IN`-list lookups
against `siphon_routes` and the entity tables. A `SELECT … FROM source_data`
against a single in-memory block cannot express that second hop.

The trap is to conclude "SystemNote needs its own pipeline." It does not, and it
must not. **The pipeline is mostly shared, hard-won extraction and writing
machinery that every SDLC entity needs**, and re-owning it per entity is exactly
the duplication this ADR exists to prevent. The optimizations that live in the
pipeline today, none of which are entity-specific:

| Optimization | Where |
|---|---|
| Keyset pagination with DNF cursor predicate (uses the CH sort-key index) | `CursorFilter` (`plan/mod.rs:156`), `Producer::run` loop (`pipeline.rs:455-532`) |
| Watermark windowing + traversal-path scoping pushed into extract SQL | `WatermarkFilter`/`TraversalPathFilter` (`plan/mod.rs:94`, `:122`) |
| Block-level streaming with bounded read-ahead — next page's read overlaps current page's writes | producer/consumer split over `mpsc` (`pipeline.rs:162-195`) |
| Adaptive retry: halve `max_block_size` on transient failure down to a floor | `Extractor` (`pipeline.rs:310-356`) |
| Lazy, per-destination-table streaming writers (no insert opened for an empty table) | `Loader`/`PageWriter` (`pipeline.rs:538-633`) |
| Page-boundary checkpointing + crash-safe cursor resume | `run_plan`/`consume` (`pipeline.rs:96`), `Cursor` (`plan/mod.rs:19`) |
| Idempotent re-processing via `ReplacingMergeTree` | graph DDL |
| Read/write stats + observer wiring | `PipelineStats` (`pipeline.rs:50`), `PipelineContext` (`pipeline.rs:68`) |

Crucially, the only entity-specific object inside that machinery is the
`Transformer`, constructed from `plan.transforms` at a single line
(`pipeline.rs:181`). Everything wrapped around it is generic. That is the seam.

ADR 014 named an `EntityPipeline` trait with SystemNotes as the custom-pipeline
example. Taken literally — one custom *pipeline* implementation per hard entity —
that framing reintroduces the duplication above. This ADR refines it: the
extension point is the **transform stage**, not the whole pipeline. There remains
exactly one pipeline *type* — instantiated per entity, parameterized by its
transform.

## Decision

Keep one generic `Pipeline` that owns all extraction and writing. Make the
**transform** the single pluggable seam, as a trait that can read the datalake.
Every SDLC entity — SQL-projected or hand-written Rust — runs on the same
pipeline; only its transform differs.

### The seam: a `BlockTransform` trait

Replace the concrete `Transformer { transforms: Vec<Transformation> }`
(`pipeline.rs:361`) with a trait object the pipeline drives per block, exactly
where the `Transformer` is built today (`pipeline.rs:181`):

```rust
#[async_trait]
pub(in crate::modules::sdlc) trait BlockTransform: Send + Sync {
    /// Destination tables this transform writes, in output-index order.
    /// Drives the Loader's per-table streaming writers and central
    /// dict-encoding — the transform never opens a writer itself.
    fn destinations(&self) -> &[OutputTable];

    /// Transform one extracted block into rows for one or more destinations.
    /// Each `TableBatch.output_index` selects a `destinations()` entry.
    async fn transform(
        &self,
        ctx: &TransformContext<'_>,
        block: &RecordBatch,
    ) -> Result<Vec<TableBatch>, HandlerError>;
}

pub(in crate::modules::sdlc) struct TransformContext<'a> {
    /// Multi-hop reads. Point/IN-list enrichment lookups, not paginated scans.
    pub datalake: &'a dyn DatalakeQuery,
    /// Scope for the transform's own lookups (e.g. SystemNote ref resolution).
    pub traversal_path: Option<&'a str>,
}
```

Two design rules the trait enforces:

- **No `SessionContext` on the trait surface.** DataFusion is an implementation
  detail of the SQL transform and must never leak to a transform that does not run
  SQL. A non-SQL transform must not be handed a DataFusion session.
- **Datalake access is granted, not pipeline ownership.** The multi-hop capability
  SystemNote needs already exists on `DatalakeQuery`
  (`crates/indexer/src/modules/sdlc/datalake.rs:41`: `query_batches`). Handing it
  to the transform via `TransformContext` is all that's required — the transform
  does *not* need to own pagination, checkpointing, or writing to do a second-hop
  read.

The pipeline stays per-block, preserving the streaming/read-ahead memory bound.
Per-block granularity also naturally bounds a transform's enrichment `IN`-list to
one block (`DEFAULT_STREAM_BLOCK_SIZE`, `datalake.rs:71`) rather than a whole
page; a transform that needs wider batching can buffer internally.

### Two implementations of one trait

- **`SqlTransform`** — today's behavior, unchanged. Owns a `SessionContext`
  internally (register/deregister take `&self` via DataFusion's interior
  mutability, so no `&mut` and no leak), registers the block as `source_data`, runs
  the ontology-generated SQL list, returns `TableBatch`es. Built from
  `plan.transforms` by `lower.rs` as now.
- **`SystemNotesTransform`** (ADR 013) — hand-written Rust. Parses note bodies,
  collects distinct refs, calls `ctx.datalake.query_batches` for the
  `siphon_routes` and entity-table resolution hops, emits edge rows. No DataFusion.

### Extraction and writing are not duplicated — they are reused as-is

SystemNote needs no bespoke extractor: its source is `siphon_notes ⋈
siphon_system_note_metadata`, which is an ordinary `query`-type ETL plan (a JOIN in
`extract_template`). It rides the same keyset pagination, watermark window, retry,
read-ahead, checkpoint, and streaming-write path as every other entity. The only
Rust-specific code is the transform body. This is the whole point: a new
hand-written entity contributes a `BlockTransform`, nothing else.

### Output tables and dict-encoding centralized

Today `dict_encode_columns` and the destination table are carried per
`Transformation` and applied inside the transform (`prepare_batches`,
`pipeline.rs:431`). Move both to the `OutputTable` spec, derived from the ontology
(`edge_specs(ontology)` / node storage columns), and apply dict-encoding centrally
in the `Loader` keyed by `TableBatch.output_index`. A Rust transform then gets
correct dict-encoding and schema conformance for free and cannot silently drift
from `config/graph.sql`.

### The transform is fed to the pipeline, selected by a registry

Today there is one shared `Pipeline` singleton (`mod.rs:58`), reused by every
handler, so it cannot hold anything entity-specific. That is the only reason the
transform would otherwise have to thread through `EntityHandler`. Drop the
singleton: a `Pipeline` is an Arc-bundle of stateless collaborators (`datalake`,
`checkpoint_store`, `metrics`, `retry_config`), so building one *per entity*
duplicates no logic — it Arc-clones the same collaborators and adds the entity's
`plan` + `transform`. "Shared machinery" is shared *code*, not a shared *instance*.

`Pipeline::new` takes `(plan, transform, …shared collaborators)`. The transform is
fed straight to the pipeline at construction and never appears on
`EntityHandler`'s surface. `run_plan`/`run`/`Producer` drop their `plan` and
`transform` parameters — they read `self.plan` / `self.transform`.

Selection avoids a central `match`: a `TransformRegistry` maps entity kind →
factory, with `SqlTransform` as the implicit default (SQL entities register
nothing). Custom transforms self-register from their own module, the same
composition pattern as `*::register_handlers`:

```rust
// register_handlers, building one pipeline per plan
let mut transforms = TransformRegistry::default();
transform::system_notes::register(&mut transforms);   // additive; one line per custom kind

for plan in plans.namespaced {
    let transform = transforms.resolve(&plan, &build_ctx);  // default → SqlTransform
    let pipeline = Arc::new(Pipeline::new(
        plan, transform,
        Arc::clone(&datalake), Arc::clone(&checkpoint_store),
        metrics.clone(), retry_config.clone(),
    ));
    registry.register_handler(Box::new(EntityHandler::new(
        pipeline, EtlScope::Namespaced,
        Arc::clone(&datalake),  // handler still needs it for partition quantiles
        subscription.clone(), strategy, analytics.clone(),
    )));
}
```

**Minimal handler/pipeline split.** The pipeline owns per-page execution (extract,
transform, write, checkpoint). `EntityHandler` keeps only its dispatch decisions —
watermark derivation, partition-range computation via `self.datalake`
(`entity.rs:168`), request decoding — and reads plan metadata it needs (watermark
column, name) through `pipeline.plan()`. Pushing watermark/partition logic into the
pipeline too is deferred; it is more churn for no dedup gain.

## Consequences

What improves:

- The transform stage is no longer SQL-only, and the extraction/writing
  optimizations are inherited by every entity with zero duplication — the explicit
  goal. A new Rust entity is one `BlockTransform` impl plus an extract plan.
- ADR 013 unblocks with a smaller surface than "a whole custom pipeline":
  SystemNote becomes an extract plan + one trait impl.
- No behavior change for existing entities; they run `SqlTransform` over the same
  pipeline, built from the same ontology plans.

What gets harder:

- A trait boundary where there was a concrete `Transformer`. Mechanical refactor
  of `pipeline.rs:181` and the `TableBatch` index (`transform_index` →
  `output_index`).
- Risk of hand-written-transform proliferation. Mitigation: the default stays
  "express it as an ontology plan + `SqlTransform`." A Rust transform is justified
  only when the projection cannot be SQL — concretely, when it needs multi-hop
  datalake reads or cross-row work the SQL projection can't do. Document that bar
  in `crates/indexer/AGENTS.md` beside the reuse checklist.

## Relationship to ADR 014

ADR 014 (Accepted) decided entity-level dispatch and named `EntityPipeline` /
`SimpleEntityPipeline`, with SystemNotes as the custom example. This ADR refines
the *granularity* of that extension point: rather than one custom **pipeline** per
hard entity (which would re-own extraction and writing), the seam is the
**transform stage**, and the per-entity `Pipeline` instance *is* the
`EntityPipeline` ADR 014 named — one shared implementation parameterized by
`(plan, transform)` and instantiated per entity, not one impl per hard entity.
ADR 014's dispatch model, per-entity NATS subjects, and partitioning are
unaffected.

## Non-goals

- **Per-entity custom pipelines.** Rejected as the duplication source this ADR
  exists to prevent.
- **Exposing DataFusion in the trait.** Stays internal to `SqlTransform`.
- **Bespoke extractors/writers for Rust entities.** They reuse the shared pipeline;
  a different source shape is a different extract plan, not new machinery.
- **Ontology schema changes for transform selection.** Selection is a Rust
  registration concern (entity kind → transform builder); edge/node kinds remain
  ontology-declared.
- **Code and namespace-deletion modules.** Out of scope; they sit outside the SQL
  plan path already.

## References

- ADR 014 (Entity-level SDLC indexing) — [014_entity_level_indexing.md](014_entity_level_indexing.md)
- ADR 013 (Materialize edges from system notes); multi-hop resolution shape —
  [013_system_note_edges.md](013_system_note_edges.md)
- Shared pipeline + the transform seam: `crates/indexer/src/modules/sdlc/pipeline.rs`
  (`Pipeline::run`, `Producer`, `Loader`, `Extractor`, `Transformer`)
- Transform spec today: `crates/indexer/src/modules/sdlc/plan/mod.rs` (`Transformation`,
  `Cursor`, filters); ontology → SQL in `plan/lower.rs`
- Generic handler: `crates/indexer/src/modules/sdlc/handler/entity.rs`
- Datalake query capability: `crates/indexer/src/modules/sdlc/datalake.rs` (`DatalakeQuery`)
- Reuse-infra checklist: `crates/indexer/AGENTS.md`
- SDLC pipeline overview: `docs/design-documents/indexing/sdlc_indexing.md`
