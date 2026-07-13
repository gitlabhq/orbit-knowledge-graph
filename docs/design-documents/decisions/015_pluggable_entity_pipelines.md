---
title: "GKG ADR 015: Pluggable transforms over a shared SDLC pipeline"
creation-date: "2026-06-03"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-06-03

## Context

Before this change, the SDLC indexer's transform stage was SQL-only. Every entity
flows through one generic `EntityHandler`
(`crates/indexer/src/modules/sdlc/handler/entity.rs`) that owns a `Plan` and drives
a shared `Pipeline` (`crates/indexer/src/modules/sdlc/pipeline.rs`). The runtime
pipeline still owns extraction, transformation, writing, and checkpointing, but the
ontology input has been unified: nodes, edges, and derived entities all declare
top-level `pipelines:` entries with `extract` and `transform` sections. The loader
resolves those YAML entries into `ontology::etl::Pipeline { extract, transform }`;
the indexer lowers them into a `Plan` whose `extract_template` is the pipeline's
resolved SQL template and whose `TransformSpec` decides how rows become graph
outputs.

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
| Keyset pagination with DNF cursor predicate (uses the CH sort-key index) | `CursorFilter` (`plan/mod.rs`), `run_plan` page loop (`pipeline.rs`) |
| Watermark windowing + traversal-path scoping pushed into extract SQL | `WatermarkFilter`/`TraversalPathFilter` (`plan/mod.rs:94`, `:122`) |
| Whole-page read with single-page read-ahead — next page's read overlaps current page's writes | `run_plan` `tokio::join!` (`pipeline.rs`) |
| Adaptive retry: shrink `max_block_size` on a datalake failure down to a floor (an Arrow 2 GiB string-offset overflow drops straight to the floor) | `Pipeline::extract_batch` (`pipeline.rs`) |
| Lazy, per-destination-table bulk writers (no insert opened for an empty table) | `Pipeline::build_writes` (`pipeline.rs`) |
| Page-boundary checkpointing + crash-safe cursor resume | `run_plan` (`pipeline.rs`), `Cursor` (`plan/mod.rs:19`) |
| Idempotent re-processing via `ReplacingMergeTree` | graph DDL |
| Read/write stats + observer wiring | `PipelineStats` (`pipeline.rs:50`), `PipelineContext` (`pipeline.rs:68`) |

Crucially, the only entity-specific object inside that machinery was the
`Transformer`, built from a plan's SQL `Transformation` list. Everything wrapped
around it is generic. That is the seam this ADR replaces with a trait.

ADR 014 named an `EntityPipeline` trait with SystemNotes as the custom-pipeline
example. Taken literally — one custom *pipeline* implementation per hard entity —
that framing reintroduces the duplication above. This ADR refines it: the
extension point is the **transform stage**, not the whole pipeline. There remains
exactly one pipeline *type*, and a single shared instance, parameterized per run by
the plan's transform.

## Decision

Keep one generic `Pipeline` that owns all extraction and writing. Make the
**transform** the single pluggable seam, as a trait that can read the datalake.
Every SDLC entity — SQL-projected or hand-written Rust — runs on the same
pipeline; only its transform differs.

### The seam: a `BlockTransform` trait

Replace the concrete `Transformer { transforms: Vec<Transformation> }` with a
trait object the pipeline drives per block, built once per run from the plan's
transform spec (`Pipeline::run`, `transform.rs`):

```rust
#[async_trait]
pub(in crate::modules::sdlc) trait BlockTransform: Send + Sync {
    fn name(&self) -> &str;

    /// Destination tables this transform writes, in output-index order.
    /// Drives the per-table bulk writers; the transform never opens a writer itself.
    fn outputs(&self) -> &[String];

    /// Transform one extracted block into rows for one or more outputs.
    /// Each `TableBatch.output_index` selects an `outputs()` entry.
    async fn transform(&self, block: &RecordBatch) -> Result<Vec<TableBatch>, HandlerError>;
}
```

A transform takes no per-call context. Its dependencies (the datalake handle, any
config) are captured at construction by the registry factory, and the namespace
scope for a transform's own lookups travels in the block's `traversal_path` column.
There is no `TransformContext` parameter threaded through the pipeline.

Two design rules the trait enforces:

- **No `SessionContext` on the trait surface.** DataFusion is an implementation
  detail of the SQL transform and must never leak to a transform that does not run
  SQL. A non-SQL transform must not be handed a DataFusion session.
- **Datalake access is granted at construction, not via pipeline ownership.** The
  multi-hop capability SystemNote needs already exists on `DatalakeQuery`
  (`datalake.rs`: `query_batches`). The registry factory captures that handle when
  it builds the transform, so the transform does *not* need to own pagination,
  checkpointing, or writing to do a second-hop read.

The pipeline drives the transform per block (the page's blocks are fed through it
one at a time and the output rows are grouped per destination table before a single
bulk write). Per-block granularity also naturally bounds a transform's enrichment
`IN`-list to one block rather than a whole page; a transform that needs wider
batching can buffer internally.

### Two implementations of one trait

- **`DataFusionTransform`** — today's SQL behavior, unchanged. Owns a
  `SessionContext` internally (register/deregister take `&self` via DataFusion's
  interior mutability, so no `&mut` and no leak), registers the block as
  `source_data`, runs the ontology-generated SQL list, returns `TableBatch`es.
  Built from a plan's `TransformSpec::DataFusion(Vec<Transformation>)`.
- **`SystemNotesTransform`** (ADR 013, follow-up MR) — hand-written Rust. Parses
  note bodies, collects distinct refs, calls `datalake.query_batches` for the
  `siphon_routes` and entity-table resolution hops, emits edge rows. No DataFusion.
  Its datalake handle is captured at construction.

### Extraction and writing are not duplicated — they are reused as-is

SystemNote needs no bespoke extractor: its source is the `SystemNote` pipeline in
`config/ontology/derived/core/system_note.yaml`, whose extract is an authored
`system_note.sql.j2` — a `_batch` scan of `siphon_notes` plus a page-bounded join over
`siphon_system_note_metadata` for the note action. It rides the same keyset
pagination, watermark window, retry, read-ahead, checkpoint, and streaming-write
path as every other entity. The only Rust-specific code is the transform body. This
is the whole point: a new hand-written entity contributes a `BlockTransform` and an
ontology pipeline, nothing else.

### Output routing

A transform exposes its destination tables via `outputs() -> &[String]`, and
`build_writes` opens one bulk writer per non-empty entry, selected by `TableBatch.output_index`.
`DataFusionTransform` keeps its own dict-encoding (`prepare_batches` over each
`Transformation`'s `dict_encode_columns`); a Rust transform is responsible for
emitting batches that conform to `config/graph.sql`. Centralizing dict-encoding in
the `Loader` so a Rust transform inherits it for free was considered but not
adopted here; it can follow if Rust transforms find it error-prone.

### The transform travels in the plan, resolved per run by a registry

The transform spec lives on the `Plan` itself, as a `TransformSpec`:

```rust
pub(in crate::modules::sdlc) enum TransformSpec {
    DataFusion(Vec<Transformation>),  // built-in SQL projection; the default
    Rust(String),                     // a Rust transform, named, resolved from the registry
}
```

`transform.rs` sets it when the plan builder walks each ontology pipeline: node and
standalone-edge pipelines with `transform.type: datafusion` get `DataFusion(..)`; a
derived entity pipeline gets `Rust(<transform.type>)`.

### Ontology is a declarative model; the indexer owns all SQL

The ontology crate (`crates/ontology`) is a dumb YAML → declarative model: it holds
no ClickHouse SQL and no knowledge of runtime markers. `ExtractQuery` is either
`Generated { filter }` (the indexer builds the SQL from the declaration) or
`Sql(String)` (the raw content of a co-located `.sql.j2` MiniJinja template, carried
verbatim — markers unresolved). Derived-entity pipelines are always `Sql`: their rows
are neither node properties nor edge endpoints, so the indexer has nothing to generate
a projection from. Seven `.sql.j2` files exist — the six genuinely complex nodes (Group,
Project, MergeRequest, Commit, MergeRequestDiffFile, Finding) and the SystemNote
derived entity; every other node and edge is `generated`.

The indexer's `plan` module is the one entry point that turns that model into runs.
`plan/build.rs::build_plans` is the **only** place that reads `pipeline.transform`; it
walks nodes, edge ETL configs, and derived entities, decomposes each pipeline once,
and hands each stage exactly its inputs so data flows top-down:

- `plan/enrichment.rs` is the single `_eN` join convention. Two constructors produce
  the same `EnrichmentJoin` core with different key semantics: `node_ref_joins`
  (edges — side table keyed by its own `id`, matched against an endpoint field) and
  `declared_joins` (nodes/derived — side table keyed by the declared `key`, matched
  against the page `id` and scoped by traversal_path). `build.rs` calls the right one
  per plan and passes the joins to the extract renderer (and, for edges, to the
  transform's denorm mapping).
- `plan/extract/` produces one `ExtractSpec` (validated `ExtractTemplate` +
  effective watermark/deleted + sort key) from `&ontology::Extract` plus
  transform-neutral inputs (typed source columns, `_batch` column lists, enrichment
  joins) computed by `build.rs`. It imports **nothing** from the transform stage.
  Its shape-specific entry points (`extract::node`, `extract::flat`,
  `extract::enriched`, `extract::authored_sql`) are selected by the exhaustive
  `match` on `ExtractQuery` in `build.rs`; `extract/generated.rs` renders SQL,
  `extract/sql.rs` handles the authored escape hatch.
- `plan/transform.rs` builds the `TransformSpec` (node column projection + FK edge
  rows, or a standalone edge row, or a named Rust transform). For an enriching edge
  `build.rs` calls `transform::enriched_denormalized(joins, …)` to map join output
  columns to the transform's private denorm projections. Net dependency direction:
  `build → {enrichment, extract, transform}`, `extract → enrichment`,
  `transform → enrichment`; there is no extract↔transform edge.

`ExtractTemplate::new` is the only way a `Plan` gets its `extract_template`, so an
unvalidated template cannot reach the runtime. A unit test in the `ontology` crate
(`authored_sql_uses_lifecycle_markers_and_aliases`) enforces that every authored
`.sql.j2` file projects `AS _version`/`AS _deleted` and uses
`{{watermark_column}}`/`{{deleted_column}}` markers instead of hardcoding the column
names; projection completeness (order_by and enrich columns) is exercised end-to-end
by the indexer's Docker integration scenarios.

One shared `Pipeline` is built once in `register_handlers` and Arc-cloned to every
handler. It is an Arc-bundle of stateless collaborators (`datalake`,
`checkpoint_store`, `metrics`, `retry_config`), so sharing one instance duplicates
no logic. The pipeline carries a `TransformRegistry`, supplied via `with_registry`.
At the start of each run, `Pipeline::run` calls `registry.build(plan)`:

- `TransformSpec::DataFusion(transforms)` builds a `DataFusionTransform` inline.
- `TransformSpec::Rust(name)` resolves a registered factory by name.

`datafusion` is therefore *not* a registry entry; it is the default arm. The
registry holds only Rust transforms, which self-register from their own module
(the same composition pattern as `*::register_handlers`):

```rust
// register_handlers
let mut transform_registry = TransformRegistry::default();
transform::system_notes::register(&mut transform_registry);  // additive; one line per Rust transform
let transform_registry = Arc::new(transform_registry);

let pipeline = Arc::new(
    Pipeline::new(datalake.clone(), checkpoint_store.clone(), metrics.clone(), retry.clone())
        .with_registry(Arc::clone(&transform_registry)),
);

for plan in plans.namespaced {
    if !transform_registry.is_registered(&plan.transform) {  // unregistered Rust transform → skip
        continue;
    }
    // … register an EntityHandler that drives this shared pipeline for `plan`
}
```

A `Rust` plan whose transform is not registered is skipped at handler registration
(`is_registered`), so a derived entity stays dormant until its transform lands.
Because the spec rides in the `Plan`, the transform never has to thread through
`EntityHandler`'s surface, and the pipeline stays a single shared instance.

**Handler/pipeline split.** The pipeline owns per-page execution (extract,
transform, write, checkpoint). `EntityHandler` owns the `Plan` and its dispatch
decisions — watermark derivation, partition-range computation via `self.datalake`,
request decoding — and passes the plan into `Pipeline::run_plan` per request. The
plan carries its own `TransformSpec`, so the pipeline resolves the transform
without the handler holding one.

## Consequences

What improves:

- The transform stage is no longer SQL-only, and the extraction/writing
  optimizations are inherited by every entity with zero duplication — the explicit
  goal. A new Rust entity is one `BlockTransform` impl plus an extract plan.
- ADR 013 unblocks with a smaller surface than "a whole custom pipeline":
  SystemNote becomes an extract plan + one trait impl.
- No behavior change for existing entities; they run `DataFusionTransform` over the
  same pipeline, built from the same ontology plans.

What gets harder:

- A trait boundary where there was a concrete `Transformer`. Mechanical refactor
  where the `Transformer` was built, plus the `TableBatch` index rename
  (`transform_index` → `output_index`).
- Risk of hand-written-transform proliferation. Mitigation: the default stays
  "express it as an ontology plan + `DataFusionTransform`." A Rust transform is justified
  only when the projection cannot be SQL — concretely, when it needs multi-hop
  datalake reads or cross-row work the SQL projection can't do. Document that bar
  in `crates/indexer/AGENTS.md` beside the reuse checklist.

## Relationship to ADR 014

ADR 014 (Accepted) decided entity-level dispatch and named `EntityPipeline` /
`SimpleEntityPipeline`, with SystemNotes as the custom example. This ADR refines
the *granularity* of that extension point: rather than one custom **pipeline** per
hard entity (which would re-own extraction and writing), the seam is the
**transform stage**. One shared `Pipeline` runs every entity and resolves each
plan's transform from its `TransformSpec`; there is no per-entity pipeline impl.
ADR 014's dispatch model, per-entity NATS subjects, and partitioning are
unaffected.

## Non-goals

- **Per-entity custom pipelines.** Rejected as the duplication source this ADR
  exists to prevent.
- **Exposing DataFusion in the trait.** Stays internal to `DataFusionTransform`.
- **Bespoke extractors/writers for Rust entities.** They reuse the shared pipeline;
  a different source shape is a different extract plan, not new machinery.
- **A transform-type taxonomy in the ontology.** The ontology names the transform in
  each pipeline's `transform.type` (`datafusion` for the built-in path, or a
  registered Rust transform name), but it does not model transform behavior; whether
  a named Rust transform resolves is a registration concern. Edge/node kinds remain
  ontology-declared.
- **Code and namespace-deletion modules.** Out of scope; they sit outside the SQL
  plan path already.

## References

- ADR 014 (Entity-level SDLC indexing) — [014_entity_level_indexing.md](014_entity_level_indexing.md)
- ADR 013 (Materialize edges from system notes); multi-hop resolution shape —
  [013_system_note_edges.md](013_system_note_edges.md)
- Shared pipeline + the transform seam: `crates/indexer/src/modules/sdlc/pipeline.rs`
  (`Pipeline::run`, `Producer`, `Loader`, `Extractor`) and
  `crates/indexer/src/modules/sdlc/transform.rs` (`BlockTransform`,
  `DataFusionTransform`, `TransformRegistry`)
- Ontology pipeline model: `crates/ontology/src/etl.rs` (`Pipeline`, `Extract`,
  `ExtractQuery`, `Transform`, `EdgeMapping`); YAML loading in
  `crates/ontology/src/loading/node.rs`; authored `.sql.j2` marker/alias check in
  `crates/ontology/src/lib.rs` (`authored_sql_uses_lifecycle_markers_and_aliases`)
- Transform spec: `crates/indexer/src/modules/sdlc/plan/mod.rs` (`TransformSpec`,
  `Transformation`, `Cursor`, filters); plan building in `plan/build.rs`; enrichment
  joins in `plan/enrichment.rs`; extract stage in `plan/extract/` (`ExtractSpec`,
  `ExtractTemplate`, `SourceColumn`); transform building in `plan/transform.rs`
- Generic handler: `crates/indexer/src/modules/sdlc/handler/entity.rs`
- Datalake query capability: `crates/indexer/src/modules/sdlc/datalake.rs` (`DatalakeQuery`)
- Reuse-infra checklist: `crates/indexer/AGENTS.md`
- SDLC pipeline overview: `docs/design-documents/indexing/sdlc_indexing.md`
