---
title: "GKG ADR 015: Independent extraction and transformation stages"
creation-date: "2026-06-03"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-06-03

## Context

SDLC ontology pipelines declare independent `extract` and `transform` sections. Planning already
compiled those declarations separately, and Arrow `RecordBatch` field names were their only shared
contract. The runtime did not preserve the same boundary: the generic `Pipeline` owned ClickHouse
SQL preparation, keyset pagination, watermark windows, retries, checkpoint cursors, transforms,
and graph writes.

That coupling prevented a non-ClickHouse source from reusing the existing DataFusion or Rust
transforms. An API or Git extractor would have needed to emulate ClickHouse query and checkpoint
internals even though its only useful output is a sequence of Arrow batches.

The existing runtime also carries performance behavior that must remain shared and unchanged:

| Behavior | Required shape |
|---|---|
| Initial-load partitioning | Extraction sessions run concurrently |
| Page buffering | Page N+1 extraction overlaps page N writes |
| Memory bound | The runner holds roughly two source pages per session |
| Graph writes | Transformed batches are grouped into one bulk write per destination and page |
| ClickHouse retries | Block sizes follow the existing halving sequence; Arrow string overflow jumps to the configured minimum |
| Query behavior | Compiled SQL, parameters, sort keys, and batch sizes do not change |

## Decision

Keep one generic SDLC runner, but make extraction and transformation independent runtime stages.
Arrow `RecordBatch` values are the only data contract between them.

### Planning boundary

`Plan` contains an `ExtractPlan` and a `TransformSpec`. `ExtractPlan` is a typed enum whose current
variant is `ClickHouseExtractPlan`. The ClickHouse compiler owns source columns, lookup joins,
query templates, lifecycle columns, sort keys, and batch sizing. Transform compilation owns only
the fields and graph outputs it reads from the extracted batches.

The plan builder assembles both outputs but neither compiler imports the other. Adding another
source requires another `ExtractPlan` variant and source compiler; it does not change transform
declarations or DataFusion SQL.

### Runtime boundary

The source-neutral runtime uses three contracts in
`crates/indexer/src/modules/sdlc/extract/mod.rs`:

```rust
#[async_trait]
trait Extractor {
    async fn start_extraction(
        &self,
        context: ExtractRunContext,
    ) -> Result<ExtractRun, HandlerError>;
}

#[async_trait]
trait ExtractSession {
    async fn get_next_page(&mut self) -> Result<Option<ExtractPage>, HandlerError>;
    async fn save_page_resume(&self, resume: &ExtractResume) -> Result<(), HandlerError>;
    async fn save_completed(&self, durability: WriteDurability) -> Result<(), HandlerError>;
}

struct ExtractPage {
    batches: Vec<RecordBatch>,
    resume: ExtractResume,
    stats: ExtractPageStats,
    has_more: bool,
}
```

An `Extractor` starts one or more sessions. Multiple sessions preserve parallel initial-load
partitions without exposing source partition types to the runner. Each session owns page creation,
resume state, and completion persistence. `ExtractRunCompletion` performs source-level work after
all sessions finish, such as consolidating ClickHouse partition checkpoints.

The generic `Pipeline` in `crates/indexer/src/modules/sdlc/pipeline.rs` owns only orchestration:

1. Start source sessions.
2. Build one transform per session.
3. Transform each page's Arrow batches.
4. Bulk-write batches grouped by destination table.
5. Overlap the next source page with current graph writes.
6. Persist page progress only after graph writes and the overlapping read complete.
7. Finish the source run after every session succeeds.

This keeps extract and transform implementations independent. A memory, API, or Git extractor can
feed the same `TransformSpec`; a different transform can consume ClickHouse pages without learning
how ClickHouse pagination or checkpoints work.

### ClickHouse implementation

`ClickHouseExtractor` owns all ClickHouse-specific runtime behavior:

- watermark-window selection and traversal-path filters;
- initial-load partition range computation;
- keyset cursor SQL and query parameters;
- page scan accounting;
- adaptive `max_block_size` retries;
- source resume encoding and checkpoint persistence;
- completed-partition checkpoint consolidation.

The generic runner does not inspect SQL, cursors, partition assignments, or checkpoint payloads.
The handler constructs the typed extractor once and passes only dispatch context to it. There is no
extractor registry while ClickHouse remains the only production source.

### Resume compatibility

`Checkpoint` stores a watermark plus an opaque source resume string. The physical ClickHouse column
remains `cursor_values`, so this decision requires no DDL migration. New resumes contain a source
name, version, and source-owned payload. `ClickHouseExtractor` also decodes the previous compact
`{"c": [...], "f": ...}` cursor format so interrupted runs survive rollout.

Completed checkpoints continue to store `"null"` or an empty value. Progress checkpoints remain
fire-and-forget, and completion durability still follows `RunDurability`.

### Transform implementation

`BlockTransform` remains the transform seam. `TransformSpec::DataFusion` builds a
`DataFusionTransform`; `TransformSpec::Rust` resolves a registered factory. Transform dependencies
are captured by their factories, and transforms read only the fields present in each extracted
`RecordBatch`.

## Consequences

What improves:

- A source can change without changing transform compilation or execution.
- A transform can change without changing source pagination or checkpoint behavior.
- The generic runner no longer imports ClickHouse query, cursor, retry, partition, or checkpoint
  types.
- Source-neutral tests can prove concurrency and read/write overlap without ClickHouse.
- The ClickHouse and in-memory extractors can feed the same unchanged DataFusion transform.

What gets harder:

- Source implementations must define resumable page semantics and completion behavior.
- A run has a small amount of dynamic dispatch per session and page. Row processing remains inside
  Arrow, DataFusion, and bulk writers rather than crossing the trait boundary per row.
- ClickHouse rollout must preserve legacy checkpoint decoding until old in-progress cursors are no
  longer present.

## Non-goals

- **An extractor registry.** Typed handler assembly is sufficient for one production source.
- **A loader seam.** Destination writing remains part of the shared runner.
- **Ontology changes.** Existing `extract.type: clickhouse` declarations already identify the
  source compiler; this runtime refactor does not change YAML.
- **Different ClickHouse SQL.** Source ownership changes where queries run, not how plans compile.
- **Code and namespace-deletion pipelines.** They remain outside the SDLC entity-plan path.

## Relationship to ADR 014

ADR 014 defines entity-level dispatch and initial-load partitioning. This decision keeps that
dispatch model and moves source-specific execution behind `Extractor` and `ExtractSession`.
`EntityHandler` still owns request decoding and observer setup; it no longer computes ClickHouse
windows or partitions itself.

## References

- ADR 014: [014_entity_level_indexing.md](014_entity_level_indexing.md)
- System-note Rust transform: [013_system_note_edges.md](013_system_note_edges.md)
- Source contracts: `crates/indexer/src/modules/sdlc/extract/mod.rs`
- ClickHouse source: `crates/indexer/src/modules/sdlc/extract/clickhouse.rs`
- Source-neutral runner: `crates/indexer/src/modules/sdlc/pipeline.rs`
- Handler assembly: `crates/indexer/src/modules/sdlc/handler/entity.rs`
- Plan assembly: `crates/indexer/src/modules/sdlc/plan/build.rs`
- Extract compilation: `crates/indexer/src/modules/sdlc/plan/extract/`
- Transform compilation: `crates/indexer/src/modules/sdlc/plan/transform.rs`
- SDLC indexing overview: [../indexing/sdlc_indexing.md](../indexing/sdlc_indexing.md)
