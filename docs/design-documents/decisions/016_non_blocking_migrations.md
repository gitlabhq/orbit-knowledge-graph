---
title: "GKG ADR 016: Non-blocking re-index on schema change"
creation-date: "2026-06-04"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Proposed.

This ADR builds on the in-flight migration framework, not a parallel system:

- `crates/migration-framework/` — trait-registry migrations + `gkg_migrations`
  ledger + NATS lock/reconciler + metrics (feature branches; not yet on `main`).
- The additive schema reconciler (MR !1004, `crates/indexer/src/schema/reconcile.rs`)
  — diffs `ontology.storage` against the live ClickHouse `system.*` tables and
  applies additive `ADD COLUMN` / `ADD INDEX` / `ADD PROJECTION` + codec/default
  `MODIFY` in place.
- The framework design doc (`docs/design-documents/schema-management-framework/`,
  dgruzd) — describes the registry/ledger/lock as V1 and a "convergence" re-index
  layer as V2/V3.

It records two decisions on top of that work: **(1)** build the non-blocking
re-index ("convergence") layer on the existing framework rather than inventing a
new crate or SQL-file migrator, and **(2)** how **code** (repository-parsed)
entities re-index — which the framework design left as an open question.

## Date

2026-06-04

## Context

The indexer evolves its graph schema by bumping one global `u32`
(`config/SCHEMA_VERSION`, embedded via `include_str!` in
`crates/indexer/src/schema/version.rs:40`). That number is overloaded:

1. **Table identity** — every table is prefixed `v54_gl_user`, `v54_gl_edge`,
   `v54_checkpoint` (`table_prefix()`, `version.rs:136`;
   `Ontology::with_schema_version_prefix()`, `crates/ontology/src/lib.rs:508`).
2. **Migration trigger** — a higher embedded version creates a brand-new empty
   prefixed table set (`schema::migration::run_if_needed()`, `migration.rs:74`).
3. **Re-index scope** — the new empty `checkpoint` table forces a re-pull of the
   **entire graph**; promotion is gated on checkpoint *presence*
   (`sdlc_count >= enabled_count`, `completion.rs`), then old tables are dropped.

This causes: every change is a full re-index; there is no notion of a delta; and a
correctness hazard — presence-based promotion plus a one-way watermark
(`col > last AND col <= current`, `handler/entity.rs:127`) is the v48 SDLC
data-loss incident (a namespace that committed an in-progress checkpoint counted
toward promotion, the version was promoted and old tables dropped, and the gap
below the watermark became permanently unreachable).

**What already exists toward fixing this** (in-flight, not merged): the
migration-framework provides the durable ledger, cross-replica NATS lock, and a
background reconciler — but ships zero concrete migrations and runs hand-written
DDL. MR !1004 provides the real ontology-derived **additive DDL reconciliation**.
The framework design doc proposes the re-index layer but no code implements it.

**The gap this ADR targets:** none of the existing tracks re-index data after a
schema change. There is no per-entity schema fingerprint, no entity-level SDLC
re-index, and — critically — no story for **code** entities. The framework design
flags the code path as an explicit open question. That is the missing piece.

## Decision

### 1. Build on the existing framework; do not fork it

DDL changes are applied **in place** by the additive reconciler (!1004), recorded
in the `gkg_migrations` ledger and serialized by the migration-framework NATS lock
(`crates/migration-framework/`). We do **not** add a separate `gkg-migrations`
crate, and we do **not** adopt the SQL-file migrator (`crates/migrations/`,
`feat/setup-migrations`). The reconciler's additive-only safety gate stays: a new
column is applied only if it is `Nullable` or has a `DEFAULT` (`reconcile.rs:219`);
sort-key / incompatible type / column-removal changes remain out of scope and
require a deliberate, out-of-band baseline change.

The generator's in-place/breaking split was validated against the ClickHouse
`ALTER` reference (`sql-reference/statements/alter/{column,skipping-index,
projection,order-by,setting}`):

- **Applied in place** (metadata, no row rewrite): `CREATE TABLE`; `ADD COLUMN`;
  `MODIFY COLUMN` for **default/codec-only** drift (leaves existing data alone);
  `ADD`/`DROP INDEX`; `ADD`/`DROP PROJECTION`. A changed index or projection is
  re-created as drop-then-add.
- **Refused as breaking**: a column **type** change (a rewriting mutation, with
  `Nullable`→non-`Nullable` hazards); a dropped column or table (data loss — see
  the finalization path below); `ORDER BY`, primary-key, and engine changes (no
  safe in-place path — `MODIFY ORDER BY` only *appends* a brand-new column); and
  settings changes (`MODIFY SETTING` covers most, but immutable settings such as
  `index_granularity` would fail at apply, so auto-generation is excluded).

This matches the in-flight reconciler (!1004), which applies codec/default, index,
and projection changes in place and treats type/sort-key/column-removal as needing
a version bump. Generated `down` is best-effort: `MODIFY COLUMN` overrides a codec
or default but does not `REMOVE` one, so reverting an *added* property is not
symmetric.

### 2. The non-blocking re-index ("convergence") layer — the new work

This is what no track implements yet, and it aligns with the framework design's V2:

- **Per-scope schema version + fingerprint.** A migration that changes an entity's
  stored shape advances a target version; each indexing scope records the schema
  version it was last indexed at (the design's `checkpoint.schema_version` /
  `gkg_migration_scopes`). The fingerprint is **per entity type** (hash of its
  columns, types, sort key, and ETL source mapping), so we can tell *which*
  entities a migration actually changed.
- **SDLC entity-level re-index by stale-scope detection.** A convergence dispatcher
  finds scopes whose recorded version is below the target for a changed entity and
  re-pulls **only those** (`schema_version < target`). Unchanged entities keep
  their checkpoints, so their incremental indexing **never pauses** — the
  property we get for free by never wiping the checkpoint table wholesale. The
  re-indexed entity converges back to live through the existing cursor/watermark
  state machine (`handler/entity.rs:127`).
- **No promotion gate.** In place + additive means there is no table swap to gate;
  an incomplete re-index is recoverable staleness, not the v48 permanent loss.

### 3. Code entities re-index per project, not per entity — see below

The convergence model above is datalake-backed and entity-scoped. Code does not fit
it, and resolving that is decision (2) of this ADR.

## Code indexing re-indexes per project, not per entity

SDLC entities are datalake-backed: each has its own checkpoint and is re-pulled
independently from ClickHouse, so stale-scope detection per entity works. Code-graph
entities — `Definition`, code edges (references), `Import`, `File`, `Directory` —
do not work that way:

- They are produced **together** by parsing a repository archive, behind a single
  per-`(project, branch)` checkpoint (`code_indexing_checkpoint`), not per-entity
  checkpoints. There is no "re-pull just `Definition`"; you re-parse the whole repo.
- Code indexing is incremental per **commit** (`last_commit`). After a shape change
  the existing rows are in the old shape, and a diff-from-`last_commit` would never
  rewrite them — the project must be re-parsed from scratch at HEAD.
- The trigger is **not only** an ontology/DDL change: a tree-sitter grammar bump, a
  linker change, or an extraction-code change alters parser output with **no DDL
  diff at all**, so it is invisible to a migration that only diffs the ontology.
- Re-parsing every repository is expensive (archive download + parse per project
  across the fleet), so it must be drained **gradually** — "eventually" — while
  incremental code indexing for untouched projects keeps flowing.

Therefore code needs a distinct mechanism from SDLC stale-scope detection:

- A **code-pipeline fingerprint** spanning the entire code-extraction surface — all
  code entity shapes **plus** the grammar / linker / extraction-code version — as a
  single value, because the entities are co-produced. Any change bumps it.
- A **project-level re-index queue**: when the code-pipeline fingerprint changes,
  enqueue every project for a full re-parse, with per-project completion tracking so
  the work is resumable and the fleet-wide reparse has a definite "done".
- `NamespaceCodeBackfillDispatcher` drains the queue **rate-limited**, dispatching
  `CodeIndexingTaskRequest` with a force-full-reparse flag that bypasses the
  incremental `last_commit`.
- Decoupled from promotion (code is already telemetry-only for SDLC promotion).

```text
                 migration-framework (exists)
ontology ─► reconciler (!1004) ─► live tables, additive ALTER, in place
   │           gkg_migrations ledger + NATS lock
   │
   ▼  fingerprints (NEW — this ADR)
   ├─ SDLC entity X changed ─► re-pull scopes where scope.schema_version < target
   │                           (unchanged entities keep indexing — free)
   └─ code-pipeline changed ─► enqueue ALL projects for full reparse,
                               drained rate-limited by code backfill dispatcher
                               (force reparse, bypass last_commit)
```

## Safe drops: the finalization path

The generator never emits a `DROP TABLE` or `DROP COLUMN` — those are destructive
and are handled as a separate, gated **finalization** step (the framework's
`MigrationType::Finalization`, which the reconciler currently refuses to execute;
that refusal is the interlock). A drop runs only after the additive rollout is
fully adopted, following expand → deprecate → soak → contract:

1. **Expand** — the new shape is added and re-indexed (the in-place path above);
   the old column/table still exists.
2. **Deprecate** — the column/entity leaves the ontology's active read and ETL
   surface, but the physical object is *retained*, not dropped. Nothing reads or
   writes it; it is orphaned but intact. This needs a "retained" state distinct
   from "removed" so the generator neither exposes it nor tries to drop it.
3. **Soak** — confirm via telemetry that nothing references or writes it, across
   enough deploys that the rollback horizon passes (no serving binary still expects
   the old shape). Deprecate and drop must therefore be **different releases**.
4. **Contract** — a deliberate, approved `Finalization` migration runs the physical
   `DROP`, recorded in the ledger off the automated path.

Recovery nets that make the drop safe:

- `DROP TABLE` has a window: on an Atomic database (CH 23.3+), `UNDROP TABLE` within
  `database_atomic_delay_before_drop_table_sec` (default 8 minutes) restores it, and
  dropped tables are listed in `system.dropped_tables`
  (`sql-reference/statements/undrop`). Back up for longer safety.
- `DROP COLUMN` has no undo (`sql-reference/statements/alter/column`). The real net
  is **re-derivation**: the graph is read-only-derived from the datalake and repos,
  so a wrongly-dropped column or table can be rebuilt by re-indexing — provided the
  datalake data *and* the ETL mapping are kept until the drop is confirmed good.
  Order the steps so the physical drop is last and the mapping is retired only after
  verification; a drop is then recoverable cost (a re-index), not permanent loss.

## Why not the alternatives

### A separate `gkg-migrations` crate / SQL-file migrator

Earlier drafts of this ADR proposed a standalone crate with timestamped `up`/`down`
SQL files and an ontology-diff → `ALTER` engine. Both already exist: the SQL-file
migrator on `feat/setup-migrations` (`crates/migrations/`) and the ontology-derived
additive differ in MR !1004. Building a third is exactly the "reinvent infra the
codebase already provides" anti-pattern. Rejected in favor of building on
migration-framework + !1004.

### Ad-hoc checkpoint-key tombstoning for re-index

An earlier draft proposed deleting `*.X` checkpoint keys to force re-index. We
prefer the framework design's **scope schema-version** approach (a version column +
`schema_version < target` detection): it is idempotent, observable, survives
restarts, and is already designed — whereas key deletion is a side effect with no
record of intent.

### Keep the global version swap / shadow-table RENAME / non-additive in place

The global swap is the status quo whose costs (full re-index, v48) motivate this
work. A per-entity shadow-table + `RENAME` would preserve zero-downtime for
breaking changes but adds machinery for a rare case; additive-only is simpler.
Non-additive in-place changes cannot preserve query correctness during a rewrite
and reintroduce the swap hazard. All deferred to an out-of-band baseline path.

## Consequences

### What improves

- A one-column ontology addition becomes one additive `ALTER` (reconciler) plus a
  re-index of only the changed entity, not a full-graph rebuild.
- Incremental indexing for unchanged entities never pauses across a schema change.
- The v48 failure mode is structurally impossible for additive changes: no
  promotion gate, no table drop → incomplete re-index is recoverable staleness.
- Code re-parse becomes a tracked, gradual, fleet-wide process with a definite
  completion, instead of an implicit side effect.

### What gets harder

- Two re-index mechanisms to maintain: entity-scoped (SDLC) and project-scoped
  (code), with different fingerprint granularities.
- The code-pipeline fingerprint must capture non-ontology inputs (grammar, linker,
  extraction code), so a code change with no DDL diff still triggers reparse.
- During an additive backfill, queries can read `DEFAULT`/stale values on
  not-yet-re-indexed rows — eventual consistency by design; must be documented for
  query consumers.
- Breaking schema changes remain out-of-band, not self-service.

## References

- Migration framework design: `docs/design-documents/schema-management-framework/`
  (dgruzd) — registry/ledger/lock (V1), convergence (V2), finalization (V3)
- Migration framework crate: `crates/migration-framework/` (`feat/migration-*`)
- Additive schema reconciler: MR !1004 (`feat/additive-schema-reconciliation`,
  `crates/indexer/src/schema/reconcile.rs`)
- SQL-file migrator (alternative, not chosen): `crates/migrations/`
  (`feat/setup-migrations`)
- ADR 014: Entity-level SDLC indexing (`014_entity_level_indexing.md`)
- ADR 015: Pluggable transforms over a shared SDLC pipeline (`015_pluggable_entity_pipelines.md`)
- Work item: gitlab-org/orbit/knowledge-graph#748 (Non-blocking migrations)
- Checkpoint store and watermark logic: `crates/indexer/src/checkpoint.rs`,
  `crates/indexer/src/modules/sdlc/handler/entity.rs`
- Code indexing checkpoint: `crates/indexer/src/modules/code/checkpoint.rs`
- v48 data-loss incident: prod schema-migration data loss (2026-05-29)
</content>
