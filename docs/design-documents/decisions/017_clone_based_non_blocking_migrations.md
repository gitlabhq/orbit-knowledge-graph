---
title: "GKG ADR 017: Clone-based non-blocking schema migrations"
creation-date: "2026-07-13"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-07-13

## Context

Breaking schema changes write to a new set of `v<N>_`-prefixed tables while the
old version keeps serving queries, then promote once the new set is populated.
That layout is described in [`schema_management.md`](../schema_management.md) and
this ADR does not change it.

The problem is cost. The original flow rebuilt *every* table on every
`SCHEMA_VERSION` bump: create the full `v<N>_*` set empty, then re-index every
namespace and repository into it. A one-column change to one SDLC entity cost a
full re-pull of billions of Siphon rows and every code archive before it could
promote — even though most tables were byte-for-byte identical to the old version.

Inferring the invalidation set at runtime (diff the ontology, guess which tables
changed) is unsafe: a missed edge-key change silently leaves a stale row in a
cloned table, uncatchable after the fact. The decision has to be declared and
reviewable.

## Decision

On a forward migration, clone every table a version does not invalidate from the
active set and re-index only the entities it does. The invalidated set is declared
per version in a **migration ledger**, and defaults to a full rebuild whenever it
is ambiguous.

### Ledger declares scope

`config/schema-migrations.yaml` carries one entry per `SCHEMA_VERSION`, prepended
by `mise schema:bump`:

- `scope: "*"` — full rebuild; the fail-safe default for anything unmapped.
- `scope: sdlc` — SDLC tables, with an optional `entities:` list to narrow further.
- `scope: code` — the code-graph tables and their edge table.

A human may widen an entry but never narrow it below the fingerprint-snapshot
drift; `migration-ledger-check` (CI) and the `gkg-server` build script enforce
that floor. A migration across several versions unions their scopes via
`MigrationLedger::resolve_migration_scope_between`, widening to `Full` on any gap
or backward range.

### Runtime widening for shared tables

A declared scope is a request. `get_migration_scope_for_table_writers`
(`crates/indexer/src/schema/invalidation.rs`) widens it to `Full` when a clone
would corrupt data:

- The scope touches the shared `gl_edge` table. Cloning it while rebuilding only
  some of its writers would keep old-key rows the corrected writer no longer emits.
- A rebuilt table has writers outside the scope, whose rows the clone would drop.

`code` is the exception: it clones `gl_edge` intact because the code stale sweep
tombstones code's own edge rows as the re-index drains. So a code bump touches
only code.

`classify_tables_for_scope` then marks each table `CloneFromActive` or
`RebuildEmpty` — rebuilt only when every writer is invalidated.
`find_invalidated_pipelines` maps the invalidated entities to the pipelines that
must rerun (`HAS_NOTE` → the `Note` pipeline).

### Checkpoint seeding drives re-dispatch

The re-index falls out of which checkpoints exist in the new set — the namespace
sweep re-dispatches any pipeline with no completed checkpoint:

- **Selective SDLC**: copy completed checkpoints for unchanged pipelines, drop the
  invalidated ones plus the `dispatch.*` cursors (`seed_sdlc_checkpoint`).
- **Code**: clone the checkpoint intact (keeping `dispatch.*`) and drop only
  `maintenance.code_stale_sweep` (`seed_code_scope_checkpoint`).
- **Full**: empty checkpoint table, so everything backfills.

Control tables like `gkg_schema_version` are never prefixed, cloned, or dropped.

### Promotion gates on the plan

`MigrationCompletionChecker` (`crates/indexer/src/migration_completion.rs`)
promotes the `migrating` version only when every currently enabled namespace has a
completed checkpoint for every required namespaced pipeline, plus every required
global pipeline. "Required" is exactly the plan the scope produced, so a selective
migration gates only on what it re-dispatched.

- A checkpoint from a since-disabled namespace does not count; the enabled set is
  recomputed each check.
- Code coverage is reported but does not block — a single slow or failing repo must
  not hold a migration open.

The gate is checkpoint-based, not row-count-based; full correctness validation
stays in staging E2E (see
[`schema_management.md`](../schema_management.md#known-trade-off-checkpoint-based-validation)).

## Relationship to blue-green deployment

GKG already runs blue-green at the data layer: the active version keeps serving
queries and streaming incremental updates from its `v<N>_` table set while the
migrating version populates its own set, then promotion flips reads atomically.
The two releases run fully side by side — `/ready` keeps migrating-version pods
out of Kubernetes rotation until their version is `active`, and every gkg-owned
NATS stream, subject, and KV bucket carries a version segment so the releases
have independent work queues and locks
([`schema_management.md`](../schema_management.md),
[`indexing/sdlc_indexing.md`](../indexing/sdlc_indexing.md)).

Clone-based migration is what makes that cutover cheap and non-blocking. Without
it the green set starts empty and is only promotable after a full re-index from
epoch — a multi-hour window in which blue alone carries fresh data under heavy
Siphon and Gitaly load. Cloning stands the green set up near-complete in
milliseconds and re-indexes only the invalidated delta, so:

- Blue serves and streams uninterrupted throughout; nothing in the clone or
  force-backfill path touches the active set.
- The window shrinks from re-indexing the whole graph to re-indexing what
  changed, a small fraction of it for a narrow bump.
- Promotion — the cutover trigger — gates on exactly the plan the scope produced
  ([Promotion gates on the plan](#promotion-gates-on-the-plan)), so green goes
  `active` as soon as the delta is complete.

The deployment machinery that runs the two versions side by side is owned by
[`schema_management.md`](../schema_management.md) and
[`indexing/sdlc_indexing.md`](../indexing/sdlc_indexing.md); this ADR covers only
the migration mechanism that populates the green set.

## Consequences

What improves:

- A narrow bump re-indexes only what it invalidates, cutting the migration window
  and the Siphon/Gitaly load.
- The invalidation set is a reviewed artifact with a CI-enforced floor, not a
  runtime guess.
- Fail-safe by construction: unmapped versions, ledger gaps, shared edge tables,
  and cross-scope writers all widen to `Full`.

What gets harder:

- Every breaking bump owns a ledger entry, and a too-narrow scope is a
  data-correctness bug, not just wasted work.
- The shared `gl_edge` table forces many SDLC scopes to `Full`; the savings are
  real for table-local entities and code, less so for edge-heavy changes.
- Two seeding paths (SDLC vs code) with different cursor handling.

## Non-goals

- **Changing the prefix layout or promotion mechanism.** Unchanged; see
  [`schema_management.md`](../schema_management.md).
- **Row-count validation at the gate.** Stays in staging E2E.
- **Runtime ontology diffing.** Scope is declared, only ever widened at runtime.
- **Per-row cleanup for shared SDLC edge tables.** Not built; those changes widen
  to `Full` instead.

## References

- Schema management as-built: [`schema_management.md`](../schema_management.md)
- SDLC migration section: [`indexing/sdlc_indexing.md`](../indexing/sdlc_indexing.md)
- Ledger and scope union: `crates/ontology/src/migrations/ledger.rs`, `scope.rs`;
  `config/schema-migrations.yaml`
- Clone/rebuild classification and widening:
  `crates/indexer/src/schema/invalidation.rs`
- Orchestration, seeding, rollback:
  `crates/indexer/src/schema/migration.rs`, `version.rs`
- Promotion gate: `crates/indexer/src/migration_completion.rs`
- Related: [ADR 014](014_entity_level_indexing.md),
  [ADR 015](015_pluggable_entity_pipelines.md)
- Blue-green deployment epic: [Non-blocking migrations and blue/green deployment](https://gitlab.com/groups/gitlab-org/orbit/-/work_items/7)
