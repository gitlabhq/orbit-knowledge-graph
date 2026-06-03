---
title: "GKG ADR 013: Materialize edges from system notes"
creation-date: "2026-05-18"
authors: [ "@dgruzd" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-05-18

## Context

The current Knowledge Graph drops a large class of cross-entity relationships before they reach `gl_edge`. `config/ontology/nodes/core/note.yaml` filters with `where: "system = false"`, which silently excludes every system note Rails writes when an issue is closed, a merge request is merged, a commit is added to an MR, or one entity references another in free text. The graph-completeness epic and Angelo's "you don't have anything useful" verdict from the 2026-04-20 Orbit sync both name this gap. The concrete shapes missing today are MR<->MR mentions, MR<->WorkItem mentions, MR<->Commit linkages from `commit`/`merge` actions, and the `REOPENED` lifecycle transition (the `MERGED` and `CLOSED` slices exist via FK but are sparse on older data).

Three things make this hard. The structured discriminator, `system_note_metadata.action`, is **not yet replicated** into Siphon (`siphon_notes` exists; `siphon_system_note_metadata` does not, verified by absence from `fixtures/siphon.sql` and from the Siphon repo's sample config). The target entities of cross-references are encoded as GFM reference tokens inside the **free-text body** (e.g. `mentioned in !123`, `mentioned in group/subgroup/project#456`, `mentioned in 54f7727c`), not as structured foreign keys. And the source data is large: [`gitlab-org/orbit/knowledge-graph#499`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/499) cites ~6.7M system notes for `gitlab-org` alone, against a ~4TB global notes table.

The previous attempt at this work, [!1109][prev-mr], reached green CI and was then closed by the author on 2026-05-18 with *"I'll close this MR, and open an MR with an ADR first."* That MR scoped itself to the lifecycle subset (`merged`, `closed`, `reopened`), added a `siphon_system_note_metadata` fixture, and materialized one pre-filtered ClickHouse view per action (`siphon_system_note_merged`, `siphon_system_note_closed`, `siphon_system_note_reopened`) so the existing standalone-edge ETL machinery could consume them. The closure was process-driven, not correctness-driven.

Three inputs from that MR: (a) standalone edge ETL has **no `WHERE` clause** in `config/schemas/ontology.schema.json`'s `edgeEtlConfig` definition, which forces a per-action view pattern that does not scale to 10+ cross-reference actions × 3 noteable types; (b) the lifecycle-only slice has small marginal value because `merge_user_id` and `closed_by_id` FKs already cover most of it (only `REOPENED` is novel ground); (c) the Siphon prerequisite was acknowledged in the MR description but never filed as a Siphon-side issue, so the MR would have been inert in production.

[prev-mr]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1109

Constraints: Rails owns authorization and is the source of system note bodies, so we do not control templates and must accept Rails-side phrasing changes as a maintenance cost. The Analytics team owns Siphon, so the new source-table replication is cross-team coordination on the critical path. The v0.5 migration framework from [`gitlab-org/orbit/knowledge-graph#443`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/443) is complete and validated on staging, so adding new edge kinds is a `SCHEMA_VERSION` bump, not a table rebuild.

This ADR proposes a Rust extraction handler running inside the SDLC indexer, with two batched ClickHouse lookups for entity resolution, gated on a one-time Siphon-side replication of `system_note_metadata`. The recommendation is backed by a POC harness ([!1335][poc-mr]) that measured parser throughput on both a 21-entry synthetic golden corpus and a 74,125-note GDK-seeded real corpus, plus an end-to-end ClickHouse resolver pass against a local GDK instance.

[poc-mr]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335

## Decision

Build a dedicated Rust system-notes handler at `crates/indexer/src/modules/sdlc/handler/system_notes/`. Read `siphon_notes` joined to `siphon_system_note_metadata` on `note_id` (Mode A) or fall back to body-text filtering (Mode B) when the join target is unavailable. Parse the Rails `TYPES_WITH_CROSS_REFERENCES` subset (10 actions) plus `commit` (Markdown SHA list) and `merge` (auto-merge inline SHA, MR-ref fallback).

Resolve targets with two batched IN-list queries against `siphon_routes` and the entity tables (`siphon_merge_requests`, `siphon_issues`, `siphon_work_items`). Emit `MENTIONS`, `RELATED_TO` (supplemental), `ADDS_COMMIT`, `MERGED_AT_COMMIT`, `CLOSED` (supplemental), `MERGED` (supplemental), and a new `REOPENED` edge kind via the standard `gl_edge` writer.

Vendor Rails' `ICON_TYPES` constant with a CI drift check modeled on `scripts/check-goon-format-version.sh`. Ship behind a feature flag with staging benchmarks 2–5 (lookup latency, end-to-end pass, edge-density gain) as the gate to GA.

### Scope and edge kinds

| Edge | From → To | Source action(s) | Body token to extract |
|---|---|---|---|
| `MENTIONS` | MergeRequest / WorkItem / Commit → same (cross-typed) | `cross_reference` | GFM ref after `"mentioned in "` |
| `RELATED_TO` (supplement) | WorkItem <-> WorkItem | `relate`, `unrelate` | GFM ref after `"marked … as related to"` / `"removed the relation with"` |
| `MENTIONS` (parent/child) | WorkItem → WorkItem | `relate_to_parent`, `relate_to_child`, `unrelate_from_parent`, `unrelate_from_child` | GFM ref + relation type |
| `MENTIONS` (lifecycle moves) | WorkItem → WorkItem | `moved`, `cloned`, `duplicate` | GFM ref after verb phrase |
| `ADDS_COMMIT` | MergeRequest → Commit | `commit` | Markdown list of SHAs |
| `MERGED_AT_COMMIT` | MergeRequest → Commit | `merge` (auto-merge variant) | SHA inline |
| `MERGED` (supplement to FK) | User → MergeRequest | `merged` (lifecycle string) | n/a |
| `CLOSED` (supplement to FK) | User → MergeRequest, WorkItem | `closed` | n/a |
| `REOPENED` (new edge kind) | User → MergeRequest, WorkItem | `reopened` | n/a |

Explicit non-goals: `@`-mention edges (separate `*_user_mentions` tables, tracked under a follow-up to [`gitlab-org/orbit/knowledge-graph#482`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/482)), resource state / label / milestone events (dedicated `resource_*_events` tables under #482), Banzai HTML rendering (the parser is regex-only on plain text), external (Jira) issue references, and the Siphon-side replication MR itself.

### Handler architecture

```plaintext
crates/indexer/src/modules/sdlc/handler/system_notes/
  mod.rs            // process_batch / plan_for_batch pure core + ExtractedNote
  handler.rs        // SystemNotesHandler (thin Handler: builds a Plan + calls
                    //   Pipeline::run_plan) + SystemNotesTransform (PageTransform)
  extract.rs        // SQL for the JOIN(siphon_notes ⋈ siphon_system_note_metadata)
  parse.rs          // Regex + per-action dispatch (lifted verbatim from xtask POC)
  resolve.rs        // siphon_routes IN-list, (project_id, iid) IN-list, ResolvedIndex
  emit.rs           // Edge row construction → gl_edge writer
  vendored/icon_types.rs  // Vendored copy of Rails ICON_TYPES, pinned SHA
```

> **Implementation note (as built).** The registration below is described
> against an ADR 014 `EntityPipeline` extension point. That extension point
> is not on `main` — the merged ADR 014 work (!1341) shipped a no-op
> scaffold, and the `EntityIndexingHandler`/`EntityPipeline` routing layer
> lives in a still-draft MR stack (!1349 → !1360 → !1362). It is also not a
> prerequisite. The handler instead ships as a thin
> [`crate::handler::Handler`] (`SystemNotesHandler`) registered through
> `HandlerRegistry::register_handler`, riding the existing
> `NamespaceIndexingRequest` subscription. **It does not hand-roll its own
> paging loop**: it builds a `Plan` whose transform stage is a page-wise
> `PageTransform` (the `SystemNotesTransform` parse/resolve/emit core) and
> calls the shared `modules::sdlc::pipeline::Pipeline::run_plan`, the same
> orchestration the ontology entity handlers use. The shared pipeline owns
> windowed extraction, the keyset `Cursor`/`CursorFilter`, retry/halving,
> the streaming inserts, and per-page checkpoint cadence. `PageTransform` is
> the seam that lets a handler plug a Rust transform into that pipeline:
> entity plans use a block-wise SQL transform (constant memory); system-notes
> uses a page-wise Rust transform because its resolution batches references
> across the whole page (routes + MR + work-item `IN`-lists), so it buffers
> one page before transforming. Like the entity handlers, it drains the
> whole watermark window to completion with per-page checkpoints (no bespoke
> per-message page cap). The forward-compatibility analysis below is retained
> as the eventual target shape, not the current one.

Under ADR 014's entity-level SDLC dispatch (scaffolded in [!1341][adr014-mr]), each entity-kind dispatched by `EntityDispatcher` flows through a single shared `EntityIndexingHandler`, which routes by `entity_kind` to a per-kind pipeline. ADR 014 introduces `SimpleEntityPipeline` as the default plan-driven pipeline and names SystemNotes specifically as the motivating example for the **`EntityPipeline`** custom-pipeline extension point:

> *"All current entities use `SimpleEntityPipeline` … Future entities (e.g., SystemNotes) can implement `EntityPipeline` with custom logic instead of using `SimpleEntityPipeline`."* — ADR 014, "Handler and pipeline"

ADR 013's `SystemNotesPipeline` is that custom impl. It receives an `EntityIndexingRequest` (`entity_kind = "SystemNote"`, `scope = IndexingScope::Namespace { namespace_id, traversal_path }`, `partition = None` for v1) on `sdlc.entity.indexing.requested.SystemNote.{dotted_traversal_path}` and applies the two-stage extract → resolve → emit pipeline below. See [Compatibility with entity-level SDLC indexing (ADR 014)](#compatibility-with-entity-level-sdlc-indexing-adr-014) for the full forward-compatibility analysis.

[adr014-mr]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1341

This is a **custom `EntityPipeline` implementation**, not an ontology-driven plan. The custom-pipeline path is the *intended* extension point for entities whose ETL shape (multi-edge dispatch off body parsing, cross-typed targets) cannot be expressed in YAML. The departure from the ontology-first convention (AGENTS.md: *"New entity types start there, not in Rust"*) is deliberate and bounded:

- Edge ETL YAML has no `WHERE` clause (verified against `config/schemas/ontology.schema.json::edgeEtlConfig`, which has `additionalProperties: false` and exposes only `scope`, `source`, `watermark`, `deleted`, `order_by`, `from`, `to`).
- A single source table needs to dispatch into ~9 edge variants whose target type depends on body parsing, not on a fixed source-column enum.
- The 10-cross-reference-action × 3-noteable-type expansion through the only available alternative (per-action ClickHouse views, MR !1109's pattern) would land ~30 redundant projections in `fixtures/siphon.sql`. Adam's guidance on the Siphon side ("skip indexes, not projections") argues against that shape generally.

Edge **kinds** are still declared in ontology YAML (`config/ontology/edges/{mentions,adds_commit,merged_at_commit,reopened}.yaml`); only the ETL **logic** is Rust. Existing edges that get new supplemental writes (`MERGED`, `CLOSED`, `RELATED_TO`) gain a documented comment pointing at the system-notes handler as an additional emitter.

#### Why `CLOSED` / `MERGED` are re-emitted, and their mixed `_version` provenance

`closed.yaml` (`fk_column: closed_by_id`) and `merged.yaml` (`fk_column: merge_user_id`) already materialize `User → entity` edges from the FK columns, which hold only the **last** closer/merger. The system-note path is intentionally additive: `system_note_metadata` records **every** lifecycle transition, so a workitem closed → reopened → closed again by two different users yields two distinct `(source_id=user, target=entity, CLOSED)` edges — historical coverage the FK column cannot express. This is the same reason `REOPENED` is net-new (Rails has no `reopened_by_id` FK at all).

`gl_edge` is `ReplacingMergeTree(_version, _deleted)` keyed on `(traversal_path, relationship_kind, source_id, target_id, source_kind, target_kind)`. When both pipelines emit the *same* (last-closer) edge key, the rows collapse to one and the survivor's `_version` is whichever pipeline wrote the larger timestamp — the FK ETL stamps the entity's replicated/updated time, the handler stamps the note's `created_at`.

**This mixed `_version` provenance for the duplicated last-closer edge is expected and benign:** the edge's existence and endpoints are identical from both sources, and `_version` only drives ReplacingMergeTree dedup, not query results. Distinct historical closers have distinct `source_id`s and never collide. A future reader should not "fix" the apparent duplication by dropping the handler's `CLOSED`/`MERGED` emission — that would lose the historical-closer coverage.

### Extraction pipeline

```plaintext
for batch in extract():       # paginated, watermark-bounded, traversal-path-scoped
  parsed = []
  for row in batch:           # siphon_notes ⋈ siphon_system_note_metadata
    action = row.action       # Mode A
    if action not in TARGET_ACTIONS:
      continue
    refs = parse_dispatch(action, row.note, row.namespace_traversal_path)
    parsed.push((row.id, row.note_id, row.noteable_id, row.noteable_type,
                 row.author_id, action, refs))

  # Two batched lookups (same pattern as namespace_deletion/store.rs)
  paths = distinct(ref.full_path for refs in parsed for ref in refs if ref.kind != Commit)
  routes = ch.query("""
    SELECT source_type, source_id, path, traversal_path
    FROM siphon_routes
    WHERE _siphon_deleted = false
      AND startsWith(traversal_path, {traversal_path:String})
      AND path IN ({paths:Array(String)})
  """)

  iid_pairs = distinct((route.source_id, ref.iid) for ...)
  entity_ids = ch.query("""
    SELECT id, iid, target_project_id AS project_id FROM merge_requests
    WHERE _siphon_deleted = false
      AND startsWith(traversal_path, {traversal_path:String})
      AND (target_project_id, iid) IN ({pairs:Array(Tuple(Int64,Int64))})
    UNION ALL
    SELECT id, iid, project_id FROM issues
    WHERE ... (analogous)
  """)

  edges = build_edges(parsed, routes, entity_ids)
  emit(edges)
```

Dedup key: `(system_note_metadata.id, edge_kind, source_kind, source_id, target_kind, target_id)`. ReplacingMergeTree handles re-processing idempotency. The two-stage `IN (…)` resolution pattern is precedented at `config/ontology/nodes/core/project.yaml:115-121` and `config/ontology/nodes/core/group.yaml:101-107` (both resolve in the inverse direction `source_id → path` via `siphon_routes`).

**Default-project resolution for unqualified refs.** Each `siphon_notes` row carries `noteable_id` and `noteable_type`. The handler resolves that pair to the source entity's `project_id` (via `siphon_merge_requests` / `siphon_issues` / `siphon_work_items`) and looks up that project's `traversal_path` from `siphon_routes`; the resulting path becomes the scope for unqualified GFM references on that row (`!N`, `#N`, short SHAs). The bench harness's `--default-project` flag is a harness-only artefact — the production handler derives the default per-row from the noteable, and does **not** call Gitaly to validate commit SHAs.

**Future resolver shape: graph-DB dictionaries.** Once the graph DB carries a projected, licensed-namespaces-only view of routes, the path-resolution step can be replaced by `dictGetOrDefault('project_traversal_paths_dict', 'traversal_path', PROJECT_ID, '0/')` — a strictly cheaper shape than the `siphon_routes` IN-list. This is left as a v2 lever (see [Future optimization: graph-DB-side lookup dictionaries](#future-optimization-graph-db-side-lookup-dictionaries)).

### Mode A / Mode B

Both modes operate exclusively on **system-authored notes** — they read `siphon_notes` with `WHERE system = true AND _siphon_deleted = false`. The current `config/ontology/nodes/core/note.yaml` ETL is the exact inverse (`where: "system = false"`, feeding the user-note `Note` node table); the system-notes handler is the first ETL that consumes the complementary half of the `siphon_notes` table.

- **Mode A (preferred, production default):** join `siphon_notes` to `siphon_system_note_metadata` on `note_id` and filter `snm.action IN (TARGET_ACTIONS)`. The join is needed because `siphon_system_note_metadata` carries only `(note_id, action, commit_count, description_version_id)` — the body, noteable (`noteable_id`, `noteable_type`), and author live on `siphon_notes`. The `system = true` constraint is implicit on the join (the join target only contains system rows) but is still emitted on `siphon_notes` for query-shape clarity. This is the shape the handler is designed for.
- **Mode B (degraded fallback):** if `siphon_system_note_metadata` is not yet replicated, the handler reads `siphon_notes WHERE system = true` and dispatches by body content: lifecycle actions are detected by exact equality (`notes.note = 'closed'`, `'merged'`, `'reopened'`), and cross-reference actions are detected with an anchored regex on the body prefix (`^"mentioned in "`, `^"marked this issue as related to "`, etc.). The `system = true` filter is **load-bearing for Mode B's precision claim**: it scopes the prefix-regex away from arbitrary user-typed notes that happen to start with `"mentioned in "`. Mode B is still slower than Mode A (no metadata-based pre-filter) and is intended only to unblock staging benchmarks and demos while the Siphon-side MR is in flight.

The handler reads its mode from config; production deployment uses Mode A.

### Compatibility with entity-level SDLC indexing (ADR 014)

ADR 014 replaced the former `GlobalHandler` + `NamespaceHandler` split with one `EntityHandler` per ontology entity type. Each handler subscribes to the shared global or namespace NATS topic and processes a single entity kind per message. Partitioning for initial loads is configured per entity via `partition_overrides`.

ADR 013's system-notes pipeline is fully compatible with this model: it registers as a namespaced `EntityHandler` via `Plan`, reuses the standard checkpoint key format (`ns.{id}.SystemNote`), and can opt into partitioning later via `partition_overrides.SystemNote: N`.

### Action-coverage drift mitigation

Three-layer defence:

1. **Vendored constant.** `crates/indexer/src/modules/sdlc/handler/system_notes/vendored/icon_types.rs` carries a literal copy of upstream Rails `ICON_TYPES` (61 values at the time of writing), pinned to a SHA and documented in a header comment.
2. **CI drift check.** `scripts/check-system-note-actions.sh` mirrors the working pattern of `scripts/check-goon-format-version.sh` (ADR 012, the analogous "upstream owns the source of truth, we vendor a copy" problem): the script fetches the upstream `system_note_metadata.rb`, diffs the `ICON_TYPES` array against the vendored constant, and fails with an explicit message listing values present upstream but missing locally. Wired into lefthook pre-commit and into the `lint` CI stage.
3. **Runtime safety.** The handler's dispatch is `match action { ... _ => log_and_drop }`, never `panic!`. Unknown actions surface as a new metric `gkg.indexer.sdlc.system_notes.unknown_action_total{action}` registered in `crates/gkg-observability/src/indexer/sdlc.rs`; cardinality is bounded by `ICON_TYPES` size (~60–100), so a label dimension is safe. See the [metrics step](#implementation-plan) of the implementation plan for the full instrument list.

## Implementation plan

1. **(Parallel, lead time):** File a Siphon-side issue against `gitlab-org/analytics-section/siphon` requesting `system_note_metadata` replication; loop in `@ahegyi @arun.sori`. Specify: skip index on `action`, primary key `(traversal_path, note_id)`, mirror Rails `db/structure.sql` exactly. This work item can run in parallel with ADR review; it does not block "Accepted".
2. Add `siphon_system_note_metadata` to `fixtures/siphon.sql`. The DDL bytes from MR !1109 are reusable; the two fixture-only bugs the author hit there (stray `;` inside a SQL comment that broke the integration-testkit's naive `split(';')` schema runner, and a `USING(note_id)` clause that should be `ON sn.id = snm.note_id`) are documented in the research package and fixed in this round.
3. Vendor `crates/indexer/src/modules/sdlc/handler/system_notes/vendored/icon_types.rs` from upstream Rails at a pinned SHA.
4. Add the CI drift check `scripts/check-system-note-actions.sh` + lefthook hook (model: `scripts/check-goon-format-version.sh`).
5. Add new edge YAML: `config/ontology/edges/{mentions.yaml, adds_commit.yaml, merged_at_commit.yaml, reopened.yaml}`. `RELATED_TO`, `CLOSED`, `MERGED` get a documented comment that the system-notes handler is an additional emitter; no YAML schema change.
6. Register the new edge kinds in `config/ontology/schema.yaml`.
7. Implement the handler at `crates/indexer/src/modules/sdlc/handler/system_notes/`, lifting the parser and resolver from the POC at `crates/xtask/src/system_notes_bench/`. **As built, it is a thin `Handler` (`SystemNotesHandler` in `handler.rs`) that reuses the shared SDLC `Pipeline`** via the `PageTransform` seam — not an `EntityPipeline` (that extension point is not on `main`; see the implementation note above), and not a hand-rolled paging loop. `handle()` builds a `Plan` whose transform stage is `SystemNotesTransform` (a page-wise `PageTransform`) and calls `Pipeline::run_plan`, which owns windowed extraction, the keyset `Cursor`/`CursorFilter`, retry/halving, streaming inserts, and per-page checkpointing. `register_handlers` calls `HandlerRegistry::register_handler` from `modules/sdlc/mod.rs::register_handlers`, alongside the ontology entity handlers. It rides the existing `NamespaceIndexingRequest` subscription and keeps its own checkpoint key (`ns.{id}.SystemNote`). When the ADR 014 `EntityPipeline` slot lands, the `SystemNotesTransform` core moves over unchanged; only the `handler.rs` plan-building shell is replaced.

    Custom-pipeline precedent: ADR 014 names SystemNotes specifically as the motivating example for the `EntityPipeline` extension point. Custom-handler precedent in the existing codebase: `crates/indexer/src/modules/code/`.
8. Metrics. Hook into the existing `gkg.indexer.sdlc.*` catalog (`crates/gkg-observability/src/indexer/sdlc.rs`) wherever an instrument already fits; add two narrowly-scoped new instruments. This directly addresses the review request to "hook ourselves in the existing metrics":

    | Concern | Instrument | Status |
    |---|---|---|
    | Per-batch duration | `gkg.indexer.sdlc.pipeline.duration{entity="SystemNote"}` | Reuse existing |
    | Rows extracted | `gkg.indexer.sdlc.pipeline.rows.processed{entity="SystemNote"}` | Reuse existing |
    | Parse failures | `gkg.indexer.sdlc.pipeline.errors{entity="SystemNote", error_kind="parse_failure"}` | Reuse existing |
    | Edges emitted | `gkg.indexer.sdlc.edges_emitted_total{entity, edge_kind}` | **Add** to `sdlc.rs` (general-purpose; future entities benefit) |
    | Unknown action drift | `gkg.indexer.sdlc.system_notes.unknown_action_total{action}` | **Add**; cardinality bounded by `ICON_TYPES` (~60–100) |

    Catalog regeneration via `metrics-catalog-check`. The two new instruments land in `gkg-observability/src/indexer/sdlc.rs` (not in a system-notes-specific module) so the catalog stays domain-aligned.
9. Bump `config/SCHEMA_VERSION` (currently 44 → 45).
10. Update `docs/design-documents/data_model.md`, `docs/design-documents/indexing/sdlc_indexing.md`, `AGENTS.md`, and `CLAUDE.md` in the same MR (per the AGENTS.md design-doc sync rule).
11. Integration test `crates/integration-tests/tests/indexer/sdlc/notes.rs::materialises_cross_reference_edges`, plus a lifecycle test ported from the closed !1109. The full source of the !1109 lifecycle test is preserved alongside the research package at [`dgruzd/droid-workspace/task/2685`](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685/) so future implementers do not need to spelunk a closed-MR branch.
12. Feature flag (config-driven) defaulting to off; staging-only first. The flag is handler-config-driven and lives where the rest of the handler config lives under ADR 014's entity-dispatch model:

    ```yaml
    handlers:
      entity-handler:
        batch_size_overrides:
          SystemNote: 100000     # ~13s of resolver budget per pass on local GDK
        # No partition_overrides for v1; see "Out of scope".
    ```

    `HandlersConfiguration` uses `deny_unknown_fields` (`crates/gkg-server-config/src/engine.rs`), so the SystemNote-specific knobs must fit inside `entity-handler.batch_size_overrides` (the map key is the entity kind) rather than introducing a new top-level config block. Toggling between staging-on and staging-off is a config push, no code change.

## POC results

POC harness: [!1335][poc-mr], `crates/xtask/src/system_notes_bench/`. **43/43 unit tests pass, clippy `-D warnings` clean, `cargo fmt` clean.** Breakdown:

| Check | Result |
|---|---|
| Parser correctness (16 actions, deep namespaces, multiple refs, lifecycle no-ops, negative cases) | 25 unit tests, all green |
| SQL template shape (named parameters, `startsWith(traversal_path)`, tuple IN-list, `_siphon_deleted = false`) | 8 unit tests, all green |
| In-memory join semantics (path → `source_id` → entity, namespace rows filtered) | 2 unit tests, all green |
| Golden corpus end-to-end through the parser (real Rails-template bodies vendored from `app/services/system_notes/*.rb`) | 3 corpus-level tests, all green |
| `xtask system-notes-bench inspect` smoke against the corpus | Output verified against Rails templates |
| End-to-end against a 74,125-note GDK-seeded real corpus, parser + ClickHouse resolver | All 16 action types round-tripped, zero panics, zero incorrect parses over 75,125 × 100 iterations; CH resolver 3-query batch ≤15 ms at batch=5,000. See [Benchmark 1 (real data)](#benchmark-1--parser-throughput-poc-measured) and [Benchmarks 2–3 (early E2E numbers)](#benchmarks-23--early-e2e-numbers-against-gdk) below. |

### Benchmark 1 — parser throughput (POC measured)

Pure-CPU, single core, release build. Two corpora: the 21-entry synthetic golden corpus and a 74,125-note GDK-seeded real corpus covering all 16 action types (full E2E report: [!1335 (note 3360033462)][e2e-note]).

| Corpus | Notes | Iterations | Median ns/note | Median notes/sec | Refs/pass |
|---|---|---|---|---|---|
| Golden (synthetic) | 21 | 5,000 | 666 | **1,501,501** | 20 |
| Real GDK (seeded) | 75,125 | 100 | 575 | **1,739,130** | 42,155 |

[e2e-note]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335#note_3360033462

The real corpus runs **~16% faster** than the synthetic baseline. The cause is the action mix: about 40% of real GDK notes are lifecycle actions (`closed`, `merged`, `reopened`, `opened`) that short-circuit before any regex work; the golden corpus over-represents multi-ref `relate`/`commit` patterns where the regex actually fires. Real-corpus body length distribution: min 6 chars, median 15, p95 53, max 121, short enough that the regex engine's startup dominates per-note cost.

**Correctness on the real corpus:** 75,125 notes × 100 iterations produced zero panics, zero incorrect parses, and zero false positives across all 16 action types. Unknown actions seeded into the GDK data (`work_item_status`, `assignee`, `start_date_or_due_date`) were silently dropped with a `WARN` log as expected by the `log_and_drop` design.

Both figures exceed the 500k/sec/core pass criterion by ~3×. At `gitlab-org`'s ~6.7M system notes (#499, 2026-04-27) this puts pure-CPU parse time at **~4 seconds per full indexing pass on a single core**.

### Benchmarks 2–3 — early E2E numbers against GDK

Benchmarks 2 (`siphon_routes` IN-list latency) and 3 (entity tuple IN-list latency) were originally scoped to staging ClickHouse only. The E2E validation pass ran them against a local GDK instance (Docker CH 25.12.11.4, 93 routes, 120 MRs, 609 issues, traversal_path `""`) using the real 75k corpus. Staging measurements are still needed for the production-scale verdict, but the GDK numbers validate the query plan.

| batch_size | distinct paths | routes lookup | MR tuple lookup | WorkItem tuple lookup | **3-query total** | MR hits | WI hits |
|---|---|---|---|---|---|---|---|
| 100 (synthetic) | 3 | 3 ms | 2 ms | 2 ms | **7 ms** | — | — |
| 1,000 (real GDK) | 18 | 3 ms | 2 ms | 3 ms | **8 ms** | 11 / 247 pairs | 33 / 337 pairs |
| 5,000 (real GDK) | 18 | 4 ms | 5 ms | 6 ms | **15 ms** | 48 / 761 pairs | 119 / 1,217 pairs |

At batch=1,000 (the configured per-batch resolution size) the full 3-query plan resolves in **8 ms** against real GDK data, against a ≤50 ms per-batch budget. Combined with the 575 ns/note parse cost: end-to-end throughput (parse + resolve) of **~125k notes/sec** on a single core.

Hit rates of 6–10% are realistic for the GDK seed: the seeder references random IIDs up to 500, but GDK only has 120 MRs and 609 issues, so most synthetic references are unresolvable. Unresolvable refs produced zero rows from the entity-lookup queries (correct behaviour; the edge writer drops them).

### Benchmarks 4–5 — deferred to staging

The GDK numbers validate the query shape but do not exercise (a) the `gitlab-org`-scale ~6.7M-note corpus, (b) a non-empty `traversal_path` filter, or (c) `siphon_routes.path` IN-list against millions of rows where a skip index would matter. The remaining benchmarks need staging ClickHouse access and (for Mode A) the in-progress Siphon replication:

- **Benchmark 4 — end-to-end pass against `gitlab-org`.** Pass criterion: ≤10 min wall-clock for the full namespace. The 125k notes/sec end-to-end figure from the GDK E2E run extrapolates to ~54 s of pure compute for 6.7M notes; the 10-minute budget is dominated by ClickHouse scan time, not by parse + resolve.
- **Benchmark 5 — edge density gain (the #499 acceptance criterion).** Pass criteria: **≥3× MR<->WorkItem edges**, **≥10× MR<->MR edges**, both vs. current `gl_edge` state for `gitlab-org`. Proposed as the concrete numeric form of "a material increase in edge density" from the upstream issue; to be agreed at ADR review.

The handler implementation MR will not merge to behind-flag-on until Benchmarks 4 and 5 have produced numbers and the report is attached.

### E2E bugs found and fixed

The GDK E2E pass uncovered two bugs in the bench harness (not in the parser or resolver). Both fixed in [!1335 commit `be315e04`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335).

1. **`Array(Tuple)` parameter serialization** (blocked all CH benchmark runs). The bench built the `(project_id, iid)` pairs as `serde_json::json!([0_i64, *iid])`; the `clickhouse` crate v0.15.0 serialises `Value::Array` as `[0,123]`, but ClickHouse requires tuple syntax `(0,123)` for `Array(Tuple(Int64,Int64))`. Every CH bench run failed with `CANNOT_PARSE_INPUT_ASSERTION_FAILED`. Fix: use Rust `(i64, i64)` tuples directly so the serde serialiser produces the correct `(pid,iid)` form. Bench-harness call-site issue; the resolver SQL template and parameter typing in `resolver.rs` were already correct.
2. **Hardcoded `gitlab-org/gitlab` default project** (zero entity hits). The bench used `"gitlab-org/gitlab"` as the default project path for un-namespaced refs. That path does not exist outside GitLab.com, so on GDK the routes lookup returned 0 rows and all `(project_id, iid)` pairs collapsed to `(0, iid)`, matching no entity. Fix: added a `--default-project` flag (default `toolbox/gitlab-smoke-tests`, present in every GDK seed) and an `--input` flag so the CH bench consumes the same JSONL dump as the parser bench. The production handler receives the default project path from indexer config.

Both fixes are localised to `crates/xtask/src/system_notes_bench/`; the parser and resolver modules themselves did not require changes.

## Test coverage

| Layer | Where | Covers |
|---|---|---|
| Unit (Rust regex) | `handler/system_notes/parse.rs` tests | All 16 action variants, namespace-prefixed refs (1–20 segments), shorthand refs, commit SHAs (7–40 hex), malformed bodies, lifecycle no-ops |
| Unit (dispatch) | `handler/system_notes/mod.rs` tests | Action → edge-kind mapping, unknown action → log + drop, target-type resolution |
| Unit (POC, lifted) | `crates/xtask/src/system_notes_bench/{parser,resolver,golden}.rs` | 43/43 tests, carried over verbatim into the production handler |
| Snapshot | `tests/snapshots/system_note_bodies.rs` | Real production-style note bodies pinned in fixtures |
| Integration | `crates/integration-tests/tests/indexer/sdlc/notes.rs` | Full extract → transform → write against ClickHouse testcontainers, per-action assertions, lifecycle + cross-reference |
| CI drift | `scripts/check-system-note-actions.sh` | Vendored `ICON_TYPES` vs. upstream Rails source-of-truth |
| Benchmark report | `crates/xtask/src/system_notes_bench/` (POC harness output) | Reproduces Benchmarks 1–5 numbers, attached to the implementation MR description |

## Why not the alternatives

**Why not Option B — Rails internal endpoint.** Calling Rails on the indexing hot path couples GKG throughput to Puma thread capacity for a workload whose primary cost is text regex matching. The MR-diff resolver ([`gitlab-org/orbit/knowledge-graph#482`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/482)) chose Rails precisely because diff data lives in object storage; system notes are already in `siphon_notes`, so there is nothing structural Rails can give us that the lake does not have. Backfilling a ~4TB notes table through Rails would be a production incident waiting to happen. Where Option B *could* fit is a future contingency: calling Rails *only* for unknown `action` values as a schema authority. That is not a Stage 1 plan.

**Why not Option C — structured-action only, no text parsing.** MR !1109 was effectively Option C for the lifecycle subset and the author closed it as insufficient. The cross-reference gap (the actual "you don't have anything useful" problem) stays unaddressed. Lifecycle edges are 80% redundant with existing FKs (`merge_user_id`, `closed_by_id`); only `REOPENED` is novel ground. Option C is preserved as a degraded mode (Mode B above), but it is not the recommended target shape.

**Why not Option D — Siphon-side precomputation.** Siphon's table-mapping config supports only `TransformationType: "ignore"` (`pkg/siphon/table_mapping_config.go`). A reference-extraction primitive would be a net-new Go feature in a repo GKG does not own, carrying the same parser-drift risk as Option A but in a codebase where our reviewers cannot land fixes. No offsetting benefit; unconditionally dominated by Option A.

**Option E — Cached HTML DOM parsing (future evolution path).** Rails resolves GFM references at note *insert time* and caches the rendered HTML in `notes.note_html` (and `description_html` on issues / MRs / work items). Each resolved reference is emitted as an `<a>` element with `data-` attributes (`data-reference-type`, `data-project`, `data-issue` / `data-merge-request` / `data-commit` / `data-work-item`) — a stable machine-readable contract that the Rails redactor already relies on. An alternative extraction shape is: read `note_html` instead of `note`, walk the DOM (e.g. `lol_html` streaming, or `scraper`), and extract `(reference_type, target_id, target_path)` tuples directly from the `<a data-…>` attributes — falling back to body-parse when `note_html` is `NULL` or stale.

Why not in v1: (a) `notes.note_html` replication via Siphon has not been verified — `siphon_notes.note` is the known quantity; (b) the POC's 1.74M notes/sec/core throughput against body text is a measured baseline, while DOM-walk throughput on a `note_html` payload (10–50× larger per row) is unmeasured; (c) Option E still needs `siphon_system_note_metadata` for the lifecycle subset (`closed` / `merged` / `reopened`, which render no `<a>`), so it does not eliminate the Siphon coordination item on the critical path; (d) Option A's body-template surface, while less stable than the HTML render surface, is small (16 actions, vendored constant + CI drift check).

When we would flip: if Benchmark 4 (`gitlab-org` end-to-end) shows the body-parse regex path is too brittle at production scale, *or* the graph-completeness epic expands scope to user notes (`system = false`) and entity descriptions — Option E is a strict superset of Option A's coverage there, and the noteable-driven default-project resolution becomes free (Rails has already resolved `data-project` at render time). The implementation MR's POC harness (`crates/xtask/src/system_notes_bench/`) can be extended cheaply with a `parse-html` subcommand to microbench `note` vs `note_html` extraction on the existing 75k GDK corpus once Siphon replication of `note_html` is confirmed. Option E is therefore documented here as a future evolution path, not a v1 option.

**Why not the per-action ClickHouse VIEW approach (MR !1109's pattern).** Works for 3 lifecycle actions (the !1109 scope). Does not scale to 10+ cross-reference actions × 3 noteable types ≈ 30 views, each materializing redundant projections of `siphon_notes ⋈ siphon_system_note_metadata`. A single Rust handler with one inline `WHERE snm.action IN (…)` is the cleaner shape. Adam's guidance ("skip indexes, not projections") for new Siphon tables argues against introducing many derivative views in `fixtures/siphon.sql` as a matter of project style.

**Why not split into two MRs (lifecycle first, cross-references second).** This is the open question the ADR review should settle. The case for one MR (the current recommendation): both slices share the same Siphon prerequisite, the same handler module, the same `SCHEMA_VERSION` bump, and the same staging cycle; splitting forces two reviews of largely-overlapping code. The case for two MRs: smaller code per review, lifecycle ships visible value to dashboards faster, lower risk that a regex bug in cross-reference parsing blocks the lifecycle ship. The handler is feature-flagged per action, so the two-MR plan is recoverable from the one-MR codebase by toggling flags.

## Consequences

What improves:

- Closes the MR<->MR / MR<->WorkItem / MR<->Commit edge gap that motivates the graph-completeness epic.
- One end-to-end story for system-note edges: one handler, one CI drift check, one new metric family, one feature flag. Future cross-reference actions (Rails ships a new action; agent training surfaces a new edge need) become a regex / match-arm change, not a YAML + fixture + ETL change.
- Under the entity-based dispatch model (ADR 014 / !1341), system-notes gets its own NATS subject (`sdlc.entity.indexing.requested.SystemNote.{dotted_traversal_path}`) and ack lifecycle. A slow or failing system-notes pass no longer redelivers MergeRequest / Issue / Pipeline messages for the same namespace; conversely, a slow MR pass does not block system-notes. The stream's `max_messages_per_subject: 1` + `discard_new_per_subject: true` deduplication operates at the exact `(entity_kind, scope)` level, which is strictly finer-grained than today's per-namespace ack scope.
- The POC harness output is reusable as a regression baseline: any throughput regression in the production handler can be checked against the 1.5M notes/sec/core synthetic POC number and the 1.74M notes/sec/core real-GDK E2E number, plus the 8 ms 3-query CH resolver budget at batch=1,000.

What gets harder:

- A new Rust module to maintain. Action-template drift is a real ongoing cost, quantified by the CI check and the unknown-action metric, but a real cost.
- Two new Siphon-side prerequisites: `system_note_metadata` table replication, and (likely, pending Benchmark 2) a skip index on `siphon_routes.path`.
- The handler is the first non-ontology-driven SDLC handler outside `modules/code/` and `modules/namespace_deletion/`. It departs from the documented "ontology first" convention and the deviation needs to be motivated in code comments + AGENTS.md.
- Mode B is dead code if Analytics replication lands cleanly; carrying it adds test surface that exists purely as a fallback.

### Future optimization: graph-DB-side lookup dictionaries

The current resolver runs against the **analytics DB** (`siphon_routes`, `siphon_merge_requests`, `siphon_issues`, `siphon_work_items`) because that is where the source rows live. @ahegyi and @michaelangeloio both raised, from different angles, the same structural question: the resolver workload is a hot-path lookup against a relatively small projected set (licensed namespaces' routes plus per-entity `(project_id, iid) → id` tuples), and the **graph DB** is the more natural home for that lookup once a projected view exists there.

Two concrete shapes have been proposed:

- **ClickHouse `DICTIONARY` (`project_traversal_paths_dict`).** Replaces the `siphon_routes` IN-list with `dictGetOrDefault('project_traversal_paths_dict', 'traversal_path', PROJECT_ID, '0/')` — a constant-time per-row lookup that avoids the table scan dimension entirely. Pre-requisite: the dictionary must be defined and refreshed in the graph DB (cadence, ownership, and source query are open questions for @ahegyi).
- **Load-routes-once (in-memory).** The handler loads the licensed-namespaces route table once per pass into an in-process `HashMap<path, traversal_path>` and resolves per-row in memory. Cheaper than even a dictionary for small route counts; bounded only by per-worker memory and route-table size.

Neither is needed for v1: the measured 3-query plan resolves in 8 ms at batch=1,000 against GDK, well inside the ≤50 ms per-batch budget. The trigger to revisit is **either Benchmark 4 failing the 10-minute wall-clock criterion against `gitlab-org`, or >2× growth in route-lookup latency observed at staging scale**. Both shapes are graph-DB-resident, so they are not blocked on Siphon coordination.

## Out of scope

- **Intra-batch parallelism (rayon / per-row parallel parse).** Not pursued in v1: pure-CPU parse is ~4 s for the full 6.7M `gitlab-org` corpus at 1.74M notes/sec/core — it is not the bottleneck. Horizontal partitioning via ADR 014's `partition_overrides.SystemNote: N` is the lever once we outgrow a single worker.
- **`@`-mention edges** (`*_user_mentions` tables). Separate effort, separate Siphon prerequisite.
- **Resource state / label / milestone events.** Tracked under #482 with dedicated `resource_*_events` Siphon replication.
- **Banzai HTML rendering.** The parser only extracts GFM references from plain text; `lib/banzai/reference_parser/*_parser.rb` (HTML-AST-based) is explicitly *not* what we port.
- **External (Jira) issue references.** Out per upstream #499.
- **The Siphon replication MR itself.** Filed as a separate Analytics-owned MR against `gitlab-org/analytics-section/siphon`; this ADR depends on it but does not specify it.
- **`@-link_type` property on `MENTIONS` to distinguish `relate` vs. `moved` vs. `duplicate`.** Open design question for review feedback; default proposal is yes, using the existing `link_type` enum pattern from `related_to.yaml`. To be settled in the implementation MR, not the ADR.
- **Partitioning of the system-notes ETL across workers.**
  v1 ships with `partition = None` on the `EntityIndexingRequest` because the POC measured ~125k notes/sec end-to-end on a single core, giving ~54 s per `gitlab-org`-scale pass — well inside the per-message budget.
  Partitioning is available later through ADR 014's dispatcher-owned `PartitionAssignment` machinery: setting `handlers.entity-handler.partition_overrides.SystemNote: N` makes the dispatcher compute quantile boundaries and publish N messages, each carrying a `PartitionAssignment` whose `Range { lower_bound, upper_bound }` the pipeline applies as a SQL `WHERE` conjunct.
  The default partition column derivation in ADR 014 picks the *first non-scope column* of the source `order_by`; for `siphon_notes` that is `noteable_type` (low-cardinality, ~10 enum values), so when partitioning is enabled the implementation will need either a per-entity `partition_column` override or a custom `PartitionStrategy` registered for `SystemNote`.
  The natural partition column is `siphon_system_note_metadata.note_id` (high cardinality, primary key).
  Listed here so a future contributor does not re-derive that we already considered it.

## Coverage and known limitations (E2E-validated)

A full real-Siphon E2E (`GDK Postgres → Siphon CDC → ClickHouse → handler → gl_edge`, not testcontainers) confirmed the `MENTIONS` and lifecycle paths materialize end-to-end and the `Commit`-noteable negative path drops. Two non-bug coverage limitations surfaced and are recorded here so they are not later mistaken for regressions:

- **Lifecycle edges (`CLOSED` / `MERGED` / `REOPENED`) cover imported/legacy namespaces, not native GDK lifecycle.** Modern issue/MR close/reopen/merge writes through `resource_state_events` + the `work_item_status` action, **not** the legacy `closed`/`reopened`/`merged` system-note actions in `HANDLED_LIFECYCLE_ACTIONS`. Those actions only land in `system_note_metadata` for projects imported via the GitHub/Bitbucket importers (which still write them) or for legacy data; the E2E confirmed the edges materialize when those rows exist. In production these supplemental lifecycle edges appear only for imported/legacy namespaces. The headline `MENTIONS` path (and net-new `REOPENED` where it fires) is unaffected; resource-state-event lifecycle coverage is tracked under #482.
- **Group-relative GFM cross-references are not resolved.** Rails writes a cross-reference as a *group-relative* path (`proj-b#2`, dropping a shared top-level ancestor) when source and target share an ancestor, but `siphon_routes.path` stores full paths, so the resolver's literal-token lookup misses the same-group relative form. Full-path references (`group/proj-b#2`) and same-project shorthand (`#2`, `!5`) resolve correctly. This is an acceptable first-cut gap rather than a v1 blocker: it under-counts a subset of same-group cross-project mentions but never produces a wrong edge. Closing it needs the resolver to reconstruct candidate full paths from the source note's namespace ancestry (or a group-relative route lookup); deferred to a follow-up.

## Key risks

1. **`system_note_metadata` Siphon replication slip.** Longest lead-time item. The Siphon-side MR is filed in parallel with this ADR; if it slips past the implementation MR review, the handler ships in Mode B and flips to Mode A when replication lands. The mode switch is a single config change with no schema impact.
2. **Parser drift against Rails' `ICON_TYPES` and body templates.** `ICON_TYPES` has grown across releases (now 61 values; prior captures show ~50) and Rails has moved system-note phrasing more than once. Mitigation: vendored constant + CI drift check + `log_and_drop` on unknown actions + regex anchored on the GFM-reference token rather than on the verb phrase (so phrasing changes do not break extraction). E2E validation against a real 75k-note GDK corpus seeded with unknown actions confirmed the `log_and_drop` path silently absorbs unrecognised values without breaking the pass (see [POC results](#poc-results)).
3. **Custom-handler maintenance cost.** This is the first cross-reference-oriented handler departing from the ontology-first convention. Mitigation: confine the deviation to the *materialization logic* only (edge **kinds** still declare in YAML) and document the rationale in `AGENTS.md` so future ADRs do not treat this as precedent for arbitrary custom handlers.
4. **`siphon_routes.path` IN-list scan cost.** No skip index on `path` today (only a `pg_pkey_ordered` projection on `id`). The GDK E2E pass showed 8 ms at batch=1,000 against 93 routes, but that does not stress the path-index dimension; the staging run at production scale still needs to confirm. If it fails the threshold, a `set(N)` or `bloom_filter` skip index on `path` is the prepared mitigation, and Adam's guidance (skip indexes over projections) aligns with the Siphon team's review preferences.
5. **Acceptance threshold vagueness.** [`gitlab-org/orbit/knowledge-graph#499`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/499) says "a material increase in edge density (numeric target to be set after initial measurement)". This ADR proposes ≥3× MR<->WorkItem and ≥10× MR<->MR as concrete numeric forms. If review prefers different thresholds, the POC harness (`xtask system-notes-bench`) can re-run cheaply.

## References

- Upstream issue: [`gitlab-org/orbit/knowledge-graph#499`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/499)
- POC MR: [!1335](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335)
- POC E2E validation report against GDK: [!1335 note 3360033462](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335#note_3360033462) (74,125-note seeded corpus, parser + ClickHouse resolver, bench-harness bug fixes)
- Closed prior MR: [!1109](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1109)
- Related: [`gitlab-org/orbit/knowledge-graph#482`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/482) (MR ingestion gaps), [`gitlab-org/orbit/knowledge-graph#443`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/443) (migration framework, complete), graph-completeness epic
- Entity-level SDLC indexing scaffold: [!1341](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1341) (ADR 014, supersedes the earlier !1272 draft); pipeline-owned vs. dispatcher-owned partitioning trade-off discussed in this ADR's [Compatibility section](#compatibility-with-entity-level-sdlc-indexing-adr-014)
- Prior research and POC plan: [dgruzd/droid-workspace/task/2685](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685/), E2E validation report: [task/2685-e2e](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685-e2e/), entity-refactor compatibility analysis: [task/2685-entity-refactor](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685-entity-refactor/)
- Rails source of truth: `app/models/system_note_metadata.rb` (`ICON_TYPES`, `TYPES_WITH_CROSS_REFERENCES`), `app/services/system_notes/*.rb` (body templates), `app/models/{issue,merge_request,commit,project}.rb` (`reference_pattern`)
- Siphon repo: `gitlab-org/analytics-section/siphon`
- ADR precedent: [009 (Code Indexer Service)](009_code_indexer_service.md) for implementation-plan shape; [012 (GOON Format)](012_goon_format.md) for benchmark-driven decision rationale and the vendored-constant + CI drift-check pattern
- Custom-handler precedent in code: `crates/indexer/src/modules/code/`, `crates/indexer/src/modules/namespace_deletion/`
- Schema version file: `config/SCHEMA_VERSION` (44 → 45 with this work)
- Routes-join precedent: `config/ontology/nodes/core/project.yaml:115-121`, `config/ontology/nodes/core/group.yaml:101-107`
- Note filter today: `config/ontology/nodes/core/note.yaml` (`where: "system = false"`)
