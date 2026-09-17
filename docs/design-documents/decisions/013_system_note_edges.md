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

Orbit today drops a large class of cross-entity relationships before they reach `gl_edge`. The `Note` pipeline in `config/ontology/nodes/core/note.yaml` is `query: generated` with `extract.filter: "system = false"`. That filter excludes every system note Rails writes. Triggers include an issue closing, a merge request merging, a commit added to an MR, or one entity referencing another in free text. The graph-completeness epic and Angelo's "you don't have anything useful" verdict from the 2026-04-20 Orbit sync both name this gap. Several shapes are missing today: MR<->MR mentions, MR<->WorkItem mentions, MR<->Commit linkages from `commit`/`merge` actions, and the `REOPENED` lifecycle transition. The `MERGED` and `CLOSED` slices exist via FK but are sparse on older data.

Three things make this hard. The structured discriminator, `system_note_metadata.action`, is **not yet replicated** into Siphon. `siphon_notes` exists. `siphon_system_note_metadata` does not. Its absence from `fixtures/siphon.sql` and from the Siphon repo's sample config confirms this. The target entities of cross-references are encoded as GFM reference tokens inside the **free-text body** (e.g. `mentioned in !123`, `mentioned in group/subgroup/project#456`, `mentioned in 54f7727c`), not as structured foreign keys. And the source data is large: [`gitlab-org/orbit/knowledge-graph#499`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/499) cites ~6.7M system notes for `gitlab-org` alone, against a ~4TB global notes table.

The previous attempt at this work was [!1109][prev-mr]. It reached green CI. The author then closed it on 2026-05-18 with *"I'll close this MR, and open an MR with an ADR first."* That MR scoped itself to the lifecycle subset (`merged`, `closed`, `reopened`). It added a `siphon_system_note_metadata` fixture. It materialized one pre-filtered ClickHouse view per action (`siphon_system_note_merged`, `siphon_system_note_closed`, `siphon_system_note_reopened`), so the existing standalone-edge ETL machinery could consume them. The closure was process-driven, not correctness-driven.

Three inputs from that MR. (a) The old standalone edge ETL shape had no `WHERE` clause in `config/schemas/ontology.schema.json`. This forced a per-action view pattern that did not scale to 10+ cross-reference actions × 3 noteable types. (b) The lifecycle-only slice has small marginal value, because `merge_user_id` and `closed_by_id` FKs already cover most of it (only `REOPENED` is novel ground). (c) The Siphon prerequisite was acknowledged in the MR description but never filed as a Siphon-side issue. So the MR would have been inert in production. The current unified pipeline shape replaces that old ETL schema; a generated extract carries such a predicate in its `extract.filter` field (e.g. `state = 5`).

[prev-mr]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1109

Constraints: Rails owns authorization and is the source of system note bodies. So we do not control templates. We must accept Rails-side phrasing changes as a maintenance cost. The Analytics team owns Siphon, so the new source-table replication is cross-team coordination on the critical path. The v0.5 migration framework from [`gitlab-org/orbit/knowledge-graph#443`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/443) is complete and validated on staging. So adding new edge kinds needs only a `SCHEMA_VERSION` bump. No table rebuild is required.

This ADR proposes a Rust extraction handler running inside the SDLC indexer. It uses two batched ClickHouse lookups for entity resolution. It is gated on a one-time Siphon-side replication of `system_note_metadata`. A POC harness ([!1335][poc-mr]) backs the recommendation. It measured parser throughput on two corpora: a 21-entry synthetic golden corpus and a 74,125-note GDK-seeded real corpus. It also ran an end-to-end ClickHouse resolver pass against a local GDK instance.

[poc-mr]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335

## Decision

Build a dedicated Rust system-notes handler at `crates/indexer/src/modules/sdlc/transform/system_notes/`. Read `siphon_notes` joined to `siphon_system_note_metadata` on `note_id` for Mode A. When the join target is unavailable, fall back to body-text filtering for Mode B. Parse the Rails `TYPES_WITH_CROSS_REFERENCES` subset plus selected EE cross-reference actions, `commit` (Markdown SHA list), and `merge` (auto-merge inline SHA, MR-ref fallback).

Resolve targets with two batched IN-list queries against `siphon_routes` and the entity tables (`siphon_merge_requests`, `siphon_issues`, `siphon_work_items`). Emit these edges via the standard `gl_edge` writer: `MENTIONS`, `RELATED_TO` (supplemental), `ADDS_COMMIT`, `MERGED_AT_COMMIT`, `CLOSED` (supplemental), `MERGED` (supplemental), and a new `REOPENED` edge kind.

> **Update (2026-06-17): `CONTAINS` is no longer note-derived.** The note path
> described below originally co-wrote `WorkItem → WorkItem` `CONTAINS` from the
> containment actions (`epic_issue_added`, `issue_added_to_epic`,
> `epic_issue_moved`, hierarchy-shaped `task`). That path was removed. The FK path
> (`siphon_work_item_parent_links` → `config/ontology/edges/contains.yaml`) is the
> single current-state source of truth for hierarchy containment. The note path
> redundantly co-wrote the same `gl_edge` tuple. Because both writers shared the
> `ReplacingMergeTree` identity, ranking mattered. The note row's wall-clock
> `_version` (and hardcoded `_deleted = false`) permanently outranked the FK
> tombstone whenever a child was removed from its parent. This resurrected
> containment the graph no longer holds. The
> four containment actions are no longer parsed or routed, and `CONTAINS` is dropped
> from `config/ontology/derived/core/system_note.yaml`'s `emits:` list (leaving
> `[MENTIONS, REOPENED]`). The MR only stops *new* note-derived `CONTAINS` writes.
> Existing stale/resurrected rows clear on the next reindex (no `SCHEMA_VERSION`
> bump, accepted in staging per the maintainer). See `gitlab-org/orbit/knowledge-graph`
> MR removing note-derived `CONTAINS` and the research verdict in `dgruzd/tasks#2902`.
> The remaining sections that still mention `CONTAINS` describe the original
> (now-superseded) note-derived design.
>
> **Update (2026-06-22): `REOPENED` is sourced from `resource_state_events`, not
> system notes.** This ADR and the closed [!1109][prev-mr] assumed reopen surfaces
> as a `system_note_metadata.action = "reopened"` row. So the original design routed
> `REOPENED` through the note path alongside `CLOSED`/`MERGED`. **That premise is
> wrong.** Reopen is never a system-note action. Rails records it as a
> `resource_state_events` row with `state = 5` (`reopened`). See
> `app/models/resource_state_event.rb` (`enum :state ... .merge(reopened: 5)`) over
> the issuable base states in `app/models/concerns/issuable.rb` (`opened: 1`,
> `closed: 2`, `merged: 3`, `locked: 4`). The note-path `reopened` branch matched
> nothing, so zero `REOPENED` edges were ever emitted
> (`gitlab-org/orbit/knowledge-graph#883`). `REOPENED` is now two standalone edge
> pipelines over `siphon_resource_state_events`. One targets `MergeRequest` via
> `merge_request_id`. One targets `WorkItem` via `issue_id`. Each is filtered to
> `state = 5` in its committed SQL file. This relies on the now-merged Siphon
> replication of `resource_state_events` → `siphon_resource_state_events`
> (`gitlab-org/gitlab!241505`). The `reopened` action and its parser/emit branches
> are removed from the note path; `CLOSED`/`MERGED` stay note-derived. Reconciling
> those lifecycle edges against `resource_state_events` (current-state vs
> event-history semantics, dedupe/tombstone) is deferred. See
> `gitlab-org/orbit/knowledge-graph#883` and its follow-up.
>
> **Update (2026-06-29): ontology ETL now uses unified pipelines.** The system-note path is a derived entity at `config/ontology/derived/core/system_note.yaml` with a `SystemNote` pipeline, `extract.query: system_note.sql.j2`, and `transform.type: system_notes`. The standalone `REOPENED` edge uses two `pipelines:` entries in `config/ontology/edges/reopened.yaml`, each `query: generated`. The `state = 5` predicate now lives in each pipeline's `extract.filter`, not in a `where:` YAML field.

Vendor Rails' `ICON_TYPES` constant with a CI drift check modeled on `scripts/check-goon-format-version.sh`. Ship behind a feature flag with staging benchmarks 2 to 5 (lookup latency, end-to-end pass, edge-density gain) as the gate to GA.

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

Explicit non-goals:

- `@`-mention edges (separate `*_user_mentions` tables, tracked under a follow-up to [`gitlab-org/orbit/knowledge-graph#482`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/482)).
- Resource state / label / milestone events (dedicated `resource_*_events` tables under #482).
- Banzai HTML rendering (the parser is regex-only on plain text).
- External (Jira) issue references.
- The Siphon-side replication MR itself.

### Handler architecture

```plaintext
crates/indexer/src/modules/sdlc/transform/system_notes/
  mod.rs            // SystemNotesPipeline: impl EntityPipeline + registration
  extract.rs        // SQL for the JOIN(siphon_notes ⋈ siphon_system_note_metadata)
  parse.rs          // Regex + per-action dispatch (lifted verbatim from xtask POC)
  resolve.rs        // siphon_routes IN-list and (project_id, iid) IN-list batchers
  emit.rs           // Edge row construction → gl_edge writer
  vendored/icon_types.rs  // Vendored copy of Rails ICON_TYPES, pinned SHA
  tests/            // Unit tests for parse.rs (regex coverage)
```

Under ADR 014's entity-level SDLC dispatch (scaffolded in [!1341][adr014-mr]), each entity-kind dispatched by `EntityDispatcher` flows through a single shared `EntityIndexingHandler`. That handler routes by `entity_kind` to a per-kind pipeline. ADR 014 introduces `SimpleEntityPipeline` as the default plan-driven pipeline and names SystemNotes specifically as the motivating example for the **`EntityPipeline`** custom-pipeline extension point:

> *"All current entities use `SimpleEntityPipeline` … Future entities (e.g., SystemNotes) can implement `EntityPipeline` with custom logic instead of using `SimpleEntityPipeline`."* Source: ADR 014, "Handler and pipeline"

ADR 013's `SystemNotesPipeline` is that custom impl. It receives an `EntityIndexingRequest` (`entity_kind = "SystemNote"`, `scope = IndexingScope::Namespace { namespace_id, traversal_path }`, `partition = None` for v1) on `sdlc.entity.indexing.requested.SystemNote.{dotted_traversal_path}` and applies the two-stage extract → resolve → emit pipeline below. See [Compatibility with entity-level SDLC indexing (ADR 014)](#compatibility-with-entity-level-sdlc-indexing-adr-014) for the full forward-compatibility analysis.

[adr014-mr]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1341

This is a custom `BlockTransform` selected by an ontology-driven derived-entity pipeline, not a custom `EntityPipeline`. The transform-stage extension point is the intended path for entities whose row-to-edge shape cannot be expressed as a DataFusion projection. The departure from the built-in `datafusion` transform is deliberate and bounded:

- `config/ontology/derived/core/system_note.yaml` declares the `SystemNote` pipeline and names the `system_notes` Rust transform.
- `config/ontology/derived/core/system_note.sql.j2` owns the `siphon_notes` / `siphon_system_note_metadata` extract, including page-bounded enrichment and runtime `{{filters}}`.
- A single source table needs to dispatch into ~9 edge variants whose target type depends on body parsing, not on a fixed source-column enum.
- The 10-cross-reference-action × 3-noteable-type expansion through per-action ClickHouse views (MR !1109's pattern) would land ~30 redundant projections in `fixtures/siphon.sql`. Adam's guidance on the Siphon side ("skip indexes, not projections") argues against that shape generally.

Edge **kinds** are still declared in ontology YAML (`config/ontology/edges/{mentions,adds_commit,merged_at_commit,reopened}.yaml`); only the ETL **logic** is Rust. Existing edges that get new supplemental writes (`MERGED`, `CLOSED`, `RELATED_TO`) gain a documented comment pointing at the system-notes handler as an additional emitter.

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

**Default-project resolution for unqualified refs.** Each `siphon_notes` row carries `noteable_id` and `noteable_type`. The handler resolves that pair to the source entity's `project_id` (via `siphon_merge_requests` / `siphon_issues` / `siphon_work_items`). It then looks up that project's `traversal_path` from `siphon_routes`. The resulting path becomes the scope for unqualified GFM references on that row (`!N`, `#N`, short SHAs). The bench harness's `--default-project` flag is a harness-only artefact. The production handler derives the default per-row from the noteable. It does **not** call Gitaly to validate commit SHAs.

**Future resolver shape: graph-DB dictionaries.** Once the graph DB carries a projected, licensed-namespaces-only view of routes, the path-resolution step can use `dictGetOrDefault('project_traversal_paths_dict', 'traversal_path', PROJECT_ID, '0/')`. That shape is strictly cheaper than the `siphon_routes` IN-list. This is left as a v2 lever (see [Future optimization: graph-DB-side lookup dictionaries](#future-optimization-graph-db-side-lookup-dictionaries)).

### Mode A / Mode B

Both modes operate exclusively on **system-authored notes**. They read `siphon_notes` with `WHERE system = true AND _siphon_deleted = false`. The current `config/ontology/nodes/core/note.yaml` pipeline is `query: generated` with `extract.filter: "system = false"` for the user-note `Note` node table; the system-notes transform consumes the complementary half of the `siphon_notes` table.

- **Mode A (preferred, production default):** join `siphon_notes` to `siphon_system_note_metadata` on `note_id` and filter `snm.action IN (TARGET_ACTIONS)`. The join is needed because `siphon_system_note_metadata` carries only `(note_id, action, commit_count, description_version_id)`. The body, noteable (`noteable_id`, `noteable_type`), and author live on `siphon_notes`. The `system = true` constraint is implicit on the join (the join target only contains system rows) but is still emitted on `siphon_notes` for query-shape clarity. This is the shape the handler is designed for.
- **Mode B (degraded fallback):** if `siphon_system_note_metadata` is not yet replicated, the handler reads `siphon_notes WHERE system = true` and dispatches by body content. Lifecycle actions are detected by exact equality (`notes.note = 'closed'`, `'merged'`, `'reopened'`). Cross-reference actions are detected with an anchored regex on the body prefix (`^"mentioned in "`, `^"marked this issue as related to "`, etc.). The `system = true` filter drives Mode B's precision. It scopes the prefix-regex away from arbitrary user-typed notes that happen to start with `"mentioned in "`. Mode B is still slower than Mode A, because it has no metadata-based pre-filter. It is intended only to unblock staging benchmarks and demos while the Siphon-side MR is in flight.

The handler reads its mode from config; production deployment uses Mode A.

### Compatibility with entity-level SDLC indexing (ADR 014)

ADR 014 replaced the former `GlobalHandler` + `NamespaceHandler` split with one `EntityHandler` per ontology entity type. Each handler subscribes to the shared global or namespace NATS topic and processes a single entity kind per message. Partitioning for initial loads is declared per pipeline via `extract.partition_count` in the ontology node YAML.

ADR 013's system-notes pipeline is fully compatible with this model. It registers as a namespaced `EntityHandler` via `Plan`. It reuses the standard checkpoint key format (`ns.{id}.SystemNote`). It can opt into partitioning later via `extract.partition_count: N` on its pipeline.

### Action-coverage drift mitigation

Three-layer defence:

1. **Vendored constant.** `crates/indexer/src/modules/sdlc/transform/system_notes/vendored/icon_types.rs` carries a literal copy of upstream Rails `ICON_TYPES` (61 values at the time of writing). It is pinned to a SHA and documented in a header comment.
2. **CI drift check.** `scripts/check-system-note-actions.sh` mirrors the working pattern of `scripts/check-goon-format-version.sh` (ADR 012, the analogous "upstream owns the source of truth, we vendor a copy" problem). The script fetches the upstream `system_note_metadata.rb`. It diffs the `ICON_TYPES` array against the vendored constant. It fails with an explicit message listing values present upstream but missing locally. Wired into lefthook pre-commit and into the `lint` CI stage.
3. **Runtime safety.** The handler's dispatch is `match action { ... _ => log_and_drop }`, never `panic!`. Unknown actions surface as a new metric `gkg.indexer.sdlc.system_notes.unknown_action_total{action}` registered in `crates/orbit-observability/src/indexer/sdlc.rs`. Cardinality is bounded by `ICON_TYPES` size (~60 to 100), so a label dimension is safe. See the [metrics step](#implementation-plan) of the implementation plan for the full instrument list.

## Implementation plan

1. **(Parallel, lead time):** File a Siphon-side issue against `gitlab-org/analytics-section/siphon` requesting `system_note_metadata` replication; loop in `@ahegyi @arun.sori`. Specify: skip index on `action`, primary key `(traversal_path, note_id)`, mirror Rails `db/structure.sql` exactly. This work item can run in parallel with ADR review; it does not block "Accepted".
2. Add `siphon_system_note_metadata` to `fixtures/siphon.sql`. The DDL bytes from MR !1109 are reusable. The author hit two fixture-only bugs there. One was a stray `;` inside a SQL comment that broke the integration-testkit's naive `split(';')` schema runner. The other was a `USING(note_id)` clause that should be `ON sn.id = snm.note_id`. Both are documented in the research package and fixed in this round.
3. Vendor `crates/indexer/src/modules/sdlc/transform/system_notes/vendored/icon_types.rs` from upstream Rails at a pinned SHA.
4. Add the CI drift check `scripts/check-system-note-actions.sh` + lefthook hook (model: `scripts/check-goon-format-version.sh`).
5. Add new edge YAML: `config/ontology/edges/{mentions.yaml, adds_commit.yaml, merged_at_commit.yaml, reopened.yaml}`. `RELATED_TO`, `CLOSED`, `MERGED` get a documented comment that the system-notes handler is an additional emitter; no YAML schema change.
6. Register the new edge kinds in `config/ontology/schema.yaml`.
7. Implement `SystemNotesTransform` at `crates/indexer/src/modules/sdlc/transform/system_notes/`, lifting `parser.rs` and `resolver.rs` verbatim from the POC at `crates/xtask/src/system_notes_bench/`. The type implements `BlockTransform` (ADR 015) and is registered as `system_notes` in the `TransformRegistry` when the `SystemNotes` feature is enabled. The single shared `Pipeline` still owns extraction, paging, checkpointing, and writes.

    Custom-pipeline precedent: ADR 014 names SystemNotes specifically as the motivating example for the `EntityPipeline` extension point. Custom-handler precedent in the existing codebase: `crates/indexer/src/modules/code/`.
8. Metrics. Hook into the existing `gkg.indexer.sdlc.*` catalog (`crates/orbit-observability/src/indexer/sdlc.rs`) wherever an instrument already fits; add two narrowly-scoped new instruments. This directly addresses the review request to "hook ourselves in the existing metrics":

    | Concern | Instrument | Status |
    |---|---|---|
    | Per-batch duration | `gkg.indexer.sdlc.pipeline.duration{entity="SystemNote"}` | Reuse existing |
    | Rows extracted | `gkg.indexer.sdlc.pipeline.rows.processed{entity="SystemNote"}` | Reuse existing |
    | Parse failures | `gkg.indexer.sdlc.pipeline.errors{entity="SystemNote", error_kind="parse_failure"}` | Reuse existing |
    | Edges emitted | `gkg.indexer.sdlc.edges_emitted_total{entity, edge_kind}` | **Add** to `sdlc.rs` (general-purpose; future entities benefit) |
    | Unknown action drift | `gkg.indexer.sdlc.system_notes.unknown_action_total{action}` | **Add**; cardinality bounded by `ICON_TYPES` (~60–100) |

    Catalog regeneration via `metrics-catalog-check`. The two new instruments land in `orbit-observability/src/indexer/sdlc.rs` (not in a system-notes-specific module) so the catalog stays domain-aligned.
9. Bump `config/SCHEMA_VERSION` (currently 44 → 45).
10. Update `docs/design-documents/data_model.md`, `docs/design-documents/indexing/sdlc_indexing.md`, `AGENTS.md`, and `CLAUDE.md` in the same MR (per the AGENTS.md design-doc sync rule).
11. Integration test `crates/integration-tests/tests/indexer/sdlc/notes.rs::materialises_cross_reference_edges`, plus a lifecycle test ported from the closed !1109. The full source of the !1109 lifecycle test is preserved alongside the research package at [`dgruzd/droid-workspace/task/2685`](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685/). So future implementers do not need to spelunk a closed-MR branch.
12. Feature flag (config-driven) defaulting to off; staging-only first. The flag is handler-config-driven and lives where the rest of the handler config lives under ADR 014's entity-dispatch model:

    ```yaml
    handlers:
      entity-handler:
        batch_size_overrides:
          SystemNote: 100000     # ~13s of resolver budget per pass on local GDK
        # No extract.partition_count for v1; see "Out of scope".
    ```

    `HandlersConfiguration` uses `deny_unknown_fields` (`crates/orbit-server-config/src/engine.rs`). So the SystemNote-specific knobs must fit inside `entity-handler.batch_size_overrides` (the map key is the entity kind). They do not introduce a new top-level config block. Toggling between staging-on and staging-off is a config push, no code change.

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

Pure-CPU, single core, release build. Two corpora were used. The first is the 21-entry synthetic golden corpus. The second is a 74,125-note GDK-seeded real corpus covering all 16 action types (full E2E report: [!1335 (note 3360033462)][e2e-note]).

| Corpus | Notes | Iterations | Median ns/note | Median notes/sec | Refs/pass |
|---|---|---|---|---|---|
| Golden (synthetic) | 21 | 5,000 | 666 | **1,501,501** | 20 |
| Real GDK (seeded) | 75,125 | 100 | 575 | **1,739,130** | 42,155 |

[e2e-note]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335#note_3360033462

The real corpus runs **~16% faster** than the synthetic baseline. The cause is the action mix. About 40% of real GDK notes are lifecycle actions (`closed`, `merged`, `reopened`, `opened`) that short-circuit before any regex work. The golden corpus over-represents multi-ref `relate`/`commit` patterns where the regex actually fires. Real-corpus body length distribution: min 6 chars, median 15, p95 53, max 121, short enough that the regex engine's startup dominates per-note cost.

**Correctness on the real corpus:** 75,125 notes × 100 iterations produced zero panics, zero incorrect parses, and zero false positives across all 16 action types. Unknown actions seeded into the GDK data (`work_item_status`, `assignee`, `start_date_or_due_date`) were silently dropped with a `WARN` log as expected by the `log_and_drop` design.

Both figures exceed the 500k/sec/core pass criterion by ~3×. At `gitlab-org`'s ~6.7M system notes (#499, 2026-04-27) this puts pure-CPU parse time at **~4 seconds per full indexing pass on a single core**.

### Benchmarks 2–3 — early E2E numbers against GDK

Benchmarks 2 (`siphon_routes` IN-list latency) and 3 (entity tuple IN-list latency) were originally scoped to staging ClickHouse only. The E2E validation pass ran them against a local GDK instance, using the real 75k corpus. The instance was Docker CH 25.12.11.4 with 93 routes, 120 MRs, 609 issues, and traversal_path `""`. Staging measurements are still needed for the production-scale verdict, but the GDK numbers validate the query plan.

| batch_size | distinct paths | routes lookup | MR tuple lookup | WorkItem tuple lookup | **3-query total** | MR hits | WI hits |
|---|---|---|---|---|---|---|---|
| 100 (synthetic) | 3 | 3 ms | 2 ms | 2 ms | **7 ms** | — | — |
| 1,000 (real GDK) | 18 | 3 ms | 2 ms | 3 ms | **8 ms** | 11 / 247 pairs | 33 / 337 pairs |
| 5,000 (real GDK) | 18 | 4 ms | 5 ms | 6 ms | **15 ms** | 48 / 761 pairs | 119 / 1,217 pairs |

At batch=1,000 (the configured per-batch resolution size) the full 3-query plan resolves in **8 ms** against real GDK data. The per-batch budget is ≤50 ms. Combined with the 575 ns/note parse cost: end-to-end throughput (parse + resolve) of **~125k notes/sec** on a single core.

Hit rates of 6 to 10% are realistic for the GDK seed. The seeder references random IIDs up to 500. GDK only has 120 MRs and 609 issues. So most synthetic references are unresolvable. Unresolvable refs produced zero rows from the entity-lookup queries (correct behaviour; the edge writer drops them).

### Benchmarks 4–5 — deferred to staging

The GDK numbers validate the query shape. They do not exercise the `gitlab-org`-scale ~6.7M-note corpus. They do not exercise a non-empty `traversal_path` filter. They also skip the `siphon_routes.path` IN-list against millions of rows where a skip index would matter. The remaining benchmarks need staging ClickHouse access and (for Mode A) the in-progress Siphon replication:

- **Benchmark 4: end-to-end pass against `gitlab-org`.** Pass criterion: ≤10 min wall-clock for the full namespace. The 125k notes/sec end-to-end figure from the GDK E2E run extrapolates to ~54 s of pure compute for 6.7M notes. The 10-minute budget is dominated by ClickHouse scan time, not by parse + resolve.
- **Benchmark 5: edge density gain (the #499 acceptance criterion).** Pass criteria: **≥3× MR<->WorkItem edges**, **≥10× MR<->MR edges**, both vs. current `gl_edge` state for `gitlab-org`. Proposed as the concrete numeric form of "a material increase in edge density" from the upstream issue; to be agreed at ADR review.

The handler implementation MR will not merge to behind-flag-on until Benchmarks 4 and 5 have produced numbers and the report is attached.

### E2E bugs found and fixed

The GDK E2E pass uncovered two bugs in the bench harness (not in the parser or resolver). Both fixed in [!1335 commit `be315e04`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335).

1. **`Array(Tuple)` parameter serialization** (blocked all CH benchmark runs). The bench built the `(project_id, iid)` pairs as `serde_json::json!([0_i64, *iid])`. The `clickhouse` crate v0.15.0 serialises `Value::Array` as `[0,123]`. But ClickHouse requires tuple syntax `(0,123)` for `Array(Tuple(Int64,Int64))`. Every CH bench run failed with `CANNOT_PARSE_INPUT_ASSERTION_FAILED`. Fix: use Rust `(i64, i64)` tuples directly so the serde serialiser produces the correct `(pid,iid)` form. Bench-harness call-site issue; the resolver SQL template and parameter typing in `resolver.rs` were already correct.
2. **Hardcoded `gitlab-org/gitlab` default project** (zero entity hits). The bench used `"gitlab-org/gitlab"` as the default project path for un-namespaced refs. That path does not exist outside GitLab.com. So on GDK the routes lookup returned 0 rows. All `(project_id, iid)` pairs collapsed to `(0, iid)`, matching no entity. Fix: added a `--default-project` flag (default `toolbox/gitlab-smoke-tests`, present in every GDK seed). Also added an `--input` flag so the CH bench consumes the same JSONL dump as the parser bench. The production handler receives the default project path from indexer config.

Both fixes are localised to `crates/xtask/src/system_notes_bench/`; the parser and resolver modules themselves did not require changes.

## Test coverage

| Layer | Where | Covers |
|---|---|---|
| Unit (Rust regex) | `transform/system_notes/parse.rs` tests | All 16 action variants, namespace-prefixed refs (1–20 segments), shorthand refs, commit SHAs (7–40 hex), malformed bodies, lifecycle no-ops |
| Unit (dispatch) | `transform/system_notes/mod.rs` tests | Action → edge-kind mapping, unknown action → log + drop, target-type resolution |
| Unit (POC, lifted) | `crates/xtask/src/system_notes_bench/{parser,resolver,golden}.rs` | 43/43 tests, carried over verbatim into the production handler |
| Snapshot | `tests/snapshots/system_note_bodies.rs` | Real production-style note bodies pinned in fixtures |
| Integration | `crates/integration-tests/tests/indexer/sdlc/notes.rs` | Full extract → transform → write against ClickHouse testcontainers, per-action assertions, lifecycle + cross-reference |
| CI drift | `scripts/check-system-note-actions.sh` | Vendored `ICON_TYPES` vs. upstream Rails source-of-truth |
| Benchmark report | `crates/xtask/src/system_notes_bench/` (POC harness output) | Reproduces Benchmarks 1–5 numbers, attached to the implementation MR description |

## Why not the alternatives

**Why not Option B: Rails internal endpoint.** Calling Rails on the indexing hot path couples GKG throughput to Puma thread capacity for a workload whose primary cost is text regex matching. The MR-diff resolver ([`gitlab-org/orbit/knowledge-graph#482`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/482)) chose Rails precisely because diff data lives in object storage. System notes are already in `siphon_notes`. So there is nothing structural Rails can give us that the lake does not have. Backfilling a ~4TB notes table through Rails would be a production incident waiting to happen. Where Option B *could* fit is a future contingency: calling Rails *only* for unknown `action` values as a schema authority. That is not a Stage 1 plan.

**Why not Option C: structured-action only, no text parsing.** MR !1109 was effectively Option C for the lifecycle subset and the author closed it as insufficient. The cross-reference gap (the actual "you don't have anything useful" problem) stays unaddressed. Lifecycle edges are 80% redundant with existing FKs (`merge_user_id`, `closed_by_id`); only `REOPENED` is novel ground. Option C is preserved as a degraded mode (Mode B above), but it is not the recommended target shape.

**Why not Option D: Siphon-side precomputation.** Siphon's table-mapping config supports only `TransformationType: "ignore"` (`pkg/siphon/table_mapping_config.go`). A reference-extraction primitive would be a net-new Go feature in a repo GKG does not own. It carries the same parser-drift risk as Option A, but in a codebase where our reviewers cannot land fixes. No offsetting benefit; unconditionally dominated by Option A.

**Option E: Cached HTML DOM parsing (future evolution path).** Rails resolves GFM references at note *insert time* and caches the rendered HTML in `notes.note_html` (and `description_html` on issues / MRs / work items). Each resolved reference is emitted as an `<a>` element with `data-` attributes (`data-reference-type`, `data-project`, `data-issue` / `data-merge-request` / `data-commit` / `data-work-item`). This is a stable machine-readable contract that the Rails redactor already relies on. An alternative extraction shape reads `note_html` instead of `note`. It walks the DOM (e.g. `lol_html` streaming, or `scraper`) and extracts `(reference_type, target_id, target_path)` tuples directly from the `<a data-…>` attributes. It falls back to body-parse when `note_html` is `NULL` or stale.

Why not in v1:

- `notes.note_html` replication via Siphon has not been verified. `siphon_notes.note` is the known quantity.
- The POC's 1.74M notes/sec/core throughput against body text is a measured baseline. DOM-walk throughput on a `note_html` payload (10 to 50× larger per row) is unmeasured.
- Option E still needs `siphon_system_note_metadata` for the lifecycle subset (`closed` / `merged` / `reopened`, which render no `<a>`). So it does not eliminate the Siphon coordination item on the critical path.
- Option A's body-template surface, while less stable than the HTML render surface, is small (16 actions, vendored constant + CI drift check).

When we would flip: Benchmark 4 (`gitlab-org` end-to-end) shows the body-parse regex path is too brittle at production scale. Or the graph-completeness epic expands scope to user notes (`system = false`) and entity descriptions. In that case Option E is a strict superset of Option A's coverage. The noteable-driven default-project resolution becomes free, because Rails has already resolved `data-project` at render time. The implementation MR's POC harness (`crates/xtask/src/system_notes_bench/`) can be extended cheaply with a `parse-html` subcommand. That subcommand would microbench `note` vs `note_html` extraction on the existing 75k GDK corpus. This waits until Siphon replication of `note_html` is confirmed. Option E is therefore documented as a future evolution path, not a v1 option.

**Why not the per-action ClickHouse VIEW approach (MR !1109's pattern).** Works for 3 lifecycle actions (the !1109 scope). Does not scale to 10+ cross-reference actions × 3 noteable types ≈ 30 views, each materializing redundant projections of `siphon_notes ⋈ siphon_system_note_metadata`. A single Rust handler with one inline `WHERE snm.action IN (…)` is the cleaner shape. Adam's guidance ("skip indexes, not projections") for new Siphon tables argues against introducing many derivative views in `fixtures/siphon.sql` as a matter of project style.

**Why not split into two MRs (lifecycle first, cross-references second).** This is the open question the ADR review should settle. The case for one MR is the current recommendation. Both slices share the same Siphon prerequisite, the same handler module, the same `SCHEMA_VERSION` bump, and the same staging cycle. Splitting forces two reviews of largely-overlapping code. The case for two MRs has three parts. It means smaller code per review. Lifecycle ships visible value to dashboards faster. There is lower risk that a regex bug in cross-reference parsing blocks the lifecycle ship. The handler is feature-flagged per action, so the two-MR plan is recoverable from the one-MR codebase by toggling flags.

## Consequences

What improves:

- Closes the MR<->MR / MR<->WorkItem / MR<->Commit edge gap that motivates the graph-completeness epic.
- One end-to-end story for system-note edges: one handler, one CI drift check, one new metric family, one feature flag. Future cross-reference actions become a regex / match-arm change, not a YAML + fixture + ETL change. Examples are Rails shipping a new action, or agent training surfacing a new edge need.
- Under the entity-based dispatch model (ADR 014 / !1341), system-notes gets its own NATS subject (`sdlc.entity.indexing.requested.SystemNote.{dotted_traversal_path}`) and ack lifecycle. A slow or failing system-notes pass no longer redelivers MergeRequest / Issue / Pipeline messages for the same namespace. Conversely, a slow MR pass does not block system-notes. The stream's `max_messages_per_subject: 1` + `discard_new_per_subject: true` deduplication operates at the exact `(entity_kind, scope)` level, which is strictly finer-grained than today's per-namespace ack scope.
- The POC harness output is reusable as a regression baseline. Check any throughput regression in the production handler against these numbers:
  - the 1.5M notes/sec/core synthetic POC number,
  - the 1.74M notes/sec/core real-GDK E2E number,
  - the 8 ms 3-query CH resolver budget at batch=1,000.

What gets harder:

- A new Rust module to maintain. Action-template drift is a real ongoing cost, quantified by the CI check and the unknown-action metric, but a real cost.
- Two new Siphon-side prerequisites: `system_note_metadata` table replication, and (likely, pending Benchmark 2) a skip index on `siphon_routes.path`.
- The handler is the first non-ontology-driven SDLC handler outside `modules/code/` and `modules/namespace_deletion/`. It departs from the documented "ontology first" convention and the deviation needs to be motivated in code comments + AGENTS.md.
- Mode B is dead code if Analytics replication lands cleanly; carrying it adds test surface that exists purely as a fallback.

### Future optimization: graph-DB-side lookup dictionaries

The current resolver runs against the **analytics DB** (`siphon_routes`, `siphon_merge_requests`, `siphon_issues`, `siphon_work_items`) because that is where the source rows live. @ahegyi and @michaelangeloio both raised the same structural question, from different angles. The resolver workload is a hot-path lookup against a relatively small projected set. That set is licensed namespaces' routes plus per-entity `(project_id, iid) → id` tuples. The **graph DB** is the more natural home for that lookup once a projected view exists there.

Two concrete shapes have been proposed:

- **ClickHouse `DICTIONARY` (`project_traversal_paths_dict`).** Replaces the `siphon_routes` IN-list with `dictGetOrDefault('project_traversal_paths_dict', 'traversal_path', PROJECT_ID, '0/')`. This is a constant-time per-row lookup that avoids the table scan dimension entirely. Pre-requisite: the dictionary must be defined and refreshed in the graph DB (cadence, ownership, and source query are open questions for @ahegyi).
- **Load-routes-once (in-memory).** The handler loads the licensed-namespaces route table once per pass into an in-process `HashMap<path, traversal_path>` and resolves per-row in memory. Cheaper than even a dictionary for small route counts; bounded only by per-worker memory and route-table size.

Neither is needed for v1. The measured 3-query plan resolves in 8 ms at batch=1,000 against GDK. That is well inside the ≤50 ms per-batch budget. The trigger to revisit is **either Benchmark 4 failing the 10-minute wall-clock criterion against `gitlab-org`, or >2× growth in route-lookup latency observed at staging scale**. Both shapes are graph-DB-resident, so they are not blocked on Siphon coordination.

## Out of scope

- **Intra-batch parallelism (rayon / per-row parallel parse).** Not pursued in v1: pure-CPU parse is ~4 s for the full 6.7M `gitlab-org` corpus at 1.74M notes/sec/core. It is not the bottleneck. Horizontal partitioning via ADR 014's `extract.partition_count: N` on the pipeline is the lever once we outgrow a single worker.
- **`@`-mention edges** (`*_user_mentions` tables). Separate effort, separate Siphon prerequisite.
- **Resource state / label / milestone events.** Tracked under #482 with dedicated `resource_*_events` Siphon replication.
- **Banzai HTML rendering.** The parser only extracts GFM references from plain text; `lib/banzai/reference_parser/*_parser.rb` (HTML-AST-based) is explicitly *not* what we port.
- **External (Jira) issue references.** Out per upstream #499.
- **The Siphon replication MR itself.** Filed as a separate Analytics-owned MR against `gitlab-org/analytics-section/siphon`; this ADR depends on it but does not specify it.
- **`@-link_type` property on `MENTIONS` to distinguish `relate` vs. `moved` vs. `duplicate`.** Open design question for review feedback; default proposal is yes, using the existing `link_type` enum pattern from `related_to.yaml`. To be settled in the implementation MR, not the ADR.
- **Partitioning of the system-notes ETL across workers.**
  v1 ships with `partition = None` on the `EntityIndexingRequest`, because the POC measured ~125k notes/sec end-to-end on a single core. That gives ~54 s per `gitlab-org`-scale pass, well inside the per-message budget.
  Partitioning is available later through ADR 014's dispatcher-owned `PartitionAssignment` machinery. Declaring `extract.partition_count: N` on the SystemNote pipeline makes the dispatcher compute quantile boundaries and publish N messages. Each message carries a `PartitionAssignment` whose `Range { lower_bound, upper_bound }` the pipeline applies as a SQL `WHERE` conjunct.
  The default partition column derivation in ADR 014 picks the *first non-scope column* of the source `order_by`. For `siphon_notes` that is `noteable_type` (low-cardinality, ~10 enum values). So when partitioning is enabled, the implementation will need one of two things: a per-entity `partition_column` override, or a custom `PartitionStrategy` registered for `SystemNote`.
  The natural partition column is `siphon_system_note_metadata.note_id` (high cardinality, primary key).
  Listed here so a future contributor does not re-derive that we already considered it.

## Key risks

1. **`system_note_metadata` Siphon replication slip.** Longest lead-time item. The Siphon-side MR is filed in parallel with this ADR. If it slips past the implementation MR review, the handler ships in Mode B. It flips to Mode A when replication lands. The mode switch is a single config change with no schema impact.
2. **Parser drift against Rails' `ICON_TYPES` and body templates.** `ICON_TYPES` has grown across releases (now 61 values; prior captures show ~50) and Rails has moved system-note phrasing more than once. Mitigation: vendored constant + CI drift check + `log_and_drop` on unknown actions. The regex is anchored on the GFM-reference token rather than on the verb phrase, so phrasing changes do not break extraction. E2E validation ran against a real 75k-note GDK corpus seeded with unknown actions. It confirmed the `log_and_drop` path silently absorbs unrecognised values without breaking the pass (see [POC results](#poc-results)).
3. **Custom-handler maintenance cost.** This is the first cross-reference-oriented handler departing from the ontology-first convention. Mitigation: confine the deviation to the *materialization logic* only (edge **kinds** still declare in YAML). Also document the rationale in `AGENTS.md`. This stops future ADRs from treating it as precedent for arbitrary custom handlers.
4. **Full-table-scan cost on resolver second-hop lookups.** `siphon_routes`,
   `merge_requests`, and `work_items` are `ORDER BY (traversal_path, ...)`. But the
   resolver's filter columns (`path`, `source_id`, `(target_project_id, iid)`,
   `(project_id, iid)`) are not usable PK prefixes. Without a `traversal_path`
   leg, every lookup is a full scan of the shared Siphon datalake.
   **Mitigation (implemented):** all four resolver queries carry
   `startsWith(traversal_path, {root_prefix:String})` where `root_prefix` is the
   source note's top-level namespace prefix (`<org>/<top_level_ns>/`). This turns
   each full scan into a primary-index range scan bounded to one top-level namespace
   partition. The trade-off is that v1 resolves only **same-top-level-namespace
   references**. A cross-top-level reference (`other-group/proj#5`) lives outside
   the prefix. It is silently not resolved (under-counts, never a wrong edge).
   Cross-top-level resolution is deferred to the graph-DB dictionary lever (see
   [Future optimization](#future-optimization-graph-db-side-lookup-dictionaries)),
   which also resolves the cross-namespace edge-visibility (authz) question. The
   `cross_top_level_reference_is_not_resolved` integration test guards this as a
   deliberate limitation.
5. **Acceptance threshold vagueness.** [`gitlab-org/orbit/knowledge-graph#499`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/499) says "a material increase in edge density (numeric target to be set after initial measurement)". This ADR proposes ≥3× MR<->WorkItem and ≥10× MR<->MR as concrete numeric forms. If review prefers different thresholds, the POC harness (`xtask system-notes-bench`) can re-run cheaply.

## Coverage and known limitations

v1 resolves only references whose target lives under the **same top-level namespace** as the source note. Every resolver query (`siphon_routes`, `merge_requests`, `work_items`) is bounded by `startsWith(traversal_path, {root_prefix:String})` to avoid full-scanning the shared Siphon datalake tables. This means:

- **Same-project references** (`#123`, `!456`): always resolved.
- **Same-top-level cross-project references** (`sibling-group/project!42`): resolved, because the target's route falls under the same top-level namespace prefix.
- **Cross-top-level references** (`other-org/project#5`): intentionally **not** resolved. The target's route lives outside the `root_prefix`, so it never appears in the bounded scan. This produces under-counts (missing edges), never wrong edges.
- **Commit references** (`deadbeef`): parsed but not resolved (no `Commit` node type yet).

Cross-top-level resolution is deferred to the graph-DB dictionary lever ([§ Future optimization](#future-optimization-graph-db-side-lookup-dictionaries)). That lever replaces the `siphon_routes` IN-list with a constant-time dictionary lookup that naturally spans all namespaces. That lever also resolves the cross-namespace edge-visibility (authz) question. A cross-top-level edge might reference a project the querying user cannot see. The dictionary can enforce that at read time.

### Implementation shape (post-ADR 015)

The system-notes handler is implemented as a `BlockTransform` (ADR 015) rather than
the `EntityPipeline` described in the original decision section. ADR 015 refined
ADR 014's extension point: the seam is the **transform stage**, not a custom
pipeline. The `SystemNotesTransform` implements `BlockTransform` and is registered
via `TransformRegistry::register("system_notes", factory)`. The extract plan is
declared as a derived entity in `config/ontology/derived/core/system_note.yaml` and
rides the shared `Pipeline` for paging, checkpointing, and streaming writes.
Resolver lookups against `siphon_routes`, `merge_requests`, and `work_items` split
bound array parameters into
`engine.handlers.entity-handler.system_notes_resolve_lookup_batch_size` chunks. They
then union the decoded rows. This is needed because ClickHouse HTTP parameters are
serialized into the request URL.

#### Update (2026-06-25): MENTIONS edge direction corrected

The original emitter set `source = noteable` (the entity the note
lives on) and `target = body-ref` (the entity parsed from the note
body). For every note row processed, this is backwards. The noteable is
the entity whose page receives the system note. The parsed ref is the
other endpoint. The ontology's
directional `from_node → to_node` means *mentioner → mentioned*, so
the source should be the parsed ref and the target the noteable.

For one-sided actions (`cross_reference`, `new_merge_request`) this
was a clear global inversion. Every edge pointed the wrong way. For
symmetric actions (`relate`/`unrelate`, hierarchy, `moved`, `cloned`,
`duplicate`), Rails writes reciprocal notes, so the graph already
contained both directions. The per-row orientation was still wrong.
But the aggregate graph was not missing a direction.

The fix:

- **Direction:** swaps `source_id`/`target_id` and
  `source_kind`/`target_kind` so the edge points from the parsed ref
  (mentioner) to the noteable (mentioned).
- **Partition:** changes the edge's `traversal_path` from the resolved
  ref's namespace to the noteable's namespace. Inbound-degree
  queries on the target then still hit the correct `gl_edge` partition.
- **Rollout:** requires a `SCHEMA_VERSION` bump (70 → 71). The
  corrected rows have a different `ReplacingMergeTree` sort-key
  identity. The `source_id`, `target_id`, `source_kind`, `target_kind`,
  and (for cross-project rows) `traversal_path` all change. So an
  in-place re-index would insert corrected rows alongside the stale
  inverted ones without replacing them. The version bump forces
  migration into fresh `v71_` tables that are re-indexed from scratch,
  so no stale inverted rows survive.

See [#912](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/912)
for the full investigation.

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
- Schema version file: `config/SCHEMA_VERSION` (50 → 51 with the initial system-notes work; 70 → 71 with the MENTIONS direction fix)
- Routes-join precedent: `config/ontology/nodes/core/project.yaml:115-121`, `config/ontology/nodes/core/group.yaml:101-107`
- Note extract today: `config/ontology/nodes/core/note.yaml` is `query: generated` with `extract.filter: "system = false"`
