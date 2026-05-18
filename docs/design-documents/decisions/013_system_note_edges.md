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

The current Knowledge Graph drops a large class of cross-entity relationships before they reach `gl_edge`. `config/ontology/nodes/core/note.yaml` filters with `where: "system = false"`, which silently excludes every system note Rails writes when an issue is closed, a merge request is merged, a commit is added to an MR, or one entity references another in free text. The graph-completeness epic and Angelo's "you don't have anything useful" verdict from the 2026-04-20 Orbit sync both name this gap. The concrete shapes missing today are MR↔MR mentions, MR↔WorkItem mentions, MR↔Commit linkages from `commit`/`merge` actions, and the `REOPENED` lifecycle transition (the `MERGED` and `CLOSED` slices exist via FK but are sparse on older data).

The problem is non-trivial for three intertwined reasons.

First, the structured discriminator that says *what kind of event a system note is* — `system_note_metadata.action` — is **not yet replicated** into Siphon. `siphon_notes` is available; `siphon_system_note_metadata` is not (verified by absence from `fixtures/siphon.sql` and from the Siphon repo's sample config; no open Siphon issue mentions the table).

Second, the target entities of cross-references are encoded as GitLab Flavored Markdown reference tokens inside the **free-text body** of the note (e.g. `mentioned in !123`, `mentioned in group/subgroup/project#456`, `mentioned in 54f7727c`), not as structured foreign keys.

Third, the source data is large: kg#499 cites ~6.7M system notes already replicated for `gitlab-org` alone, against a ~4TB global notes table.

The previous attempt at this work, [!1109][prev-mr], reached green CI and was then closed by the author on 2026-05-18 with *"I'll close this MR, and open an MR with an ADR first."* That MR scoped itself to the lifecycle subset (`merged`, `closed`, `reopened`), added a `siphon_system_note_metadata` fixture, and materialized one pre-filtered ClickHouse view per action (`siphon_system_note_merged`, `siphon_system_note_closed`, `siphon_system_note_reopened`) so the existing standalone-edge ETL machinery could consume them. The closure was process-driven, not correctness-driven.

Three things the MR exposed are inputs to this ADR: (a) standalone edge ETL has **no `WHERE` clause** in `config/schemas/ontology.schema.json`'s `edgeEtlConfig` definition, which forces a per-action view pattern that does not scale to 10+ cross-reference actions × 3 noteable types; (b) the lifecycle-only slice has small marginal value because `merge_user_id` and `closed_by_id` FKs already cover most of it (only `REOPENED` is novel ground); (c) the Siphon prerequisite was acknowledged in the MR description but never filed as a Siphon-side issue, so the MR would have been inert in production.

[prev-mr]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1109

The constraints in play: Rails owns authorization and is the source of system note bodies, so we do not control templates and must accept Rails-side phrasing changes as a maintenance cost. The Analytics team owns Siphon, so the new source-table replication is cross-team coordination on the critical path. The v0.5 migration framework from kg#443 is complete and validated on staging, so adding new edge kinds is a `SCHEMA_VERSION` bump, not a table rebuild.

This ADR proposes a Rust extraction handler running inside the SDLC indexer, with two batched ClickHouse lookups for entity resolution, gated on a one-time Siphon-side replication of `system_note_metadata`. The recommendation is anchored by a POC harness ([!1335][poc-mr]) that produced real parser-throughput numbers and locked in the parser/resolver shape the production handler will reuse verbatim.

[poc-mr]: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335

## Decision

Build a dedicated Rust system-notes handler at `crates/indexer/src/modules/sdlc/handler/system_notes/`. Read `siphon_notes` joined to `siphon_system_note_metadata` on `note_id` (Mode A) or fall back to body-text filtering (Mode B) when the join target is unavailable. Parse the Rails `TYPES_WITH_CROSS_REFERENCES` subset (10 actions) plus `commit` (Markdown SHA list) and `merge` (auto-merge inline SHA, MR-ref fallback).

Resolve targets with two batched IN-list queries against `siphon_routes` and the entity tables (`siphon_merge_requests`, `siphon_issues`, `siphon_work_items`). Emit `MENTIONS`, `RELATED_TO` (supplemental), `ADDS_COMMIT`, `MERGED_AT_COMMIT`, `CLOSED` (supplemental), `MERGED` (supplemental), and a new `REOPENED` edge kind via the standard `gl_edge` writer.

Vendor Rails' `ICON_TYPES` constant with a CI drift check modeled on `scripts/check-goon-format-version.sh`. Ship behind a feature flag with staging benchmarks 2–5 (lookup latency, end-to-end pass, edge-density gain) as the gate to GA.

### Scope and edge kinds

| Edge | From → To | Source action(s) | Body token to extract |
|---|---|---|---|
| `MENTIONS` | MergeRequest / WorkItem / Commit → same (cross-typed) | `cross_reference` | GFM ref after `"mentioned in "` |
| `RELATED_TO` (supplement) | WorkItem ↔ WorkItem | `relate`, `unrelate` | GFM ref after `"marked … as related to"` / `"removed the relation with"` |
| `MENTIONS` (parent/child) | WorkItem → WorkItem | `relate_to_parent`, `relate_to_child`, `unrelate_from_parent`, `unrelate_from_child` | GFM ref + relation type |
| `MENTIONS` (lifecycle moves) | WorkItem → WorkItem | `moved`, `cloned`, `duplicate` | GFM ref after verb phrase |
| `ADDS_COMMIT` | MergeRequest → Commit | `commit` | Markdown list of SHAs |
| `MERGED_AT_COMMIT` | MergeRequest → Commit | `merge` (auto-merge variant) | SHA inline |
| `MERGED` (supplement to FK) | User → MergeRequest | `merged` (lifecycle string) | n/a |
| `CLOSED` (supplement to FK) | User → MergeRequest, WorkItem | `closed` | n/a |
| `REOPENED` (new edge kind) | User → MergeRequest, WorkItem | `reopened` | n/a |

Explicit non-goals: `@`-mention edges (separate `*_user_mentions` tables, tracked under a follow-up to kg#482), resource state / label / milestone events (dedicated `resource_*_events` tables under kg#482), Banzai HTML rendering (the parser is regex-only on plain text), external (Jira) issue references, and the Siphon-side replication MR itself.

### Handler architecture

```text
crates/indexer/src/modules/sdlc/handler/system_notes/
  mod.rs            // SystemNotesEdgeHandler impl, registration
  extract.rs        // SQL for the JOIN(siphon_notes ⋈ siphon_system_note_metadata)
  parse.rs          // Regex + per-action dispatch (lifted verbatim from xtask POC)
  resolve.rs        // siphon_routes IN-list and (project_id, iid) IN-list batchers
  emit.rs           // Edge row construction → gl_edge writer
  vendored/icon_types.rs  // Vendored copy of Rails ICON_TYPES, pinned SHA
  tests/            // Unit tests for parse.rs (regex coverage)
```

This is a **custom handler**, not an ontology-driven plan. The departure from the ontology-first convention (AGENTS.md: *"New entity types start there, not in Rust"*) is deliberate and bounded:

- Edge ETL YAML has no `WHERE` clause (verified against `config/schemas/ontology.schema.json::edgeEtlConfig`, which has `additionalProperties: false` and exposes only `scope`, `source`, `watermark`, `deleted`, `order_by`, `from`, `to`).
- A single source table needs to dispatch into ~9 edge variants whose target type depends on body parsing, not on a fixed source-column enum.
- The 10-cross-reference-action × 3-noteable-type expansion through the only available alternative (per-action ClickHouse views, MR !1109's pattern) would land ~30 redundant projections in `fixtures/siphon.sql`. Adam's guidance on the Siphon side ("skip indexes, not projections") argues against that shape generally.

Edge **kinds** are still declared in ontology YAML (`config/ontology/edges/{mentions,adds_commit,merged_at_commit,reopened}.yaml`); only the ETL **logic** is Rust. Existing edges that get new supplemental writes (`MERGED`, `CLOSED`, `RELATED_TO`) gain a documented comment pointing at the system-notes handler as an additional emitter.

### Extraction pipeline

```text
for batch in extract():       # paginated, watermark-bounded, traversal-path-scoped
  parsed = []
  for row in batch:           # siphon_notes ⋈ siphon_system_note_metadata
    action = row.action       # Mode A
    if action not in TARGET_ACTIONS:
      continue
    refs = parse_dispatch(action, row.note, row.namespace_traversal_path)
    parsed.push((row.id, row.note_id, row.noteable_id, row.noteable_type,
                 row.author_id, action, refs))

  # Two batched lookups, both already established patterns in the codebase
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

Dedup key: `(system_note_metadata.id, edge_kind, source_kind, source_id, target_kind, target_id)`. ReplacingMergeTree handles re-processing idempotency. The two-stage `IN (…)` resolution pattern is precedented at `config/ontology/nodes/core/project.yaml:115-121` (the inverse direction `source_id → path`) and `crates/indexer/src/modules/namespace_deletion/store.rs:92`.

### Mode A / Mode B

- **Mode A (preferred, production default):** join `siphon_notes` to `siphon_system_note_metadata` on `note_id` and filter `snm.action IN (TARGET_ACTIONS)`. This is the shape the handler is designed for.
- **Mode B (degraded fallback):** if `siphon_system_note_metadata` is not yet replicated, lifecycle actions can be detected directly from `notes.note` (the body strings are exact, e.g. `notes.note = 'closed'`) and cross-reference actions can be detected with an anchored regex on the body prefix (`"mentioned in "`, `"marked this issue as related to "`, etc.). Mode B is slower (no metadata-based pre-filter), less precise (a user-typed note that happens to start with `"mentioned in "` could match, though `system = true` filtering keeps the surface tiny), and is intended only to unblock staging benchmarks and demos while the Siphon-side MR is in flight.

The handler reads its mode from config; production deployment uses Mode A.

### Action-coverage drift mitigation

Three-layer defence:

1. **Vendored constant.** `crates/indexer/src/modules/sdlc/handler/system_notes/vendored/icon_types.rs` carries a literal copy of upstream Rails `ICON_TYPES` (61 values at the time of writing), pinned to a SHA and documented in a header comment.
2. **CI drift check.** `scripts/check-system-note-actions.sh` mirrors the working pattern of `scripts/check-goon-format-version.sh` (ADR 012, the analogous "upstream owns the source of truth, we vendor a copy" problem): the script fetches the upstream `system_note_metadata.rb`, diffs the `ICON_TYPES` array against the vendored constant, and fails with an explicit message listing values present upstream but missing locally. Wired into lefthook pre-commit and into the `lint` CI stage.
3. **Runtime safety.** The handler's dispatch is `match action { ... _ => log_and_drop }`, never `panic!`. Unknown actions surface as a new metric `gkg.indexer.system_notes.unknown_action_total{action}` registered in `crates/gkg-observability/src/indexer.rs`; cardinality is bounded by `ICON_TYPES` size (~60–100), so a label dimension is safe.

## Implementation plan

1. **(Parallel, lead time):** File a Siphon-side issue against `gitlab-org/analytics-section/siphon` requesting `system_note_metadata` replication; loop in `@ahegyi @arun.sori`. Specify: skip index on `action`, primary key `(traversal_path, note_id)`, mirror Rails `db/structure.sql` exactly. This work item can run in parallel with ADR review; it does not block "Accepted".
2. Add `siphon_system_note_metadata` to `fixtures/siphon.sql`. The DDL bytes from MR !1109 are reusable; the two fixture-only bugs the author hit there (stray `;` inside a SQL comment that broke the integration-testkit's naive `split(';')` schema runner, and a `USING(note_id)` clause that should be `ON sn.id = snm.note_id`) are documented in the research package and fixed in this round.
3. Vendor `crates/indexer/src/modules/sdlc/handler/system_notes/vendored/icon_types.rs` from upstream Rails at a pinned SHA.
4. Add the CI drift check `scripts/check-system-note-actions.sh` + lefthook hook (model: `scripts/check-goon-format-version.sh`).
5. Add new edge YAML: `config/ontology/edges/{mentions.yaml, adds_commit.yaml, merged_at_commit.yaml, reopened.yaml}`. `RELATED_TO`, `CLOSED`, `MERGED` get a documented comment that the system-notes handler is an additional emitter; no YAML schema change.
6. Register the new edge kinds in `config/ontology/schema.yaml`.
7. Implement the handler module `crates/indexer/src/modules/sdlc/handler/system_notes/`, lifting `parser.rs` and `resolver.rs` verbatim from the POC at `crates/xtask/src/system_notes_bench/`. Register in `modules/sdlc/mod.rs::register_handlers` alongside `NamespaceHandler` and `GlobalHandler`. Custom-handler precedent: `crates/indexer/src/modules/code/`.
8. Add metrics: `gkg.indexer.system_notes.edges_emitted_total{edge_kind}`, `gkg.indexer.system_notes.unknown_action_total{action}`, `gkg.indexer.system_notes.parse_failures_total{action}`, `gkg.indexer.system_notes.batch_duration_seconds`. Catalog regeneration via `metrics-catalog-check`.
9. Bump `config/SCHEMA_VERSION` (currently 44 → 45).
10. Update `docs/design-documents/data_model.md`, `docs/design-documents/indexing/sdlc_indexing.md`, `AGENTS.md`, and `CLAUDE.md` in the same MR (per the AGENTS.md design-doc sync rule).
11. Integration test `crates/integration-tests/tests/indexer/sdlc/notes.rs::materialises_cross_reference_edges` plus the lifecycle test from MR !1109 (recoverable from `git show 0871d6c2^...`).
12. Feature flag (config-driven) defaulting to off; staging-only first.

## POC results

The POC harness ([!1335][poc-mr]) lives under `crates/xtask/src/system_notes_bench/` and produces the numbers this ADR rests on. **43/43 unit tests pass, clippy `-D warnings` clean, `cargo fmt` clean** at the time of writing. The breakdown:

| Check | Result |
|---|---|
| Parser correctness (16 actions, deep namespaces, multiple refs, lifecycle no-ops, negative cases) | 25 unit tests, all green |
| SQL template shape (named params, `startsWith(traversal_path)`, tuple IN-list, `_siphon_deleted = false`) | 8 unit tests, all green |
| In-memory join semantics (path → `source_id` → entity, namespace rows filtered) | 2 unit tests, all green |
| Golden corpus end-to-end through the parser (real Rails-template bodies vendored from `app/services/system_notes/*.rb`) | 3 corpus-level tests, all green |
| `xtask system-notes-bench inspect` smoke against the corpus | Output verified against Rails templates |

### Benchmark 1 — parser throughput (POC measured)

Pure-CPU, single core, release build, golden corpus of 21 entries, 5000 iterations:

```text
=== Parser benchmark ===
corpus size:           21
iterations:            5000
min   per-pass:        13750 ns
median per-pass:       13990 ns
max   per-pass:        12676930 ns
median per-note:       666 ns
median notes/sec:      1501501
refs/pass:             20
```

**~1.5M notes/sec/core**, roughly **3× above the 500k/sec/core pass criterion** from the POC plan. At `gitlab-org`'s ~6.7M system notes (per kg#499 comment dated 2026-04-27) this puts pure-CPU parse time at **~5 seconds per indexing pass on a single core**, with comfortable headroom for the multi-core production handler.

### Benchmarks 2–5 — deferred to staging

The remaining four benchmarks need staging ClickHouse access and (for Mode A) the in-progress Siphon replication:

- **Benchmark 2 — `siphon_routes` IN-list latency.** Harness ready (`xtask system-notes-bench clickhouse --url … --traversal-path 1/100/`). Pass criteria: ≤100 ms p95 at 1000-path batch warm; ≤500 ms p95 at 10000-path batch cold. If either fails, a `set(N)` or `bloom_filter` skip index on `siphon_routes.path` is the prepared mitigation (currently the table has only a `pg_pkey_ordered` projection on `id`, no index on `path`).
- **Benchmark 3 — entity (project_id, iid) tuple IN-list latency.** Same harness, same shape; expected to be fast because the entity tables' primary keys already include `target_project_id` / `project_id`.
- **Benchmark 4 — end-to-end pass against `gitlab-org`.** Pass criterion: ≤10 min wall-clock for the full namespace.
- **Benchmark 5 — edge density gain (the kg#499 acceptance criterion).** Pass criteria: **≥3× MR↔WorkItem edges**, **≥10× MR↔MR edges**, both vs. current `gl_edge` state for `gitlab-org`. Proposed as the concrete numeric form of "a material increase in edge density" from the upstream issue; to be agreed at ADR review.

The handler implementation MR will not merge to behind-flag-on until Benchmarks 2–5 have produced numbers and the report is attached.

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

**Why not Option B — Rails internal endpoint.** Calling Rails on the indexing hot path couples GKG throughput to Puma thread capacity for a workload whose primary cost is text regex matching. The MR-diff resolver (kg#482) chose Rails precisely because diff data lives in object storage; system notes are already in `siphon_notes`, so there is nothing structural Rails can give us that the lake does not have. Backfilling a ~4TB notes table through Rails would be a production incident waiting to happen. Where Option B *could* fit is a future contingency: calling Rails *only* for unknown `action` values as a schema authority. That is not a Stage 1 plan.

**Why not Option C — structured-action only, no text parsing.** MR !1109 was effectively Option C for the lifecycle subset and the author closed it as insufficient. The cross-reference gap (the actual "you don't have anything useful" problem) stays unaddressed. Lifecycle edges are 80% redundant with existing FKs (`merge_user_id`, `closed_by_id`); only `REOPENED` is novel ground. Option C is preserved as a degraded mode (Mode B above), but it is not the recommended target shape.

**Why not Option D — Siphon-side precomputation.** Siphon's table-mapping config supports only `TransformationType: "ignore"` (`pkg/siphon/table_mapping_config.go`). A reference-extraction primitive would be a net-new Go feature in a repo GKG does not own, carrying the same parser-drift risk as Option A but in a codebase where our reviewers cannot land fixes. No offsetting benefit; unconditionally dominated by Option A.

**Why not the per-action ClickHouse VIEW approach (MR !1109's pattern).** Works for 3 lifecycle actions (the !1109 scope). Does not scale to 10+ cross-reference actions × 3 noteable types ≈ 30 views, each materializing redundant projections of `siphon_notes ⋈ siphon_system_note_metadata`. A single Rust handler with one inline `WHERE snm.action IN (…)` is the cleaner shape. Adam's guidance ("skip indexes, not projections") for new Siphon tables argues against introducing many derivative views in `fixtures/siphon.sql` as a matter of project style.

**Why not split into two MRs (lifecycle first, cross-references second).** This is the open question the ADR review should settle. The case for one MR (the current recommendation): both slices share the same Siphon prerequisite, the same handler module, the same `SCHEMA_VERSION` bump, and the same staging cycle; splitting forces two reviews of largely-overlapping code. The case for two MRs: smaller code per review, lifecycle ships visible value to dashboards faster, lower risk that a regex bug in cross-reference parsing blocks the lifecycle ship. The handler is feature-flagged per action, so the two-MR plan is recoverable from the one-MR codebase by toggling flags.

## Consequences

What improves:

- Closes the MR↔MR / MR↔WorkItem / MR↔Commit edge gap that motivates the graph-completeness epic.
- One end-to-end story for system-note edges: one handler, one CI drift check, one new metric family, one feature flag. Future cross-reference actions (Rails ships a new action; agent training surfaces a new edge need) become a regex / match-arm change, not a YAML + fixture + ETL change.
- The POC harness output is reusable as a regression baseline: any throughput regression in the production handler can be checked against the 1.5M notes/sec/core POC number.

What gets harder:

- A new Rust module to maintain. Action-template drift is a real ongoing cost — quantified by the CI check and the unknown-action metric, but a real cost.
- Two new Siphon-side prerequisites: `system_note_metadata` table replication, and (likely, pending Benchmark 2) a skip index on `siphon_routes.path`.
- The handler is the first non-ontology-driven SDLC handler outside `modules/code/` and `modules/namespace_deletion/`. It departs from the documented "ontology first" convention and the deviation needs to be motivated in code comments + AGENTS.md.
- Mode B is dead code if Analytics replication lands cleanly; carrying it adds test surface that exists purely as a fallback.

## Out of scope

- **`@`-mention edges** (`*_user_mentions` tables). Separate effort, separate Siphon prerequisite.
- **Resource state / label / milestone events.** Tracked under kg#482 with dedicated `resource_*_events` Siphon replication.
- **Banzai HTML rendering.** The parser only extracts GFM references from plain text; `lib/banzai/reference_parser/*_parser.rb` (HTML-AST-based) is explicitly *not* what we port.
- **External (Jira) issue references.** Out per upstream kg#499.
- **The Siphon replication MR itself.** Filed as a separate Analytics-owned MR against `gitlab-org/analytics-section/siphon`; this ADR depends on it but does not specify it.
- **`@-link_type` property on `MENTIONS` to distinguish `relate` vs. `moved` vs. `duplicate`.** Open design question for review feedback; default proposal is yes, using the existing `link_type` enum pattern from `related_to.yaml`. To be settled in the implementation MR, not the ADR.

## Key risks

1. **`system_note_metadata` Siphon replication slip.** This is the single longest-lead-time item. The Siphon-side MR is filed in parallel with this ADR; if it slips materially past the implementation MR review, the handler ships in Mode B with a documented degradation note and flips to Mode A when replication lands. The handler is designed to make the mode switch a single config change with no schema impact.
2. **Parser drift against Rails' `ICON_TYPES` and body templates.** `ICON_TYPES` has grown across releases (now 61 values; prior captures show ~50) and Rails has moved system-note phrasing more than once. Mitigation: vendored constant + CI drift check + `log_and_drop` on unknown actions + regex anchored on the GFM-reference token rather than on the verb phrase (so phrasing changes do not break extraction).
3. **Custom-handler maintenance cost.** This is the first cross-reference-oriented handler departing from the ontology-first convention. Mitigation: confine the deviation to the *materialization logic* only — edge **kinds** still declare in YAML — and document the rationale in `AGENTS.md` so future ADRs do not treat this as precedent for arbitrary custom handlers.
4. **`siphon_routes.path` IN-list scan cost.** No skip index on `path` today (only a `pg_pkey_ordered` projection on `id`). Benchmark 2 will produce the number; if it fails the threshold, a `set(N)` or `bloom_filter` skip index on `path` is the prepared mitigation, and Adam's guidance (skip indexes over projections) aligns with the Siphon team's review preferences.
5. **Acceptance threshold vagueness.** kg#499 says "a material increase in edge density (numeric target to be set after initial measurement)". This ADR proposes ≥3× MR↔WorkItem and ≥10× MR↔MR as concrete numeric forms. If review prefers different thresholds, the POC harness (`xtask system-notes-bench`) can re-run cheaply.

## References

- Upstream issue: [kg#499](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/499)
- POC MR: [!1335](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335)
- Closed prior MR: [!1109](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1109)
- Related: kg#482 (MR ingestion gaps), kg#443 (migration framework, complete), graph-completeness epic
- Prior research and POC plan: [dgruzd/droid-workspace/task/2685](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685/)
- Rails source of truth: `app/models/system_note_metadata.rb` (`ICON_TYPES`, `TYPES_WITH_CROSS_REFERENCES`), `app/services/system_notes/*.rb` (body templates), `app/models/{issue,merge_request,commit,project}.rb` (`reference_pattern`)
- Siphon repo: `gitlab-org/analytics-section/siphon`
- ADR precedent: [009 (Code Indexer Service)](009_code_indexer_service.md) for implementation-plan shape; [012 (GOON Format)](012_goon_format.md) for benchmark-driven decision rationale and the vendored-constant + CI drift-check pattern
- Custom-handler precedent in code: `crates/indexer/src/modules/code/`, `crates/indexer/src/modules/namespace_deletion/`
- Schema version file: `config/SCHEMA_VERSION` (44 → 45 with this work)
- Routes-join precedent: `config/ontology/nodes/core/project.yaml:115-121`
- Note filter today: `config/ontology/nodes/core/note.yaml` (`where: "system = false"`)
