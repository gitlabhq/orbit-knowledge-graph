## Query Performance and Compiler Improvement Tracker

Consolidated backlog from a full compiler audit and production performance test on v0.37.0. 69 queries exercised against `gitlab-org/gitlab` (project 278964, group 9970) on the production cluster.

Full results, debug SQL analysis, and schema audit: https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/snippets/5986918

### Production results summary

| Tier | Count |
|---|---|
| TIMEOUT (504, >30s) | 8 |
| SLOW (7-30s) | 15 |
| MODERATE (3-7s) | 14 |
| FAST (<3s) | 10 |
| ERROR (400) | 2 |

Five failure clusters identified from the corpus:

- **C1 — Dense table tax**: CI tables (Job, Pipeline, Stage, Note) as unfiltered join targets. Cascade CTE narrows by edge existence but property filters (`status`, `state`) apply only after a full `LIMIT 1 BY` dedup scan.
- **C2 — Code graph wall**: All code-edge types (CALLS, DEFINES, EXTENDS, IMPORTS) timeout or near-timeout without `node_ids`. No cascade CTEs for code entities. Dead rows accumulate because code graph tables lack `_deleted` in RMT engine.
- **C3 — 3-node aggregation cliff**: Aggregation through intermediate entity with dense table. Cascade is just an ID set; doesn't carry property filters down.
- **C4 — Null-result scan**: Queries returning 0 rows take 5-21s proving it. No early termination when intermediate CTEs are empty.
- **C5 — Auth OR duplication**: Every CTE and JOIN carries its own copy of the N-way `startsWith(traversal_path, ...)` OR block. 7+ copies of a 30-clause OR in a 3-node query.
- **C6 — Schema-server mismatch**: `collect` aggregation in DSL schema but rejected by server. Scalar aggregation error unclear.

---

### DDL: Schema-only changes

| Status | Task | Summary | Cluster | Queries fixed | MRs |
|---|---|---|---|---|---|
| [ ] | **`_deleted` on code graph ReplacingMergeTree** | `gl_definition`, `gl_file`, `gl_directory`, `gl_imported_symbol` use `ReplacingMergeTree(_version)` without `_deleted`. Dead rows accumulate forever. All other entity tables already have `_deleted`. Schema migration required (can't ALTER ENGINE in ClickHouse). | C2 | Q1,Q21,Q26,Q32,G1 (5 timeouts); Q22,Q31,G3 (3 slow) | |
| [ ] | **`by_project_state` projection on `gl_merge_request`** | `ORDER BY (traversal_path, project_id, state, id, _version)`. `project_id` column already exists. Collapses cascade CTE + dedup into a projection range scan for "MRs in project X with state Y." | C1,C3 | Q4(8.5s), F2(9.8s), I5(11.2s), Q6(6.6s), E2(5.9s) | |
| [ ] | **`by_noteable` projection on `gl_note`** | `ORDER BY (traversal_path, noteable_type, noteable_id, id, _version)`. Notes are the largest SDLC entity, scattered by insert order. Clusters them for `HAS_NOTE` traversals and per-MR aggregations. | C1 | I5(11.2s), A1(11.4s) | |
| [ ] | **`add_minmax_index_for_temporal_columns` on edge tables** | SDLC node tables have this setting; `gl_edge` and `gl_code_edge` don't. Minmax on `_version` helps skip granules during dedup scans. | C1,C2 | General dedup improvement | |
| [ ] | **Index granularity 1024 for code graph node tables** | `gl_definition`, `gl_file` use 2048; edge tables use 1024. Halving granularity gives bloom/text skip indexes 2x more skip opportunities. | C2 | All code graph queries | |

### DDL + write-path changes

| Status | Task | Summary | Cluster | Queries fixed | MRs |
|---|---|---|---|---|---|
| [ ] | **`pipeline_id` FK + projection on `gl_job`** | Add column and `by_pipeline_status` projection `ORDER BY (traversal_path, pipeline_id, status, id, _version)`. Bypasses edge table for Job-Pipeline traversals. Write-path must populate `pipeline_id`. | C1 | Q24(TIMEOUT), I1(17s), Q30(15s) | |
| [ ] | **`pipeline_id` FK + projection on `gl_stage`** | Same pattern as Job, for Stage. | C1 | A2(TIMEOUT) | |
| [ ] | **Edge denormalization (!1095)** | Add `source_state`, `source_status`, `source_type` LowCardinality columns to `gl_edge`. Eliminates node table JOINs in cascade CTEs for denormalized properties. Highest absolute impact. Requires schema migration + write-path changes + write amplification on property updates. | C1,C3 | Q12,Q24,A2 (3 timeouts); Q30,I1,I5,E1,F2,I6 (6 slow) | |
| [ ] | **Composite projection on `gl_edge` for denorm filters** | Conditional on edge denormalization. `ORDER BY (target_id, relationship_kind, source_kind, source_status, source_id, traversal_path, target_kind)`. Lets cascade CTEs use contiguous key ranges on denormalized columns. | C1,C3 | Same as edge denormalization | |

### DDL + compiler changes

| Status | Task | Summary | Cluster | Queries fixed | MRs |
|---|---|---|---|---|---|
| [ ] | **`dedup_ids` projection on dense tables + compiler emit** | Add `SELECT id, _version, _deleted ORDER BY (id, _version DESC)` projection to `gl_job`, `gl_pipeline`, `gl_note`, `gl_merge_request`, `gl_stage`. Compiler emits `ORDER BY id, _version DESC` (matching projection) when CTE is fed by a cascade ID set, reading 3 columns instead of all. Today it emits `ORDER BY traversal_path, id, _version DESC` which forces the main table ORDER BY and reads all columns. | C1,C3 | All cascade queries (Q4, Q30, Q25, D1-D3, E1-E2, F2, I1, I5, I6) | |

### Compiler: query plan improvements

| Status | Task | Summary | Cluster | Queries fixed | MRs |
|---|---|---|---|---|---|
| [ ] | **Materialize authorization scope once** | Every CTE and JOIN subquery gets its own copy of the N-way `startsWith(traversal_path, ...)` OR block. 3-node queries have 7+ copies of a 30-clause OR. Hoist into a single `_auth_paths` CTE or `Array(String)` parameter referenced everywhere. Pure change in `security.rs`. | C5 | All queries (constant overhead reduction) | |
| [ ] | **Push node property filters into cascade CTEs** | `_cascade_B` is a pure ID set from edge scans. Dense table filters (`status='failed'`, `state='merged'`) apply only after the full `LIMIT 1 BY` dedup. Semi-join the node table with filters directly in the cascade CTE so the dedup scan only processes matching rows. | C1,C3,C4 | Q30(15s), D1(11s), E1(11.6s), I1(17s), F2(9.8s), I6(9.8s) | |
| [ ] | **Add cascade CTEs for code graph entities** | Code entities on `gl_code_edge` never get cascades because they lack `IN_PROJECT` edges. Use `_nf_*` CTE from the filtered endpoint to generate a project-scoped code cascade. Extend existing `build_multihop_cascade_for_node()` in `optimize.rs` to recognize code-edge tables via ontology edge table routing. | C2 | Q1,Q21,Q26,Q32,G1 (timeouts); Q22(14.5s), Q31(21.5s), G3(23.6s) | |
| [ ] | **Collapse `_nf_*` and inline dedup into one scan** | Many queries scan the same node table twice: once in `_nf_*` CTE (ID set for SIP) and again as inline dedup subquery in the main JOIN. Both carry the same auth OR and `LIMIT 1 BY`. Emit `_nf_*` as the sole materialized scan and reference it in the main JOIN. Partially addressed by !1119 but dual-scan persists in several shapes. | C1,C5 | All traversal/aggregation queries with node filters | |
| [ ] | **Semi-join rewrite for `gl_code_edge` projection utilization** | Compiler emits `e0.source_id IN (SELECT id FROM _nf_f)` which ClickHouse may not optimize via the `by_source` projection. Rewrite as a direct JOIN against `_nf_*` CTE or explicit key-lookup pattern the projection can serve. | C2 | Q22(14.5s), G3(23.6s) | |

### Server-layer improvements

| Status | Task | Summary | Cluster | Queries fixed | MRs |
|---|---|---|---|---|---|
| [ ] | **Early-terminate on empty intermediate CTEs** | Queries returning 0 rows take 5-21s proving it. If `_cascade_*` or `_nf_*` returns 0 rows, the main query is guaranteed empty. Server issues `SELECT 1 FROM _cascade_B LIMIT 1` probe; if empty, return immediately. | C4 | A1(11.4s/0rows), H3(6.9s/0rows), H4(6.3s/0rows), D4(5.2s/0rows), Q31(21.5s/0rows) | |
| [ ] | **Server-side query caching for cursor pagination** | `input.rs:254` TODO: cursor pagination re-executes the full query for every page. Server-side result caching with TTL would avoid re-running on page 2+. | — | All paginated queries | |

### DSL and schema

| Status | Task | Summary | Cluster | Queries fixed | MRs |
|---|---|---|---|---|---|
| [ ] | **`group_by_property` for self-group aggregations** | `group_by` references a node ID; can't express "count Jobs grouped by `failure_reason`" natively. Adding a `group_by_property` field enables common analytics patterns without workarounds. | C6 | H2(400 error), I1 pattern | |
| [ ] | **`collect` aggregation: implement or remove from schema** | DSL schema lists `collect` as valid but server rejects it. Either implement `groupArray()` mapping or remove from `graph_query.schema.json`. | C6 | Q27(400 error) | |
| [ ] | **Improve scalar aggregation error for multi-node queries** | "multi-node aggregation requires group_by" (from !1087) is intentional, but the DSL schema doesn't communicate the constraint. Add schema-level validation or a clearer error message. | C6 | H2(400 error) | |

### Code quality

| Status | Task | Summary | MRs |
|---|---|---|---|
| [ ] | **Unify codegen backends via trait-based dialect** | `clickhouse.rs` (1019 lines) and `duckdb.rs` (573 lines) share ~80% structure (`emit_query`, `emit_ctes`, `emit_query_body`, `emit_table_ref`). Differences: param syntax, function remapping, LIKE escaping, SETTINGS. Extract shared `SqlEmitter` trait with dialect-specific hooks. ~300 line reduction. DuckDB backend silently misses ClickHouse bug fixes today. | |
| [ ] | **Decompose `lower.rs` (4145 lines) and `optimize.rs` (3546 lines)** | Highest churn files in the crate (100+ commits each in 2 months). Split into per-query-type submodules (`lower/traversal.rs`, etc.) and per-optimization submodules (`optimize/sip.rs`, `optimize/cascade.rs`, etc.). Reduces merge conflict blast radius. | |
| [ ] | **Share `GL_TABLE_RE` regex in `constants.rs`** | Identical regex defined in `deduplicate.rs:32` (`LazyLock`) and `security.rs:34` (`OnceLock`). Different lazy-init mechanisms for the same pattern. | |
| [ ] | **Standardize on `std::sync::LazyLock`** | `types.rs:4` uses `once_cell::sync::Lazy`; rest of crate uses `std::sync::LazyLock` (stabilized in 1.80). | |
| [ ] | **Cache `Regex::new()` in `ParameterizedQuery::render()`** | Both codegen backends compile a fresh regex on every call. Use `LazyLock`. Debug-only method but wasteful. | |
| [ ] | **Remove sentinel `TableRef::scan("_placeholder", "_")`** | Borrow-checker workaround in `enforce.rs:268` and `optimize.rs`. Replace with `Option<TableRef>` or `std::mem::take` with a proper default. | |
| [ ] | **Fix `CodegenPass` silent `unwrap_or_default` fallback** | `passes/mod.rs:233-236` silently uses default `QueryConfig` if `SettingsPass` didn't run. Hides pipeline misconfiguration. Should `expect()` or propagate error. | |

---

### Suggested shipping order

Sequence maximizes cumulative impact at each step:

1. **`_deleted` on code graph RMT** — trivial migration, unblocks C2
2. **MR + Note projections** — low effort DDL, immediate slow-query wins
3. **Minmax indexes + granularity** — trivial DDL, broad marginal gains
4. **Materialize auth scope** — compiler-only, constant overhead reduction everywhere
5. **Code graph cascades** — compiler-only, closes code graph timeout wall
6. **Filter pushdown into cascades + dedup_ids projections** — together close the dense table tax
7. **pipeline_id FK on Job/Stage** — closes CI timeouts
8. **Edge denormalization + composite projection** — closes remaining timeouts
