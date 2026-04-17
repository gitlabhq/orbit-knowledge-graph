# GetGraphStatus — Comprehensive E2E Test Matrix

Every row is a concrete executable scenario. Each test targets a specific
invariant so failures point at a narrow piece of the system (reader,
writer, scheduler, auth, KV, staleness, etc.). Tests are grouped by the
invariant class and implemented as Robot Framework test cases under
`e2e/local/suites/`.

Legend:
- **Prod = Production value** (what a real client would see).
- **Pre = Precondition**; **Act = Action**; **Assert = Observable check**.
- **Scope**: `ns` (namespace_id), `proj` (project_id), `path` (full_path).

## A. State transitions (T-series)

Exercise the pending → indexing → idle lifecycle as it actually runs.

| ID | Name | Pre | Act | Assert |
|---|---|---|---|---|
| **T1** | First cycle sequence | fresh group, KG disabled | enable KG, poll every 1s for up to 180s | observe `pending` at least once, then `indexing` (optional — may be too fast), then `idle`. `initial_backfill_done` transitions `false → true` exactly once. |
| **T2** | Subsequent cycle | group already `idle` | wait for a dispatcher tick, poll every 0.5s | observe `idle → (indexing possibly) → idle`; `cycle_count` advances by exactly 1 |
| **T3** | Zero-row re-dispatch | `idle` with data | wait 150s | `cycle_count++`, `updated_at` advances, all `domains[].items[].count` unchanged |
| **T4** | No regression `idle → pending` | `idle` with `initial_backfill_done=true` | poll 10 times over 300s | never observe `state=pending` |
| **T5** | Mid-`indexing` serves prior counts | catch state during cycle | poll in tight loop | if `state=indexing` and a prior `idle` snapshot exists, `edge_counts` = prior values |

## B. Monotonic invariants (M-series)

Track values that must never decrease.

| ID | Invariant | Method |
|---|---|---|
| **M1** | `initial_backfill_done` never `true → false` | 5 polls, 30s apart |
| **M2** | `sdlc.cycle_count` non-decreasing | 5 polls, 30s apart |
| **M5** | `updated_at` non-decreasing (parse RFC3339) | same 5 polls |
| **M6** | `last_started_at ≤ last_completed_at` when `state=idle` | after any idle observation |
| **M7** | N observed transitions ⇒ `cycle_count` advances by ≥ N | count `idle→indexing→idle` loops |
| **M8** | `last_completed_at` advances iff `cycle_count` advanced | cross-check delta |
| **M9** | `last_duration_ms > 0` after first cycle | trivial non-zero |
| **M10** | `initial_backfill_done` stays `true` across a forced error | corrupt counts KV key, wait for next cycle, re-verify |

## C. Scope consistency (S-series)

Same backend data viewed through `namespace_id`, `project_id`, `full_path`.

| ID | Check | Method |
|---|---|---|
| **S1** | `project_id` scope returns counts | call with `?project_id=X`, assert `Project >= 1` in counts |
| **S2** | `full_path` vs `namespace_id` return the same meta | call both, assert state / sdlc / code / initial_backfill_done match |
| **S3** | Subgroup inherits top-group meta | subgroup status.state, sdlc, initial_backfill_done == top-group's |
| **S4** | Subgroup totals ⊆ parent totals | sum `domains[].items[].count` at both levels, assert subgroup ≤ parent |
| **S5** | Deep nested path (≥ 4 segments) resolves | 4-level group/subgroup/sub-subgroup/project, assert status 200 + state=idle |
| **S6** | `project_id` on non-indexed project: `source_code` all `pending` | KG enabled but no push yet, query project |

## D. Code indexing (C-series)

Drive the code pipeline end-to-end: push → Siphon → CodeProgressWriter.

| ID | Check | Method |
|---|---|---|
| **C1** | Push → code entities appear | create project, push 1 commit with source files, wait 120s, assert `File`+`Directory`+`Definition` > 0 |
| **C2** | `code.projects_indexed == 1` after first index | same as C1 |
| **C3** | `code.projects_total` tracks total projects | create 3 projects, assert `projects_total == 3` |
| **C4** | Code edge kinds present | after C1, assert `edge_counts.CONTAINS > 0` |
| **C5** | Subsequent commit refreshes `last_indexed_at` | push again, assert `code.last_indexed_at` advances |
| **C6** | Multi-branch KV preservation | push to `main` and `feature`, inspect `code.<pid>` via `nats kv get`, assert both branches present |
| **C7** | Code indexing error preserves monotonic flag | inject parse-failing file, verify flag stays `true` |

## E. Deletion lifecycle (D-series)

Disable KG ⇒ deletion handler purges rows + KV keys.

| ID | Check | Method |
|---|---|---|
| **D1** | Disable → eventual `pending` | `DELETE /admin/knowledge_graph/namespaces/<gid>`, force deletion run, poll |
| **D2** | KV keys purged | `nats kv get indexing_progress meta.<ns>` → `not found`; same for `counts.*` and `code.*` |
| **D3** | Reader sees fresh `pending` after deletion | no stale snapshot leaks |
| **D4** | Re-enable during deletion window: keys survive | disable → re-enable fast → assert keys preserved |
| **D5** | Re-enable after deletion completed: resets to `pending` | deletion run → re-enable → assert `state=pending`, flag `false` |

## F. Boundary / error paths (B-series)

HTTP-layer contract. These are the fast-running, no-setup tests.

| ID | Scenario | Expected |
|---|---|---|
| **B1** | `GET /graph_status` no params | 400 |
| **B2** | Both `namespace_id` and `project_id` | 400 |
| **B3** | `namespace_id=99999999` | 404 |
| **B4** | `project_id=99999999` | 404 |
| **B5** | `full_path=does/not/exist` | 404 |
| **B6** | No auth | 401 |
| **B7** | PAT without `read_api` scope | 403 |
| **B8** | User personal namespace id | 404 (no existence leak) |
| **B9** | Private group user doesn't belong to | 404 (no existence leak) |
| **B10** | NATS down at request time | 503 / `UNAVAILABLE` |
| **B11** | Corrupt JSON in KV | 500 / `Internal`, no serde detail leaked |
| **B12** | Project in KG-disabled namespace | 404 or `state=pending` |
| **B13** | `namespace_id=-1` | 400 or 404 |
| **B14** | `full_path` > 4KB | 400 or 414 |

## G. Staleness (F-series)

`stale` flag behavior relative to `staleness_threshold_secs`.

| ID | Check | Method |
|---|---|---|
| **F1** | `stale=true` when age > threshold | set threshold=5s via config, idle namespace 10s, poll |
| **F2** | `stale=false` immediately after a cycle | right after `idle`, assert false |
| **F3** | Empty `updated_at` ⇒ `stale=true` | fresh namespace with no counts, assert stale |
| **F4** | `stale` resets to false after next cycle | after F1, wait for dispatch, assert false |

## H. Payload shape / content (P-series)

Structural invariants on the JSON body.

| ID | Check | Method |
|---|---|---|
| **P1** | All ontology domains present | extract ontology domains, assert every one appears in `domains[].name` |
| **P2** | Every `has_traversal_path=true` entity appears in some domain | cross-reference via ontology |
| **P3** | `User` (no traversal_path) never appears | assert no item named "User" |
| **P4** | `status=completed` ⇔ `count > 0` | traverse items, verify bijection |
| **P5** | Deterministic item ordering | two consecutive calls return identical ordering |
| **P6** | `edge_counts` only contains known rel kinds | intersect with ontology-known kinds |
| **P7** | `sdlc == null` iff no `meta.<ns>` key | correlate with KV contents |
| **P8** | RFC3339 timestamps parse back | for every `*_at` field |

## I. Concurrency / race (X-series)

| ID | Check | Method |
|---|---|---|
| **X1** | Concurrent reads stable during writes | 20 req/s curl loop for 60s, no 500s, all shapes valid |
| **X2** | Cross-field consistency | if `state=idle`, `updated_at ≥ last_completed_at` |
| **X3** | Multi-namespace isolation | parallel queries on ns A and ns B, payloads independent |

## K. Permissions / AuthZ (Z-series)

Exercise the Rails-side access control. `graph_status` must return the same
visibility as the underlying group/project:

| ID | Case | Expected |
|---|---|---|
| **Z1** | Unauthenticated request on any group | 401 |
| **Z2** | Non-member user queries private group by `namespace_id` | 404 (no existence leak) |
| **Z3** | Non-member user queries private group by `full_path` | 404 |
| **Z4** | Developer member of a private group queries that group | 200 (guest-level members are blocked at the GKG auth layer) |
| **Z5** | Non-member queries member-only private group | 404 |
| **Z6** | Authenticated non-member queries public group | **403 / 404 / 503** (graph_status is member-only regardless of visibility) |
| **Z7** | Authenticated non-member queries internal group | **403 / 404 / 503** (same; member-only) |
| **Z8** | Project scope inherits enclosing group access (developer: 200, non-member: 404) | 200 / 404 |
| **Z9** | Non-member 404 == nonexistent-id 404 (shape equality — no leak) | status codes match |
| **Z10** | Admin can read any group regardless of membership | 200 |
| **Z11** | Revoking membership immediately removes access | 200 → 404 |
| **Z12** | PAT with only `read_user` scope (no api / read_api) is rejected | 401 / 403 |

## J. Ops / integration (O-series)

| ID | Check | Method |
|---|---|---|
| **O1** | `readyz` up while `graph_status` works | 200 on both |
| **O2** | gRPC response equals REST response | `grpcurl GetGraphStatus` ≡ REST JSON |
| **O3** | Prometheus counter increments | scrape `:9394/metrics`, assert `graph_status_requests_total` delta > 0 |

## Execution notes

- `e2e/local/suites/boundary.robot` (B) — fast, no setup, always runs.
- `e2e/local/suites/payload.robot` (P) — fast, read-only.
- `e2e/local/suites/scope.robot` (S) — needs fixtures (group + subgroup + project).
- `e2e/local/suites/state.robot` (T, M, F) — polling tests; uses `Wait Until Keyword Succeeds`.
- `e2e/local/suites/code.robot` (C) — needs git push; slowest.
- `e2e/local/suites/deletion.robot` (D) — destructive; isolated fixtures.
- `e2e/local/suites/concurrency.robot` (X) — separate because can perturb others.
- `e2e/local/suites/ops.robot` (O) — separate because touches metrics/health.
- `e2e/local/suites/permissions.robot` (Z) — creates two non-admin users with
  PATs; isolated fixtures; asserts Rails-side AuthZ is honored by
  `graph_status`.

Each suite uses a shared `common.resource` for auth/fixture helpers.
Use `robot --outputdir results e2e/local/suites` to execute; `report.html`
opens in a browser for review.
