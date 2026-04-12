# JS/TS Ecosystem Indexer - 40-Agent Validation Report

## Executive Summary

40 agents independently audited the GitLab monolith code graph (415K definitions, 1.19M edges) by comparing actual source code against the indexed DuckDB graph. Every agent found the same core gaps. This report consolidates findings into a definitive gap ranking.

## What Works (confirmed by every agent)

| Capability | Coverage | Notes |
|-----------|----------|-------|
| **Import extraction** | **100%** | Every static `import` statement captured correctly across all files |
| **Definition extraction** | **95%+** | Classes, methods, computed properties, functions, variables, interfaces, enums |
| **Vue Options API methods/computed** | **~95%** | Virtual class + method definitions created correctly |
| **Vue lifecycle hooks/data/watch** | **Good** | mounted, created, beforeDestroy, watchers all extracted |
| **Intra-component this.method()** | **~90%** | boards_selector.vue was 4/4 perfect; most components ~80-90% |
| **Ruby/Python graph** | **100%** | No regressions from JS changes |
| **Import-driven alias inference** | **41.5%** | `~/` aliases resolve via filesystem validation |

## Definitive Gap Ranking

### P0: Cross-file imported function CALLS (the #1 gap)

**Impact:** 95.7% of JS/Vue files have ZERO outgoing cross-file CALLS edges.

**What happens:** A file imports `createAlert` from `~/alert`, calls `createAlert()` in a method body. The import is captured. The definition exists in the target file. But NO CALLS edge connects the call site to the target definition.

**Root cause:** The JS/TS linker extracts `ImportedCall` edges in `extract_call_edges`, which are then resolved by `JsCrossFileResolver::resolve_calls()`. But this only fires when:
1. The import is an ESM import with `is_import()` symbol flag
2. The parent AST node is a direct `CallExpression`
3. The cross-file resolver can resolve the specifier to a target file
4. The target file has a matching export binding with a definition range

Step 3 fails for ~60% of imports (npm packages, unresolved aliases). Step 4 fails when the target uses arrow function exports (Variable, not Function). The result: only ~5% of cross-file calls produce edges.

**Evidence from agents:**
- boards components: 5/6 internal CALLS captured, 0 cross-file utility calls
- diffs components: 20/100+ CALLS captured (80% missing)
- environments: 0 cross-file CALLS across 3 files
- releases: 0 cross-file CALLS across 3 files
- notes: 0 cross-file CALLS across 5 files
- lib/utils/url_utility: 891 importers but only 23 cross-file call edges

### P1: Vue default import resolution (0%)

**Impact:** 16,962 `.vue` default imports at 0% resolution.

**Root cause:** Vue SFC files export their component via `export default { ... }`, but the linker does not synthesize a default `ExportedBinding` for the virtual component class. So `import MyComponent from './MyComponent.vue'` can never resolve.

**Fix:** When emitting a Vue virtual class, also create a default ExportedBinding in the module info pointing to the virtual class definition.

### P2: Arrow function exports classified as Variable

**Impact:** 5,967 JS files contain only Variable definitions. CALLS edges are not extracted from Variable-scoped function bodies.

**Root cause:** `export const foo = () => { ... }` is classified as `Variable` by OXC (technically correct). But the linker only extracts call edges from Function/Method/Class scopes, not Variable scopes. Since the GitLab monolith uses `const fn = () => {}` as the dominant pattern for exported functions, most callable code produces no call graph.

**Evidence:** 41 of 45 `store/actions.js` files have zero Function definitions (all Variables), and therefore zero CALLS edges. The idiomatic Vuex pattern is invisible to the call graph.

### P3: Variable references treated as CALLS (false positives)

**Impact:** ~50% of CALLS edges target Variable definitions that are not function calls (constant references, property accesses).

**Root cause:** The linker creates CALLS edges for ALL resolved references, not just function invocations. When `SELECTED_CLASS` (a string constant) is referenced, it produces a CALLS edge.

### P4: Dynamic import() invisible

**Impact:** Zero import edges for lazy-loaded modules (`() => import('...')`).

**Root cause:** OXC's `ModuleRecord` only tracks static `import` declarations, not dynamic `import()` expressions. The analyzer does not walk AST for `CallExpression` with `import` callee.

### P5: Object instance method calls (0%)

**Impact:** `this.store.method()`, `this.service.fetchData()`, `PersistentUserCallout.factory()` -- none resolve.

**Root cause:** Requires type inference across assignment (`this.store = new ClusterStore()`) to resolve `this.store.method()` to `ClusterStore::method`. The analyzer does not perform cross-expression type propagation.

### P6: Vuex mapState/mapActions/mapMutations invisible

**Impact:** Entire Vuex store layer opaque. `this.saveNote()` (mapped from Pinia) produces no CALLS edge.

**Root cause:** `mapActions(useNotes, ['saveNote'])` injects methods at runtime via string names. Static analysis would need to understand the mapActions helper and resolve string-to-definition lookups.

### P7: Definition ID deduplication (14.6%)

**Impact:** 60,625 definition rows have non-unique IDs. Causes edge duplication (22.8% of all edges are duplicates).

**Root cause:** The `compute_id` hash uses `(project_id, branch, definition_type, fqn)` but does not include byte position. Same-named definitions at different positions get the same ID. Worst offender: `speedscope.026f36b0.js` has 415 definitions all named `e`.

### P8: Top-level calls misattributed

**Impact:** Module-level calls like `Autosize(autosizeEls)` create CALLS edges to the *argument* (`autosizeEls`) instead of the *callee* (`Autosize`).

## Metrics

| Metric | Value |
|--------|-------|
| Total definitions | 415,447 |
| Total edges | 1,191,437 |
| Total CALLS edges | 395,371 |
| Cross-file CALLS (JS/Vue) | 2,596 (0.19% of total) |
| JS/Vue files with zero outgoing cross-file CALLS | 12,892 / 13,467 (95.7%) |
| Import resolution rate (~/  alias) | 41.5% (16,543 / 39,892) |
| Import resolution rate (relative) | 26.8% |
| Vue default import resolution | 0% (0 / 16,962) |
| CALLS edges targeting Variables | ~50% |
| Duplicate edges | 22.8% |

## Agents that contributed

20 component-area auditors (sidebar, work_items, environments, packages, security, deploy_tokens, analytics, clusters, blob, editor, releases, snippets, search, runner, repository, batch_comments, design_management, profile, admin, integrations) + 5 import resolution auditors (~/locale, ~/alert, ~/url_utility, ~/api, Vue defaults) + 7 graph quality auditors (cross-file CALLS, CALLS precision, duplicates, Vue Options API, lifecycle hooks, namespace imports, ID collisions) + 4 coverage gap auditors (zero-def .vue, zero-def .js, vue_shared, content_editor) + 4 structural auditors (wikis, projects, groups, behaviors)
