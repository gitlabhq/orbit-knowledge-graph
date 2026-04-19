# V2 JS Integration Task List

This tracks the remaining cleanup and hardening work on `michaelangeloio/v2-js-integration`.

## Active

1. Remove all legacy dependencies from the v2 JS path.
   The custom JS pipeline should not depend on legacy linker types, legacy analyzers, or legacy graph/result shapes in any way.
2. Replace `JsModuleInfo` as a second module-semantics model.
   Analyzer output, phase 1 lowering, and `JsModuleIndex` still duplicate the same module/export state. Collapse this into one runtime authority.
3. Keep re-running focused validation after each change.
   At minimum: `cargo fmt --all --check`, `cargo clippy -p code-graph --all-targets --all-features -- -D warnings`, and `cargo test -p integration-tests-codegraph --test suites`.

## Next

1. Unify import-edge and imported-call export resolution.
   `resolver.rs` and `cross_file.rs` still have parallel export traversal stacks. Default/named/star/CJS resolution should go through one `JsModuleIndex`-backed API.
2. Make Vue-specific relationship handling explicit.
   Computed properties, watchers, and lifecycle hooks should not rely on catch-all relationship assumptions; add explicit mapping even if the current relationship kind stays the same.
3. Remove or justify remaining dead JS v2 surface area.
   Audit any JS-only helper types, lookup maps, or builder fields that are not consumed outside tests or can be folded into shared helpers.
4. Expand accuracy coverage for call graph, definitions, references, and module resolution.
   Keep adding YAML coverage for GitLab monolith patterns, Vue/SFC behavior, reexports, callback discrimination, transpiled interop, and real-world JS/TS import styles.
5. Port any remaining legacy behavior into v2-native code and delete the legacy dependency path entirely.
   This includes removing any remaining legacy-only contracts from `custom/js`.

## Done

1. Bench-xtasks workflow repair was intentionally removed from this list for now.
2. Added repo-boundary enforcement for webpack alias and local module resolution.
   Resolved paths are now normalized and rejected if they escape the repository root.
3. Fixed JS-family extension routing across the v2 config and pipeline surface.
   `jsx`, `tsx`, `mjs`, `cjs`, `mts`, and `cts` are now consistently detected and routed.
4. Fixed the YAML harness so generic multi-language pipeline outputs are merged instead of overwritten.
   This was required to test mixed JavaScript and TypeScript suites accurately.
5. Added and strengthened YAML regressions for path containment and mixed JS-family call-graph resolution.
   The extension suite now asserts real import-target resolution and cross-file `Calls` edges.
6. Re-ran focused validation after these changes.
   `cargo fmt --all --check`, `cargo clippy -p code-graph --all-targets --all-features -- -D warnings`, and `cargo test -p integration-tests-codegraph --test suites` all passed.
7. Removed the legacy relationship bridge from the v2 JS cross-file resolver.
   `custom/js` no longer uses `ConsolidatedRelationship`, `RelationshipKind`, `RelationshipType`, or the stale `_modules` parameter for cross-file call resolution.
8. Removed dead JS v2 helper surface.
   Deleted `JsImportedMemberBinding`, `JsImportedBinding::member()`, and the unused `JsPhase1FileInfo::export_def_nodes` field.
9. Re-ran the local GitLab v2 benchmark and RSS pass on `/Users/angelo.rivera/gitlab/gdk/gitlab`.
   Release build: `cargo build --bin orbit --release --features duckdb-client/bundled`
   Timing: `23.29s` wall clock, `91.36s` user, `19.24s` system, `474%` CPU, `1158544 KB` max RSS from `gtime`.
   Sampled RSS: `455852 KB` average, `1210320 KB` max, `34464 KB` min.
10. Consolidated duplicated JS analysis traversal helpers.
   Binding-pattern walking now lives in `analysis/patterns.rs`, and static object-property walking is shared between `analysis/cjs.rs` and `analysis/analyzer.rs`.
11. Folded the `!950` review feedback into this task list.
   The active cleanup set now includes the security review, maintainability notes, and the need to keep reducing the largest JS v2 files.
12. Stopped the webpack alias evaluator from reading host environment variables.
   `process.env.*` now evaluates to `Undefined`, and the repo-boundary YAML suite covers the env- and `fs.existsSync`-gated escape cases.
13. Stopped swallowing SFC parse failures in the JS v2 front-end.
   Embedded script extraction now returns an error that is surfaced into the pipeline error path.
14. Removed a bit more dead JS v2 surface.
   Deleted the unused `GraphLookup.def_by_file_and_fqn` map and the unused `module_binding` invocation-support parameter.
15. Collapsed the repeated Vue lifecycle hook list into one source of truth.
   Detection and extraction now share the same lifecycle hook helper, including `serverPrefetch`.
16. Made `JsModuleInfo::merge` collision-aware.
   Export and definition-range merges now keep the first binding and `debug_assert!` on duplicate keys instead of silently overwriting.
17. Collapsed the repeated JS export/reexport traversal branches in `cross_file.rs`.
   Specifier normalization, reexport following, star-export fallback, and CJS binding adaptation now run through shared local helpers instead of repeating the same recursion pattern across multiple methods.
18. Centralized JS invocation-support inference.
   Analyzer, CJS export extraction, and resolver fallback now share one invocation-classification helper instead of re-deriving the same rules in three places.
19. Expanded YAML coverage for type-only TS, transpiled CJS interop, Vue async/setup, and sharper monolith call edges.
   The suite now asserts `interface` / `type` / `enum` extraction, `is_type_only`, `exports.default` call resolution, `<script setup>`, `defineAsyncComponent`, and namespace-member calls in GitLab-style fixtures.
20. Exposed boolean import metadata in the YAML harness.
   `ImportedSymbol.is_type_only` now round-trips through the Arrow datasets and validator, including boolean row assertions and filtered batches.
21. Removed `process` and `fs.existsSync` emulation from the webpack alias evaluator.
   The evaluator is smaller and more deterministic now: untrusted repos can no longer branch alias resolution on host `process` objects or filesystem probes, and the repo-boundary suite still verifies that escape attempts do not resolve outside the checkout.
22. Replaced the duplicate cross-file call-resolution model with the graph-backed module layer.
   `attach_resolution_edges` now resolves imported calls through `JsModuleIndex` instead of cloning `JsModuleInfo` into a second per-file export map, and CommonJS calls keep concrete definition ranges after the rewrite.
23. Added hard evaluator budgets and split the evaluator out of `cross_file.rs`.
   The webpack alias evaluator now lives in `custom/js/evaluator.rs`, enforces per-file, total-byte, module-count, statement-count, and recursion-depth limits, and keeps `JsCrossFileResolver` focused on module/export resolution.
24. Collapsed the repeated symbol-analysis passes in `analysis/analyzer.rs`.
   Symbol facts, invocation support, and definition extraction now come from one `symbol_ids()` pass instead of separate extraction and annotation walks.
25. Replaced the alias fixed-point rescans in `analysis/calls.rs` with a dependency worklist.
   Alias declarators are now processed when their base import symbol becomes available, which removes repeated rescans of unrelated declarators in long alias chains.
26. Shared more CFG/control-flow helpers with the base SSA engine.
   `dsl/ssa.rs` now provides sealed-successor and join-block helpers, and the OXC walker reuses them for child scopes, conditionals, logical joins, and loop exits instead of reimplementing the block wiring inline.
27. Measured real GitLab monolith alias/path coverage against the local DuckDB artifact.
   On `/Users/angelo.rivera/gitlab/gdk/gitlab`, CE path-like JS imports are strong, but direct `ee/` alias imports resolved at `0/9352`, `.vue` imports at `0/11061`, and `.graphql` imports at `0/2702`, so overall JS/TS/Vue alias-path coverage is still far below the desired 90% bar.
28. Removed unresolved default/named import fallback to synthetic module proxies.
   Declarative imports now stay unresolved when the requested export does not exist. Runtime `require()` bindings still fall back to the module node only when the module exposes no primary export and is being used as a namespace-like object.
29. Added ambiguity-aware `export *` handling.
   Both import-edge resolution and imported-call resolution now refuse to pick an arbitrary target when multiple star reexports expose the same export name.
30. Replaced the leaked `__js_module__` surface with path-based module identity.
   Synthetic module definitions now use the repository-relative file path as their internal scope key, the module index no longer exposes lookup-by-synthetic-fqn helpers, and the YAML assertions no longer depend on a magic prefix.
31. Reduced retained JS phase-1 duplication.
   The pipeline now consumes `JsPhase1File` while building the graph and retains only the analysis state needed for resolution, instead of carrying the lowered phase-1 payload through the entire resolver pass.
32. Restored safe repo-contained config evaluation for alias gating.
   The webpack evaluator now models `process.env` as an empty object, allows `fs.existsSync()` only for repo-contained paths under the existing containment checks, and still refuses to read host environment variables or resolve paths outside the checkout.
33. Routed Vue, Svelte, Astro, GraphQL, GQL, and JSON files through the JS v2 discovery path.
   The v2 config and JS resolver now treat these files as first-class JS ecosystem inputs, which enables SFC extraction and file-backed module stubs without hardcoded GitLab logic.
34. Added YAML regressions for unresolved exports and ambiguous star barrels.
   The JS module suite now asserts that missing named/default exports remain unresolved and that conflicting `export *` chains do not create import edges.
35. Added `ImportedSymbol.has_target` to the YAML harness.
   This makes unresolved import assertions first-class without relying on unsupported `OPTIONAL MATCH` or negated edge-pattern syntax in the mini Cypher harness.
36. Re-measured the real GitLab monolith after the routing and evaluator fixes.
   On `/Users/angelo.rivera/gitlab/gdk/gitlab` with the current release build, direct `ee/` imports now resolve at `11428/11462` (`99.70%`), `.vue` imports at `17106/17178` (`99.58%`), and `.graphql` / `.gql` imports at `4375/4402` (`99.39%`). The artifact lives at `temp/orbit-gitlab-v2-current/graph.duckdb`.
