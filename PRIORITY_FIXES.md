# Priority Fixes for JS/TS Code Graph

Based on 60+ validation agents, 20 AI-usefulness evaluations, and OXC source code research.

## Priority 1: Arrow Function Variable -> Function Promotion
**Impact: 8 AI use cases. Easy fix.**
**Evidence:** 41/45 Vuex store action files produce zero CALLS edges. 68.9% of Vue methods have zero outgoing CALLS. 5,967 JS files contain only Variable definitions.

**OXC capability:** `VariableDeclarator.init` can be pattern-matched against `Expression::ArrowFunctionExpression` and `Expression::FunctionExpression`. This is NOT exposed via `SymbolFlags` (no `is_arrow` flag), but the AST node IS accessible via `scoping.symbol_declaration(symbol_id)` -> `AstKind::VariableDeclarator(decl)` -> `decl.init`.

**Fix location:** `crates/code-graph/linker/src/analysis/languages/js/analysis/analyzer.rs`, function `classify_symbol_kind`, around line 149:
```rust
// Current: returns Variable for all variables
if flags.is_variable() {
    if matches!(nodes.parent_kind(decl_node_id), AstKind::FormalParameter(_)) {
        return None;
    }
    return Some(JsDefKind::Variable);
}

// Fixed: check if the variable's initializer is an arrow/function expression
if flags.is_variable() {
    if matches!(nodes.parent_kind(decl_node_id), AstKind::FormalParameter(_)) {
        return None;
    }
    // Promote arrow functions and function expressions to Function
    if let AstKind::VariableDeclarator(decl) = nodes.kind(decl_node_id) {
        if let Some(init) = &decl.init {
            if matches!(init, Expression::ArrowFunctionExpression(_) | Expression::FunctionExpression(_)) {
                return Some(JsDefKind::Function);
            }
        }
    }
    return Some(JsDefKind::Variable);
}
```

**Cascading effect:** Once arrow functions are classified as Function instead of Variable:
- `build_scope_def_map` will include them (it checks `is_function()` on SymbolFlags, but we also need to add the check based on JsDefKind since the SymbolFlags won't change)
- Actually, the SymbolFlags won't change. So we need a DIFFERENT approach: instead of relying on SymbolFlags in build_scope_def_map, we should use the range-based fallback (Fix 1 from the plan) which already works.
- The main benefit: emit.rs and consumers will see these as Function, not Variable. Queries filtering by definition_type='Function' will include them.
- CALLS edges from arrow function bodies will work via the range-based fallback already in place.

**Effort:** ~10 lines of code change. Zero risk.

---

## Priority 2: Vue Props/Emits/Slots Extraction
**Impact: 5 AI use cases. Medium fix.**
**Evidence:** Vue component API is only 15-20% visible. Props (0%), emits (0%), slots (0%), inject (0%). 15+ props per component invisible.

**OXC capability:** The `ObjectProperty` AST node with key "props" is directly walkable. Each prop is a child ObjectProperty with its own key (prop name) and value (type/default/required object).

**Fix location:** `crates/code-graph/linker/src/analysis/languages/js/analysis/vue.rs`, extend `extract_vue_options_api`:
```rust
// Add to the property iteration loop:
else if key == "props" {
    let oxc::ast::ast::Expression::ObjectExpression(props_obj) = &p.value else { continue };
    for prop in &props_obj.properties {
        let ObjectPropertyKind::ObjectProperty(pp) = prop else { continue };
        let Some(prop_name) = pp.key.static_name() else { continue };
        defs.push(JsDef {
            name: prop_name.to_string(),
            fqn: format!("{component_name}::prop_{}", prop_name),
            kind: JsDefKind::Variable, // or a new JsDefKind::Prop
            range: span_to_range(pp.span),
            is_exported: false,
            type_annotation: None,
        });
    }
}
// Similarly for "emits" (array of strings or object)
else if key == "emits" {
    // Handle both array syntax: emits: ['click', 'update']
    // and object syntax: emits: { click: null, update: (val) => true }
}
// And "inject"
else if key == "inject" {
    // Handle both array: inject: ['foo'] and object: inject: { foo: { from: 'bar' } }
}
```

**New definition types needed:** Consider adding `JsDefKind::Prop`, `JsDefKind::Emit`, `JsDefKind::Inject` to preserve semantics. Or use naming conventions (prop_X, emit_X).

**Effort:** ~50-80 lines. Low risk.

---

## Priority 3: Minified/Vendored File Exclusion
**Impact: All queries. Easy fix.**
**Evidence:** speedscope.026f36b0.js alone produces 1.4M junk edges and 6,700 single-character definitions. Pollutes every query.

**Fix location:** `crates/code-graph/linker/src/parsing/processor.rs` or `crates/code-graph/linker/src/loading/mod.rs`. Add exclusion before parsing:
```rust
fn should_exclude(file_path: &str) -> bool {
    file_path.starts_with("public/")
        || file_path.starts_with("vendor/assets/")
        || file_path.contains("/node_modules/")
        || file_path.ends_with(".min.js")
        || file_path.ends_with(".min.mjs")
        || file_path.ends_with(".bundle.js")
}
```

**Effort:** ~10 lines. Zero risk.

---

## Priority 4: Index .graphql Files
**Impact: 3 AI use cases. Medium fix.**
**Evidence:** 1,698 .graphql imports at 0% resolution. Data layer architecture invisible. "What data does this component fetch?" unanswerable.

**Approach:** Parse .graphql files as simple definition files. Each query/mutation/subscription/fragment becomes a Definition with definition_type "Query"/"Mutation"/"Subscription"/"Fragment". The name comes from the operation name. No need for full GraphQL schema understanding -- just extract named operations.

**Fix location:** Add a new language handler in the parser, or handle it in the JS linker as a special file type. The simplest approach: when processing imports with `.graphql` extension, create a synthetic definition for the imported name and file.

**Effort:** ~100 lines for a basic implementation. Low-medium risk.

---

## Priority 5: Visibility/Export Flag on Definitions
**Impact: 3 AI use cases (unused exports, API surface, dead code).**
**Evidence:** No way to distinguish `export function foo()` from `function _helper()`. Dead code detection has 82% false positive rate partly because internal helpers are flagged.

**OXC capability:** Already used! `exported_bindings` HashMap on ModuleRecord is checked in `extract_definitions`. The `is_exported` field exists on `JsDef`. But it's NOT propagated to the DuckDB `gl_definition` table.

**Fix:** Add `is_exported` column to `gl_definition` in the DuckDB schema (`config/graph_local.sql`). Map `JsDef.is_exported` through emit.rs to the output. This is already computed -- just not stored.

**Effort:** ~20 lines (schema + emit mapping). Zero risk.

---

## Priority 6: Parameter Signature Extraction
**Impact: 4 AI use cases (API surface, debugging, code review, test generation).**
**Evidence:** "What parameters does this function take?" unanswerable from graph alone. 0% parameter visibility.

**OXC capability:** `FormalParameters.items` provides parameter names, type annotations, optionality, and default values. `ArrowFunctionExpression.params` provides the same for arrow functions.

**Approach:** Add a `parameters` text column to `gl_definition` storing a compact signature string like `(message: string, variant?: AlertVariant, title?: string)`. Extract from `Function.params` or `ArrowFunctionExpression.params`.

**Effort:** ~60 lines. Low risk.

---

## Priority 7: Computed vs Method vs Watcher Distinction
**Impact: 3 AI use cases.**
**Evidence:** All Vue Options API members are "Method". Cannot distinguish reactive computations from imperative actions. 28,891 methods with no semantic distinction.

**Fix:** The `vue.rs` extractor already knows which block (`methods`, `computed`, `watch`, lifecycle hooks) each member comes from. Add specific `JsDefKind` variants or use the existing `as_str()` method to return "ComputedProperty", "Watcher", "LifecycleHook" instead of "Method" for these.

**Effort:** ~30 lines. Zero risk.

---

## Priority 8: Test-to-Implementation Mapping
**Impact: 3 AI use cases (test coverage, code review, refactoring).**
**Evidence:** 23,916 Ruby spec files are disconnected from implementations. Zero test-to-production mapping.

**Approach:** Convention-based: `spec/foo_spec.rb` -> `app/foo.rb`. Add a TESTS edge kind. Also extract `describe ClassName` from RSpec files to link by class name.

**Effort:** ~80 lines for convention-based. Medium for describe-based.

---

## Priority 9: EE/CE Path Resolution
**Impact: 2 AI use cases. Medium fix.**
**Evidence:** 11,485 `ee/` and `ee_else_ce/` imports at 0% resolution.

**Approach:** Add `ee/app/assets/javascripts` as an alias source in `infer_aliases_from_imports`. Or better: detect `ee_else_ce` import patterns and resolve them to either `ee/` or `app/assets/` paths.

**Effort:** ~30 lines in cross_file.rs.

---

## Priority 10: Inheritance/Includes Edges
**Impact: 4 AI use cases (debugging, security, onboarding, architecture).**
**Evidence:** 24,500 Class nodes with no EXTENDS edges. Cannot trace method resolution order. "What controllers inherit from ApplicationController?" unanswerable.

**OXC capability:** `Class.super_class` already read in `build_class_hierarchy`. We just need to emit it as a graph edge, not just use it internally.

**Effort:** ~40 lines (new edge type + emission). Requires schema change.
