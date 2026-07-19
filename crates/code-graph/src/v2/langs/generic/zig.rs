use crate::v2::config::Language;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::DefKind;
use treesitter_visit::extract::Extract;
use treesitter_visit::extract::{child_of_kind, field, field_chain, text};
use treesitter_visit::predicate::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::{Node, SupportLang};

use crate::v2::types::BindingKind;

use crate::v2::linker::rules::{ImportStrategy, ReceiverMode, ResolutionRules, ResolveStage};
use crate::v2::linker::HasRules;

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct ZigDsl;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// Zig builtin primitive types skipped for binding type annotations.
/// Arbitrary-width integers (`u7`, `i3`, ...) can't be enumerated, so
/// this covers the common spellings only.
const ZIG_BUILTIN_TYPES: &[&str] = &[
    "bool",
    "void",
    "type",
    "anyerror",
    "anyopaque",
    "anytype",
    "noreturn",
    "comptime_int",
    "comptime_float",
    "isize",
    "usize",
    "i8",
    "i16",
    "i32",
    "i64",
    "i128",
    "u8",
    "u16",
    "u32",
    "u64",
    "u128",
    "f16",
    "f32",
    "f64",
    "f80",
    "f128",
];

/// Classify a plain `variable_declaration` (non-container initializer)
/// by its introducing keyword. tree-sitter-zig emits the `const`/`var`
/// keyword as an anonymous child of the declaration node.
fn classify_zig_binding(node: &N<'_>) -> &'static str {
    if node.children().any(|c| c.kind().as_ref() == "var") {
        return "Var";
    }
    "Const"
}

/// Record `const std = @import("std");` as an import. tree-sitter-zig
/// parses the initializer as `builtin_function(builtin_identifier, arguments)`,
/// so we hook on the `variable_declaration` whose initializer is an
/// `@import` builtin call. Phase 1 only records the import; no resolution.
fn zig_extract_imports(node: &N<'_>, imports: &mut Vec<crate::v2::types::CanonicalImport>) -> bool {
    use crate::v2::types::{CanonicalImport, ImportBindingKind, ImportMode};

    if node.kind().as_ref() != "variable_declaration" {
        return false;
    }

    let Some(builtin) = node
        .children()
        .find(|c| c.kind().as_ref() == "builtin_function")
    else {
        return false;
    };

    let is_import = builtin
        .children()
        .any(|c| c.kind().as_ref() == "builtin_identifier" && c.text().as_ref() == "@import");
    if !is_import {
        return false;
    }

    let Some(path) = builtin
        .find(Descendant, Kind("string_content"))
        .map(|s| s.text().to_string())
    else {
        return false;
    };

    let name = node
        .children()
        .find(|c| c.kind().as_ref() == "identifier")
        .map(|c| c.text().to_string());

    imports.push(CanonicalImport {
        import_type: "Import",
        binding_kind: ImportBindingKind::Named,
        mode: ImportMode::Declarative,
        path,
        name,
        alias: None,
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard: false,
    });
    true
}

/// Build a scope rule for a Zig container type declared as a value binding,
/// e.g. `const Point = struct { ... };`. All four container kinds
/// (struct/enum/union/opaque) share the same shape: a `variable_declaration`
/// classified as a `DefKind::Class` scope, named from its identifier.
///
/// The `const`/`var` keyword guard is required on every one: tree-sitter-zig
/// also parses reassignments like `Shape = struct {...};` as a
/// `variable_declaration` with the same container child but no keyword, and
/// those must not produce a definition. Centralizing the guard here keeps the
/// four rules consistent and impossible to get subtly out of sync.
fn zig_container(label: &'static str, decl_kinds: &'static [&'static str]) -> ScopeRule {
    scope("variable_declaration", label)
        .def_kind(DefKind::Class)
        .when(has_child(&["const", "var"]).and(has_child(decl_kinds)))
        .name_from(child_of_kind("identifier"))
}

/// Index anonymous `test {}` blocks. These are valid Zig (they always run and
/// cannot be selected/skipped by name-based test filtering), so they represent
/// real test coverage worth surfacing. tree-sitter-zig parses them as a
/// `test_declaration` with only a `block` child — no `identifier` or `string`
/// to name them from — so the scope rules skip them and this hook injects the
/// definition instead.
///
/// The name is synthesized from the 1-based start line (`test@L12`) so multiple
/// anonymous tests in the same file get distinct names and FQNs, keeping each
/// one individually locatable rather than collapsing them into a single
/// ambiguous entry.
fn zig_anonymous_test(
    node: &N<'_>,
    defs: &mut Vec<crate::v2::types::CanonicalDefinition>,
    scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> bool {
    use crate::v2::types::{CanonicalDefinition, Fqn};

    if node.kind().as_ref() != "test_declaration" {
        return false;
    }

    // Named (`test foo {}`) and string (`test "name" {}`) tests are handled by
    // the scope rules; only anonymous blocks fall through to here.
    let is_named = node
        .children()
        .any(|c| matches!(c.kind().as_ref(), "identifier" | "string"));
    if is_named {
        return false;
    }

    let line = node.start_pos().line() + 1;
    let name = format!("test@L{line}");
    let fqn = Fqn::from_scope(scope_stack, &name, sep);

    defs.push(CanonicalDefinition {
        definition_type: "Test",
        kind: DefKind::Function,
        name,
        fqn,
        range: crate::v2::dsl::utils::canonical_range(&crate::utils::node_to_range(node)),
        is_top_level: scope_stack.is_empty(),
        metadata: None,
    });

    // Return false so the engine still walks the test body and captures any
    // references inside it.
    false
}

impl DslLanguage for ZigDsl {
    fn name() -> &'static str {
        "zig"
    }

    fn language() -> Language {
        Language::Zig
    }

    fn scopes() -> Vec<ScopeRule> {
        // Scope rules are checked in reverse order, so the unconditional
        // fallbacks come first and the conditional container rules after.
        vec![
            // Plain `const X = ...;` / `var x = ...;` (non-container initializer).
            // The keyword guard is required: tree-sitter-zig also parses plain
            // reassignments (`x = true;`) as `variable_declaration` nodes with
            // no `const`/`var` keyword, and those are not definitions.
            scope_fn("variable_declaration", classify_zig_binding)
                .def_kind(DefKind::Property)
                .no_scope()
                .when(has_child(&["const", "var"]))
                .name_from(child_of_kind("identifier")),
            // Container types declared as value bindings, e.g.
            // `const Point = struct { ... };`. These create a scope so that
            // functions declared inside get FQNs like `Point.init`. See
            // `zig_container` for the shared const/var reassignment guard.
            zig_container("Struct", &["struct_declaration"]),
            zig_container("Enum", &["enum_declaration"]),
            zig_container("Union", &["union_declaration"]),
            zig_container("Opaque", &["opaque_declaration"]),
            scope("function_declaration", "Function")
                .def_kind(DefKind::Function)
                .name_from(field("name")),
            // `test foo {}` decltest form — name from the identifier.
            scope("test_declaration", "Test")
                .def_kind(DefKind::Function)
                .when(has_child(&["identifier"]))
                .name_from(child_of_kind("identifier")),
            // `test "name" {}` — name from the string content, without quotes.
            scope("test_declaration", "Test")
                .def_kind(DefKind::Function)
                .when(has_child(&["string"]))
                .name_from(child_of_kind("string").child_of_kind("string_content")),
            // Anonymous `test {}` blocks (valid Zig — they always run and
            // can't be excluded by name-based test filtering) have no
            // identifier or string to name them, so the two rules above don't
            // match. They're indexed instead via the `on_scope` hook
            // (`zig_anonymous_test`), which synthesizes a position-based name
            // so each anonymous test is a distinct, locatable definition.
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        // Zig builtins (`@import`, `@intCast`, ...) parse as `builtin_function`
        // nodes, not `call_expression`, so they are skipped automatically.
        vec![
            reference("call_expression")
                .name_from(field_chain(&["function", "member"]))
                .when(field_kind("function", &["field_expression"]))
                .receiver_chain(&["function", "object"]),
            reference("call_expression")
                .name_from(field("function"))
                .when(field_kind("function", &["identifier"])),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier"],
            // Zig has no `this`/`super`; method calls go through explicit
            // `self` parameters, which resolve as ordinary bindings.
            this_kinds: &[],
            super_kinds: &[],
            field_access: vec![FieldAccessEntry {
                kind: "field_expression",
                object: field("object"),
                member: field("member"),
            }],
            constructor: &[],
            qualified_type_kinds: &[],
        })
    }

    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        // The file is the namespace in Zig; there is no package clause.
        None
    }

    fn bindings() -> Vec<BindingRule> {
        vec![
            binding("variable_declaration", BindingKind::Assignment)
                .name_from_extract(child_of_kind("identifier"))
                .value_from_extract(
                    text()
                        .nth(Child, Named, -1)
                        .try_descendant("call_expression"),
                )
                .typed(vec![field("type")], ZIG_BUILTIN_TYPES),
            binding("parameter", BindingKind::Parameter)
                .name_from(&["name"])
                .no_value()
                .typed(vec![field("type")], ZIG_BUILTIN_TYPES),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_statement")
                .branches(&["block_expression", "else_clause"])
                .condition("condition"),
            branch("switch_expression").branches(&["switch_case"]),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("while_statement").body("body"),
            loop_rule("for_statement").body("body"),
        ]
    }

    fn hooks() -> types::LanguageHooks {
        types::LanguageHooks {
            return_kinds: &["return_expression"],
            on_import: Some(zig_extract_imports),
            on_scope: Some(zig_anonymous_test),
            ..Default::default()
        }
    }

    fn ssa_config() -> types::SsaConfig {
        // Zig has no implicit receiver: `self` is an ordinary, explicitly
        // declared parameter, so no self/super names are configured.
        types::SsaConfig::default()
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct ZigRules;

impl HasRules for ZigRules {
    fn rules() -> ResolutionRules {
        let spec = ZigDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "zig",
            scopes,
            spec,
            vec![ResolveStage::SSA, ResolveStage::ImportStrategies],
            // No SamePackage/wildcard semantics: Zig has no packages or
            // wildcard imports, so only explicit-import and same-file
            // resolution apply.
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::SameFile,
            ],
            // Zig has no receiver keyword (`this`); methods take an explicit
            // `self` parameter that resolves as a plain binding.
            ReceiverMode::None,
            ".",
            &[],
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    fn parse(
        code: &str,
    ) -> Result<crate::v2::dsl::engine::ParsedDefs, crate::v2::pipeline::PipelineError> {
        ZigDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.zig",
                crate::v2::config::Language::Zig,
                &Tracer::new(false),
                Default::default(),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| {
                crate::v2::pipeline::PipelineError::parse(
                    "test.zig",
                    format!("parse error: {:?}", e),
                )
            })
    }

    #[test]
    fn fn_declaration() {
        let result = parse("pub fn add(a: i32, b: i32) i32 {\n    return a + b;\n}\n").unwrap();
        assert!(result.definitions.iter().any(|d| d.name == "add"
            && d.kind == DefKind::Function
            && d.definition_type == "Function"));
    }

    #[test]
    fn struct_with_scoped_method() {
        let result = parse(
            "const Point = struct {\n    x: f64,\n    y: f64,\n\n    pub fn init(x: f64, y: f64) Point {\n        return Point{ .x = x, .y = y };\n    }\n};\n",
        )
        .unwrap();
        let point = result
            .definitions
            .iter()
            .find(|d| d.name == "Point")
            .unwrap();
        assert_eq!(point.kind, DefKind::Class);
        assert_eq!(point.definition_type, "Struct");
        let init = result
            .definitions
            .iter()
            .find(|d| d.name == "init")
            .unwrap();
        assert!(
            init.fqn.to_string().ends_with("Point.init"),
            "expected FQN scoped under Point, got {}",
            init.fqn
        );
    }

    #[test]
    fn enum_union_opaque_classification() {
        let result = parse(
            "const Color = enum { red, green, blue };\nconst Value = union(enum) {\n    int: i64,\n    float: f64,\n};\nconst Handle = opaque {};\n",
        )
        .unwrap();
        assert!(result
            .definitions
            .iter()
            .any(|d| d.name == "Color" && d.kind == DefKind::Class && d.definition_type == "Enum"));
        assert!(result.definitions.iter().any(|d| d.name == "Value"
            && d.kind == DefKind::Class
            && d.definition_type == "Union"));
        assert!(result.definitions.iter().any(|d| d.name == "Handle"
            && d.kind == DefKind::Class
            && d.definition_type == "Opaque"));
    }

    #[test]
    fn plain_const_and_var() {
        let result = parse("const MAX: u32 = 100;\nvar counter: u32 = 0;\n").unwrap();
        assert!(result.definitions.iter().any(|d| d.name == "MAX"
            && d.kind == DefKind::Property
            && d.definition_type == "Const"));
        assert!(result.definitions.iter().any(|d| d.name == "counter"
            && d.kind == DefKind::Property
            && d.definition_type == "Var"));
    }

    #[test]
    fn test_block_named_from_string() {
        let result = parse(
            "fn add(a: i32, b: i32) i32 {\n    return a + b;\n}\n\ntest \"addition works\" {\n    _ = add(1, 2);\n}\n",
        )
        .unwrap();
        assert!(result.definitions.iter().any(|d| d.name == "addition works"
            && d.kind == DefKind::Function
            && d.definition_type == "Test"));
    }

    #[test]
    fn reassignment_is_not_a_definition() {
        // tree-sitter-zig parses `persist = true;` (no const/var keyword) as a
        // `variable_declaration` node; it must not produce a definition.
        let result =
            parse("pub fn main() void {\n    var persist = false;\n    persist = true;\n}\n")
                .unwrap();
        let persist_defs: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.name == "persist")
            .collect();
        assert_eq!(
            persist_defs.len(),
            1,
            "expected exactly one definition for `persist`, got {:?}",
            persist_defs
                .iter()
                .map(|d| &d.definition_type)
                .collect::<Vec<_>>()
        );
        assert_eq!(persist_defs[0].definition_type, "Var");
    }

    #[test]
    fn container_reassignment_is_not_a_definition() {
        // `Shape = struct {...};` (a reassignment) also parses as a
        // `variable_declaration` with a `struct_declaration` child but no
        // const/var keyword; it must not produce a container definition.
        let result = parse(
            "var Shape: type = u32;\npub fn main() void {\n    Shape = struct { x: f64 };\n}\n",
        )
        .unwrap();
        let shape_defs: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.name == "Shape")
            .collect();
        assert_eq!(
            shape_defs.len(),
            1,
            "expected exactly one definition for `Shape`, got {:?}",
            shape_defs
                .iter()
                .map(|d| &d.definition_type)
                .collect::<Vec<_>>()
        );
        assert_eq!(shape_defs[0].definition_type, "Var");
    }

    #[test]
    fn anonymous_test_block_is_indexed() {
        // `test {}` without a name is valid Zig (it always runs and cannot be
        // excluded by name-based test filtering), so it must be indexed as a
        // Test definition with a synthesized, non-empty name.
        let result = parse("test {\n    const x = 1;\n    _ = x;\n}\n").unwrap();
        let tests: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.definition_type == "Test")
            .collect();
        assert_eq!(
            tests.len(),
            1,
            "anonymous test block should produce exactly one Test definition, got {:?}",
            result
                .definitions
                .iter()
                .map(|d| (&d.name, d.definition_type))
                .collect::<Vec<_>>()
        );
        assert_eq!(tests[0].kind, DefKind::Function);
        // Name is synthesized from the 1-based start line (`test {` is on line 1).
        assert_eq!(tests[0].name, "test@L1");
        assert!(
            !result.definitions.iter().any(|d| d.name.is_empty()),
            "no definition should have an empty name"
        );
    }

    #[test]
    fn multiple_anonymous_test_blocks_get_distinct_fqns() {
        // Two anonymous tests in one file must remain individually locatable,
        // so their synthesized names (and thus FQNs) must not collide.
        let result = parse(
            "test {\n    const a = 1;\n    _ = a;\n}\n\ntest {\n    const b = 2;\n    _ = b;\n}\n",
        )
        .unwrap();
        let mut fqns: Vec<String> = result
            .definitions
            .iter()
            .filter(|d| d.definition_type == "Test")
            .map(|d| d.fqn.to_string())
            .collect();
        assert_eq!(
            fqns.len(),
            2,
            "expected two anonymous Test defs, got {fqns:?}"
        );
        fqns.sort();
        fqns.dedup();
        assert_eq!(
            fqns.len(),
            2,
            "anonymous test FQNs must be distinct, got {fqns:?}"
        );
    }

    #[test]
    fn named_and_anonymous_tests_coexist() {
        // The `on_scope` hook must only fire for anonymous tests: named and
        // string tests are still handled by the scope rules and must not be
        // double-counted.
        let result = parse(
            "test \"named one\" {\n    const a = 1;\n    _ = a;\n}\n\ntest {\n    const b = 2;\n    _ = b;\n}\n",
        )
        .unwrap();
        let names: Vec<&str> = result
            .definitions
            .iter()
            .filter(|d| d.definition_type == "Test")
            .map(|d| d.name.as_str())
            .collect();
        assert!(
            names.contains(&"named one"),
            "expected the string-named test, got {names:?}"
        );
        assert!(
            names.iter().any(|n| n.starts_with("test@L")),
            "expected the anonymous test, got {names:?}"
        );
        assert_eq!(
            names.len(),
            2,
            "named test must not be double-counted by the on_scope hook, got {names:?}"
        );
    }

    #[test]
    fn import_declaration_parses() {
        let result = parse("const std = @import(\"std\");\n").unwrap();
        assert!(result
            .imports
            .iter()
            .any(|i| i.path == "std" && i.name == Some("std".to_string())));
    }
}
