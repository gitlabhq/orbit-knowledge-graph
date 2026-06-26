use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::*;
use crate::v2::types::{BindingKind, DefKind};
use treesitter_visit::extract::Extract;
use treesitter_visit::extract::{child_of_kind, field, field_chain, text};
use treesitter_visit::predicate::*;

use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolveStage, ResolverHooks,
};
use crate::v2::linker::{HasRules, ResolutionRules};
use treesitter_visit::Node;
use treesitter_visit::syntax_tree as rw;
use treesitter_visit::syntax_tree::SyntaxTree;

type N<'a> = Node<'a, SyntaxTree>;

const GO_PRIMITIVE_TYPES: &[&str] = &[
    "int",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float32",
    "float64",
    "complex64",
    "complex128",
    "string",
    "bool",
    "byte",
    "rune",
    "error",
];

#[derive(Default)]
pub struct GoDsl;

impl DslLanguage for GoDsl {
    fn name() -> &'static str {
        "go"
    }

    fn language() -> Language {
        Language::Go
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("function_declaration", "Function")
                .def_kind(DefKind::Function)
                .metadata(metadata().return_type(field("result"))),
            scope("method_declaration", "Method")
                .def_kind(DefKind::Method)
                .metadata(
                    metadata().return_type(field("result")).receiver_type(
                        field("receiver").then(
                            child_of_kind("parameter_declaration")
                                .then(field("type").inner("pointer_type", "type_identifier")),
                        ),
                    ),
                ),
            // Interface method specs — nested inside the interface scope,
            // so they get FQNs like Repository.Get.
            scope("method_elem", "Method")
                .def_kind(DefKind::Method)
                .no_scope()
                .metadata(metadata().return_type(field("result"))),
            // Unconditional fallback first — reverse iteration means conditional
            // rules (Struct, Interface) are checked before the fallback.
            scope("type_spec", "Type").def_kind(DefKind::Other),
            scope("type_spec", "Struct")
                .def_kind(DefKind::Class)
                .when(field_kind("type", &["struct_type"]))
                .metadata(metadata().super_types(|n: &Node<'_, SyntaxTree>| {
                    n.children()
                        .filter(|c| c.kind().as_ref() == "__supertype")
                        .map(|c| c.text().to_string())
                        .collect()
                })),
            scope("type_spec", "Interface")
                .def_kind(DefKind::Interface)
                .when(field_kind("type", &["interface_type"])),
            scope("var_spec", "Var")
                .def_kind(DefKind::Property)
                .no_scope()
                .when(!ancestor_is(&[
                    "function_declaration",
                    "method_declaration",
                    "func_literal",
                ])),
            scope("const_spec", "Const")
                .def_kind(DefKind::Property)
                .no_scope()
                .when(!ancestor_is(&[
                    "function_declaration",
                    "method_declaration",
                    "func_literal",
                ])),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // Method call: svc.Log("hello") — name from selector_expression's field
            reference("call_expression")
                .name_from(field_chain(&["function", "field"]))
                .when(field_kind("function", &["selector_expression"]))
                .receiver_chain(&["function", "operand"]),
            // Simple call: Log("hello") — name from function field directly
            reference("call_expression").name_from(field("function")),
            // Bare type references: declarations, type assertions.
            // Skip inside composite_literal (already tracked via chain_config.constructor).
            reference("type_identifier")
                .name_from(text())
                .when(!parent_is("composite_literal")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        vec![
            import("__blank_import")
                .label("Import")
                .path_from(child_of_kind("__import_path"))
                .side_effect(),
            import("__wildcard_go_import")
                .label("Import")
                .path_from(child_of_kind("__import_path"))
                .always_wildcard(),
            import("__aliased_go_import")
                .label("Import")
                .path_from(child_of_kind("__import_path"))
                .alias_from(child_of_kind("__import_alias")),
            import("__go_import")
                .label("Import")
                .path_from(child_of_kind("__import_path")),
        ]
    }

    fn bindings() -> Vec<BindingRule> {
        vec![
            binding("short_var_declaration", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right")
                .typed(
                    vec![
                        field("right")
                            .then(child_of_kind("composite_literal"))
                            .then(field("type")),
                    ],
                    GO_PRIMITIVE_TYPES,
                ),
            binding("var_spec", BindingKind::Assignment)
                .name_from(&["name"])
                .value_from("value")
                .typed(vec![field("type")], &[]),
            binding("assignment_statement", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right")
                .typed(
                    vec![
                        field("right")
                            .then(child_of_kind("composite_literal"))
                            .then(field("type")),
                    ],
                    GO_PRIMITIVE_TYPES,
                ),
            binding("parameter_declaration", BindingKind::Assignment)
                .name_from(&["name"])
                .typed(
                    vec![field("type").inner("pointer_type", "type_identifier")],
                    GO_PRIMITIVE_TYPES,
                ),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_statement")
                .branches(&["block"])
                .condition("condition"),
            branch("expression_switch_statement")
                .branches(&["expression_case", "default_case"])
                .condition("value"),
            branch("type_switch_statement").branches(&["type_case", "default_case"]),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![loop_rule("for_statement").body("body")]
    }

    fn rewrite(tree: &mut SyntaxTree) {
        let mut rules = go_import_rules();
        rules.insert(
            0,
            rw::insert(
                "type_spec",
                field("type")
                    .child_of_kind("field_declaration_list")
                    .collect_field(
                        treesitter_visit::Match::KindWithoutField("field_declaration", "name"),
                        "type",
                    )
                    .strip_prefix("*"),
                "__supertype",
            )
            .when(field_kind("type", &["struct_type"])),
        );
        tree.apply_rewrites(&rules);
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        Some(("package_clause", child_of_kind("package_identifier")))
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier", "field_identifier", "type_identifier"],
            this_kinds: &[],
            super_kinds: &[],
            field_access: vec![FieldAccessEntry {
                kind: "selector_expression",
                object: field("operand"),
                member: field("field"),
            }],
            constructor: &[("composite_literal", "type")],
            qualified_type_kinds: &[],
        })
    }
}

const QUOTE: &[char] = &['"'];

fn go_import_rules() -> Vec<rw::Rule> {
    let path = || child_of_kind("interpreted_string_literal").trim_matches(QUOTE);
    vec![
        // Extract path and name from ALL import_specs BEFORE renaming
        rw::insert("import_spec", path(), "__import_path"),
        rw::insert("import_spec", path().split_last("/"), "__import_name"),
        // Alias: extract package_identifier text
        rw::insert(
            "import_spec",
            child_of_kind("package_identifier"),
            "__import_alias",
        ),
        // Classify by alias type
        rw::rename("import_spec", "__blank_import").when(has_child(&["blank_identifier"])),
        rw::rename("import_spec", "__wildcard_go_import").when(has_child(&["dot"])),
        rw::rename("import_spec", "__aliased_go_import").when(has_child(&["package_identifier"])),
    ]
}

// ── Resolution rules ────────────────────────────────────────────

pub struct GoRules;

impl HasRules for GoRules {
    fn rules() -> ResolutionRules {
        let spec = GoDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "go",
            scopes,
            spec,
            vec![ResolveStage::SSA, ResolveStage::ImportStrategies],
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::SameFile,
            ],
            ReceiverMode::None,
            ".",
            &[],
            None,
        )
        .with_hooks(ResolverHooks {
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            excluded_ambient_imported_symbol_names: &[
                "append", "cap", "clear", "close", "complex", "copy", "delete", "imag", "len",
                "make", "max", "min", "new", "panic", "print", "println", "real", "recover",
            ],
            ..Default::default()
        })
    }
}
