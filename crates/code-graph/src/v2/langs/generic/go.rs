use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::*;
use crate::v2::types::{BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::extract::Extract;
use treesitter_visit::extract::{child_of_kind, field, field_chain, text};
use treesitter_visit::predicate::*;

use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolveStage, ResolverHooks,
};
use crate::v2::linker::{HasRules, ResolutionRules};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

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
                .metadata(metadata().super_types(go_embedded_types)),
            scope("type_spec", "Interface")
                .def_kind(DefKind::Interface)
                .when(field_kind("type", &["interface_type"])),
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
        vec![]
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

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_import: Some(go_extract_imports),
            ..LanguageHooks::default()
        }
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

/// Extract embedded (promoted) types from a Go struct's field_declaration_list.
/// Embedded fields have a `type` but no `name`, e.g. `type Foo struct { Bar }`.
fn go_embedded_types(node: &N<'_>) -> Vec<String> {
    let Some(struct_type) = node.field("type") else {
        return vec![];
    };
    let Some(fdl) = struct_type.child_of_kind("field_declaration_list") else {
        return vec![];
    };
    fdl.children_matching(Kind("field_declaration"))
        .filter(|fd| fd.field("name").is_none())
        .filter_map(|fd| {
            fd.field("type").map(|t| {
                let s = t.text().to_string();
                // Strip pointer prefix: `*Bar` → `Bar`
                s.strip_prefix('*')
                    .map(|stripped| stripped.to_string())
                    .unwrap_or(s)
            })
        })
        .collect()
}

fn go_extract_imports(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "import_declaration" {
        return false;
    }

    for child in node.children() {
        let kind = child.kind();
        match kind.as_ref() {
            "import_spec" => extract_single_import(&child, imports),
            "import_spec_list" => {
                for spec in child.children() {
                    if spec.kind().as_ref() == "import_spec" {
                        extract_single_import(&spec, imports);
                    }
                }
            }
            _ => {}
        }
    }
    true
}

fn extract_single_import(node: &N<'_>, imports: &mut Vec<CanonicalImport>) {
    let path_node = node.find(Child, Kind("interpreted_string_literal"));

    let Some(path_node) = path_node else {
        return;
    };

    let raw_path = path_node.text().to_string();
    let import_path = raw_path.trim_matches('"').to_string();

    let alias_node = node
        .children_matching(AnyKind(&["package_identifier", "blank_identifier", "dot"]))
        .find(|n| n.range().start < path_node.range().start);

    let alias = alias_node.map(|n| n.text().to_string());
    let pkg_name = alias
        .clone()
        .filter(|a| a != "_" && a != ".")
        .or_else(|| import_path.rsplit('/').next().map(|s| s.to_string()));

    let is_blank = alias.as_deref() == Some("_");
    let is_dot = alias.as_deref() == Some(".");
    let binding_kind = if is_blank {
        ImportBindingKind::SideEffect
    } else {
        ImportBindingKind::Named
    };

    imports.push(CanonicalImport {
        import_type: "Import",
        binding_kind,
        mode: ImportMode::Declarative,
        path: import_path,
        name: pkg_name,
        alias: alias.filter(|_| !is_blank && !is_dot),
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard: is_dot,
    });
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
            excluded_ambient_imported_symbol_names: &["print", "println"],
            ..Default::default()
        })
    }
}
