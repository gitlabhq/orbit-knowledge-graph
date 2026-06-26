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
        tree.apply_rewrites(&[rw::custom(rewrite_go), rw::custom(rewrite_go_imports)]);
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

fn rewrite_go(tree: &mut SyntaxTree) {
    let mut supertypes: Vec<(u32, String)> = Vec::new();

    for ts in tree.nodes_of_kind("type_spec").collect::<Vec<_>>() {
        if let Some(struct_type) = tree
            .field(ts, "type")
            .filter(|&t| tree.kind(t) == "struct_type")
        {
            for fdl in tree.children_of_kind(struct_type, "field_declaration_list") {
                for fd in tree
                    .children_of_kind(fdl, "field_declaration")
                    .collect::<Vec<_>>()
                {
                    if tree.field(fd, "name").is_some() {
                        continue;
                    }
                    if let Some(type_node) = tree.field(fd, "type") {
                        let s = tree.text(type_node);
                        let name = s.strip_prefix('*').unwrap_or(s);
                        if !name.is_empty() {
                            supertypes.push((ts, name.to_string()));
                        }
                    }
                }
            }
        }
    }

    for (ts, text) in supertypes {
        tree.insert_child(ts, "__supertype", &text);
    }
}

fn rewrite_go_imports(tree: &mut SyntaxTree) {
    struct GoImport {
        spec: u32,
        path: String,
        kind: &'static str,
        alias: Option<String>,
    }

    let mut imports: Vec<GoImport> = Vec::new();

    for decl in tree.nodes_of_kind("import_declaration").collect::<Vec<_>>() {
        let specs: Vec<_> = tree
            .children_of_kind(decl, "import_spec")
            .chain(
                tree.children_of_kind(decl, "import_spec_list")
                    .flat_map(|list| {
                        tree.children_of_kind(list, "import_spec")
                            .collect::<Vec<_>>()
                    }),
            )
            .collect();

        for spec in specs {
            let path_node = tree
                .children_of_kind(spec, "interpreted_string_literal")
                .next();
            let Some(pn) = path_node else { continue };
            let import_path = tree.text(pn).trim_matches('"').to_string();
            if import_path.is_empty() {
                continue;
            }

            let alias_kinds = ["package_identifier", "blank_identifier", "dot"];
            let alias_node = tree.children(spec).iter().copied().find(|&c| {
                let n = tree.n(c);
                alias_kinds.iter().any(|&k| tree.kind(c) == k)
                    && n.start_byte < tree.n(pn).start_byte
            });
            let alias_text = alias_node.map(|a| tree.text(a).to_string());

            let (kind, alias) = match alias_text.as_deref() {
                Some("_") => ("__blank_import", None),
                Some(".") => ("__wildcard_go_import", None),
                Some(a) => ("__aliased_go_import", Some(a.to_string())),
                None => ("__go_import", None),
            };

            imports.push(GoImport {
                spec,
                path: import_path,
                kind,
                alias,
            });
        }
    }

    for imp in imports {
        tree.set_kind(imp.spec, imp.kind);
        tree.insert_child(imp.spec, "__import_path", &imp.path);
        if let Some(pkg) = imp.path.rsplit('/').next() {
            tree.insert_child(imp.spec, "__import_name", pkg);
        }
        if let Some(alias) = &imp.alias {
            tree.insert_child(imp.spec, "__import_alias", alias);
        }
    }
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
