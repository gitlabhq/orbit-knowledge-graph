use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::Node;
use treesitter_visit::extract::Extract;
use treesitter_visit::extract::{child_of_kind, constant, default_name, text};
use treesitter_visit::predicate::*;
use treesitter_visit::syntax_tree::SyntaxTree;

use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolutionRules, ResolveStage,
    ResolverHooks,
};

type N<'a> = Node<'a, SyntaxTree>;

const SWIFT_IMPORT_KINDS: &[&str] = &[
    "struct",
    "class",
    "enum",
    "var",
    "let",
    "func",
    "typealias",
    "protocol",
    "actor",
];

fn rewrite_swift(tree: &mut SyntaxTree) {
    let mut supertypes: Vec<(u32, String)> = Vec::new();

    let class_kinds = ["class_declaration", "protocol_declaration"];
    for kind in class_kinds {
        for cls in tree.nodes_of_kind(kind).collect::<Vec<_>>() {
            for tic in tree
                .children_of_kind(cls, "type_inheritance_clause")
                .collect::<Vec<_>>()
            {
                for &inner in tree.children(tic) {
                    match tree.kind(inner) {
                        "type_identifier" => {
                            supertypes.push((cls, tree.text(inner).to_string()));
                        }
                        "inheritance_specifier" => {
                            for &t in tree.children(inner) {
                                if tree.kind(t) == "type_identifier" {
                                    supertypes.push((cls, tree.text(t).to_string()));
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    for (cls, text) in supertypes {
        tree.insert_child(cls, "__supertype", &text);
    }
}

fn swift_extract_imports(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "import_declaration" {
        return false;
    }

    let raw = node.text().to_string();
    let rest = raw.trim().strip_prefix("import").unwrap_or("").trim();

    // Strip optional import-kind qualifier (e.g. "import struct UIKit.UIColor")
    let path = SWIFT_IMPORT_KINDS
        .iter()
        .find_map(|kind| {
            let after = rest.strip_prefix(kind)?;
            if after.starts_with(|c: char| c.is_whitespace()) {
                Some(after.trim())
            } else {
                None
            }
        })
        .unwrap_or(rest)
        .to_string();

    if path.is_empty() {
        return false;
    }

    let name = path.rsplit('.').next().unwrap_or(&path).to_string();

    imports.push(CanonicalImport {
        import_type: "Import",
        binding_kind: ImportBindingKind::Named,
        mode: ImportMode::Declarative,
        path,
        name: Some(name),
        alias: None,
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard: false,
    });
    true
}

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct SwiftDsl;

impl DslLanguage for SwiftDsl {
    fn name() -> &'static str {
        "swift"
    }

    fn language() -> Language {
        Language::Swift
    }

    fn scopes() -> Vec<ScopeRule> {
        let st_meta = || {
            metadata().super_types(|n: &Node<'_, SyntaxTree>| {
                n.children()
                    .filter(|c| c.kind().as_ref() == "__supertype")
                    .map(|c| c.text().to_string())
                    .collect()
            })
        };
        vec![
            scope("class_declaration", "Class")
                .def_kind(DefKind::Class)
                .name_from(child_of_kind("type_identifier"))
                .metadata(st_meta()),
            scope("class_declaration", "Extension")
                .def_kind(DefKind::Other)
                .when(has_child_text("extension"))
                .name_from(child_of_kind("type_identifier"))
                .no_scope(),
            scope("class_declaration", "Enum")
                .def_kind(DefKind::Class)
                .when(has_child_text("enum"))
                .name_from(child_of_kind("type_identifier")),
            scope("class_declaration", "Struct")
                .def_kind(DefKind::Class)
                .when(has_child_text("struct"))
                .name_from(child_of_kind("type_identifier"))
                .metadata(st_meta()),
            scope("class_declaration", "Class")
                .def_kind(DefKind::Class)
                .when(has_child_text("class"))
                .name_from(child_of_kind("type_identifier"))
                .metadata(st_meta()),
            scope("protocol_declaration", "Interface")
                .def_kind(DefKind::Interface)
                .name_from(child_of_kind("type_identifier"))
                .metadata(st_meta()),
            scope("function_declaration", "Function")
                .def_kind(DefKind::Function)
                .name_from(child_of_kind("simple_identifier")),
            // Protocol method requirements have no body; no_scope keeps them as flat defs
            // nested under the protocol's FQN (e.g. Drawable.draw).
            scope("protocol_function_declaration", "Method")
                .def_kind(DefKind::Method)
                .no_scope()
                .name_from(child_of_kind("simple_identifier")),
            scope("enum_entry", "EnumEntry")
                .def_kind(DefKind::EnumEntry)
                .no_scope()
                .name_from(child_of_kind("simple_identifier")),
            scope("init_declaration", "Constructor")
                .def_kind(DefKind::Constructor)
                .name_from(constant("init")),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("call_expression")
                .name_from(child_of_kind("simple_identifier"))
                .when(!has_child(&["navigation_expression"])),
            reference("call_expression")
                .name_from(
                    child_of_kind("navigation_expression")
                        .then(child_of_kind("navigation_suffix").then(default_name())),
                )
                .when(has_child(&["navigation_expression"]))
                .receiver_via(child_of_kind("navigation_expression").first_named()),
            reference("type_identifier").name_from(text()),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["simple_identifier"],
            this_kinds: &[],
            super_kinds: &[],
            field_access: vec![FieldAccessEntry {
                kind: "navigation_expression",
                object: Extract::one(Child, Named),
                member: child_of_kind("navigation_suffix").then(default_name()),
            }],
            constructor: &[],
            qualified_type_kinds: &["type_identifier"],
        })
    }

    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn bindings() -> Vec<BindingRule> {
        vec![
            binding("parameter", BindingKind::Parameter)
                .name_from(&["simple_identifier"])
                .no_value(),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_statement")
                .branches(&["statements"])
                .condition("condition"),
            branch("guard_statement")
                .branches(&["statements"])
                .condition("condition"),
            branch("switch_statement").branches(&["switch_entry"]),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("for_statement").body("body"),
            loop_rule("while_statement"),
            loop_rule("repeat_while_statement"),
        ]
    }

    fn rewrite(tree: &mut SyntaxTree) {
        rewrite_swift(tree);
    }

    fn hooks() -> types::LanguageHooks {
        types::LanguageHooks {
            on_import: Some(swift_extract_imports),
            return_kinds: &["return_statement"],
            ..Default::default()
        }
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig {
            self_names: &["self"],
            super_name: Some("super"),
            ..Default::default()
        }
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct SwiftRules;

impl HasRules for SwiftRules {
    fn rules() -> ResolutionRules {
        let spec = SwiftDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "swift",
            scopes,
            spec,
            vec![
                ResolveStage::SSA,
                ResolveStage::ImportStrategies,
                ResolveStage::ImplicitMember,
            ],
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::SameFile,
            ],
            ReceiverMode::Keyword,
            ".",
            &["self"],
            Some("super"),
        )
        .with_hooks(ResolverHooks {
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            excluded_ambient_imported_symbol_names: &[
                "print",
                "debugPrint",
                "fatalError",
                "preconditionFailure",
                "assertionFailure",
            ],
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    fn parse(
        code: &str,
    ) -> Result<crate::v2::dsl::engine::ParsedDefs, crate::v2::pipeline::PipelineError> {
        SwiftDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "Test.swift",
                crate::v2::config::Language::Swift,
                &Tracer::new(false),
                Default::default(),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| {
                crate::v2::pipeline::PipelineError::parse(
                    "Test.swift",
                    format!("Invalid UTF-8: {:?}", e),
                )
            })
    }

    #[test]
    fn class_and_method() {
        let result = parse("class Animal {\n    func speak() {}\n}\n").unwrap();
        assert_eq!(result.definitions.len(), 2);
        assert_eq!(result.definitions[0].name, "Animal");
        assert_eq!(result.definitions[0].kind, DefKind::Class);
    }

    #[test]
    fn struct_definition() {
        let result = parse("struct Point {\n    var x: Double\n    var y: Double\n}\n").unwrap();
        let point = result
            .definitions
            .iter()
            .find(|d| d.name == "Point")
            .unwrap();
        assert_eq!(point.kind, DefKind::Class);
    }

    #[test]
    fn protocol_definition() {
        let result = parse("protocol Drawable {\n    func draw()\n}\n").unwrap();
        let drawable = result
            .definitions
            .iter()
            .find(|d| d.name == "Drawable")
            .unwrap();
        assert_eq!(drawable.kind, DefKind::Interface);
    }

    #[test]
    fn import_extraction() {
        let result = parse("import Foundation\nimport UIKit\n\nclass Foo {}\n").unwrap();
        assert_eq!(result.imports.len(), 2);
        assert!(result.imports.iter().any(|i| i.path == "Foundation"));
        assert!(result.imports.iter().any(|i| i.path == "UIKit"));
    }

    #[test]
    fn dotted_import_extraction() {
        let result = parse("import UIKit.UIViewController\nclass Foo {}\n").unwrap();
        assert_eq!(result.imports.len(), 1);
        assert_eq!(result.imports[0].path, "UIKit.UIViewController");
        assert_eq!(result.imports[0].name.as_deref(), Some("UIViewController"));
    }
}
