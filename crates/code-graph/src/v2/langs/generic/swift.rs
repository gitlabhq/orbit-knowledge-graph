use crate::v2::config::Language;
use crate::v2::dsl::extractors::{DisplayRules, Token, metadata};
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{
    BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode, Modifier, Visibility,
};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::Extract;
use treesitter_visit::extract::{child_of_kind, constant, default_name, field, text};
use treesitter_visit::predicate::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolutionRules, ResolveStage,
    ResolverHooks,
};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

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

fn swift_super_types(node: &N<'_>) -> Vec<String> {
    let mut result = Vec::new();
    let specifiers = node
        .children()
        .flat_map(|child| match child.kind().as_ref() {
            "type_inheritance_clause" => child.children().collect::<Vec<_>>(),
            "inheritance_specifier" => vec![child],
            _ => Vec::new(),
        });
    for spec in specifiers {
        match spec.kind().as_ref() {
            "type_identifier" => result.push(spec.text().to_string()),
            "inheritance_specifier" => {
                let target = spec.field("inherits_from").unwrap_or(spec);
                match target.kind().as_ref() {
                    "type_identifier" => result.push(target.text().to_string()),
                    _ => {
                        if let Some(t) = target
                            .children()
                            .find(|t| t.kind().as_ref() == "type_identifier")
                        {
                            result.push(t.text().to_string());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    result
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
    let binding_kind = if path.contains('.') {
        ImportBindingKind::Named
    } else {
        ImportBindingKind::Namespace
    };

    imports.push(CanonicalImport {
        import_type: "Import",
        binding_kind,
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

fn swift_return_type(node: &N<'_>) -> Option<String> {
    let mut after_arrow = false;
    for child in node.children() {
        if child.kind() == "->" {
            after_arrow = true;
        } else if after_arrow && child.is_named() {
            return Some(child.text().to_string());
        }
    }
    None
}

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
        // Rules for class_declaration are evaluated in reverse insertion order (last → first).
        // The unconditional fallback at index 0 catches actors and any future keyword variants.
        vec![
            scope("class_declaration", "Class")
                .def_kind(DefKind::Class)
                .name_from(child_of_kind("type_identifier"))
                .metadata(metadata().super_types(swift_super_types)),
            // Extensions index the name but do not push a scope — methods inside would receive
            // the wrong FQN prefix without a reattach hook (tracked in #765).
            scope("class_declaration", "Extension")
                .def_kind(DefKind::Class)
                .when(has_child_text("extension"))
                .name_from(child_of_kind("user_type").child_of_kind("type_identifier"))
                .scope_only(),
            scope("class_declaration", "Enum")
                .def_kind(DefKind::Enum)
                .when(has_child_text("enum"))
                .name_from(child_of_kind("type_identifier")),
            scope("class_declaration", "Struct")
                .def_kind(DefKind::Struct)
                .when(has_child_text("struct"))
                .name_from(child_of_kind("type_identifier"))
                .metadata(metadata().super_types(swift_super_types)),
            scope("class_declaration", "Class")
                .def_kind(DefKind::Class)
                .when(has_child_text("class"))
                .name_from(child_of_kind("type_identifier"))
                .metadata(metadata().super_types(swift_super_types)),
            scope("protocol_declaration", "Protocol")
                .def_kind(DefKind::Interface)
                .name_from(child_of_kind("type_identifier"))
                .metadata(metadata().super_types(swift_super_types)),
            scope("function_declaration", "Function")
                .def_kind(DefKind::Function)
                .name_from(child_of_kind("simple_identifier")),
            scope("function_declaration", "Method")
                .def_kind(DefKind::Method)
                .when(parent_is("class_body"))
                .name_from(child_of_kind("simple_identifier")),
            scope("property_declaration", "Variable")
                .def_kind(DefKind::Variable)
                .no_scope()
                .name_from(field("name").field("bound_identifier")),
            scope("property_declaration", "Property")
                .def_kind(DefKind::Property)
                .no_scope()
                .when(parent_is("class_body"))
                .name_from(field("name").field("bound_identifier")),
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

    fn display() -> DisplayRules {
        DisplayRules {
            annotations: &["attribute"],
            test_prefixes: &["test"],
            body: &["body", "value"],
            parameters: &[],
            parameter_items: &["parameter"],
            return_type: &[],
            declared_type: &["type_annotation"],
            doc_comments: &["comment", "multiline_comment"],
            doc_prefixes: &["///", "/**"],
            doc_markers: &["///", "/**", "*/", "*"],
            modifier_hosts: &["modifiers"],
            tokens: &[
                ("public", Token::Visibility(Visibility::Public)),
                ("open", Token::Visibility(Visibility::Public)),
                ("internal", Token::Visibility(Visibility::Internal)),
                ("fileprivate", Token::Visibility(Visibility::File)),
                ("private", Token::Visibility(Visibility::Private)),
                ("static", Token::Modifier(Modifier::Static)),
                ("final", Token::Modifier(Modifier::Final)),
                ("override", Token::Modifier(Modifier::Override)),
                ("async", Token::Modifier(Modifier::Async)),
            ],
            visibility: Some(|_, _| Some(Visibility::Internal)),
            return_type_with: Some(swift_return_type),
            ..DisplayRules::default()
        }
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
            reference("type_identifier").name_from(text()).type_use(&[
                ("type_arguments", crate::v2::types::TypeUsage::Generic),
                ("as_expression", crate::v2::types::TypeUsage::Cast),
                ("parameter", crate::v2::types::TypeUsage::Parameter),
                ("function_declaration", crate::v2::types::TypeUsage::Return),
                ("type_annotation", crate::v2::types::TypeUsage::Field),
            ]),
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
        assert_eq!(point.kind, DefKind::Struct);
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
