use code_graph_config::Language;
use code_graph_types::{BindingKind, DefKind};
use parser_core::dsl::extractors::{Extract, ExtractList, field, metadata};
use parser_core::dsl::types::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::linker::v2::rules::{
    ChainMode, ImportStrategy, ReceiverMode, ResolutionRules, ResolveStage,
};
use crate::linker::v2::{HasRules, ResolveSettings};

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct JavaDsl;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

fn java_super_types(node: &N<'_>) -> Vec<String> {
    let mut result = Vec::new();
    let type_kinds = ["type_identifier", "generic_type", "scoped_type_identifier"];

    if let Some(superclass) = node.field("superclass") {
        let text = superclass.text().to_string();
        let name = text.strip_prefix("extends ").unwrap_or(&text).trim();
        if !name.is_empty() {
            result.push(name.to_string());
        }
    }
    if let Some(interfaces) = node.field("interfaces") {
        for child in interfaces.children() {
            if type_kinds.iter().any(|&k| k == child.kind().as_ref()) {
                result.push(child.text().to_string());
            }
        }
    }
    for child in node.children() {
        if child.kind() == "extends_interfaces" {
            for inner in child.children() {
                if type_kinds.iter().any(|&k| k == inner.kind().as_ref()) {
                    result.push(inner.text().to_string());
                }
            }
        }
    }
    result
}

impl DslLanguage for JavaDsl {
    fn name() -> &'static str {
        "java"
    }

    fn language() -> Language {
        Language::Java
    }

    fn scopes() -> Vec<ScopeRule> {
        let class_meta = || metadata().super_types(ExtractList::Fn(java_super_types));

        vec![
            scopes(
                &[
                    "class_declaration",
                    "enum_declaration",
                    "record_declaration",
                ],
                "Class",
            )
            .def_kind(DefKind::Class)
            .metadata(class_meta()),
            scope("interface_declaration", "Interface")
                .def_kind(DefKind::Interface)
                .metadata(class_meta()),
            scope("annotation_type_declaration", "AnnotationDeclaration")
                .def_kind(DefKind::Interface),
            scope("enum_constant", "EnumConstant")
                .def_kind(DefKind::EnumEntry)
                .no_scope(),
            scope("constructor_declaration", "Constructor").def_kind(DefKind::Constructor),
            scope("method_declaration", "Method")
                .def_kind(DefKind::Method)
                .metadata(metadata().return_type(field("type"))),
            scope("lambda_expression", "Lambda")
                .def_kind(DefKind::Lambda)
                .no_scope()
                .name_from(field("parameters")),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("method_invocation")
                .name_from(field("name"))
                .receiver("object"),
            reference("object_creation_expression").name_from(field("type")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        fn java_import_classify(node: &N<'_>) -> &'static str {
            let text = node.text().to_string();
            let is_static = text.trim_start().starts_with("import static");
            let is_wildcard = node.children().any(|c| c.kind() == "asterisk");
            match (is_static, is_wildcard) {
                (true, _) => "StaticImport",
                (false, true) => "WildcardImport",
                (false, false) => "Import",
            }
        }

        vec![
            import("import_declaration")
                .classify(java_import_classify)
                .split_last(".")
                .wildcard_child("asterisk"),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier", "type_identifier"],
            this_kinds: &["this"],
            super_kinds: &["super"],
            field_access: &[("field_access", "object", "field")],
            constructor: &[("object_creation_expression", "type")],
        })
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        Some(("package_declaration", Extract::Default))
    }

    fn bindings() -> Vec<BindingRule> {
        let java_type = |rule: BindingRule| {
            rule.typed(
                &["type"],
                &[
                    "int", "long", "short", "byte", "float", "double", "boolean", "char", "void",
                    "String",
                ],
            )
        };
        vec![
            java_type(
                binding("local_variable_declaration", BindingKind::Assignment)
                    .name_from(&["declarator", "name"]),
            ),
            java_type(
                binding("field_declaration", BindingKind::Assignment)
                    .name_from(&["declarator", "name"])
                    .instance_attrs(&["this."]),
            ),
            java_type(
                binding("formal_parameter", BindingKind::Parameter)
                    .name_from(&["name"])
                    .no_value(),
            ),
            binding("catch_formal_parameter", BindingKind::Parameter)
                .name_from(&["name"])
                .no_value(),
            java_type(
                binding("resource", BindingKind::Assignment)
                    .name_from(&["name"])
                    .value_from("value"),
            ),
            binding("assignment_expression", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right"),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_statement")
                .branches(&["block", "else_clause"])
                .condition("condition")
                .catch_all("else_clause"),
            branch("try_statement").branches(&["block", "catch_clause", "finally_clause"]),
            branch("try_with_resources_statement").branches(&[
                "block",
                "catch_clause",
                "finally_clause",
            ]),
            branch("switch_expression").branches(&["switch_block_statement_group", "switch_rule"]),
            branch("switch_statement").branches(&["switch_block_statement_group"]),
            branch("ternary_expression")
                .branches(&["consequence", "alternative"])
                .catch_all("alternative"),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("for_statement"),
            loop_rule("while_statement"),
            loop_rule("enhanced_for_statement").iter_over("value"),
            loop_rule("do_statement"),
        ]
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct JavaRules;

impl HasRules for JavaRules {
    fn rules() -> ResolutionRules {
        let spec = JavaDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "java",
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
                ImportStrategy::WildcardImport,
                ImportStrategy::SamePackage,
                ImportStrategy::SameFile,
            ],
            ChainMode::TypeFlow {
                type_fields: &["type"],
                skip_types: &[
                    "int", "long", "short", "byte", "float", "double", "boolean", "char", "void",
                    "String",
                ],
            },
            ReceiverMode::Keyword,
            ".",
            &["this", "self"],
            Some("super"),
        )
        .with_settings(ResolveSettings {
            per_file_timeout: Some(std::time::Duration::from_millis(10000)),
            ..ResolveSettings::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_types::CanonicalParser;

    fn parse(code: &str) -> code_graph_types::CanonicalResult {
        DslParser::<JavaDsl>::default()
            .parse_file(code.as_bytes(), "Test.java")
            .unwrap()
            .0
    }

    #[test]
    fn class_with_methods() {
        let result = parse(
            "public class Calculator {\n    public int add(int a, int b) {\n        return a + b;\n    }\n}\n",
        );
        assert_eq!(result.definitions.len(), 2);
        assert_eq!(result.definitions[0].name, "Calculator");
        assert_eq!(result.definitions[0].kind, DefKind::Class);
        assert_eq!(result.definitions[1].name, "add");
        assert_eq!(result.definitions[1].fqn.to_string(), "Calculator.add");
    }

    #[test]
    fn package_scoping() {
        let result =
            parse("package com.example;\n\npublic class Service {\n    public void run() {}\n}\n");
        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "Service")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "com.example.Service");
    }

    #[test]
    fn super_types_extracted() {
        let result = parse("public class Dog extends Animal implements Serializable {\n}\n");
        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        let meta = dog.metadata.as_ref().expect("Dog should have metadata");
        assert!(!meta.super_types.is_empty());
    }

    #[test]
    fn imports_extracted() {
        let result = parse("import java.util.List;\nimport java.util.*;\n\npublic class Test {}\n");
        assert!(result.imports.len() >= 2);
    }
}
