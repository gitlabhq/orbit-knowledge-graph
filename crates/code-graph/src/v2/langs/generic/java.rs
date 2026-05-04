use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, DefKind};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::{Extract, default_name, field, text};
use treesitter_visit::predicate::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolutionRules, ResolveStage,
    ResolverHooks,
};
use crate::v2::linker::{HasRules, ResolveSettings};

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
            let ck = child.kind();
            if type_kinds.iter().any(|&k| k == ck.as_ref()) {
                result.push(child.text().to_string());
            } else if ck.as_ref() == "type_list" {
                for inner in child.children() {
                    if type_kinds.iter().any(|&k| k == inner.kind().as_ref()) {
                        result.push(inner.text().to_string());
                    }
                }
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

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            return_kinds: &["return_statement"],
            adopt_sibling_refs: &["marker_annotation", "annotation"],
            ..LanguageHooks::default()
        }
    }

    fn scopes() -> Vec<ScopeRule> {
        let class_meta = || metadata().super_types(java_super_types);

        vec![
            scope("class_declaration", "Class")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("enum_declaration", "Enum")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("record_declaration", "Record")
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
            scope("field_declaration", "Field")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(field("declarator").field("name"))
                .metadata(
                    metadata()
                        .type_annotation(field("type").inner("type_arguments", "type_identifier")),
                ),
            scope("constructor_declaration", "Constructor").def_kind(DefKind::Constructor),
            scope("method_declaration", "Method")
                .def_kind(DefKind::Method)
                .metadata(
                    metadata()
                        .return_type(field("type").inner("type_arguments", "type_identifier")),
                ),
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
            // Simple constructor: new Foo() — extract type name directly
            reference("object_creation_expression")
                .name_from(field("type").inner("type_arguments", "type_identifier"))
                .when(!has_descendant("scoped_type_identifier")),
            // Qualified constructor: new Outer.Inner() — name is last segment,
            // receiver is first segment. Chain becomes [Ident("Outer"), Call("Inner")]
            // and the resolver looks up Inner as a nested member of Outer.
            reference("object_creation_expression")
                .name_from(field("type").nth(Child, Kind("type_identifier"), -1))
                .when(has_descendant("scoped_type_identifier"))
                .receiver_via(field("type").nth(Child, Kind("type_identifier"), 0)),
            // Bare type references: declarations, casts, instanceof, annotations.
            // Skip inside object_creation_expression (already tracked above).
            reference("type_identifier")
                .name_from(text())
                .when(!parent_is("object_creation_expression")),
            // Method references: Executor::executeFn
            reference("method_reference")
                .name_from(
                    Extract::terminal(treesitter_visit::extract::Emit::Text).nth(
                        Child,
                        Kind("identifier"),
                        -1,
                    ),
                )
                .receiver_via(
                    Extract::terminal(treesitter_visit::extract::Emit::Text).nth(
                        Child,
                        Kind("identifier"),
                        0,
                    ),
                ),
            // Annotation references: @Override, @Deprecated
            references(&["marker_annotation", "annotation"]).name_from(field("name")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        fn java_import_classify(node: &N<'_>) -> &'static str {
            let text = node.text().to_string();
            let is_static = text.trim_start().starts_with("import static");
            let is_wildcard = node.has(Child, Kind("asterisk"));
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
            field_access: vec![FieldAccessEntry {
                kind: "field_access",
                object: field("object"),
                member: field("field"),
            }],
            constructor: &[("object_creation_expression", "type")],
            qualified_type_kinds: &["scoped_type_identifier"],
        })
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        Some(("package_declaration", default_name()))
    }

    fn bindings() -> Vec<BindingRule> {
        let skip = &[
            "int", "long", "short", "byte", "float", "double", "boolean", "char", "void", "String",
            "var",
        ];
        let java_type = |rule: BindingRule| {
            rule.typed(
                vec![
                    // Dotted types (Outer.Inner): navigate to type field, match scoped_type_identifier,
                    // emit full text. The engine resolves first segment via imports, appends rest.
                    Extract::one(Field("type"), Kind("scoped_type_identifier")),
                    // Simple types (Foo): strip generics, extract type_identifier text
                    field("type").inner("type_arguments", "type_identifier"),
                ],
                skip,
            )
        };
        vec![
            java_type(
                binding("local_variable_declaration", BindingKind::Assignment)
                    .name_from(&["declarator", "name"])
                    .value_from_extract(field("declarator").field("value")),
            ),
            java_type(
                binding("field_declaration", BindingKind::Assignment)
                    .name_from(&["declarator", "name"])
                    .value_from_extract(field("declarator").field("value"))
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
            // instanceof pattern variable: `expr instanceof Bar bar`
            binding("instanceof_expression", BindingKind::Assignment)
                .name_from(&["name"])
                .typed(vec![field("right")], skip)
                .no_value(),
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

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig {
            self_names: &["this", "self"],
            super_name: Some("super"),
            ..Default::default()
        }
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
            ReceiverMode::Keyword,
            ".",
            &["this", "self"],
            Some("super"),
        )
        .with_hooks(ResolverHooks {
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            excluded_ambient_imported_symbol_names: &["print", "println"],
            ..Default::default()
        })
        .with_settings(ResolveSettings {
            per_file_timeout: Some(std::time::Duration::from_millis(10000)),
            ..ResolveSettings::default()
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
        JavaDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "Test.java",
                crate::v2::config::Language::Java,
                &Tracer::new(false),
            )
            .map_err(|e| {
                crate::v2::pipeline::PipelineError::parse(
                    "Test.java",
                    format!("Invalid UTF-8: {:?}", e),
                )
            })
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
    }

    #[test]
    fn class_with_methods() {
        let result = parse(
            "public class Calculator {\n    public int add(int a, int b) {\n        return a + b;\n    }\n}\n",
        ).unwrap();
        assert_eq!(result.definitions.len(), 2);
        assert_eq!(result.definitions[0].name, "Calculator");
        assert_eq!(result.definitions[0].kind, DefKind::Class);
        assert_eq!(result.definitions[1].name, "add");
        assert_eq!(result.definitions[1].fqn.to_string(), "Calculator.add");
    }

    #[test]
    fn package_scoping() {
        let result =
            parse("package com.example;\n\npublic class Service {\n    public void run() {}\n}\n")
                .unwrap();
        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "Service")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "com.example.Service");
    }

    #[test]
    fn super_types_extracted() {
        let result =
            parse("public class Dog extends Animal implements Serializable {\n}\n").unwrap();
        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        let meta = dog.metadata.as_ref().expect("Dog should have metadata");
        assert!(!meta.super_types.is_empty());
    }

    #[test]
    fn imports_extracted() {
        let result =
            parse("import java.util.List;\nimport java.util.*;\n\npublic class Test {}\n").unwrap();
        assert!(result.imports.len() >= 2);
    }
}
