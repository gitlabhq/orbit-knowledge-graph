use crate::v2::config::Language;
use crate::v2::dsl::extractors::{ExtractList, metadata};
use crate::v2::dsl::types::{self, *};
use crate::v2::types::DefKind;
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::Extract;
use treesitter_visit::extract::{child_of_kind, default_name, field, no_extract, text};
use treesitter_visit::predicate::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::types::BindingKind;

use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{
    ChainMode, ImportStrategy, ReceiverMode, ResolutionRules, ResolveStage, ResolverHooks,
};

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct KotlinDsl;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

fn kotlin_super_types(node: &N<'_>) -> Vec<String> {
    let mut result = Vec::new();
    for child in node.children() {
        if child.kind() == "delegation_specifiers" {
            for spec in child.children() {
                let sk = spec.kind();
                if sk == "delegation_specifier" || sk == "constructor_invocation" {
                    let text = spec
                        .children()
                        .find(|c| c.kind() == "user_type")
                        .map(|n| n.text().to_string())
                        .unwrap_or_else(|| spec.text().to_string());
                    if !text.is_empty() && text != "," {
                        result.push(text);
                    }
                } else if sk == "user_type" {
                    result.push(spec.text().to_string());
                }
            }
        }
    }
    result
}

fn classify_kotlin_class(node: &N<'_>) -> &'static str {
    if node.has(Child, Kind("enum_class_body")) {
        return "Enum";
    }
    if let Some(type_id) = node.find(Child, Kind("type_identifier")) {
        let prefix_len = type_id.range().start.saturating_sub(node.range().start);
        let prefix = &node.text()[..prefix_len];
        if prefix.contains("interface") {
            return "Interface";
        }
    }
    if let Some(modifiers) = node.find(Child, Kind("modifiers"))
        && let Some(class_mod) = modifiers.find(Child, Kind("class_modifier"))
    {
        match class_mod.text().as_ref() {
            "data" => return "DataClass",
            "value" => return "ValueClass",
            "annotation" => return "AnnotationClass",
            _ => {}
        }
    }
    "Class"
}

impl DslLanguage for KotlinDsl {
    fn name() -> &'static str {
        "kotlin"
    }

    fn language() -> Language {
        Language::Kotlin
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope_fn("class_declaration", classify_kotlin_class)
                .def_kind(DefKind::Class)
                .name_from(child_of_kind("type_identifier"))
                .metadata(metadata().super_types(ExtractList::Fn(kotlin_super_types))),
            scopes(&["object_declaration", "companion_object"], "Object")
                .def_kind(DefKind::Class)
                .name_from(child_of_kind("type_identifier")),
            // Extension function: has receiver type before the dot
            scope("function_declaration", "ExtensionFunction")
                .def_kind(DefKind::Function)
                .when(has_child(&["."]))
                .metadata(metadata().receiver_type(child_of_kind("user_type"))),
            scope("function_declaration", "Function").def_kind(DefKind::Function),
            scope("secondary_constructor", "Constructor")
                .def_kind(DefKind::Constructor)
                .name_from(no_extract()),
            scope("property_declaration", "Property")
                .def_kind(DefKind::Property)
                .no_scope(),
            scope("enum_entry", "EnumEntry")
                .def_kind(DefKind::EnumEntry)
                .no_scope(),
            scopes(&["lambda_literal", "anonymous_function"], "Lambda")
                .def_kind(DefKind::Lambda)
                .no_scope()
                .name_from(no_extract()),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // Simple call: create() — name from direct simple_identifier child
            reference("call_expression")
                .name_from(child_of_kind("simple_identifier"))
                .when(!has_child(&["navigation_expression"])),
            // Chain call: Foo.create() — name from navigation_suffix's identifier.
            // Receiver: navigation_expression → first named child (the object).
            reference("call_expression")
                .name_from(
                    child_of_kind("navigation_expression")
                        .then(child_of_kind("navigation_suffix").then(default_name())),
                )
                .when(has_child(&["navigation_expression"]))
                .receiver_via(child_of_kind("navigation_expression").first_named()),
            // Bare type references: declarations, type casts, is checks
            reference("type_identifier").name_from(text()),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["simple_identifier"],
            this_kinds: &["this_expression"],
            super_kinds: &["super_expression"],
            field_access: &[],
            constructor: &[],
        })
    }

    fn imports() -> Vec<ImportRule> {
        fn kotlin_import_classify(node: &N<'_>) -> &'static str {
            if node
                .children()
                .any(|c| c.kind() == "MULT" || c.text() == "*")
            {
                return "WildcardImport";
            }
            if node.has(Child, Kind("import_alias")) {
                return "AliasedImport";
            }
            "Import"
        }

        vec![
            import("import_header")
                .classify(kotlin_import_classify)
                .split_last("."),
        ]
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        Some(("package_header", default_name()))
    }

    fn bindings() -> Vec<BindingRule> {
        let skip = &[
            "Int", "Long", "Short", "Byte", "Float", "Double", "Boolean", "Char", "Unit",
            "Nothing", "String",
        ];
        let kotlin_type = |rule: BindingRule| {
            rule.typed(
                vec![
                    field("user_type").inner("type_arguments", "type_identifier"),
                    field("type"),
                ],
                skip,
            )
        };
        vec![
            kotlin_type(
                binding("property_declaration", BindingKind::Assignment)
                    .name_from(&["name"])
                    .value_from("value"),
            ),
            kotlin_type(
                binding("variable_declaration", BindingKind::Assignment)
                    .name_from(&["name"])
                    .no_value(),
            ),
            kotlin_type(
                binding("value_parameter", BindingKind::Parameter)
                    .name_from(&["simple_identifier"])
                    .no_value(),
            ),
            binding("assignment", BindingKind::Assignment)
                .name_from(&["directly_assignable_expression"])
                .value_from("expression"),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_expression")
                .branches(&["control_structure_body"])
                .condition("condition")
                .catch_all("control_structure_body"),
            branch("when_expression").branches(&["when_entry"]),
            branch("try_expression").branches(&["statements", "catch_block", "finally_block"]),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("for_statement").iter_over("expression"),
            loop_rule("while_statement"),
            loop_rule("do_while_statement"),
        ]
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig {
            self_names: &["this", "self"],
            super_name: Some("super"),
        }
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct KotlinRules;

impl HasRules for KotlinRules {
    fn rules() -> ResolutionRules {
        let spec = KotlinDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "kotlin",
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
                type_fields: &["user_type", "type"],
                skip_types: &[
                    "Int", "Long", "Short", "Byte", "Float", "Double", "Boolean", "Char", "Unit",
                    "Nothing", "String",
                ],
            },
            ReceiverMode::Keyword,
            ".",
            &["this", "self"],
            Some("super"),
        )
        .with_hooks(ResolverHooks {
            call_method: Some("invoke"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(code: &str) -> crate::v2::dsl::engine::ParsedDefs {
        KotlinDsl::spec()
            .parse_defs_only(
                code.as_bytes(),
                "Test.kt",
                crate::v2::config::Language::Kotlin,
            )
            .unwrap()
    }

    #[test]
    fn class_with_methods() {
        let result = parse("class Calculator {\n    fun add(a: Int, b: Int): Int = a + b\n}\n");
        assert_eq!(result.definitions.len(), 2);
        assert_eq!(result.definitions[0].name, "Calculator");
        assert_eq!(result.definitions[0].kind, DefKind::Class);
    }

    #[test]
    fn package_scoping() {
        let result = parse("package com.example\n\nclass Service {\n    fun run() {}\n}\n");
        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "Service")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "com.example.Service");
    }

    #[test]
    fn super_types() {
        let result = parse("open class Animal\nclass Dog : Animal() {\n}\n");
        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        if let Some(meta) = &dog.metadata {
            assert!(!meta.super_types.is_empty());
        }
    }
}
