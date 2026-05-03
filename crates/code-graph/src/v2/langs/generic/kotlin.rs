use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::DefKind;
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::Extract;
use treesitter_visit::extract::{child_of_kind, constant, default_name, field, no_extract, text};
use treesitter_visit::predicate::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::types::BindingKind;

use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolutionRules, ResolveStage,
    ResolverHooks,
};

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct KotlinDsl;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

fn extract_user_type(node: &N<'_>) -> Option<String> {
    // Look for user_type in direct children first, then in constructor_invocation
    if let Some(ut) = node.children().find(|c| c.kind() == "user_type") {
        return Some(ut.text().to_string());
    }
    if let Some(ci) = node
        .children()
        .find(|c| c.kind() == "constructor_invocation")
        && let Some(ut) = ci.children().find(|c| c.kind() == "user_type")
    {
        return Some(ut.text().to_string());
    }
    None
}

fn extract_delegation_specifier(spec: &N<'_>, result: &mut Vec<String>) {
    let sk = spec.kind();
    if sk == "delegation_specifier" || sk == "constructor_invocation" {
        let text = extract_user_type(spec).unwrap_or_else(|| spec.text().to_string());
        if !text.is_empty() && text != "," {
            result.push(text);
        }
    } else if sk == "user_type" {
        result.push(spec.text().to_string());
    }
}

fn kotlin_super_types(node: &N<'_>) -> Vec<String> {
    let mut result = Vec::new();
    for child in node.children() {
        let ck = child.kind();
        if ck == "delegation_specifiers" {
            for spec in child.children() {
                extract_delegation_specifier(&spec, &mut result);
            }
        } else if ck == "delegation_specifier"
            || ck == "constructor_invocation"
            || ck == "user_type"
        {
            extract_delegation_specifier(&child, &mut result);
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
                .metadata(metadata().super_types(kotlin_super_types)),
            scope("object_declaration", "Object")
                .def_kind(DefKind::Class)
                .name_from(child_of_kind("type_identifier")),
            // Companion objects: named (companion object Foo {}) uses type_identifier,
            // anonymous (companion object {}) defaults to "Companion".
            scope("companion_object", "Object")
                .def_kind(DefKind::Class)
                .name_from_or(child_of_kind("type_identifier"), "Companion"),
            // Unconditional fallback first — reverse iteration means
            // conditional rule (ExtensionFunction) is checked before fallback.
            scope("function_declaration", "Function").def_kind(DefKind::Function),
            scope("function_declaration", "ExtensionFunction")
                .def_kind(DefKind::Function)
                .when(has_child(&["."]))
                .metadata(metadata().receiver_type(child_of_kind("user_type"))),
            scope("secondary_constructor", "Constructor")
                .def_kind(DefKind::Constructor)
                .name_from(no_extract()),
            // Unconditional fallback first for property_declaration.
            scope("property_declaration", "Property")
                .def_kind(DefKind::Property)
                .no_scope(),
            scope("property_declaration", "ExtensionProperty")
                .def_kind(DefKind::Property)
                .no_scope()
                .when(has_child(&["."]))
                .name_from(
                    child_of_kind("variable_declaration").then(child_of_kind("simple_identifier")),
                )
                .metadata(
                    metadata()
                        .receiver_type(child_of_kind("user_type"))
                        .return_type(
                            text()
                                .next_sibling("getter")
                                .then(child_of_kind("function_body"))
                                .then(child_of_kind("call_expression"))
                                .then(default_name()),
                        ),
                ),
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
            // Operator desugaring: binary operators map to named methods.
            // The left operand is the receiver, the method name is derived from the operator.
            reference("additive_expression")
                .name_from(constant("plus"))
                .when(has_child_text("+"))
                .receiver_via(child_of_kind("simple_identifier")),
            reference("additive_expression")
                .name_from(constant("minus"))
                .when(has_child_text("-"))
                .receiver_via(child_of_kind("simple_identifier")),
            reference("multiplicative_expression")
                .name_from(constant("times"))
                .when(has_child_text("*"))
                .receiver_via(child_of_kind("simple_identifier")),
            reference("multiplicative_expression")
                .name_from(constant("div"))
                .when(has_child_text("/"))
                .receiver_via(child_of_kind("simple_identifier")),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["simple_identifier"],
            this_kinds: &["this_expression"],
            super_kinds: &["super_expression"],
            field_access: vec![FieldAccessEntry {
                kind: "navigation_expression",
                object: Extract::one(Child, Named),
                member: child_of_kind("navigation_suffix").then(default_name()),
            }],
            constructor: &[],
            qualified_type_kinds: &[],
        })
    }

    fn imports() -> Vec<ImportRule> {
        fn kotlin_import_classify(node: &N<'_>) -> &'static str {
            if node.children().any(|c| {
                let k = c.kind();
                k == "MULT" || k == "wildcard_import" || c.text() == "*"
            }) {
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
                .split_last(".")
                .alias_from(
                    Extract::one(Child, Kind("import_alias")).child_of_kind("type_identifier"),
                )
                .wildcard_child("wildcard_import"),
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
                    // variable_declaration > user_type — extract full text so
                    // dotted types like Parent.GrandChild are preserved. The
                    // engine's dotted type resolution splits on separator and
                    // resolves the first segment via imports.
                    child_of_kind("variable_declaration")
                        .then(child_of_kind("user_type"))
                        .then(text()),
                    // direct user_type child (for parameters)
                    field("user_type").inner("type_arguments", "type_identifier"),
                    field("type"),
                ],
                skip,
            )
        };
        vec![
            // val/var foo = Foo()
            // Name is in variable_declaration > simple_identifier
            // Value is the call_expression / navigation_expression / simple_identifier child
            kotlin_type(
                binding("property_declaration", BindingKind::Assignment)
                    .name_from_extract(
                        child_of_kind("variable_declaration")
                            .then(child_of_kind("simple_identifier")),
                    )
                    .value_from_extract(
                        text()
                            .nth(Child, Named, -1)
                            .try_descendant("call_expression"),
                    ),
            ),
            kotlin_type(
                binding("variable_declaration", BindingKind::Assignment)
                    .name_from(&["simple_identifier"])
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

    fn hooks() -> types::LanguageHooks {
        types::LanguageHooks {
            return_kinds: &["jump_expression"],
            expression_body_kinds: &["function_body"],
            ..Default::default()
        }
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
            ReceiverMode::Keyword,
            ".",
            &["this", "self"],
            Some("super"),
        )
        .with_implicit_sub_scopes(&["Companion"])
        .with_hooks(ResolverHooks {
            call_method: Some("invoke"),
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            excluded_ambient_imported_symbol_names: &["print", "println"],
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
        KotlinDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "Test.kt",
                crate::v2::config::Language::Kotlin,
                &Tracer::new(false),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| {
                crate::v2::pipeline::PipelineError::parse(
                    "Test.kt",
                    format!("Invalid UTF-8: {:?}", e),
                )
            })
    }

    #[test]
    fn class_with_methods() {
        let result =
            parse("class Calculator {\n    fun add(a: Int, b: Int): Int = a + b\n}\n").unwrap();
        assert_eq!(result.definitions.len(), 2);
        assert_eq!(result.definitions[0].name, "Calculator");
        assert_eq!(result.definitions[0].kind, DefKind::Class);
    }

    #[test]
    fn package_scoping() {
        let result =
            parse("package com.example\n\nclass Service {\n    fun run() {}\n}\n").unwrap();
        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "Service")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "com.example.Service");
    }

    #[test]
    fn super_types() {
        let result = parse("open class Animal\nclass Dog : Animal() {\n}\n").unwrap();
        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        if let Some(meta) = &dog.metadata {
            assert!(!meta.super_types.is_empty());
        }
    }
}
