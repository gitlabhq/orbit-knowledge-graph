use code_graph_config::Language;
use code_graph_types::DefKind;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::dsl::extractors::{Extract, ExtractList, field, metadata};
use crate::dsl::types::*;

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
    if node.children().any(|c| c.kind() == "enum_class_body") {
        return "Enum";
    }
    if let Some(type_id) = node.children().find(|c| c.kind() == "type_identifier") {
        let prefix_len = type_id.range().start.saturating_sub(node.range().start);
        let prefix = &node.text()[..prefix_len];
        if prefix.contains("interface") {
            return "Interface";
        }
    }
    if let Some(modifiers) = node.children().find(|c| c.kind() == "modifiers")
        && let Some(class_mod) = modifiers.children().find(|c| c.kind() == "class_modifier")
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
                .name_from(Extract::ChildOfKind("type_identifier"))
                .metadata(metadata().super_types(ExtractList::Fn(kotlin_super_types))),
            scope("object_declaration", "Object")
                .def_kind(DefKind::Class)
                .name_from(Extract::ChildOfKind("type_identifier")),
            scope("companion_object", "CompanionObject")
                .def_kind(DefKind::Class)
                .name_from(Extract::ChildOfKind("type_identifier")),
            scope("function_declaration", "Function").def_kind(DefKind::Function),
            scope("secondary_constructor", "Constructor")
                .def_kind(DefKind::Constructor)
                .name_from(Extract::None),
            scope("property_declaration", "Property")
                .def_kind(DefKind::Property)
                .no_scope(),
            scope("enum_entry", "EnumEntry")
                .def_kind(DefKind::EnumEntry)
                .no_scope(),
            scope("lambda_literal", "Lambda")
                .def_kind(DefKind::Lambda)
                .no_scope()
                .name_from(Extract::None),
            scope("anonymous_function", "Lambda")
                .def_kind(DefKind::Lambda)
                .no_scope()
                .name_from(Extract::None),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("call_expression")
                .receiver("navigation_expression"),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["simple_identifier"],
            this_kinds: &["this_expression"],
            super_kinds: &["super_expression"],
            field_access: &[("navigation_expression", "expression", "navigation_suffix")],
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
            if node.children().any(|c| c.kind() == "import_alias") {
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

    fn bindings() -> Vec<ParseBindingRule> {
        vec![
            // val x = getValue()
            parse_binding("property_declaration")
                .name_from(Extract::ChildOfKind("simple_identifier"))
                .value_from(Extract::Field("expression")),
            // x = newValue
            parse_binding("assignment")
                .name_from(field("directly_assignable_expression"))
                .value_from(Extract::Field("expression")),
        ]
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        Some(("package_header", Extract::Default))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_types::CanonicalParser;

    fn parse(code: &str) -> code_graph_types::CanonicalResult {
        DslParser::<KotlinDsl>::default()
            .parse_file(code.as_bytes(), "Test.kt")
            .unwrap()
            .0
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
        assert!(service.is_top_level);
    }

    #[test]
    fn imports() {
        let result = parse("import com.example.Foo\nimport com.example.*\n");
        assert!(
            result.imports.len() >= 2,
            "got {} imports",
            result.imports.len()
        );
    }

    #[test]
    fn call_references() {
        let result = parse("fun main() {\n    println(\"hello\")\n}\n");
        assert!(!result.references.is_empty());
    }

    #[test]
    fn super_types() {
        let result = parse("open class Animal\nclass Dog : Animal() {\n}\n");
        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        if let Some(meta) = &dog.metadata {
            assert!(
                !meta.super_types.is_empty(),
                "super_types: {:?}",
                meta.super_types
            );
        }
    }

    #[test]
    fn language() {
        let result = parse("class X");
        assert_eq!(result.language, Language::Kotlin);
    }
}
