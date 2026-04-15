use code_graph_config::Language;
use code_graph_types::DefKind;
use parser_core::dsl::extractors::{field, Extract};
use parser_core::dsl::types::*;

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct CSharpDsl;

impl DslLanguage for CSharpDsl {
    fn name() -> &'static str {
        "csharp"
    }

    fn language() -> Language {
        Language::CSharp
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("namespace_declaration", "Namespace").def_kind(DefKind::Other),
            scopes(
                &[
                    "class_declaration",
                    "struct_declaration",
                    "enum_declaration",
                    "record_declaration",
                ],
                "Class",
            )
            .def_kind(DefKind::Class),
            scope("interface_declaration", "Interface").def_kind(DefKind::Interface),
            scope("method_declaration", "Method").def_kind(DefKind::Method),
            scope("constructor_declaration", "Constructor").def_kind(DefKind::Constructor),
            scope("property_declaration", "Property")
                .def_kind(DefKind::Property)
                .no_scope(),
            scope("enum_member_declaration", "EnumMember")
                .def_kind(DefKind::EnumEntry)
                .no_scope(),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("invocation_expression").name_from(field("function")),
            reference("object_creation_expression").name_from(field("type")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        vec![import("using_directive")]
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        Some(("namespace_declaration", Extract::Default))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_types::CanonicalParser;

    fn parse(code: &str) -> code_graph_types::CanonicalResult {
        DslParser::<CSharpDsl>::default()
            .parse_file(code.as_bytes(), "Test.cs")
            .unwrap()
            .0
    }

    #[test]
    fn class_with_methods() {
        let result = parse(
            "namespace MyApp {\n    public class Controller {\n        public void Index() {}\n    }\n}\n",
        );
        assert!(result.definitions.len() >= 2);
        assert!(result.definitions.iter().any(|d| d.name == "Controller"));
    }

    #[test]
    fn language() {
        let result = parse("class X {}");
        assert_eq!(result.language, Language::CSharp);
    }
}
