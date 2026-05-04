use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::{Extract, field, text};
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
pub struct CSharpDsl;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

fn csharp_super_types(node: &N<'_>) -> Vec<String> {
    let mut result = Vec::new();
    for child in node.children() {
        if child.kind() == "base_list" {
            for inner in child.children() {
                let ik = inner.kind();
                if ik == "identifier" || ik == "qualified_name" || ik == "generic_name" {
                    result.push(inner.text().to_string());
                }
            }
        }
    }
    result
}

impl DslLanguage for CSharpDsl {
    fn name() -> &'static str {
        "csharp"
    }

    fn language() -> Language {
        Language::CSharp
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            return_kinds: &["return_statement"],
            adopt_sibling_refs: &["attribute_list"],
            on_import: Some(csharp_extract_alias_using),
            ..LanguageHooks::default()
        }
    }

    fn scopes() -> Vec<ScopeRule> {
        let class_meta = || metadata().super_types(csharp_super_types);

        vec![
            scope("namespace_declaration", "Namespace").def_kind(DefKind::Other),
            scope("class_declaration", "Class")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("struct_declaration", "Struct")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("record_declaration", "Record")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("enum_declaration", "Enum").def_kind(DefKind::Class),
            scope("interface_declaration", "Interface")
                .def_kind(DefKind::Interface)
                .metadata(class_meta()),
            scope("method_declaration", "Method")
                .def_kind(DefKind::Method)
                .metadata(metadata().return_type(field("returns"))),
            scope("constructor_declaration", "Constructor").def_kind(DefKind::Constructor),
            scope("property_declaration", "Property")
                .def_kind(DefKind::Property)
                .no_scope()
                .metadata(metadata().type_annotation(field("type"))),
            scope("field_declaration", "Field")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(field("declaration").field("name"))
                .metadata(metadata().type_annotation(field("declaration").field("type"))),
            scope("event_field_declaration", "Event")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(field("declaration").field("name")),
            scope("enum_member_declaration", "EnumMember")
                .def_kind(DefKind::EnumEntry)
                .no_scope(),
            scope("lambda_expression", "Lambda")
                .def_kind(DefKind::Lambda)
                .no_scope()
                .name_from(field("parameters")),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // Chain method call: obj.Method() — function is a member_access_expression
            reference("invocation_expression")
                .name_from(field("function").field("name"))
                .when(has_descendant("member_access_expression"))
                .receiver_via(field("function").field("expression")),
            // Simple method call: Method() — function is an identifier
            reference("invocation_expression")
                .name_from(field("function"))
                .when(!has_descendant("member_access_expression")),
            // Constructor: new Foo()
            reference("object_creation_expression").name_from(field("type")),
            // Bare type references: type casts, is, as
            reference("type_identifier")
                .name_from(text())
                .when(!parent_is("object_creation_expression")),
            // is pattern: x is Foo
            reference("is_pattern_expression").name_from(field("pattern")),
            // Attribute references: [Serializable]
            reference("attribute").name_from(field("name")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        fn csharp_import_classify(node: &N<'_>) -> &'static str {
            let text = node.text().to_string();
            if text.contains("static") {
                "StaticImport"
            } else if text.contains('=') {
                "AliasedImport"
            } else {
                // Regular using directives are namespace-level wildcards:
                // `using MyApp.Models;` makes all types in MyApp.Models available.
                "WildcardImport"
            }
        }

        vec![
            import("using_directive")
                .path_from(Extract::one(
                    Child,
                    AnyKind(&["qualified_name", "identifier"]),
                ))
                .alias_from(field("name"))
                .classify(csharp_import_classify)
                // C# using directives import all types from a namespace.
                // `using MyApp.Models;` ≈ Java's `import MyApp.Models.*;`
                .always_wildcard(),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier"],
            this_kinds: &[],  // 'this' is anonymous in C# tree-sitter
            super_kinds: &[], // 'base' is anonymous
            field_access: vec![FieldAccessEntry {
                kind: "member_access_expression",
                object: field("expression"),
                member: field("name"),
            }],
            constructor: &[("object_creation_expression", "type")],
            qualified_type_kinds: &["qualified_name"],
        })
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        // C# namespaces are scopes, not package declarations. The FQN
        // is built from scope nesting (namespace → class → method).
        // No separate package_node needed.
        None
    }

    fn bindings() -> Vec<BindingRule> {
        let skip = &[
            "int", "long", "short", "byte", "float", "double", "bool", "char", "void", "string",
            "object", "decimal", "var",
        ];
        let csharp_type = |rule: BindingRule| {
            rule.typed(
                vec![
                    // Qualified types (Outer.Inner): extract full text
                    Extract::one(Field("type"), Kind("qualified_name")),
                    // Generic types (List<T>): extract the base identifier
                    field("type").child_of_kind("identifier"),
                    // Simple types: identifier text
                    field("type"),
                ],
                skip,
            )
        };
        use treesitter_visit::extract::child_of_kind;
        vec![
            // variable_declaration: covers both local vars and fields.
            // C# variable_declarator has name as identifier child, and
            // the initializer (= expr) as equals_value_clause child.
            csharp_type(
                binding("variable_declaration", BindingKind::Assignment)
                    .name_from_extract(child_of_kind("variable_declarator").field("name"))
                    .value_from_extract(
                        // The initializer is the last named child of variable_declarator
                        // (after identifier and anonymous "=").
                        child_of_kind("variable_declarator").nth(Child, Named, -1),
                    )
                    .instance_attrs(&["this."]),
            ),
            // Property declarations
            csharp_type(
                binding("property_declaration", BindingKind::Assignment)
                    .name_from(&["name"])
                    .no_value(),
            ),
            // Method/constructor parameters
            csharp_type(
                binding("parameter", BindingKind::Parameter)
                    .name_from(&["name"])
                    .no_value(),
            ),
            // foreach variable
            binding("foreach_statement", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right"),
            // x = y
            binding("assignment_expression", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right"),
            // catch (Exception e)
            binding("catch_declaration", BindingKind::Parameter)
                .name_from(&["name"])
                .typed(vec![field("type")], skip)
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
            branch("switch_statement").branches(&["switch_section"]),
            branch("conditional_expression")
                .branches(&["consequence", "alternative"])
                .catch_all("alternative"),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("for_statement"),
            loop_rule("while_statement"),
            loop_rule("foreach_statement").iter_over("right"),
            loop_rule("do_statement"),
        ]
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig {
            self_names: &["this"],
            super_name: Some("base"),
            ..Default::default()
        }
    }
}

fn csharp_extract_alias_using(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "using_directive" {
        return false;
    }

    let Some(alias_node) = node.field("name") else {
        return false;
    };
    let alias = alias_node.text().to_string();
    let target = node
        .children_matching(AnyKind(&[
            "qualified_name",
            "identifier",
            "generic_name",
            "alias_qualified_name",
        ]))
        .find(|child| child.range().start > alias_node.range().end)
        .map(|child| child.text().to_string());

    let Some(path) = target else {
        return true;
    };

    imports.push(CanonicalImport {
        import_type: "AliasedImport",
        binding_kind: ImportBindingKind::Named,
        mode: ImportMode::Declarative,
        path,
        name: None,
        alias: Some(alias),
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard: false,
    });

    true
}

// ── Resolution rules ────────────────────────────────────────────

pub struct CSharpRules;

impl HasRules for CSharpRules {
    fn rules() -> ResolutionRules {
        let spec = CSharpDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "csharp",
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
                ImportStrategy::SameFile,
            ],
            ReceiverMode::Keyword,
            ".",
            &["this"],
            Some("base"),
        )
        .with_hooks(ResolverHooks {
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            excluded_ambient_imported_symbol_names: &["Read", "ReadLine", "Write", "WriteLine"],
            ..Default::default()
        })
        .with_settings(ResolveSettings::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    fn parse(
        code: &str,
    ) -> Result<crate::v2::dsl::engine::ParsedDefs, crate::v2::pipeline::PipelineError> {
        CSharpDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "Test.cs",
                crate::v2::config::Language::CSharp,
                &Tracer::new(false),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| {
                crate::v2::pipeline::PipelineError::parse(
                    "Test.cs",
                    format!("Parse error: {:?}", e),
                )
            })
    }

    #[test]
    fn class_with_methods() {
        let result = parse(
            "namespace MyApp {\n    public class Controller {\n        public void Index() {}\n        public string Get(int id) { return \"\"; }\n    }\n}\n",
        ).unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"MyApp"), "should have namespace");
        assert!(names.contains(&"Controller"), "should have class");
        assert!(names.contains(&"Index"), "should have method");
        assert!(names.contains(&"Get"), "should have method");
    }

    #[test]
    fn namespace_scoping() {
        let result = parse(
            "namespace Com.Example {\n    public class Service {\n        public void Run() {}\n    }\n}\n",
        ).unwrap();
        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "Service")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "Com.Example.Service");
    }

    #[test]
    fn struct_and_enum() {
        let result = parse(
            "public struct Point { public int X; public int Y; }\npublic enum Color { Red, Green, Blue }\n",
        ).unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"Point"));
        assert!(names.contains(&"Color"));
        assert!(names.contains(&"Red"));
    }

    #[test]
    fn interface_declaration() {
        let result = parse("public interface IService {\n    void Execute();\n}\n").unwrap();
        let iface = result
            .definitions
            .iter()
            .find(|d| d.name == "IService")
            .unwrap();
        assert_eq!(iface.kind, DefKind::Interface);
    }

    #[test]
    fn super_types_extracted() {
        let result = parse("public class Dog : Animal, IRunnable {\n}\n").unwrap();
        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        let meta = dog.metadata.as_ref().expect("Dog should have metadata");
        assert!(meta.super_types.contains(&"Animal".to_string()));
        assert!(meta.super_types.contains(&"IRunnable".to_string()));
    }

    #[test]
    fn imports_extracted() {
        let result =
            parse("using System;\nusing System.Collections.Generic;\n\npublic class Test {}\n")
                .unwrap();
        assert!(
            result.imports.len() >= 2,
            "Expected at least 2 imports, got {}",
            result.imports.len()
        );
        let paths: Vec<&str> = result.imports.iter().map(|i| i.path.as_str()).collect();
        assert!(
            paths.iter().any(|p| p.contains("System")),
            "should have System import"
        );
    }

    #[test]
    fn static_import_extracted() {
        let result = parse("using static System.Math;\n\npublic class Test {}\n").unwrap();
        // using static may parse differently — verify we get at least the basic form
        assert!(!result.imports.is_empty(), "should extract static import");
    }

    #[test]
    fn property_and_field() {
        let result = parse(
            "public class Foo {\n    public int Count { get; set; }\n    private string _name;\n}\n",
        ).unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"Count"), "should have property");
    }

    #[test]
    fn enum_members() {
        let result = parse("public enum Direction { North, South, East, West }\n").unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"North"));
        assert!(names.contains(&"West"));
    }

    #[test]
    fn record_declaration() {
        let result = parse("public record Person(string Name, int Age);\n").unwrap();
        let person = result
            .definitions
            .iter()
            .find(|d| d.name == "Person")
            .unwrap();
        assert_eq!(person.kind, DefKind::Class);
    }

    #[test]
    fn constructor_declaration() {
        let result = parse("public class Foo {\n    public Foo(int x) {}\n}\n").unwrap();
        let ctor = result
            .definitions
            .iter()
            .find(|d| d.kind == DefKind::Constructor);
        assert!(ctor.is_some(), "should find constructor");
    }
}
