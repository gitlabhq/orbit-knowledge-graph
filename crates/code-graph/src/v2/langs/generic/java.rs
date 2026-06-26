use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, DefKind};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::Node;
use treesitter_visit::extract::{Extract, child_of_kind, default_name, field, text};
use treesitter_visit::predicate::*;
use treesitter_visit::syntax_tree as rw;
use treesitter_visit::syntax_tree::SyntaxTree;

use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolutionRules, ResolveStage,
    ResolverHooks,
};
use crate::v2::linker::{HasRules, ResolveSettings};

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct JavaDsl;

const JAVA_TYPE_KINDS: &[&str] = &["type_identifier", "generic_type", "scoped_type_identifier"];

/// `method_declaration` whose `parameters` field has no real parameters.
fn no_arg_method() -> Pred {
    is_kind("method_declaration").and(field_lacks_children(
        "parameters",
        &["formal_parameter", "spread_parameter"],
    ))
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

    fn rewrite(tree: &mut SyntaxTree) {
        let tk = treesitter_visit::Match::AnyKind(JAVA_TYPE_KINDS);
        tree.apply_rewrites(&[
            // Supertypes from superclass/interfaces/extends_interfaces
            rw::insert(
                "class_declaration",
                field("superclass").collect_shallow(tk),
                "__supertype",
            ),
            rw::insert(
                "class_declaration",
                field("interfaces").collect_shallow(tk),
                "__supertype",
            ),
            rw::insert(
                "class_declaration",
                child_of_kind("extends_interfaces").collect_shallow(tk),
                "__supertype",
            ),
            rw::insert(
                "enum_declaration",
                field("superclass").collect_shallow(tk),
                "__supertype",
            ),
            rw::insert(
                "enum_declaration",
                field("interfaces").collect_shallow(tk),
                "__supertype",
            ),
            rw::insert(
                "record_declaration",
                field("superclass").collect_shallow(tk),
                "__supertype",
            ),
            rw::insert(
                "record_declaration",
                field("interfaces").collect_shallow(tk),
                "__supertype",
            ),
            rw::insert(
                "interface_declaration",
                child_of_kind("extends_interfaces").collect_shallow(tk),
                "__supertype",
            ),
            rw::insert(
                "interface_declaration",
                field("interfaces").collect_shallow(tk),
                "__supertype",
            ),
            // Import classification
            rw::rename("import_declaration", "__static_import").when(has_child_text("static")),
            rw::rename("import_declaration", "__wildcard_import").when(has_child(&["asterisk"])),
            // Record accessors: param names minus no-arg method names
            rw::insert(
                "record_declaration",
                field("parameters").collect_field(Kind("formal_parameter"), "name"),
                "__accessor",
            )
            .except(field("body").each(text().where_pred(no_arg_method()).field("name"))),
        ]);
    }

    fn scopes() -> Vec<ScopeRule> {
        let collect_supertypes = |n: &Node<'_, SyntaxTree>| -> Vec<String> {
            n.children()
                .filter(|c| c.kind().as_ref() == "__supertype")
                .map(|c| c.text().to_string())
                .collect()
        };
        let class_meta = || metadata().super_types(collect_supertypes);

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
            scope("compact_constructor_declaration", "Constructor").def_kind(DefKind::Constructor),
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
            scope("__accessor", "Method")
                .def_kind(DefKind::Method)
                .no_scope(),
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
        vec![
            import("__static_import")
                .label("StaticImport")
                .split_last(".")
                .wildcard_child("asterisk"),
            import("__wildcard_import")
                .label("WildcardImport")
                .split_last(".")
                .wildcard_child("asterisk"),
            import("import_declaration")
                .label("Import")
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
                Default::default(),
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
    fn permits_clause_names_reach_the_reference_stream() {
        // JEP 409 sealed types: the `permits` clause is not extracted into
        // metadata and produces no semantic Extends edge (#847) — in valid
        // Java the child-side extends/implements clause already provides it.
        // The permitted type names are still picked up by the bare
        // `reference("type_identifier")` rule, attributed to the sealed
        // parent, so the parent->child relationship stays reachable even
        // when a child file is missing or malformed.
        let probe = |code: &str| {
            JavaDsl::spec()
                .parse_full_collect(
                    code.as_bytes(),
                    "Test.java",
                    crate::v2::config::Language::Java,
                    &Tracer::new(false),
                    Default::default(),
                )
                .unwrap()
        };

        // Sealed interface
        let r = probe(
            "public sealed interface Shape permits Circle, Rectangle, Triangle {\n    double area();\n}\n",
        );
        let shape_idx = r
            .definitions
            .iter()
            .position(|d| d.name == "Shape")
            .unwrap() as u32;
        for name in ["Circle", "Rectangle", "Triangle"] {
            let r#ref = r
                .refs
                .iter()
                .find(|x| x.name == name)
                .unwrap_or_else(|| panic!("{name} should appear as a reference"));
            assert_eq!(r#ref.enclosing_def, Some(shape_idx));
        }

        // Sealed class, with extends/implements alongside permits
        let r = probe(
            "public sealed class Animal extends Creature implements Living permits Dog, Cat {\n}\n",
        );
        for name in ["Dog", "Cat"] {
            assert!(
                r.refs.iter().any(|x| x.name == name),
                "{name} should appear as a reference"
            );
        }
    }

    #[test]
    fn record_compact_constructor() {
        let result = parse(
            "public record Bounds(int lo, int hi) {\n    public Bounds {\n        if (lo > hi) throw new IllegalArgumentException(\"lo > hi\");\n    }\n}\n",
        )
        .unwrap();
        let ctor = result
            .definitions
            .iter()
            .find(|d| d.kind == DefKind::Constructor)
            .expect("compact constructor should be extracted");
        assert_eq!(ctor.name, "Bounds");
        assert_eq!(ctor.fqn.to_string(), "Bounds.Bounds");
    }

    #[test]
    fn record_implicit_accessors() {
        let result = parse("public record Point(int x, int y) {}\n").unwrap();
        let accessors: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.kind == DefKind::Method)
            .collect();
        assert_eq!(accessors.len(), 2);
        assert_eq!(accessors[0].fqn.to_string(), "Point.x");
        assert_eq!(accessors[1].fqn.to_string(), "Point.y");
        assert_eq!(
            accessors[0].metadata.as_ref().unwrap().return_type,
            Some("int".to_string())
        );
    }

    #[test]
    fn record_parameterized_overload_does_not_suppress_accessor() {
        let result = parse(
            "public record Circle(int radius) {\n    public int radius(int base) {\n        return this.radius() + base;\n    }\n}\n",
        )
        .unwrap();
        let mut lines: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.name == "radius" && d.kind == DefKind::Method)
            .map(|d| d.range.start.line)
            .collect();
        lines.sort_unstable();
        // Line 0 is the synthetic accessor anchored to the component; line 1
        // is the explicit radius(int) overload, which must not suppress it.
        assert_eq!(lines, [0, 1]);
    }

    #[test]
    fn record_zero_arg_override_beside_overload_suppresses_accessor() {
        let result = parse(
            "public record Circle(int radius) {\n    public int radius() {\n        return 0;\n    }\n    public int radius(int base) {\n        return this.radius() + base;\n    }\n}\n",
        )
        .unwrap();
        let mut lines: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.name == "radius" && d.kind == DefKind::Method)
            .map(|d| d.range.start.line)
            .collect();
        lines.sort_unstable();
        assert_eq!(lines, [1, 4], "both defs must be the explicit declarations");
    }

    #[test]
    fn record_explicit_accessor_override_not_duplicated() {
        let result = parse(
            "public record Wrapped(int raw) {\n    public int raw() {\n        return raw < 0 ? 0 : raw;\n    }\n}\n",
        )
        .unwrap();
        let raws: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.name == "raw" && d.kind == DefKind::Method)
            .collect();
        assert_eq!(raws.len(), 1);
        assert_ne!(raws[0].range.start.line, 0, "must be the explicit override");
    }

    #[test]
    fn imports_extracted() {
        let result =
            parse("import java.util.List;\nimport java.util.*;\n\npublic class Test {}\n").unwrap();
        assert!(result.imports.len() >= 2);
    }
}
