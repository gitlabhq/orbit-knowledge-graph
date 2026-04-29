use crate::v2::config::Language;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, DefKind};
use treesitter_visit::extract::{Extract, field};
use treesitter_visit::predicate::*;

use crate::v2::linker::rules::{
    ImportStrategy, ReceiverMode, ResolutionRules, ResolveStage, ResolverHooks,
};
use crate::v2::linker::{HasRules, ResolveSettings};

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct CppDsl;

impl DslLanguage for CppDsl {
    fn name() -> &'static str {
        "cpp"
    }

    fn language() -> Language {
        Language::Cpp
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            return_kinds: &["return_statement"],
            ..LanguageHooks::default()
        }
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            // Free function: int add(int a, int b) { ... }
            scope("function_definition", "Function")
                .def_kind(DefKind::Function)
                .name_from(field("declarator").field("declarator")),
            // Class: class Foo { ... };
            scope("class_specifier", "Class")
                .def_kind(DefKind::Class)
                .when(has_descendant("field_declaration_list")),
            // Struct (C++ treats as class with public default)
            scope("struct_specifier", "Struct")
                .def_kind(DefKind::Class)
                .when(has_descendant("field_declaration_list")),
            // Enum: enum Color { ... };
            scope("enum_specifier", "Enum")
                .def_kind(DefKind::Class)
                .when(has_descendant("enumerator_list")),
            // Union
            scope("union_specifier", "Union")
                .def_kind(DefKind::Class)
                .when(has_descendant("field_declaration_list")),
            // Namespace: namespace foo { ... }
            scope("namespace_definition", "Namespace").def_kind(DefKind::Module),
            // typedef: typedef struct { ... } Name;
            scope("type_definition", "Typedef")
                .def_kind(DefKind::Other)
                .name_from(field("declarator"))
                .no_scope(),
            // Enum constants
            scope("enumerator", "EnumConstant")
                .def_kind(DefKind::EnumEntry)
                .no_scope(),
            // Template declarations wrap function/class definitions.
            // The template itself doesn't create a named scope; the
            // inner class/function scope handles naming.
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // Direct function call: foo()
            reference("call_expression")
                .name_from(field("function"))
                .when(!has_descendant("field_expression")),
            // Member call: obj.method() or obj->method()
            reference("call_expression")
                .name_from(field("function").field("field"))
                .when(has_descendant("field_expression"))
                .receiver_via(field("function").field("argument")),
            // Qualified call: Ns::func() or Class::static_method()
            reference("call_expression")
                .name_from(field("function").field("name"))
                .when(has_descendant("qualified_identifier")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        vec![
            // #include "header.h" / #include <header.h>
            import("preproc_include").path_from(field("path")),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier"],
            this_kinds: &["this"],
            super_kinds: &[],
            field_access: vec![FieldAccessEntry {
                kind: "field_expression",
                object: field("argument"),
                member: field("field"),
            }],
            constructor: &[],
            qualified_type_kinds: &["qualified_identifier"],
        })
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        None
    }

    fn file_scope() -> bool {
        true
    }

    fn bindings() -> Vec<BindingRule> {
        vec![
            // Variable declarations: int x = foo();
            binding("init_declarator", BindingKind::Assignment)
                .name_from(&["declarator"])
                .value_from("value"),
            // Parameters: void foo(int x, char *y)
            binding("parameter_declaration", BindingKind::Parameter)
                .name_from(&["declarator"])
                .no_value(),
            // Assignment: x = expr
            binding("assignment_expression", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right"),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_statement")
                .branches(&["consequence", "alternative"])
                .condition("condition")
                .catch_all("alternative"),
            branch("switch_statement").branches(&["body"]),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("for_statement"),
            loop_rule("for_range_loop"),
            loop_rule("while_statement"),
            loop_rule("do_statement"),
        ]
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig {
            self_names: &["this"],
            ..types::SsaConfig::default()
        }
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct CppRules;

impl HasRules for CppRules {
    fn rules() -> ResolutionRules {
        let spec = CppDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "cpp",
            scopes,
            spec,
            vec![
                ResolveStage::SSA,
                ResolveStage::ImportStrategies,
                ResolveStage::ImplicitMember,
            ],
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::SameFile,
                ImportStrategy::IncludeGraph,
            ],
            ReceiverMode::None,
            "::",
            &["this"],
            None,
        )
        .with_hooks(ResolverHooks::default())
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
        CppDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.cpp",
                Language::Cpp,
                &Tracer::new(false),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| crate::v2::pipeline::PipelineError::parse("test.cpp", format!("{e:?}")))
    }

    #[test]
    fn function_definitions() {
        let result = parse("int add(int a, int b) { return a + b; }\nvoid greet() {}\n").unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"add"), "should find add");
        assert!(names.contains(&"greet"), "should find greet");
    }

    #[test]
    fn class_definition() {
        let result = parse("class Point { public: int x; int y; };\n").unwrap();
        let point = result.definitions.iter().find(|d| d.name == "Point");
        assert!(point.is_some(), "should find class Point");
        assert_eq!(point.unwrap().kind, DefKind::Class);
    }

    #[test]
    fn struct_definition() {
        let result = parse("struct Vec3 { double x; double y; double z; };\n").unwrap();
        let v = result.definitions.iter().find(|d| d.name == "Vec3");
        assert!(v.is_some(), "should find struct Vec3");
        assert_eq!(v.unwrap().kind, DefKind::Class);
    }

    #[test]
    fn namespace_definition() {
        let result = parse("namespace math { int add(int a, int b) { return a + b; } }\n").unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"math"), "should find namespace math");
        assert!(names.contains(&"add"), "should find add inside namespace");
    }

    #[test]
    fn enum_definition() {
        let result = parse("enum Color { RED, GREEN, BLUE };\n").unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"Color"), "should find enum Color");
        assert!(names.contains(&"RED"), "should find RED");
        assert!(names.contains(&"GREEN"), "should find GREEN");
        assert!(names.contains(&"BLUE"), "should find BLUE");
    }

    #[test]
    fn include_imports() {
        let result = parse("#include <iostream>\n#include \"mylib.h\"\nvoid main() {}\n").unwrap();
        assert!(
            result.imports.len() >= 2,
            "should find at least 2 includes, got {}",
            result.imports.len()
        );
    }
}
