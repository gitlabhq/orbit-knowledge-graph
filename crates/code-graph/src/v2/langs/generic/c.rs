use crate::v2::config::Language;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, DefKind};
use treesitter_visit::extract::{Extract, field};
use treesitter_visit::predicate::*;

use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolutionRules, ResolveStage,
    ResolverHooks,
};
use crate::v2::linker::{HasRules, ResolveSettings};

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct CDsl;

impl DslLanguage for CDsl {
    fn name() -> &'static str {
        "c"
    }

    fn language() -> Language {
        Language::C
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            return_kinds: &["return_statement"],
            ..LanguageHooks::default()
        }
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("function_definition", "Function")
                .def_kind(DefKind::Function)
                .name_from(field("declarator").field("declarator")),
            scope("struct_specifier", "Struct")
                .def_kind(DefKind::Class)
                .when(has_descendant("field_declaration_list")),
            scope("enum_specifier", "Enum")
                .def_kind(DefKind::Class)
                .when(has_descendant("enumerator_list")),
            scope("union_specifier", "Union")
                .def_kind(DefKind::Class)
                .when(has_descendant("field_declaration_list")),
            // typedef struct { ... } Name;
            scope("type_definition", "Typedef")
                .def_kind(DefKind::Other)
                .name_from(field("declarator"))
                .no_scope(),
            // Enum constants
            scope("enumerator", "EnumConstant")
                .def_kind(DefKind::EnumEntry)
                .no_scope(),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // Direct function call: foo()
            reference("call_expression")
                .name_from(field("function"))
                .when(!has_descendant("field_expression")),
            // Member call via pointer/struct: obj->method() or obj.method()
            reference("call_expression")
                .name_from(field("function").field("field"))
                .when(has_descendant("field_expression"))
                .receiver_via(field("function").field("argument")),
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
            this_kinds: &[],
            super_kinds: &[],
            field_access: vec![FieldAccessEntry {
                kind: "field_expression",
                object: field("argument"),
                member: field("field"),
            }],
            constructor: &[],
            qualified_type_kinds: &[],
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
            loop_rule("while_statement"),
            loop_rule("do_statement"),
        ]
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig::default()
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct CRules;

impl HasRules for CRules {
    fn rules() -> ResolutionRules {
        let spec = CDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "c",
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
            &[],
            None,
        )
        .with_hooks(ResolverHooks {
            imported_symbol_fallback: ImportedSymbolFallbackPolicy {
                explicit_reaching_imports: false,
                ..Default::default()
            },
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
        CDsl::spec()
            .parse_full_collect(code.as_bytes(), "test.c", Language::C, &Tracer::new(false))
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| crate::v2::pipeline::PipelineError::parse("test.c", format!("{e:?}")))
    }

    #[test]
    fn function_definitions() {
        let result = parse("int add(int a, int b) { return a + b; }\nvoid greet() {}\n").unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"add"), "should find add");
        assert!(names.contains(&"greet"), "should find greet");
    }

    #[test]
    fn struct_definition() {
        let result = parse("struct Point { int x; int y; };\n").unwrap();
        let point = result.definitions.iter().find(|d| d.name == "Point");
        assert!(point.is_some(), "should find struct Point");
        assert_eq!(point.unwrap().kind, DefKind::Class);
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
    fn typedef_definition() {
        let result = parse("typedef struct { int x; int y; } Point;\n").unwrap();
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"Point"), "should find typedef Point");
    }

    #[test]
    fn include_imports() {
        let result = parse("#include <stdio.h>\n#include \"mylib.h\"\nvoid main() {}\n").unwrap();
        assert!(
            result.imports.len() >= 2,
            "should find at least 2 includes, got {}",
            result.imports.len()
        );
    }
}
