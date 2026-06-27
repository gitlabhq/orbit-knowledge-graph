use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::DefKind;
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::Extract;
use treesitter_visit::extract::{child_of_kind, default_name, field, text};
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
pub struct ScalaDsl;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// Classify a `class_definition` node into the appropriate Scala kind.
/// tree-sitter-scala represents case classes, traits, objects all via
/// related but distinct node types — only plain `class_definition` needs
/// sub-classification (case class vs plain class).
fn classify_scala_class(node: &N<'_>) -> &'static str {
    if node.children().any(|c| c.text().as_ref() == "case") {
        return "CaseClass";
    }
    "Class"
}

/// Extract super-types from a Scala class/trait definition.
/// Scala uses `extends Foo with Bar with Baz` syntax.
fn scala_super_types(node: &N<'_>) -> Vec<String> {
    let mut result = Vec::new();
    for child in node.children() {
        let ck = child.kind();
        if ck == "extends_clause" || ck == "with_clause" {
            for spec in child.children() {
                let sk = spec.kind();
                if sk == "type_identifier" || sk == "generic_type" || sk == "stable_identifier" {
                    let text = spec.text().to_string();
                    if !text.is_empty() && text != "extends" && text != "with" {
                        result.push(text);
                    }
                }
            }
        }
    }
    result
}

impl DslLanguage for ScalaDsl {
    fn name() -> &'static str {
        "scala"
    }

    fn language() -> Language {
        Language::Scala
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope_fn("class_definition", classify_scala_class)
                .def_kind(DefKind::Class)
                .name_from(child_of_kind("identifier"))
                .metadata(metadata().super_types(scala_super_types)),
            scope("object_definition", "Object")
                .def_kind(DefKind::Class)
                .name_from(child_of_kind("identifier")),
            scope("package_object_definition", "Object")
                .def_kind(DefKind::Class)
                .name_from(child_of_kind("identifier")),
            scope("trait_definition", "Trait")
                .def_kind(DefKind::Class)
                .name_from(child_of_kind("identifier"))
                .metadata(metadata().super_types(scala_super_types)),
            scope("function_definition", "Function")
                .def_kind(DefKind::Function)
                .name_from(child_of_kind("identifier")),
            scope("function_declaration", "Function")
                .def_kind(DefKind::Function)
                .name_from(child_of_kind("identifier")),
            scope("val_definition", "Val")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(child_of_kind("identifier")),
            scope("val_declaration", "Val")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(child_of_kind("identifier")),
            scope("var_definition", "Var")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(child_of_kind("identifier")),
            scope("var_declaration", "Var")
                .def_kind(DefKind::Property)
                .no_scope()
                .name_from(child_of_kind("identifier")),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("call_expression")
                .name_from(child_of_kind("identifier"))
                .when(!has_child(&["."])),
            reference("field_expression").name_from(child_of_kind("identifier")),
            reference("type_identifier").name_from(text()),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier"],
            this_kinds: &["this"],
            super_kinds: &["super"],
            field_access: vec![FieldAccessEntry {
                kind: "field_expression",
                object: Extract::one(Child, Named),
                member: child_of_kind("identifier"),
            }],
            constructor: &[],
            qualified_type_kinds: &["stable_identifier"],
        })
    }

    fn imports() -> Vec<ImportRule> {
        fn scala_import_classify(node: &N<'_>) -> &'static str {
            for child in node.children() {
                let k = child.kind();
                if k == "namespace_wildcard" {
                    return "WildcardImport";
                }
                if k == "namespace_selectors" {
                    for inner in child.children() {
                        if inner.kind() == "arrow_renamed_identifier" {
                            return "AliasedImport";
                        }
                    }
                    return "GroupedImport";
                }
                if k == "arrow_renamed_identifier" {
                    return "AliasedImport";
                }
            }
            "Import"
        }

        vec![
            import("import_declaration")
                .classify(scala_import_classify)
                .split_last(".")
                .wildcard_child("namespace_wildcard"),
        ]
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        Some(("package_clause", default_name()))
    }

    fn bindings() -> Vec<BindingRule> {
        let skip = &[
            "Int", "Long", "Short", "Byte", "Float", "Double", "Boolean", "Char", "Unit",
            "Nothing", "String", "Any", "AnyRef", "AnyVal",
        ];
        let scala_type = |rule: BindingRule| {
            rule.typed(
                vec![field("type"), child_of_kind("type_identifier").then(text())],
                skip,
            )
        };
        vec![
            scala_type(
                binding("val_definition", BindingKind::Assignment)
                    .name_from_extract(child_of_kind("identifier"))
                    .value_from_extract(
                        text()
                            .nth(Child, Named, -1)
                            .try_descendant("call_expression"),
                    ),
            ),
            scala_type(
                binding("val_declaration", BindingKind::Assignment)
                    .name_from_extract(child_of_kind("identifier"))
                    .no_value(),
            ),
            scala_type(
                binding("var_definition", BindingKind::Assignment)
                    .name_from_extract(child_of_kind("identifier"))
                    .value_from_extract(
                        text()
                            .nth(Child, Named, -1)
                            .try_descendant("call_expression"),
                    ),
            ),
            scala_type(
                binding("var_declaration", BindingKind::Assignment)
                    .name_from_extract(child_of_kind("identifier"))
                    .no_value(),
            ),
            scala_type(
                binding("parameter", BindingKind::Parameter)
                    .name_from(&["identifier"])
                    .no_value(),
            ),
            scala_type(
                binding("class_parameter", BindingKind::Parameter)
                    .name_from(&["identifier"])
                    .no_value(),
            ),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_expression")
                .branches(&["block", "expression"])
                .condition("condition")
                .catch_all("block"),
            branch("match_expression").branches(&["case_clause"]),
            branch("try_expression").branches(&["block", "catch_clause", "finally_clause"]),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("for_expression"),
            loop_rule("while_expression"),
            loop_rule("do_expression"),
        ]
    }

    fn hooks() -> types::LanguageHooks {
        types::LanguageHooks {
            return_kinds: &["return_expression"],
            expression_body_kinds: &[],
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

pub struct ScalaRules;

impl HasRules for ScalaRules {
    fn rules() -> ResolutionRules {
        let spec = ScalaDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "scala",
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
            call_method: Some("apply"),
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            excluded_ambient_imported_symbol_names: &["println", "print"],
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
        ScalaDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "Test.scala",
                crate::v2::config::Language::Scala,
                &Tracer::new(false),
                Default::default(),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| {
                crate::v2::pipeline::PipelineError::parse(
                    "Test.scala",
                    format!("Invalid UTF-8: {:?}", e),
                )
            })
    }

    #[test]
    fn class_with_methods() {
        let result =
            parse("class Calculator {\n  def add(a: Int, b: Int): Int = a + b\n}\n").unwrap();
        assert!(!result.definitions.is_empty());
        assert_eq!(result.definitions[0].name, "Calculator");
        assert_eq!(result.definitions[0].kind, DefKind::Class);
    }

    #[test]
    fn package_scoping() {
        let result =
            parse("package com.example\n\nclass Service {\n  def run(): Unit = {}\n}\n").unwrap();
        let service = result
            .definitions
            .iter()
            .find(|d| d.name == "Service")
            .unwrap();
        assert_eq!(service.fqn.to_string(), "com.example.Service");
    }

    #[test]
    fn case_class() {
        let result = parse("case class User(id: Int, name: String)\n").unwrap();
        assert!(result.definitions.iter().any(|d| d.name == "User"));
    }

    #[test]
    fn object_and_trait() {
        let result = parse(
            "trait Repository[T] {\n  def find(id: Int): T\n}\nobject Repo extends Repository[String] {\n  def find(id: Int): String = id.toString\n}\n",
        )
        .unwrap();
        assert!(result.definitions.iter().any(|d| d.name == "Repository"));
        assert!(result.definitions.iter().any(|d| d.name == "Repo"));
    }

    #[test]
    fn case_class_with_modifiers() {
        let result =
            parse("final case class Money(cents: Long)\nsealed case class Result(value: Int)\n")
                .unwrap();
        assert!(
            result
                .definitions
                .iter()
                .any(|d| d.name == "Money" && d.definition_type == "CaseClass")
        );
        assert!(
            result
                .definitions
                .iter()
                .any(|d| d.name == "Result" && d.definition_type == "CaseClass")
        );
    }

    #[test]
    fn abstract_val_var_in_trait() {
        let result =
            parse("trait Shape {\n  val name: String\n  var counter: Int\n  def area: Double\n}\n")
                .unwrap();
        assert!(result.definitions.iter().any(|d| d.name == "name"));
        assert!(result.definitions.iter().any(|d| d.name == "counter"));
    }
}
