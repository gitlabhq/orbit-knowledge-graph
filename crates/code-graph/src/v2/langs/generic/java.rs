use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::ssa::ArgType;
use crate::v2::types::{BindingKind, DefKind};
use rustc_hash::FxHashMap;
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

fn is_annotation(node: &N<'_>) -> bool {
    matches!(node.kind().as_ref(), "annotation" | "marker_annotation")
}

fn bracket_pairs(node: &N<'_>) -> usize {
    node.children().filter(|c| c.kind() == "[").count()
}

fn erased_type(node: &N<'_>) -> String {
    match node.kind().as_ref() {
        "array_type" => {
            let element = node.field("element").map(|e| erased_type(&e));
            let dims = node.field("dimensions").map_or(0, |d| bracket_pairs(&d));
            format!("{}{}", element.unwrap_or_default(), "[]".repeat(dims))
        }
        "generic_type" | "scoped_type_identifier" => node
            .children()
            .filter(|c| c.is_named() && !is_annotation(c) && c.kind() != "type_arguments")
            .map(|c| erased_type(&c))
            .collect::<Vec<_>>()
            .join("."),
        _ => node.text().to_string(),
    }
}

fn java_signature(node: &N<'_>) -> Option<String> {
    let parameters = node.field("parameters")?;
    if parameters.get_inner_node().has_error() {
        return None;
    }
    let types: Vec<String> = parameters
        .children()
        .filter_map(|param| match param.kind().as_ref() {
            "formal_parameter" => {
                let ty = erased_type(&param.field("type")?);
                let dims = param.field("dimensions").map_or(0, |d| bracket_pairs(&d));
                Some(format!("{ty}{}", "[]".repeat(dims)))
            }
            "spread_parameter" => {
                let ty = param
                    .children()
                    .take_while(|c| c.kind() != "...")
                    .filter(|c| c.is_named() && c.kind() != "modifiers" && !is_annotation(c))
                    .last()?;
                Some(format!("{}...", erased_type(&ty)))
            }
            _ => None,
        })
        .collect();
    Some(format!("({})", types.join(", ")))
}

fn java_literal_type(node: &N<'_>) -> Option<&'static str> {
    let text = node.text();
    let has_suffix = |suffixes: [char; 2]| text.ends_with(suffixes);
    Some(match node.kind().as_ref() {
        "decimal_integer_literal"
        | "hex_integer_literal"
        | "octal_integer_literal"
        | "binary_integer_literal" => {
            if has_suffix(['l', 'L']) {
                "long"
            } else {
                "int"
            }
        }
        "decimal_floating_point_literal" | "hex_floating_point_literal" => {
            if has_suffix(['f', 'F']) {
                "float"
            } else {
                "double"
            }
        }
        "true" | "false" => "boolean",
        "character_literal" => "char",
        "string_literal" => "String",
        "null_literal" => "null",
        _ => return None,
    })
}

fn named(type_name: impl Into<smol_str::SmolStr>) -> ArgType {
    ArgType::Named(type_name.into())
}

fn written_type(type_owner: &N<'_>, dimensions_owner: &N<'_>) -> Option<String> {
    let base = erased_type(&type_owner.field("type")?);
    let dimensions = dimensions_owner
        .field("dimensions")
        .map_or(0, |d| bracket_pairs(&d));
    Some(format!("{base}{}", "[]".repeat(dimensions)))
}

fn declarator_named(declaration: &N<'_>, name: &str) -> Option<String> {
    let declarator = declaration.children().find(|d| {
        d.kind() == "variable_declarator" && d.field("name").is_some_and(|n| n.text() == name)
    })?;
    written_type(declaration, &declarator)
}

fn field_types(body: &N<'_>) -> FxHashMap<String, Option<String>> {
    let mut fields = FxHashMap::default();
    for declaration in body.children().filter(|c| c.kind() == "field_declaration") {
        for declarator in declaration
            .children()
            .filter(|d| d.kind() == "variable_declarator")
        {
            if let Some(name) = declarator.field("name") {
                fields.insert(
                    name.text().to_string(),
                    written_type(&declaration, &declarator),
                );
            }
        }
    }
    fields
}

fn pattern_variables(member: &N<'_>) -> FxHashMap<String, Option<String>> {
    let mut names = FxHashMap::default();
    for node in member.dfs() {
        let binding = match node.kind().as_ref() {
            "instanceof_expression" => node.field("name"),
            "type_pattern" | "record_pattern_component" => {
                node.children().filter(|c| c.kind() == "identifier").last()
            }
            _ => None,
        };
        if let Some(binding) = binding {
            names.insert(binding.text().to_string(), None);
        }
    }
    names
}

fn parameter_name(param: &N<'_>) -> Option<String> {
    let named = match param.field("name") {
        Some(name) => name,
        None => param
            .children()
            .find(|c| c.kind() == "variable_declarator")?
            .field("name")?,
    };
    Some(named.text().to_string())
}

fn parameter_type(param: &N<'_>) -> Option<String> {
    if param.kind() != "spread_parameter" {
        return written_type(param, param);
    }
    let element = param
        .children()
        .take_while(|c| c.kind() != "...")
        .filter(|c| c.is_named() && c.kind() != "modifiers" && !is_annotation(c))
        .last()?;
    Some(format!("{}[]", erased_type(&element)))
}

fn declared_type(
    usage: &N<'_>,
    name: &str,
    fields_only: bool,
    index: &mut DeclarationIndex,
) -> Option<String> {
    let usage_start = usage.range().start;
    let mut child = usage.clone();
    while let Some(parent) = child.parent() {
        let kind = parent.kind();
        if matches!(
            kind.as_ref(),
            "class_body" | "enum_body_declarations" | "interface_body"
        ) {
            return index
                .entry(parent.node_id())
                .or_insert_with(|| field_types(&parent))
                .get(name)
                .cloned()
                .flatten();
        }
        if !fields_only {
            match kind.as_ref() {
                "lambda_expression"
                    if parent.field("parameters").is_some_and(|p| {
                        p.text()
                            .split(|c: char| !c.is_alphanumeric() && c != '_')
                            .any(|t| t == name)
                    }) =>
                {
                    return None;
                }
                "enhanced_for_statement"
                    if parent.field("name").is_some_and(|n| n.text() == name) =>
                {
                    return written_type(&parent, &parent);
                }
                "try_with_resources_statement" => {
                    let resource = parent
                        .children()
                        .filter(|c| c.kind() == "resource_specification")
                        .flat_map(|spec| spec.children().collect::<Vec<_>>())
                        .find(|r| {
                            r.kind() == "resource"
                                && r.field("name").is_some_and(|n| n.text() == name)
                        });
                    if let Some(resource) = resource {
                        return written_type(&resource, &resource);
                    }
                }
                "catch_clause" => {
                    let formal = parent.children().find(|c| {
                        c.kind() == "catch_formal_parameter"
                            && c.field("name").is_some_and(|n| n.text() == name)
                    });
                    if let Some(formal) = formal {
                        let mut caught = formal.children().filter(|c| c.kind() == "catch_type");
                        let single = caught.next().filter(|_| caught.next().is_none())?;
                        let mut alternatives = single.children().filter(|c| c.is_named());
                        let only = alternatives
                            .next()
                            .filter(|_| alternatives.next().is_none())?;
                        return Some(erased_type(&only));
                    }
                }
                "method_declaration" | "constructor_declaration" | "static_initializer" => {
                    let parameter = parent
                        .field("parameters")
                        .into_iter()
                        .flat_map(|p| p.children().collect::<Vec<_>>())
                        .find(|p| parameter_name(p).is_some_and(|n| n == name));
                    if let Some(parameter) = parameter {
                        return parameter_type(&parameter);
                    }
                    let is_pattern_variable = index
                        .entry(parent.node_id())
                        .or_insert_with(|| pattern_variables(&parent))
                        .contains_key(name);
                    if is_pattern_variable {
                        return None;
                    }
                }
                "block" | "constructor_body" | "switch_block_statement_group" | "for_statement" => {
                    let local = parent
                        .children()
                        .take_while(|s| s.range().start < usage_start)
                        .filter(|s| s.kind() == "local_variable_declaration")
                        .find_map(|s| declarator_named(&s, name));
                    if local.is_some() {
                        return local;
                    }
                }
                _ => {}
            }
        }
        child = parent;
    }
    None
}

fn java_call_arg(node: &N<'_>, index: &mut DeclarationIndex) -> ArgType {
    if let Some(literal) = java_literal_type(node) {
        return named(literal);
    }
    match node.kind().as_ref() {
        "identifier" => declared_type(node, &node.text(), false, index)
            .filter(|t| t != "var")
            .map_or(ArgType::Unknown, named),
        "parenthesized_expression" => node
            .children()
            .find(|c| c.is_named())
            .map_or(ArgType::Unknown, |inner| java_call_arg(&inner, index)),
        "unary_expression" => match node.field("operand") {
            Some(operand) if operand.kind().ends_with("_literal") => java_call_arg(&operand, index),
            _ => ArgType::Unknown,
        },
        "cast_expression" | "object_creation_expression" => node
            .field("type")
            .map_or(ArgType::Unknown, |ty| named(erased_type(&ty))),
        "field_access" => match (node.field("object"), node.field("field")) {
            (Some(object), Some(field)) if object.kind() == "this" => {
                declared_type(node, &field.text(), true, index).map_or(ArgType::Unknown, named)
            }
            _ => ArgType::Unknown,
        },
        _ => ArgType::Unknown,
    }
}

fn java_call_args(node: &N<'_>, index: &mut DeclarationIndex) -> Option<Vec<ArgType>> {
    if node.kind() != "method_invocation" {
        return None;
    }
    let arguments = node.field("arguments")?;
    Some(
        arguments
            .children()
            .filter(|c| c.is_named() && !c.kind().contains("comment"))
            .map(|arg| java_call_arg(&arg, index))
            .collect(),
    )
}

fn java_record_accessors(
    node: &N<'_>,
    defs: &mut Vec<crate::v2::types::CanonicalDefinition>,
    scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> bool {
    if node.kind() != "record_declaration" {
        return false;
    }
    let Some(params) = node.field("parameters") else {
        return false;
    };
    let no_arg_body_methods: Vec<String> = node
        .field("body")
        .map(|body| {
            body.children()
                .filter(|c| c.kind() == "method_declaration")
                .filter(|m| {
                    m.field("parameters").is_none_or(|p| {
                        !p.children().any(|c| {
                            matches!(c.kind().as_ref(), "formal_parameter" | "spread_parameter")
                        })
                    })
                })
                .filter_map(|m| m.field("name").map(|n| n.text().to_string()))
                .collect()
        })
        .unwrap_or_default();
    for param in params.children() {
        if param.kind() != "formal_parameter" {
            continue;
        }
        let Some(name) = param.field("name").map(|n| n.text().to_string()) else {
            continue;
        };
        if no_arg_body_methods.contains(&name) {
            continue;
        }
        let return_type = field("type")
            .inner("type_arguments", "type_identifier")
            .apply(&param);
        let fqn = crate::v2::types::Fqn::from_scope(scope_stack, &name, sep);
        defs.push(crate::v2::types::CanonicalDefinition {
            definition_type: "Method",
            kind: DefKind::Method,
            name,
            fqn,
            range: crate::v2::dsl::utils::canonical_range(&crate::utils::node_to_range(&param)),
            is_top_level: false,
            metadata: return_type.map(|rt| {
                Box::new(crate::v2::types::DefinitionMetadata {
                    return_type: Some(rt),
                    ..Default::default()
                })
            }),
        });
    }
    false
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
            on_scope: Some(java_record_accessors),
            call_args: Some(java_call_args),
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
            scope("constructor_declaration", "Constructor")
                .def_kind(DefKind::Constructor)
                .metadata(metadata().signature(java_signature)),
            scope("compact_constructor_declaration", "Constructor").def_kind(DefKind::Constructor),
            scope("method_declaration", "Method")
                .def_kind(DefKind::Method)
                .metadata(
                    metadata()
                        .return_type(field("type").inner("type_arguments", "type_identifier"))
                        .signature(java_signature),
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
            reference("object_creation_expression")
                .name_from(field("type").inner("type_arguments", "type_identifier"))
                .when(!has_descendant("scoped_type_identifier")),
            reference("object_creation_expression")
                .name_from(field("type").nth(Child, Kind("type_identifier"), -1))
                .when(has_descendant("scoped_type_identifier"))
                .receiver_via(field("type").nth(Child, Kind("type_identifier"), 0)),
            reference("type_identifier")
                .name_from(text())
                .when(!parent_is("object_creation_expression")),
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
                    Extract::one(Field("type"), Kind("scoped_type_identifier")),
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
            select_overloads: Some(super::java_overloads::select_overloads),
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
