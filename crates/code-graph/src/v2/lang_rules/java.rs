use crate::linker::v2::reaching::HasRules;
use crate::linker::v2::rules::*;
use parser_core::dsl::types::DslLanguage;
use parser_core::v2::langs::java::JavaDsl;

pub struct JavaRules;

impl HasRules for JavaRules {
    fn rules() -> ResolutionRules {
        ResolutionRules {
            name: "java",

            scopes: vec![
                isolated_scope("class_declaration", ScopeKind::Class),
                isolated_scope("interface_declaration", ScopeKind::Class),
                isolated_scope("enum_declaration", ScopeKind::Class),
                isolated_scope("record_declaration", ScopeKind::Class),
                isolated_scope("annotation_type_declaration", ScopeKind::Class),
                isolated_scope("method_declaration", ScopeKind::Function),
                isolated_scope("constructor_declaration", ScopeKind::Function),
                isolated_scope("lambda_expression", ScopeKind::Function).name_from("parameters"),
            ],

            branches: vec![
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
                branch("switch_expression")
                    .branches(&["switch_block_statement_group", "switch_rule"]),
                branch("switch_statement").branches(&["switch_block_statement_group"]),
                branch("ternary_expression")
                    .branches(&["consequence", "alternative"])
                    .catch_all("alternative"),
            ],

            loops: vec![
                loop_rule("for_statement"),
                loop_rule("while_statement"),
                loop_rule("enhanced_for_statement").iter_over("value"),
                loop_rule("do_statement"),
            ],

            bindings: vec![
                binding("local_variable_declaration", BindingKind::Assignment)
                    .name_from("declarator"),
                binding("field_declaration", BindingKind::Assignment)
                    .name_from("declarator")
                    .instance_attrs(&["this."]),
                binding("formal_parameter", BindingKind::Parameter)
                    .name_from("name")
                    .no_value(),
                binding("catch_formal_parameter", BindingKind::Parameter)
                    .name_from("name")
                    .no_value(),
                binding("resource", BindingKind::Assignment)
                    .name_from("name")
                    .value_from("value"),
                binding("assignment_expression", BindingKind::Assignment)
                    .name_from("left")
                    .value_from("right"),
            ],

            references: vec![
                reference_rule("method_invocation").name_from("name"),
                reference_rule("object_creation_expression").name_from("type"),
            ],

            import_strategies: vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::WildcardImport,
                ImportStrategy::SamePackage,
                ImportStrategy::SameFile,
                ImportStrategy::GlobalName { max_candidates: 3 },
            ],

            chain_mode: ChainMode::TypeFlow {
                type_fields: &["type"],
                skip_types: &[
                    "int", "long", "short", "byte", "float", "double", "boolean", "char", "void",
                    "String",
                ],
            },
            receiver: ReceiverMode::Keyword,
            fqn_separator: ".",
            language_spec: Some(JavaDsl::spec()),
        }
    }
}
