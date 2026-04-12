use crate::linker::v2::reaching::HasRules;
use crate::linker::v2::rules::*;

pub struct KotlinRules;

impl HasRules for KotlinRules {
    fn default_rules() -> ResolutionRules {
        ResolutionRules {
            name: "kotlin",

            scopes: vec![
                isolated_scope("class_declaration", ScopeKind::Class),
                isolated_scope("object_declaration", ScopeKind::Class),
                isolated_scope("companion_object", ScopeKind::Class),
                isolated_scope("function_declaration", ScopeKind::Function),
                isolated_scope("lambda_literal", ScopeKind::Function)
                    .name_from("lambda_parameters"),
                isolated_scope("anonymous_function", ScopeKind::Function),
            ],

            branches: vec![
                branch("if_expression")
                    .branches(&["control_structure_body"])
                    .condition("condition")
                    .catch_all("control_structure_body"), // kotlin if is an expression with mandatory else
                branch("when_expression").branches(&["when_entry"]),
                branch("try_expression").branches(&["statements", "catch_block", "finally_block"]),
            ],

            loops: vec![
                loop_rule("for_statement").iter_over("expression"),
                loop_rule("while_statement"),
                loop_rule("do_while_statement"),
            ],

            bindings: vec![
                binding("property_declaration", BindingKind::Assignment)
                    .name_from("name")
                    .value_from("value"),
                binding("variable_declaration", BindingKind::Assignment)
                    .name_from("name")
                    .no_value(),
                binding("value_parameter", BindingKind::Parameter)
                    .name_from("simple_identifier")
                    .no_value(),
                binding("assignment", BindingKind::Assignment)
                    .name_from("directly_assignable_expression")
                    .value_from("expression"),
            ],

            references: vec![
                reference_rule("call_expression").name_from("simple_identifier"),
                reference_rule("navigation_expression").name_from("simple_identifier"),
            ],

            import_strategies: vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::WildcardImport,
                ImportStrategy::SamePackage,
                ImportStrategy::SameFile,
                ImportStrategy::GlobalName { max_candidates: 3 },
            ],

            chain_mode: ChainMode::TypeFlow,
            receiver: ReceiverMode::Keyword,
            fqn_separator: ".",
        }
    }
}
