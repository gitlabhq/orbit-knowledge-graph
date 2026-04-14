use crate::linker::v2::reaching::HasRules;
use crate::linker::v2::rules::*;
use parser_core::dsl::types::DslLanguage;
use parser_core::v2::langs::python::PythonDsl;

pub struct PythonRules;

impl HasRules for PythonRules {
    fn rules() -> ResolutionRules {
        ResolutionRules {
            name: "python",

            scopes: vec![
                isolated_scope("class_definition", ScopeKind::Class),
                isolated_scope("function_definition", ScopeKind::Function),
                isolated_scope("lambda", ScopeKind::Function).name_from("parameters"),
            ],

            branches: vec![
                branch("if_statement")
                    .branches(&["block", "elif_clause", "else_clause"])
                    .condition("condition")
                    .catch_all("else_clause"),
                branch("try_statement").branches(&[
                    "block",
                    "except_clause",
                    "except_group_clause",
                    "finally_clause",
                ]),
                branch("match_statement")
                    .branches(&["case_clause"])
                    .catch_all("case_clause"),
                branch("conditional_expression").branches(&[]),
            ],

            loops: vec![
                loop_rule("for_statement").iter_over("right"),
                loop_rule("while_statement").body("body"),
                loop_rule("list_comprehension").body("body"),
                loop_rule("set_comprehension").body("body"),
                loop_rule("dictionary_comprehension").body("body"),
                loop_rule("generator_expression").body("body"),
            ],

            bindings: vec![
                binding("assignment", BindingKind::Assignment)
                    .name_from(&["left"])
                    .value_from("right")
                    .instance_attrs(&["self."]),
                binding("augmented_assignment", BindingKind::Assignment)
                    .name_from(&["left"])
                    .no_value(),
                binding("named_expression", BindingKind::Assignment)
                    .name_from(&["name"])
                    .value_from("value"),
                binding("delete_statement", BindingKind::Deletion)
                    .name_from(&["argument"])
                    .no_value(),
                binding("for_in_clause", BindingKind::ForTarget)
                    .name_from(&["left"])
                    .no_value(),
                binding("with_item", BindingKind::WithAlias)
                    .name_from(&["value"])
                    .no_value(),
            ],

            references: vec![reference_rule("call").name_from("function")],

            import_strategies: vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::FilePath,
                ImportStrategy::SameFile,
                ImportStrategy::GlobalName { max_candidates: 3 },
            ],

            chain_mode: ChainMode::ValueFlow,
            receiver: ReceiverMode::Convention {
                instance_decorators: &[],
                classmethod_decorators: &["classmethod"],
                staticmethod_decorators: &["staticmethod"],
            },
            fqn_separator: ".",
            self_names: &["self"],
            super_name: Some("super"),
            implicit_member_lookup: false,
            language_spec: Some(PythonDsl::spec()),
        }
    }
}
