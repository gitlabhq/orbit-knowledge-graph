use code_graph_config::Language;
use code_graph_types::DefKind;
use parser_core::dsl::extractors::{Extract, ExtractList, field, metadata};
use parser_core::dsl::predicates::*;
use parser_core::dsl::types::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::linker::v2::HasRules;
use crate::linker::v2::rules::*;

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct PythonDsl;

type N<'a> = Node<'a, StrDoc<SupportLang>>;

fn python_super_types(node: &N<'_>) -> Vec<String> {
    let mut result = Vec::new();
    if let Some(superclasses) = node.field("superclasses") {
        for child in superclasses.children() {
            let kind = child.kind();
            if kind == "identifier" || kind == "attribute" || kind == "call" {
                let text = if kind == "call" {
                    child
                        .field("function")
                        .map(|f| f.text().to_string())
                        .unwrap_or_else(|| child.text().to_string())
                } else {
                    child.text().to_string()
                };
                if !text.is_empty() {
                    result.push(text);
                }
            }
        }
    }
    result
}

fn python_decorators(node: &N<'_>) -> Vec<String> {
    if let Some(parent) = node.parent()
        && parent.kind() == "decorated_definition"
    {
        parent
            .children()
            .filter(|c| c.kind() == "decorator")
            .map(|c| c.text().trim_start_matches('@').trim().to_string())
            .collect()
    } else {
        vec![]
    }
}

fn classify_python_function(node: &N<'_>) -> &'static str {
    let is_async = node.children().any(|c| c.kind() == "async");
    let has_decorator = node
        .parent()
        .is_some_and(|p| p.kind() == "decorated_definition");
    let is_method = node.parent().and_then(|p| p.parent()).is_some_and(|gp| {
        gp.kind() == "class_definition"
            || gp.kind() == "block"
                && gp
                    .parent()
                    .is_some_and(|ggp| ggp.kind() == "class_definition")
    });

    match (is_method, is_async, has_decorator) {
        (true, true, true) => "DecoratedAsyncMethod",
        (true, true, false) => "AsyncMethod",
        (true, false, true) => "DecoratedMethod",
        (true, false, false) => "Method",
        (false, true, true) => "DecoratedAsyncFunction",
        (false, true, false) => "AsyncFunction",
        (false, false, true) => "DecoratedFunction",
        (false, false, false) => "Function",
    }
}

impl DslLanguage for PythonDsl {
    fn name() -> &'static str {
        "python"
    }

    fn language() -> Language {
        Language::Python
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("class_definition", "Class")
                .def_kind(DefKind::Class)
                .metadata(metadata().super_types(ExtractList::Fn(python_super_types))),
            scope("class_definition", "DecoratedClass")
                .def_kind(DefKind::Class)
                .when(parent_is("decorated_definition"))
                .metadata(metadata().super_types(ExtractList::Fn(python_super_types))),
            scope_fn("function_definition", classify_python_function)
                .def_kind(DefKind::Function)
                .metadata(
                    metadata()
                        .return_type(field("return_type"))
                        .decorators(ExtractList::Fn(python_decorators)),
                ),
            scope_fn("function_definition", |_| "Method")
                .def_kind(DefKind::Method)
                .when(grandparent_is("class_definition"))
                .metadata(
                    metadata()
                        .return_type(field("return_type"))
                        .decorators(ExtractList::Fn(python_decorators)),
                ),
            scope("assignment", "Lambda")
                .def_kind(DefKind::Lambda)
                .when(field_kind("right", &["lambda"]))
                .name_from(field("left"))
                .no_scope(),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("call")
                .when(field_kind("function", &["attribute"]))
                .name_from(Extract::FieldChain(&["function", "attribute"]))
                .receiver_chain(&["function", "object"]),
            reference("call").name_from(field("function")),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier"],
            this_kinds: &[],
            super_kinds: &[],
            field_access: &[("attribute", "object", "attribute")],
            constructor: &[],
        })
    }

    fn imports() -> Vec<ImportRule> {
        fn python_import_classify(node: &N<'_>) -> &'static str {
            let _text = node.text().to_string();
            if node.children().any(|c| c.kind() == "wildcard_import") {
                return "WildcardImport";
            }
            if node.children().any(|c| c.kind() == "aliased_import") {
                return "AliasedImport";
            }
            "Import"
        }

        fn python_from_classify(node: &N<'_>) -> &'static str {
            if node.children().any(|c| c.kind() == "wildcard_import") {
                return "WildcardImport";
            }
            "FromImport"
        }

        vec![
            import("import_statement")
                .classify(python_import_classify)
                .path_from(Extract::None)
                .multi(&["dotted_name"])
                .alias_child("aliased_import")
                .wildcard_child("wildcard_import"),
            import("import_from_statement")
                .classify(python_from_classify)
                .path_from(field("module_name"))
                .multi(&["dotted_name", "identifier"])
                .alias_child("aliased_import")
                .wildcard_child("wildcard_import"),
            import("future_import_statement")
                .label("FutureImport")
                .path_from(Extract::ChildOfKind("__future__"))
                .multi(&["dotted_name", "identifier"])
                .alias_child("aliased_import")
                .wildcard_child("wildcard_import"),
        ]
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct PythonRules;

impl HasRules for PythonRules {
    fn rules() -> ResolutionRules {
        let spec = PythonDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "python",
            scopes,
            spec,
            vec![
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
            vec![
                loop_rule("for_statement").iter_over("right"),
                loop_rule("while_statement").body("body"),
                loop_rule("list_comprehension").body("body"),
                loop_rule("set_comprehension").body("body"),
                loop_rule("dictionary_comprehension").body("body"),
                loop_rule("generator_expression").body("body"),
            ],
            vec![
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
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::FilePath,
                ImportStrategy::SameFile,
            ],
            ChainMode::ValueFlow,
            ReceiverMode::Convention {
                instance_decorators: &[],
                classmethod_decorators: &["classmethod"],
                staticmethod_decorators: &["staticmethod"],
            },
            ".",
            &["self"],
            Some("super"),
            false,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_types::CanonicalParser;

    fn parse(code: &str) -> code_graph_types::CanonicalResult {
        DslParser::<PythonDsl>::default()
            .parse_file(code.as_bytes(), "test.py")
            .unwrap()
            .0
    }

    #[test]
    fn classes_and_methods() {
        let result = parse("class Calculator:\n    def add(self, a, b):\n        return a + b\n");

        assert_eq!(result.definitions.len(), 2);
        assert_eq!(result.definitions[0].name, "Calculator");
        assert_eq!(result.definitions[0].kind, DefKind::Class);
        assert!(result.definitions[0].is_top_level);

        assert_eq!(result.definitions[1].name, "add");
        assert_eq!(result.definitions[1].fqn.to_string(), "Calculator.add");
    }

    #[test]
    fn super_types() {
        let result = parse("class Dog(Animal, Serializable):\n    pass\n");
        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        let meta = dog.metadata.as_ref().expect("should have metadata");
        assert_eq!(meta.super_types.len(), 2);
    }

    #[test]
    fn return_type_annotation() {
        let result = parse("def greet(name: str) -> str:\n    return f'Hello, {name}'\n");
        let greet = result
            .definitions
            .iter()
            .find(|d| d.name == "greet")
            .unwrap();
        let meta = greet.metadata.as_ref().expect("should have metadata");
        assert_eq!(meta.return_type.as_deref(), Some("str"));
    }

    #[test]
    fn call_references() {
        let result = parse("def foo():\n    bar()\n");
        assert!(!result.references.is_empty());
        assert!(result.references.iter().any(|r| r.name == "bar"));
    }

    #[test]
    fn imports() {
        let result = parse("import os\nfrom pathlib import Path\n");
        assert!(result.imports.len() >= 2);
        assert!(result.imports.iter().any(|i| i.path == "os"));
        assert!(
            result
                .imports
                .iter()
                .any(|i| i.name.as_deref() == Some("Path"))
        );
    }
}
