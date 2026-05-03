use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{CanonicalImport, DefKind};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::{child_of_kind, field, field_chain, no_extract, text};
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
    if let Some(parent) = node.find(Parent, Kind("decorated_definition")) {
        parent
            .children_matching(Kind("decorator"))
            .map(|c| c.text().trim_start_matches('@').trim().to_string())
            .collect()
    } else {
        vec![]
    }
}

fn classify_python_function(node: &N<'_>) -> &'static str {
    let is_async = node.has(Child, Kind("async"));
    let has_decorator = node.has(Parent, Kind("decorated_definition"));
    let is_method = node.parent().and_then(|p| p.parent()).is_some_and(|gp| {
        gp.kind() == "class_definition"
            || gp.kind() == "block" && gp.has(Parent, Kind("class_definition"))
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

    fn hooks() -> crate::v2::dsl::types::LanguageHooks {
        crate::v2::dsl::types::LanguageHooks {
            module_scope: Some(python_module_from_path),
            return_kinds: &["return_statement"],
            adopt_sibling_refs: &["decorator"],
            resolve_import_path: Some(resolve_python_relative_import),
            import_scope_name: Some(python_import_scope_name),
            ..crate::v2::dsl::types::LanguageHooks::default()
        }
    }

    fn scopes() -> Vec<ScopeRule> {
        let class_meta = || metadata().super_types(python_super_types);
        let func_meta = || {
            metadata()
                .return_type(field("return_type"))
                .decorators(python_decorators)
        };

        let mut rules = vec![
            scope("class_definition", "Class")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("class_definition", "DecoratedClass")
                .def_kind(DefKind::Class)
                .when(parent_is("decorated_definition"))
                .metadata(class_meta()),
            scope_fn("function_definition", classify_python_function)
                .def_kind(DefKind::Function)
                .metadata(func_meta()),
            scope("assignment", "Lambda")
                .def_kind(DefKind::Lambda)
                .when(field_kind("right", &["lambda"]))
                .name_from(field("left"))
                .no_scope(),
        ];

        // Inside a class: functions become methods
        rules.extend(within(
            grandparent_is("class_definition"),
            vec![
                scope_fn("function_definition", |_| "Method")
                    .def_kind(DefKind::Method)
                    .metadata(func_meta()),
            ],
        ));

        rules
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("call")
                .when(field_kind("function", &["attribute"]))
                .name_from(field_chain(&["function", "attribute"]))
                .receiver_chain(&["function", "object"]),
            reference("call").name_from(field("function")),
            // Bare type references in annotations: x: MyClass, def foo() -> MyClass
            reference("type").name_from(text()),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier"],
            this_kinds: &[],
            super_kinds: &[],
            field_access: vec![FieldAccessEntry {
                kind: "attribute",
                object: field("object"),
                member: field("attribute"),
            }],
            constructor: &[],
            qualified_type_kinds: &[],
        })
    }

    fn imports() -> Vec<ImportRule> {
        fn python_import_classify(node: &N<'_>) -> &'static str {
            if node.has(Child, Kind("wildcard_import")) {
                return "WildcardImport";
            }
            if node.has(Child, Kind("aliased_import")) {
                return "AliasedImport";
            }
            "Import"
        }

        fn python_from_classify(node: &N<'_>) -> &'static str {
            if node.has(Child, Kind("wildcard_import")) {
                return "WildcardImport";
            }
            "FromImport"
        }

        vec![
            import("import_statement")
                .classify(python_import_classify)
                .path_from(no_extract())
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
                .path_from(child_of_kind("__future__"))
                .multi(&["dotted_name", "identifier"])
                .alias_child("aliased_import")
                .wildcard_child("wildcard_import"),
        ]
    }

    fn bindings() -> Vec<BindingRule> {
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
        ]
    }

    fn branches() -> Vec<BranchRule> {
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
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("for_statement").iter_over("right"),
            loop_rule("while_statement").body("body"),
            LoopRule {
                kinds: vec![
                    "list_comprehension",
                    "set_comprehension",
                    "dictionary_comprehension",
                    "generator_expression",
                ],
                body_field: "body",
                iter_field: None,
            },
        ]
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig {
            self_names: &["self"],
            super_name: Some("super"),
            ..Default::default()
        }
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
            vec![ResolveStage::SSA, ResolveStage::ImportStrategies],
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::WildcardImport,
                ImportStrategy::FilePath,
                ImportStrategy::SameFile,
            ],
            ReceiverMode::Convention {
                instance_decorators: &[],
                classmethod_decorators: &["classmethod"],
                staticmethod_decorators: &["staticmethod"],
            },
            ".",
            &["self"],
            Some("super"),
        )
        .with_hooks(ResolverHooks {
            call_method: Some("__call__"),
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            excluded_ambient_imported_symbol_names: &["print"],
            ..Default::default()
        })
    }
}

/// Derive module scope from file path.
/// `services/user_service.py` → `services.user_service`
/// `models/__init__.py` → `models`
/// `main.py` → `main`
/// Resolve Python relative import paths against the current module scope.
/// `from .models import User` in module `pkg.sub.mod` → `pkg.sub.models`
/// `from ..services import Auth` in `pkg.sub.mod` → `pkg.services`
fn resolve_python_relative_import(raw_path: &str, module_scope: &str, sep: &str) -> Option<String> {
    if !raw_path.starts_with('.') {
        return None; // absolute import, no resolution needed
    }
    let dots = raw_path.chars().take_while(|&c| c == '.').count();
    let suffix = &raw_path[dots..];

    // Module scope is the file's module (e.g. "pkg.sub.module").
    // 1 dot = same package (drop last component), 2 dots = parent, etc.
    let parts: Vec<&str> = module_scope.split(sep).collect();
    if dots > parts.len() {
        return None; // too many dots, can't resolve
    }
    let base = &parts[..parts.len() - dots];
    if suffix.is_empty() {
        let joined = base.join(sep);
        if joined.is_empty() {
            None
        } else {
            Some(joined)
        }
    } else {
        let suffix_clean = suffix.trim_start_matches('.');
        if base.is_empty() {
            Some(suffix_clean.to_string())
        } else {
            Some(format!("{}{sep}{suffix_clean}", base.join(sep)))
        }
    }
}

fn python_module_from_path(file_path: &str, sep: &str) -> Option<String> {
    let path = std::path::Path::new(file_path);
    let stem = path.with_extension("");
    let stem_str = stem.to_str()?;
    let module = stem_str.replace(['/', '\\'], sep);
    let module = module
        .strip_suffix(&format!("{sep}__init__"))
        .unwrap_or(&module);
    if module.is_empty() {
        return None;
    }
    Some(module.to_string())
}

fn python_import_scope_name(imp: &CanonicalImport, sep: &str) -> Option<String> {
    if let Some(alias) = &imp.alias {
        return Some(alias.clone());
    }

    if imp.import_type == "Import" || imp.import_type == "AliasedImport" {
        return imp
            .path
            .split(sep)
            .next()
            .filter(|segment| !segment.is_empty())
            .map(ToString::to_string);
    }

    imp.name.clone().or_else(|| {
        (!imp.path.is_empty()).then(|| {
            imp.path
                .rsplit_once(sep)
                .map_or(imp.path.as_str(), |(_, name)| name)
                .to_string()
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    fn parse(
        code: &str,
    ) -> Result<crate::v2::dsl::engine::ParsedDefs, crate::v2::pipeline::PipelineError> {
        PythonDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.py",
                crate::v2::config::Language::Python,
                &Tracer::new(false),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| {
                crate::v2::pipeline::PipelineError::parse(
                    "test.py",
                    format!("Invalid UTF-8: {:?}", e),
                )
            })
    }

    #[test]
    fn classes_and_methods() {
        let result =
            parse("class Calculator:\n    def add(self, a, b):\n        return a + b\n").unwrap();

        assert_eq!(result.definitions.len(), 2);
        assert_eq!(result.definitions[0].name, "Calculator");
        assert_eq!(result.definitions[0].kind, DefKind::Class);
        assert!(result.definitions[0].is_top_level);

        assert_eq!(result.definitions[1].name, "add");
        // FQN includes module prefix from file path (test.py → "test")
        assert_eq!(result.definitions[1].fqn.to_string(), "test.Calculator.add");
    }

    #[test]
    fn super_types() {
        let result = parse("class Dog(Animal, Serializable):\n    pass\n").unwrap();
        let dog = result.definitions.iter().find(|d| d.name == "Dog").unwrap();
        let meta = dog.metadata.as_ref().expect("should have metadata");
        assert_eq!(meta.super_types.len(), 2);
    }

    #[test]
    fn return_type_annotation() {
        let result = parse("def greet(name: str) -> str:\n    return f'Hello, {name}'\n").unwrap();
        let greet = result
            .definitions
            .iter()
            .find(|d| d.name == "greet")
            .unwrap();
        let meta = greet.metadata.as_ref().expect("should have metadata");
        // "str" is FQN-qualified with the module prefix from "test.py"
        assert_eq!(meta.return_type.as_deref(), Some("test.str"));
    }

    #[test]
    fn call_references() {
        let tracer = crate::v2::trace::Tracer::new(false);
        let result = PythonDsl::spec()
            .parse_full_collect(
                b"def foo():\n    bar()\n",
                "test.py",
                crate::v2::config::Language::Python,
                &tracer,
            )
            .unwrap();
        let ref_names: Vec<_> = result.refs.iter().map(|r| r.name.as_str()).collect();
        assert!(!ref_names.is_empty());
        assert!(ref_names.contains(&"bar"));
    }

    #[test]
    fn imports() {
        let result = parse("import os\nfrom pathlib import Path\n").unwrap();
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
