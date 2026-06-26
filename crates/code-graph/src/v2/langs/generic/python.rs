use crate::v2::config::Language;
use crate::v2::dsl::extractors::{decorator_children, metadata};
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{CanonicalImport, DefKind};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::{Extract, child_of_kind, field, field_chain, no_extract, text};
use treesitter_visit::predicate::*;
use treesitter_visit::syntax_tree::SyntaxTree;

use crate::v2::types::BindingKind;

use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolutionRules, ResolveStage,
    ResolverHooks,
};

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct PythonDsl;

use treesitter_visit::syntax_tree as rw;

fn in_class_body() -> Pred {
    Pred::Exists(Box::new(
        Extract::one(Parent, Any)
            .nav(Parent, Any)
            .nav(Parent, Kind("class_definition")),
    ))
}

fn python_rewrites() -> Vec<rw::Rule> {
    let m = in_scope("class_definition");
    let a = has_child_text("async");
    let d = parent_is("decorated_definition");

    vec![
        rw::rename("function_definition", "__decorated_async_method")
            .when(m.clone().and(a.clone()).and(d.clone())),
        rw::rename("function_definition", "__async_method").when(m.clone().and(a.clone())),
        rw::rename("function_definition", "__decorated_method").when(m.clone().and(d.clone())),
        rw::rename("function_definition", "__method").when(m),
        rw::rename("function_definition", "__decorated_async_function")
            .when(a.clone().and(d.clone())),
        rw::rename("function_definition", "__async_function").when(a),
        rw::rename("function_definition", "__decorated_function").when(d),
        rw::rename("class_definition", "__decorated_class").when(parent_is("decorated_definition")),
        rw::rename("import_statement", "__wildcard_import_statement")
            .when(has_child(&["wildcard_import"])),
        rw::rename("import_statement", "__aliased_import_statement")
            .when(has_child(&["aliased_import"])),
        rw::rename("import_from_statement", "__wildcard_from_statement")
            .when(has_child(&["wildcard_import"])),
        rw::insert(
            "decorated_definition",
            text().collect(Kind("decorator")).strip_prefix("@"),
            "__decorator",
        )
        .onto(no_extract().nth(Child, Named, -1)),
        // Super types: handled by custom fn because of call→function fallback
        rw::insert(
            "class_definition",
            field("superclasses").collect(AnyKind(&["identifier", "attribute"])),
            "__supertype",
        ),
        rw::insert(
            "class_definition",
            field("superclasses").collect_field(Kind("call"), "function"),
            "__supertype",
        ),
    ]
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

    fn rewrite(tree: &mut SyntaxTree) {
        tree.apply_rewrites(&python_rewrites());
    }

    fn scopes() -> Vec<ScopeRule> {
        let class_meta = || metadata().supertypes();
        let func_meta = || {
            metadata()
                .return_type(field("return_type"))
                .decorators(decorator_children)
        };
        let func = |kind, label| {
            scope(kind, label)
                .def_kind(DefKind::Function)
                .metadata(func_meta())
        };
        let method = |kind, label| {
            scope(kind, label)
                .def_kind(DefKind::Method)
                .metadata(func_meta())
        };

        vec![
            scope("class_definition", "Class")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            scope("__decorated_class", "DecoratedClass")
                .def_kind(DefKind::Class)
                .metadata(class_meta()),
            func("function_definition", "Function"),
            func("__async_function", "AsyncFunction"),
            func("__decorated_function", "DecoratedFunction"),
            func("__decorated_async_function", "DecoratedAsyncFunction"),
            method("__method", "Method"),
            method("__async_method", "AsyncMethod"),
            method("__decorated_method", "DecoratedMethod"),
            method("__decorated_async_method", "DecoratedAsyncMethod"),
            scope("assignment", "Lambda")
                .def_kind(DefKind::Lambda)
                .when(field_kind("right", &["lambda"]))
                .name_from(field("left"))
                .no_scope(),
            scope("assignment", "Field")
                .def_kind(DefKind::Property)
                .no_scope()
                .when(field_kind("type", &["type"]).and(in_class_body()))
                .name_from(field("left"))
                .metadata(metadata().type_annotation(field("type"))),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("call")
                .when(field_kind("function", &["attribute"]))
                .name_from(field_chain(&["function", "attribute"]))
                .receiver_chain(&["function", "object"]),
            reference("call").name_from(field("function")),
            // Instance field access: obj.email
            reference("attribute")
                .name_from(field("attribute"))
                .receiver_chain(&["object"])
                .when(!parent_is("call")),
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
        let import_base = || {
            import("import_statement")
                .path_from(no_extract())
                .multi(&["dotted_name"])
                .alias_child("aliased_import")
                .wildcard_child("wildcard_import")
        };
        let from_base = || {
            import("import_from_statement")
                .path_from(field("module_name"))
                .multi(&["dotted_name", "identifier"])
                .alias_child("aliased_import")
                .wildcard_child("wildcard_import")
        };

        vec![
            import("__wildcard_import_statement")
                .label("WildcardImport")
                .path_from(no_extract())
                .multi(&["dotted_name"])
                .wildcard_child("wildcard_import"),
            import("__aliased_import_statement")
                .label("AliasedImport")
                .path_from(no_extract())
                .multi(&["dotted_name"])
                .alias_child("aliased_import"),
            import_base().label("Import"),
            import("__wildcard_from_statement")
                .label("WildcardImport")
                .path_from(field("module_name"))
                .multi(&["dotted_name", "identifier"])
                .wildcard_child("wildcard_import"),
            from_base().label("FromImport"),
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
            binding("typed_parameter", BindingKind::Assignment)
                .name_from_extract(child_of_kind("identifier"))
                .typed(vec![field("type")], &[]),
            binding("typed_default_parameter", BindingKind::Assignment)
                .name_from(&["name"])
                .typed(vec![field("type")], &[]),
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
            excluded_ambient_imported_symbol_names: &[
                "abs",
                "all",
                "any",
                "bool",
                "dict",
                "enumerate",
                "filter",
                "float",
                "getattr",
                "hasattr",
                "int",
                "iter",
                "len",
                "list",
                "map",
                "max",
                "min",
                "next",
                "open",
                "print",
                "range",
                "repr",
                "set",
                "setattr",
                "sorted",
                "str",
                "sum",
                "super",
                "tuple",
                "type",
                "zip",
            ],
            ..Default::default()
        })
    }
}

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

/// Derive module scope from a file path.
///
/// `services/user_service.py` → `services.user_service`
/// `models/__init__.py`       → `models`
/// `main.py`                  → `main`
/// `__init__.py`              → `None` (no enclosing package — skip scope)
fn python_module_from_path(file_path: &str, sep: &str) -> Option<String> {
    let path = std::path::Path::new(file_path);
    let stem = path.with_extension("");
    let stem_str = stem.to_str()?;
    let module = stem_str.replace(['/', '\\'], sep);
    let module = module
        .strip_suffix(&format!("{sep}__init__"))
        .unwrap_or(&module);
    // A bare `__init__.py` at the repository root has no enclosing package
    // name. The strip_suffix above only removes a dot-prefixed `.__init__`
    // segment, so the bare form arrives here unchanged and must be caught
    // explicitly before it escapes as the nonsensical module name `"__init__"`.
    if module.is_empty() || module == "__init__" {
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
                Default::default(),
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
    fn class_fields_as_properties() {
        let result = parse(
            "@dataclass\nclass User:\n    id: int\n    name: str = \"\"\n\n    def greet(self):\n        return self.name\n",
        )
        .unwrap();

        let fields: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.kind == DefKind::Property)
            .map(|d| d.fqn.to_string())
            .collect();
        assert!(fields.contains(&"test.User.id".to_string()), "{fields:?}");
        assert!(fields.contains(&"test.User.name".to_string()), "{fields:?}");

        let id = result.definitions.iter().find(|d| d.name == "id").unwrap();
        let meta = id.metadata.as_ref().expect("field should have metadata");
        assert_eq!(meta.type_annotation.as_deref(), Some("test.int"));
    }

    #[test]
    fn method_alongside_fields_still_extracted() {
        let result =
            parse("class User:\n    id: int\n\n    def greet(self):\n        return self.id\n")
                .unwrap();

        let greet = result
            .definitions
            .iter()
            .find(|d| d.name == "greet")
            .expect("method should be extracted");
        assert_eq!(greet.kind, DefKind::Method);
    }

    #[test]
    fn module_level_typed_assignment_is_not_a_property() {
        let result = parse("X: int = 1\n").unwrap();
        assert!(
            result
                .definitions
                .iter()
                .all(|d| d.kind != DefKind::Property)
        );
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
                Default::default(),
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
