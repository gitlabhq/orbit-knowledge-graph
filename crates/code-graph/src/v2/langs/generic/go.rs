use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::*;
use crate::v2::types::{BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::extract::Extract;
use treesitter_visit::extract::{child_of_kind, field, field_chain, text};
use treesitter_visit::predicate::*;

use crate::v2::linker::graph::ImportDirectoryIndex;
use crate::v2::linker::imports::dir_of;
use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackPolicy, ReceiverMode, ResolveStage, ResolverHooks,
};
use crate::v2::linker::{CodeGraph, HasRules, ResolutionRules};
use rustc_hash::FxHashSet;
use std::path::Path;
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

const GO_PRIMITIVE_TYPES: &[&str] = &[
    "int",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float32",
    "float64",
    "complex64",
    "complex128",
    "string",
    "bool",
    "byte",
    "rune",
    "error",
];

#[derive(Default)]
pub struct GoDsl;

impl DslLanguage for GoDsl {
    fn name() -> &'static str {
        "go"
    }

    fn language() -> Language {
        Language::Go
    }

    fn scopes() -> Vec<ScopeRule> {
        let mut scopes: Vec<ScopeRule> = go_callable_scopes(field("result")).into();
        scopes.extend(
            go_callable_scopes(go_pointer_result())
                .map(|rule| rule.when(field_kind("result", &["pointer_type"]))),
        );
        scopes.extend([
            scope("type_spec", "Type").def_kind(DefKind::Other),
            scope("type_spec", "Struct")
                .def_kind(DefKind::Class)
                .when(field_kind("type", &["struct_type"]))
                .metadata(metadata().super_types(go_embedded_types)),
            scope("type_spec", "Interface")
                .def_kind(DefKind::Interface)
                .when(field_kind("type", &["interface_type"])),
            scope("var_spec", "Var")
                .def_kind(DefKind::Property)
                .no_scope()
                .when(!ancestor_is(&[
                    "function_declaration",
                    "method_declaration",
                    "func_literal",
                ])),
            scope("const_spec", "Const")
                .def_kind(DefKind::Property)
                .no_scope()
                .when(!ancestor_is(&[
                    "function_declaration",
                    "method_declaration",
                    "func_literal",
                ])),
        ]);
        scopes
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            reference("call_expression")
                .name_from(field_chain(&["function", "field"]))
                .when(field_kind("function", &["selector_expression"]))
                .receiver_chain(&["function", "operand"]),
            reference("call_expression").name_from(field("function")),
            reference("type_identifier")
                .name_from(text())
                .when(!parent_is("composite_literal")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn bindings() -> Vec<BindingRule> {
        vec![
            binding("short_var_declaration", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right")
                .typed(
                    vec![
                        field("right")
                            .then(child_of_kind("composite_literal"))
                            .then(field("type")),
                    ],
                    GO_PRIMITIVE_TYPES,
                ),
            binding("var_spec", BindingKind::Assignment)
                .name_from(&["name"])
                .value_from("value")
                .typed(vec![field("type")], &[]),
            binding("assignment_statement", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right")
                .typed(
                    vec![
                        field("right")
                            .then(child_of_kind("composite_literal"))
                            .then(field("type")),
                    ],
                    GO_PRIMITIVE_TYPES,
                ),
            binding("parameter_declaration", BindingKind::Assignment)
                .name_from(&["name"])
                .typed(
                    vec![field("type").inner("pointer_type", "type_identifier")],
                    GO_PRIMITIVE_TYPES,
                ),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_statement")
                .branches(&["block"])
                .condition("condition"),
            branch("expression_switch_statement")
                .branches(&["expression_case", "default_case"])
                .condition("value"),
            branch("type_switch_statement").branches(&["type_case", "default_case"]),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![loop_rule("for_statement").body("body")]
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_import: Some(go_extract_imports),
            ..LanguageHooks::default()
        }
    }

    fn package_node() -> Option<(&'static str, Extract)> {
        Some(("package_clause", child_of_kind("package_identifier")))
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier", "field_identifier", "type_identifier"],
            this_kinds: &[],
            super_kinds: &[],
            field_access: vec![FieldAccessEntry {
                kind: "selector_expression",
                object: field("operand"),
                member: field("field"),
            }],
            constructor: &[("composite_literal", "type")],
            qualified_type_kinds: &[],
        })
    }
}

fn go_pointer_result() -> Extract {
    field("result").nav(Child, AnyKind(&["type_identifier", "qualified_type"]))
}

fn go_callable_scopes(result: Extract) -> [ScopeRule; 3] {
    [
        scope("function_declaration", "Function")
            .def_kind(DefKind::Function)
            .metadata(metadata().return_type(result.clone())),
        scope("method_declaration", "Method")
            .def_kind(DefKind::Method)
            .metadata(
                metadata().return_type(result.clone()).receiver_type(
                    field("receiver").then(
                        child_of_kind("parameter_declaration")
                            .then(field("type").inner("pointer_type", "type_identifier")),
                    ),
                ),
            ),
        scope("method_elem", "Method")
            .def_kind(DefKind::Method)
            .no_scope()
            .metadata(metadata().return_type(result)),
    ]
}

fn go_embedded_types(node: &N<'_>) -> Vec<String> {
    let Some(struct_type) = node.field("type") else {
        return vec![];
    };
    let Some(fdl) = struct_type.child_of_kind("field_declaration_list") else {
        return vec![];
    };
    fdl.children_matching(Kind("field_declaration"))
        .filter(|fd| fd.field("name").is_none())
        .filter_map(|fd| {
            fd.field("type").map(|t| {
                let s = t.text().to_string();
                s.strip_prefix('*')
                    .map(|stripped| stripped.to_string())
                    .unwrap_or(s)
            })
        })
        .collect()
}

fn go_extract_imports(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "import_declaration" {
        return false;
    }

    for child in node.children() {
        let kind = child.kind();
        match kind.as_ref() {
            "import_spec" => extract_single_import(&child, imports),
            "import_spec_list" => {
                for spec in child.children() {
                    if spec.kind().as_ref() == "import_spec" {
                        extract_single_import(&spec, imports);
                    }
                }
            }
            _ => {}
        }
    }
    true
}

fn extract_single_import(node: &N<'_>, imports: &mut Vec<CanonicalImport>) {
    let path_node = node.find(Child, Kind("interpreted_string_literal"));

    let Some(path_node) = path_node else {
        return;
    };

    let raw_path = path_node.text().to_string();
    let import_path = raw_path.trim_matches('"').to_string();

    let alias_node = node
        .children_matching(AnyKind(&["package_identifier", "blank_identifier", "dot"]))
        .find(|n| n.range().start < path_node.range().start);

    let alias = alias_node.map(|n| n.text().to_string());
    let pkg_name = alias
        .clone()
        .filter(|a| a != "_" && a != ".")
        .or_else(|| import_path.rsplit('/').next().map(|s| s.to_string()));

    let is_blank = alias.as_deref() == Some("_");
    let is_dot = alias.as_deref() == Some(".");
    let binding_kind = if is_blank {
        ImportBindingKind::SideEffect
    } else {
        ImportBindingKind::Named
    };

    imports.push(CanonicalImport {
        import_type: "Import",
        binding_kind,
        mode: ImportMode::Declarative,
        path: import_path,
        name: pkg_name,
        alias: alias.filter(|_| !is_blank && !is_dot),
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard: is_dot,
    });
}

pub struct GoRules;

impl HasRules for GoRules {
    fn rules() -> ResolutionRules {
        let spec = GoDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "go",
            scopes,
            spec,
            vec![ResolveStage::SSA, ResolveStage::ImportStrategies],
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::ExplicitImport,
                ImportStrategy::SameFile,
                ImportStrategy::WildcardImport,
            ],
            ReceiverMode::None,
            ".",
            &[],
            None,
        )
        .with_hooks(ResolverHooks {
            imported_symbol_fallback: ImportedSymbolFallbackPolicy::ambient_wildcard(),
            excluded_ambient_imported_symbol_names: &[
                "append", "cap", "clear", "close", "complex", "copy", "delete", "imag", "len",
                "make", "max", "min", "new", "panic", "print", "println", "real", "recover",
            ],
            import_directory_index_builder: Some(build_go_import_directory_index),
            ..Default::default()
        })
    }
}

const GO_MOD_MAX_BYTES: u64 = 1 << 20;

fn build_go_import_directory_index(graph: &CodeGraph) -> ImportDirectoryIndex {
    let mut index = ImportDirectoryIndex::default();
    let package_dirs: FxHashSet<&str> = graph
        .files()
        .filter(|(_, file)| file.language == Some(Language::Go))
        .map(|(_, file)| dir_of(&file.path))
        .collect();
    let modules = go_modules(Path::new(&graph.root_path), &package_dirs);
    if modules.is_empty() {
        return index;
    }
    let mut seen: FxHashSet<&str> = FxHashSet::default();
    for (_, _, import) in graph.imports_iter() {
        let import_path = graph.str(import.path);
        if !seen.insert(import_path) {
            continue;
        }
        if let Some(directory) = go_package_directory(&modules, import_path)
            && package_dirs.contains(directory.as_str())
        {
            index.insert(import_path.to_string(), directory);
        }
    }
    index
}

struct GoModule {
    path: String,
    directory: String,
}

fn go_modules(root: &Path, package_dirs: &FxHashSet<&str>) -> Vec<GoModule> {
    let mut checked: FxHashSet<&str> = FxHashSet::default();
    let mut modules: Vec<GoModule> = Vec::new();
    for &package_dir in package_dirs {
        let mut dir = package_dir;
        while checked.insert(dir) {
            if let Some(path) = read_go_module(&root.join(dir).join("go.mod")) {
                modules.push(GoModule {
                    path,
                    directory: dir.to_string(),
                });
            }
            if dir.is_empty() {
                break;
            }
            dir = dir_of(dir);
        }
    }
    let mut declarations: rustc_hash::FxHashMap<String, usize> = Default::default();
    for module in &modules {
        *declarations.entry(module.path.clone()).or_default() += 1;
    }
    modules.retain(|module| declarations[&module.path] == 1);
    modules
}

fn read_go_module(path: &Path) -> Option<String> {
    let metadata = std::fs::symlink_metadata(path).ok()?;
    if !metadata.is_file() || metadata.len() > GO_MOD_MAX_BYTES {
        return None;
    }
    parse_go_module(&std::fs::read_to_string(path).ok()?)
}

fn parse_go_module(source: &str) -> Option<String> {
    source.lines().find_map(|line| {
        let line = line.split("//").next()?.trim();
        let module = line.strip_prefix("module")?;
        if !module.starts_with(char::is_whitespace) {
            return None;
        }
        let module = module.trim().trim_matches(|c| c == '"' || c == '`');
        (!module.is_empty()).then(|| module.to_string())
    })
}

fn go_package_directory(modules: &[GoModule], import_path: &str) -> Option<String> {
    let module = modules
        .iter()
        .filter(|module| {
            import_path
                .strip_prefix(module.path.as_str())
                .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        })
        .max_by_key(|module| module.path.len())?;
    let rest = import_path[module.path.len()..].trim_start_matches('/');
    let module_dir = &module.directory;
    Some(match (module_dir.is_empty(), rest.is_empty()) {
        (true, _) => rest.to_string(),
        (false, true) => module_dir.clone(),
        (false, false) => format!("{module_dir}/{rest}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_go_module_reads_the_module_directive() {
        let source = "// header\nmodule example.com/m // trailing\n\ngo 1.22\n";
        assert_eq!(parse_go_module(source).as_deref(), Some("example.com/m"));
        assert_eq!(
            parse_go_module("module \"example.com/quoted\"\n").as_deref(),
            Some("example.com/quoted")
        );
        assert_eq!(parse_go_module("modules x\ngo 1.22\n"), None);
    }

    #[test]
    fn go_package_directory_picks_the_longest_module() {
        let module = |path: &str, directory: &str| GoModule {
            path: path.to_string(),
            directory: directory.to_string(),
        };
        let modules = vec![
            module("example.com/m", ""),
            module("example.com/m/tools", "tools"),
            module("k8s.io/api", "staging/api"),
        ];
        let dir = |path| go_package_directory(&modules, path);
        assert_eq!(
            dir("example.com/m/internal/x").as_deref(),
            Some("internal/x")
        );
        assert_eq!(dir("example.com/m/tools/gen").as_deref(), Some("tools/gen"));
        assert_eq!(dir("example.com/m/tools").as_deref(), Some("tools"));
        assert_eq!(dir("k8s.io/api/v1").as_deref(), Some("staging/api/v1"));
        assert_eq!(dir("example.com/m").as_deref(), Some(""));
        assert_eq!(dir("example.com/mx/y"), None);
        assert_eq!(dir("fmt"), None);
    }
}
