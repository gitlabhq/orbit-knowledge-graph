use crate::v2::config::Language;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, DefKind};
use treesitter_visit::Node;
use treesitter_visit::extract::{child_of_kind, field};
use treesitter_visit::predicate::*;
use treesitter_visit::syntax_tree as rw;
use treesitter_visit::syntax_tree::SyntaxTree;

use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{
    ImportStrategy, ReceiverMode, ResolutionRules, ResolveStage, ResolverHooks,
};

type N<'a> = Node<'a, SyntaxTree>;

// ── DSL parser spec ─────────────────────────────────────────────

#[derive(Default)]
pub struct LuaDsl;

impl DslLanguage for LuaDsl {
    fn name() -> &'static str {
        "lua"
    }

    fn language() -> Language {
        Language::Lua
    }

    fn scopes() -> Vec<ScopeRule> {
        // Evaluated last-to-first: [2] checked before [1] before [0].
        // [0] is the unconditional fallback for plain `function foo()`.
        vec![
            scope("function_declaration", "Function").def_kind(DefKind::Function),
            // function M.helper() — table dot syntax; name = bare field identifier
            scope("function_declaration", "Function")
                .def_kind(DefKind::Function)
                .when(field_kind("name", &["dot_index_expression"]))
                .name_from(field("name").field("field")),
            // function M:method() — colon syntax with implicit self
            scope("function_declaration", "Method")
                .def_kind(DefKind::Method)
                .when(field_kind("name", &["method_index_expression"]))
                .name_from(field("name").field("method")),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // obj:method() — colon call; receiver is the table
            reference("function_call")
                .name_from(field("name").field("method"))
                .when(field_kind("name", &["method_index_expression"]))
                .receiver_via(field("name").field("table")),
            // M.func() — dot call; receiver is the table
            reference("function_call")
                .name_from(field("name").field("field"))
                .when(field_kind("name", &["dot_index_expression"]))
                .receiver_via(field("name").field("table")),
            // foo() — simple call
            reference("function_call").name_from(field("name")),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        vec![
            import("__require")
                .label("Require")
                .path_from(child_of_kind("__import_path"))
                .split_last(".")
                .runtime(),
        ]
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            return_kinds: &["return_statement"],
            ..LanguageHooks::default()
        }
    }

    fn rewrite(tree: &mut SyntaxTree) {
        tree.apply_rewrites(&[rw::custom(rewrite_lua)]);
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier"],
            this_kinds: &[],
            super_kinds: &[],
            field_access: vec![
                FieldAccessEntry {
                    kind: "dot_index_expression",
                    object: field("table"),
                    member: field("field"),
                },
                FieldAccessEntry {
                    kind: "method_index_expression",
                    object: field("table"),
                    member: field("method"),
                },
            ],
            constructor: &[],
            qualified_type_kinds: &[],
        })
    }

    fn package_node() -> Option<(&'static str, treesitter_visit::extract::Extract)> {
        None
    }

    // Lua files have no module declaration; the file path forms the FQN root.
    fn file_scope() -> bool {
        true
    }

    fn bindings() -> Vec<BindingRule> {
        vec![
            // local x = expr  /  local x, y = e1, e2
            binding("assignment_statement", BindingKind::Assignment)
                .name_from(&["variable_list"])
                .value_from("expression_list"),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if_statement")
                .branches(&["consequence", "alternative"])
                .condition("condition"),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("while_statement"),
            loop_rule("repeat_statement"),
            loop_rule("for_statement"),
        ]
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig {
            self_names: &["self"],
            ..types::SsaConfig::default()
        }
    }
}

fn extract_string_content(tree: &SyntaxTree, node: u32) -> Option<String> {
    let raw = tree.text(node);
    let trimmed = raw.trim_matches(|c: char| c == '"' || c == '\'');
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn rewrite_lua(tree: &mut SyntaxTree) {
    let mut rewrites: Vec<(u32, String)> = Vec::new();

    for call in tree.nodes_of_kind("function_call").collect::<Vec<_>>() {
        let name = tree.field(call, "name");
        if name.is_none_or(|n| tree.kind(n) != "identifier" || tree.text(n) != "require") {
            continue;
        }
        let Some(args) = tree.field(call, "arguments") else {
            continue;
        };
        // `require "mod"` → args is a string node directly
        // `require("mod")` → args contains a string child
        let string_node = if tree.kind(args) == "string" {
            Some(args)
        } else {
            tree.children_of_kind(args, "string").next()
        };
        if let Some(s) = string_node.and_then(|s| extract_string_content(tree, s)) {
            rewrites.push((call, s));
        }
    }

    for (call, path) in rewrites {
        tree.set_kind(call, "__require");
        tree.insert_child(call, "__import_path", &path);
    }
}

// ── Resolution rules ────────────────────────────────────────────

pub struct LuaRules;

impl HasRules for LuaRules {
    fn rules() -> ResolutionRules {
        let spec = LuaDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "lua",
            scopes,
            spec,
            vec![
                ResolveStage::SSA,
                ResolveStage::ImportStrategies,
                ResolveStage::ImplicitMember,
            ],
            vec![ImportStrategy::ScopeFqnWalk, ImportStrategy::SameFile],
            ReceiverMode::None,
            ".",
            &["self"],
            None,
        )
        .with_hooks(ResolverHooks::default())
    }
}

// ── Unit tests ───────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    fn parse(
        code: &str,
    ) -> Result<crate::v2::dsl::engine::ParsedDefs, crate::v2::pipeline::PipelineError> {
        LuaDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.lua",
                Language::Lua,
                &Tracer::new(false),
                Default::default(),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| crate::v2::pipeline::PipelineError::parse("test.lua", format!("{e:?}")))
    }

    #[test]
    fn simple_function() {
        let result = parse("function greet()\n  print('hello')\nend\n").unwrap();
        let def = result.definitions.iter().find(|d| d.name == "greet");
        assert!(def.is_some(), "should find function greet");
        assert_eq!(def.unwrap().kind, DefKind::Function);
    }

    #[test]
    fn local_function() {
        let result = parse("local function add(a, b)\n  return a + b\nend\n").unwrap();
        let def = result.definitions.iter().find(|d| d.name == "add");
        assert!(def.is_some(), "should find local function add");
        assert_eq!(def.unwrap().kind, DefKind::Function);
    }

    #[test]
    fn dot_function() {
        let result = parse("function M.greet()\n  print('hi')\nend\n").unwrap();
        let def = result.definitions.iter().find(|d| d.name == "greet");
        assert!(def.is_some(), "should find M.greet as 'greet'");
        assert_eq!(def.unwrap().kind, DefKind::Function);
    }

    #[test]
    fn method_function() {
        let result = parse("function M:speak()\n  print(self.name)\nend\n").unwrap();
        let def = result.definitions.iter().find(|d| d.name == "speak");
        assert!(def.is_some(), "should find M:speak as 'speak'");
        assert_eq!(def.unwrap().kind, DefKind::Method);
    }

    #[test]
    fn require_extraction() {
        let result = parse("local utils = require('utils')\n").unwrap();
        assert_eq!(result.imports.len(), 1, "should find one require import");
        let imp = &result.imports[0];
        assert_eq!(imp.path, "utils");
        assert_eq!(imp.name.as_deref(), Some("utils"));
    }

    #[test]
    fn dotted_require_extraction() {
        let result = parse("local json = require('vendor.json')\n").unwrap();
        assert_eq!(result.imports.len(), 1);
        let imp = &result.imports[0];
        assert_eq!(imp.path, "vendor.json");
        assert_eq!(imp.name.as_deref(), Some("json"));
    }
}
