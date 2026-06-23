use crate::v2::config::Language;
use crate::v2::dsl::types::{self, *};
use crate::v2::types::{BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::extract::field;
use treesitter_visit::predicate::*;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::linker::HasRules;
use crate::v2::linker::rules::{
    ImportStrategy, ReceiverMode, ResolutionRules, ResolveStage, ResolverHooks,
};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

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
        // require("module") is handled entirely via the on_import hook below.
        vec![]
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_import: Some(lua_extract_require),
            return_kinds: &["return_statement"],
            ..LanguageHooks::default()
        }
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

/// Extract `require("module")` and `require "module"` as runtime imports.
///
/// Called for every AST node; returns `true` only for `function_call` nodes
/// whose `name` field is the identifier `require`, preventing the default
/// import extractor from also running on those nodes.
fn lua_extract_require(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "function_call" {
        return false;
    }
    let Some(name_node) = node.field("name") else {
        return false;
    };
    if name_node.kind().as_ref() != "identifier" || name_node.text().as_ref() != "require" {
        return false;
    }
    let Some(args) = node.field("arguments") else {
        return true;
    };
    // `arguments` is either a `string` node directly (require "mod") or
    // a parenthesized `arguments` node containing a string (require("mod")).
    let string_node = if args.kind().as_ref() == "string" {
        args
    } else {
        let Some(s) = args.child_of_kind("string") else {
            // Dynamic argument (e.g. require(var)) — mark as consumed so no
            // double-processing, but we can't resolve the path statically.
            return true;
        };
        s
    };

    let raw = string_node.text().to_string();
    let module_path = raw
        .trim_matches(|c: char| c == '"' || c == '\'')
        .to_string();
    if module_path.is_empty() {
        return true;
    }

    let name = module_path.rsplit('.').next().map(|s| s.to_string());
    imports.push(CanonicalImport {
        import_type: "Require",
        binding_kind: ImportBindingKind::Named,
        mode: ImportMode::Runtime,
        path: module_path,
        name,
        alias: None,
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard: false,
    });
    true
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
