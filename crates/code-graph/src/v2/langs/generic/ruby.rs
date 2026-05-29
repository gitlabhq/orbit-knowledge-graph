use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{
    self, BindingRule, BranchRule, ChainConfig, DslLanguage, FieldAccessEntry, ImportRule,
    LanguageHooks, LoopRule, ReferenceRule, ScopeRule, binding, branch, loop_rule, reference,
    scope, scopes,
};
use crate::v2::types::{BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use petgraph::graph::NodeIndex;
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::{field, no_extract, text};
use treesitter_visit::predicate::*;

use crate::v2::linker::rules::{
    ImportStrategy, ImportedSymbolFallbackContext, ReceiverMode, ResolveStage, ResolverHooks,
};
use crate::v2::linker::{CodeGraph, HasRules, ResolutionRules};

/// Methods that act as constructors — `Class.method(args)` returns a
/// `Class` instance. Shared between `SsaConfig` (binding analysis) and
/// `ResolverHooks` (chain resolution) to ensure consistency.
const CONSTRUCTOR_METHODS: &[&str] = &["new", "find", "find_by", "create", "first", "last"];
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

#[derive(Default)]
pub struct RubyDsl;

impl DslLanguage for RubyDsl {
    fn name() -> &'static str {
        "ruby"
    }

    fn language() -> Language {
        Language::Ruby
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("class", "Class")
                .def_kind(DefKind::Class)
                .metadata(metadata().super_types(ruby_super_types)),
            scope("module", "Module").def_kind(DefKind::Class),
            scope("method", "Method").def_kind(DefKind::Method),
            scope("singleton_method", "SingletonMethod").def_kind(DefKind::Method),
            // class << self: transparent scope, methods inside are
            // scoped to the parent class (don't add to FQN).
            scope("singleton_class", "SingletonClass")
                .def_kind(DefKind::Class)
                .no_scope()
                .name_from(no_extract()),
            scopes(&["lambda", "do_block", "block"], "Lambda")
                .def_kind(DefKind::Lambda)
                .no_scope()
                .name_from(no_extract()),
            // Constant assignments: `MAX = 2`, `Foo::Bar = obj`. Tree-sitter
            // tags any uppercase-leading identifier on the LHS as `constant`,
            // and qualified `Foo::Bar` LHSes as `scope_resolution`. We emit a
            // Definition so queries like "where is MAX_TRACKED_REFS_PER_PROJECT
            // defined" return a node; `no_scope` keeps the constant from
            // pushing a new FQN segment for nested defs.
            scope("assignment", "Constant")
                .def_kind(DefKind::Other)
                .when(field_kind("left", &["constant", "scope_resolution"]))
                .name_from(field("left"))
                .no_scope(),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // obj.method or method(args) — explicit call node
            reference("call")
                .name_from(field("method"))
                .receiver("receiver"),
            // Qualified constant reference: Foo::Bar::Baz used as a value
            // (not as a scope definition). The full text is the ref name,
            // resolved via scope_fqn_walk or GlobalName.
            reference("scope_resolution")
                .name_from(text())
                .when(!parent_is("scope_resolution").and(!parent_is("call"))),
            // Bare constant reference: `MAX_REFS` standalone in an expression.
            // Tree-sitter tags any uppercase-leading identifier as `constant`,
            // so this captures the same-class case (`@count < MAX_REFS`) that
            // the `identifier` rule misses. Definitional positions are always
            // the first named child of a parent, so we exclude with
            // `parent_is(...).and(!has_named_prev_sibling)`: the class/module
            // name, the LHS of an assignment, etc. The chain handler already
            // picks up constants used as receivers (`Foo.bar`), so
            // `!parent_is("call")` avoids a duplicate edge there.
            reference("constant").name_from(text()).when(
                (!parent_is("scope_resolution"))
                    .and(!parent_is("call"))
                    .and(!parent_is("superclass"))
                    .and(!parent_is("left_assignment_list"))
                    .and(!parent_is("singleton_class"))
                    .and(!parent_is("class").and(!has_named_prev_sibling()))
                    .and(!parent_is("module").and(!has_named_prev_sibling()))
                    .and(!parent_is("assignment").and(!has_named_prev_sibling()))
                    .and(!parent_is("operator_assignment").and(!has_named_prev_sibling())),
            ),
            // bare method call without parens/args — just an identifier in Ruby
            // e.g. `validate_name` inside a method body. Exclude positions
            // where identifiers are definitely not method calls.
            reference("identifier").name_from(text()).when(
                (!parent_is("call"))
                    .and(!parent_is("argument_list"))
                    .and(!parent_is("method"))
                    .and(!parent_is("singleton_method"))
                    .and(!parent_is("method_parameters"))
                    .and(!parent_is("block_parameters"))
                    .and(!parent_is("pair"))
                    .and(!parent_is("interpolation"))
                    .and(!parent_is("scope_resolution")),
            ),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_scope: Some(ruby_extract_attr_methods),
            on_import: Some(ruby_extract_imports),
            ref_name_rewrite: Some(ruby_rewrite_send),
            ..LanguageHooks::default()
        }
    }

    fn bindings() -> Vec<BindingRule> {
        vec![
            binding("assignment", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right")
                .instance_attrs(&["@"]),
            binding("operator_assignment", BindingKind::Assignment)
                .name_from(&["left"])
                .no_value(),
            binding("multiple_assignment", BindingKind::Assignment)
                .name_from(&["left"])
                .value_from("right"),
        ]
    }

    fn branches() -> Vec<BranchRule> {
        vec![
            branch("if")
                .branches(&["then", "else"])
                .condition("condition"),
            branch("unless")
                .branches(&["then", "else"])
                .condition("condition"),
            branch("case").branches(&["when", "else"]),
            // Ruby 3+ pattern matching: case x; in pattern; end
            branch("case_match").branches(&["in_clause", "else"]),
            branch("ternary")
                .branches(&["consequence", "alternative"])
                .condition("condition"),
            // begin/rescue/ensure: rescue and ensure are alternate branches.
            // Main body children are walked in the pre-block (non-branch path).
            branch("begin").branches(&["rescue", "ensure"]),
        ]
    }

    fn loops() -> Vec<LoopRule> {
        vec![
            loop_rule("while").iter_over("condition"),
            loop_rule("until").iter_over("condition"),
            loop_rule("for").iter_over("value"),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier", "constant", "scope_resolution"],
            this_kinds: &["self"],
            super_kinds: &["super"],
            field_access: vec![FieldAccessEntry {
                kind: "call",
                object: field("receiver"),
                member: field("method"),
            }],
            constructor: &[],
            qualified_type_kinds: &[],
        })
    }

    fn ssa_config() -> types::SsaConfig {
        types::SsaConfig {
            self_names: &["self"],
            super_name: Some("super"),
            constructor_methods: CONSTRUCTOR_METHODS,
        }
    }
}

/// Extract synthetic definitions from attr_accessor/attr_reader/attr_writer
/// and alias declarations.
fn ruby_extract_attr_methods(
    node: &N<'_>,
    defs: &mut Vec<crate::v2::types::CanonicalDefinition>,
    scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> bool {
    let nk = node.kind();
    let nk_ref = nk.as_ref();

    // alias new_name old_name → synthetic method def for new_name
    if nk_ref == "alias" {
        if let Some(name_node) = node.field("name") {
            let name = name_node.text().to_string();
            if !name.is_empty() {
                let fqn = crate::v2::types::Fqn::from_scope(scope_stack, &name, sep);
                defs.push(crate::v2::types::CanonicalDefinition {
                    definition_type: "Method",
                    kind: DefKind::Method,
                    name,
                    fqn,
                    range: crate::v2::types::Range::empty(),
                    is_top_level: false,
                    metadata: None,
                });
            }
        }
        return true;
    }

    if nk_ref != "call" {
        return false;
    }
    let method = node
        .field("method")
        .map(|m| m.text().to_string())
        .unwrap_or_default();

    let Some(args) = node.field("arguments") else {
        return RUBY_DSL_METHODS.contains(&method.as_str());
    };

    // Helper: extract a method name from a simple_symbol node.
    // `:foo` → `"foo"`. Returns None for non-symbol nodes.
    let symbol_name = |node: &N<'_>| -> Option<String> {
        if node.kind().as_ref() != "simple_symbol" {
            return None;
        }
        let text = node.text();
        let name = text.strip_prefix(':')?;
        (!name.is_empty()).then(|| name.to_string())
    };

    // Helper: extract a method name from a simple_symbol OR string node.
    // `:foo` → `"foo"`, `"foo"` → `"foo"`. Returns None for anything else.
    let literal_name = |node: &N<'_>| -> Option<String> {
        match node.kind().as_ref() {
            "simple_symbol" => {
                let text = node.text();
                text.strip_prefix(':')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
            }
            "string" => node
                .children()
                .find(|c| c.kind().as_ref() == "string_content")
                .map(|c| c.text().to_string())
                .filter(|s| !s.is_empty()),
            _ => None,
        }
    };

    // Helper: push a synthetic def for each symbol arg (skips non-symbol children like pairs)
    let push_symbol_defs = |args: &N<'_>,
                            defs: &mut Vec<crate::v2::types::CanonicalDefinition>,
                            def_type: &'static str,
                            kind: DefKind| {
        for arg in args.children() {
            let Some(name) = symbol_name(&arg) else {
                continue;
            };
            let fqn = crate::v2::types::Fqn::from_scope(scope_stack, &name, sep);
            defs.push(crate::v2::types::CanonicalDefinition {
                definition_type: def_type,
                kind,
                name,
                fqn,
                range: crate::v2::types::Range::empty(),
                is_top_level: false,
                metadata: None,
            });
        }
    };

    // Helper: push one synthetic def from the first symbol arg
    let push_first_symbol = |args: &N<'_>,
                             defs: &mut Vec<crate::v2::types::CanonicalDefinition>,
                             def_type: &'static str,
                             kind: DefKind| {
        for arg in args.children() {
            let Some(name) = symbol_name(&arg) else {
                continue;
            };
            let fqn = crate::v2::types::Fqn::from_scope(scope_stack, &name, sep);
            defs.push(crate::v2::types::CanonicalDefinition {
                definition_type: def_type,
                kind,
                name,
                fqn,
                range: crate::v2::types::Range::empty(),
                is_top_level: false,
                metadata: None,
            });
            break;
        }
    };

    match method.as_str() {
        "attr_accessor" | "attr_reader" | "attr_writer" => {
            push_symbol_defs(&args, defs, "Attribute", DefKind::Property);
            true
        }
        "class_attribute" | "mattr_accessor" | "cattr_accessor" | "mattr_reader"
        | "mattr_writer" | "cattr_reader" | "cattr_writer" => {
            push_symbol_defs(&args, defs, "Attribute", DefKind::Property);
            true
        }
        "delegate" => {
            push_symbol_defs(&args, defs, "Method", DefKind::Method);
            true
        }
        "def_delegators" | "def_delegator" => {
            let mut skip_first = true;
            for arg in args.children() {
                let Some(name) = symbol_name(&arg) else {
                    continue;
                };
                if skip_first {
                    skip_first = false;
                    continue;
                }
                let fqn = crate::v2::types::Fqn::from_scope(scope_stack, &name, sep);
                defs.push(crate::v2::types::CanonicalDefinition {
                    definition_type: "Method",
                    kind: DefKind::Method,
                    name,
                    fqn,
                    range: crate::v2::types::Range::empty(),
                    is_top_level: false,
                    metadata: None,
                });
            }
            true
        }
        "define_method" => {
            for arg in args.children() {
                let Some(name) = literal_name(&arg) else {
                    continue;
                };
                let fqn = crate::v2::types::Fqn::from_scope(scope_stack, &name, sep);
                defs.push(crate::v2::types::CanonicalDefinition {
                    definition_type: "Method",
                    kind: DefKind::Method,
                    name,
                    fqn,
                    range: crate::v2::types::Range::empty(),
                    is_top_level: false,
                    metadata: None,
                });
                break;
            }
            true
        }
        "scope" => {
            push_first_symbol(&args, defs, "StaticMethod", DefKind::Method);
            true
        }
        "has_many" | "belongs_to" | "has_one" | "has_and_belongs_to_many" => {
            push_first_symbol(&args, defs, "Method", DefKind::Method);
            true
        }
        // Callbacks: not handled here. Symbol args are refs, not defs.
        "before_action" | "after_action" | "around_action" | "before_filter" | "after_filter"
        | "around_filter" | "before_validation" | "after_validation" | "after_create"
        | "after_save" | "before_save" | "before_create" | "after_update" | "before_update"
        | "after_destroy" | "before_destroy" | "after_commit" | "after_rollback" => false,
        _ => false,
    }
}

/// DSL methods recognized by the on_scope hook. Used for early-return
/// when the call has no arguments.
const RUBY_DSL_METHODS: &[&str] = &[
    "attr_accessor",
    "attr_reader",
    "attr_writer",
    "class_attribute",
    "mattr_accessor",
    "cattr_accessor",
    "mattr_reader",
    "mattr_writer",
    "cattr_reader",
    "cattr_writer",
    "delegate",
    "def_delegators",
    "def_delegator",
    "define_method",
    "scope",
    "has_many",
    "belongs_to",
    "has_one",
    "has_and_belongs_to_many",
];

/// Resolve a constant identifier as a class/module FQN for chain
/// resolution. `Model.new.save!` needs `Model` to resolve to the
/// `Model` class so the chain can look up `Model::save!`.
fn ruby_resolve_ident_type(graph: &CodeGraph, name: &str) -> Option<String> {
    let nodes = graph.resolve_scope_nodes(name);
    for &node in &nodes {
        if let Some(did) = graph.graph[node].def_id() {
            let gdef = &graph.defs[did.0 as usize];
            if gdef.kind.is_type_container() {
                return Some(graph.str(gdef.fqn).to_string());
            }
        }
    }
    None
}

/// Rewrite `obj.send(:foo, ...)` / `obj.public_send(:foo, ...)` to resolve
/// as `obj.foo(...)`. Only rewrites when the first argument is a literal
/// symbol or string.
fn ruby_rewrite_send(node: &N<'_>, name: &str) -> Option<String> {
    if name != "send" && name != "public_send" && name != "__send__" {
        return None;
    }
    let args = node.field("arguments")?;
    for arg in args.children() {
        let k = arg.kind();
        match k.as_ref() {
            "simple_symbol" => return arg.text().strip_prefix(':').map(|s| s.to_string()),
            "string" => {
                return arg
                    .children()
                    .find(|c| c.kind().as_ref() == "string_content")
                    .map(|c| c.text().to_string())
                    .filter(|s| !s.is_empty());
            }
            _ => continue,
        }
    }
    None
}

fn strip_leading_scope(name: &str) -> String {
    name.strip_prefix("::").unwrap_or(name).to_string()
}

/// Extract super types: superclass + include/extend calls in the class body.
fn ruby_super_types(node: &N<'_>) -> Vec<String> {
    let mut types = Vec::new();

    // Direct superclass: class Dog < Animal
    // The "superclass" field wraps the type in a `superclass` node
    // that includes the `<` token. Extract the inner constant or
    // scope_resolution child directly.
    if let Some(s) = node.field("superclass")
        && let Some(type_node) = s.children().find(|c| {
            let k = c.kind();
            k.as_ref() == "constant" || k.as_ref() == "scope_resolution"
        })
    {
        let name = strip_leading_scope(&type_node.text());
        if !name.is_empty() {
            types.push(name);
        }
    }

    // include/extend in body: include Foo, extend Bar
    if let Some(body) = node.field("body") {
        for child in body.children() {
            if child.kind().as_ref() != "call" {
                continue;
            }
            let method_name = child
                .field("method")
                .map(|m| m.text().to_string())
                .unwrap_or_default();
            if method_name != "include" && method_name != "extend" && method_name != "prepend" {
                continue;
            }
            if let Some(args) = child.field("arguments") {
                for arg in args.children() {
                    let kind = arg.kind();
                    if kind.as_ref() == "constant" || kind.as_ref() == "scope_resolution" {
                        types.push(strip_leading_scope(&arg.text()));
                    }
                }
            }
        }
    }

    types
}

fn ruby_extract_imports(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "call" {
        return false;
    }
    let Some(method) = node.field("method").map(|m| m.text().to_string()) else {
        return false;
    };
    match method.as_str() {
        "require" | "require_relative" => {
            let Some(args) = node.field("arguments") else {
                return true;
            };
            let path = args
                .find(Child, Kind("string"))
                .and_then(|s| s.find(Child, Kind("string_content")))
                .map(|c| c.text().to_string());
            if let Some(path) = path {
                imports.push(CanonicalImport {
                    import_type: if method == "require_relative" {
                        "RequireRelative"
                    } else {
                        "Require"
                    },
                    binding_kind: ImportBindingKind::SideEffect,
                    mode: ImportMode::Runtime,
                    path,
                    name: None,
                    alias: None,
                    scope_fqn: None,
                    range: crate::v2::types::Range::empty(),
                    is_type_only: false,
                    wildcard: false,
                });
            }
            true
        }
        "include" | "extend" | "prepend" => {
            let Some(args) = node.field("arguments") else {
                return true;
            };
            let import_type = match method.as_str() {
                "include" => "Include",
                "extend" => "Extend",
                _ => "Prepend",
            };
            for arg in args.children() {
                if !matches!(arg.kind().as_ref(), "constant" | "scope_resolution") {
                    continue;
                }
                push_named_import(imports, import_type, arg.text().to_string());
            }
            true
        }
        _ => false,
    }
}

fn push_named_import(imports: &mut Vec<CanonicalImport>, import_type: &'static str, fqn: String) {
    if fqn.is_empty() {
        return;
    }
    let (path, leaf) = fqn
        .rsplit_once("::")
        .map(|(p, l)| (p.to_string(), l.to_string()))
        .unwrap_or((String::new(), fqn));
    imports.push(CanonicalImport {
        import_type,
        binding_kind: ImportBindingKind::Named,
        mode: ImportMode::Declarative,
        path,
        name: Some(leaf),
        alias: None,
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard: false,
    });
}

fn ruby_imported_symbol_candidates(
    graph: &CodeGraph,
    import_nodes: &[NodeIndex],
    ctx: ImportedSymbolFallbackContext<'_>,
) -> Vec<NodeIndex> {
    let Some(require_path) = ctx.chain.and_then(ruby_require_path_from_chain) else {
        return Vec::new();
    };

    import_nodes
        .iter()
        .copied()
        .filter(|&import_node| {
            let imp = graph.import(import_node);
            matches!(imp.import_type, "Require" | "RequireRelative")
                && matches!(imp.binding_kind, ImportBindingKind::SideEffect)
                && ruby_require_paths_match(graph.str(imp.path), &require_path)
        })
        .collect()
}

fn ruby_require_path_from_chain(chain: &[crate::v2::types::ExpressionStep]) -> Option<String> {
    let first = chain.first()?;
    let constant = match first {
        crate::v2::types::ExpressionStep::Ident(name)
        | crate::v2::types::ExpressionStep::Call(name) => name.as_str(),
        _ => return None,
    };
    ruby_constant_to_require_path(constant)
}

fn ruby_constant_to_require_path(constant: &str) -> Option<String> {
    let mut segments = Vec::new();
    for segment in constant.split("::") {
        if segment.is_empty() {
            return None;
        }
        let path_segment = ruby_constant_segment_to_path_segment(segment);
        if path_segment.is_empty() {
            return None;
        }
        segments.push(path_segment);
    }
    (!segments.is_empty()).then(|| segments.join("/"))
}

fn ruby_constant_segment_to_path_segment(segment: &str) -> String {
    let mut output = String::new();
    let chars: Vec<char> = segment.chars().collect();
    for (idx, ch) in chars.iter().enumerate() {
        if ch.is_uppercase() {
            let prev_is_lower_or_digit = idx > 0
                && chars
                    .get(idx - 1)
                    .is_some_and(|prev| prev.is_lowercase() || prev.is_ascii_digit());
            let next_is_lower = chars.get(idx + 1).is_some_and(|next| next.is_lowercase());
            if !output.is_empty() && (prev_is_lower_or_digit || next_is_lower) {
                output.push('_');
            }
            output.extend(ch.to_lowercase());
        } else {
            output.push(*ch);
        }
    }
    output
}

fn ruby_require_paths_match(import_path: &str, expected_path: &str) -> bool {
    let mut import_path = import_path.strip_suffix(".rb").unwrap_or(import_path);
    import_path = import_path.strip_prefix("./").unwrap_or(import_path);
    import_path == expected_path
        || import_path
            .strip_suffix(expected_path)
            .is_some_and(|prefix| prefix.ends_with('/'))
}

// ── Resolution rules ────────────────────────────────────────────

pub struct RubyRules;

impl HasRules for RubyRules {
    fn rules() -> ResolutionRules {
        let spec = RubyDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "ruby",
            scopes,
            spec,
            vec![
                ResolveStage::SSA,
                ResolveStage::ImplicitMember,
                ResolveStage::ImportStrategies,
            ],
            vec![
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::SameFile,
                ImportStrategy::GlobalName,
            ],
            ReceiverMode::None,
            "::",
            &["self"],
            Some("super"),
        )
        .with_hooks(ResolverHooks {
            constructor_methods: CONSTRUCTOR_METHODS,
            imported_symbol_candidates: Some(ruby_imported_symbol_candidates),
            resolve_ident_type: Some(ruby_resolve_ident_type),
            ..Default::default()
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
        RubyDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.rb",
                Language::Ruby,
                &Tracer::new(false),
            )
            .map(|r| crate::v2::dsl::engine::ParsedDefs {
                definitions: r.definitions,
                imports: r.imports,
            })
            .map_err(|e| crate::v2::pipeline::PipelineError::parse("test.rb", format!("{e:?}")))
    }

    #[test]
    fn require_extracts_runtime_side_effect_imports() {
        let result = parse("require \"json\"\nrequire_relative \"app/models/user.rb\"\n").unwrap();

        assert_eq!(result.imports.len(), 2);
        assert!(result.imports.iter().all(|import| {
            import.binding_kind == ImportBindingKind::SideEffect
                && import.mode == ImportMode::Runtime
                && import.name.is_none()
                && import.alias.is_none()
        }));
    }

    #[test]
    fn constructor_chain_produces_refs() {
        let result = RubyDsl::spec()
            .parse_full_collect(
                b"class Foo\n  def bar; end\nend\nclass Worker\n  def run\n    Foo.new.bar\n  end\nend\n",
                "test.rb",
                Language::Ruby,
                &Tracer::new(false),
            )
            .unwrap();
        let ref_names: Vec<&str> = result.refs.iter().map(|r| r.name.as_str()).collect();
        let ref_chains: Vec<_> = result
            .refs
            .iter()
            .filter(|r| r.chain.is_some())
            .map(|r| {
                (
                    r.name.as_str(),
                    r.chain
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(|s| format!("{s:?}"))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        eprintln!("refs: {ref_names:?}");
        eprintln!("chains: {ref_chains:?}");
        assert!(
            ref_chains.iter().any(|(name, _)| *name == "bar"),
            "should have a chain ref for 'bar', got: {ref_chains:?}"
        );
    }

    #[test]
    fn constant_assignments_emit_definitions() {
        let code = "MAX_REFS = 2\n\
                    class Tracker\n  \
                      MAX_TRACKED_REFS_PER_PROJECT = 2\n  \
                      DEFAULT_NAME = \"foo\"\n  \
                      counter = 0\n\
                    end\n\
                    Tracker::EXTRA = 5\n";
        let result = parse(code).unwrap();

        let consts: Vec<(&str, &str)> = result
            .definitions
            .iter()
            .filter(|d| d.definition_type == "Constant")
            .map(|d| (d.name.as_str(), d.fqn.as_str()))
            .collect();

        assert!(
            consts.contains(&("MAX_REFS", "MAX_REFS")),
            "top-level constant should be indexed: {consts:?}"
        );
        assert!(
            consts.contains(&(
                "MAX_TRACKED_REFS_PER_PROJECT",
                "Tracker::MAX_TRACKED_REFS_PER_PROJECT"
            )),
            "nested constant should carry the enclosing class FQN: {consts:?}"
        );
        assert!(
            consts.contains(&("DEFAULT_NAME", "Tracker::DEFAULT_NAME")),
            "string-valued constant should still be indexed: {consts:?}"
        );
        assert!(
            consts.contains(&("Tracker::EXTRA", "Tracker::EXTRA")),
            "qualified constant assignment should be indexed: {consts:?}"
        );
        assert!(
            !consts.iter().any(|(name, _)| *name == "counter"),
            "lowercase local variable must not be indexed as a Constant: {consts:?}"
        );
    }

    #[test]
    fn bare_constant_reference_emits_a_ref() {
        let code = "class Foo\n  \
                      MAX = 2\n  \
                      OTHER = MAX + 1\n  \
                      def consume\n    \
                        @value < MAX\n  \
                      end\n\
                    end\n\
                    X = Foo\n";
        let result = RubyDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.rb",
                Language::Ruby,
                &Tracer::new(false),
            )
            .unwrap();
        let ref_names: Vec<&str> = result.refs.iter().map(|r| r.name.as_str()).collect();
        let count_max = ref_names.iter().filter(|n| **n == "MAX").count();
        // Two MAX refs expected: one on the RHS of `OTHER = MAX + 1`, and
        // one inside `@value < MAX`. The LHS `MAX = 2` must not produce
        // a ref. Same for the LHS of `OTHER = ...` and `X = ...`, and
        // the class name `Foo` and `class Foo`.
        assert_eq!(
            count_max, 2,
            "expected 2 MAX refs (RHS of `OTHER = MAX + 1`, and `@value < MAX`), got {count_max}: {ref_names:?}"
        );
        // RHS Foo of `X = Foo` should produce a ref.
        assert!(
            ref_names.contains(&"Foo"),
            "RHS of `X = Foo` should ref Foo: {ref_names:?}"
        );
        // Definitional positions must not show up as refs.
        let lhs_count = ref_names
            .iter()
            .filter(|n| **n == "OTHER" || **n == "X")
            .count();
        assert_eq!(
            lhs_count, 0,
            "LHS constants (OTHER, X) must not be refs: {ref_names:?}"
        );
    }

    #[test]
    fn qualified_constant_reference_emits_a_ref() {
        let code = "module Foo\n  \
                      class Bar\n    \
                        MAX = 1\n  \
                      end\n\
                    end\n\
                    class User\n  \
                      def value\n    \
                        Foo::Bar::MAX\n  \
                      end\n\
                    end\n";
        let result = RubyDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.rb",
                Language::Ruby,
                &Tracer::new(false),
            )
            .unwrap();
        let ref_names: Vec<&str> = result.refs.iter().map(|r| r.name.as_str()).collect();
        assert!(
            ref_names.contains(&"Foo::Bar::MAX"),
            "qualified constant reference should appear in refs: {ref_names:?}"
        );
    }

    #[test]
    fn constants_convert_to_require_paths_without_stdlib_map() {
        assert_eq!(
            ruby_constant_to_require_path("JSON").as_deref(),
            Some("json")
        );
        assert_eq!(
            ruby_constant_to_require_path("Net::HTTP").as_deref(),
            Some("net/http")
        );
        assert_eq!(
            ruby_constant_to_require_path("ExternalClient").as_deref(),
            Some("external_client")
        );
    }

    #[test]
    fn leading_scope_stripped_from_super_types() {
        let result = parse(
            "class AbuseReportPolicy < ::BasePolicy\nend\n\
             class GroupPolicy < BasePolicy\n  include ::Gitlab::Allowable\nend\n",
        )
        .unwrap();

        let abuse = result
            .definitions
            .iter()
            .find(|d| d.name == "AbuseReportPolicy")
            .unwrap();
        let meta = abuse.metadata.as_ref().expect("AbuseReportPolicy metadata");
        assert!(
            meta.super_types.contains(&"BasePolicy".to_string()),
            "leading :: should be stripped: {:?}",
            meta.super_types
        );
        assert!(
            !meta.super_types.iter().any(|s| s.starts_with("::")),
            "no super_type should retain a leading ::: {:?}",
            meta.super_types
        );

        let group = result
            .definitions
            .iter()
            .find(|d| d.name == "GroupPolicy")
            .unwrap();
        let gmeta = group.metadata.as_ref().expect("GroupPolicy metadata");
        assert!(
            gmeta.super_types.contains(&"BasePolicy".to_string()),
            "unqualified superclass still works: {:?}",
            gmeta.super_types
        );
        assert!(
            gmeta.super_types.contains(&"Gitlab::Allowable".to_string()),
            "qualified include should be stripped of leading ::: {:?}",
            gmeta.super_types
        );
    }
}
