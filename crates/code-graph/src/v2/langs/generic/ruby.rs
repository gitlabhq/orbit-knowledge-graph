use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{
    self, BindingRule, BranchRule, ChainConfig, DslLanguage, FieldAccessEntry, ImportRule,
    LanguageHooks, LoopRule, ReferenceRule, ScopeHookOutcome, ScopeRule, binding, branch,
    loop_rule, reference, scope, scopes,
};
use crate::v2::types::{
    BindingKind, CanonicalDefinition, CanonicalImport, DefKind, ImportBindingKind, ImportMode,
};
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

/// Extract synthetic definitions from Ruby DSL declarations
/// (`attr_accessor`, `delegate`, `scope`, ActiveRecord associations, etc.)
/// and from CanCanCan `condition :name { ... }` / `policy :name do ... end`
/// — the latter return `OwnsSubtree` so refs inside the block attribute to
/// the synthetic def, not the enclosing class.
fn ruby_extract_attr_methods(
    node: &N<'_>,
    defs: &mut Vec<CanonicalDefinition>,
    scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> ScopeHookOutcome {
    let nk = node.kind();
    let nk_ref = nk.as_ref();

    // alias new_name old_name → synthetic method def for new_name
    if nk_ref == "alias" {
        if let Some(name_node) = node.field("name") {
            let name = name_node.text().to_string();
            if !name.is_empty() {
                push_synthetic_method(defs, scope_stack, sep, name, "Method", DefKind::Method);
            }
        }
        return ScopeHookOutcome::Handled;
    }

    if nk_ref != "call" {
        return ScopeHookOutcome::NotHandled;
    }
    let method = node
        .field("method")
        .map(|m| m.text().to_string())
        .unwrap_or_default();

    let Some(args) = node.field("arguments") else {
        return if RUBY_DSL_METHODS.contains(&method.as_str()) {
            ScopeHookOutcome::Handled
        } else {
            ScopeHookOutcome::NotHandled
        };
    };

    let symbols =
        |args: &N<'_>| -> Vec<String> { args.children().filter_map(|a| symbol_text(&a)).collect() };
    let push_each = |defs: &mut Vec<CanonicalDefinition>,
                     names: Vec<String>,
                     def_type: &'static str,
                     kind: DefKind| {
        for n in names {
            push_synthetic_method(defs, scope_stack, sep, n, def_type, kind);
        }
    };
    let push_first = |defs: &mut Vec<CanonicalDefinition>,
                      mut names: Vec<String>,
                      def_type: &'static str,
                      kind: DefKind| {
        if !names.is_empty() {
            push_synthetic_method(defs, scope_stack, sep, names.remove(0), def_type, kind);
        }
    };

    match method.as_str() {
        "attr_accessor" | "attr_reader" | "attr_writer" | "class_attribute" | "mattr_accessor"
        | "cattr_accessor" | "mattr_reader" | "mattr_writer" | "cattr_reader" | "cattr_writer" => {
            push_each(defs, symbols(&args), "Attribute", DefKind::Property);
            ScopeHookOutcome::Handled
        }
        "delegate" => {
            push_each(defs, symbols(&args), "Method", DefKind::Method);
            ScopeHookOutcome::Handled
        }
        "def_delegators" | "def_delegator" => {
            // First symbol is the receiver; the rest are the methods.
            let mut names = symbols(&args);
            if !names.is_empty() {
                names.remove(0);
            }
            push_each(defs, names, "Method", DefKind::Method);
            ScopeHookOutcome::Handled
        }
        "define_method" => {
            // Accepts a symbol OR string literal as the method name.
            let name = args.children().find_map(|a| literal_text(&a));
            if let Some(n) = name {
                push_synthetic_method(defs, scope_stack, sep, n, "Method", DefKind::Method);
            }
            ScopeHookOutcome::Handled
        }
        "scope" => {
            push_first(defs, symbols(&args), "StaticMethod", DefKind::Method);
            ScopeHookOutcome::Handled
        }
        "has_many" | "belongs_to" | "has_one" | "has_and_belongs_to_many" => {
            push_first(defs, symbols(&args), "Method", DefKind::Method);
            ScopeHookOutcome::Handled
        }
        // CanCanCan: `condition(:name) { body }` / `policy :name do ... end`
        // produce a Method def whose CALLS edges come from the block body.
        "condition" | "policy" => {
            let has_block = node
                .children()
                .any(|c| matches!(c.kind().as_ref(), "do_block" | "block"));
            let Some(name) = args.children().find_map(|a| symbol_text(&a)) else {
                return ScopeHookOutcome::Handled;
            };
            let idx = defs.len() as u32;
            push_synthetic_method(defs, scope_stack, sep, name, "Method", DefKind::Method);
            if has_block {
                ScopeHookOutcome::OwnsSubtree(idx)
            } else {
                ScopeHookOutcome::Handled
            }
        }
        // Callbacks take symbol args that REFERENCE existing methods.
        "before_action" | "after_action" | "around_action" | "before_filter" | "after_filter"
        | "around_filter" | "before_validation" | "after_validation" | "after_create"
        | "after_save" | "before_save" | "before_create" | "after_update" | "before_update"
        | "after_destroy" | "before_destroy" | "after_commit" | "after_rollback" => {
            ScopeHookOutcome::NotHandled
        }
        _ => ScopeHookOutcome::NotHandled,
    }
}

/// Push a `Method`-shaped synthetic def with `name` under the current scope.
fn push_synthetic_method(
    defs: &mut Vec<CanonicalDefinition>,
    scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
    name: String,
    def_type: &'static str,
    kind: DefKind,
) {
    let fqn = crate::v2::types::Fqn::from_scope(scope_stack, &name, sep);
    defs.push(CanonicalDefinition {
        definition_type: def_type,
        kind,
        name,
        fqn,
        range: crate::v2::types::Range::empty(),
        is_top_level: false,
        metadata: None,
    });
}

/// `:foo` → `Some("foo")`. None for non-symbol nodes or empty symbols.
fn symbol_text(node: &N<'_>) -> Option<String> {
    if node.kind().as_ref() != "simple_symbol" {
        return None;
    }
    node.text()
        .strip_prefix(':')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Static text of `:foo` or `"foo"`. None for dynamic / non-literal nodes.
fn literal_text(node: &N<'_>) -> Option<String> {
    match node.kind().as_ref() {
        "simple_symbol" => symbol_text(node),
        "string" => string_text(node),
        _ => None,
    }
}

/// Static contents of a `"foo"` node. None for interpolated strings.
fn string_text(node: &N<'_>) -> Option<String> {
    if node.kind().as_ref() != "string" {
        return None;
    }
    node.children()
        .find(|c| c.kind().as_ref() == "string_content")
        .map(|c| c.text().to_string())
        .filter(|s| !s.is_empty())
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

/// Extract super types from a class node: direct superclass, body-level
/// `include`/`extend`/`prepend`, and GitLab `prepend_mod_with`-family calls
/// (which often live at file scope, outside the class body — we walk
/// ancestors to find them).
fn ruby_super_types(node: &N<'_>) -> Vec<String> {
    let mut types = Vec::new();

    if let Some(s) = node.field("superclass")
        && let Some(type_node) = s
            .children()
            .find(|c| matches!(c.kind().as_ref(), "constant" | "scope_resolution"))
    {
        let name = type_node.text().to_string();
        if !name.is_empty() {
            types.push(name);
        }
    }

    if let Some(body) = node.field("body") {
        for child in body.children() {
            if child.kind().as_ref() != "call" {
                continue;
            }
            let method = child
                .field("method")
                .map(|m| m.text().to_string())
                .unwrap_or_default();
            if !matches!(method.as_str(), "include" | "extend" | "prepend") {
                continue;
            }
            if let Some(args) = child.field("arguments") {
                for arg in args.children() {
                    if matches!(arg.kind().as_ref(), "constant" | "scope_resolution") {
                        types.push(arg.text().to_string());
                    }
                }
            }
        }
    }

    let class_name = node
        .field("name")
        .map(|n| n.text().to_string())
        .unwrap_or_default();
    if !class_name.is_empty() {
        ruby_collect_mod_with_super_types(node, &class_name, &mut types);
    }

    types
}

/// Walk ancestors of `class_node` collecting `EE::Arg` entries from
/// `<Receiver>.prepend_mod_with('Arg')` / `include_mod_with` / `extend_mod_with`
/// calls whose receiver's terminal `::` segment matches `class_name`.
/// Walking ancestors (not just direct siblings) covers the common nested
/// case `module Foo; class Bar; end; end; Foo::Bar.prepend_mod_with('Bar')`.
fn ruby_collect_mod_with_super_types(
    class_node: &N<'_>,
    class_name: &str,
    types: &mut Vec<String>,
) {
    let mut current = class_node.parent();
    while let Some(ancestor) = current {
        for sibling in ancestor.children() {
            let call = if sibling.kind().as_ref() == "call" {
                sibling
            } else if let Some(c) = sibling.children().find(|c| c.kind().as_ref() == "call") {
                c
            } else {
                continue;
            };
            let Some((_, names)) = parse_mod_with_call(&call) else {
                continue;
            };
            // Receiver may be bare (`Project`), top-level-qualified
            // (`::Project`), or namespace-qualified (`Foo::Project`).
            let receiver_matches = call
                .field("receiver")
                .map(|r| {
                    let t = r.text();
                    let t = t.as_ref();
                    t.rsplit("::").next().unwrap_or(t) == class_name
                })
                .unwrap_or(false);
            if receiver_matches {
                types.extend(names);
            }
        }
        current = ancestor.parent();
    }
}

/// Recognize `prepend_mod_with`/`include_mod_with`/`extend_mod_with` and
/// return the matching `import_type` label + the `EE::`-qualified names
/// derived from string literal args. Returns `None` if `call` isn't one
/// of these macros. Skips non-literal-string args (kwargs, constants).
fn parse_mod_with_call(call: &N<'_>) -> Option<(&'static str, Vec<String>)> {
    let method = call.field("method")?.text().to_string();
    let label = match method.as_str() {
        "prepend_mod_with" => "PrependModWith",
        "include_mod_with" => "IncludeModWith",
        "extend_mod_with" => "ExtendModWith",
        _ => return None,
    };
    let args = call.field("arguments")?;
    let names = args
        .children()
        .filter_map(|arg| {
            let raw = string_text(&arg)?;
            Some(if raw.starts_with("EE::") {
                raw
            } else {
                format!("EE::{raw}")
            })
        })
        .collect();
    Some((label, names))
}

fn ruby_extract_imports(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "call" {
        return false;
    }
    let Some(method) = node.field("method").map(|m| m.text().to_string()) else {
        return false;
    };
    let Some(args) = node.field("arguments") else {
        return false;
    };

    match method.as_str() {
        "require" | "require_relative" => {
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
        // GitLab CE/EE composition: `Project.prepend_mod_with('Project')`
        // resolves to `EE::Project` at runtime. EXTENDS edges are emitted
        // separately by `ruby_super_types`.
        "prepend_mod_with" | "include_mod_with" | "extend_mod_with" => {
            if let Some((import_type, names)) = parse_mod_with_call(node) {
                for fqn in names {
                    push_named_import(imports, import_type, fqn);
                }
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
}
