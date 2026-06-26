use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{
    self, BindingRule, BranchRule, ChainConfig, DslLanguage, FieldAccessEntry, ImportRule,
    LoopRule, ReferenceRule, ScopeRule, binding, branch, import, loop_rule, reference, scope,
    scopes,
};
use crate::v2::types::{BindingKind, DefKind, ImportBindingKind};
use petgraph::graph::NodeIndex;
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
use treesitter_visit::Node;
use treesitter_visit::syntax_tree::SyntaxTree;

type N<'a> = Node<'a, SyntaxTree>;

#[derive(Default)]
pub struct RubyDsl;

impl DslLanguage for RubyDsl {
    fn name() -> &'static str {
        "ruby"
    }

    fn language() -> Language {
        Language::Ruby
    }

    fn rewrite(tree: &mut SyntaxTree) {
        rewrite_ruby(tree);
    }

    fn scopes() -> Vec<ScopeRule> {
        let st_meta = || {
            metadata().super_types(|n: &Node<'_, SyntaxTree>| {
                n.children()
                    .filter(|c| c.kind().as_ref() == "__supertype")
                    .map(|c| c.text().to_string())
                    .collect()
            })
        };
        vec![
            scope("class", "Class")
                .def_kind(DefKind::Class)
                .metadata(st_meta()),
            scope("module", "Module")
                .def_kind(DefKind::Class)
                .metadata(st_meta()),
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
            // `NAME = <callable>`: a lambda or Proc bound to a constant or
            // variable (`TRIPLE = ->(x) {…}`, `DOUBLE = lambda {…}`,
            // `VALIDATE = proc {…}`, `MAKER = Proc.new {…}`). Emitted as a
            // named `Lambda` so callers can ask "what callable constants
            // exist?" and resolve references to them, rather than seeing an
            // opaque `Constant`. Declared after the `Constant` rule so it
            // wins for callable RHSes — scope dispatch tries the
            // last-declared matching rule first — while a plain `MAX = 2`
            // still falls through to `Constant`.
            scope("assignment", "Lambda")
                .def_kind(DefKind::Lambda)
                .when(ruby_lambda_assignment())
                .name_from(field("left"))
                .no_scope(),
            // Synthetic nodes injected by rewrite_ruby
            scope("__property", "Attribute")
                .def_kind(DefKind::Property)
                .no_scope(),
            scope("__method", "Method")
                .def_kind(DefKind::Method)
                .no_scope(),
            scope("__static_method", "StaticMethod")
                .def_kind(DefKind::Method)
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
        use treesitter_visit::extract::child_of_kind;
        vec![
            import("__require")
                .label("Require")
                .path_from(child_of_kind("__import_path"))
                .side_effect(),
            import("__require_relative")
                .label("RequireRelative")
                .path_from(child_of_kind("__import_path"))
                .side_effect(),
            import("__include")
                .label("Include")
                .path_from(child_of_kind("__import_path"))
                .symbol_from(child_of_kind("__import_name")),
            import("__extend")
                .label("Extend")
                .path_from(child_of_kind("__import_path"))
                .symbol_from(child_of_kind("__import_name")),
            import("__prepend")
                .label("Prepend")
                .path_from(child_of_kind("__import_path"))
                .symbol_from(child_of_kind("__import_name")),
        ]
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

/// Predicate for the `assignment → Lambda` scope rule: true when the RHS
/// of an assignment constructs a callable. Covers the four Ruby forms —
/// `-> {…}` (stabby lambda, a `lambda` node), and the `call`-shaped
/// `lambda {…}`, `proc {…}`, and `Proc.new {…}`.
fn ruby_lambda_assignment() -> Pred {
    // A bare `lambda`/`proc` call has no receiver; `foo.lambda {…}` is a
    // method named `lambda` on some object, not a Proc constructor.
    let bare_call_to = |method: &'static str| {
        Pred::Exists(Box::new(
            field("right").field("method").where_(Text(method)),
        ))
        .and(!Pred::Exists(Box::new(field("right").field("receiver"))))
    };
    let proc_new = Pred::Exists(Box::new(field("right").field("method").where_(Text("new")))).and(
        Pred::Exists(Box::new(
            field("right").field("receiver").where_(Text("Proc")),
        )),
    );

    field_kind("right", &["lambda"])
        .or(bare_call_to("lambda"))
        .or(bare_call_to("proc"))
        .or(proc_new)
}

fn symbol_name(tree: &SyntaxTree, id: u32) -> Option<String> {
    if tree.kind(id) != "simple_symbol" {
        return None;
    }
    tree.text(id)
        .strip_prefix(':')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

fn literal_name(tree: &SyntaxTree, id: u32) -> Option<String> {
    match tree.kind(id) {
        "simple_symbol" => symbol_name(tree, id),
        "string" => tree
            .children_of_kind(id, "string_content")
            .next()
            .map(|c| tree.text(c).to_string())
            .filter(|s| !s.is_empty()),
        _ => None,
    }
}

fn rewrite_ruby_synthetics(tree: &mut SyntaxTree) {
    let mut inserts: Vec<(u32, &str, String)> = Vec::new();
    let mut send_rewrites: Vec<(u32, String)> = Vec::new();

    // alias → __method
    for alias in tree.nodes_of_kind("alias").collect::<Vec<_>>() {
        if let Some(name) = tree.field(alias, "name") {
            let text = tree.text(name);
            if !text.is_empty() {
                inserts.push((alias, "__method", text.to_string()));
            }
        }
    }

    for call in tree.nodes_of_kind("call").collect::<Vec<_>>() {
        let method = tree
            .field_text(call, "method")
            .unwrap_or_default()
            .to_string();
        let args = match tree.field(call, "arguments") {
            Some(a) => a,
            None => continue,
        };

        match method.as_str() {
            "attr_accessor" | "attr_reader" | "attr_writer" | "class_attribute"
            | "mattr_accessor" | "cattr_accessor" | "mattr_reader" | "mattr_writer"
            | "cattr_reader" | "cattr_writer" => {
                for &arg in tree.children(args) {
                    if let Some(name) = symbol_name(tree, arg) {
                        inserts.push((call, "__property", name));
                    }
                }
            }
            "delegate" => {
                for &arg in tree.children(args) {
                    if let Some(name) = symbol_name(tree, arg) {
                        inserts.push((call, "__method", name));
                    }
                }
            }
            "def_delegators" | "def_delegator" => {
                let mut skip = true;
                for &arg in tree.children(args) {
                    if let Some(name) = symbol_name(tree, arg) {
                        if skip {
                            skip = false;
                            continue;
                        }
                        inserts.push((call, "__method", name));
                    }
                }
            }
            "define_method" => {
                for &arg in tree.children(args) {
                    if let Some(name) = literal_name(tree, arg) {
                        inserts.push((call, "__method", name));
                        break;
                    }
                }
            }
            "scope" => {
                for &arg in tree.children(args) {
                    if let Some(name) = symbol_name(tree, arg) {
                        inserts.push((call, "__static_method", name));
                        break;
                    }
                }
            }
            "has_many" | "belongs_to" | "has_one" | "has_and_belongs_to_many" => {
                for &arg in tree.children(args) {
                    if let Some(name) = symbol_name(tree, arg) {
                        inserts.push((call, "__method", name));
                        break;
                    }
                }
            }
            // send/public_send/__send__: rewrite the method text
            "send" | "public_send" | "__send__" => {
                for &arg in tree.children(args) {
                    if let Some(name) = literal_name(tree, arg) {
                        if let Some(method_node) = tree.field(call, "method") {
                            send_rewrites.push((method_node, name));
                        }
                        break;
                    }
                }
            }
            _ => {}
        }
    }

    for (parent, kind, text) in inserts {
        tree.insert_child(parent, kind, &text);
    }
    for (id, text) in send_rewrites {
        tree.set_text(id, &text);
    }
}

fn strip_leading_scope(s: &str) -> String {
    s.strip_prefix("::").unwrap_or(s).to_string()
}

const INCLUDE_METHODS: &[&str] = &["include", "extend", "prepend"];

fn collect_include_supertypes(
    tree: &SyntaxTree,
    call: u32,
    out: &mut Vec<(u32, String)>,
    target: u32,
) {
    if let Some(args) = tree.field(call, "arguments") {
        for &arg in tree.children(args) {
            let k = tree.kind(arg);
            if k == "constant" || k == "scope_resolution" {
                let name = strip_leading_scope(tree.text(arg));
                if !name.is_empty() {
                    out.push((target, name));
                }
            }
        }
    }
}

fn rewrite_ruby_supertypes(tree: &mut SyntaxTree) {
    let mut supertypes: Vec<(u32, String)> = Vec::new();

    for cls in tree
        .nodes_of_kind("class")
        .chain(tree.nodes_of_kind("module"))
        .collect::<Vec<_>>()
    {
        // Direct superclass
        if let Some(sc) = tree.field(cls, "superclass") {
            for &child in tree.children(sc) {
                let k = tree.kind(child);
                if k == "constant" || k == "scope_resolution" {
                    let name = strip_leading_scope(tree.text(child));
                    if !name.is_empty() {
                        supertypes.push((cls, name));
                    }
                }
            }
        }
        // include/extend/prepend in body
        if let Some(body) = tree.field(cls, "body") {
            for call in tree.children_of_kind(body, "call").collect::<Vec<_>>() {
                let method = tree
                    .field_text(call, "method")
                    .unwrap_or_default()
                    .to_string();
                if INCLUDE_METHODS.contains(&method.as_str()) {
                    collect_include_supertypes(tree, call, &mut supertypes, cls);
                } else if method == "included" || method == "prepended" {
                    // Walk into do-block body for nested include/extend/prepend
                    let block_body = tree
                        .field(call, "block")
                        .and_then(|b| tree.field(b, "body"));
                    if let Some(bb) = block_body {
                        for inner in tree.children_of_kind(bb, "call").collect::<Vec<_>>() {
                            let m = tree
                                .field_text(inner, "method")
                                .unwrap_or_default()
                                .to_string();
                            if INCLUDE_METHODS.contains(&m.as_str()) {
                                collect_include_supertypes(tree, inner, &mut supertypes, cls);
                            }
                        }
                    }
                }
            }
        }
    }

    for (cls, text) in supertypes {
        tree.insert_child(cls, "__supertype", &text);
    }
}

fn rewrite_ruby_imports(tree: &mut SyntaxTree) {
    struct RubyImport {
        call: u32,
        kind: &'static str,
        path: String,
        name: Option<String>,
    }

    let mut imports: Vec<RubyImport> = Vec::new();

    for call in tree.nodes_of_kind("call").collect::<Vec<_>>() {
        let method = tree
            .field_text(call, "method")
            .unwrap_or_default()
            .to_string();
        let args = match tree.field(call, "arguments") {
            Some(a) => a,
            None => continue,
        };
        match method.as_str() {
            "require" | "require_relative" => {
                let path = tree
                    .children_of_kind(args, "string")
                    .next()
                    .and_then(|s| tree.children_of_kind(s, "string_content").next())
                    .map(|c| tree.text(c).to_string());
                if let Some(path) = path.filter(|p| !p.is_empty()) {
                    let kind = if method == "require_relative" {
                        "__require_relative"
                    } else {
                        "__require"
                    };
                    imports.push(RubyImport {
                        call,
                        kind,
                        path,
                        name: None,
                    });
                }
            }
            "include" | "extend" | "prepend" => {
                let kind = match method.as_str() {
                    "include" => "__include",
                    "extend" => "__extend",
                    _ => "__prepend",
                };
                for &arg in tree.children(args) {
                    let k = tree.kind(arg);
                    if k == "constant" || k == "scope_resolution" {
                        let fqn = strip_leading_scope(tree.text(arg));
                        if fqn.is_empty() {
                            continue;
                        }
                        let (path, leaf) = match fqn.rsplit_once("::") {
                            Some((p, l)) => (p.to_string(), l.to_string()),
                            None => (String::new(), fqn),
                        };
                        imports.push(RubyImport {
                            call,
                            kind,
                            path,
                            name: Some(leaf),
                        });
                    }
                }
            }
            _ => {}
        }
    }

    for imp in imports {
        tree.set_kind(imp.call, imp.kind);
        tree.insert_child(imp.call, "__import_path", &imp.path);
        if let Some(name) = &imp.name {
            tree.insert_child(imp.call, "__import_name", name);
        }
    }
}

fn rewrite_ruby(tree: &mut SyntaxTree) {
    rewrite_ruby_synthetics(tree);
    rewrite_ruby_supertypes(tree);
    rewrite_ruby_imports(tree);
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
                Default::default(),
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
        let result = RubyDsl::spec().parse_full_collect(b"class Foo\n  def bar; end\nend\nclass Worker\n  def run\n    Foo.new.bar\n  end\nend\n",
        "test.rb",
        Language::Ruby,
        &Tracer::new(false), Default::default())
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
    fn lambda_and_proc_assignments_emit_lambda_definitions() {
        let code = "TRIPLE = ->(x) { x * 3 }\n\
                    DOUBLE = lambda { |x| x * 2 }\n\
                    VALIDATE = proc { |e| e }\n\
                    MAKER = Proc.new { |x| x }\n\
                    class Box\n  \
                      HALVE = ->(x) { x / 2 }\n\
                    end\n\
                    triple = ->(x) { x * 3 }\n\
                    MAX = 2\n\
                    BUILDER = Widget.new\n";
        let result = parse(code).unwrap();

        let lambdas: Vec<(&str, &str)> = result
            .definitions
            .iter()
            .filter(|d| d.definition_type == "Lambda")
            .map(|d| (d.name.as_str(), d.fqn.as_str()))
            .collect();

        for name in ["TRIPLE", "DOUBLE", "VALIDATE", "MAKER", "triple"] {
            assert!(
                lambdas.iter().any(|(n, _)| *n == name),
                "{name} should be emitted as a Lambda: {lambdas:?}"
            );
        }
        assert!(
            lambdas.contains(&("HALVE", "Box::HALVE")),
            "a nested callable constant carries its enclosing FQN: {lambdas:?}"
        );

        let kinds = |name: &str| -> Vec<&str> {
            result
                .definitions
                .iter()
                .filter(|d| d.name == name)
                .map(|d| d.definition_type)
                .collect()
        };
        // The callable RHS rule wins over the Constant rule, so a lambda
        // constant is a Lambda only — never double-emitted as a Constant.
        assert_eq!(
            kinds("TRIPLE"),
            ["Lambda"],
            "TRIPLE must not also be a Constant"
        );
        // A plain value constant still falls through to the Constant rule.
        assert_eq!(kinds("MAX"), ["Constant"]);
        // `Widget.new` is an ordinary constructor, not `Proc.new`.
        assert_eq!(kinds("BUILDER"), ["Constant"]);
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
                Default::default(),
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
                Default::default(),
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

        let allowable_import = result
            .imports
            .iter()
            .find(|i| i.name.as_deref() == Some("Allowable"))
            .expect("include ::Gitlab::Allowable should be recorded as an import");
        assert_eq!(
            allowable_import.path, "Gitlab",
            "import path should drop the leading ::, got {:?}",
            allowable_import.path
        );
    }

    #[test]
    fn module_includes_become_super_types() {
        let result = parse("module ProjectsHelper\n  include Gitlab::Allowable\nend\n").unwrap();
        let m = result
            .definitions
            .iter()
            .find(|d| d.name == "ProjectsHelper")
            .unwrap();
        let meta = m.metadata.as_ref().expect("ProjectsHelper metadata");
        assert!(
            meta.super_types.contains(&"Gitlab::Allowable".to_string()),
            "module include should be captured as a super type: {:?}",
            meta.super_types
        );
    }

    #[test]
    fn concern_included_block_includes_become_super_types() {
        let result = parse(
            "module RequestAwareEntity\n  extend ActiveSupport::Concern\n  \
             included do\n    include Gitlab::Allowable\n  end\nend\n",
        )
        .unwrap();
        let m = result
            .definitions
            .iter()
            .find(|d| d.name == "RequestAwareEntity")
            .unwrap();
        let meta = m.metadata.as_ref().expect("RequestAwareEntity metadata");
        assert!(
            meta.super_types.contains(&"Gitlab::Allowable".to_string()),
            "include inside `included do` should be captured: {:?}",
            meta.super_types
        );
    }
}
