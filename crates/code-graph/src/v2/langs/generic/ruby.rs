use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{
    self, BindingRule, BranchRule, ChainConfig, DslLanguage, FieldAccessEntry, ImportRule,
    LoopRule, ReferenceRule, ScopeRule, binding, branch, import, loop_rule, reference, scope,
    scopes,
};
use crate::v2::types::{BindingKind, DefKind, ImportBindingKind};
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
use treesitter_visit::syntax_tree::SyntaxTree;

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
        let mut rules = ruby_rewrites();
        rules.extend(ruby_import_rules());
        tree.apply_rewrites(&rules);
    }

    fn scopes() -> Vec<ScopeRule> {
        let st_meta = || metadata().supertypes();
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

use treesitter_visit::syntax_tree as rw;

const SEND_METHODS: &[&str] = &["send", "public_send", "__send__"];

fn ruby_import_rules() -> Vec<rw::Rule> {
    let require_path = || {
        field("arguments")
            .child_of_kind("string")
            .child_of_kind("string_content")
    };
    let mixin_args = || {
        field("arguments")
            .collect(AnyKind(&["constant", "scope_resolution"]))
            .strip_prefix("::")
    };

    vec![
        // require / require_relative
        rw::insert("call", require_path(), "__import_path").when(method_is("require")),
        rw::rename("call", "__require").when(method_is("require")),
        rw::insert("call", require_path(), "__import_path").when(method_is("require_relative")),
        rw::rename("call", "__require_relative").when(method_is("require_relative")),
        // include
        rw::insert("call", mixin_args().split_init("::"), "__import_path")
            .when(method_is("include")),
        rw::insert("call", mixin_args().split_last("::"), "__import_name")
            .when(method_is("include")),
        rw::rename("call", "__include").when(method_is("include")),
        // extend
        rw::insert("call", mixin_args().split_init("::"), "__import_path")
            .when(method_is("extend")),
        rw::insert("call", mixin_args().split_last("::"), "__import_name")
            .when(method_is("extend")),
        rw::rename("call", "__extend").when(method_is("extend")),
        // prepend
        rw::insert("call", mixin_args().split_init("::"), "__import_path")
            .when(method_is("prepend")),
        rw::insert("call", mixin_args().split_last("::"), "__import_name")
            .when(method_is("prepend")),
        rw::rename("call", "__prepend").when(method_is("prepend")),
    ]
}

const ATTR_METHODS: &[&str] = &[
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
];

fn method_is(name: &'static str) -> Pred {
    field_text("method", name)
}

fn method_in(names: &'static [&'static str]) -> Pred {
    field_text_in("method", names)
}

fn ruby_rewrites() -> Vec<rw::Rule> {
    let sym = || {
        field("arguments")
            .collect(Kind("simple_symbol"))
            .strip_prefix(":")
    };
    let sym1 = || {
        field("arguments")
            .collect(Kind("simple_symbol"))
            .strip_prefix(":")
    }; // same but .first()
    vec![
        // attr_accessor/reader/writer → __property for each symbol arg
        rw::insert("call", sym(), "__property").when(method_in(ATTR_METHODS)),
        // delegate → __method for each symbol arg
        rw::insert("call", sym(), "__method").when(method_is("delegate")),
        // def_delegators → __method, skip first symbol
        rw::insert("call", sym(), "__method")
            .when(method_in(&["def_delegators", "def_delegator"]))
            .skip(1),
        // define_method → __method from first symbol or string
        rw::insert(
            "call",
            field("arguments")
                .collect(AnyKind(&["simple_symbol", "string"]))
                .strip_prefix(":")
                .try_child("string_content"),
            "__method",
        )
        .when(method_is("define_method"))
        .first(),
        // scope → __static_method from first symbol
        rw::insert("call", sym1(), "__static_method")
            .when(method_is("scope"))
            .first(),
        // has_many etc → __method from first symbol
        rw::insert("call", sym1(), "__method")
            .when(method_in(&[
                "has_many",
                "belongs_to",
                "has_one",
                "has_and_belongs_to_many",
            ]))
            .first(),
        // alias → __method from name field
        rw::insert("alias", field("name"), "__method"),
        // send/public_send/__send__ → rewrite method text
        rw::set_text(
            "call",
            field("arguments")
                .nav(Child, AnyKind(&["simple_symbol", "string"]))
                .try_child("string_content")
                .strip_prefix(":"),
        )
        .when(field_text_in("method", SEND_METHODS))
        .onto(field("method")),
    ]
    .into_iter()
    .chain(ruby_super_type_rules())
    .collect()
}

const MIXIN_METHODS: &[&str] = &["include", "extend", "prepend"];
const CONST_KINDS: &[&str] = &["constant", "scope_resolution"];

/// Supertypes from `class Foo < Bar` and `include`/`extend`/`prepend` calls,
/// including those nested inside `included`/`prepended` hook blocks.
fn ruby_super_type_rules() -> Vec<rw::Rule> {
    // arguments → constant/scope_resolution names, `::`-stripped.
    let mixin_consts = || {
        text()
            .where_pred(method_in(MIXIN_METHODS))
            .field("arguments")
            .collect(AnyKind(CONST_KINDS))
            .strip_prefix("::")
    };
    // call inside an `included`/`prepended` block body.
    let hook_mixin_consts = || {
        text()
            .where_pred(method_in(&["included", "prepended"]))
            .field("block")
            .field("body")
            .each(mixin_consts())
    };
    ["class", "module"]
        .into_iter()
        .flat_map(|kind| {
            [
                rw::insert(
                    kind,
                    field("superclass")
                        .collect(AnyKind(CONST_KINDS))
                        .strip_prefix("::"),
                    "__supertype",
                ),
                rw::insert(kind, field("body").each(mixin_consts()), "__supertype"),
                rw::insert(kind, field("body").each(hook_mixin_consts()), "__supertype"),
            ]
        })
        .collect()
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
    use crate::v2::types::ImportMode;

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
    fn supertypes_from_superclass_include_and_hook_block() {
        let code = "class Foo < Bar\n  include Mod1\n  prepend ::Mod2\n  included do\n    include Mod3\n  end\nend\n";
        let result = parse(code).unwrap();
        let foo = result
            .definitions
            .iter()
            .find(|d| d.fqn.as_str().ends_with("Foo"))
            .expect("Foo def");
        let mut supers: Vec<&str> = foo
            .metadata
            .as_ref()
            .expect("metadata")
            .super_types
            .iter()
            .map(|s| s.as_str())
            .collect();
        supers.sort_unstable();
        assert_eq!(supers, vec!["Bar", "Mod1", "Mod2", "Mod3"]);
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
