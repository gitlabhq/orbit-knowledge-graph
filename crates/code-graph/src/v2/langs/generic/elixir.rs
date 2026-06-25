use crate::v2::config::Language;
use crate::v2::dsl::types::{
    ChainConfig, DslLanguage, FieldAccessEntry, LanguageHooks, ReferenceRule, ScopeRule, reference,
    scope,
};
use crate::v2::types::{CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::{Extract, child_of_kind, field, text};
use treesitter_visit::predicate::{Pred, field_kind, has_child, has_named_prev_sibling};

use crate::v2::linker::rules::{ImportStrategy, ReceiverMode, ResolveStage, ResolverHooks};
use crate::v2::linker::{HasRules, ResolutionRules};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

/// Tree-sitter-elixir parses every keyword construct (`defmodule`,
/// `def`, `alias`, ...) as a `call` whose `target` field holds the
/// keyword identifier, so rules dispatch on the target text. `target`
/// is the only field on `call`; `arguments` and `do_block` are plain
/// named children.
fn target_text(t: &'static str) -> Pred {
    Pred::Exists(Box::new(Extract::one(Field("target"), Text(t))))
}

/// Forms whose head parses as a nested `call` (`def greet(name)`);
/// the head must not emit a self-reference.
const DEF_HEAD_KEYWORDS: &[&str] = &[
    "def",
    "defp",
    "defmacro",
    "defmacrop",
    "defguard",
    "defguardp",
    "defdelegate",
];

/// Definition and import keywords; never references.
const ELIXIR_KEYWORDS: &[&str] = &[
    "def",
    "defp",
    "defmodule",
    "defmacro",
    "defmacrop",
    "defprotocol",
    "defimpl",
    "defstruct",
    "defguard",
    "defguardp",
    "defdelegate",
    "defexception",
    "defoverridable",
    "alias",
    "import",
    "use",
    "require",
];

/// Kernel.SpecialForms: control flow parses as plain calls but is
/// not callable.
const SPECIAL_FORMS: &[&str] = &[
    "if",
    "unless",
    "case",
    "cond",
    "with",
    "for",
    "try",
    "receive",
    "quote",
    "unquote",
    "unquote_splicing",
    "super",
];

fn target_is_keyword() -> Pred {
    ELIXIR_KEYWORDS
        .iter()
        .chain(SPECIAL_FORMS)
        .map(|&k| target_text(k))
        .reduce(Pred::or)
        .expect("non-empty keyword list")
}

/// The def head is either a direct `arguments` child or, with a
/// `when` guard, the left operand of the `binary_operator`. The guard
/// call on the right is a real reference.
fn is_def_head() -> Pred {
    DEF_HEAD_KEYWORDS
        .iter()
        .map(|&k| {
            let direct = Pred::Exists(Box::new(
                Extract::one(Parent, Kind("arguments"))
                    .nav(Parent, Kind("call"))
                    .nav(Field("target"), Text(k)),
            ));
            let guarded = (!has_named_prev_sibling()).and(Pred::Exists(Box::new(
                Extract::one(Parent, Kind("binary_operator"))
                    .nav(Parent, Kind("arguments"))
                    .nav(Parent, Kind("call"))
                    .nav(Field("target"), Text(k)),
            )));
            direct.or(guarded)
        })
        .reduce(Pred::or)
        .expect("non-empty keyword list")
}

/// Attributes (`@doc`, `@spec`) parse as `unary_operator` with an
/// anonymous `@` child; their contents are docs and typespecs, not
/// calls. The outermost ancestor is checked so a nested `!` inside
/// `@spec a :: !b` does not mask the `@`.
fn is_inside_module_attribute() -> Pred {
    Pred::Exists(Box::new(
        text()
            .nth(Ancestor, Kind("unary_operator"), -1)
            .nav(Child, Text("@")),
    ))
}

/// True when a call's dot target has an `alias` (module) receiver.
fn dot_receiver_is_alias() -> Pred {
    Pred::Exists(Box::new(
        Extract::one(Field("target"), Kind("dot")).nav(Field("left"), Kind("alias")),
    ))
}

#[derive(Default)]
pub struct ElixirDsl;

impl DslLanguage for ElixirDsl {
    fn name() -> &'static str {
        "elixir"
    }

    fn language() -> Language {
        Language::Elixir
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_import: Some(elixir_extract_imports),
            ..LanguageHooks::default()
        }
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            // The module name is a single `alias` token, so a dotted
            // `defmodule Foo.Bar` lands as one FQN-aligned segment.
            scope("call", "Module")
                .def_kind(DefKind::Class)
                .when(target_text("defmodule"))
                .name_from(child_of_kind("arguments").child_of_kind("alias")),
            // The head is the first named child of `arguments`: an
            // inner call, a bare identifier (zero arity), or the left
            // operand of a `when` guard. The identifier guard drops
            // dynamic heads (`def unquote(name)()`). Other def-like
            // forms (defmacro/defguard/defdelegate) have their heads
            // suppressed via DEF_HEAD_KEYWORDS but do not emit
            // definitions yet.
            scope("call", "Function")
                .def_kind(DefKind::Function)
                .when(target_text("def").or(target_text("defp")))
                .name_from(
                    child_of_kind("arguments")
                        .first_named()
                        .try_field("left")
                        .try_field("target")
                        .where_(Kind("identifier")),
                ),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![
            // Remote call (`Baz.hello(name)`). Without parentheses,
            // field access (`user.name`) parses identically to a
            // zero-arity call, so an argument-less dot only counts
            // when the receiver is an `alias` (`x |> String.downcase`).
            reference("call")
                .when(
                    field_kind("target", &["dot"])
                        .and(has_child(&["arguments"]).or(dot_receiver_is_alias()))
                        .and(!is_inside_module_attribute()),
                )
                .name_from(field("target").field("right"))
                .receiver_via(field("target").field("left")),
            // Local call (`helper(x)`).
            reference("call")
                .when(
                    field_kind("target", &["identifier"])
                        .and(!target_is_keyword())
                        .and(!is_def_head())
                        .and(!is_inside_module_attribute()),
                )
                .name_from(field("target")),
        ]
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier", "alias"],
            this_kinds: &[],
            super_kinds: &[],
            field_access: vec![FieldAccessEntry {
                kind: "dot",
                object: field("left"),
                member: field("right"),
            }],
            constructor: &[],
            qualified_type_kinds: &[],
        })
    }
}

/// Emit imports for `alias`/`import`/`require`/`use` calls.
fn elixir_extract_imports(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "call" {
        return false;
    }
    let Some(target) = node.field("target") else {
        return false;
    };
    let target_text = target.text();
    let import_type = match target_text.as_ref() {
        "alias" => "Alias",
        "import" => "Import",
        "require" => "Require",
        "use" => "Use",
        _ => return false,
    };
    // An import keyword without arguments is malformed; consume it
    // so no rule processes it further.
    let Some(args) = node.find(Child, Kind("arguments")) else {
        return true;
    };

    let keyword_opts = args.find(Child, Kind("keywords"));

    // `as:` option: alias Foo.Bar, as: Fb
    let as_alias = keyword_opts
        .as_ref()
        .and_then(|kw| find_pair(kw, "as:"))
        .and_then(|p| p.field("value"))
        .map(|v| v.text().to_string());

    // Multi-alias `alias Foo.{Bar, Baz}`: dot(left: alias, right: tuple).
    // Members may be dotted; the bound name is the last segment.
    if let Some(dot) = args.find(Child, Kind("dot")) {
        let prefix = dot
            .field("left")
            .map(|l| l.text().to_string())
            .unwrap_or_default();
        if let Some(tuple) = dot.field("right").filter(|r| r.kind().as_ref() == "tuple") {
            for member in tuple.children() {
                if member.kind().as_ref() != "alias" {
                    continue;
                }
                let full = member.text().to_string();
                let (path, name) = match full.rsplit_once('.') {
                    Some((sub, n)) if prefix.is_empty() => (sub.to_string(), n.to_string()),
                    Some((sub, n)) => (format!("{prefix}.{sub}"), n.to_string()),
                    None => (prefix.clone(), full),
                };
                push_import(imports, import_type, path, name, None, false);
            }
            return true;
        }
    }

    // Single form: one `alias` token carries the dotted module name.
    let Some(module) = args.find(Child, Kind("alias")) else {
        return true;
    };
    let full = module.text().to_string();

    if import_type == "Import" {
        // Wildcard keyed on the full module path: the wildcard
        // strategy looks up `{path}{sep}{name}` for bare calls.
        // `only:`/`except:` are not modeled; restricted imports
        // over-approximate to the whole module.
        let last = full
            .rsplit_once('.')
            .map_or(full.as_str(), |(_, n)| n)
            .to_string();
        push_import(imports, import_type, full.clone(), last, None, true);
        return true;
    }

    // alias/require/use bind the last segment as the local name.
    let (path, name) = match full.rsplit_once('.') {
        Some((p, n)) => (p.to_string(), n.to_string()),
        None => (String::new(), full),
    };
    push_import(imports, import_type, path, name, as_alias, false);
    true
}

/// Find the pair in a `keywords` node whose key matches. Keyword
/// tokens include the trailing colon and whitespace (`as: `).
fn find_pair<'a>(keywords: &N<'a>, key: &str) -> Option<N<'a>> {
    keywords
        .children()
        .filter(|c| c.kind().as_ref() == "pair")
        .find(|p| {
            p.field("key")
                .is_some_and(|k| k.text().as_ref().trim_end() == key)
        })
}

fn push_import(
    imports: &mut Vec<CanonicalImport>,
    import_type: &'static str,
    path: String,
    name: String,
    alias: Option<String>,
    wildcard: bool,
) {
    if name.is_empty() {
        return;
    }
    imports.push(CanonicalImport {
        import_type,
        binding_kind: ImportBindingKind::Named,
        mode: ImportMode::Declarative,
        path,
        name: Some(name),
        alias,
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard,
    });
}

// ── Resolution rules ────────────────────────────────────────────

pub struct ElixirRules;

impl HasRules for ElixirRules {
    fn rules() -> ResolutionRules {
        let spec = ElixirDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "elixir",
            scopes,
            spec,
            vec![ResolveStage::SSA, ResolveStage::ImportStrategies],
            vec![
                ImportStrategy::ExplicitImport,
                ImportStrategy::WildcardImport,
                ImportStrategy::ScopeFqnWalk,
                ImportStrategy::SameFile,
                ImportStrategy::GlobalName,
            ],
            ReceiverMode::None,
            ".",
            &[],
            None,
        )
        .with_hooks(ResolverHooks::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;
    use crate::v2::types::ExpressionStep;

    fn parse(
        code: &str,
    ) -> Result<crate::v2::dsl::engine::ParseFullResult, crate::v2::pipeline::PipelineError> {
        ElixirDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.ex",
                Language::Elixir,
                &Tracer::new(false),
                Default::default(),
            )
            .map_err(|e| crate::v2::pipeline::PipelineError::parse("test.ex", format!("{e:?}")))
    }

    #[test]
    fn modules_and_functions_carry_dotted_fqns() {
        let result = parse(
            "defmodule Greeter do\n  \
               def hello(name) do\n    name\n  end\n  \
               defp helper(x) do\n    x\n  end\n  \
               defmodule Inner do\n    def run, do: :ok\n  end\n\
             end\n\
             defmodule Foo.Bar do\n  def baz(x), do: x\nend\n",
        )
        .unwrap();

        let defs: Vec<(&str, &str, &str)> = result
            .definitions
            .iter()
            .map(|d| (d.definition_type, d.name.as_str(), d.fqn.as_str()))
            .collect();

        assert!(defs.contains(&("Module", "Greeter", "Greeter")), "{defs:?}");
        assert!(
            defs.contains(&("Function", "hello", "Greeter.hello")),
            "{defs:?}"
        );
        assert!(
            defs.contains(&("Function", "helper", "Greeter.helper")),
            "defp should define a function: {defs:?}"
        );
        assert!(
            defs.contains(&("Module", "Inner", "Greeter.Inner")),
            "{defs:?}"
        );
        assert!(
            defs.contains(&("Function", "run", "Greeter.Inner.run")),
            "nested module FQN: {defs:?}"
        );
        assert!(
            defs.contains(&("Module", "Foo.Bar", "Foo.Bar")),
            "dotted module name is one segment: {defs:?}"
        );
        assert!(
            defs.contains(&("Function", "baz", "Foo.Bar.baz")),
            "{defs:?}"
        );
    }

    #[test]
    fn def_head_variants_extract_the_name() {
        let result = parse(
            "defmodule Calc do\n  \
               def zero do\n    0\n  end\n  \
               def keyword, do: :ok\n  \
               def add(a, b) when is_integer(a) do\n    a + b\n  end\n  \
               def add(a, b, c) do\n    a + b + c\n  end\n\
             end\n",
        )
        .unwrap();

        let names: Vec<&str> = result
            .definitions
            .iter()
            .filter(|d| d.definition_type == "Function")
            .map(|d| d.name.as_str())
            .collect();
        assert!(names.contains(&"zero"), "zero-arity do-block: {names:?}");
        assert!(names.contains(&"keyword"), "keyword do: form: {names:?}");
        assert!(names.contains(&"add"), "when-guard head: {names:?}");

        // Arity overloads collapse to the same name-only FQN.
        let add_fqns = result
            .definitions
            .iter()
            .filter(|d| d.name == "add")
            .map(|d| d.fqn.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(add_fqns.len(), 2, "{add_fqns:?}");
        assert!(add_fqns.iter().all(|f| f == "Calc.add"), "{add_fqns:?}");
    }

    #[test]
    fn import_forms_emit_canonical_imports() {
        let result = parse(
            "defmodule Foo do\n  \
               alias Helpers.Format\n  \
               alias Foo.Bar, as: Fb\n  \
               alias Foo.{Bar, Baz}\n  \
               alias Phoenix.{LiveView, LiveView.Socket}\n  \
               import Enum\n  \
               import Helpers.Format\n  \
               use GenServer\n  \
               require Logger\n\
             end\n",
        )
        .unwrap();

        let imports: Vec<(&str, &str, &str, Option<&str>, bool)> = result
            .imports
            .iter()
            .map(|i| {
                (
                    i.import_type,
                    i.path.as_str(),
                    i.name.as_deref().unwrap_or(""),
                    i.alias.as_deref(),
                    i.wildcard,
                )
            })
            .collect();

        assert!(
            imports.contains(&("Alias", "Helpers", "Format", None, false)),
            "{imports:?}"
        );
        assert!(
            imports.contains(&("Alias", "Foo", "Bar", Some("Fb"), false)),
            "as: option becomes the alias: {imports:?}"
        );
        assert!(
            imports.contains(&("Alias", "Foo", "Baz", None, false)),
            "multi-alias expands per member: {imports:?}"
        );
        assert!(
            imports.contains(&("Alias", "Phoenix.LiveView", "Socket", None, false)),
            "dotted multi-alias member binds its last segment: {imports:?}"
        );
        assert!(
            imports.contains(&("Import", "Enum", "Enum", None, true)),
            "bare import is a wildcard keyed on the full module path: {imports:?}"
        );
        assert!(
            imports.contains(&("Import", "Helpers.Format", "Format", None, true)),
            "dotted import keeps the full module as the wildcard path: {imports:?}"
        );
        assert!(
            imports.contains(&("Use", "", "GenServer", None, false)),
            "{imports:?}"
        );
        assert!(
            imports.contains(&("Require", "", "Logger", None, false)),
            "{imports:?}"
        );
        // alias Foo.{Bar, Baz} plus alias Foo.Bar, as: Fb → two Bar rows
        assert_eq!(
            imports
                .iter()
                .filter(|(_, p, n, ..)| *p == "Foo" && *n == "Bar")
                .count(),
            2,
            "{imports:?}"
        );
    }

    #[test]
    fn remote_and_local_calls_emit_refs() {
        let result = parse(
            "defmodule Foo do\n  \
               def greet(name), do: Baz.hello(name)\n  \
               def run(x) do\n    helper(x)\n  end\n\
             end\n",
        )
        .unwrap();

        let ref_names: Vec<&str> = result.refs.iter().map(|r| r.name.as_str()).collect();
        assert!(ref_names.contains(&"hello"), "{ref_names:?}");
        assert!(ref_names.contains(&"helper"), "{ref_names:?}");

        let hello_chain = result
            .refs
            .iter()
            .find(|r| r.name == "hello")
            .and_then(|r| r.chain.clone());
        assert_eq!(
            hello_chain,
            Some(vec![
                ExpressionStep::Ident("Baz".into()),
                ExpressionStep::Call("hello".into()),
            ]),
            "remote call should chain through the Baz receiver"
        );
    }

    #[test]
    fn bare_field_access_is_not_a_call_ref() {
        let result = parse(
            "defmodule Shop do\n  \
               def total(order), do: order.price\n  \
               def show(socket) do\n    socket.assigns\n  end\n  \
               def norm(x), do: x |> String.downcase\n  \
               def trim(x), do: String.trim(x)\n\
             end\n",
        )
        .unwrap();

        let ref_names: Vec<&str> = result.refs.iter().map(|r| r.name.as_str()).collect();
        assert!(
            !ref_names.contains(&"price") && !ref_names.contains(&"assigns"),
            "lowercase-receiver field access must not become a call ref: {ref_names:?}"
        );
        assert!(
            ref_names.contains(&"downcase"),
            "parens-less module call in a pipeline is a real ref: {ref_names:?}"
        );
        assert!(
            ref_names.contains(&"trim"),
            "parenthesized module call is a real ref: {ref_names:?}"
        );
    }

    #[test]
    fn special_forms_are_not_refs() {
        let result = parse(
            "defmodule Flow do\n  \
               def decide(x) do\n    \
                 if x > 1 do\n      fetch(x)\n    end\n    \
                 case x do\n      _ -> :ok\n    end\n    \
                 with {:ok, v} <- fetch(x), do: v\n    \
                 for i <- x, do: double(i)\n  \
               end\n  \
               defmacro mk(name) do\n    \
                 quote do\n      unquote(name)\n    end\n  \
               end\n\
             end\n",
        )
        .unwrap();

        let ref_names: Vec<&str> = result.refs.iter().map(|r| r.name.as_str()).collect();
        for form in SPECIAL_FORMS {
            assert!(
                !ref_names.contains(form),
                "special form {form} must not be a ref: {ref_names:?}"
            );
        }
        assert!(
            ref_names.contains(&"fetch") && ref_names.contains(&"double"),
            "calls inside control flow are real refs: {ref_names:?}"
        );
    }

    #[test]
    fn dynamically_named_defs_are_dropped_not_garbled() {
        let result = parse(
            "defmodule Gen do\n  \
               for {k, v} <- [a: 1, b: 2] do\n    \
                 def unquote(k)(), do: unquote(v)\n  \
               end\n\
             end\n",
        )
        .unwrap();

        assert!(
            !result
                .definitions
                .iter()
                .any(|d| d.name.contains("unquote")),
            "macro-generated def heads must not emit source text as a name: {:?}",
            result.definitions
        );
    }

    #[test]
    fn keywords_def_heads_and_attributes_are_not_refs() {
        let result = parse(
            "defmodule Foo do\n  \
               alias Helpers.Format\n  \
               import Enum\n  \
               @doc \"docs\"\n  \
               @spec greet(String.t()) :: String.t()\n  \
               @enabled !feature?()\n  \
               def greet(name) do\n    name\n  end\n  \
               def add(a, b) when is_integer(a) do\n    a + b\n  end\n  \
               def check(x) do\n    !valid?(x)\n  end\n\
             end\n",
        )
        .unwrap();

        let ref_names: Vec<&str> = result.refs.iter().map(|r| r.name.as_str()).collect();
        for keyword in ELIXIR_KEYWORDS {
            assert!(
                !ref_names.contains(keyword),
                "keyword {keyword} must not be a ref: {ref_names:?}"
            );
        }
        assert!(
            !ref_names.contains(&"greet"),
            "def head must not self-reference: {ref_names:?}"
        );
        assert!(
            !ref_names.contains(&"add"),
            "when-guard def head must not self-reference: {ref_names:?}"
        );
        assert!(
            !ref_names.contains(&"doc") && !ref_names.contains(&"spec"),
            "module attributes are not calls: {ref_names:?}"
        );
        assert!(
            ref_names.contains(&"is_integer"),
            "guard body is a real ref: {ref_names:?}"
        );
        assert!(
            ref_names.contains(&"valid?"),
            "calls under ! in function bodies are real refs: {ref_names:?}"
        );
        assert!(
            !ref_names.contains(&"feature?"),
            "calls nested in attribute values stay excluded even under !: {ref_names:?}"
        );
    }
}
