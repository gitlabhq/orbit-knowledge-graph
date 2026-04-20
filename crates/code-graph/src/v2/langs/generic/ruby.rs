use crate::v2::config::Language;
use crate::v2::dsl::extractors::metadata;
use crate::v2::dsl::types::{
    self, BindingRule, BranchRule, ChainConfig, DslLanguage, FieldAccessEntry, ImportRule,
    LanguageHooks, LoopRule, ReferenceRule, ScopeRule, binding, branch, loop_rule, reference,
    scope, scopes,
};
use crate::v2::types::{BindingKind, CanonicalImport, DefKind, ImportBindingKind, ImportMode};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::{field, no_extract, text};

use crate::v2::linker::rules::{ImportStrategy, ReceiverMode, ResolveStage};
use crate::v2::linker::{HasRules, ResolutionRules};
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
            // bare method call without parens/args — just an identifier in Ruby
            // e.g. `validate_name` inside a method body. No parent filter —
            // identifiers appear in body_statement, then, else, begin, rescue, etc.
            // The early exit filter (lookup_name) rejects names not in the graph.
            reference("identifier").name_from(text()),
        ]
    }

    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_scope: Some(ruby_extract_attr_methods),
            on_import: Some(ruby_extract_imports),
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
            branch("ternary")
                .branches(&["consequence", "alternative"])
                .condition("condition"),
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
            ident_kinds: &["identifier", "constant"],
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
        }
    }
}

/// Extract synthetic method definitions from attr_accessor/attr_reader/attr_writer.
fn ruby_extract_attr_methods(
    node: &N<'_>,
    defs: &mut Vec<crate::v2::types::CanonicalDefinition>,
    scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> bool {
    if node.kind().as_ref() != "call" {
        return false;
    }
    let method = node
        .field("method")
        .map(|m| m.text().to_string())
        .unwrap_or_default();
    if method != "attr_accessor" && method != "attr_reader" && method != "attr_writer" {
        return false;
    }
    let Some(args) = node.field("arguments") else {
        return true;
    };
    for arg in args.children() {
        if arg.kind().as_ref() != "simple_symbol" {
            continue;
        }
        let name = arg.text().trim_start_matches(':').to_string();
        if name.is_empty() {
            continue;
        }
        let fqn = crate::v2::types::Fqn::from_scope(scope_stack, &name, sep);
        defs.push(crate::v2::types::CanonicalDefinition {
            definition_type: "Attribute",
            kind: DefKind::Property,
            name,
            fqn,
            range: crate::v2::types::Range::empty(),
            is_top_level: false,
            metadata: None,
        });
    }
    true
}

/// Extract super types: superclass + include/extend calls in the class body.
fn ruby_super_types(node: &N<'_>) -> Vec<String> {
    let mut types = Vec::new();

    // Direct superclass: class Dog < Animal
    if let Some(s) = node.field("superclass") {
        types.push(s.text().to_string());
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
                        types.push(arg.text().to_string());
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

    let method = match node.field("method") {
        Some(m) => m.text().to_string(),
        None => return false,
    };

    if method != "require" && method != "require_relative" {
        return false;
    }

    let arg = node
        .field("arguments")
        .and_then(|args| args.find(Child, Kind("string")))
        .and_then(|s| s.find(Child, Kind("string_content")))
        .map(|c| c.text().to_string());

    let Some(path) = arg else {
        return true;
    };

    let name = path.rsplit('/').next().map(|s| s.to_string());

    let import_type = if method == "require_relative" {
        "RequireRelative"
    } else {
        "Require"
    };

    imports.push(CanonicalImport {
        import_type,
        binding_kind: ImportBindingKind::Named,
        mode: ImportMode::Runtime,
        path,
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
            vec![ImportStrategy::ScopeFqnWalk, ImportStrategy::SameFile],
            ReceiverMode::None,
            "::",
            &["self"],
            Some("super"),
        )
    }
}
