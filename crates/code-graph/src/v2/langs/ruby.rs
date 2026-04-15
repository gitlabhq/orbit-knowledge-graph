use code_graph_config::Language;
use code_graph_types::{CanonicalImport, DefKind};
use parser_core::dsl::extractors::{field, metadata, Extract, ExtractList};
use parser_core::dsl::types::{
    reference, scope, scopes, ChainConfig, DslLanguage, ImportRule, ReferenceRule, ScopeRule,
};

use crate::linker::v2::rules::{ChainMode, ImportStrategy, ReceiverMode, ResolveStage};
use crate::linker::v2::{HasRules, ResolutionRules};
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
                .metadata(metadata().super_types(ExtractList::Fn(ruby_super_class))),
            scope("module", "Module").def_kind(DefKind::Class),
            scope("method", "Method").def_kind(DefKind::Method),
            scope("singleton_method", "SingletonMethod").def_kind(DefKind::Method),
            scopes(&["lambda", "do_block", "block"], "Lambda")
                .def_kind(DefKind::Lambda)
                .no_scope()
                .name_from(Extract::None),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![reference("call")
            .name_from(field("method"))
            .receiver("receiver")]
    }

    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn custom_import(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
        ruby_extract_imports(node, imports)
    }

    fn chain_config() -> Option<ChainConfig> {
        Some(ChainConfig {
            ident_kinds: &["identifier", "constant"],
            this_kinds: &["self"],
            super_kinds: &["super"],
            field_access: &[("call", "receiver", "method")],
            constructor: &[],
        })
    }
}

fn ruby_super_class(node: &N<'_>) -> Vec<String> {
    node.field("superclass")
        .map(|s| vec![s.text().to_string()])
        .unwrap_or_default()
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
        .and_then(|args| args.children().find(|c| c.kind().as_ref() == "string"))
        .and_then(|s| s.children().find(|c| c.kind().as_ref() == "string_content"))
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
        path,
        name,
        alias: None,
        scope_fqn: None,
        range: code_graph_types::Range::empty(),
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
            ChainMode::ValueFlow,
            ReceiverMode::None,
            "::",
            &["self"],
            Some("super"),
        )
    }
}
