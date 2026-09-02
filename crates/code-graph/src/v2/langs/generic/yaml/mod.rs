//! YAML as a graph language. Anchors become definitions and aliases
//! become references in every file; mapping keys become definitions
//! and imports only where an embedded document-type config claims
//! them (see [`document_types`]). Unclaimed YAML keeps its `File` node
//! and nothing else.

mod config;
mod document_types;

use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::types::DefKind;
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::child_of_kind;
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::linker::rules::{ReceiverMode, ResolutionRules};
use crate::v2::linker::{HasRules, ResolveSettings};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

const PAIR_KINDS: &[&str] = &["block_mapping_pair", "flow_pair"];

#[derive(Default)]
pub struct YamlDsl;

impl DslLanguage for YamlDsl {
    fn name() -> &'static str {
        "yaml"
    }

    fn language() -> Language {
        Language::Yaml
    }

    fn scopes() -> Vec<ScopeRule> {
        vec![
            scope("anchor", "Anchor")
                .def_kind(DefKind::Other)
                .no_scope()
                .name_from(child_of_kind("anchor_name")),
        ]
    }

    fn refs() -> Vec<ReferenceRule> {
        vec![reference("alias").name_from(child_of_kind("alias_name"))]
    }

    fn imports() -> Vec<ImportRule> {
        vec![]
    }

    fn bindings() -> Vec<BindingRule> {
        vec![]
    }

    fn hooks() -> LanguageHooks {
        LanguageHooks {
            on_scope_with_path: Some(document_types::extract_definitions),
            on_import_with_path: Some(document_types::extract_imports),
            ..LanguageHooks::default()
        }
    }

    fn chain_config() -> Option<ChainConfig> {
        None
    }
}

fn strip_quotes(s: &str) -> &str {
    s.trim_matches(|c| c == '"' || c == '\'')
}

const SCALAR_KINDS: &[&str] = &["plain_scalar", "single_quote_scalar", "double_quote_scalar"];

pub(super) fn scalar_text(node: &N<'_>) -> Option<String> {
    if node.kind().as_ref() != "flow_node" {
        return None;
    }
    let scalar = node.find(Child, AnyKind(SCALAR_KINDS))?;
    let text = strip_quotes(scalar.text().as_ref().trim()).to_string();
    (!text.is_empty()).then_some(text)
}

pub(super) fn pair_key(pair: &N<'_>) -> Option<String> {
    pair.field("key").as_ref().and_then(scalar_text)
}

pub(super) fn is_pair(node: &N<'_>) -> bool {
    PAIR_KINDS.contains(&node.kind().as_ref())
}

pub(super) fn pairs<'a>(mapping: &N<'a>) -> impl Iterator<Item = N<'a>> {
    mapping.children().filter(is_pair)
}

pub(super) fn find_pair<'a>(mapping: &N<'a>, key: &str) -> Option<N<'a>> {
    pairs(mapping).find(|pair| pair_key(pair).as_deref() == Some(key))
}

fn child_of_kinds<'a>(node: &N<'a>, kinds: &'static [&'static str]) -> Option<N<'a>> {
    node.find(Child, AnyKind(kinds)).or_else(|| {
        node.find(Child, AnyKind(&["block_node", "flow_node"]))
            .and_then(|wrapper| wrapper.find(Child, AnyKind(kinds)))
    })
}

pub(super) fn child_mapping<'a>(node: &N<'a>) -> Option<N<'a>> {
    child_of_kinds(node, &["block_mapping", "flow_mapping"])
}

pub(super) fn child_sequence<'a>(node: &N<'a>) -> Option<N<'a>> {
    child_of_kinds(node, &["block_sequence", "flow_sequence"])
}

pub(super) fn item_scalar(item: &N<'_>) -> Option<String> {
    scalar_text(item).or_else(|| {
        item.find(Child, Kind("flow_node"))
            .as_ref()
            .and_then(scalar_text)
    })
}

pub struct YamlRules;

impl HasRules for YamlRules {
    fn rules() -> ResolutionRules {
        let spec = YamlDsl::spec();
        let scopes = ResolutionRules::derive_scopes(&spec);

        ResolutionRules::new(
            "yaml",
            scopes,
            spec,
            vec![],
            vec![],
            ReceiverMode::None,
            ".",
            &[],
            None,
        )
        .with_settings(ResolveSettings::default())
    }
}
