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

pub(super) fn scalar_text(node: &N<'_>) -> Option<String> {
    if node.kind().as_ref() != "flow_node" {
        return None;
    }
    if node
        .find(Child, AnyKind(&["flow_sequence", "flow_mapping"]))
        .is_some()
    {
        return None;
    }
    let text = strip_quotes(node.text().as_ref().trim()).to_string();
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

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    pub(in super::super) fn parse_at(
        file_path: &str,
        code: &str,
    ) -> crate::v2::dsl::engine::ParseFullResult {
        YamlDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                file_path,
                Language::Yaml,
                &Tracer::new(false),
                Default::default(),
            )
            .unwrap()
    }

    pub(in super::super) fn defs_at(file_path: &str, code: &str) -> Vec<(String, String, String)> {
        parse_at(file_path, code)
            .definitions
            .iter()
            .map(|d| {
                (
                    d.definition_type.to_string(),
                    d.name.clone(),
                    d.fqn.as_str().to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn unclaimed_yaml_emits_no_key_definitions() {
        let defs = defs_at(
            "config/default.yaml",
            "billing:\n  quota:\n    enabled: true\n",
        );
        assert!(defs.is_empty(), "{defs:?}");
    }

    #[test]
    fn ci_shapes_yield_stages_variables_and_jobs_but_not_reserved_keywords() {
        let defs = defs_at(
            ".gitlab-ci.yml",
            "stages: [lint, test]\nvariables:\n  RUNNER_LARGE_ARM: saas-linux-large-arm64\n  CARGO_HOME: .cargo\ndefault:\n  image: alpine\n.base:\n  image: alpine\nmr-title-check:\n  extends: .base\n  script: [true]\n",
        );
        assert_eq!(
            defs,
            vec![
                ("CiStage".into(), "lint".into(), "stages.lint".into()),
                ("CiStage".into(), "test".into(), "stages.test".into()),
                (
                    "CiVariable".into(),
                    "RUNNER_LARGE_ARM".into(),
                    "variables.RUNNER_LARGE_ARM".into()
                ),
                (
                    "CiVariable".into(),
                    "CARGO_HOME".into(),
                    "variables.CARGO_HOME".into()
                ),
                ("CiJob".into(), ".base".into(), ".base".into()),
                (
                    "CiJob".into(),
                    "mr-title-check".into(),
                    "mr-title-check".into()
                ),
            ]
        );
    }

    #[test]
    fn jobs_are_top_level_and_stages_are_not() {
        let result = parse_at(
            ".gitlab-ci.yml",
            "stages: [lint]\nbuild:\n  variables:\n    FOO: bar\n  script: make\n",
        );
        let top: Vec<(&str, bool)> = result
            .definitions
            .iter()
            .map(|d| (d.name.as_str(), d.is_top_level))
            .collect();
        assert_eq!(top, vec![("lint", false), ("build", true)]);
        assert!(result.definitions.iter().all(|d| d.kind == DefKind::Other));
    }

    #[test]
    fn quoted_flow_and_multi_document_root_keys_become_jobs() {
        let defs = defs_at(
            ".gitlab-ci.yml",
            "\"build:linux\":\n  script: make\nstages: test\nvariables: null\n---\n{ b: { script: y } }\n",
        );
        assert_eq!(
            defs,
            vec![
                ("CiJob".into(), "build:linux".into(), "build:linux".into()),
                ("CiJob".into(), "b".into(), "b".into()),
            ]
        );
    }

    #[test]
    fn anchor_produces_def_and_alias_produces_ref() {
        let result = parse_at(
            "test.yml",
            "defaults: &base\n  retries: 2\njob:\n  <<: *base\n",
        );
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"base"), "{names:?}");
        assert!(
            result.refs.iter().any(|r| r.name == "base"),
            "alias should produce a ref"
        );
    }
}
