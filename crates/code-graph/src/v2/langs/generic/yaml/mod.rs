//! YAML as a graph language: mapping keys become `MappingKey`
//! definitions with dotted FQNs, anchors become definitions, aliases
//! become references. Filename-gated document types live in submodules
//! ([`gitlab_ci`] handles `*.gitlab-ci.yml`).

mod gitlab_ci;

use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::types::{DefKind, Fqn};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;
use treesitter_visit::extract::{child_of_kind, field};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::linker::rules::{ReceiverMode, ResolutionRules};
use crate::v2::linker::{HasRules, ResolveSettings};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

// Key depth is deliberately unbounded: hand-written helm values reach
// depth 7+, so capping truncates real config. Pathological files are
// bounded whole via YAML_PARSER_MAX_FILE_SIZE in v2/pipeline.rs.
pub(super) const PAIR_KINDS: &[&str] = &["block_mapping_pair", "flow_pair"];

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
            scope("block_mapping_pair", "MappingKey")
                .def_kind(DefKind::Property)
                .name_from(field("key")),
            scope("flow_pair", "MappingKey")
                .def_kind(DefKind::Property)
                .name_from(field("key")),
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
            on_scope: Some(yaml_strip_key_quotes),
            on_import: Some(gitlab_ci::extract_ci_includes),
            on_import_file_filter: Some(gitlab_ci::is_ci_config_path),
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

#[expect(
    clippy::ptr_arg,
    reason = "signature is dictated by ScopeHookFn, which passes &mut Vec"
)]
fn yaml_strip_key_quotes(
    node: &N<'_>,
    defs: &mut Vec<crate::v2::types::CanonicalDefinition>,
    scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> bool {
    if !PAIR_KINDS.contains(&node.kind().as_ref()) {
        return false;
    }
    if let Some(last) = defs.last_mut()
        && last.definition_type == "MappingKey"
        && (last.name.contains('"')
            || last.name.contains('\'')
            || scope_stack
                .iter()
                .any(|s| s.contains('"') || s.contains('\'')))
    {
        let stripped = strip_quotes(&last.name).to_string();
        let enclosing = scope_stack.len().saturating_sub(1);
        let mut parts: Vec<&str> = scope_stack[..enclosing]
            .iter()
            .map(|s| strip_quotes(s.as_ref()))
            .collect();
        parts.push(stripped.as_str());
        last.fqn = Fqn::from_parts(&parts, sep);
        last.name = stripped;
    }
    false
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

pub(super) fn child_mapping<'a>(node: &N<'a>) -> Option<N<'a>> {
    node.find(Child, AnyKind(&["block_mapping", "flow_mapping"]))
        .or_else(|| {
            node.find(Child, AnyKind(&["block_node", "flow_node"]))
                .and_then(|wrapper| {
                    wrapper.find(Child, AnyKind(&["block_mapping", "flow_mapping"]))
                })
        })
}

pub(super) fn child_sequence<'a>(node: &N<'a>) -> Option<N<'a>> {
    node.find(Child, AnyKind(&["block_sequence", "flow_sequence"]))
        .or_else(|| {
            node.find(Child, AnyKind(&["block_node", "flow_node"]))
                .and_then(|wrapper| {
                    wrapper.find(Child, AnyKind(&["block_sequence", "flow_sequence"]))
                })
        })
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

    pub(in super::super) fn parse(code: &str) -> crate::v2::dsl::engine::ParseFullResult {
        parse_at("test.yml", code)
    }

    fn defs(code: &str) -> Vec<(String, String)> {
        parse(code)
            .definitions
            .iter()
            .map(|d| (d.name.clone(), d.fqn.as_str().to_string()))
            .collect()
    }

    #[test]
    fn nested_mapping_keys_produce_dotted_fqns() {
        let defs = defs("billing:\n  quota:\n    enabled: true\n");
        assert!(
            defs.contains(&("billing".into(), "billing".into())),
            "{defs:?}"
        );
        assert!(
            defs.contains(&("quota".into(), "billing.quota".into())),
            "{defs:?}"
        );
        assert!(
            defs.contains(&("enabled".into(), "billing.quota.enabled".into())),
            "{defs:?}"
        );
    }

    #[test]
    fn keys_nested_in_sequences_attach_to_parent_key() {
        let defs = defs("containers:\n  - name: web\n    image: nginx\n");
        assert!(
            defs.contains(&("name".into(), "containers.name".into())),
            "{defs:?}"
        );
        assert!(
            defs.contains(&("image".into(), "containers.image".into())),
            "{defs:?}"
        );
    }

    #[test]
    fn flow_mapping_keys_produce_defs() {
        let defs = defs("job: { stage: test, when: manual }\n");
        assert!(
            defs.contains(&("stage".into(), "job.stage".into())),
            "{defs:?}"
        );
        assert!(
            defs.contains(&("when".into(), "job.when".into())),
            "{defs:?}"
        );
    }

    #[test]
    fn quoted_keys_are_stripped() {
        let defs = defs("\"rules\":\n  'when': manual\n");
        assert!(defs.contains(&("rules".into(), "rules".into())), "{defs:?}");
        assert!(
            defs.contains(&("when".into(), "rules.when".into())),
            "{defs:?}"
        );
    }

    #[test]
    fn unquoted_child_under_quoted_parent_gets_clean_fqn() {
        let defs = defs("\"rules\":\n  when: manual\n");
        assert!(
            defs.contains(&("when".into(), "rules.when".into())),
            "{defs:?}"
        );
    }

    #[test]
    fn deeply_nested_keys_keep_full_fqns() {
        let code = "a:\n b:\n  c:\n   d:\n    e:\n     f:\n      g:\n       h:\n        i:\n         j: 1\n";
        let all = defs(code);
        assert!(
            all.contains(&("j".into(), "a.b.c.d.e.f.g.h.i.j".into())),
            "{all:?}"
        );
    }

    #[test]
    fn anchor_produces_def_and_alias_produces_ref() {
        let result = parse("defaults: &base\n  retries: 2\njob:\n  <<: *base\n");
        let names: Vec<&str> = result.definitions.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"base"), "{names:?}");
        assert!(
            result.refs.iter().any(|r| r.name == "base"),
            "alias should produce a ref"
        );
    }

    #[test]
    fn multi_document_streams_parse_cleanly() {
        let defs = defs("a: 1\n---\nb:\n  c: 2\n");
        assert!(defs.contains(&("a".into(), "a".into())), "{defs:?}");
        assert!(defs.contains(&("c".into(), "b.c".into())), "{defs:?}");
    }
}
