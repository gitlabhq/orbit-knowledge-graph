use crate::v2::config::Language;
use crate::v2::dsl::types::*;
use crate::v2::types::{DefKind, Fqn};
use treesitter_visit::extract::{child_of_kind, field};
use treesitter_visit::tree_sitter::StrDoc;
use treesitter_visit::{Node, SupportLang};

use crate::v2::linker::rules::{ReceiverMode, ResolutionRules};
use crate::v2::linker::{HasRules, ResolveSettings};

type N<'a> = Node<'a, StrDoc<SupportLang>>;

// Key depth is deliberately unbounded: depth is a poor proxy for
// machine-generated YAML (openapi dumps nest at 3-6 while hand-written
// helm values legitimately reach 7+), so a depth cap silently truncates
// exactly the config surface this language exists to index. Pathological
// files are excluded whole at the file level instead — see
// YAML_PARSER_MAX_FILE_SIZE in v2/pipeline.rs.
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
mod tests {
    use super::*;
    use crate::v2::trace::Tracer;

    fn parse(code: &str) -> crate::v2::dsl::engine::ParseFullResult {
        YamlDsl::spec()
            .parse_full_collect(
                code.as_bytes(),
                "test.yml",
                Language::Yaml,
                &Tracer::new(false),
                Default::default(),
            )
            .unwrap()
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

    #[test]
    fn yaml_files_produce_no_imports() {
        let result = parse("include: '/ci/build.yml'\njob:\n  script: make\n");
        assert!(result.imports.is_empty(), "{:?}", result.imports);
    }
}
