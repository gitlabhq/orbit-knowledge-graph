//! Each YAML config in `document_types/` declares how a named
//! document type matches files, which shapes become definitions, and
//! which keys become imports.

use std::sync::LazyLock;

use rust_embed::Embed;
use serde::Deserialize;

use super::{N, PAIR_KINDS, child_mapping, child_sequence, item_scalar, pair_key, scalar_text};
use crate::v2::types::{
    CanonicalDefinition, CanonicalImport, DefKind, Fqn, ImportBindingKind, ImportMode,
};
use treesitter_visit::Axis::*;
use treesitter_visit::Match::*;

#[derive(Embed)]
#[folder = "src/v2/langs/generic/yaml/document_types"]
struct ConfigFiles;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DocumentType {
    name: String,
    #[serde(rename = "match")]
    matcher: Matcher,
    #[serde(default)]
    keywords: Vec<String>,
    #[serde(default)]
    definitions: Vec<DefinitionRule>,
    #[serde(default)]
    imports: Vec<KeyRule>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DefinitionRule {
    #[serde(rename = "type")]
    definition_type: String,
    #[serde(flatten)]
    shape: Shape,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Shape {
    ChildrenOf(String),
    ItemsOf(String),
    RootKeys(bool),
    ValueOf(String),
    AllKeys(bool),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Matcher {
    #[serde(default)]
    filename_suffixes: Vec<String>,
    #[serde(default)]
    filename_prefixes: Vec<String>,
    #[serde(default)]
    document_keys: std::collections::BTreeMap<String, KeyCondition>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyCondition {
    #[serde(rename = "in", default)]
    any_of: Vec<String>,
    starts_with: Option<String>,
}

impl KeyCondition {
    fn holds(&self, value: &str) -> bool {
        (self.any_of.is_empty() || self.any_of.iter().any(|v| v == value))
            && self
                .starts_with
                .as_deref()
                .is_none_or(|prefix| value.starts_with(prefix))
    }
}

fn filename(file_path: &str) -> &str {
    file_path.rsplit('/').next().unwrap_or(file_path)
}

fn filename_matches(file_path: &str, suffix: &str) -> bool {
    let filename = filename(file_path);
    filename == suffix || (suffix.starts_with('.') && filename.ends_with(suffix))
}

impl Matcher {
    fn matches(&self, node: &N<'_>, file_path: &str) -> bool {
        if self
            .filename_suffixes
            .iter()
            .any(|suffix| filename_matches(file_path, suffix))
            || self
                .filename_prefixes
                .iter()
                .any(|prefix| filename(file_path).starts_with(prefix.as_str()))
        {
            return true;
        }
        if self.document_keys.is_empty() {
            return false;
        }
        let Some(mapping) = node
            .find(Ancestor, Kind("document"))
            .and_then(|document| document.find(Child, Kind("block_node")))
            .and_then(|block| block.find(Child, Kind("block_mapping")))
        else {
            return false;
        };
        self.document_keys.iter().all(|(key, condition)| {
            mapping
                .children()
                .filter(|c| PAIR_KINDS.contains(&c.kind().as_ref()))
                .find(|pair| pair_key(pair).as_deref() == Some(key.as_str()))
                .and_then(|pair| pair.field("value").as_ref().and_then(scalar_text))
                .is_some_and(|value| condition.holds(&value))
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyRule {
    key: String,
    #[serde(default)]
    also_under: Vec<String>,
    scalar_type: Option<String>,
    #[serde(default)]
    mapping_forms: Vec<MappingForm>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MappingForm {
    #[serde(rename = "type")]
    import_type: String,
    path_key: String,
    #[serde(default)]
    name_keys: Vec<String>,
    alias_key: Option<String>,
    version_split: Option<char>,
}

const CONFIG_SCHEMA: &str = include_str!(concat!(
    env!("SCHEMA_DIR"),
    "/yaml_document_type.schema.json"
));

fn config_validator() -> jsonschema::Validator {
    let schema: serde_json::Value =
        serde_json::from_str(CONFIG_SCHEMA).expect("yaml_document_type.schema.json must be JSON");
    jsonschema::validator_for(&schema).expect("yaml_document_type.schema.json must be a schema")
}

static DOCUMENT_TYPES: LazyLock<Vec<DocumentType>> = LazyLock::new(|| {
    let validator = config_validator();
    ConfigFiles::iter()
        .map(|path| {
            let file = ConfigFiles::get(&path).expect("iterated embedded file must exist");
            let document: serde_json::Value = orbit_utils::yaml::from_slice(&file.data)
                .unwrap_or_else(|e| panic!("document-type config {path} must parse: {e}"));
            let errors: Vec<String> = validator
                .iter_errors(&document)
                .map(|e| format!("{}: {e}", e.instance_path()))
                .collect();
            assert!(
                errors.is_empty(),
                "document-type config {path} violates yaml_document_type.schema.json: {}",
                errors.join("; ")
            );
            let doc_type: DocumentType = serde_json::from_value(document)
                .unwrap_or_else(|e| panic!("document-type config {path} must deserialize: {e}"));
            let stem = path.trim_end_matches(".yaml").trim_end_matches(".yml");
            assert_eq!(
                doc_type.name, stem,
                "document-type config {path} must be named after its `name`"
            );
            doc_type
        })
        .collect()
});

fn node_range(node: &N<'_>) -> crate::v2::types::Range {
    crate::v2::dsl::utils::canonical_range(&crate::utils::node_to_range(node))
}

fn push_import(
    imports: &mut Vec<CanonicalImport>,
    import_type: &'static str,
    path: String,
    name: Option<String>,
    alias: Option<String>,
    range: crate::v2::types::Range,
) {
    imports.push(CanonicalImport {
        import_type,
        binding_kind: ImportBindingKind::SideEffect,
        mode: ImportMode::Declarative,
        path,
        name,
        alias,
        scope_fqn: None,
        range,
        is_type_only: false,
        wildcard: false,
    });
}

fn value_list(value: Option<&N<'_>>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    if let Some(single) = scalar_text(value) {
        return vec![single];
    }
    let Some(sequence) = child_sequence(value) else {
        return Vec::new();
    };
    sequence
        .children()
        .filter_map(|item| item_scalar(&item))
        .collect()
}

fn emit_mapping(rule: &'static KeyRule, mapping: &N<'_>, imports: &mut Vec<CanonicalImport>) {
    let range = node_range(mapping);
    for form in &rule.mapping_forms {
        let mut path: Option<String> = None;
        let mut names: Vec<String> = Vec::new();
        let mut alias: Option<String> = None;

        for pair in mapping
            .children()
            .filter(|c| PAIR_KINDS.contains(&c.kind().as_ref()))
        {
            let Some(key) = pair_key(&pair) else { continue };
            let value = pair.field("value");
            if key == form.path_key {
                path = value.as_ref().and_then(scalar_text);
            } else if form.name_keys.contains(&key) {
                names.extend(value_list(value.as_ref()));
            } else if form.alias_key.as_deref() == Some(key.as_str()) {
                alias = value.as_ref().and_then(scalar_text);
            }
        }

        let Some(mut path) = path else { continue };
        if let Some(sep) = form.version_split
            && let Some((base, version)) = path.rsplit_once(sep)
        {
            alias = Some(version.to_string());
            path = base.to_string();
        }
        if names.is_empty() {
            push_import(imports, form.import_type.as_str(), path, None, alias, range);
        } else {
            for name in names {
                push_import(
                    imports,
                    form.import_type.as_str(),
                    path.clone(),
                    Some(name),
                    alias.clone(),
                    range,
                );
            }
        }
    }
}

fn enclosing_pair<'a>(node: &N<'a>) -> Option<N<'a>> {
    let mut current = node.parent();
    while let Some(parent) = current {
        if PAIR_KINDS.contains(&parent.kind().as_ref()) {
            return Some(parent);
        }
        current = parent.parent();
    }
    None
}

fn key_applies(rule: &KeyRule, node: &N<'_>) -> bool {
    match enclosing_pair(node) {
        Some(parent) => pair_key(&parent).is_some_and(|key| rule.also_under.contains(&key)),
        None => true,
    }
}

fn push_definition(
    defs: &mut Vec<CanonicalDefinition>,
    rule: &'static DefinitionRule,
    parts: &[&str],
    node: &N<'_>,
    sep: &'static str,
) {
    defs.push(CanonicalDefinition {
        definition_type: rule.definition_type.as_str(),
        kind: DefKind::Other,
        name: parts[parts.len() - 1].to_string(),
        fqn: Fqn::from_parts(parts, sep),
        range: node_range(node),
        is_top_level: parts.len() == 1,
        metadata: None,
    });
}

fn emit_shape(
    doc_type: &'static DocumentType,
    rule: &'static DefinitionRule,
    key: &str,
    pair: &N<'_>,
    defs: &mut Vec<CanonicalDefinition>,
    sep: &'static str,
) -> bool {
    match &rule.shape {
        Shape::RootKeys(enabled) => {
            if !enabled || doc_type.keywords.iter().any(|k| k == key) {
                return false;
            }
            push_definition(defs, rule, &[key], pair, sep);
            true
        }
        Shape::ChildrenOf(parent) => {
            if parent != key {
                return false;
            }
            let Some(mapping) = pair.field("value").as_ref().and_then(child_mapping) else {
                return true;
            };
            for child in mapping
                .children()
                .filter(|c| PAIR_KINDS.contains(&c.kind().as_ref()))
            {
                if let Some(name) = pair_key(&child) {
                    push_definition(defs, rule, &[key, &name], &child, sep);
                }
            }
            true
        }
        Shape::ItemsOf(parent) => {
            if parent != key {
                return false;
            }
            let Some(sequence) = pair.field("value").as_ref().and_then(child_sequence) else {
                return true;
            };
            for item in sequence.children() {
                if let Some(name) = item_scalar(&item) {
                    push_definition(defs, rule, &[key, &name], &item, sep);
                }
            }
            true
        }
        Shape::ValueOf(path) => {
            let mut segments = path.split('.');
            if segments.next() != Some(key) {
                return false;
            }
            let mut current = pair.clone();
            for segment in segments {
                let Some(next) = current
                    .field("value")
                    .as_ref()
                    .and_then(child_mapping)
                    .and_then(|mapping| {
                        mapping
                            .children()
                            .filter(|c| PAIR_KINDS.contains(&c.kind().as_ref()))
                            .find(|c| pair_key(c).as_deref() == Some(segment))
                    })
                else {
                    return true;
                };
                current = next;
            }
            if let Some(name) = current.field("value").as_ref().and_then(scalar_text) {
                let document = pair.find(Ancestor, Kind("document"));
                push_definition(defs, rule, &[&name], document.as_ref().unwrap_or(pair), sep);
            }
            true
        }
        Shape::AllKeys(enabled) => {
            if !enabled {
                return false;
            }
            emit_key_tree(rule, pair, &mut vec![key.to_string()], defs, sep);
            true
        }
    }
}

fn emit_key_tree(
    rule: &'static DefinitionRule,
    pair: &N<'_>,
    path: &mut Vec<String>,
    defs: &mut Vec<CanonicalDefinition>,
    sep: &'static str,
) {
    let parts: Vec<&str> = path.iter().map(String::as_str).collect();
    push_definition(defs, rule, &parts, pair, sep);
    let Some(value) = pair.field("value") else {
        return;
    };
    let nested: Vec<N<'_>> = if let Some(mapping) = child_mapping(&value) {
        mapping.children().collect()
    } else if let Some(sequence) = child_sequence(&value) {
        sequence
            .children()
            .filter_map(|item| child_mapping(&item))
            .flat_map(|mapping| mapping.children().collect::<Vec<_>>())
            .collect()
    } else {
        return;
    };
    for child in nested
        .iter()
        .filter(|c| PAIR_KINDS.contains(&c.kind().as_ref()))
    {
        if let Some(name) = pair_key(child) {
            path.push(name);
            emit_key_tree(rule, child, path, defs, sep);
            path.pop();
        }
    }
}

pub(super) fn extract_definitions(
    node: &N<'_>,
    file_path: &str,
    defs: &mut Vec<CanonicalDefinition>,
    _scope_stack: &[std::sync::Arc<str>],
    sep: &'static str,
) -> bool {
    if !PAIR_KINDS.contains(&node.kind().as_ref()) || enclosing_pair(node).is_some() {
        return false;
    }
    let Some(key) = pair_key(node) else {
        return false;
    };
    let document_types: &'static [DocumentType] = &DOCUMENT_TYPES;
    document_types.iter().any(|doc_type| {
        !doc_type.definitions.is_empty()
            && doc_type.matcher.matches(node, file_path)
            && doc_type
                .definitions
                .iter()
                .any(|rule| emit_shape(doc_type, rule, &key, node, defs, sep))
    });
    false
}

fn extract_with_rule(
    rule: &'static KeyRule,
    node: &N<'_>,
    imports: &mut Vec<CanonicalImport>,
) -> bool {
    if !key_applies(rule, node) {
        return false;
    }
    let Some(value) = node.field("value") else {
        return true;
    };

    if let Some(scalar) = scalar_text(&value) {
        if let Some(scalar_type) = &rule.scalar_type {
            push_import(
                imports,
                scalar_type.as_str(),
                scalar,
                None,
                None,
                node_range(&value),
            );
        }
    } else if let Some(mapping) = child_mapping(&value) {
        emit_mapping(rule, &mapping, imports);
    } else if let Some(sequence) = child_sequence(&value) {
        for item in sequence.children() {
            if let Some(scalar) = item_scalar(&item) {
                if let Some(scalar_type) = &rule.scalar_type {
                    push_import(
                        imports,
                        scalar_type.as_str(),
                        scalar,
                        None,
                        None,
                        node_range(&item),
                    );
                }
            } else if let Some(mapping) = child_mapping(&item) {
                emit_mapping(rule, &mapping, imports);
            }
        }
    }
    true
}

pub(super) fn extract_imports(
    node: &N<'_>,
    file_path: &str,
    imports: &mut Vec<CanonicalImport>,
) -> bool {
    if node.kind().as_ref() != "block_mapping_pair" {
        return false;
    }
    let Some(key) = pair_key(node) else {
        return false;
    };
    let document_types: &'static [DocumentType] = &DOCUMENT_TYPES;
    document_types.iter().any(|doc_type| {
        doc_type
            .imports
            .iter()
            .filter(|rule| rule.key == key)
            .any(|rule| {
                doc_type.matcher.matches(node, file_path) && extract_with_rule(rule, node, imports)
            })
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::parse_at;

    fn parse(code: &str) -> crate::v2::dsl::engine::ParseFullResult {
        parse_at(".gitlab-ci.yml", code)
    }

    #[test]
    fn embedded_configs_parse() {
        assert!(!super::DOCUMENT_TYPES.is_empty());
    }

    fn schema_errors(config: &str) -> Vec<String> {
        let document: serde_json::Value =
            orbit_utils::yaml::from_str(config).expect("config parses");
        super::config_validator()
            .iter_errors(&document)
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn schema_accepts_a_minimal_config() {
        let errors = schema_errors(
            "name: helm_chart\nmatch:\n  filename_suffixes: [Chart.yaml]\nimports:\n  - key: dependencies\n    mapping_forms:\n      - type: HelmChartDependency\n        path_key: name\n",
        );
        assert!(errors.is_empty(), "{errors:?}");
    }

    #[test]
    fn schema_accepts_each_definition_shape() {
        for shape in [
            "children_of: variables",
            "items_of: stages",
            "root_keys: true",
        ] {
            let errors = schema_errors(&format!(
                "name: x\nmatch:\n  filename_suffixes: [x.yaml]\ndefinitions:\n  - type: Thing\n    {shape}\n"
            ));
            assert!(errors.is_empty(), "{shape}: {errors:?}");
        }
    }

    #[test]
    fn schema_rejects_invalid_configs() {
        for config in [
            "name: x\nmatch: {}\nimports:\n  - key: include\n    scalar_type: CiLocalInclude\n",
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\n",
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\ndefinitions:\n  - children_of: a\n",
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\ndefinitions:\n  - type: Thing\n",
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\ndefinitions:\n  - type: Thing\n    children_of: a\n    items_of: b\n",
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\ndefinitions:\n  - type: Thing\n    root_keys: [a]\n",
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\nkeywords: []\ndefinitions:\n  - type: Thing\n    root_keys: true\n",
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\nimports:\n  - key: include\n",
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\nimports:\n  - key: include\n    scalar_type: CiLocalInclude\n    typo_key: true\n",
        ] {
            assert!(!schema_errors(config).is_empty(), "{config}");
        }
    }

    #[test]
    fn include_bare_string_and_flow_sequence_are_local_includes() {
        let result = parse("include: '/ci/build.yml'\n");
        assert_eq!(result.imports.len(), 1, "{:?}", result.imports);
        assert_eq!(result.imports[0].import_type, "CiLocalInclude");
        assert_eq!(result.imports[0].path, "/ci/build.yml");

        let seq = parse("include: ['a.yml', 'b.yml']\n");
        let paths: Vec<&str> = seq.imports.iter().map(|i| i.path.as_str()).collect();
        assert_eq!(paths, vec!["a.yml", "b.yml"], "{:?}", seq.imports);
    }

    #[test]
    fn include_project_with_files_and_ref() {
        let result = parse(
            "include:\n  - project: gitlab-org/pipeline-common\n    ref: main\n    file:\n      - /templates/a.yml\n      - /templates/b.yml\n",
        );
        let includes: Vec<_> = result
            .imports
            .iter()
            .filter(|i| i.import_type == "CiProjectInclude")
            .collect();
        assert_eq!(includes.len(), 2, "{:?}", result.imports);
        assert!(
            includes
                .iter()
                .all(|i| i.path == "gitlab-org/pipeline-common")
        );
        assert!(
            includes
                .iter()
                .any(|i| i.name.as_deref() == Some("/templates/a.yml"))
        );
        assert!(includes.iter().all(|i| i.alias.as_deref() == Some("main")));
    }

    #[test]
    fn include_single_mapping_without_sequence() {
        let result = parse("include:\n  project: gitlab-org/x\n  file: /y.yml\n");
        assert_eq!(result.imports.len(), 1, "{:?}", result.imports);
        assert_eq!(result.imports[0].path, "gitlab-org/x");
        assert_eq!(result.imports[0].name.as_deref(), Some("/y.yml"));
    }

    #[test]
    fn include_template_component_and_remote() {
        let result = parse(
            "include:\n  - template: Security/SAST.gitlab-ci.yml\n  - component: gitlab.com/comp/scan@1.2.0\n  - remote: https://example.com/ci.yml\n",
        );
        assert!(
            result
                .imports
                .iter()
                .any(|i| i.import_type == "CiTemplateInclude"
                    && i.path == "Security/SAST.gitlab-ci.yml"),
            "{:?}",
            result.imports
        );
        assert!(
            result
                .imports
                .iter()
                .any(|i| i.import_type == "CiComponentInclude"
                    && i.path == "gitlab.com/comp/scan"
                    && i.alias.as_deref() == Some("1.2.0")),
            "{:?}",
            result.imports
        );
        assert!(
            result
                .imports
                .iter()
                .any(|i| i.import_type == "CiRemoteInclude"
                    && i.path == "https://example.com/ci.yml"),
            "{:?}",
            result.imports
        );
    }

    #[test]
    fn trigger_include_produces_project_import() {
        let result = parse(
            "deploy:\n  trigger:\n    include:\n      - project: gitlab-org/child\n        file: /child.yml\n",
        );
        assert_eq!(result.imports.len(), 1, "{:?}", result.imports);
        assert_eq!(result.imports[0].import_type, "CiProjectInclude");
        assert_eq!(result.imports[0].path, "gitlab-org/child");
        assert_eq!(result.imports[0].name.as_deref(), Some("/child.yml"));
    }

    #[test]
    fn include_key_outside_ci_position_is_ordinary_yaml() {
        let result = parse("job:\n  include: something\nvalues:\n  include: [a, b]\n");
        assert!(result.imports.is_empty(), "{:?}", result.imports);
    }

    #[test]
    fn include_in_non_ci_named_file_is_ordinary_yaml() {
        let result = parse_at("values.yml", "include: '/ci/build.yml'\n");
        assert!(result.imports.is_empty(), "{:?}", result.imports);
    }

    #[test]
    fn nested_ci_config_filename_is_recognized() {
        let result = parse_at("ci/child.gitlab-ci.yml", "include: 'x.yml'\n");
        assert_eq!(result.imports.len(), 1, "{:?}", result.imports);
    }

    #[test]
    fn argo_application_name_is_a_definition_spanning_the_document() {
        let result = parse_at(
            "apps/gkg.yaml",
            "apiVersion: argoproj.io/v1alpha1\nkind: Application\nmetadata:\n  name: gkg\n  namespace: argocd\nspec:\n  source:\n    repoURL: https://example.com/x.git\n",
        );
        let apps: Vec<_> = result
            .definitions
            .iter()
            .filter(|d| d.definition_type == "ArgoCdApplication")
            .collect();
        assert_eq!(apps.len(), 1, "{:?}", result.definitions);
        assert_eq!(apps[0].fqn.as_str(), "gkg");
        assert_eq!(apps[0].range.start.line, 0);
        assert!(apps[0].range.end.line >= 7, "{:?}", apps[0].range);
    }

    #[test]
    fn helm_chart_name_is_a_definition() {
        let result = parse_at("Chart.yaml", "apiVersion: v2\nname: gkg\nversion: 0.1.0\n");
        let names: Vec<(&str, &str)> = result
            .definitions
            .iter()
            .map(|d| (d.definition_type, d.fqn.as_str()))
            .collect();
        assert_eq!(names, vec![("HelmChart", "gkg")]);
    }

    #[test]
    fn helm_values_index_every_nested_key() {
        for path in ["chart/values.yaml", "env/prd/values-vault-secrets.yaml"] {
            let result = parse_at(
                path,
                "image:\n  repository: registry/gkg\n  tag: \"1.0\"\nindexer:\n  resources:\n    limits:\n      memory: 2Gi\n  env:\n    - name: RUST_LOG\n      value: info\n",
            );
            let fqns: Vec<&str> = result.definitions.iter().map(|d| d.fqn.as_str()).collect();
            assert_eq!(
                fqns,
                vec![
                    "image",
                    "image.repository",
                    "image.tag",
                    "indexer",
                    "indexer.resources",
                    "indexer.resources.limits",
                    "indexer.resources.limits.memory",
                    "indexer.env",
                    "indexer.env.name",
                    "indexer.env.value",
                ],
                "{path}"
            );
            assert!(
                result
                    .definitions
                    .iter()
                    .all(|d| d.definition_type == "HelmValue")
            );
            assert!(result.definitions[0].is_top_level);
            assert!(!result.definitions[1].is_top_level);
        }
    }

    #[test]
    fn compose_services_volumes_and_networks_are_definitions() {
        for path in [
            "docker-compose.yml",
            "compose.yaml",
            "docker-compose.override.yml",
        ] {
            let result = parse_at(
                path,
                "services:\n  postgres:\n    image: postgres:16\n  redis:\n    image: redis\nvolumes:\n  pgdata: {}\nnetworks:\n  backend: {}\n",
            );
            let defs: Vec<(&str, &str)> = result
                .definitions
                .iter()
                .map(|d| (d.definition_type, d.fqn.as_str()))
                .collect();
            assert_eq!(
                defs,
                vec![
                    ("ComposeService", "services.postgres"),
                    ("ComposeService", "services.redis"),
                    ("ComposeVolume", "volumes.pgdata"),
                    ("ComposeNetwork", "networks.backend"),
                ],
                "{path}"
            );
        }
    }

    #[test]
    fn value_of_missing_path_yields_nothing() {
        let result = parse_at(
            "app.yaml",
            "apiVersion: argoproj.io/v1alpha1\nkind: Application\nmetadata:\n  namespace: argocd\n",
        );
        assert!(result.definitions.is_empty(), "{:?}", result.definitions);
    }

    #[test]
    fn argo_sources_produce_imports_with_path_name_and_revision() {
        let result = parse_at(
            "apps/gkg.yaml",
            "apiVersion: argoproj.io/v1alpha1\nkind: Application\nmetadata:\n  name: gkg\nspec:\n  source:\n    repoURL: https://gitlab.com/gitlab-com/gl-infra/charts.git\n    path: gkg\n    targetRevision: main\n",
        );
        assert_eq!(result.imports.len(), 1, "{:?}", result.imports);
        let import = &result.imports[0];
        assert_eq!(import.import_type, "ArgoCdSource");
        assert_eq!(
            import.path,
            "https://gitlab.com/gitlab-com/gl-infra/charts.git"
        );
        assert_eq!(import.name.as_deref(), Some("gkg"));
        assert_eq!(import.alias.as_deref(), Some("main"));

        let multi = parse_at(
            "app.yaml",
            "apiVersion: argoproj.io/v1alpha1\nkind: Application\nspec:\n  sources:\n    - repoURL: https://gitlab.com/a/charts.git\n      path: app\n      targetRevision: v1.2.3\n    - repoURL: https://charts.example.com\n      chart: redis\n",
        );
        assert_eq!(multi.imports.len(), 2, "{:?}", multi.imports);
        assert!(
            multi
                .imports
                .iter()
                .any(|i| i.path == "https://gitlab.com/a/charts.git"
                    && i.alias.as_deref() == Some("v1.2.3"))
        );
        assert!(
            multi
                .imports
                .iter()
                .any(|i| i.path == "https://charts.example.com"
                    && i.name.as_deref() == Some("redis")),
            "helm repo source must use chart as the name"
        );

        let appset = parse_at(
            "appset.yaml",
            "apiVersion: argoproj.io/v1alpha1\nkind: ApplicationSet\nspec:\n  template:\n    spec:\n      source:\n        repoURL: https://gitlab.com/a/charts.git\n        path: app\n",
        );
        assert_eq!(appset.imports.len(), 1, "{:?}", appset.imports);
    }

    #[test]
    fn source_outside_argo_manifest_is_ordinary_yaml() {
        let result = parse_at(
            "config.yaml",
            "spec:\n  source:\n    repoURL: https://example.com/x.git\n",
        );
        assert!(result.imports.is_empty(), "{:?}", result.imports);
    }

    #[test]
    fn helm_chart_dependencies_produce_imports() {
        let result = parse_at(
            "charts/gitlab/Chart.yaml",
            "apiVersion: v2\nname: gitlab\ndependencies:\n  - name: redis\n    version: ~17.0.0\n    repository: https://charts.bitnami.com/bitnami\n  - name: postgresql\n    version: 12.x.x\n    repository: https://charts.example.com\n    condition: postgresql.install\n",
        );
        let deps: Vec<_> = result
            .imports
            .iter()
            .filter(|i| i.import_type == "HelmChartDependency")
            .collect();
        assert_eq!(deps.len(), 2, "{:?}", result.imports);
        assert!(deps.iter().any(|i| i.path == "redis"
            && i.alias.as_deref() == Some("~17.0.0")
            && i.name.as_deref() == Some("https://charts.bitnami.com/bitnami")));
        assert!(
            deps.iter()
                .any(|i| i.path == "postgresql" && i.alias.as_deref() == Some("12.x.x"))
        );
    }

    #[test]
    fn dependencies_key_outside_chart_file_is_ordinary_yaml() {
        let result = parse_at(
            "values.yaml",
            "dependencies:\n  - name: redis\n    version: 1.0.0\n",
        );
        assert!(result.imports.is_empty(), "{:?}", result.imports);
    }

    #[test]
    fn chart_without_dependencies_produces_no_imports() {
        let result = parse_at(
            "Chart.yaml",
            "apiVersion: v2\nname: gkg\ndescription: GitLab Orbit - indexer, webserver, and data pipeline\ntype: application\nversion: 0.1.0\n",
        );
        assert!(result.imports.is_empty(), "{:?}", result.imports);
    }

    #[test]
    fn non_argo_application_kind_is_ignored() {
        let result = parse_at(
            "app.yaml",
            "apiVersion: app.k8s.io/v1beta1\nkind: Application\nspec:\n  source:\n    repoURL: https://example.com/x.git\n",
        );
        assert!(result.imports.is_empty(), "{:?}", result.imports);
    }
}
