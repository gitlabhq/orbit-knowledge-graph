//! Each YAML config in `document_types/` declares how a named
//! document type matches files and which keys become imports.

use std::sync::LazyLock;

use rust_embed::Embed;
use serde::Deserialize;

use super::{N, PAIR_KINDS, child_mapping, child_sequence, item_scalar, pair_key, scalar_text};
use crate::v2::types::{CanonicalImport, ImportBindingKind, ImportMode};
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
    imports: Vec<KeyRule>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Matcher {
    #[serde(default)]
    filename_suffixes: Vec<String>,
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

fn filename_matches(file_path: &str, suffix: &str) -> bool {
    let filename = file_path.rsplit('/').next().unwrap_or(file_path);
    filename == suffix || (suffix.starts_with('.') && filename.ends_with(suffix))
}

impl Matcher {
    fn matches(&self, node: &N<'_>, file_path: &str) -> bool {
        if self
            .filename_suffixes
            .iter()
            .any(|suffix| filename_matches(file_path, suffix))
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

fn key_applies(rule: &KeyRule, node: &N<'_>) -> bool {
    let mut current = node.parent();
    while let Some(parent) = current {
        if PAIR_KINDS.contains(&parent.kind().as_ref()) {
            return pair_key(&parent).is_some_and(|key| rule.also_under.contains(&key));
        }
        current = parent.parent();
    }
    true
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
    fn schema_rejects_a_matcher_without_criteria() {
        let errors = schema_errors(
            "name: x\nmatch: {}\nimports:\n  - key: include\n    scalar_type: CiLocalInclude\n",
        );
        assert!(!errors.is_empty());
    }

    #[test]
    fn schema_rejects_a_rule_that_emits_nothing() {
        let errors = schema_errors(
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\nimports:\n  - key: include\n",
        );
        assert!(!errors.is_empty());
    }

    #[test]
    fn schema_rejects_unknown_keys() {
        let errors = schema_errors(
            "name: x\nmatch:\n  filename_suffixes: [x.yaml]\nimports:\n  - key: include\n    scalar_type: CiLocalInclude\n    typo_key: true\n",
        );
        assert!(!errors.is_empty());
    }

    #[test]
    fn include_bare_string_is_a_local_include() {
        let result = parse("include: '/ci/build.yml'\n");
        assert_eq!(result.imports.len(), 1, "{:?}", result.imports);
        assert_eq!(result.imports[0].import_type, "CiLocalInclude");
        assert_eq!(result.imports[0].path, "/ci/build.yml");
    }

    #[test]
    fn include_flow_sequence_of_locals() {
        let result = parse("include: ['a.yml', 'b.yml']\n");
        let paths: Vec<&str> = result.imports.iter().map(|i| i.path.as_str()).collect();
        assert_eq!(paths, vec!["a.yml", "b.yml"], "{:?}", result.imports);
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
    fn argo_application_source_produces_import() {
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
    }

    #[test]
    fn argo_multi_source_produces_one_import_per_source() {
        let result = parse_at(
            "app.yaml",
            "apiVersion: argoproj.io/v1alpha1\nkind: Application\nspec:\n  sources:\n    - repoURL: https://gitlab.com/a/charts.git\n      path: app\n      targetRevision: v1.2.3\n    - repoURL: https://gitlab.com/b/values.git\n      ref: values\n",
        );
        assert_eq!(result.imports.len(), 2, "{:?}", result.imports);
        assert!(
            result
                .imports
                .iter()
                .any(|i| i.path == "https://gitlab.com/a/charts.git"
                    && i.alias.as_deref() == Some("v1.2.3"))
        );
        assert!(
            result
                .imports
                .iter()
                .any(|i| i.path == "https://gitlab.com/b/values.git" && i.name.is_none())
        );
    }

    #[test]
    fn argo_helm_repo_source_uses_chart_as_name() {
        let result = parse_at(
            "app.yaml",
            "apiVersion: argoproj.io/v1alpha1\nkind: Application\nspec:\n  source:\n    repoURL: https://charts.example.com\n    chart: redis\n    targetRevision: 17.0.0\n",
        );
        assert_eq!(result.imports.len(), 1, "{:?}", result.imports);
        assert_eq!(result.imports[0].name.as_deref(), Some("redis"));
    }

    #[test]
    fn argo_application_set_template_source_produces_import() {
        let result = parse_at(
            "appset.yaml",
            "apiVersion: argoproj.io/v1alpha1\nkind: ApplicationSet\nspec:\n  template:\n    spec:\n      source:\n        repoURL: https://gitlab.com/a/charts.git\n        path: app\n",
        );
        assert_eq!(result.imports.len(), 1, "{:?}", result.imports);
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
