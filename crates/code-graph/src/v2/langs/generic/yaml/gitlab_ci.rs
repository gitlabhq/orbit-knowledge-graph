//! GitLab CI documents: `include:` statements become imports.

use super::{N, PAIR_KINDS, child_mapping, child_sequence, item_scalar, pair_key, scalar_text};
use crate::v2::types::{CanonicalImport, ImportBindingKind, ImportMode};

pub(super) fn is_ci_config_path(file_path: &str) -> bool {
    file_path.ends_with(".gitlab-ci.yml") || file_path.ends_with(".gitlab-ci.yaml")
}

enum IncludePosition {
    /// Document top level, or child-pipeline config under `trigger:`.
    Include,
    Other,
}

fn include_position(node: &N<'_>) -> IncludePosition {
    let mut current = node.parent();
    while let Some(parent) = current {
        if PAIR_KINDS.contains(&parent.kind().as_ref()) {
            return if pair_key(&parent).as_deref() == Some("trigger") {
                IncludePosition::Include
            } else {
                IncludePosition::Other
            };
        }
        current = parent.parent();
    }
    IncludePosition::Include
}

fn push_include(
    imports: &mut Vec<CanonicalImport>,
    import_type: &'static str,
    path: String,
    name: Option<String>,
    alias: Option<String>,
) {
    imports.push(CanonicalImport {
        import_type,
        binding_kind: ImportBindingKind::SideEffect,
        mode: ImportMode::Declarative,
        path,
        name,
        alias,
        scope_fqn: None,
        range: crate::v2::types::Range::empty(),
        is_type_only: false,
        wildcard: false,
    });
}

fn file_list(value: Option<&N<'_>>) -> Vec<String> {
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

fn emit_include_spec(mapping: &N<'_>, imports: &mut Vec<CanonicalImport>) {
    let mut project: Option<String> = None;
    let mut ref_name: Option<String> = None;
    let mut files: Vec<String> = Vec::new();

    for pair in mapping
        .children()
        .filter(|c| PAIR_KINDS.contains(&c.kind().as_ref()))
    {
        let Some(key) = pair_key(&pair) else { continue };
        let value = pair.field("value");
        match key.as_str() {
            "project" => project = value.as_ref().and_then(scalar_text),
            "ref" => ref_name = value.as_ref().and_then(scalar_text),
            "file" | "files" => files.extend(file_list(value.as_ref())),
            "local" | "template" | "remote" => {
                if let Some(path) = value.as_ref().and_then(scalar_text) {
                    let import_type = match key.as_str() {
                        "local" => "CiLocalInclude",
                        "template" => "CiTemplateInclude",
                        _ => "CiRemoteInclude",
                    };
                    push_include(imports, import_type, path, None, None);
                }
            }
            "component" => {
                if let Some(spec) = value.as_ref().and_then(scalar_text) {
                    let (path, version) = match spec.rsplit_once('@') {
                        Some((path, version)) => (path.to_string(), Some(version.to_string())),
                        None => (spec, None),
                    };
                    push_include(imports, "CiComponentInclude", path, None, version);
                }
            }
            _ => {}
        }
    }

    if let Some(project) = project {
        if files.is_empty() {
            push_include(imports, "CiProjectInclude", project, None, ref_name);
        } else {
            for file in files {
                push_include(
                    imports,
                    "CiProjectInclude",
                    project.clone(),
                    Some(file),
                    ref_name.clone(),
                );
            }
        }
    }
}

pub(super) fn extract_ci_includes(node: &N<'_>, imports: &mut Vec<CanonicalImport>) -> bool {
    if node.kind().as_ref() != "block_mapping_pair" {
        return false;
    }
    if pair_key(node).as_deref() != Some("include") {
        return false;
    }
    if matches!(include_position(node), IncludePosition::Other) {
        return false;
    }
    let Some(value) = node.field("value") else {
        return true;
    };

    if let Some(local) = scalar_text(&value) {
        push_include(imports, "CiLocalInclude", local, None, None);
    } else if let Some(mapping) = child_mapping(&value) {
        emit_include_spec(&mapping, imports);
    } else if let Some(sequence) = child_sequence(&value) {
        for item in sequence.children() {
            if let Some(local) = item_scalar(&item) {
                push_include(imports, "CiLocalInclude", local, None, None);
            } else if let Some(mapping) = child_mapping(&item) {
                emit_include_spec(&mapping, imports);
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::super::tests::parse_at;

    fn parse(code: &str) -> crate::v2::dsl::engine::ParseFullResult {
        parse_at(".gitlab-ci.yml", code)
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
}
