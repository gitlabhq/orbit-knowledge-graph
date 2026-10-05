//! Config discovery is driven entirely off the indexed file list held
//! by [`super::super::WorkspaceProbe`]: any file whose basename matches
//! `webpack.config.{js,cjs,mjs,ts}` in any folder is eligible. No
//! filesystem walking happens here.

use crate::v2::config::Role;
use orbit_utils::vfs::Vfs;
use oxc_resolver::AliasValue;
use std::path::Path;

use super::evaluator::{
    EvaluatedValue, ModuleEvalCache, contained_repo_path, evaluate_module_exports,
};

/// Stops at the first config that yields a non-empty alias table — a
/// deliberate "first win" behaviour matching the pre-split evaluator.
pub(super) fn load_project_aliases(
    probe: &super::super::WorkspaceProbe,
) -> Vec<(String, Vec<AliasValue>)> {
    let mut cache = ModuleEvalCache::default();
    probe
        .webpack_configs()
        .iter()
        .find_map(|config_path| {
            let aliases = load_webpack_aliases(&probe.vfs, config_path, &mut cache);
            (!aliases.is_empty()).then_some(aliases)
        })
        .unwrap_or_default()
}

fn load_webpack_aliases(
    repo: &Vfs<Role>,
    config_path: &Path,
    cache: &mut ModuleEvalCache,
) -> Vec<(String, Vec<AliasValue>)> {
    let Some(exports) = evaluate_module_exports(repo, config_path, cache, 0) else {
        return vec![];
    };

    let mut aliases = Vec::new();
    let config_dir = config_path.parent().unwrap_or(Path::new("/"));
    collect_aliases_from_value(&exports, repo, config_dir, &mut aliases);
    aliases.sort_by(|left, right| left.0.cmp(&right.0));
    aliases
}

/// Arrays of configs (function-factory or multi-config exports) are
/// flattened — every entry contributes.
fn collect_aliases_from_value(
    value: &EvaluatedValue,
    repo: &Vfs<Role>,
    config_dir: &Path,
    aliases: &mut Vec<(String, Vec<AliasValue>)>,
) {
    match value {
        EvaluatedValue::Object(object) => {
            if let Some(EvaluatedValue::Object(resolve)) = object.get("resolve")
                && let Some(alias_value) = resolve.get("alias")
            {
                merge_alias_entries(alias_value, repo, config_dir, aliases);
            }

            if let Some(alias_value) = object.get("alias") {
                merge_alias_entries(alias_value, repo, config_dir, aliases);
            }
        }
        EvaluatedValue::Array(items) => {
            for item in items {
                collect_aliases_from_value(item, repo, config_dir, aliases);
            }
        }
        _ => {}
    }
}

fn merge_alias_entries(
    value: &EvaluatedValue,
    repo: &Vfs<Role>,
    config_dir: &Path,
    aliases: &mut Vec<(String, Vec<AliasValue>)>,
) {
    let EvaluatedValue::Object(object) = value else {
        return;
    };

    for (alias_key, alias_value) in object {
        let resolved_values = alias_values_from_evaluated(alias_value, repo, config_dir);
        if resolved_values.is_empty() {
            continue;
        }
        aliases.push((alias_key.clone(), resolved_values));
    }
}

fn alias_values_from_evaluated(
    value: &EvaluatedValue,
    repo: &Vfs<Role>,
    config_dir: &Path,
) -> Vec<AliasValue> {
    match value {
        EvaluatedValue::String(path) => {
            if Path::new(path).is_absolute() || path.starts_with('.') {
                contained_repo_path(repo, config_dir, path)
                    .map(|resolved| vec![AliasValue::Path(resolved.to_string_lossy().to_string())])
                    .unwrap_or_default()
            } else if is_safe_package_specifier(path) {
                vec![AliasValue::Path(path.clone())]
            } else {
                vec![]
            }
        }
        EvaluatedValue::Bool(false) => vec![AliasValue::Ignore],
        EvaluatedValue::Array(values) => values
            .iter()
            .flat_map(|value| alias_values_from_evaluated(value, repo, config_dir))
            .collect(),
        _ => vec![],
    }
}

/// ASCII-only, no path separators, no protocol prefix: matches `lodash`,
/// `@scope/pkg/sub` and rejects anything that could smuggle in a node
/// built-in (`node:fs`) or filesystem escape.
fn is_safe_package_specifier(value: &str) -> bool {
    if value.is_empty() || value.contains(':') || value.contains('\\') {
        return false;
    }
    if value.starts_with('/') || value.starts_with('.') {
        return false;
    }
    value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '@' | '/' | '.'))
}
