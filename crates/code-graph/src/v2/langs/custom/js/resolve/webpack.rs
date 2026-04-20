//! Webpack-specific alias harvest.
//!
//! Everything webpack-aware lives here. The [`super::evaluator`] module
//! is a generic JS config interpreter; this file owns the knowledge of
//! what a webpack config looks like (`{resolve: {alias: ...}}` or a
//! bare `alias` field), turns its shape into oxc_resolver alias
//! entries, and feeds them back to the specifier resolver.
//!
//! Config discovery is driven entirely off the indexed file list held
//! by [`super::super::WorkspaceProbe`]: any file whose basename matches
//! `webpack.config.{js,cjs,mjs,ts}` in any folder is eligible. No
//! filesystem walking happens here.

use oxc_resolver::AliasValue;
use std::path::Path;

use super::evaluator::{
    EvaluatedValue, ModuleEvalCache, contained_repo_path, evaluate_module_exports,
};

/// Gather resolver aliases from every webpack config the probe found.
/// Stops at the first config that yields a non-empty alias table — a
/// deliberate "first win" behaviour matching the pre-split evaluator.
pub(super) fn load_project_aliases(
    probe: &super::super::WorkspaceProbe,
) -> Vec<(String, Vec<AliasValue>)> {
    let root_dir = probe.root_dir();
    let mut cache = ModuleEvalCache::default();
    probe
        .webpack_configs()
        .iter()
        .find_map(|config_path| {
            let aliases = load_webpack_aliases(root_dir, config_path, &mut cache);
            (!aliases.is_empty()).then_some(aliases)
        })
        .unwrap_or_default()
}

fn load_webpack_aliases(
    root_dir: &Path,
    config_path: &Path,
    cache: &mut ModuleEvalCache,
) -> Vec<(String, Vec<AliasValue>)> {
    let Some(exports) = evaluate_module_exports(root_dir, config_path, cache, 0) else {
        return vec![];
    };

    let mut aliases = Vec::new();
    let config_dir = config_path.parent().unwrap_or(root_dir);
    collect_aliases_from_value(&exports, root_dir, config_dir, &mut aliases);
    aliases.sort_by(|left, right| left.0.cmp(&right.0));
    aliases
}

/// Walk an evaluated webpack config and pull alias maps out of
/// `{resolve: {alias: ...}}` or a bare top-level `alias` field.
/// Arrays of configs (function-factory or multi-config exports) are
/// flattened — every entry contributes.
fn collect_aliases_from_value(
    value: &EvaluatedValue,
    root_dir: &Path,
    config_dir: &Path,
    aliases: &mut Vec<(String, Vec<AliasValue>)>,
) {
    match value {
        EvaluatedValue::Object(object) => {
            if let Some(EvaluatedValue::Object(resolve)) = object.get("resolve")
                && let Some(alias_value) = resolve.get("alias")
            {
                merge_alias_entries(alias_value, root_dir, config_dir, aliases);
            }

            if let Some(alias_value) = object.get("alias") {
                merge_alias_entries(alias_value, root_dir, config_dir, aliases);
            }
        }
        EvaluatedValue::Array(items) => {
            for item in items {
                collect_aliases_from_value(item, root_dir, config_dir, aliases);
            }
        }
        _ => {}
    }
}

fn merge_alias_entries(
    value: &EvaluatedValue,
    root_dir: &Path,
    config_dir: &Path,
    aliases: &mut Vec<(String, Vec<AliasValue>)>,
) {
    let EvaluatedValue::Object(object) = value else {
        return;
    };

    for (alias_key, alias_value) in object {
        let resolved_values = alias_values_from_evaluated(alias_value, root_dir, config_dir);
        if resolved_values.is_empty() {
            continue;
        }
        aliases.push((alias_key.clone(), resolved_values));
    }
}

fn alias_values_from_evaluated(
    value: &EvaluatedValue,
    root_dir: &Path,
    config_dir: &Path,
) -> Vec<AliasValue> {
    match value {
        EvaluatedValue::String(path) => {
            if Path::new(path).is_absolute() || path.starts_with('.') {
                contained_repo_path(root_dir, config_dir, path)
                    .map(|resolved| vec![AliasValue::Path(resolved.to_string_lossy().to_string())])
                    .unwrap_or_default()
            } else {
                vec![AliasValue::Path(path.clone())]
            }
        }
        EvaluatedValue::Bool(false) => vec![AliasValue::Ignore],
        EvaluatedValue::Array(values) => values
            .iter()
            .flat_map(|value| alias_values_from_evaluated(value, root_dir, config_dir))
            .collect(),
        _ => vec![],
    }
}
