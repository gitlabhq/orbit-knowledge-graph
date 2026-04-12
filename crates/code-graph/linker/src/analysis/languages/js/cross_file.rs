use crate::analysis::types::ConsolidatedRelationship;
use crate::graph::{RelationshipKind, RelationshipType};
use internment::ArcIntern;
use oxc_resolver::{ResolveOptions, Resolver, TsconfigDiscovery};
use parser_core::utils::Range;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::types::{
    CjsExport, ExportedBinding, ImportedName, JsCallEdge, JsCallSite, JsCallTarget, JsModuleInfo,
};

pub struct JsCrossFileResolver {
    resolver: Resolver,
    root_dir: PathBuf,
}

impl JsCrossFileResolver {
    pub fn new(root_dir: PathBuf, is_bun: bool, has_tsconfig: bool) -> Self {
        let root_dir = std::fs::canonicalize(&root_dir).unwrap_or(root_dir);
        let resolver = create_resolver(is_bun, has_tsconfig, &root_dir);
        Self { resolver, root_dir }
    }

    pub fn resolve(
        &self,
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Vec<ConsolidatedRelationship> {
        let mut relationships = Vec::new();

        for (file_path, module_info) in modules {
            let abs_path = self.root_dir.join(file_path);
            let abs_dir = abs_path.parent().unwrap_or(&self.root_dir);

            for import_entry in &module_info.imports {
                let resolved = self.resolver.resolve(abs_dir, &import_entry.specifier);

                let resolved_path = match resolved {
                    Ok(resolution) => resolution.into_path_buf(),
                    Err(_) => continue,
                };

                let relative_resolved = match resolved_path.strip_prefix(&self.root_dir) {
                    Ok(rel) => rel.to_string_lossy().to_string(),
                    Err(_) => continue,
                };

                if !modules.contains_key(&relative_resolved) {
                    continue;
                }

                let Some((final_path, final_binding)) =
                    self.resolve_binding(&import_entry.imported_name, &relative_resolved, modules)
                else {
                    continue;
                };
                let Some(definition_range) =
                    self.binding_definition_range(&final_path, &final_binding, modules)
                else {
                    continue;
                };

                let source_path = ArcIntern::new(file_path.clone());
                let target_path = ArcIntern::new(final_path);

                relationships.push(ConsolidatedRelationship {
                    source_path: Some(source_path),
                    target_path: Some(target_path),
                    kind: RelationshipKind::ImportedSymbolToDefinition,
                    relationship_type: RelationshipType::ImportedSymbolToDefinition,
                    source_range: ArcIntern::new(import_entry.range),
                    target_range: ArcIntern::new(definition_range),
                    target_definition_range: Some(ArcIntern::new(definition_range)),
                    ..Default::default()
                });
            }
        }

        relationships
    }

    /// Resolve cross-file CALLS edges for imported function calls.
    ///
    /// For each file's `ImportedCall` edges, resolves the import specifier to a
    /// target file, finds the matching exported definition, and produces a
    /// definition-to-definition CALLS relationship across files.
    pub fn resolve_calls(
        &self,
        calls_by_file: &[(String, Vec<JsCallEdge>)],
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Vec<ConsolidatedRelationship> {
        let mut relationships = Vec::new();

        for (file_path, calls) in calls_by_file {
            let abs_path = self.root_dir.join(file_path);
            let abs_dir = abs_path.parent().unwrap_or(&self.root_dir);

            for call in calls {
                let JsCallTarget::ImportedCall {
                    specifier,
                    imported_name,
                    ..
                } = &call.callee
                else {
                    continue;
                };

                let resolved = match self.resolver.resolve(abs_dir, specifier) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let resolved_path = resolved.into_path_buf();
                let relative_resolved = match resolved_path.strip_prefix(&self.root_dir) {
                    Ok(rel) => rel.to_string_lossy().to_string(),
                    Err(_) => continue,
                };

                let Some((final_path, final_binding)) =
                    self.resolve_binding(imported_name, &relative_resolved, modules)
                else {
                    continue;
                };
                let Some(final_range) =
                    self.binding_definition_range(&final_path, &final_binding, modules)
                else {
                    continue;
                };

                let source_path = ArcIntern::new(file_path.clone());
                let target_path = ArcIntern::new(final_path);

                let (caller_range, caller_def_range) = match &call.caller {
                    JsCallSite::Definition { range, .. } => {
                        (call.call_range, Some(ArcIntern::new(*range)))
                    }
                    JsCallSite::ModuleLevel => (call.call_range, None),
                };

                let rel = ConsolidatedRelationship {
                    source_path: Some(source_path),
                    target_path: Some(target_path),
                    kind: if caller_def_range.is_some() {
                        RelationshipKind::DefinitionToDefinition
                    } else {
                        RelationshipKind::FileToDefinition
                    },
                    relationship_type: RelationshipType::Calls,
                    source_range: ArcIntern::new(caller_range),
                    target_range: ArcIntern::new(final_range),
                    source_definition_range: caller_def_range,
                    target_definition_range: Some(ArcIntern::new(final_range)),
                    ..Default::default()
                };

                relationships.push(rel);
            }
        }

        relationships
    }

    fn resolve_reexport(
        &self,
        source: &str,
        name: &str,
        from_file: &str,
        modules: &HashMap<String, JsModuleInfo>,
        depth: usize,
    ) -> Option<(String, ExportedBinding)> {
        if depth > 10 {
            return None;
        }

        let abs_dir = self.root_dir.join(from_file).parent()?.to_path_buf();
        let resolution = self.resolver.resolve(&abs_dir, source).ok()?;
        let rel = resolution
            .into_path_buf()
            .strip_prefix(&self.root_dir)
            .ok()?
            .to_string_lossy()
            .to_string();
        let target_module = modules.get(&rel)?;
        let binding = target_module.exports.get(name)?;

        if let Some(ref next_source) = binding.reexport_source {
            self.resolve_reexport(
                next_source,
                binding.reexport_name.as_deref().unwrap_or(name),
                &rel,
                modules,
                depth + 1,
            )
        } else {
            Some((rel, binding.clone()))
        }
    }

    fn resolve_star_export(
        &self,
        name: &str,
        current_file: &str,
        modules: &HashMap<String, JsModuleInfo>,
        visited: &mut HashSet<String>,
        depth: usize,
    ) -> Option<(String, ExportedBinding)> {
        if depth > 10 || !visited.insert(current_file.to_string()) {
            return None;
        }

        let module = modules.get(current_file)?;

        if let Some(binding) = module.exports.get(name) {
            if let Some(ref source) = binding.reexport_source {
                return self.resolve_reexport(
                    source,
                    binding.reexport_name.as_deref().unwrap_or(name),
                    current_file,
                    modules,
                    0,
                );
            }
            return Some((current_file.to_string(), binding.clone()));
        }

        for star_source in &module.star_export_sources {
            let abs_path = self.root_dir.join(current_file);
            let abs_dir = abs_path.parent().unwrap_or(&self.root_dir);
            if let Ok(resolution) = self.resolver.resolve(abs_dir, star_source) {
                let resolved_path = resolution.into_path_buf();
                if let Ok(rel) = resolved_path.strip_prefix(&self.root_dir) {
                    let rel_str = rel.to_string_lossy().to_string();
                    if let Some(result) =
                        self.resolve_star_export(name, &rel_str, modules, visited, depth + 1)
                    {
                        return Some(result);
                    }
                }
            }
        }

        None
    }

    fn resolve_binding(
        &self,
        imported_name: &ImportedName,
        module_path: &str,
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Option<(String, ExportedBinding)> {
        let target_module = modules.get(module_path)?;
        let target_binding = match imported_name {
            ImportedName::Named(name) => target_module.exports.get(name),
            ImportedName::Default => target_module.exports.values().find(|b| b.is_default),
            ImportedName::Namespace => return None,
        };

        if let Some(binding) = target_binding {
            if let Some(ref source) = binding.reexport_source {
                let export_name = match imported_name {
                    ImportedName::Named(name) => name.as_str(),
                    ImportedName::Default => "default",
                    ImportedName::Namespace => return None,
                };

                return self
                    .resolve_reexport(
                        source,
                        binding.reexport_name.as_deref().unwrap_or(export_name),
                        module_path,
                        modules,
                        0,
                    )
                    .or_else(|| Some((module_path.to_string(), binding.clone())));
            }

            return Some((module_path.to_string(), binding.clone()));
        }

        // Fall through to star exports, then CJS exports
        if let ImportedName::Named(name) = imported_name
            && let Some(result) =
                self.resolve_star_export(name, module_path, modules, &mut HashSet::new(), 0)
        {
            return Some(result);
        }

        // CJS fallback: module.exports = ... or exports.name = ...
        let cjs_binding = match imported_name {
            ImportedName::Named(name) => target_module.cjs_exports.iter().find_map(|e| {
                if let CjsExport::Named { name: n, range, .. } = e
                    && n == name
                {
                    Some(ExportedBinding {
                        local_fqn: name.clone(),
                        range: *range,
                        definition_range: target_module.definition_fqns.get(name.as_str()).copied(),
                        is_type: false,
                        is_default: false,
                        reexport_source: None,
                        reexport_name: None,
                    })
                } else {
                    None
                }
            }),
            ImportedName::Default => target_module.cjs_exports.iter().find_map(|e| {
                if let CjsExport::Default { range } = e {
                    Some(ExportedBinding {
                        local_fqn: "default".to_string(),
                        range: *range,
                        definition_range: Some(*range),
                        is_type: false,
                        is_default: true,
                        reexport_source: None,
                        reexport_name: None,
                    })
                } else {
                    None
                }
            }),
            ImportedName::Namespace => None,
        };

        cjs_binding.map(|b| (module_path.to_string(), b))
    }

    fn binding_definition_range(
        &self,
        module_path: &str,
        binding: &ExportedBinding,
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Option<Range> {
        binding.definition_range.or_else(|| {
            modules
                .get(module_path)
                .and_then(|module| module.definition_fqns.get(&binding.local_fqn).copied())
        })
    }
}

/// Detect module aliases from the project's bundler/framework configuration.
///
/// Checks (in order):
/// 1. webpack.config.js -- parses `alias` object for simple `key: path.join(ROOT, 'dir')` patterns
/// 2. vite.config.{js,ts} -- parses `resolve.alias` for `{ find, replacement }` patterns
/// 3. Fallback heuristic -- common conventions (`~` -> `app/assets/javascripts`, `@` -> `src`)
fn detect_aliases(root_dir: &Path) -> Vec<(String, Vec<oxc_resolver::AliasValue>)> {
    let mut aliases = Vec::new();

    // Try to parse webpack config
    let webpack_path = root_dir.join("config/webpack.config.js");
    let alt_webpack_path = root_dir.join("webpack.config.js");
    let webpack_content = std::fs::read_to_string(&webpack_path)
        .or_else(|_| std::fs::read_to_string(&alt_webpack_path))
        .unwrap_or_default();

    if !webpack_content.is_empty() {
        parse_webpack_aliases(&webpack_content, root_dir, &mut aliases);
    }

    // If no aliases found from config, try heuristic detection
    if aliases.is_empty() {
        let candidates: &[(&str, &str)] =
            &[("~", "app/assets/javascripts"), ("~", "src"), ("@", "src")];
        for (prefix, target) in candidates {
            let target_path = root_dir.join(target);
            if target_path.is_dir() && !aliases.iter().any(|(p, _)| p == *prefix) {
                aliases.push((
                    prefix.to_string(),
                    vec![oxc_resolver::AliasValue::Path(
                        target_path.to_string_lossy().to_string(),
                    )],
                ));
            }
        }
    }

    aliases
}

/// Parse webpack alias entries from config source.
/// Matches patterns like: `'~': path.join(ROOT_PATH, 'app/assets/javascripts')`
fn parse_webpack_aliases(
    content: &str,
    root_dir: &Path,
    aliases: &mut Vec<(String, Vec<oxc_resolver::AliasValue>)>,
) {
    // Match: 'key': path.join(SOMETHING, 'relative/path')
    // or:   key: path.join(SOMETHING, 'relative/path')
    for line in content.lines() {
        let trimmed = line.trim();

        // Skip comments
        if trimmed.starts_with("//") || trimmed.starts_with("/*") {
            continue;
        }

        // Match alias entries: 'alias_name': path.join(..., 'target')
        // or: alias_name: path.join(..., 'target')
        if let Some(colon_pos) = trimmed.find(':') {
            let key = trimmed[..colon_pos]
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .trim_end_matches('$');

            if key.is_empty() || key.contains(' ') || key.starts_with("//") {
                continue;
            }

            let value = trimmed[colon_pos + 1..].trim();

            // path.join(SOMETHING, 'relative/dir')
            if value.contains("path.join")
                && let Some(target) = extract_path_join_target(value)
            {
                let target_path = root_dir.join(&target);
                if target_path.is_dir() || target_path.exists() {
                    aliases.push((
                        key.to_string(),
                        vec![oxc_resolver::AliasValue::Path(
                            target_path.to_string_lossy().to_string(),
                        )],
                    ));
                }
            }
        }
    }
}

/// Extract the last string argument from `path.join(ROOT, 'some/path')`.
fn extract_path_join_target(s: &str) -> Option<String> {
    // Find the last quoted string in the path.join call
    let mut last_quoted = None;
    let mut in_quote = false;
    let mut quote_char = '\'';
    let mut start = 0;

    for (i, c) in s.char_indices() {
        if !in_quote && (c == '\'' || c == '"') {
            in_quote = true;
            quote_char = c;
            start = i + 1;
        } else if in_quote && c == quote_char {
            last_quoted = Some(&s[start..i]);
            in_quote = false;
        }
    }

    last_quoted.map(|s| s.to_string())
}

fn create_resolver(is_bun: bool, has_tsconfig: bool, root_dir: &std::path::Path) -> Resolver {
    let tsconfig = if has_tsconfig {
        Some(TsconfigDiscovery::Auto)
    } else {
        None
    };

    let extensions: Vec<String> = if is_bun {
        [".tsx", ".jsx", ".ts", ".mjs", ".js", ".cjs", ".json"]
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    } else {
        [
            ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".cts", ".json",
        ]
        .iter()
        .map(|s| (*s).to_string())
        .collect()
    };

    let extension_alias = if has_tsconfig {
        vec![
            (
                ".js".to_string(),
                vec![".js".to_string(), ".ts".to_string()],
            ),
            (
                ".mjs".to_string(),
                vec![".mjs".to_string(), ".mts".to_string()],
            ),
            (
                ".cjs".to_string(),
                vec![".cjs".to_string(), ".cts".to_string()],
            ),
        ]
    } else {
        vec![]
    };

    let alias = detect_aliases(root_dir);

    Resolver::new(ResolveOptions {
        extensions,
        main_fields: vec!["module".to_string(), "main".to_string()],
        condition_names: vec!["module".to_string(), "import".to_string()],
        extension_alias,
        tsconfig,
        alias,
        ..ResolveOptions::default()
    })
}
