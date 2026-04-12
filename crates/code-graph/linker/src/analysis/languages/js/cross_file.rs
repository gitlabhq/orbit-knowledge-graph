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

    /// Infer aliases from the project's import patterns and rebuild the
    /// resolver if any are found. Call this after collecting all modules
    /// but before resolving imports/calls.
    pub fn apply_inferred_aliases(
        &mut self,
        is_bun: bool,
        has_tsconfig: bool,
        modules: &HashMap<String, JsModuleInfo>,
    ) {
        let aliases = infer_aliases_from_imports(&self.root_dir, modules);
        if !aliases.is_empty() {
            self.resolver =
                create_resolver_with_aliases(is_bun, has_tsconfig, &self.root_dir, aliases);
        }
    }

    pub fn resolve(
        &self,
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Vec<ConsolidatedRelationship> {
        let mut relationships = Vec::new();

        for (file_path, module_info) in modules {
            let abs_path = self.root_dir.join(file_path);

            for import_entry in &module_info.imports {
                let resolved = self
                    .resolver
                    .resolve_file(&abs_path, &import_entry.specifier);

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

            for call in calls {
                let JsCallTarget::ImportedCall {
                    specifier,
                    imported_name,
                    ..
                } = &call.callee
                else {
                    continue;
                };

                let resolved = match self.resolver.resolve_file(&abs_path, specifier) {
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

        let abs_file = self.root_dir.join(from_file);
        let resolution = self.resolver.resolve_file(&abs_file, source).ok()?;
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
            let abs_file = self.root_dir.join(current_file);
            if let Ok(resolution) = self.resolver.resolve_file(&abs_file, star_source) {
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

/// Discover tsconfig/jsconfig for the resolver.
///
/// oxc_resolver's `TsconfigDiscovery::Auto` only searches for `tsconfig.json`,
/// not `jsconfig.json`. Since `jsconfig.json` is structurally identical and
/// commonly used by JS-only projects (VS Code, Vite, webpack 5.105+), we
/// explicitly check for it and use Manual discovery if found.
///
/// For projects without any config, we scan for common bundler alias
/// patterns in package.json `imports` field, which is the Node.js standard
/// for subpath imports and is natively supported by oxc_resolver.
///
/// Resolution priority:
/// 1. `tsconfig.json` in root or parents -> Auto (oxc_resolver walks parent dirs)
/// 2. `jsconfig.json` in root -> Manual (oxc_resolver parses it identically)
/// 3. Neither exists -> Auto (harmless, falls through to ResolveOptions::alias)
fn discover_tsconfig(root_dir: &Path) -> TsconfigDiscovery {
    // jsconfig.json is functionally identical to tsconfig.json but Auto doesn't find it
    let jsconfig = root_dir.join("jsconfig.json");
    if jsconfig.exists() {
        return TsconfigDiscovery::Manual(oxc_resolver::TsconfigOptions {
            config_file: jsconfig,
            references: oxc_resolver::TsconfigReferences::Auto,
        });
    }
    TsconfigDiscovery::Auto
}

/// Infer module aliases from the project's actual import patterns.
///
/// When no tsconfig/jsconfig paths are available, this function examines the
/// collected module imports to discover alias prefixes (e.g., `~/`, `@/`) and
/// validates candidate directory mappings by checking whether substituting the
/// alias with the candidate directory resolves to real files.
///
/// This is import-driven, not heuristic: the evidence comes from the codebase's
/// own import statements, not from guessing directory names.
pub(super) fn infer_aliases_from_imports(
    root_dir: &Path,
    modules: &HashMap<String, JsModuleInfo>,
) -> Vec<(String, Vec<oxc_resolver::AliasValue>)> {
    // If tsconfig.json or jsconfig.json exists, aliases are handled by oxc_resolver natively
    if root_dir.join("tsconfig.json").exists() || root_dir.join("jsconfig.json").exists() {
        return vec![];
    }

    // Collect import prefixes that look like aliases (single char + /)
    let mut prefix_counts: HashMap<String, usize> = HashMap::new();
    let mut prefix_samples: HashMap<String, Vec<String>> = HashMap::new();

    for module_info in modules.values() {
        for imp in &module_info.imports {
            let spec = &imp.specifier;
            // Skip relative, absolute, and bare specifiers that look like npm packages
            if spec.starts_with('.')
                || spec.starts_with('/')
                || spec.starts_with('@')
                || !spec.contains('/')
            {
                continue;
            }

            // Extract the prefix before the first /
            if let Some(slash_pos) = spec.find('/') {
                let prefix = &spec[..slash_pos];
                // Only consider short prefixes (1-2 chars like ~, @, #)
                if prefix.len() <= 2 && !prefix.chars().all(|c| c.is_alphanumeric()) {
                    *prefix_counts.entry(prefix.to_string()).or_default() += 1;
                    let samples = prefix_samples.entry(prefix.to_string()).or_default();
                    if samples.len() < 20 {
                        samples.push(spec[slash_pos + 1..].to_string());
                    }
                }
            }
        }
    }

    let mut aliases = Vec::new();

    // Common directory candidates for JS/TS project aliases
    let candidates = [
        "app/assets/javascripts",
        "src",
        "lib",
        "app/javascript",
        "app/frontend",
        "packages",
    ];

    for (prefix, count) in &prefix_counts {
        if *count < 5 {
            continue;
        }

        let samples = match prefix_samples.get(prefix) {
            Some(s) => s,
            None => continue,
        };

        // Try each candidate directory and score by how many sample imports resolve
        let mut best: Option<(String, usize)> = None;

        for candidate in &candidates {
            let candidate_dir = root_dir.join(candidate);
            if !candidate_dir.is_dir() {
                continue;
            }

            let resolved_count = samples
                .iter()
                .filter(|sample| {
                    let path = candidate_dir.join(sample);
                    // Check with common extensions
                    path.exists()
                        || path.with_extension("js").exists()
                        || path.with_extension("ts").exists()
                        || path.with_extension("vue").exists()
                        || path.join("index.js").exists()
                        || path.join("index.ts").exists()
                })
                .count();

            if resolved_count > 0 {
                match &best {
                    Some((_, best_count)) if resolved_count > *best_count => {
                        best = Some((candidate.to_string(), resolved_count));
                    }
                    None => {
                        best = Some((candidate.to_string(), resolved_count));
                    }
                    _ => {}
                }
            }
        }

        if let Some((dir, resolved)) = best {
            // Require at least 25% of samples to resolve as evidence
            if resolved * 4 >= samples.len() {
                let target_path = root_dir.join(&dir);
                aliases.push((
                    prefix.clone(),
                    vec![oxc_resolver::AliasValue::Path(
                        target_path.to_string_lossy().to_string(),
                    )],
                ));
            }
        }
    }

    aliases
}

fn create_resolver(is_bun: bool, has_tsconfig: bool, root_dir: &Path) -> Resolver {
    let tsconfig = Some(discover_tsconfig(root_dir));

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

    Resolver::new(ResolveOptions {
        extensions,
        main_fields: vec!["module".to_string(), "main".to_string()],
        condition_names: vec!["module".to_string(), "import".to_string()],
        extension_alias,
        tsconfig,
        ..ResolveOptions::default()
    })
}

fn create_resolver_with_aliases(
    is_bun: bool,
    has_tsconfig: bool,
    root_dir: &Path,
    alias: Vec<(String, Vec<oxc_resolver::AliasValue>)>,
) -> Resolver {
    let tsconfig = Some(discover_tsconfig(root_dir));

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
