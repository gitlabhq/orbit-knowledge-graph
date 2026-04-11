use crate::analysis::types::ConsolidatedRelationship;
use crate::graph::{RelationshipKind, RelationshipType};
use internment::ArcIntern;
use oxc_resolver::{ResolveOptions, Resolver, TsconfigDiscovery};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use super::js_types::{ExportedBinding, ImportedName, JsModuleInfo};

/// Cross-file resolver using oxc_resolver.
pub struct JsCrossFileResolver {
    resolver: Resolver,
    root_dir: PathBuf,
}

impl JsCrossFileResolver {
    pub fn new(root_dir: PathBuf, is_bun: bool, has_tsconfig: bool) -> Self {
        let resolver = create_resolver(is_bun, has_tsconfig);
        Self { resolver, root_dir }
    }

    /// Resolve all cross-file edges for a set of analyzed modules.
    /// Returns new relationships to add to the graph.
    pub fn resolve(
        &self,
        modules: &HashMap<String, JsModuleInfo>,
    ) -> Vec<ConsolidatedRelationship> {
        let mut relationships = Vec::new();

        for (file_path, module_info) in modules {
            let abs_path = self.root_dir.join(file_path);
            // resolve() takes a directory, not a file path
            let abs_dir = abs_path.parent().unwrap_or(&self.root_dir);

            for import_entry in &module_info.imports {
                let resolved = self.resolver.resolve(abs_dir, &import_entry.specifier);

                let resolved_path = match resolved {
                    Ok(resolution) => resolution.into_path_buf(),
                    Err(_) => continue, // external dep or unresolvable, skip silently
                };

                // Convert resolved absolute path to relative
                let relative_resolved = match resolved_path.strip_prefix(&self.root_dir) {
                    Ok(rel) => rel.to_string_lossy().to_string(),
                    Err(_) => continue, // outside repo, skip
                };

                let target_module = match modules.get(&relative_resolved) {
                    Some(m) => m,
                    None => continue, // file not in our analysis set
                };

                // Match imported binding to target's exports
                let target_binding = match &import_entry.imported_name {
                    ImportedName::Named(name) => target_module.exports.get(name),
                    ImportedName::Default => target_module.exports.values().find(|b| b.is_default),
                    ImportedName::Namespace => None, // links to all exports, handled separately
                };

                if let Some(binding) = target_binding {
                    let source_path = ArcIntern::new(file_path.clone());
                    let target_path = ArcIntern::new(relative_resolved.clone());

                    // ImportedSymbol -> Definition edge
                    relationships.push(ConsolidatedRelationship {
                        source_path: Some(source_path),
                        target_path: Some(target_path),
                        kind: RelationshipKind::ImportedSymbolToDefinition,
                        relationship_type: RelationshipType::ImportedSymbolToDefinition,
                        source_range: ArcIntern::new(import_entry.range),
                        target_range: ArcIntern::new(binding.range),
                        ..Default::default()
                    });
                }

                // If target has star exports, follow the chain
                if target_binding.is_none()
                    && let ImportedName::Named(name) = &import_entry.imported_name
                    && let Some(binding) = self.resolve_star_export(
                        name,
                        &relative_resolved,
                        modules,
                        &mut HashSet::new(),
                        0,
                    )
                {
                    let source_path = ArcIntern::new(file_path.clone());
                    let target_path = ArcIntern::new(binding.0);

                    relationships.push(ConsolidatedRelationship {
                        source_path: Some(source_path),
                        target_path: Some(target_path),
                        kind: RelationshipKind::ImportedSymbolToDefinition,
                        relationship_type: RelationshipType::ImportedSymbolToDefinition,
                        source_range: ArcIntern::new(import_entry.range),
                        target_range: ArcIntern::new(binding.1.range),
                        ..Default::default()
                    });
                }
            }
        }

        relationships
    }

    /// Follow star re-export chains to find a named export.
    /// Returns (file_path, ExportedBinding) if found.
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

        // Check direct exports first
        if let Some(binding) = module.exports.get(name) {
            return Some((current_file.to_string(), binding.clone()));
        }

        // Follow star re-exports
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
}

fn create_resolver(is_bun: bool, has_tsconfig: bool) -> Resolver {
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

    Resolver::new(ResolveOptions {
        extensions,
        main_fields: vec!["module".to_string(), "main".to_string()],
        condition_names: vec!["module".to_string(), "import".to_string()],
        extension_alias,
        tsconfig,
        ..ResolveOptions::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_resolver_default() {
        let resolver = create_resolver(false, false);
        // Just verify it doesn't panic
        let _ = resolver;
    }

    #[test]
    fn test_create_resolver_bun() {
        let resolver = create_resolver(true, false);
        let _ = resolver;
    }

    #[test]
    fn test_create_resolver_with_tsconfig() {
        let resolver = create_resolver(false, true);
        let _ = resolver;
    }

    #[test]
    fn test_resolve_star_export_cycle_detection() {
        let mut modules = HashMap::new();

        // Create a cycle: a.ts re-exports from b.ts, b.ts re-exports from a.ts
        modules.insert(
            "a.ts".to_string(),
            JsModuleInfo {
                exports: HashMap::new(),
                imports: vec![],
                star_export_sources: vec!["./b".to_string()],
                cjs_exports: vec![],
                has_module_syntax: true,
                definition_fqns: HashMap::new(),
            },
        );
        modules.insert(
            "b.ts".to_string(),
            JsModuleInfo {
                exports: HashMap::new(),
                imports: vec![],
                star_export_sources: vec!["./a".to_string()],
                cjs_exports: vec![],
                has_module_syntax: true,
                definition_fqns: HashMap::new(),
            },
        );

        let resolver = JsCrossFileResolver::new(PathBuf::from("/tmp/test"), false, false);
        let result = resolver.resolve_star_export("foo", "a.ts", &modules, &mut HashSet::new(), 0);

        // Should return None without infinite loop
        assert!(result.is_none());
    }
}
