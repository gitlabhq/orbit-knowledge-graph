use crate::analysis::types::ConsolidatedRelationship;
use crate::graph::{RelationshipKind, RelationshipType};
use internment::ArcIntern;
use oxc_resolver::{ResolveOptions, Resolver, TsconfigDiscovery};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use super::types::{ExportedBinding, ImportedName, JsModuleInfo};

pub struct JsCrossFileResolver {
    resolver: Resolver,
    root_dir: PathBuf,
}

impl JsCrossFileResolver {
    pub fn new(root_dir: PathBuf, is_bun: bool, has_tsconfig: bool) -> Self {
        let resolver = create_resolver(is_bun, has_tsconfig);
        let root_dir = std::fs::canonicalize(&root_dir).unwrap_or(root_dir);
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

                let target_module = match modules.get(&relative_resolved) {
                    Some(m) => m,
                    None => continue,
                };

                let target_binding = match &import_entry.imported_name {
                    ImportedName::Named(name) => target_module.exports.get(name),
                    ImportedName::Default => target_module.exports.values().find(|b| b.is_default),
                    ImportedName::Namespace => None,
                };

                if let Some(binding) = target_binding {
                    let (final_path, final_binding) =
                        if let Some(ref source) = binding.reexport_source {
                            let name = match &import_entry.imported_name {
                                ImportedName::Named(n) => n.as_str(),
                                ImportedName::Default => "default",
                                _ => continue,
                            };
                            match self.resolve_reexport(
                                source,
                                binding.reexport_name.as_deref().unwrap_or(name),
                                &relative_resolved,
                                modules,
                                0,
                            ) {
                                Some(resolved) => resolved,
                                None => (relative_resolved.clone(), binding.clone()),
                            }
                        } else {
                            (relative_resolved.clone(), binding.clone())
                        };

                    let source_path = ArcIntern::new(file_path.clone());
                    let target_path = ArcIntern::new(final_path);

                    relationships.push(ConsolidatedRelationship {
                        source_path: Some(source_path),
                        target_path: Some(target_path),
                        kind: RelationshipKind::ImportedSymbolToDefinition,
                        relationship_type: RelationshipType::ImportedSymbolToDefinition,
                        source_range: ArcIntern::new(import_entry.range),
                        target_range: ArcIntern::new(final_binding.range),
                        ..Default::default()
                    });
                }

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
    use parser_core::utils::Range;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_create_resolver_default() {
        let resolver = create_resolver(false, false);
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

        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_named_import_across_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("a.ts"), "export const foo = 1;\n").unwrap();
        fs::write(
            dir.path().join("b.ts"),
            "import { foo } from './a';\nconsole.log(foo);\n",
        )
        .unwrap();

        let resolver = JsCrossFileResolver::new(dir.path().to_path_buf(), false, false);
        let modules = HashMap::from([
            (
                "a.ts".to_string(),
                JsModuleInfo {
                    exports: HashMap::from([(
                        "foo".to_string(),
                        ExportedBinding {
                            local_fqn: "foo".to_string(),
                            range: Range::empty(),
                            is_type: false,
                            is_default: false,
                            reexport_source: None,
                            reexport_name: None,
                        },
                    )]),
                    ..Default::default()
                },
            ),
            (
                "b.ts".to_string(),
                JsModuleInfo {
                    imports: vec![super::super::types::OwnedImportEntry {
                        specifier: "./a".to_string(),
                        imported_name: ImportedName::Named("foo".to_string()),
                        local_name: "foo".to_string(),
                        is_type: false,
                        range: Range::empty(),
                    }],
                    ..Default::default()
                },
            ),
        ]);

        let relationships = resolver.resolve(&modules);
        assert_eq!(relationships.len(), 1);
        assert_eq!(
            relationships[0]
                .source_path
                .as_ref()
                .map(|path| path.as_str()),
            Some("b.ts")
        );
        assert_eq!(
            relationships[0]
                .target_path
                .as_ref()
                .map(|path| path.as_str()),
            Some("a.ts")
        );
        assert_eq!(
            relationships[0].relationship_type,
            RelationshipType::ImportedSymbolToDefinition
        );
    }
}
