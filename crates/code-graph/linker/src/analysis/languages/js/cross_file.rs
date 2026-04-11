use crate::analysis::types::ConsolidatedRelationship;
use crate::graph::{RelationshipKind, RelationshipType};
use internment::ArcIntern;
use oxc_resolver::{ResolveOptions, Resolver, TsconfigDiscovery};
use parser_core::utils::Range;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use super::types::{
    ExportedBinding, ImportedName, JsCallEdge, JsCallSite, JsCallTarget, JsModuleInfo,
};

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

        match imported_name {
            ImportedName::Named(name) => {
                self.resolve_star_export(name, module_path, modules, &mut HashSet::new(), 0)
            }
            ImportedName::Default | ImportedName::Namespace => None,
        }
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
}
