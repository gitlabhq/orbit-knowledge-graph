use crate::utils::Range;
use oxc_resolver::{ResolveOptions, Resolver};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::super::types::{
    ExportedBinding, ImportedName, JsCallEdge, JsCallSite, JsCallTarget, JsResolutionMode,
    JsResolvedCallRelationship,
};
use super::super::{JsExportName, JsModuleBinding, JsModuleIndex, JsModuleRecord, WorkspaceProbe};
use super::webpack::load_project_aliases;

pub struct JsCrossFileResolver {
    import_resolver: Resolver,
    require_resolver: Resolver,
    root_dir: PathBuf,
}

const MAX_EXPORT_RESOLUTION_DEPTH: usize = 10;
type ResolvedBinding = (String, ExportedBinding);

impl JsCrossFileResolver {
    pub fn new(probe: &WorkspaceProbe) -> Self {
        // Probe canonicalized root already; reuse it so every subsystem
        // compares paths against the same absolute form.
        let root_dir = probe.root_dir().to_path_buf();
        let import_resolver = create_resolver(probe, &root_dir, JsResolutionMode::Import, vec![]);
        let require_resolver = create_resolver(probe, &root_dir, JsResolutionMode::Require, vec![]);
        Self {
            import_resolver,
            require_resolver,
            root_dir,
        }
    }

    /// Apply explicit project alias config when the repository exposes it
    /// through supported config files.
    pub fn apply_project_resolution_hints(&mut self, probe: &WorkspaceProbe) {
        let aliases = load_project_aliases(probe);
        if !aliases.is_empty() {
            self.import_resolver = create_resolver(
                probe,
                &self.root_dir,
                JsResolutionMode::Import,
                aliases.clone(),
            );
            self.require_resolver =
                create_resolver(probe, &self.root_dir, JsResolutionMode::Require, aliases);
        }
    }

    /// Resolve cross-file CALLS edges for imported function calls.
    ///
    /// For each file's `ImportedCall` edges, resolves the import specifier to a
    /// target file, finds the matching exported definition, and produces a
    /// definition-to-definition CALLS relationship across files.
    pub fn resolve_calls(
        &self,
        calls_by_file: &[(String, Vec<JsCallEdge>)],
        modules: &JsModuleIndex,
    ) -> Vec<JsResolvedCallRelationship> {
        let mut relationships = Vec::new();

        for (file_path, calls) in calls_by_file {
            let abs_path = self.root_dir.join(file_path);

            'call_loop: for call in calls {
                let JsCallTarget::ImportedCall {
                    imported_call:
                        super::super::types::JsImportedCall {
                            binding,
                            member_path,
                            invocation_kind,
                        },
                } = &call.callee;

                let resolved = match self.resolve_specifier(
                    &abs_path,
                    &binding.specifier,
                    binding.resolution_mode,
                ) {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                let resolved_path = resolved.into_path_buf();
                let relative_resolved = match resolved_path.strip_prefix(&self.root_dir) {
                    Ok(rel) => rel.to_string_lossy().to_string(),
                    Err(_) => continue,
                };

                let Some((mut final_path, mut final_binding)) =
                    self.resolve_binding(&binding.imported_name, &relative_resolved, modules)
                else {
                    continue;
                };

                for member_name in member_path {
                    let Some((next_path, next_binding)) = self.resolve_member_binding(
                        &final_path,
                        &final_binding,
                        member_name,
                        modules,
                    ) else {
                        continue 'call_loop;
                    };
                    final_path = next_path;
                    final_binding = next_binding;
                }

                if !binding_supports_invocation(&final_binding, *invocation_kind) {
                    continue;
                }
                let Some(final_range) = self.binding_definition_range(&final_binding) else {
                    continue;
                };

                relationships.push(JsResolvedCallRelationship {
                    source_path: file_path.clone(),
                    source_definition_range: match &call.caller {
                        JsCallSite::Definition { range, .. } => Some(*range),
                        JsCallSite::ModuleLevel => None,
                    },
                    target_path: final_path,
                    target_definition_range: final_range,
                });
            }
        }

        relationships
    }

    pub fn resolve_import_path(
        &self,
        from_file: &str,
        specifier: &str,
        resolution_mode: JsResolutionMode,
    ) -> Option<String> {
        self.resolve_relative_specifier(from_file, specifier, resolution_mode)
    }

    fn resolve_specifier(
        &self,
        abs_path: &Path,
        specifier: &str,
        resolution_mode: JsResolutionMode,
    ) -> Result<oxc_resolver::Resolution, oxc_resolver::ResolveError> {
        self.resolver_for_mode(resolution_mode)
            .resolve_file(abs_path, specifier)
    }

    fn resolver_for_mode(&self, resolution_mode: JsResolutionMode) -> &Resolver {
        match resolution_mode {
            JsResolutionMode::Import => &self.import_resolver,
            JsResolutionMode::Require => &self.require_resolver,
        }
    }

    fn resolve_relative_specifier(
        &self,
        from_file: &str,
        specifier: &str,
        resolution_mode: JsResolutionMode,
    ) -> Option<String> {
        let abs_path = self.root_dir.join(from_file);
        let resolved = self
            .resolve_specifier(&abs_path, specifier, resolution_mode)
            .ok()?;
        let resolved_path = resolved.into_path_buf();
        let relative = resolved_path.strip_prefix(&self.root_dir).ok()?;
        Some(relative.to_string_lossy().to_string())
    }

    fn resolve_reexport(
        &self,
        source: &str,
        imported_name: &ImportedName,
        from_file: &str,
        modules: &JsModuleIndex,
        depth: usize,
    ) -> Option<ResolvedBinding> {
        if depth > MAX_EXPORT_RESOLUTION_DEPTH {
            return None;
        }

        let rel = self.resolve_relative_specifier(from_file, source, JsResolutionMode::Import)?;
        self.resolve_binding_with_depth(imported_name, &rel, modules, depth + 1)
    }

    fn resolve_star_export(
        &self,
        name: &str,
        current_file: &str,
        modules: &JsModuleIndex,
        visited: &mut HashSet<String>,
        depth: usize,
    ) -> Option<ResolvedBinding> {
        if depth > MAX_EXPORT_RESOLUTION_DEPTH || !visited.insert(current_file.to_string()) {
            return None;
        }

        let module = modules.module_for_path(current_file)?;
        if let Some(binding) = module.bindings.get(&JsExportName::Named(name.to_string())) {
            return self.follow_binding_target(
                current_file,
                binding,
                ImportedName::Named(name.to_string()),
                modules,
                depth,
            );
        }

        let mut resolved = None;
        for star_source in &module.star_reexports {
            if let Some(resolved_path) = self.resolve_relative_specifier(
                current_file,
                &star_source.specifier,
                JsResolutionMode::Import,
            ) && let Some(result) =
                self.resolve_star_export(name, &resolved_path, modules, visited, depth + 1)
            {
                match &resolved {
                    Some(existing) if *existing != result => return None,
                    Some(_) => {}
                    None => resolved = Some(result),
                }
            }
        }

        resolved
    }

    fn resolve_binding(
        &self,
        imported_name: &ImportedName,
        module_path: &str,
        modules: &JsModuleIndex,
    ) -> Option<ResolvedBinding> {
        self.resolve_binding_with_depth(imported_name, module_path, modules, 0)
    }

    fn resolve_binding_with_depth(
        &self,
        imported_name: &ImportedName,
        module_path: &str,
        modules: &JsModuleIndex,
        depth: usize,
    ) -> Option<ResolvedBinding> {
        if depth > MAX_EXPORT_RESOLUTION_DEPTH {
            return None;
        }

        let target_module = modules.module_for_path(module_path)?;
        if let Some(binding) = module_binding(target_module, imported_name) {
            return self.follow_binding_target(
                module_path,
                binding,
                imported_name.clone(),
                modules,
                depth,
            );
        }

        if let ImportedName::Named(name) = imported_name
            && let Some(result) =
                self.resolve_star_export(name, module_path, modules, &mut HashSet::default(), depth)
        {
            return Some(result);
        }
        None
    }

    fn resolve_member_binding(
        &self,
        module_path: &str,
        binding: &ExportedBinding,
        member_name: &str,
        modules: &JsModuleIndex,
    ) -> Option<ResolvedBinding> {
        if let Some(member_binding) = binding.member_bindings.get(member_name) {
            return self.follow_export_binding_target(
                module_path,
                member_binding,
                ImportedName::Named(member_name.to_string()),
                modules,
                0,
            );
        }

        if let Some(source) = &binding.reexport_source {
            let next_imported_name = match binding.reexport_imported_name.clone() {
                Some(ImportedName::Namespace) => ImportedName::Named(member_name.to_string()),
                Some(imported_name) => imported_name,
                None => return None,
            };
            let (resolved_path, resolved_binding) =
                self.resolve_reexport(source, &next_imported_name, module_path, modules, 0)?;

            if matches!(
                binding.reexport_imported_name,
                Some(ImportedName::Namespace)
            ) {
                return Some((resolved_path, resolved_binding));
            }

            return self.resolve_member_binding(
                &resolved_path,
                &resolved_binding,
                member_name,
                modules,
            );
        }

        None
    }

    fn follow_binding_target(
        &self,
        module_path: &str,
        binding: &JsModuleBinding,
        fallback_imported_name: ImportedName,
        modules: &JsModuleIndex,
        depth: usize,
    ) -> Option<ResolvedBinding> {
        self.follow_export_binding_target(
            module_path,
            &binding.binding,
            fallback_imported_name,
            modules,
            depth,
        )
    }

    fn follow_export_binding_target(
        &self,
        module_path: &str,
        binding: &ExportedBinding,
        fallback_imported_name: ImportedName,
        modules: &JsModuleIndex,
        depth: usize,
    ) -> Option<ResolvedBinding> {
        if let Some(source) = &binding.reexport_source {
            if matches!(
                binding.reexport_imported_name,
                Some(ImportedName::Namespace)
            ) {
                return Some((module_path.to_string(), binding.clone()));
            }
            let imported_name = binding
                .reexport_imported_name
                .clone()
                .unwrap_or(fallback_imported_name);
            return self
                .resolve_reexport(source, &imported_name, module_path, modules, depth)
                .or_else(|| Some((module_path.to_string(), binding.clone())));
        }

        Some((module_path.to_string(), binding.clone()))
    }

    fn binding_definition_range(&self, binding: &ExportedBinding) -> Option<Range> {
        binding.definition_range
    }
}

fn binding_supports_invocation(
    binding: &ExportedBinding,
    invocation_kind: super::super::types::JsInvocationKind,
) -> bool {
    binding
        .invocation_support
        .is_some_and(|support| support.supports(invocation_kind))
}

fn module_binding<'a>(
    module: &'a JsModuleRecord,
    imported_name: &ImportedName,
) -> Option<&'a JsModuleBinding> {
    match imported_name {
        ImportedName::Named(name) => module.bindings.get(&JsExportName::Named(name.clone())),
        ImportedName::Default => module.bindings.get(&JsExportName::Primary),
        ImportedName::Namespace => None,
    }
}

fn create_resolver(
    probe: &WorkspaceProbe,
    root_dir: &Path,
    resolution_mode: JsResolutionMode,
    aliases: Vec<(String, Vec<oxc_resolver::AliasValue>)>,
) -> Resolver {
    Resolver::new(base_resolve_options(
        probe,
        root_dir,
        resolution_mode,
        aliases,
    ))
}

fn base_resolve_options(
    probe: &WorkspaceProbe,
    root_dir: &Path,
    resolution_mode: JsResolutionMode,
    alias: Vec<(String, Vec<oxc_resolver::AliasValue>)>,
) -> ResolveOptions {
    let tsconfig = probe.tsconfig_discovery();
    let has_tsconfig = probe.has_tsconfig();

    let preferred = if probe.is_bun() {
        super::super::constants::RESOLVER_EXTENSIONS_BUN
    } else {
        super::super::constants::RESOLVER_EXTENSIONS
    };
    let extensions: Vec<String> = preferred.iter().map(|ext| format!(".{ext}")).collect();

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

    let condition_names = match resolution_mode {
        JsResolutionMode::Import => vec!["node".to_string(), "import".to_string()],
        JsResolutionMode::Require => vec!["node".to_string(), "require".to_string()],
    };

    // Bound every resolution to the repo clone. `Restriction::Path`
    // in oxc_resolver is stricter than "contained": it only matches
    // the exact restriction path or `./`. Use the function form to
    // check containment ourselves.
    let root_owned = root_dir.to_path_buf();
    let restrictions = vec![oxc_resolver::Restriction::Fn(std::sync::Arc::new(
        move |path: &Path| path.starts_with(&root_owned),
    ))];

    ResolveOptions {
        extensions,
        main_fields: vec!["module".to_string(), "main".to_string()],
        condition_names,
        extension_alias,
        tsconfig,
        alias,
        restrictions,
        ..ResolveOptions::default()
    }
}
