use crate::utils::Range;
use crate::v2::config::{CodeFilter, Role, detect_language_from_path};
use orbit_utils::vfs::{Kind, Limits, Options, Vfs, sources::Memory};
use oxc_resolver::{FileMetadata, FileSystem, FileSystemOs, ResolveOptions, ResolverGeneric};
use rayon::prelude::*;
use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use super::super::types::{
    ExportedBinding, ImportedName, JsCallEdge, JsCallSite, JsCallTarget, JsResolutionMode,
    JsResolvedCallRelationship,
};
use super::super::{JsExportName, JsModuleBinding, JsModuleIndex, JsModuleRecord, WorkspaceProbe};
use super::webpack::load_project_aliases;

pub struct JsCrossFileResolver {
    import_resolver: ResolverGeneric<RepoFileSystem>,
    require_resolver: ResolverGeneric<RepoFileSystem>,
}

const MAX_EXPORT_RESOLUTION_DEPTH: usize = 10;
const MAX_RESOLVER_READ_BYTES: u64 = 512 * 1024;
type ResolvedBinding = (String, ExportedBinding);

struct RepoFileSystem {
    vfs: Arc<Vfs<Role>>,
}

impl RepoFileSystem {
    fn new(vfs: Arc<Vfs<Role>>) -> Self {
        Self { vfs }
    }

    fn check_read_size(&self, path: &Path) -> io::Result<()> {
        let len = self.vfs.stat(path)?.len;
        if len > MAX_RESOLVER_READ_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "resolver metadata file too large: {} bytes, max {MAX_RESOLVER_READ_BYTES}",
                    len
                ),
            ));
        }
        Ok(())
    }
}

impl FileSystem for RepoFileSystem {
    fn new() -> Self {
        Self {
            vfs: Arc::new(
                Vfs::load(
                    Memory(Vec::new()),
                    CodeFilter::new(None, None, detect_language_from_path),
                    Limits::default(),
                    Options::default(),
                )
                .expect("empty repository"),
            ),
        }
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.check_read_size(path)?;
        self.vfs.read(path).map(|bytes| bytes.to_vec())
    }

    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        FileSystemOs::validate_string(self.read(path)?)
    }

    fn metadata(&self, path: &Path) -> io::Result<FileMetadata> {
        let stat = self.vfs.stat(path)?;
        Ok(FileMetadata::new(
            stat.kind == Kind::File,
            stat.kind == Kind::Dir,
            false,
        ))
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<FileMetadata> {
        let stat = self.vfs.stat(path)?;
        let link = stat.link.is_some();
        Ok(FileMetadata::new(
            !link && stat.kind == Kind::File,
            !link && stat.kind == Kind::Dir,
            link,
        ))
    }

    fn read_link(&self, path: &Path) -> Result<PathBuf, oxc_resolver::ResolveError> {
        self.vfs
            .stat(path)?
            .link
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "not a symlink").into())
    }

    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        self.vfs.stat(path).map(|stat| stat.path)
    }
}

impl JsCrossFileResolver {
    pub fn new(probe: &WorkspaceProbe) -> Self {
        let import_resolver = create_resolver(probe, JsResolutionMode::Import, vec![]);
        let require_resolver = create_resolver(probe, JsResolutionMode::Require, vec![]);
        Self {
            import_resolver,
            require_resolver,
        }
    }

    pub fn new_with_hints(probe: &WorkspaceProbe) -> Self {
        let mut resolver = Self::new(probe);
        resolver.apply_project_resolution_hints(probe);
        resolver
    }

    pub fn apply_project_resolution_hints(&mut self, probe: &WorkspaceProbe) {
        let aliases = load_project_aliases(probe);
        if !aliases.is_empty() {
            self.import_resolver =
                create_resolver(probe, JsResolutionMode::Import, aliases.clone());
            self.require_resolver = create_resolver(probe, JsResolutionMode::Require, aliases);
        }
    }

    /// Files are resolved in parallel via rayon; calls within a single file
    /// are processed sequentially.
    pub fn resolve_calls(
        &self,
        calls_by_file: &[(String, Vec<JsCallEdge>)],
        modules: &JsModuleIndex,
        deadline: &Option<Instant>,
    ) -> Vec<JsResolvedCallRelationship> {
        let timed_out = AtomicBool::new(false);
        let results = calls_by_file
            .par_iter()
            .flat_map_iter(|(file_path, calls)| {
                if timed_out.load(Ordering::Relaxed)
                    || deadline.is_some_and(|d| Instant::now() >= d)
                {
                    timed_out.store(true, Ordering::Relaxed);
                    return Vec::new();
                }
                self.resolve_file_calls(file_path, calls, modules)
            })
            .collect();
        if timed_out.load(Ordering::Relaxed) {
            tracing::warn!("js cross-file call resolution timed out");
        }
        results
    }

    fn resolve_file_calls(
        &self,
        file_path: &str,
        calls: &[JsCallEdge],
        modules: &JsModuleIndex,
    ) -> Vec<JsResolvedCallRelationship> {
        let abs_path = Path::new("/").join(file_path);
        let mut relationships = Vec::new();

        'call_loop: for call in calls {
            let JsCallTarget::ImportedCall {
                imported_call:
                    super::super::types::JsImportedCall {
                        fallback_binding: _,
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
            let relative_resolved = match resolved_path.strip_prefix("/") {
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
                    0,
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
                source_path: file_path.to_string(),
                source_definition_range: match &call.caller {
                    JsCallSite::Definition { range, .. } => Some(*range),
                    JsCallSite::ModuleLevel => None,
                },
                target_path: final_path,
                target_definition_range: final_range,
            });
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

    fn resolver_for_mode(
        &self,
        resolution_mode: JsResolutionMode,
    ) -> &ResolverGeneric<RepoFileSystem> {
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
        let abs_path = Path::new("/").join(from_file);
        let resolved = self
            .resolve_specifier(&abs_path, specifier, resolution_mode)
            .ok()?;
        let resolved_path = resolved.into_path_buf();
        let relative = resolved_path.strip_prefix("/").ok()?;
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
        depth: usize,
    ) -> Option<ResolvedBinding> {
        if let Some(member_binding) = binding.member_bindings.get(member_name) {
            return self.follow_export_binding_target(
                module_path,
                member_binding,
                ImportedName::Named(member_name.to_string()),
                modules,
                depth,
            );
        }

        if let Some(source) = &binding.reexport_source {
            let next_imported_name = match binding.reexport_imported_name.clone() {
                Some(ImportedName::Namespace) => ImportedName::Named(member_name.to_string()),
                Some(imported_name) => imported_name,
                None => return None,
            };
            let (resolved_path, resolved_binding) =
                self.resolve_reexport(source, &next_imported_name, module_path, modules, depth)?;

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
                depth + 1,
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
    resolution_mode: JsResolutionMode,
    aliases: Vec<(String, Vec<oxc_resolver::AliasValue>)>,
) -> ResolverGeneric<RepoFileSystem> {
    ResolverGeneric::new_with_file_system(
        RepoFileSystem::new(probe.vfs.clone()),
        base_resolve_options(probe, resolution_mode, aliases),
    )
}

fn base_resolve_options(
    probe: &WorkspaceProbe,
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

    ResolveOptions {
        extensions,
        main_fields: vec!["module".to_string(), "main".to_string()],
        condition_names,
        extension_alias,
        tsconfig,
        alias,
        ..ResolveOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_RESOLVER_READ_BYTES, RepoFileSystem};
    use oxc_resolver::FileSystem;
    use std::io::ErrorKind;
    use tempfile::tempdir;

    #[test]
    fn repo_file_system_rejects_reads_outside_repo_root() {
        let temp = tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        let outside_root = temp.path().join("outside");
        std::fs::create_dir_all(&repo_root).unwrap();
        std::fs::create_dir_all(&outside_root).unwrap();

        let outside_file = outside_root.join("package.json");
        std::fs::write(&outside_file, "{}").unwrap();

        let fs = RepoFileSystem::new(crate::v2::pipeline::testing::checkout(&repo_root));
        let err = fs.read(&outside_file).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn repo_file_system_caps_resolver_metadata_reads() {
        let temp = tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        let package_dir = repo_root.join("node_modules/pkg");
        std::fs::create_dir_all(&package_dir).unwrap();

        let package_json = package_dir.join("package.json");
        std::fs::write(
            &package_json,
            vec![b'a'; MAX_RESOLVER_READ_BYTES as usize + 1],
        )
        .unwrap();

        let fs = RepoFileSystem::new(crate::v2::pipeline::testing::checkout(&repo_root));
        let err = fs
            .read(std::path::Path::new("/node_modules/pkg/package.json"))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn repo_file_system_resolves_relative_paths_under_repo_root() {
        let temp = tempdir().unwrap();
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(repo_root.join("src")).unwrap();
        std::fs::write(repo_root.join("src/index.js"), "export const ok = true;").unwrap();

        let fs = RepoFileSystem::new(crate::v2::pipeline::testing::checkout(&repo_root));
        let content = fs
            .read_to_string(std::path::Path::new("src/index.js"))
            .unwrap();
        assert_eq!(content, "export const ok = true;");
    }
}
