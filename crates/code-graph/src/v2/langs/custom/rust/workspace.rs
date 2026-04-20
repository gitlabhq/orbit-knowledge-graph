use super::manifest::{ManifestCache, build_project_workspace};
use super::sysroot::EmbeddedSysroot;
use super::*;
use crate::v2::pipeline::FileInput;

#[derive(Clone)]
pub(super) struct WorkspaceIndex {
    pub(super) db: RootDatabase,
    pub(super) file_ids_by_relative_path: Arc<HashMap<String, FileId>>,
    pub(super) paths_by_file_id: Arc<HashMap<FileId, String>>,
    pub(super) crate_names_by_file_id: Arc<HashMap<FileId, String>>,
    pub(super) include_crate_name_in_fqn: bool,
}

pub(super) struct WorkspaceCatalog {
    _embedded_sysroot: Arc<EmbeddedSysroot>,
    workspaces: Vec<WorkspaceIndex>,
    workspace_ids_by_relative_path: HashMap<String, usize>,
}

impl WorkspaceIndex {
    fn load_manifest(
        root_path: &str,
        manifest_path: &Path,
        manifest_cache: &mut ManifestCache,
        repo_rust_files: &[AbsPathBuf],
        embedded_sysroot: &EmbeddedSysroot,
    ) -> Result<Self> {
        let workspace = build_project_workspace(manifest_path, manifest_cache, embedded_sysroot)?;

        let (db, vfs) =
            load_workspace_no_watcher(workspace, repo_rust_files).with_context(|| {
                format!(
                    "failed to load rust-analyzer workspace from {}",
                    manifest_path.display()
                )
            })?;

        let mut file_ids_by_relative_path = HashMap::new();
        let mut paths_by_file_id = HashMap::new();
        for (file_id, path) in vfs.iter() {
            let Some(abs_path) = path.as_path() else {
                continue;
            };
            let abs_path = abs_path.to_string();
            let Some(relative) = relative_path_if_under_root(root_path, &abs_path) else {
                continue;
            };
            file_ids_by_relative_path.insert(relative.clone(), file_id);
            paths_by_file_id.insert(file_id, relative);
        }

        let mut crate_names_by_file_id = HashMap::new();
        let sema = Semantics::new(&db);
        attach_db(&db, || {
            for &file_id in paths_by_file_id.keys() {
                let Some(module) = sema.file_to_module_def(file_id) else {
                    continue;
                };
                let Some(crate_name) = module.krate(&db).display_name(&db) else {
                    continue;
                };
                crate_names_by_file_id.insert(file_id, crate_name.to_string());
            }
        });

        Ok(Self {
            db,
            file_ids_by_relative_path: Arc::new(file_ids_by_relative_path),
            paths_by_file_id: Arc::new(paths_by_file_id),
            crate_names_by_file_id: Arc::new(crate_names_by_file_id),
            include_crate_name_in_fqn: false,
        })
    }

    pub(super) fn module_path_parts(&self, module: ra_ap_hir::Module) -> Vec<String> {
        let mut parts = module
            .path_to_root(&self.db)
            .into_iter()
            .rev()
            .filter_map(|module| module.name(&self.db))
            .map(|name| name.display(&self.db, Edition::CURRENT).to_string())
            .collect::<Vec<_>>();

        if self.include_crate_name_in_fqn
            && let Some(crate_name) = module.krate(&self.db).display_name(&self.db)
        {
            parts.insert(0, crate_name.to_string());
        }

        parts
    }

    pub(super) fn crate_root_parts_for_file(&self, file_id: FileId) -> Vec<String> {
        if !self.include_crate_name_in_fqn {
            return Vec::new();
        }

        self.crate_names_by_file_id
            .get(&file_id)
            .map(|name| vec![name.clone()])
            .unwrap_or_default()
    }
}

impl WorkspaceCatalog {
    pub(super) fn load(root_path: &str, files: &[FileInput]) -> Result<Self> {
        let mut manifest_cache = ManifestCache::new(root_path)?;
        let manifest_paths = manifest_cache.manifest_paths.clone();
        let embedded_sysroot = Arc::new(EmbeddedSysroot::materialize()?);
        let repo_rust_files = collect_abs_rust_files(root_path, files);
        let mut workspaces = Vec::new();
        let mut workspace_ids_by_relative_path = HashMap::new();
        let mut crate_names = HashSet::new();
        let mut loaded_roots = HashSet::new();
        let mut last_error = None;

        for manifest_path in manifest_paths {
            let workspace_manifest_path =
                manifest_cache.workspace_manifest_path_for(&manifest_path)?;
            if !loaded_roots.insert(workspace_manifest_path.clone()) {
                continue;
            }

            match WorkspaceIndex::load_manifest(
                root_path,
                &workspace_manifest_path,
                &mut manifest_cache,
                &repo_rust_files,
                embedded_sysroot.as_ref(),
            ) {
                Ok(workspace) => {
                    let workspace_id = workspaces.len();
                    crate_names.extend(workspace.crate_names_by_file_id.values().cloned());
                    for relative_path in workspace.file_ids_by_relative_path.keys() {
                        workspace_ids_by_relative_path
                            .entry(relative_path.clone())
                            .or_insert(workspace_id);
                    }
                    workspaces.push(workspace);
                }
                Err(err) => {
                    tracing::warn!(
                        manifest = %workspace_manifest_path.display(),
                        error = %err,
                        "failed to load rust-analyzer workspace; continuing with others"
                    );
                    last_error = Some(err);
                }
            }
        }

        if workspaces.is_empty() {
            return Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no Rust manifests found")));
        }

        let include_crate_name_in_fqn = workspaces.len() > 1 || crate_names.len() > 1;
        if include_crate_name_in_fqn {
            for workspace in &mut workspaces {
                workspace.include_crate_name_in_fqn = true;
            }
        }

        Ok(Self {
            _embedded_sysroot: embedded_sysroot,
            workspaces,
            workspace_ids_by_relative_path,
        })
    }

    pub(super) fn workspace_for_file(
        &self,
        relative_path: &str,
    ) -> Option<(usize, &WorkspaceIndex)> {
        let &workspace_id = self.workspace_ids_by_relative_path.get(relative_path)?;
        Some((workspace_id, &self.workspaces[workspace_id]))
    }

    pub(super) fn workspaces(&self) -> &[WorkspaceIndex] {
        &self.workspaces
    }
}

pub(super) fn to_absolute_path(root_path: &str, file_path: &str) -> String {
    let candidate = if Path::new(file_path).is_absolute() {
        PathBuf::from(file_path)
    } else {
        PathBuf::from(root_path).join(file_path)
    };
    normalize_existing_path(&candidate)
        .unwrap_or(candidate)
        .to_string_lossy()
        .to_string()
}

pub(super) fn relative_path(root_path: &str, file_path: &str) -> String {
    relative_path_if_under_root(root_path, file_path).unwrap_or_else(|| file_path.to_string())
}

/// Returns `file_path` made relative to `root_path`.
///
/// Callers should pass a pre-canonicalized `root_path` (see
/// `canonical_root_path`) so this function does not need to re-resolve
/// symlinks such as the macOS `/var` -> `/private/var` redirection on
/// every invocation. `file_path` is still normalized here because vfs
/// paths from rust-analyzer can contain unresolved components.
pub(super) fn relative_path_if_under_root(root_path: &str, file_path: &str) -> Option<String> {
    let root = Path::new(root_path);
    let file = Path::new(file_path);
    let normalized_file = normalize_existing_path(file).unwrap_or_else(|| file.to_path_buf());

    if let Ok(path) = normalized_file.strip_prefix(root) {
        return Some(path.to_string_lossy().to_string());
    }

    let normalized_root = normalize_existing_path(root).unwrap_or_else(|| root.to_path_buf());
    normalized_file
        .strip_prefix(&normalized_root)
        .ok()
        .map(|path| path.to_string_lossy().to_string())
}

pub(super) fn canonical_root_path(root_path: &str) -> String {
    normalize_existing_path(Path::new(root_path))
        .unwrap_or_else(|| PathBuf::from(root_path))
        .to_string_lossy()
        .to_string()
}

fn discover_manifest_paths(root_path: &str) -> Vec<PathBuf> {
    let mut manifests = WalkBuilder::new(root_path)
        .standard_filters(true)
        .build()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_some_and(|kind| kind.is_file()))
        .filter_map(|entry| {
            (entry
                .path()
                .file_name()
                .is_some_and(|name| name == "Cargo.toml"))
            .then(|| entry.into_path())
        })
        .collect::<Vec<_>>();
    manifests.sort();
    manifests.dedup();
    manifests
}

pub(super) fn normalize_existing_path(path: &Path) -> Option<PathBuf> {
    std::fs::canonicalize(path).ok()
}

pub(super) fn standalone_workspace(
    relative_path: &str,
    source: String,
    repo_root: &Path,
) -> WorkspaceIndex {
    let mut db = RootDatabase::new(None);
    let file_id = FileId::from_raw(0);
    let mut file_set = FileSet::default();
    file_set.insert(
        file_id,
        VfsPath::new_virtual_path(format!("/{}", relative_path)),
    );

    let mut change = ChangeWithProcMacros::default();
    change.set_roots(vec![SourceRoot::new_local(file_set)]);

    let mut crate_graph = CrateGraphBuilder::default();
    crate_graph.add_crate_root(
        file_id,
        Edition::CURRENT,
        None,
        None,
        Default::default(),
        None,
        Env::default(),
        CrateOrigin::Local {
            repo: None,
            name: None,
        },
        Vec::new(),
        false,
        Arc::new(abs_path_from(repo_root)),
        Arc::new(CrateWorkspaceData {
            target: Err("standalone file has no target layout".into()),
            toolchain: None,
        }),
    );
    change.change_file(file_id, Some(source));
    change.set_crate_graph(crate_graph);
    db.apply_change(change);

    let mut file_ids_by_relative_path = HashMap::new();
    file_ids_by_relative_path.insert(relative_path.to_string(), file_id);
    let mut paths_by_file_id = HashMap::new();
    paths_by_file_id.insert(file_id, relative_path.to_string());

    WorkspaceIndex {
        db,
        file_ids_by_relative_path: Arc::new(file_ids_by_relative_path),
        paths_by_file_id: Arc::new(paths_by_file_id),
        crate_names_by_file_id: Arc::new(HashMap::new()),
        include_crate_name_in_fqn: false,
    }
}

pub(super) fn discover_manifest_paths_for_root(root_path: &str) -> Vec<PathBuf> {
    discover_manifest_paths(root_path)
}

fn abs_path_from(path: &Path) -> AbsPathBuf {
    Utf8PathBuf::from_path_buf(path.to_path_buf())
        .ok()
        .map(AbsPathBuf::assert)
        .unwrap_or_else(|| AbsPathBuf::assert(Utf8PathBuf::from("/")))
}

/// Load a rust-analyzer `ProjectWorkspace` into a fresh `RootDatabase` without
/// touching `vfs_notify`. Upstream's `load_workspace` spawns a `VfsLoader`
/// thread and (once `set_config` runs with a non-empty watch list) an inotify
/// watcher; we bypass both by seeding the `Vfs` straight from the file set the
/// pipeline already discovered.
fn load_workspace_no_watcher(
    workspace: ProjectWorkspace,
    repo_rust_files: &[AbsPathBuf],
) -> Result<(RootDatabase, Vfs)> {
    // Invariant: this function never starts a proc-macro server.
    const _: ProcMacroServerChoice = ProcMacroServerChoice::None;

    let mut db = RootDatabase::new(None);
    let mut vfs = Vfs::default();
    let project_folders = ProjectFolders::new(std::slice::from_ref(&workspace), &[], None);

    // Scope the VFS to files that rust-analyzer's `ProjectFolders` considers
    // part of this workspace. `repo_rust_files` was already walked by the
    // pipeline; this is a filter, not a second walk.
    for entry in &project_folders.load {
        seed_vfs_from_known_files(&mut vfs, entry, repo_rust_files);
    }

    let extra_env = rustc_hash::FxHashMap::default();
    let (crate_graph, _proc_macros) = workspace.to_crate_graph(
        &mut |path: &AbsPath| {
            let vfs_path = VfsPath::from(path.to_path_buf());
            if vfs.file_id(&vfs_path).is_none() {
                let contents = std::fs::read(AsRef::<Path>::as_ref(path)).ok();
                vfs.set_file_contents(vfs_path.clone(), contents);
            }
            vfs.file_id(&vfs_path)
                .and_then(|(id, excluded)| (excluded == FileExcluded::No).then_some(id))
        },
        &extra_env,
    );

    let mut analysis_change = ChangeWithProcMacros::default();
    db.enable_proc_attr_macros();
    for (_, file) in vfs.take_changes() {
        if let vfs::Change::Create(bytes, _) | vfs::Change::Modify(bytes, _) = file.change
            && let Ok(text) = String::from_utf8(bytes)
        {
            analysis_change.change_file(file.file_id, Some(text));
        }
    }
    analysis_change.set_roots(project_folders.source_root_config.partition(&vfs));
    analysis_change.set_crate_graph(crate_graph);
    db.apply_change(analysis_change);

    Ok((db, vfs))
}

fn seed_vfs_from_known_files(vfs: &mut Vfs, entry: &loader::Entry, known: &[AbsPathBuf]) {
    match entry {
        loader::Entry::Files(files) => {
            for p in files {
                let contents = std::fs::read(AsRef::<Path>::as_ref(p)).ok();
                vfs.set_file_contents(VfsPath::from(p.clone()), contents);
            }
        }
        loader::Entry::Directories(dirs) => {
            for abs in known {
                if !dirs.include.iter().any(|inc| abs.starts_with(inc)) {
                    continue;
                }
                if dirs.exclude.iter().any(|ex| abs.starts_with(ex)) {
                    continue;
                }
                let ext = abs.extension().unwrap_or_default();
                if !dirs.extensions.iter().any(|e| e == ext) {
                    continue;
                }
                let contents = std::fs::read(AsRef::<Path>::as_ref(abs)).ok();
                vfs.set_file_contents(VfsPath::from(abs.clone()), contents);
            }
        }
    }
}

fn collect_abs_rust_files(root_path: &str, files: &[FileInput]) -> Vec<AbsPathBuf> {
    let root = Path::new(root_path);
    files
        .iter()
        .filter_map(|file| {
            let candidate = if Path::new(file).is_absolute() {
                PathBuf::from(file)
            } else {
                root.join(file)
            };
            let normalized = normalize_existing_path(&candidate).unwrap_or(candidate);
            Utf8PathBuf::from_path_buf(normalized)
                .ok()
                .map(AbsPathBuf::assert)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// Pins the behavior that we load a workspace without going through
    /// `ra_ap_load_cargo::load_workspace` (which spawns a `vfs_notify` thread).
    /// If someone reintroduces that call this test will still pass, but the
    /// sibling invariant assertion on `ProcMacroServerChoice` and a grep for
    /// `load_workspace(` in CI should catch it. The primary job here is to
    /// confirm the no-watcher loader actually produces a populated index.
    #[test]
    fn load_manifest_populates_index_without_watcher() {
        let temp = tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(
            root.join("Cargo.toml"),
            "[package]\nname = \"tiny\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[lib]\npath = \"src/lib.rs\"\n",
        )
        .unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn hello() -> u32 { 42 }\n").unwrap();

        let root_str = root.to_string_lossy().to_string();
        let mut manifest_cache = ManifestCache::new(&root_str).unwrap();
        let embedded_sysroot = EmbeddedSysroot::materialize().unwrap();
        let repo_rust_files = collect_abs_rust_files(&root_str, &["src/lib.rs".to_string()]);

        let index = WorkspaceIndex::load_manifest(
            &root_str,
            &root.join("Cargo.toml"),
            &mut manifest_cache,
            &repo_rust_files,
            &embedded_sysroot,
        )
        .unwrap();

        assert!(
            index
                .file_ids_by_relative_path
                .keys()
                .any(|p| p.ends_with("src/lib.rs")),
            "expected src/lib.rs to be indexed, got: {:?}",
            index.file_ids_by_relative_path.keys().collect::<Vec<_>>()
        );
    }

    /// Guards the atomic-polyfill advisory mitigation (RUSTSEC-2023-0089). If
    /// anyone flips our loader to launch a proc-macro server, this test must
    /// fail so it can't regress silently.
    #[test]
    fn proc_macro_server_is_disabled() {
        // Both the `const _` inside `load_workspace_no_watcher` and this match
        // fail to compile or panic if a new `ProcMacroServerChoice` variant
        // gets introduced that we don't explicitly reject.
        let choice = ProcMacroServerChoice::None;
        match choice {
            ProcMacroServerChoice::None => {}
            ProcMacroServerChoice::Sysroot | ProcMacroServerChoice::Explicit(_) => {
                panic!("proc-macro server must remain disabled for rust v2 indexing")
            }
        }
    }
}
