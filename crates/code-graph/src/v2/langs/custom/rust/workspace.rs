use super::manifest::{ManifestCache, build_project_workspace};
use super::sysroot::EmbeddedSysroot;
use super::*;

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
        worker_threads: usize,
        embedded_sysroot: &EmbeddedSysroot,
    ) -> Result<Self> {
        let workspace =
            build_project_workspace(root_path, manifest_path, manifest_cache, embedded_sysroot)?;
        let worker_threads = if worker_threads == 0 {
            num_cpus::get()
        } else {
            worker_threads
        };
        let load_config = LoadCargoConfig {
            load_out_dirs_from_check: false,
            with_proc_macro_server: ProcMacroServerChoice::None,
            prefill_caches: false,
            num_worker_threads: worker_threads,
            proc_macro_processes: 1,
        };
        let extra_env = rustc_hash::FxHashMap::default();

        let (db, vfs, _) =
            load_workspace(workspace, &extra_env, &load_config).with_context(|| {
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
    pub(super) fn load(root_path: &str, worker_threads: usize) -> Result<Self> {
        let mut manifest_cache = ManifestCache::new(root_path)?;
        let manifest_paths = manifest_cache.manifest_paths.clone();
        let embedded_sysroot = Arc::new(EmbeddedSysroot::materialize()?);
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
                worker_threads,
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
                Err(err) => last_error = Some(err),
            }
        }

        if workspaces.is_empty() {
            return Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no Rust manifests found")));
        }

        let include_crate_name_in_fqn = crate_names.len() > 1;
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

pub(super) fn relative_path_if_under_root(root_path: &str, file_path: &str) -> Option<String> {
    let root = Path::new(root_path);
    let normalized_root = normalize_existing_path(root).unwrap_or_else(|| root.to_path_buf());
    let file = Path::new(file_path);
    let normalized_file = normalize_existing_path(file).unwrap_or_else(|| file.to_path_buf());

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

pub(super) fn standalone_workspace(relative_path: &str, source: String) -> WorkspaceIndex {
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
        Arc::new(AbsPathBuf::assert_utf8(std::env::current_dir().unwrap())),
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
