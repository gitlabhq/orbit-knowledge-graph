use super::manifest::{ManifestCache, build_project_workspace};
use super::sysroot::EmbeddedSysroot;
use super::*;
use crate::v2::pipeline::{FamilyFileInput, VIRTUAL_ROOT};

#[derive(Clone)]
pub(super) struct WorkspaceIndex {
    pub(super) db: RootDatabase,
    pub(super) file_ids_by_relative_path: Arc<HashMap<String, FileId>>,
    pub(super) paths_by_file_id: Arc<HashMap<FileId, String>>,
    pub(super) crate_names_by_file_id: Arc<HashMap<FileId, String>>,
    pub(super) include_crate_name_in_fqn: bool,
}

pub(super) struct WorkspacePlan {
    embedded_sysroot: Arc<EmbeddedSysroot>,
    repo_rust_files: Vec<AbsPathBuf>,
    entries: Vec<PlannedWorkspace>,
}

struct PlannedWorkspace {
    manifest_path: PathBuf,
    workspace: ProjectWorkspace,
    candidates: Vec<usize>,
}

impl WorkspaceIndex {
    fn load_planned(
        repo: &RepoFs,
        sysroot: &EmbeddedSysroot,
        manifest_path: &Path,
        workspace: &ProjectWorkspace,
        repo_rust_files: &[AbsPathBuf],
        multiple_roots: bool,
    ) -> Result<Self> {
        let read = |path: &AbsPath| read_repo_or_sysroot(repo, sysroot, path);
        let (db, vfs) =
            load_workspace_no_watcher(workspace, repo_rust_files, &read).with_context(|| {
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
            let Some(relative) = repo_relative_path(repo, abs_path.as_str()) else {
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

        let include_crate_name_in_fqn = multiple_roots
            || crate_names_by_file_id
                .values()
                .collect::<HashSet<_>>()
                .len()
                > 1;

        Ok(Self {
            db,
            file_ids_by_relative_path: Arc::new(file_ids_by_relative_path),
            paths_by_file_id: Arc::new(paths_by_file_id),
            crate_names_by_file_id: Arc::new(crate_names_by_file_id),
            include_crate_name_in_fqn,
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

impl WorkspacePlan {
    pub(super) fn discover(repo: &RepoFs, files: &[FamilyFileInput]) -> Result<Self> {
        let mut manifest_cache = ManifestCache::new(repo)?;
        let manifest_paths = manifest_cache.manifest_paths.clone();
        let embedded_sysroot = Arc::new(EmbeddedSysroot::materialize()?);
        let (repo_rust_files, inventory_indexes) = collect_abs_rust_files(files);
        let by_path = sorted_by_path(&repo_rust_files);
        let mut entries = Vec::new();
        let mut loaded_roots = HashSet::new();
        let mut last_error = None;

        for manifest_path in manifest_paths {
            let workspace_manifest_path =
                manifest_cache.workspace_manifest_path_for(&manifest_path)?;
            if !loaded_roots.insert(workspace_manifest_path.clone()) {
                continue;
            }

            match build_project_workspace(
                &workspace_manifest_path,
                &mut manifest_cache,
                embedded_sysroot.as_ref(),
            ) {
                Ok(workspace) => {
                    let candidates = candidate_file_indexes(&workspace, &repo_rust_files, &by_path)
                        .into_iter()
                        .map(|idx| inventory_indexes[idx])
                        .collect();
                    entries.push(PlannedWorkspace {
                        manifest_path: workspace_manifest_path,
                        workspace,
                        candidates,
                    });
                }
                Err(err) => {
                    tracing::warn!(
                        manifest = %workspace_manifest_path.display(),
                        error = %err,
                        "failed to plan rust-analyzer workspace; continuing with others"
                    );
                    last_error = Some(err);
                }
            }
        }

        if entries.is_empty() {
            return Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no Rust manifests found")));
        }

        Ok(Self {
            embedded_sysroot,
            repo_rust_files,
            entries,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Inventory indexes this root would own, decided by path matching alone so
    /// that no database has to be built to find out.
    pub(super) fn candidates(&self, idx: usize) -> &[usize] {
        &self.entries[idx].candidates
    }

    pub(super) fn manifest_path(&self, idx: usize) -> &Path {
        &self.entries[idx].manifest_path
    }

    pub(super) fn load(&self, idx: usize, repo: &RepoFs) -> Result<WorkspaceIndex> {
        let planned = &self.entries[idx];
        WorkspaceIndex::load_planned(
            repo,
            &self.embedded_sysroot,
            &planned.manifest_path,
            &planned.workspace,
            &self.repo_rust_files,
            self.entries.len() > 1,
        )
    }
}

/// The candidate filter and the VFS seed must agree, or a root can claim files
/// its own database never loaded.
fn dirs_match(dirs: &loader::Directories, path: &AbsPath) -> bool {
    dirs.include.iter().any(|inc| path.starts_with(inc))
        && !dirs.exclude.iter().any(|ex| path.starts_with(ex))
        && dirs
            .extensions
            .iter()
            .any(|ext| Some(ext.as_str()) == path.extension())
}

fn sorted_by_path(repo_rust_files: &[AbsPathBuf]) -> Vec<usize> {
    let mut order = (0..repo_rust_files.len()).collect::<Vec<_>>();
    order.sort_unstable_by_key(|&idx| repo_rust_files[idx].as_str());
    order
}

fn candidate_file_indexes(
    workspace: &ProjectWorkspace,
    repo_rust_files: &[AbsPathBuf],
    by_path: &[usize],
) -> Vec<usize> {
    let project_folders = ProjectFolders::new(std::slice::from_ref(workspace), &[], None);
    let mut hit = vec![false; repo_rust_files.len()];
    for entry in &project_folders.load {
        match entry {
            loader::Entry::Files(files) => {
                for path in files {
                    if let Some(idx) = repo_rust_files.iter().position(|known| known == path) {
                        hit[idx] = true;
                    }
                }
            }
            loader::Entry::Directories(dirs) => {
                // Every match sits under one include dir, so walking those path
                // ranges beats rescanning the whole repo per directory.
                for include in &dirs.include {
                    let prefix = match include.as_str() {
                        dir if dir.ends_with('/') => dir.to_string(),
                        dir => format!("{dir}/"),
                    };
                    let start = by_path
                        .partition_point(|&idx| repo_rust_files[idx].as_str() < prefix.as_str());
                    for &idx in &by_path[start..] {
                        let path = &repo_rust_files[idx];
                        if !path.as_str().starts_with(&prefix) {
                            break;
                        }
                        hit[idx] = hit[idx] || dirs_match(dirs, path);
                    }
                }
            }
        }
    }
    hit.iter()
        .enumerate()
        .filter_map(|(idx, &matched)| matched.then_some(idx))
        .collect()
}

/// The repository filesystem is rooted at `/`; a relative path is made
/// absolute by that root and `.`/`..` are resolved lexically.
pub(super) fn to_absolute_path(file_path: &str) -> String {
    normalize_path(&Path::new(VIRTUAL_ROOT).join(file_path))
        .to_string_lossy()
        .to_string()
}

/// A repository file's path relative to the root, `None` for anything that
/// is not a file of the repository (the sysroot, say).
pub(super) fn repo_relative_path(repo: &RepoFs, abs_path: &str) -> Option<String> {
    let relative = abs_path.strip_prefix('/')?;
    repo.is_file(Path::new(abs_path))
        .then(|| relative.to_string())
}

/// Lexical normalization: no filesystem, so `..` never follows a symlink.
pub(super) fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::CurDir => {}
            other => normalized.push(other),
        }
    }
    normalized
}

/// rust-analyzer asks for two kinds of file: the repository's, and the
/// embedded sysroot's, which live on disk where we materialized them.
fn read_repo_or_sysroot(
    repo: &RepoFs,
    sysroot: &EmbeddedSysroot,
    path: &AbsPath,
) -> Option<Vec<u8>> {
    let path: &Path = path.as_ref();
    if path.starts_with(sysroot.root_path()) {
        return std::fs::read(path).ok();
    }
    repo.read(path).ok().map(|bytes| bytes.to_vec())
}

pub(super) fn discover_manifest_paths(repo: &RepoFs) -> Vec<PathBuf> {
    let mut manifests = repo
        .paths()
        .into_iter()
        .filter(|path| {
            Path::new(path)
                .file_name()
                .is_some_and(|name| name == "Cargo.toml")
        })
        .map(|path| Path::new(VIRTUAL_ROOT).join(path))
        .collect::<Vec<_>>();
    manifests.sort();
    manifests
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
        Arc::new(abs_path_from(Path::new(VIRTUAL_ROOT))),
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
    workspace: &ProjectWorkspace,
    repo_rust_files: &[AbsPathBuf],
    read: &dyn Fn(&AbsPath) -> Option<Vec<u8>>,
) -> Result<(RootDatabase, Vfs)> {
    // Invariant: this function never starts a proc-macro server.
    const _: ProcMacroServerChoice = ProcMacroServerChoice::None;

    let mut db = RootDatabase::new(None);
    let mut vfs = Vfs::default();
    let project_folders = ProjectFolders::new(std::slice::from_ref(workspace), &[], None);

    // `repo_rust_files` was already walked by the pipeline; this is a filter, not a second walk.
    for entry in &project_folders.load {
        seed_vfs_from_known_files(&mut vfs, entry, repo_rust_files, read);
    }

    let extra_env = rustc_hash::FxHashMap::default();
    let (crate_graph, _proc_macros) = workspace.to_crate_graph(
        &mut |path: &AbsPath| {
            let vfs_path = VfsPath::from(path.to_path_buf());
            if vfs.file_id(&vfs_path).is_none() {
                vfs.set_file_contents(vfs_path.clone(), read(path));
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

fn seed_vfs_from_known_files(
    vfs: &mut Vfs,
    entry: &loader::Entry,
    known: &[AbsPathBuf],
    read: &dyn Fn(&AbsPath) -> Option<Vec<u8>>,
) {
    match entry {
        loader::Entry::Files(files) => {
            for p in files {
                vfs.set_file_contents(VfsPath::from(p.clone()), read(p));
            }
        }
        loader::Entry::Directories(dirs) => {
            for abs in known.iter().filter(|abs| dirs_match(dirs, abs)) {
                vfs.set_file_contents(VfsPath::from(abs.clone()), read(abs));
            }
        }
    }
}

fn collect_abs_rust_files(files: &[FamilyFileInput]) -> (Vec<AbsPathBuf>, Vec<usize>) {
    let mut paths = Vec::with_capacity(files.len());
    let mut indexes = Vec::with_capacity(files.len());
    for (idx, file) in files.iter().enumerate() {
        if let Ok(utf8) = Utf8PathBuf::from_path_buf(PathBuf::from(to_absolute_path(&file.path))) {
            paths.push(AbsPathBuf::assert(utf8));
            indexes.push(idx);
        }
    }
    (paths, indexes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::pipeline::testing;

    #[test]
    fn load_manifest_populates_index_without_watcher() {
        let (ctx, files) = testing::repo(&[
            (
                "Cargo.toml",
                b"[package]\nname = \"tiny\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[lib]\npath = \"src/lib.rs\"\n",
            ),
            ("src/lib.rs", b"pub fn hello() -> u32 { 42 }\n"),
        ]);
        let plan = WorkspacePlan::discover(&ctx.vfs, &files).unwrap();
        let index = plan.load(0, &ctx.vfs).unwrap();

        assert!(
            index
                .file_ids_by_relative_path
                .keys()
                .any(|p| p.ends_with("src/lib.rs")),
            "expected src/lib.rs to be indexed, got: {:?}",
            index.file_ids_by_relative_path.keys().collect::<Vec<_>>()
        );
    }

    /// A crate reached only through a path dependency roots itself as well as
    /// belonging to the workspace, so both roots must not parse it.
    #[test]
    fn implicit_member_is_claimed_and_parsed_once_by_the_root_workspace() {
        let (ctx, files) = testing::repo(&[
            ("Cargo.toml", b"[workspace]\nmembers = [\"a\"]\n"),
            (
                "a/Cargo.toml",
                b"[package]\nname = \"a\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[lib]\npath = \"src/lib.rs\"\n\n[dependencies]\nb = { path = \"../b\" }\n",
            ),
            ("a/src/lib.rs", b"pub fn from_a() {}\n"),
            (
                "b/Cargo.toml",
                b"[package]\nname = \"b\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[lib]\npath = \"src/lib.rs\"\n",
            ),
            ("b/src/lib.rs", b"pub fn from_b() {}\n"),
        ]);
        let plan = WorkspacePlan::discover(&ctx.vfs, &files).unwrap();

        assert_eq!(plan.len(), 2);
        assert_eq!(plan.candidates(0), &[0, 1]);

        let output = parse_rust_files_with_workspaces(&files, &ctx, &plan, None);
        assert!(output.errors.is_empty(), "{:?}", output.errors);
        let mut paths = output
            .parsed
            .iter()
            .map(|file| file.relative_path.as_str())
            .collect::<Vec<_>>();
        paths.sort_unstable();
        assert_eq!(paths, ["a/src/lib.rs", "b/src/lib.rs"]);
    }

    /// Guards the atomic-polyfill advisory mitigation (RUSTSEC-2023-0089). If
    /// anyone flips our loader to launch a proc-macro server, this test must
    /// fail so it can't regress silently.
    #[test]
    fn proc_macro_server_is_disabled() {
        let choice = ProcMacroServerChoice::None;
        match choice {
            ProcMacroServerChoice::None => {}
            ProcMacroServerChoice::Sysroot | ProcMacroServerChoice::Explicit(_) => {
                panic!("proc-macro server must remain disabled for rust v2 indexing")
            }
        }
    }
}

