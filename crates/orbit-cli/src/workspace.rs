use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use crate::database::{GraphReader, IncompleteRevision, schema::graph_lacks_commit};
use anyhow::{Context, Result};
use gitalisk_core::repository::gitalisk_repository::{
    CoreGitaliskRepository, FileStatusInfo, GetStatusOptions, StatusCode,
};
use gitalisk_core::workspace_folder::gitalisk_workspace::CoreGitaliskWorkspaceFolder;

pub struct Workspace {
    root: PathBuf,
}

impl Workspace {
    pub fn open_default() -> Result<Self> {
        Self::open(Self::default_root()?)
    }

    pub fn default_root() -> Result<PathBuf> {
        static ROOT: OnceLock<std::result::Result<PathBuf, String>> = OnceLock::new();
        ROOT.get_or_init(|| {
            let override_dir = std::env::var("ORBIT_DATA_DIR")
                .ok()
                .filter(|s| !s.is_empty());
            let home = dirs::home_dir().ok_or("Could not determine home directory")?;
            resolve_root(override_dir, &home).map_err(|e| e.to_string())
        })
        .clone()
        .map_err(anyhow::Error::msg)
    }

    pub fn open(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn manifest_path(&self) -> PathBuf {
        self.root.join("local/manifest.duckdb")
    }

    pub fn revision_path(&self, git: &GitInfo) -> PathBuf {
        self.root
            .join("local/revisions")
            .join(git.project_id.to_string())
            .join(format!("{}.duckdb", git.commit_sha))
    }

    pub fn var_dir(&self, project_id: i64) -> PathBuf {
        self.root.join("var").join(project_id.to_string())
    }

    /// Index state comes first so a reset waits for running indexes before it locks the
    /// manifest. `graph.duckdb` is the single-file graph from releases before the manifest.
    pub fn graph_paths(&self) -> Vec<PathBuf> {
        [
            "var",
            "local",
            "graph.duckdb",
            "graph.duckdb.wal",
            "graph.duckdb.lock",
        ]
        .into_iter()
        .map(|name| self.root.join(name))
        .filter(|path| path.exists())
        .collect()
    }

    pub fn resolve_repos(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let canonical = dunce::canonicalize(path)?;

        let mut discovered = discover_repos(&canonical);
        let found_any = !discovered.is_empty();
        discovered.retain(|repo| *repo == canonical || !is_ignored_by_enclosing_repo(repo));
        if !discovered.is_empty() {
            return Ok(discovered);
        }
        if is_git_repo(&canonical) {
            return Ok(vec![canonical]);
        }
        if found_any {
            return Ok(Vec::new());
        }
        Ok(git_toplevel(&canonical).into_iter().collect())
    }
}

fn resolve_root(override_dir: Option<String>, home: &Path) -> Result<PathBuf> {
    if let Some(dir) = override_dir {
        return Ok(PathBuf::from(dir));
    }
    let root = home.join(".gitlab").join("orbit");
    let legacy = home.join(".orbit");
    if !root.exists() && legacy.is_dir() {
        return Ok(migrate_legacy_root(&legacy, root));
    }
    Ok(root)
}

fn migrate_legacy_root(legacy: &Path, root: PathBuf) -> PathBuf {
    let moved = std::fs::create_dir_all(root.parent().unwrap_or(&root))
        .and_then(|()| std::fs::rename(legacy, &root));
    match moved {
        Ok(()) => {
            eprintln!("orbit: moved {} to {}", legacy.display(), root.display());
            root
        }
        Err(e) => {
            eprintln!(
                "orbit: kept {} in place; move to {} failed ({e})",
                legacy.display(),
                root.display()
            );
            legacy.to_path_buf()
        }
    }
}

pub fn resolve_db_path(db: Option<PathBuf>, git: &GitInfo) -> Result<PathBuf> {
    let path = match db {
        Some(p) => p,
        None => Workspace::open_default()?.revision_path(git),
    };
    absolutize(path)
}

pub fn describe_graph_lock_conflict(error: &anyhow::Error) -> Option<String> {
    static HOLDER: OnceLock<regex::Regex> = OnceLock::new();
    let holder = HOLDER.get_or_init(|| {
        regex::Regex::new(r"Conflicting lock is held in (.+?) \(PID (\d+)\)")
            .expect("lock holder pattern is valid")
    });
    let chain = format!("{error:#}");
    let captures = holder.captures(&chain)?;
    let program = Path::new(&captures[1])
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| captures[1].to_string());
    Some(format!(
        "The local graph is busy: {program} (PID {}) is using it. Wait for it to finish, then try again.",
        &captures[2]
    ))
}

pub struct IndexedRepo {
    pub git: GitInfo,
    pub client: GraphReader,
}

pub fn open_indexed_if_present(repo: Option<PathBuf>, db: Option<PathBuf>) -> Option<IndexedRepo> {
    let (git, db_path) = locate_graph(repo, &db).ok()?;
    if graph_lacks_commit(&db_path, &git.commit_sha).unwrap_or(true) {
        return None;
    }
    let client = GraphReader::open(Some(db_path)).ok()?;
    Some(IndexedRepo { git, client })
}

pub fn open_graph_reader(db: Option<PathBuf>) -> Result<GraphReader> {
    let here = git_toplevel(Path::new("."))
        .ok()
        .filter(|top_level| git_info(top_level).is_ok());
    let Some(top_level) = here else {
        return GraphReader::open(db);
    };
    if already_indexed(&db, &top_level)? {
        match GraphReader::open(db.clone()) {
            Ok(reader) if reader.has_revisions() => return Ok(reader),
            Err(error) if !error.is::<IncompleteRevision>() => return Err(error),
            _ => {}
        }
    }
    crate::commands::index::index_before_first_query(top_level, db.clone())
        .context("failed to index the repository")?;
    GraphReader::open(db)
}

fn already_indexed(db: &Option<PathBuf>, top_level: &Path) -> Result<bool> {
    match db {
        Some(path) => Ok(path.try_exists()?),
        None => {
            let manifest = Workspace::open_default()?.manifest_path();
            crate::database::manifest::has_indexed_repo(&manifest, top_level)
        }
    }
}

pub fn open_indexed(repo: Option<PathBuf>, db: Option<PathBuf>) -> Result<IndexedRepo> {
    let (git, db_path) = locate_graph(repo, &db)?;

    if graph_lacks_commit(&db_path, &git.commit_sha)? {
        crate::commands::index::index_before_first_query(git.repo_path.clone(), db)
            .context("failed to index the repository")?;
        if graph_lacks_commit(&db_path, &git.commit_sha)? {
            anyhow::bail!(
                "indexing finished but commit {} still has no rows in the local graph",
                git.commit_sha
            );
        }
    }
    let client = GraphReader::open(Some(db_path))?;
    Ok(IndexedRepo { git, client })
}

pub fn open_indexed_without_indexing(
    repo: Option<PathBuf>,
    db: Option<PathBuf>,
) -> Result<IndexedRepo> {
    let (git, db_path) = locate_graph(repo, &db)?;
    anyhow::ensure!(
        !graph_lacks_commit(&db_path, &git.commit_sha)?,
        "commit {} has no complete index in {}; --no-index does not build one",
        git.commit_sha,
        db_path.display()
    );
    let client = GraphReader::open(Some(db_path))?;
    Ok(IndexedRepo { git, client })
}

fn locate_graph(repo: Option<PathBuf>, db: &Option<PathBuf>) -> Result<(GitInfo, PathBuf)> {
    let repo_path = repo.unwrap_or_else(|| PathBuf::from("."));
    let top_level = git_toplevel(&repo_path)?;
    let git = git_info(&top_level)
        .with_context(|| format!("failed to read git info for {}", top_level.display()))?;
    let db_path = resolve_db_path(db.clone(), &git)?;
    Ok((git, db_path))
}

pub fn edited_since_index(repo: &Path, files: &[&str]) -> std::collections::BTreeSet<String> {
    let mut edited = std::collections::BTreeSet::new();
    if files.is_empty() {
        return edited;
    }
    let repository = repository_at(repo);
    let options = GetStatusOptions {
        ignore_submodules: true,
        pathspecs: files.iter().map(PathBuf::from).collect(),
        ..Default::default()
    };
    let Ok(status) = repository.get_status_with(&options) else {
        return edited;
    };
    let changed: std::collections::HashSet<String> = status
        .iter()
        .filter_map(|info| repo_relative(repo, &info.path))
        .collect();
    let tracked: std::collections::HashSet<String> = repository
        .tracked_files(files)
        .unwrap_or_default()
        .iter()
        .filter_map(|path| repo_relative(repo, path))
        .collect();
    edited.extend(
        files
            .iter()
            .filter(|file| changed.contains(**file) || !tracked.contains(**file))
            .map(|file| file.to_string()),
    );
    edited
}

pub fn repo_relative_paths(repo: &Path, paths: &[String]) -> Vec<String> {
    let cwd = std::env::current_dir().unwrap_or_default();
    let root = dunce::canonicalize(repo).unwrap_or_else(|_| repo.to_path_buf());
    paths
        .iter()
        .flat_map(|p| p.split(','))
        .map(str::trim)
        .map(|path| {
            path.rsplit_once(':')
                .filter(|(file, range)| {
                    !file.is_empty()
                        && !range.is_empty()
                        && range.chars().all(|c| c.is_ascii_digit() || c == '-')
                })
                .map_or(path, |(file, _)| file)
        })
        .filter_map(|path| {
            let relative = [cwd.join(path), repo.join(path)]
                .into_iter()
                .find_map(|candidate| {
                    let full = dunce::canonicalize(candidate).ok()?;
                    full.strip_prefix(&root)
                        .ok()
                        .map(|r| r.to_string_lossy().replace('\\', "/"))
                })
                .unwrap_or_else(|| {
                    path.trim_start_matches("./")
                        .trim_end_matches('/')
                        .to_string()
                });
            (!relative.is_empty() && relative != ".").then_some(relative)
        })
        .collect()
}

fn absolutize(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()
            .context("failed to read current directory")?
            .join(path))
    }
}

#[derive(Clone)]
pub struct GitInfo {
    pub repo_path: PathBuf,
    pub project_id: i64,
    pub branch: String,
    pub commit_sha: String,
    pub parent_repo_path: PathBuf,
}

impl GitInfo {
    pub fn short_sha(&self) -> &str {
        self.commit_sha.get(..8).unwrap_or(&self.commit_sha)
    }
}

pub fn git_info(repo_path: &Path) -> Result<GitInfo> {
    let canonical = dunce::canonicalize(repo_path)
        .with_context(|| format!("failed to canonicalize {}", repo_path.display()))?;
    let path_str = canonical.to_string_lossy().to_string();
    let project_id = project_id_from_path(&path_str);

    let repo = CoreGitaliskRepository::new(path_str.clone(), path_str);

    let branch = repo
        .get_current_branch()
        .context("failed to get current branch")?;
    let commit_sha = repo.get_current_commit_hash().map_err(|_| {
        anyhow::anyhow!(
            "{} has no commits yet. Make at least one commit before indexing.",
            canonical.display()
        )
    })?;
    let parent_repo_path = repo
        .parent_repo_path()
        .context("failed to resolve parent repo path")?;

    Ok(GitInfo {
        repo_path: canonical,
        project_id,
        branch,
        commit_sha,
        parent_repo_path,
    })
}

pub fn git_toplevel(path: &Path) -> Result<PathBuf> {
    let top = repository_at(path)
        .toplevel()
        .with_context(|| format!("failed to find git top-level for {}", path.display()))?;
    dunce::canonicalize(&top).with_context(|| format!("failed to canonicalize {}", top.display()))
}

pub(crate) fn repository_at(path: &Path) -> CoreGitaliskRepository {
    let path = path.to_string_lossy().to_string();
    CoreGitaliskRepository::new(path.clone(), path)
}

fn repo_relative(root: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(root).ok()?.to_str().map(str::to_string)
}

#[derive(Default)]
pub struct GitChanges {
    pub changed: Vec<String>,
    pub removed: Vec<String>,
}

impl GitChanges {
    pub fn paths(&self) -> impl Iterator<Item = &String> {
        self.changed.iter().chain(&self.removed)
    }

    fn from_statuses(root: &Path, statuses: &[FileStatusInfo]) -> Self {
        let mut changes = Self::default();
        for info in statuses {
            let path = repo_relative(root, &info.path);
            let original = info
                .original_path
                .as_deref()
                .and_then(|original| repo_relative(root, original));
            let status = info.status;
            let gone = [status.index, status.worktree].contains(&StatusCode::Deleted);
            changes.removed.extend(original);
            match gone {
                true => changes.removed.extend(path),
                false => changes.changed.extend(path),
            }
        }
        changes.settled()
    }

    pub fn settled(mut self) -> Self {
        for list in [&mut self.changed, &mut self.removed] {
            list.sort();
            list.dedup();
        }
        self.removed.retain(|p| !self.changed.contains(p));
        self
    }
}

pub fn git_committed_changes(repo_path: &Path, base_sha: &str) -> Result<GitChanges> {
    let committed = repository_at(repo_path)
        .changes_since(base_sha)
        .with_context(|| format!("failed to diff {base_sha} in {}", repo_path.display()))?;
    Ok(GitChanges::from_statuses(repo_path, &committed))
}

pub fn git_working_changes(repo_path: &Path) -> Result<GitChanges> {
    let working = repository_at(repo_path)
        .get_status()
        .with_context(|| format!("failed to read git status in {}", repo_path.display()))?;
    Ok(GitChanges::from_statuses(repo_path, &working))
}

pub fn git_tracked_ignored(repo_path: &Path) -> Result<Vec<String>> {
    let listed = git_output(
        repo_path,
        &[
            "ls-files",
            "-z",
            "--cached",
            "--ignored",
            "--exclude-standard",
        ],
    )?;
    Ok(listed
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .filter_map(|path| std::str::from_utf8(path).ok())
        .map(str::to_string)
        .collect())
}

fn git_output(repo_path: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo_path)
        .output()
        .with_context(|| format!("failed to run git in {}", repo_path.display()))?;
    if !output.status.success() {
        anyhow::bail!(
            "git {} failed in {}: {}",
            args.join(" "),
            repo_path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

pub fn project_id_from_path(path: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

fn is_git_repo(path: &Path) -> bool {
    let git = path.join(".git");
    git.is_dir() || git.is_file()
}

fn is_ignored_by_enclosing_repo(repo: &Path) -> bool {
    let Some(enclosing) = repo.parent().and_then(|parent| git_toplevel(parent).ok()) else {
        return false;
    };
    repository_at(&enclosing).is_ignored(repo)
}

fn discover_repos(workspace_path: &Path) -> Vec<PathBuf> {
    let ws = CoreGitaliskWorkspaceFolder::new(workspace_path.to_string_lossy().to_string());
    if let Err(e) = ws.index_repositories() {
        orbit_log_fields::warn!(
            expanded(LOCAL_PATH) = %workspace_path.display(),
            safe(ERROR_TYPE) = "repository_discovery",
            expanded(ERROR_MESSAGE) = %e,
            "repository discovery failed"
        );
        return vec![];
    }
    let mut repos: Vec<PathBuf> = ws
        .get_repositories()
        .into_iter()
        .map(|r| PathBuf::from(&r.path))
        .collect();
    repos.sort();
    repos
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::git;

    fn init_repo(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        Command::new("git")
            .args(["init"])
            .current_dir(path)
            .output()
            .unwrap();
    }

    fn workspace_in(root: &Path) -> Workspace {
        Workspace::open(root.join("orbit-home")).unwrap()
    }

    #[test]
    fn resolve_repos_climbs_from_a_subdirectory_to_its_repository() {
        let scratch = tempfile::TempDir::new().unwrap();
        let repo = scratch.path().join("repo");
        init_repo(&repo);
        std::fs::create_dir_all(repo.join("src/deep")).unwrap();

        let resolved = workspace_in(scratch.path())
            .resolve_repos(&repo.join("src/deep"))
            .unwrap();

        assert_eq!(resolved, vec![dunce::canonicalize(&repo).unwrap()]);
    }

    #[test]
    fn resolve_repos_does_not_climb_when_every_nested_repo_is_ignored() {
        let scratch = tempfile::TempDir::new().unwrap();
        let dotfiles = scratch.path().join("home");
        init_repo(&dotfiles);
        std::fs::write(dotfiles.join(".gitignore"), "*\n").unwrap();
        init_repo(&dotfiles.join("workspace/a"));

        let resolved = workspace_in(scratch.path())
            .resolve_repos(&dotfiles.join("workspace"))
            .unwrap();

        assert!(resolved.is_empty(), "{resolved:?}");
    }

    #[test]
    fn resolve_root_prefers_the_explicit_override() {
        let home = tempfile::TempDir::new().unwrap();
        let root = resolve_root(Some("/custom/dir".to_string()), home.path()).unwrap();
        assert_eq!(root, PathBuf::from("/custom/dir"));
    }

    #[test]
    fn resolve_root_uses_the_gitlab_home_on_a_fresh_install() {
        let home = tempfile::TempDir::new().unwrap();
        let root = resolve_root(None, home.path()).unwrap();
        assert_eq!(root, home.path().join(".gitlab").join("orbit"));
        assert!(!root.exists());
    }

    #[test]
    fn resolve_root_migrates_a_legacy_directory() {
        let home = tempfile::TempDir::new().unwrap();
        let legacy = home.path().join(".orbit");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("graph.duckdb"), b"data").unwrap();

        let root = resolve_root(None, home.path()).unwrap();

        assert_eq!(root, home.path().join(".gitlab").join("orbit"));
        assert!(root.join("graph.duckdb").exists());
        assert!(!legacy.exists());
    }

    #[test]
    fn resolve_root_keeps_the_new_directory_when_both_exist() {
        let home = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(home.path().join(".orbit")).unwrap();
        let expected = home.path().join(".gitlab").join("orbit");
        std::fs::create_dir_all(&expected).unwrap();

        let root = resolve_root(None, home.path()).unwrap();

        assert_eq!(root, expected);
        assert!(home.path().join(".orbit").exists());
    }

    #[test]
    fn resolve_root_keeps_the_legacy_directory_when_the_move_fails() {
        let home = tempfile::TempDir::new().unwrap();
        let legacy = home.path().join(".orbit");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("graph.duckdb"), b"data").unwrap();
        std::fs::write(home.path().join(".gitlab"), b"").unwrap();

        let root = resolve_root(None, home.path()).unwrap();

        assert_eq!(root, legacy);
        assert!(legacy.join("graph.duckdb").exists());
    }

    #[test]
    fn test_project_id_deterministic() {
        let a = project_id_from_path("/Users/alice/repo");
        let b = project_id_from_path("/Users/alice/repo");
        assert_eq!(a, b);
        assert!(a > 0);
    }

    #[test]
    fn test_project_id_different_paths() {
        let a = project_id_from_path("/Users/alice/repo-a");
        let b = project_id_from_path("/Users/alice/repo-b");
        assert_ne!(a, b);
    }

    #[test]
    fn test_resolve_single_repo() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Workspace::open(temp.path().join("orbit")).unwrap();

        let repo = temp.path().join("my-repo");
        init_repo(&repo);

        let repos = store.resolve_repos(&repo).unwrap();
        assert_eq!(repos.len(), 1);
    }

    #[test]
    fn test_resolve_workspace() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Workspace::open(temp.path().join("orbit")).unwrap();

        let workspace = temp.path().join("workspace");
        init_repo(&workspace.join("repo-a"));
        init_repo(&workspace.join("repo-b"));
        std::fs::create_dir_all(workspace.join("not-a-repo")).unwrap();

        let repos = store.resolve_repos(&workspace).unwrap();
        assert_eq!(repos.len(), 2);
    }

    #[test]
    fn test_resolve_workspace_that_is_also_a_git_repo() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = Workspace::open(temp.path().join("orbit")).unwrap();

        let workspace = temp.path().join("workspace");
        init_repo(&workspace);
        init_repo(&workspace.join("nested-a"));
        init_repo(&workspace.join("nested-b"));

        let repos = store.resolve_repos(&workspace).unwrap();
        assert!(
            repos.len() >= 3,
            "expected at least 3 repos, got {}: {:?}",
            repos.len(),
            repos
        );
    }

    #[test]
    fn git_toplevel_climbs_from_a_subdirectory() {
        let temp = tempfile::TempDir::new().unwrap();
        let repo = temp.path().join("repo");
        init_repo(&repo);
        let subdir = repo.join("src/deep");
        std::fs::create_dir_all(&subdir).unwrap();

        let top = git_toplevel(&subdir).unwrap();
        assert_eq!(top, dunce::canonicalize(&repo).unwrap());
    }

    fn commit_all(dir: &Path, message: &str) -> String {
        git(dir, &["add", "-A"]);
        git(dir, &["commit", "-m", message]);
        git_info(dir).unwrap().commit_sha
    }

    fn committed_repo() -> (tempfile::TempDir, PathBuf, String) {
        let temp = tempfile::TempDir::new().unwrap();
        let repo = dunce::canonicalize(temp.path()).unwrap();
        git(&repo, &["init"]);
        std::fs::write(repo.join("keep.rs"), "fn keep() {}\n").unwrap();
        std::fs::write(repo.join("edit.rs"), "fn edit() {}\n").unwrap();
        std::fs::write(repo.join("drop.rs"), "fn drop_me() {}\n").unwrap();
        std::fs::write(
            repo.join("move_me.rs"),
            "fn moved() { /* padding */ }\n".repeat(8),
        )
        .unwrap();
        let base = commit_all(&repo, "base");
        (temp, repo, base)
    }

    #[test]
    fn git_committed_changes_reports_edits_removals_and_renames() {
        let (_temp, repo, base) = committed_repo();
        std::fs::write(repo.join("edit.rs"), "fn edit() { 1 }\n").unwrap();
        std::fs::remove_file(repo.join("drop.rs")).unwrap();
        std::fs::rename(repo.join("move_me.rs"), repo.join("moved.rs")).unwrap();
        std::fs::write(repo.join("new.rs"), "fn new() {}\n").unwrap();
        commit_all(&repo, "change");

        let changes = git_committed_changes(&repo, &base).unwrap();

        assert_eq!(changes.changed, ["edit.rs", "moved.rs", "new.rs"]);
        assert_eq!(changes.removed, ["drop.rs", "move_me.rs"]);
    }

    #[test]
    fn git_working_changes_reports_edits_removals_untracked_and_renames() {
        let (_temp, repo, _base) = committed_repo();
        std::fs::write(repo.join("edit.rs"), "fn edit() { 1 }\n").unwrap();
        std::fs::remove_file(repo.join("drop.rs")).unwrap();
        std::fs::write(repo.join("untracked.rs"), "fn u() {}\n").unwrap();
        git(&repo, &["mv", "move_me.rs", "moved.rs"]);

        let changes = git_working_changes(&repo).unwrap();

        assert_eq!(changes.changed, ["edit.rs", "moved.rs", "untracked.rs"]);
        assert_eq!(changes.removed, ["drop.rs", "move_me.rs"]);
    }

    #[test]
    fn git_working_changes_treats_an_unstaged_move_as_removal_plus_change() {
        let (_temp, repo, _base) = committed_repo();
        std::fs::rename(repo.join("move_me.rs"), repo.join("moved.rs")).unwrap();

        let changes = git_working_changes(&repo).unwrap();

        assert_eq!(changes.changed, ["moved.rs"]);
        assert_eq!(changes.removed, ["move_me.rs"]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn git_working_changes_skips_names_that_are_not_utf8() {
        use std::os::unix::ffi::OsStrExt;
        let (_temp, repo, _base) = committed_repo();
        std::fs::write(repo.join(std::ffi::OsStr::from_bytes(b"bad\xff.rs")), "x").unwrap();
        std::fs::write(repo.join("good.rs"), "x").unwrap();

        let changes = git_working_changes(&repo).unwrap();

        assert_eq!(changes.changed, ["good.rs"]);
    }

    #[test]
    fn edited_since_index_flags_modified_and_untracked_files_only() {
        let (_temp, repo, _base) = committed_repo();
        std::fs::write(repo.join("edit.rs"), "fn edit() { 1 }\n").unwrap();
        std::fs::write(repo.join("untracked.rs"), "x").unwrap();

        let edited = edited_since_index(&repo, &["keep.rs", "edit.rs", "untracked.rs", "drop.rs"]);

        assert_eq!(
            edited.into_iter().collect::<Vec<_>>(),
            ["edit.rs", "untracked.rs"]
        );
    }

    #[test]
    fn edited_since_index_is_empty_outside_a_repository() {
        let temp = tempfile::TempDir::new().unwrap();
        assert!(edited_since_index(temp.path(), &["a.rs"]).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn edited_since_index_treats_pathspecs_literally() {
        let (_temp, repo, _base) = committed_repo();
        std::fs::write(repo.join(":odd.rs"), "fn odd() {}\n").unwrap();
        std::fs::write(repo.join("odd.rs"), "fn odd() {}\n").unwrap();
        commit_all(&repo, "odd");
        std::fs::write(repo.join("odd.rs"), "fn odd() { 1 }\n").unwrap();

        let edited = edited_since_index(&repo, &[":odd.rs"]);

        assert!(edited.is_empty(), "{edited:?}");
    }

    #[test]
    fn git_toplevel_fails_outside_a_repository() {
        let temp = tempfile::TempDir::new().unwrap();
        assert!(git_toplevel(temp.path()).is_err());
    }

    #[test]
    fn locate_graph_reports_a_missing_repository_once() {
        let temp = tempfile::TempDir::new().unwrap();
        let Err(err) = locate_graph(Some(temp.path().to_path_buf()), &None) else {
            panic!("locate_graph succeeded outside a repository");
        };
        let rendered = format!("{err:#}");
        assert_eq!(
            rendered.matches("failed to find git top-level").count(),
            1,
            "{rendered}"
        );
    }
}
