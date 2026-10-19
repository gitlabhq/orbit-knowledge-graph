use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

use crate::database::{GraphReader, schema::graph_lacks_commit};
use anyhow::{Context, Result};
use gitalisk_core::repository::gitalisk_repository::CoreGitaliskRepository;
use gitalisk_core::workspace_folder::gitalisk_workspace::CoreGitaliskWorkspaceFolder;

pub struct Workspace {
    root: PathBuf,
}

impl Workspace {
    pub fn open_default() -> Result<Self> {
        Self::open(Self::default_root()?)
    }

    pub fn default_root() -> Result<PathBuf> {
        // Resolution runs inside the cell so the legacy migration happens at most
        // once per process, even when MCP tool calls race on the first use.
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

    /// Discover git repos in a directory, including nested repos when
    /// the path itself is a git repo. A folder inside a repository, with no
    /// repositories below it, resolves to that repository's root. Returns
    /// canonical paths.
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
        // Climbing after repos were found but filtered out would index an
        // enclosing repository the user did not point at.
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

/// The checkout's graph when its commit is already indexed; never starts indexing.
pub fn open_indexed_if_present(repo: Option<PathBuf>, db: Option<PathBuf>) -> Option<IndexedRepo> {
    let repo_path = repo.unwrap_or_else(|| PathBuf::from("."));
    let git = git_info(&git_toplevel(&repo_path).ok()?).ok()?;
    let db_path = resolve_db_path(db, &git).ok()?;
    if graph_lacks_commit(&db_path, &git.commit_sha).unwrap_or(true) {
        return None;
    }
    let client = GraphReader::open(Some(db_path)).ok()?;
    Some(IndexedRepo { git, client })
}

pub fn open_indexed(repo: Option<PathBuf>, db: Option<PathBuf>) -> Result<IndexedRepo> {
    let repo_path = repo.unwrap_or_else(|| PathBuf::from("."));
    let top_level = git_toplevel(&repo_path)
        .with_context(|| format!("failed to find git top-level for {}", repo_path.display()))?;
    let git = git_info(&top_level)
        .with_context(|| format!("failed to read git info for {}", top_level.display()))?;
    let db_path = resolve_db_path(db.clone(), &git)?;

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

/// The repo-relative `files` whose working-tree contents differ from the indexed commit:
/// modified, added, renamed, or untracked. The graph describes the commit, so its definitions
/// and line spans do not fit these files. Empty when git cannot answer. Only `files` are checked,
/// so the cost follows the output rather than the size of the repository.
pub fn edited_since_index(repo: &Path, files: &[&str]) -> std::collections::BTreeSet<String> {
    let mut edited = std::collections::BTreeSet::new();
    if files.is_empty() {
        return edited;
    }
    let git_list = |args: &[&str]| -> Option<Vec<String>> {
        let output = Command::new("git")
            .arg("--literal-pathspecs")
            .arg("-C")
            .arg(repo)
            .args(args)
            .arg("--")
            .args(files)
            .stderr(Stdio::null())
            .output()
            .ok()
            .filter(|output| output.status.success())?;
        Some(
            String::from_utf8_lossy(&output.stdout)
                .split('\0')
                .filter(|field| !field.is_empty())
                .map(str::to_string)
                .collect(),
        )
    };
    let Some(status) = git_list(&[
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=no",
        "--ignore-submodules=all",
    ]) else {
        return edited;
    };
    let mut changed = std::collections::HashSet::new();
    let mut fields = status.into_iter();
    while let Some(entry) = fields.next() {
        let (Some(code), Some(path)) = (entry.get(..2), entry.get(3..)) else {
            continue;
        };
        if code.contains(['R', 'C']) {
            fields.next();
        }
        changed.insert(path.to_string());
    }
    let tracked: std::collections::HashSet<String> = git_list(&["ls-files", "-z"])
        .unwrap_or_default()
        .into_iter()
        .collect();
    edited.extend(
        files
            .iter()
            .filter(|file| changed.contains(**file) || !tracked.contains(**file))
            .map(|file| file.to_string()),
    );
    edited
}

/// Repo-relative forms of `paths` given relative to the current directory, the repo, or absolute.
/// `a,b` lists are split; `path:120-180` ranges keep only the path; unresolvable entries are kept as
/// written (trimmed) so globs still work.
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
    /// Deterministic project ID derived from `repo_path`.
    pub project_id: i64,
    pub branch: String,
    pub commit_sha: String,
    /// For worktrees, the parent repo's canonical path. For regular
    /// repos, same as `repo_path`.
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

/// Resolve the working-tree root of the repo containing `path`.
///
/// `orbit index` walks and stores file paths relative to this root, so a
/// `repo-map` invocation pointed at a subdirectory must anchor here too;
/// otherwise `read_text` globs (resolved against CWD) miss the repo-root-relative
/// paths the graph holds and `api`/`class` silently degrade. This is the git
/// working-tree root (`git rev-parse --show-toplevel`), not
/// [`GitInfo::parent_repo_path`], which for a worktree points at the origin
/// repo instead of this checkout's own root.
pub fn git_toplevel(path: &Path) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(path)
        .output()
        .with_context(|| format!("failed to run git in {}", path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "git rev-parse --show-toplevel failed in {}: {}",
            path.display(),
            stderr.trim()
        );
    }

    let top = String::from_utf8(output.stdout)
        .context("git output is not valid UTF-8")?
        .trim()
        .to_string();
    dunce::canonicalize(&top).with_context(|| format!("failed to canonicalize {top}"))
}

/// Mask clears the sign bit so the result is always a positive i64.
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
    Command::new("git")
        .args(["check-ignore", "--quiet"])
        .arg(repo)
        .current_dir(enclosing)
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn discover_repos(workspace_path: &Path) -> Vec<PathBuf> {
    let ws = CoreGitaliskWorkspaceFolder::new(workspace_path.to_string_lossy().to_string());
    if let Err(e) = ws.index_repositories() {
        let path = workspace_path.display();
        tracing::warn!("repository discovery failed in {path}: {e}");
        return vec![];
    }
    ws.get_repositories()
        .into_iter()
        .map(|r| PathBuf::from(&r.path))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // A file where the parent directory must go makes create_dir_all fail
        // without relying on permissions, which root ignores in CI containers.
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
}
