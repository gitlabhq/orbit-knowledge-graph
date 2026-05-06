use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb_client::DuckDbClient;
use gitalisk_core::repository::gitalisk_repository::CoreGitaliskRepository;
use gitalisk_core::workspace_folder::gitalisk_workspace::CoreGitaliskWorkspaceFolder;
use serde_json::json;
use strum::{AsRefStr, Display};

/// Repo indexing status, stored as a DuckDB `repo_status` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum RepoStatus {
    Indexing,
    Indexed,
    Error,
}

/// Manages the `~/.orbit/` workspace — graph database, repo discovery,
/// and manifest.
pub struct Workspace {
    root: PathBuf,
}

impl Workspace {
    pub fn open_default() -> Result<Self> {
        let root = if let Some(dir) = std::env::var("ORBIT_DATA_DIR")
            .ok()
            .filter(|s| !s.is_empty())
        {
            PathBuf::from(dir)
        } else {
            let home = dirs::home_dir().context("Could not determine home directory")?;
            home.join(".orbit")
        };
        Self::open(root)
    }

    pub fn open(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn db_path(&self) -> PathBuf {
        self.root.join("graph.duckdb")
    }

    /// Discover git repos in a directory, including nested repos when
    /// the path itself is a git repo. Returns canonical paths.
    pub fn resolve_repos(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let canonical = dunce::canonicalize(path)?;

        let discovered = discover_repos(&canonical);
        if discovered.is_empty() && is_git_repo(&canonical) {
            Ok(vec![canonical])
        } else {
            Ok(discovered)
        }
    }

    /// Return `project_id -> repo_path` mapping for all indexed repos.
    pub fn project_roots(&self) -> Result<HashMap<i64, PathBuf>> {
        let client = DuckDbClient::open_read_only(&self.db_path())
            .context("failed to open DuckDB for manifest read")?;
        let batches = client
            .query_arrow(
                "SELECT project_id, repo_path FROM _orbit_manifest WHERE status = 'indexed'",
            )
            .context("failed to query project roots")?;

        use arrow::datatypes::Int64Type;
        use gkg_utils::arrow::ArrowUtils;

        let mut map = HashMap::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                if let (Some(pid), Some(path)) = (
                    ArrowUtils::get_column::<Int64Type>(batch, "project_id", row),
                    ArrowUtils::get_column_string(batch, "repo_path", row),
                ) {
                    map.insert(pid, PathBuf::from(path));
                }
            }
        }
        Ok(map)
    }
}

/// Update manifest status on the given client connection.
pub fn set_status(
    client: &DuckDbClient,
    repo_path: &str,
    project_id: i64,
    status: RepoStatus,
    error_message: Option<&str>,
    git: Option<&GitInfo>,
) -> Result<()> {
    let parent = git.map(|g| g.parent_repo_path.to_string_lossy().to_string());
    let branch = git.map(|g| g.branch.as_str());
    let commit = git.map(|g| g.commit_sha.as_str());

    client
        .execute(
            "INSERT INTO _orbit_manifest (repo_path, project_id, parent_repo_path, branch, commit_sha, status, error_message, last_indexed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6::repo_status, ?7, CASE WHEN ?6 = 'indexed' THEN now() ELSE NULL END)
             ON CONFLICT (repo_path) DO UPDATE SET
                 parent_repo_path = COALESCE(?3, parent_repo_path),
                 branch = COALESCE(?4, branch),
                 commit_sha = COALESCE(?5, commit_sha),
                 status = ?6::repo_status,
                 error_message = ?7,
                 last_indexed_at = CASE WHEN ?6 = 'indexed' THEN now() ELSE last_indexed_at END",
            &[
                json!(repo_path),
                json!(project_id),
                parent.map_or(serde_json::Value::Null, |s| json!(s)),
                branch.map_or(serde_json::Value::Null, |s| json!(s)),
                commit.map_or(serde_json::Value::Null, |s| json!(s)),
                json!(status.as_ref()),
                error_message.map_or(serde_json::Value::Null, |s| json!(s)),
            ],
        )
        .context("failed to upsert manifest")?;
    Ok(())
}

/// Git metadata for an indexed repository.
pub struct GitInfo {
    /// Canonical repo path.
    pub repo_path: PathBuf,
    repository: CoreGitaliskRepository,
    /// Deterministic project ID derived from `repo_path`.
    pub project_id: i64,
    pub branch: String,
    pub commit_sha: String,
    /// For worktrees, the parent repo's canonical path. For regular
    /// repos, same as `repo_path`.
    pub parent_repo_path: PathBuf,
}

impl GitInfo {
    pub fn repository(&self) -> &CoreGitaliskRepository {
        &self.repository
    }
}

/// Resolve git metadata (branch, commit, parent repo) for a repo path.
pub fn git_info(repo_path: &Path) -> Result<GitInfo> {
    let canonical = dunce::canonicalize(repo_path)
        .with_context(|| format!("failed to canonicalize {}", repo_path.display()))?;
    let path_str = canonical.to_string_lossy().to_string();
    let project_id = project_id_from_path(&path_str);

    let repo = CoreGitaliskRepository::new(path_str.clone(), path_str);

    let branch = repo
        .get_current_branch()
        .context("failed to get current branch")?;
    let commit_sha = repo
        .get_current_commit_hash()
        .context("failed to get current commit hash")?;
    let parent_repo_path = repo
        .parent_repo_path()
        .context("failed to resolve parent repo path")?;

    Ok(GitInfo {
        repo_path: canonical,
        repository: repo,
        project_id,
        branch,
        commit_sha,
        parent_repo_path,
    })
}

/// Deterministic project ID from canonical path. Mask clears the sign bit
/// so the result is always a positive i64.
pub fn project_id_from_path(path: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn is_git_repo(path: &Path) -> bool {
    let git = path.join(".git");
    git.is_dir() || git.is_file()
}

fn discover_repos(workspace_path: &Path) -> Vec<PathBuf> {
    let ws = CoreGitaliskWorkspaceFolder::new(workspace_path.to_string_lossy().to_string());
    if ws.index_repositories().is_err() {
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
    use std::process::Command;

    fn init_repo(path: &Path) {
        std::fs::create_dir_all(path).unwrap();
        Command::new("git")
            .args(["init"])
            .current_dir(path)
            .output()
            .unwrap();
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
        // Should find the root repo AND both nested repos.
        assert!(
            repos.len() >= 3,
            "expected at least 3 repos, got {}: {:?}",
            repos.len(),
            repos
        );
    }
}
