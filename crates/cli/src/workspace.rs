use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb_client::DuckDbClient;
use gitalisk_core::workspace_folder::gitalisk_workspace::CoreGitaliskWorkspaceFolder;
use serde_json::json;

/// Repo indexing status, stored as a DuckDB `repo_status` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepoStatus {
    Indexing,
    Indexed,
    Error,
}

impl fmt::Display for RepoStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl RepoStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Indexing => "indexing",
            Self::Indexed => "indexed",
            Self::Error => "error",
        }
    }
}

/// Manages `~/.orbit/` — DuckDB graph file, repo discovery, and manifest.
pub struct IndexStore {
    root: PathBuf,
}

impl IndexStore {
    pub fn open_default() -> Result<Self> {
        let home = dirs::home_dir().context("Could not determine home directory")?;
        Self::open(home.join(".orbit"))
    }

    pub fn open(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn db_path(&self) -> PathBuf {
        self.root.join("graph.duckdb")
    }

    /// Discover git repos in a directory. If the path itself is a git
    /// repo, returns just that. Returns canonical paths.
    pub fn resolve_repos(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let canonical = dunce::canonicalize(path)?;

        if is_git_repo(&canonical) {
            Ok(vec![canonical])
        } else {
            Ok(discover_repos(&canonical))
        }
    }
}

// ── Manifest operations ─────────────────────────────────────────────────────

/// Insert or update a repo in the manifest table.
pub fn upsert_manifest(
    client: &DuckDbClient,
    repo_path: &str,
    project_id: i64,
    status: RepoStatus,
    error_message: Option<&str>,
) -> Result<()> {
    client
        .execute(
            "INSERT INTO _orbit_manifest (repo_path, project_id, status, error_message, last_indexed_at)
             VALUES (?1, ?2, ?3::repo_status, ?4, CASE WHEN ?3 = 'indexed' THEN now() ELSE NULL END)
             ON CONFLICT (repo_path) DO UPDATE SET
                 status = ?3::repo_status,
                 error_message = ?4,
                 last_indexed_at = CASE WHEN ?3 = 'indexed' THEN now() ELSE last_indexed_at END",
            &[
                json!(repo_path),
                json!(project_id),
                json!(status.as_str()),
                error_message.map_or(serde_json::Value::Null, |s| json!(s)),
            ],
        )
        .context("failed to upsert manifest")?;
    Ok(())
}

/// Return canonical paths of all indexed repos.
pub fn repo_roots(client: &DuckDbClient) -> Result<Vec<PathBuf>> {
    let paths = client
        .query_strings(
            "SELECT repo_path FROM _orbit_manifest WHERE status = 'indexed'",
            &[],
        )
        .context("failed to query repo roots")?;
    Ok(paths.into_iter().map(PathBuf::from).collect())
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
        let store = IndexStore::open(temp.path().join("orbit")).unwrap();

        let repo = temp.path().join("my-repo");
        init_repo(&repo);

        let repos = store.resolve_repos(&repo).unwrap();
        assert_eq!(repos.len(), 1);
    }

    #[test]
    fn test_resolve_workspace() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = IndexStore::open(temp.path().join("orbit")).unwrap();

        let workspace = temp.path().join("workspace");
        init_repo(&workspace.join("repo-a"));
        init_repo(&workspace.join("repo-b"));
        std::fs::create_dir_all(workspace.join("not-a-repo")).unwrap();

        let repos = store.resolve_repos(&workspace).unwrap();
        assert_eq!(repos.len(), 2);
    }
}
