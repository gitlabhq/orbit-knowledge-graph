use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fs4::tokio::AsyncFileExt;
use gitalisk_core::workspace_folder::gitalisk_workspace::CoreGitaliskWorkspaceFolder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use typed_path::Utf8TypedPath;

const INDEXES_DIR: &str = "indexes";
const MANIFEST_FILE: &str = "manifest.json";
const LOCK_FILE: &str = ".lock";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Indexed,
    Indexing,
    Error,
    Pending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectEntry {
    pub dir_name: String,
    pub status: Status,
    pub last_indexed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}

/// Manifest keyed by canonical repo path.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Manifest {
    pub projects: HashMap<String, ProjectEntry>,
}

/// Manages `~/.orbit/` — indexes directory, manifest, and advisory lock.
pub struct IndexStore {
    indexes_dir: PathBuf,
    manifest_path: PathBuf,
    lock_path: PathBuf,
}

impl IndexStore {
    pub fn open_default() -> Result<Self> {
        let home = dirs::home_dir().context("Could not determine home directory")?;
        Self::open(home.join(".orbit"))
    }

    pub fn open(root: PathBuf) -> Result<Self> {
        let indexes_dir = root.join(INDEXES_DIR);
        std::fs::create_dir_all(&indexes_dir)?;
        Ok(Self {
            manifest_path: root.join(MANIFEST_FILE),
            lock_path: root.join(LOCK_FILE),
            indexes_dir,
        })
    }

    async fn lock(&self) -> Result<tokio::fs::File> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.lock_path)
            .await
            .context("Failed to open lock file")?;
        file.lock_exclusive()
            .map_err(|e| anyhow::anyhow!("Failed to lock {}: {e}", self.lock_path.display()))?;
        Ok(file)
    }

    async fn load_manifest(&self) -> Result<Manifest> {
        match tokio::fs::read_to_string(&self.manifest_path).await {
            Ok(data) => serde_json::from_str(&data).context("Failed to parse manifest"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Manifest::default()),
            Err(e) => Err(e).context("Failed to read manifest"),
        }
    }

    async fn save_manifest(&self, manifest: &Manifest) -> Result<()> {
        let data = serde_json::to_string_pretty(manifest)?;
        tokio::fs::write(&self.manifest_path, data).await?;
        Ok(())
    }

    /// Discover git repos in a directory, register each, and return the
    /// canonical paths. If the path itself is a git repo, returns just that.
    pub async fn resolve_repos(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let canonical = dunce::canonicalize(path)?;

        let repo_paths = if is_git_repo(&canonical) {
            vec![canonical]
        } else {
            discover_repos(&canonical)
        };

        let _lock = self.lock().await?;
        let mut manifest = self.load_manifest().await?;

        for repo in &repo_paths {
            let key = repo.to_string_lossy().to_string();
            if let std::collections::hash_map::Entry::Vacant(e) = manifest.projects.entry(key) {
                let dir_name = path_to_dir_name(repo);
                std::fs::create_dir_all(self.indexes_dir.join(&dir_name))?;
                e.insert(ProjectEntry {
                    dir_name,
                    status: Status::Pending,
                    last_indexed_at: None,
                    error_message: None,
                });
            }
        }

        self.save_manifest(&manifest).await?;
        Ok(repo_paths)
    }

    pub fn db_path(&self, repo_path: &str) -> PathBuf {
        let dir_name = path_to_dir_name(std::path::Path::new(repo_path));
        self.indexes_dir.join(&dir_name).join("graph.duckdb")
    }

    pub async fn set_status(
        &self,
        repo_path: &str,
        status: Status,
        error: Option<String>,
    ) -> Result<()> {
        let _lock = self.lock().await?;
        let mut manifest = self.load_manifest().await?;
        if let Some(entry) = manifest.projects.get_mut(repo_path) {
            entry.error_message = error;
            if status == Status::Indexed {
                entry.last_indexed_at = Some(Utc::now());
            }
            entry.status = status;
        }
        self.save_manifest(&manifest).await
    }
}

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

/// Splits a path into components via `typed_path` (auto-detects Unix/Windows),
/// percent-encodes each component, then joins with `-`.
///
/// `/Users/alice/my-repo`    → `Users-alice-my%2Drepo`
/// `/Users/alice/my project` → `Users-alice-my%20project`
/// `C:\Users\alice\src`      → `C-Users-alice-src`
fn path_to_dir_name(path: &Path) -> String {
    let s = path.to_string_lossy();
    Utf8TypedPath::derive(&s)
        .components()
        .filter(|c| !c.is_root() && !c.is_current() && !c.is_parent())
        .map(|c| encode_component(c.as_str().trim_end_matches(':')))
        .collect::<Vec<_>>()
        .join("-")
}

fn encode_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'.' => out.push(byte as char),
            _ => {
                out.push('%');
                out.push(char::from(HEX[byte as usize >> 4]));
                out.push(char::from(HEX[byte as usize & 0xf]));
            }
        }
    }
    out
}

const HEX: &[u8; 16] = b"0123456789ABCDEF";

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
    fn test_path_to_dir_name() {
        assert_eq!(
            path_to_dir_name(Path::new("/Users/alice/src/frontend")),
            "Users-alice-src-frontend"
        );
        // dashes in components are encoded so /a-b and /a/b don't collide
        assert_eq!(
            path_to_dir_name(Path::new("/Users/alice/my-repo")),
            "Users-alice-my%2Drepo"
        );
        assert_eq!(
            path_to_dir_name(Path::new("/Users/alice/my project")),
            "Users-alice-my%20project"
        );
        assert_eq!(
            path_to_dir_name(Path::new("C:\\Users\\alice\\src")),
            "C-Users-alice-src"
        );
        assert_eq!(path_to_dir_name(Path::new("/tmp/a@b#c")), "tmp-a%40b%23c");
        assert_eq!(
            path_to_dir_name(Path::new("/tmp/100%done")),
            "tmp-100%25done"
        );
    }

    #[tokio::test]
    async fn test_resolve_single_repo() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = IndexStore::open(temp.path().join("orbit")).unwrap();

        let repo = temp.path().join("my-repo");
        init_repo(&repo);

        let repos = store.resolve_repos(&repo).await.unwrap();
        assert_eq!(repos.len(), 1);
    }

    #[tokio::test]
    async fn test_resolve_workspace() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = IndexStore::open(temp.path().join("orbit")).unwrap();

        let workspace = temp.path().join("workspace");
        init_repo(&workspace.join("repo-a"));
        init_repo(&workspace.join("repo-b"));
        std::fs::create_dir_all(workspace.join("not-a-repo")).unwrap();

        let repos = store.resolve_repos(&workspace).await.unwrap();
        assert_eq!(repos.len(), 2);
    }

    #[tokio::test]
    async fn test_resolve_idempotent() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = IndexStore::open(temp.path().join("orbit")).unwrap();

        let repo = temp.path().join("repo");
        init_repo(&repo);

        store.resolve_repos(&repo).await.unwrap();
        store.resolve_repos(&repo).await.unwrap();

        let manifest = store.load_manifest().await.unwrap();
        assert_eq!(manifest.projects.len(), 1);
    }

    #[tokio::test]
    async fn test_set_status() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = IndexStore::open(temp.path().join("orbit")).unwrap();

        let repo = temp.path().join("repo");
        init_repo(&repo);

        let repos = store.resolve_repos(&repo).await.unwrap();
        let key = repos[0].to_string_lossy().to_string();

        store.set_status(&key, Status::Indexed, None).await.unwrap();

        let manifest = store.load_manifest().await.unwrap();
        let entry = manifest.projects.get(&key).unwrap();
        assert_eq!(entry.status, Status::Indexed);
        assert!(entry.last_indexed_at.is_some());
    }

    #[tokio::test]
    async fn test_lock_released_between_calls() {
        let temp = tempfile::TempDir::new().unwrap();
        let store = IndexStore::open(temp.path().join("orbit")).unwrap();

        let repo = temp.path().join("repo");
        init_repo(&repo);

        store.resolve_repos(&repo).await.unwrap();
        store
            .set_status(
                &repo.canonicalize().unwrap().to_string_lossy(),
                Status::Indexing,
                None,
            )
            .await
            .unwrap();
    }
}
