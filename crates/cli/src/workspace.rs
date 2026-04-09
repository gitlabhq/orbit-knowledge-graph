use anyhow::{Context, Result};
use gitalisk_core::workspace_folder::gitalisk_workspace::CoreGitaliskWorkspaceFolder;
use std::path::{Path, PathBuf};
use typed_path::Utf8TypedPath;

/// Manages `~/.orbit/` — DuckDB graph file and repo discovery.
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

/// Deterministic project ID from canonical path. Mask clears the sign bit
/// so the result is always a positive i64.
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
#[allow(dead_code)]
fn path_to_dir_name(path: &Path) -> String {
    let s = path.to_string_lossy();
    Utf8TypedPath::derive(&s)
        .components()
        .filter(|c| !c.is_root() && !c.is_current() && !c.is_parent())
        .map(|c| encode_component(c.as_str().trim_end_matches(':')))
        .collect::<Vec<_>>()
        .join("-")
}

#[allow(dead_code)]
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
