use std::path::{Path, PathBuf};

use async_trait::async_trait;
#[derive(Debug)]
pub struct CachedRepository {
    pub path: PathBuf,
    pub commit: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RepositoryCacheError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("path traversal detected: {0}")]
    PathTraversal(String),
}

#[async_trait]
pub trait RepositoryCache: Send + Sync {
    async fn get(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CachedRepository>, RepositoryCacheError>;

    async fn save(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        path: &Path,
    ) -> Result<(), RepositoryCacheError>;

    async fn invalidate(&self, project_id: i64, branch: &str) -> Result<(), RepositoryCacheError>;

    fn code_repository_path(&self, project_id: i64, branch: &str) -> PathBuf;

    async fn delete_file(
        &self,
        project_id: i64,
        branch: &str,
        relative_path: &str,
    ) -> Result<(), RepositoryCacheError>;

    async fn write_file(
        &self,
        project_id: i64,
        branch: &str,
        relative_path: &str,
        content: &[u8],
    ) -> Result<(), RepositoryCacheError>;

    async fn update_commit(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
    ) -> Result<(), RepositoryCacheError>;
}

const CACHE_DIR_NAME: &str = "gkg-repository-cache";
const COMMIT_FILE: &str = ".commit";
const META_DIR: &str = "meta";
const REPOSITORY_DIR: &str = "repository";

pub struct LocalRepositoryCache {
    base_dir: PathBuf,
}

impl Default for LocalRepositoryCache {
    fn default() -> Self {
        Self {
            base_dir: std::env::temp_dir().join(CACHE_DIR_NAME),
        }
    }
}

impl LocalRepositoryCache {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    fn branch_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.base_dir.join(project_id.to_string()).join(branch)
    }

    fn repository_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.branch_dir(project_id, branch).join(REPOSITORY_DIR)
    }
}

fn validated_path(base: &Path, relative: &str) -> Result<PathBuf, RepositoryCacheError> {
    let mut depth: i32 = 0;
    for component in Path::new(relative).components() {
        match component {
            std::path::Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return Err(RepositoryCacheError::PathTraversal(relative.to_string()));
                }
            }
            std::path::Component::Normal(_) => {
                depth += 1;
            }
            _ => {}
        }
    }
    Ok(base.join(relative))
}

#[async_trait]
impl RepositoryCache for LocalRepositoryCache {
    async fn get(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CachedRepository>, RepositoryCacheError> {
        let branch_dir = self.branch_dir(project_id, branch);
        let commit_file = branch_dir.join(META_DIR).join(COMMIT_FILE);

        let commit = match tokio::fs::read_to_string(&commit_file).await {
            Ok(content) => content.trim().to_string(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        Ok(Some(CachedRepository {
            path: branch_dir.join(REPOSITORY_DIR),
            commit,
        }))
    }

    async fn save(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        _path: &Path,
    ) -> Result<(), RepositoryCacheError> {
        let branch_dir = self.branch_dir(project_id, branch);
        let meta_dir = branch_dir.join(META_DIR);
        let repository_dir = branch_dir.join(REPOSITORY_DIR);
        tokio::fs::create_dir_all(&meta_dir).await?;
        tokio::fs::create_dir_all(&repository_dir).await?;
        tokio::fs::write(meta_dir.join(COMMIT_FILE), commit_sha).await?;
        Ok(())
    }

    async fn invalidate(&self, project_id: i64, branch: &str) -> Result<(), RepositoryCacheError> {
        let branch_dir = self.branch_dir(project_id, branch);
        match tokio::fs::remove_dir_all(&branch_dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn code_repository_path(&self, project_id: i64, branch: &str) -> PathBuf {
        self.repository_dir(project_id, branch)
    }

    async fn delete_file(
        &self,
        project_id: i64,
        branch: &str,
        relative_path: &str,
    ) -> Result<(), RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);
        let target = validated_path(&repo_dir, relative_path)?;
        match tokio::fs::remove_file(&target).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_file(
        &self,
        project_id: i64,
        branch: &str,
        relative_path: &str,
        content: &[u8],
    ) -> Result<(), RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);
        let target = validated_path(&repo_dir, relative_path)?;
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&target, content).await?;
        Ok(())
    }

    async fn update_commit(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
    ) -> Result<(), RepositoryCacheError> {
        let meta_dir = self.branch_dir(project_id, branch).join(META_DIR);
        tokio::fs::create_dir_all(&meta_dir).await?;
        tokio::fs::write(meta_dir.join(COMMIT_FILE), commit_sha).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_cache() -> (TempDir, LocalRepositoryCache) {
        let temp_dir = TempDir::new().unwrap();
        let cache = LocalRepositoryCache::new(temp_dir.path().to_path_buf());
        (temp_dir, cache)
    }

    #[tokio::test]
    async fn get_returns_none_when_no_cache_exists() {
        let (_dir, cache) = create_cache();

        let result = cache.get(42, "main").await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn save_then_get_returns_cached_repository() {
        let (_dir, cache) = create_cache();
        let branch_dir = cache.branch_dir(42, "main");

        cache.save(42, "main", "abc123", &branch_dir).await.unwrap();
        let cached = cache.get(42, "main").await.unwrap().unwrap();

        assert_eq!(cached.path, branch_dir.join("repository"));
        assert_eq!(cached.commit, "abc123");
    }

    #[tokio::test]
    async fn save_overwrites_previous_commit() {
        let (_dir, cache) = create_cache();
        let branch_dir = cache.branch_dir(42, "main");

        cache.save(42, "main", "abc123", &branch_dir).await.unwrap();
        cache.save(42, "main", "def456", &branch_dir).await.unwrap();

        let cached = cache.get(42, "main").await.unwrap().unwrap();
        assert_eq!(cached.commit, "def456");
    }

    #[tokio::test]
    async fn invalidate_removes_cached_repository() {
        let (_dir, cache) = create_cache();
        let branch_dir = cache.branch_dir(42, "main");

        cache.save(42, "main", "abc123", &branch_dir).await.unwrap();
        cache.invalidate(42, "main").await.unwrap();
        let result = cache.get(42, "main").await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn invalidate_succeeds_when_no_cache_exists() {
        let (_dir, cache) = create_cache();

        cache.invalidate(42, "main").await.unwrap();
    }

    #[tokio::test]
    async fn separate_branches_are_independent() {
        let (_dir, cache) = create_cache();
        let main_dir = cache.branch_dir(42, "main");
        let develop_dir = cache.branch_dir(42, "develop");

        cache.save(42, "main", "aaa", &main_dir).await.unwrap();
        cache
            .save(42, "develop", "bbb", &develop_dir)
            .await
            .unwrap();

        assert!(cache.get(42, "main").await.unwrap().is_some());
        assert!(cache.get(42, "develop").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn separate_projects_are_independent() {
        let (_dir, cache) = create_cache();
        let dir_a = cache.branch_dir(1, "main");
        let dir_b = cache.branch_dir(2, "main");

        cache.save(1, "main", "aaa", &dir_a).await.unwrap();
        cache.save(2, "main", "bbb", &dir_b).await.unwrap();

        assert!(cache.get(1, "main").await.unwrap().is_some());
        assert!(cache.get(2, "main").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn invalidate_one_branch_preserves_others() {
        let (_dir, cache) = create_cache();
        let main_dir = cache.branch_dir(42, "main");
        let develop_dir = cache.branch_dir(42, "develop");

        cache.save(42, "main", "aaa", &main_dir).await.unwrap();
        cache
            .save(42, "develop", "bbb", &develop_dir)
            .await
            .unwrap();

        cache.invalidate(42, "main").await.unwrap();

        assert!(cache.get(42, "main").await.unwrap().is_none());
        assert!(cache.get(42, "develop").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn branch_dir_uses_expected_path_structure() {
        let (dir, cache) = create_cache();

        let path = cache.branch_dir(42, "main");

        assert_eq!(path, dir.path().join("42/main"));
    }

    #[tokio::test]
    async fn write_file_creates_parent_directories() {
        let (_dir, cache) = create_cache();
        cache
            .save(42, "main", "abc123", Path::new(""))
            .await
            .unwrap();

        cache
            .write_file(42, "main", "src/deep/file.rs", b"content")
            .await
            .unwrap();

        let repo_dir = cache.repository_dir(42, "main");
        let content = tokio::fs::read_to_string(repo_dir.join("src/deep/file.rs"))
            .await
            .unwrap();
        assert_eq!(content, "content");
    }

    #[tokio::test]
    async fn delete_file_removes_existing_file() {
        let (_dir, cache) = create_cache();
        cache
            .save(42, "main", "abc123", Path::new(""))
            .await
            .unwrap();
        cache
            .write_file(42, "main", "file.rs", b"content")
            .await
            .unwrap();

        cache.delete_file(42, "main", "file.rs").await.unwrap();

        let repo_dir = cache.repository_dir(42, "main");
        assert!(!repo_dir.join("file.rs").exists());
    }

    #[tokio::test]
    async fn delete_file_succeeds_when_file_does_not_exist() {
        let (_dir, cache) = create_cache();
        cache
            .save(42, "main", "abc123", Path::new(""))
            .await
            .unwrap();

        cache
            .delete_file(42, "main", "nonexistent.rs")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn update_commit_changes_stored_sha() {
        let (_dir, cache) = create_cache();
        cache
            .save(42, "main", "abc123", Path::new(""))
            .await
            .unwrap();

        cache.update_commit(42, "main", "def456").await.unwrap();

        let cached = cache.get(42, "main").await.unwrap().unwrap();
        assert_eq!(cached.commit, "def456");
    }

    #[tokio::test]
    async fn write_file_rejects_path_traversal() {
        let (_dir, cache) = create_cache();
        cache
            .save(42, "main", "abc123", Path::new(""))
            .await
            .unwrap();

        let result = cache
            .write_file(42, "main", "../../../etc/passwd", b"bad")
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path traversal"));
    }

    #[tokio::test]
    async fn delete_file_rejects_path_traversal() {
        let (_dir, cache) = create_cache();
        cache
            .save(42, "main", "abc123", Path::new(""))
            .await
            .unwrap();

        let result = cache.delete_file(42, "main", "../../../etc/passwd").await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path traversal"));
    }

    #[tokio::test]
    async fn preserves_files_in_repository_directory() {
        let (_dir, cache) = create_cache();
        let branch_dir = cache.branch_dir(42, "main");
        let repository_dir = branch_dir.join("repository");
        tokio::fs::create_dir_all(&repository_dir).await.unwrap();

        let test_file = repository_dir.join("src/main.rs");
        tokio::fs::create_dir_all(test_file.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&test_file, "fn main() {}").await.unwrap();

        cache.save(42, "main", "abc123", &branch_dir).await.unwrap();
        let cached = cache.get(42, "main").await.unwrap().unwrap();

        let content = tokio::fs::read_to_string(cached.path.join("src/main.rs"))
            .await
            .unwrap();
        assert_eq!(content, "fn main() {}");
    }
}
