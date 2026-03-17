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
}

const COMMIT_FILE: &str = ".commit";
const REPOSITORY_DIR: &str = "repository";

pub struct LocalRepositoryCache {
    base_dir: PathBuf,
}

impl LocalRepositoryCache {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    fn branch_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.base_dir.join(project_id.to_string()).join(branch)
    }
}

#[async_trait]
impl RepositoryCache for LocalRepositoryCache {
    async fn get(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CachedRepository>, RepositoryCacheError> {
        let branch_dir = self.branch_dir(project_id, branch);
        let commit_file = branch_dir.join(COMMIT_FILE);

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
        let repository_dir = branch_dir.join(REPOSITORY_DIR);
        tokio::fs::create_dir_all(&repository_dir).await?;
        tokio::fs::write(branch_dir.join(COMMIT_FILE), commit_sha).await?;
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
