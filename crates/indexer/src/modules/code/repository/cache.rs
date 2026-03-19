use std::path::PathBuf;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct CachedRepository {
    pub path: PathBuf,
    pub commit: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RepositoryCacheError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("archive extraction failed: {0}")]
    Archive(String),
}

#[async_trait]
pub trait RepositoryCache: Send + Sync {
    async fn get(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CachedRepository>, RepositoryCacheError>;

    async fn invalidate(&self, project_id: i64, branch: &str) -> Result<(), RepositoryCacheError>;

    async fn extract_archive(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        archive_bytes: &[u8],
    ) -> Result<PathBuf, RepositoryCacheError>;
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
        self.base_dir
            .join(project_id.to_string())
            .join(hashed_branch_name(branch))
    }

    fn repository_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.branch_dir(project_id, branch).join(REPOSITORY_DIR)
    }
}

fn hashed_branch_name(branch: &str) -> String {
    let hash = Sha256::digest(branch.as_bytes());
    format!("{:x}", hash)
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

        let repository_dir = branch_dir.join(REPOSITORY_DIR);
        match tokio::fs::metadata(&repository_dir).await {
            Ok(meta) if meta.is_dir() => {}
            _ => return Ok(None),
        }

        Ok(Some(CachedRepository {
            path: repository_dir,
            commit,
        }))
    }

    async fn invalidate(&self, project_id: i64, branch: &str) -> Result<(), RepositoryCacheError> {
        let branch_dir = self.branch_dir(project_id, branch);
        match tokio::fs::remove_dir_all(&branch_dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn extract_archive(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        archive_bytes: &[u8],
    ) -> Result<PathBuf, RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);

        match tokio::fs::remove_dir_all(&repo_dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        tokio::fs::create_dir_all(&repo_dir).await?;

        crate::modules::code::archive::extract_tar_gz(archive_bytes, &repo_dir)
            .map_err(|e| RepositoryCacheError::Archive(e.to_string()))?;

        let meta_dir = self.branch_dir(project_id, branch).join(META_DIR);
        tokio::fs::create_dir_all(&meta_dir).await?;
        tokio::fs::write(meta_dir.join(COMMIT_FILE), commit_sha).await?;

        Ok(repo_dir)
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

    fn build_tar_gz(files: &[(&str, &[u8])]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut tar_builder = tar::Builder::new(Vec::new());
        for (path, content) in files {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar_builder.append(&header, &content[..]).unwrap();
        }
        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[tokio::test]
    async fn get_returns_none_when_no_cache_exists() {
        let (_dir, cache) = create_cache();

        let result = cache.get(42, "main").await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn invalidate_removes_cached_repository() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);
        cache
            .extract_archive(42, "main", "abc123", &archive)
            .await
            .unwrap();

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
        let archive = build_tar_gz(&[("file.rs", b"content")]);

        cache
            .extract_archive(42, "main", "aaa", &archive)
            .await
            .unwrap();
        cache
            .extract_archive(42, "develop", "bbb", &archive)
            .await
            .unwrap();

        assert!(cache.get(42, "main").await.unwrap().is_some());
        assert!(cache.get(42, "develop").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn separate_projects_are_independent() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);

        cache
            .extract_archive(1, "main", "aaa", &archive)
            .await
            .unwrap();
        cache
            .extract_archive(2, "main", "bbb", &archive)
            .await
            .unwrap();

        assert!(cache.get(1, "main").await.unwrap().is_some());
        assert!(cache.get(2, "main").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn invalidate_one_branch_preserves_others() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);

        cache
            .extract_archive(42, "main", "aaa", &archive)
            .await
            .unwrap();
        cache
            .extract_archive(42, "develop", "bbb", &archive)
            .await
            .unwrap();

        cache.invalidate(42, "main").await.unwrap();

        assert!(cache.get(42, "main").await.unwrap().is_none());
        assert!(cache.get(42, "develop").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn branch_dir_hashes_branch_name() {
        let (dir, cache) = create_cache();

        let path = cache.branch_dir(42, "main");

        let expected_hash = hashed_branch_name("main");
        assert_eq!(path, dir.path().join(format!("42/{expected_hash}")));
    }

    #[tokio::test]
    async fn branch_dir_hashes_away_path_traversal_characters() {
        let (dir, cache) = create_cache();

        let safe_path = cache.branch_dir(42, "../../../tmp/evil");

        assert!(safe_path.starts_with(dir.path().join("42")));
        assert!(!safe_path.to_string_lossy().contains(".."));
    }

    #[tokio::test]
    async fn extract_archive_populates_cache() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            ("src/main.rs", b"fn main() {}"),
            ("src/lib.rs", b"pub mod lib;"),
        ]);

        let path = cache
            .extract_archive(42, "main", "abc123", &archive)
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(path.join("src/main.rs"))
            .await
            .unwrap();
        assert_eq!(content, "fn main() {}");
        let content = tokio::fs::read_to_string(path.join("src/lib.rs"))
            .await
            .unwrap();
        assert_eq!(content, "pub mod lib;");

        let cached = cache.get(42, "main").await.unwrap().unwrap();
        assert_eq!(cached.commit, "abc123");
    }

    #[tokio::test]
    async fn extract_archive_replaces_existing_files() {
        let (_dir, cache) = create_cache();
        let first_archive = build_tar_gz(&[("old_file.rs", b"old content")]);
        cache
            .extract_archive(42, "main", "commit1", &first_archive)
            .await
            .unwrap();

        let second_archive = build_tar_gz(&[("new_file.rs", b"new content")]);
        let path = cache
            .extract_archive(42, "main", "commit2", &second_archive)
            .await
            .unwrap();

        assert!(!path.join("old_file.rs").exists());
        let content = tokio::fs::read_to_string(path.join("new_file.rs"))
            .await
            .unwrap();
        assert_eq!(content, "new content");

        let cached = cache.get(42, "main").await.unwrap().unwrap();
        assert_eq!(cached.commit, "commit2");
    }
}
