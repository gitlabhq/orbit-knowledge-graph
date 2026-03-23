use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::StreamExt;
use tokio_util::io::{StreamReader, SyncIoBridge};

use super::cache_budget::{
    CacheBudget, CacheEntryGuard, directory_size, hashed_branch_name,
};
use super::service::ByteStream;
use crate::configuration::RepositoryCacheConfiguration;
use crate::modules::code::archive::extract_tar_gz_from_reader;
use crate::modules::code::metrics::CodeMetrics;

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

    async fn rename_file(
        &self,
        project_id: i64,
        branch: &str,
        old_path: &str,
        new_path: &str,
    ) -> Result<(), RepositoryCacheError>;

    async fn update_commit(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
    ) -> Result<(), RepositoryCacheError>;

    async fn extract_archive(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        archive_stream: ByteStream,
    ) -> Result<PathBuf, RepositoryCacheError>;

    /// Pin an entry so it cannot be evicted. Returns a guard that unpins on drop.
    fn pin(&self, project_id: i64, branch: &str) -> CacheEntryGuard;

    /// Atomically look up a cached entry and pin it if found.
    ///
    /// This holds the eviction read lock across both operations so that an
    /// in-flight eviction cannot delete the entry between the lookup and the pin.
    async fn get_pinned(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<(CachedRepository, CacheEntryGuard)>, RepositoryCacheError>;

    /// Measure the entry's size on disk, record it in the budget index,
    /// evict other entries if the cache is over budget, and return whether
    /// this entry is small enough to be cleaned up after indexing.
    async fn record_and_evict(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<bool, RepositoryCacheError>;
}

const COMMIT_FILE: &str = ".commit";
const META_DIR: &str = "meta";
const REPOSITORY_DIR: &str = "repository";

pub struct LocalRepositoryCache {
    base_dir: PathBuf,
    budget: CacheBudget,
}

impl LocalRepositoryCache {
    pub fn new(
        base_dir: PathBuf,
        config: &RepositoryCacheConfiguration,
        code_worker_count: usize,
        metrics: CodeMetrics,
    ) -> Self {
        let usable_budget = config.usable_budget(code_worker_count);
        if usable_budget == 0 {
            tracing::warn!(
                disk_budget_bytes = config.disk_budget_bytes,
                headroom_per_worker_bytes = config.headroom_per_worker_bytes,
                code_worker_count,
                "usable cache budget is 0 — every write will trigger full eviction; \
                 increase disk_budget_bytes or decrease headroom_per_worker_bytes"
            );
        }
        let budget = CacheBudget::new(
            base_dir.clone(),
            usable_budget,
            config.disk_budget_bytes,
            config.large_repo_threshold_bytes,
            metrics,
        );
        Self { base_dir, budget }
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

fn validated_path(base: &Path, relative: &str) -> Result<PathBuf, RepositoryCacheError> {
    let mut depth: i32 = 0;
    for component in Path::new(relative).components() {
        match component {
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                return Err(RepositoryCacheError::PathTraversal(relative.to_string()));
            }
            std::path::Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return Err(RepositoryCacheError::PathTraversal(relative.to_string()));
                }
            }
            std::path::Component::Normal(_) => {
                depth += 1;
            }
            std::path::Component::CurDir => {}
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

        let repository_dir = branch_dir.join(REPOSITORY_DIR);
        match tokio::fs::metadata(&repository_dir).await {
            Ok(meta) if meta.is_dir() => {}
            _ => return Ok(None),
        }

        self.budget.touch(project_id, branch);

        Ok(Some(CachedRepository {
            path: repository_dir,
            commit,
        }))
    }

    async fn save(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
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
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        self.budget.remove(project_id, branch);
        Ok(())
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

    async fn rename_file(
        &self,
        project_id: i64,
        branch: &str,
        old_path: &str,
        new_path: &str,
    ) -> Result<(), RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);
        let source = validated_path(&repo_dir, old_path)?;
        let destination = validated_path(&repo_dir, new_path)?;
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::rename(&source, &destination).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
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

    async fn extract_archive(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        archive_stream: ByteStream,
    ) -> Result<PathBuf, RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);

        match tokio::fs::remove_dir_all(&repo_dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        tokio::fs::create_dir_all(&repo_dir).await?;

        let reader = StreamReader::new(archive_stream.map(|r| r.map_err(std::io::Error::other)));
        let handle = tokio::runtime::Handle::current();
        let repo_dir_owned = repo_dir.clone();
        tokio::task::spawn_blocking(move || {
            let bridge = SyncIoBridge::new_with_handle(reader, handle);
            extract_tar_gz_from_reader(bridge, &repo_dir_owned)
        })
        .await
        .map_err(|e| RepositoryCacheError::Archive(format!("task join error: {e}")))?
        .map_err(|e| RepositoryCacheError::Archive(e.to_string()))?;

        let meta_dir = self.branch_dir(project_id, branch).join(META_DIR);
        tokio::fs::create_dir_all(&meta_dir).await?;
        tokio::fs::write(meta_dir.join(COMMIT_FILE), commit_sha).await?;

        Ok(repo_dir)
    }

    fn pin(&self, project_id: i64, branch: &str) -> CacheEntryGuard {
        self.budget
            .pin(self.repository_dir(project_id, branch), project_id, branch)
    }

    async fn get_pinned(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<(CachedRepository, CacheEntryGuard)>, RepositoryCacheError> {
        let _eviction_guard = self.budget.eviction_read_lock().await;
        let cached = self.get(project_id, branch).await?;
        match cached {
            Some(repo) => {
                let guard = self.pin(project_id, branch);
                Ok(Some((repo, guard)))
            }
            None => Ok(None),
        }
    }

    async fn record_and_evict(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<bool, RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);
        let size = tokio::task::spawn_blocking(move || directory_size(&repo_dir))
            .await
            .map_err(|e| RepositoryCacheError::Archive(format!("size calculation failed: {e}")))?;

        self.budget.record_size(project_id, branch, size);

        self.budget
            .make_room(size)
            .await
            .map_err(|e| RepositoryCacheError::Archive(e.to_string()))?;

        Ok(size < self.budget.large_repo_threshold())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_cache() -> (TempDir, LocalRepositoryCache) {
        let temp_dir = TempDir::new().unwrap();
        let cache = LocalRepositoryCache::new(
            temp_dir.path().to_path_buf(),
            &RepositoryCacheConfiguration::default(),
            4,
            CodeMetrics::default(),
        );
        (temp_dir, cache)
    }

    fn archive_stream(data: Vec<u8>) -> ByteStream {
        Box::pin(futures::stream::once(async {
            Ok(bytes::Bytes::from(data))
        }))
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

    async fn assert_path_traversal_rejected(result: Result<(), RepositoryCacheError>) {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("path traversal"),
            "expected path traversal error, got: {error}"
        );
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

        cache.save(42, "main", "abc123").await.unwrap();
        let cached = cache.get(42, "main").await.unwrap().unwrap();

        assert_eq!(cached.path, cache.repository_dir(42, "main"));
        assert_eq!(cached.commit, "abc123");
    }

    #[tokio::test]
    async fn save_overwrites_previous_commit() {
        let (_dir, cache) = create_cache();

        cache.save(42, "main", "abc123").await.unwrap();
        cache.save(42, "main", "def456").await.unwrap();

        let cached = cache.get(42, "main").await.unwrap().unwrap();
        assert_eq!(cached.commit, "def456");
    }

    #[tokio::test]
    async fn invalidate_removes_cached_repository() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);
        cache
            .extract_archive(42, "main", "abc123", archive_stream(archive))
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
            .extract_archive(42, "main", "aaa", archive_stream(archive.clone()))
            .await
            .unwrap();
        cache
            .extract_archive(42, "develop", "bbb", archive_stream(archive))
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
            .extract_archive(1, "main", "aaa", archive_stream(archive.clone()))
            .await
            .unwrap();
        cache
            .extract_archive(2, "main", "bbb", archive_stream(archive))
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
            .extract_archive(42, "main", "aaa", archive_stream(archive.clone()))
            .await
            .unwrap();
        cache
            .extract_archive(42, "develop", "bbb", archive_stream(archive))
            .await
            .unwrap();

        cache.invalidate(42, "main").await.unwrap();

        assert!(cache.get(42, "main").await.unwrap().is_none());
        assert!(cache.get(42, "develop").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn extract_archive_populates_cache() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            ("src/main.rs", b"fn main() {}"),
            ("src/lib.rs", b"pub mod lib;"),
        ]);

        let path = cache
            .extract_archive(42, "main", "abc123", archive_stream(archive))
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
            .extract_archive(42, "main", "commit1", archive_stream(first_archive))
            .await
            .unwrap();

        let second_archive = build_tar_gz(&[("new_file.rs", b"new content")]);
        let path = cache
            .extract_archive(42, "main", "commit2", archive_stream(second_archive))
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

    #[tokio::test]
    async fn update_commit_changes_stored_sha() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();

        cache.update_commit(42, "main", "def456").await.unwrap();

        let cached = cache.get(42, "main").await.unwrap().unwrap();
        assert_eq!(cached.commit, "def456");
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

        cache.save(42, "main", "abc123").await.unwrap();
        let cached = cache.get(42, "main").await.unwrap().unwrap();

        let content = tokio::fs::read_to_string(cached.path.join("src/main.rs"))
            .await
            .unwrap();
        assert_eq!(content, "fn main() {}");
    }

    #[tokio::test]
    async fn write_file_creates_parent_directories() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();

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
        cache.save(42, "main", "abc123").await.unwrap();
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
        cache.save(42, "main", "abc123").await.unwrap();

        cache
            .delete_file(42, "main", "nonexistent.rs")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rename_file_moves_content_to_new_path() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();
        cache
            .write_file(42, "main", "old.rs", b"content")
            .await
            .unwrap();

        cache
            .rename_file(42, "main", "old.rs", "new.rs")
            .await
            .unwrap();

        let repo_dir = cache.repository_dir(42, "main");
        assert!(!repo_dir.join("old.rs").exists());
        let content = tokio::fs::read_to_string(repo_dir.join("new.rs"))
            .await
            .unwrap();
        assert_eq!(content, "content");
    }

    #[tokio::test]
    async fn rename_file_succeeds_when_source_does_not_exist() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();

        cache
            .rename_file(42, "main", "nonexistent.rs", "new.rs")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn path_traversal_rejected_for_all_file_operations() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();
        let malicious = "../../../etc/passwd";

        assert_path_traversal_rejected(cache.write_file(42, "main", malicious, b"bad").await).await;
        assert_path_traversal_rejected(cache.delete_file(42, "main", malicious).await).await;
        assert_path_traversal_rejected(cache.rename_file(42, "main", malicious, "safe.rs").await)
            .await;
        assert_path_traversal_rejected(cache.rename_file(42, "main", "safe.rs", malicious).await)
            .await;
    }

    #[tokio::test]
    async fn absolute_paths_rejected_for_all_file_operations() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();
        let absolute = "/etc/passwd";

        assert_path_traversal_rejected(cache.write_file(42, "main", absolute, b"bad").await).await;
        assert_path_traversal_rejected(cache.delete_file(42, "main", absolute).await).await;
        assert_path_traversal_rejected(cache.rename_file(42, "main", absolute, "safe.rs").await)
            .await;
        assert_path_traversal_rejected(cache.rename_file(42, "main", "safe.rs", absolute).await)
            .await;
    }

    #[tokio::test]
    async fn sibling_path_within_base_is_allowed() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();

        let result = cache
            .write_file(42, "main", "foo/../bar.rs", b"content")
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn record_and_evict_records_nonzero_size_for_files_on_disk() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();
        cache
            .write_file(42, "main", "file.rs", b"hello world")
            .await
            .unwrap();

        let should_cleanup = cache.record_and_evict(42, "main").await.unwrap();

        assert!(!should_cleanup, "default threshold is 0 so nothing is small");
    }

    #[tokio::test]
    async fn get_pinned_returns_entry_and_guard_on_cache_hit() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();

        let result = cache.get_pinned(42, "main").await.unwrap();

        let (cached, guard) = result.expect("should return cached entry");
        assert_eq!(cached.commit, "abc123");
        assert_eq!(guard.path(), cache.repository_dir(42, "main"));
    }

    #[tokio::test]
    async fn get_pinned_returns_none_on_cache_miss() {
        let (_dir, cache) = create_cache();

        let result = cache.get_pinned(42, "main").await.unwrap();

        assert!(result.is_none());
    }
}
