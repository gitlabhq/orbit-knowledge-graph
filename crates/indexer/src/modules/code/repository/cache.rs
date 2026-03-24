use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::StreamExt;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{error, info, warn};

use super::cache_budget::{CacheBudget, RepositoryLease};
use super::disk::{directory_size, hashed_branch_name};
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

    #[error("cache budget exhausted: {0}")]
    BudgetExhausted(String),

    #[error("path traversal detected: {0}")]
    PathTraversal(String),
}

#[async_trait]
pub trait RepositoryCacheLifecycle: Send + Sync {
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

    async fn extract_archive(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        archive_stream: ByteStream,
    ) -> Result<RepositoryLease, RepositoryCacheError>;

    /// Holds the eviction read lock across get+pin so an in-flight eviction
    /// cannot delete the entry between the two operations.
    async fn acquire(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<(CachedRepository, RepositoryLease)>, RepositoryCacheError>;
}

#[async_trait]
pub trait CachedRepositoryFiles: Send + Sync {
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
}

/// Blanket-implemented for any type implementing both sub-traits.
pub trait RepositoryCache: RepositoryCacheLifecycle + CachedRepositoryFiles {}
impl<T: RepositoryCacheLifecycle + CachedRepositoryFiles> RepositoryCache for T {}

const COMMIT_FILE: &str = ".commit";
const META_DIR: &str = "meta";
const REPOSITORY_DIR: &str = "repository";

pub struct LocalRepositoryCache {
    base_dir: PathBuf,
    budget: CacheBudget,
    phantom_bytes: AtomicU64,
    headroom: u64,
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
            warn!(
                disk_budget_bytes = config.disk_budget_bytes,
                headroom_per_worker_bytes = config.headroom_per_worker_bytes,
                code_worker_count,
                "usable cache budget is 0 — every write will trigger full eviction; \
                 increase disk_budget_bytes or decrease headroom_per_worker_bytes"
            );
        }
        let budget = CacheBudget::new(usable_budget, config.large_repo_threshold_bytes, metrics);
        let headroom = config.disk_budget_bytes.saturating_sub(usable_budget);
        Self {
            base_dir,
            budget,
            phantom_bytes: AtomicU64::new(0),
            headroom,
        }
    }

    fn branch_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.base_dir
            .join(project_id.to_string())
            .join(hashed_branch_name(branch))
    }

    fn repository_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.branch_dir(project_id, branch).join(REPOSITORY_DIR)
    }

    fn pin(&self, project_id: i64, branch: &str) -> RepositoryLease {
        self.budget
            .pin(self.repository_dir(project_id, branch), project_id, branch)
    }

    async fn clear_repository_dir(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<(), RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);
        match tokio::fs::remove_dir_all(&repo_dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        tokio::fs::create_dir_all(&repo_dir).await?;
        Ok(())
    }

    async fn extract_stream_to_disk(
        &self,
        project_id: i64,
        branch: &str,
        archive_stream: ByteStream,
    ) -> Result<(), RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);
        let reader = StreamReader::new(archive_stream.map(|r| r.map_err(std::io::Error::other)));
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let bridge = SyncIoBridge::new_with_handle(reader, handle);
            extract_tar_gz_from_reader(bridge, &repo_dir)
        })
        .await
        .map_err(|e| RepositoryCacheError::Archive(format!("task join error: {e}")))?
        .map_err(|e| RepositoryCacheError::Archive(e.to_string()))
    }

    async fn write_commit_metadata(
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

    async fn make_room_for(&self, needed_bytes: u64) -> Result<(), RepositoryCacheError> {
        let _eviction_guard = self.budget.eviction_write_lock().await;

        let keys_to_evict = match self.budget.entries_to_evict(needed_bytes) {
            Ok(keys) => keys,
            Err(exhausted) => {
                if self.phantom_bytes.load(Ordering::Relaxed) > self.headroom {
                    self.purge_entire_cache().await;
                    return Ok(());
                }
                return Err(RepositoryCacheError::BudgetExhausted(exhausted.to_string()));
            }
        };

        if keys_to_evict.is_empty() {
            return Ok(());
        }

        let mut evicted_bytes = 0u64;

        for (project_id, branch) in &keys_to_evict {
            let entry_dir = self.branch_dir(*project_id, branch);
            let removed_bytes = self.budget.remove(*project_id, branch);

            if let Err(e) = tokio::fs::remove_dir_all(&entry_dir).await {
                self.phantom_bytes
                    .fetch_add(removed_bytes, Ordering::Relaxed);
                warn!(
                    project_id,
                    branch = %branch,
                    error = %e,
                    phantom_bytes = self.phantom_bytes.load(Ordering::Relaxed),
                    "failed to delete evicted repository from disk, removed from index anyway"
                );
            } else {
                evicted_bytes += removed_bytes;
                info!(
                    project_id,
                    branch = %branch,
                    size_bytes = removed_bytes,
                    "evicted cached repository"
                );
            }
        }

        if evicted_bytes > 0 {
            self.budget.record_eviction(evicted_bytes);
        }

        if self.phantom_bytes.load(Ordering::Relaxed) > self.headroom {
            self.purge_entire_cache().await;
        }

        Ok(())
    }

    /// Active workers holding leases will get I/O errors; their messages will be retried.
    async fn purge_entire_cache(&self) {
        error!(
            phantom_bytes = self.phantom_bytes.load(Ordering::Relaxed),
            headroom = self.headroom,
            "phantom bytes exceeded headroom, purging entire cache"
        );

        if let Err(e) = tokio::fs::remove_dir_all(&self.base_dir).await {
            error!(error = %e, "failed to purge cache directory");
        }
        let _ = tokio::fs::create_dir_all(&self.base_dir).await;

        self.budget.clear();
        self.phantom_bytes.store(0, Ordering::Relaxed);
    }

    async fn measure_and_enforce_budget(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<(), RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);
        let size = tokio::task::spawn_blocking(move || directory_size(&repo_dir))
            .await
            .map_err(|e| RepositoryCacheError::Archive(format!("size calculation failed: {e}")))?;

        self.make_room_for(size).await?;
        self.budget.record_size(project_id, branch, size);

        Ok(())
    }
}

fn validated_path(base: &Path, relative: &str) -> Result<PathBuf, RepositoryCacheError> {
    for component in Path::new(relative).components() {
        match component {
            std::path::Component::Normal(_) | std::path::Component::CurDir => {}
            _ => return Err(RepositoryCacheError::PathTraversal(relative.to_string())),
        }
    }
    Ok(base.join(relative))
}

#[async_trait]
impl RepositoryCacheLifecycle for LocalRepositoryCache {
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
        let repository_dir = self.repository_dir(project_id, branch);
        tokio::fs::create_dir_all(&repository_dir).await?;
        self.write_commit_metadata(project_id, branch, commit_sha)
            .await
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

    async fn extract_archive(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
        archive_stream: ByteStream,
    ) -> Result<RepositoryLease, RepositoryCacheError> {
        self.clear_repository_dir(project_id, branch).await?;
        self.extract_stream_to_disk(project_id, branch, archive_stream)
            .await?;
        self.write_commit_metadata(project_id, branch, commit_sha)
            .await?;

        let lease = self.pin(project_id, branch);
        self.measure_and_enforce_budget(project_id, branch).await?;
        Ok(lease)
    }

    async fn acquire(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<(CachedRepository, RepositoryLease)>, RepositoryCacheError> {
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
}

#[async_trait]
impl CachedRepositoryFiles for LocalRepositoryCache {
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
        self.write_commit_metadata(project_id, branch, commit_sha)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::disk::hashed_branch_name;
    use super::*;
    use tempfile::TempDir;

    fn default_config() -> RepositoryCacheConfiguration {
        RepositoryCacheConfiguration::default()
    }

    fn create_cache() -> (TempDir, LocalRepositoryCache) {
        let temp_dir = TempDir::new().unwrap();
        let cache = LocalRepositoryCache::new(
            temp_dir.path().to_path_buf(),
            &default_config(),
            4,
            CodeMetrics::default(),
        );
        (temp_dir, cache)
    }

    fn create_cache_with_budget(
        temp_dir: &Path,
        disk_budget_bytes: u64,
        headroom_per_worker_bytes: u64,
        large_repo_threshold_bytes: u64,
    ) -> LocalRepositoryCache {
        let config = RepositoryCacheConfiguration {
            path: temp_dir.to_path_buf(),
            disk_budget_bytes,
            headroom_per_worker_bytes,
            large_repo_threshold_bytes,
        };
        LocalRepositoryCache::new(temp_dir.to_path_buf(), &config, 1, CodeMetrics::default())
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

    fn assert_path_traversal_rejected<T: std::fmt::Debug>(result: Result<T, RepositoryCacheError>) {
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("path traversal"),
            "expected path traversal error, got: {error}"
        );
    }

    async fn write_entry_on_disk(base_dir: &Path, project_id: i64, branch: &str, size: usize) {
        let branch_hash = hashed_branch_name(branch);
        let branch_dir = base_dir.join(project_id.to_string()).join(branch_hash);
        let meta_dir = branch_dir.join("meta");
        let repo_dir = branch_dir.join("repository");
        tokio::fs::create_dir_all(&meta_dir).await.unwrap();
        tokio::fs::create_dir_all(&repo_dir).await.unwrap();
        tokio::fs::write(meta_dir.join(".commit"), "abc123")
            .await
            .unwrap();
        tokio::fs::write(repo_dir.join("data.bin"), vec![0u8; size])
            .await
            .unwrap();
    }

    fn entry_dir(base_dir: &Path, project_id: i64, branch: &str) -> PathBuf {
        base_dir
            .join(project_id.to_string())
            .join(hashed_branch_name(branch))
    }

    // --- Lifecycle tests ---

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

    // --- Archive tests ---

    #[tokio::test]
    async fn extract_archive_populates_cache() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            ("src/main.rs", b"fn main() {}"),
            ("src/lib.rs", b"pub mod lib;"),
        ]);

        let lease = cache
            .extract_archive(42, "main", "abc123", archive_stream(archive))
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(lease.join("src/main.rs"))
            .await
            .unwrap();
        assert_eq!(content, "fn main() {}");
        let content = tokio::fs::read_to_string(lease.join("src/lib.rs"))
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
        let lease = cache
            .extract_archive(42, "main", "commit2", archive_stream(second_archive))
            .await
            .unwrap();

        assert!(!lease.join("old_file.rs").exists());
        let content = tokio::fs::read_to_string(lease.join("new_file.rs"))
            .await
            .unwrap();
        assert_eq!(content, "new content");

        let cached = cache.get(42, "main").await.unwrap().unwrap();
        assert_eq!(cached.commit, "commit2");
    }

    #[tokio::test]
    async fn extract_archive_records_size_in_budget() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"hello world")]);

        cache
            .extract_archive(42, "main", "abc123", archive_stream(archive))
            .await
            .unwrap();

        let cached = cache.get(42, "main").await.unwrap();
        assert!(cached.is_some(), "entry should be tracked in the cache");
    }

    // --- File operation tests ---

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

    // --- Path traversal tests ---

    #[tokio::test]
    async fn path_traversal_rejected_for_all_file_operations() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();
        let malicious = "../../../etc/passwd";

        assert_path_traversal_rejected(cache.write_file(42, "main", malicious, b"bad").await);
        assert_path_traversal_rejected(cache.delete_file(42, "main", malicious).await);
        assert_path_traversal_rejected(cache.rename_file(42, "main", malicious, "safe.rs").await);
        assert_path_traversal_rejected(cache.rename_file(42, "main", "safe.rs", malicious).await);
    }

    #[tokio::test]
    async fn absolute_paths_rejected_for_all_file_operations() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();
        let absolute = "/etc/passwd";

        assert_path_traversal_rejected(cache.write_file(42, "main", absolute, b"bad").await);
        assert_path_traversal_rejected(cache.delete_file(42, "main", absolute).await);
        assert_path_traversal_rejected(cache.rename_file(42, "main", absolute, "safe.rs").await);
        assert_path_traversal_rejected(cache.rename_file(42, "main", "safe.rs", absolute).await);
    }

    #[tokio::test]
    async fn parent_dir_components_are_always_rejected() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();

        assert_path_traversal_rejected(
            cache
                .write_file(42, "main", "foo/../bar.rs", b"content")
                .await,
        );
    }

    // --- Acquire tests ---

    #[tokio::test]
    async fn acquire_returns_entry_and_guard_on_cache_hit() {
        let (_dir, cache) = create_cache();
        cache.save(42, "main", "abc123").await.unwrap();

        let result = cache.acquire(42, "main").await.unwrap();

        let (cached, guard) = result.expect("should return cached entry");
        assert_eq!(cached.commit, "abc123");
        assert_eq!(guard.path(), cache.repository_dir(42, "main"));
    }

    #[tokio::test]
    async fn acquire_returns_none_on_cache_miss() {
        let (_dir, cache) = create_cache();

        let result = cache.acquire(42, "main").await.unwrap();

        assert!(result.is_none());
    }

    // --- Eviction tests ---

    #[tokio::test]
    async fn eviction_removes_entries_from_disk() {
        let dir = TempDir::new().unwrap();
        let cache = create_cache_with_budget(dir.path(), 1200, 0, 400);

        write_entry_on_disk(dir.path(), 1, "main", 500).await;
        cache.budget.record_size(1, "main", 500);
        write_entry_on_disk(dir.path(), 2, "main", 300).await;
        cache.budget.record_size(2, "main", 300);
        write_entry_on_disk(dir.path(), 3, "main", 350).await;
        cache.budget.record_size(3, "main", 350);

        // Total: 1150. Budget: 1200. Making room for 400 → target = 800.
        // Should evict small repos (300 + 350) before the large one (500).
        cache.make_room_for(400).await.unwrap();

        assert!(
            entry_dir(dir.path(), 1, "main").exists(),
            "large repo should survive"
        );
        assert!(
            !entry_dir(dir.path(), 2, "main").exists(),
            "small repo should be evicted"
        );
        assert!(
            !entry_dir(dir.path(), 3, "main").exists(),
            "small repo should be evicted"
        );
    }

    #[tokio::test]
    async fn eviction_skips_pinned_entries() {
        let dir = TempDir::new().unwrap();
        let cache = create_cache_with_budget(dir.path(), 500, 0, 400);

        write_entry_on_disk(dir.path(), 1, "main", 600).await;
        cache.budget.record_size(1, "main", 600);

        let _guard = cache.pin(1, "main");

        let result = cache.make_room_for(0).await;

        assert!(result.is_err(), "should fail when all entries are pinned");
        assert!(
            entry_dir(dir.path(), 1, "main").exists(),
            "pinned entry should not be evicted"
        );
    }

    #[tokio::test]
    async fn eviction_is_noop_when_under_budget() {
        let dir = TempDir::new().unwrap();
        let cache = create_cache_with_budget(dir.path(), 20_000, 0, 500);

        write_entry_on_disk(dir.path(), 1, "main", 100).await;
        cache.budget.record_size(1, "main", 100);

        cache.make_room_for(100).await.unwrap();

        assert!(entry_dir(dir.path(), 1, "main").exists());
    }

    #[tokio::test]
    async fn purges_entire_cache_when_phantom_bytes_exceed_headroom() {
        let dir = TempDir::new().unwrap();
        // disk_budget=1200, headroom_per_worker=100, workers=1 → usable=1100, headroom=100
        let cache = create_cache_with_budget(dir.path(), 1200, 100, 400);

        write_entry_on_disk(dir.path(), 1, "main", 1150).await;
        cache.budget.record_size(1, "main", 1150);
        let _guard = cache.pin(1, "main");

        // Simulate phantom bytes exceeding headroom (101 > 100)
        cache.phantom_bytes.store(101, Ordering::Relaxed);

        let result = cache.make_room_for(0).await;

        assert!(
            result.is_ok(),
            "purge should recover instead of returning an error"
        );
        assert_eq!(cache.phantom_bytes.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn does_not_purge_when_phantom_bytes_within_headroom() {
        let dir = TempDir::new().unwrap();
        // disk_budget=1200, headroom_per_worker=100, workers=1 → usable=1100, headroom=100
        let cache = create_cache_with_budget(dir.path(), 1200, 100, 400);

        write_entry_on_disk(dir.path(), 1, "main", 1150).await;
        cache.budget.record_size(1, "main", 1150);
        let _guard = cache.pin(1, "main");

        // Phantom bytes within headroom (50 <= 100) → no purge, returns error
        cache.phantom_bytes.store(50, Ordering::Relaxed);

        let result = cache.make_room_for(0).await;

        assert!(result.is_err(), "should return budget exhausted, not purge");
    }

    #[tokio::test]
    async fn concurrent_eviction_and_pin_do_not_race() {
        let dir = TempDir::new().unwrap();
        let cache = std::sync::Arc::new(LocalRepositoryCache::new(
            dir.path().to_path_buf(),
            &RepositoryCacheConfiguration {
                path: dir.path().to_path_buf(),
                disk_budget_bytes: 400,
                headroom_per_worker_bytes: 0,
                large_repo_threshold_bytes: 500,
            },
            1,
            CodeMetrics::default(),
        ));

        write_entry_on_disk(dir.path(), 1, "main", 200).await;
        cache.budget.record_size(1, "main", 200);

        let entry_path = cache.repository_dir(1, "main");

        let mut readers = Vec::new();
        for _ in 0..10 {
            let cache = std::sync::Arc::clone(&cache);
            let path = entry_path.clone();
            readers.push(tokio::spawn(async move {
                let _read_guard = cache.budget.eviction_read_lock().await;
                let guard = cache.budget.pin(path, 1, "main");
                tokio::task::yield_now().await;
                drop(guard);
            }));
        }

        let evictor = {
            let cache = std::sync::Arc::clone(&cache);
            tokio::spawn(async move {
                let _ = cache.make_room_for(200).await;
            })
        };

        for reader in readers {
            reader.await.unwrap();
        }
        evictor.await.unwrap();
    }
}
