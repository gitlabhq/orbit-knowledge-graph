use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::cache::{CachedRepository, RepositoryCache, RepositoryCacheError};
use super::service::{RepositoryService, RepositoryServiceError};
use super::storage_lease::{ArchiveLeases, LeaseError};
use crate::handler::HandlerError;
use crate::retry::{Backoff, LocalRetry, RetryExhausted, Step, drive};
use gitlab_client::GitlabClientError;

const ARCHIVE_DOWNLOAD_RETRY: LocalRetry = LocalRetry {
    backoff: Backoff::Fixed(&[Duration::from_secs(1), Duration::from_secs(2)]),
    max_attempts: 3,
};

/// `EmptyRepository` is a recognized terminal outcome: the project record
/// exists but has no Gitaly content (no refs, or no repository storage at
/// all). These should be checkpointed as "indexed empty" instead of retried.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("project has no repository content ({reason}): {detail}")]
    EmptyRepository {
        reason: EmptyRepositoryReason,
        detail: String,
    },

    #[error("archive endpoint overloaded: {0}")]
    Overloaded(GitlabClientError),

    #[error("no archive download slot within {waited:?}")]
    NoArchiveSlot { waited: Duration },

    #[error(transparent)]
    Other(#[from] HandlerError),
}

impl From<RetryExhausted> for ResolveError {
    fn from(e: RetryExhausted) -> Self {
        // Satisfies the drive bound; full_download GiveUps the real error so the cap is never reached.
        ResolveError::Other(HandlerError::Processing(e.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyRepositoryReason {
    NotFound,
    /// Rails raises 500 for a repository that does not exist.
    ServerError,
    /// HTTP 200 OK with an empty or truncated archive body. Distinct from
    /// `NotFound` so dashboards can separate real 404s from quietly-empty 200s.
    EmptyArchive,
    /// Repository exceeded the total-bytes cap; indexed empty and checkpointed.
    RepositoryTooLarge,
}

impl EmptyRepositoryReason {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            EmptyRepositoryReason::NotFound => "not_found",
            EmptyRepositoryReason::ServerError => "server_error",
            EmptyRepositoryReason::EmptyArchive => "empty_archive",
            EmptyRepositoryReason::RepositoryTooLarge => "repository_too_large",
        }
    }

    /// Only the size cap is known to be permanent; the other reasons can be
    /// Gitaly visibility lag, which a retry may clear.
    pub fn is_permanent(self) -> bool {
        match self {
            EmptyRepositoryReason::RepositoryTooLarge => true,
            EmptyRepositoryReason::NotFound
            | EmptyRepositoryReason::ServerError
            | EmptyRepositoryReason::EmptyArchive => false,
        }
    }
}

impl std::fmt::Display for EmptyRepositoryReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_metric_label())
    }
}

/// Backfill resolves the branch through `project_info` first, so the project exists
/// and a 404 means it has no repository or no refs.
fn classify_download_error(
    err: RepositoryServiceError,
) -> Result<ResolveError, RepositoryServiceError> {
    let api_error = match err {
        RepositoryServiceError::GitlabApi(api_error) => api_error,
        RepositoryServiceError::Archive(_) => return Err(err),
    };
    match &api_error {
        GitlabClientError::NotFound(id) => Ok(ResolveError::EmptyRepository {
            reason: EmptyRepositoryReason::NotFound,
            detail: format!("project {id} not found (404)"),
        }),
        GitlabClientError::ServerError {
            status: 502..=504, ..
        }
        | GitlabClientError::RateLimited { .. } => Ok(ResolveError::Overloaded(api_error)),
        GitlabClientError::ServerError { project_id, status } => {
            Ok(ResolveError::EmptyRepository {
                reason: EmptyRepositoryReason::ServerError,
                detail: format!(
                    "archive endpoint returned {status} for project {project_id} (repository likely missing)"
                ),
            })
        }
        GitlabClientError::Request(_)
        | GitlabClientError::Unauthorized
        | GitlabClientError::ForcePush(_)
        | GitlabClientError::JwtSigning(_)
        | GitlabClientError::JwtDecoding(_)
        | GitlabClientError::InvalidSecret(_)
        | GitlabClientError::Unexpected(_)
        | GitlabClientError::CircuitOpen { .. } => Err(api_error.into()),
    }
}

pub struct RepositoryResolver {
    repository_service: Arc<dyn RepositoryService>,
    cache: Arc<dyn RepositoryCache>,
}

#[derive(Debug, Clone)]
pub enum Repository {
    Extracted(CachedRepository),
    IncrementalEngine(super::super::incremental::Repository),
}

impl Repository {
    pub(in crate::modules::code) fn parseable_files(&self) -> usize {
        let detected =
            |path: &str| code_graph::v2::config::detect_language_from_path(path).is_some();
        match self {
            Repository::Extracted(repository) => repository
                .file_inventory
                .count_by(|e| e.decision == code_graph::v2::Decision::Parse && detected(&e.path)),
            Repository::IncrementalEngine(repository) => repository
                .files()
                .filter(|file| file.keeps() && detected(&file.path))
                .count(),
        }
    }
}

impl RepositoryResolver {
    pub fn new(
        repository_service: Arc<dyn RepositoryService>,
        cache: Arc<dyn RepositoryCache>,
    ) -> Self {
        Self {
            repository_service,
            cache,
        }
    }

    pub async fn resolve(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: Option<&str>,
        namespace_id: Option<i64>,
        config: &orbit_server_config::CodeIndexingPipelineConfig,
        leases: &ArchiveLeases,
    ) -> Result<Repository, ResolveError> {
        let ref_name = commit_sha.unwrap_or(branch);
        let virtual_config = orbit_server_config::features::enabled_for(
            orbit_server_config::features::Feature::CodeGraphIncremental,
            namespace_id,
        )
        .then_some(config);
        let storage = self.repository_storage(project_id).await;
        self.full_download(
            project_id,
            branch,
            ref_name,
            virtual_config,
            commit_sha.is_some(),
            leases,
            storage.as_deref(),
        )
        .await
    }

    async fn repository_storage(&self, project_id: i64) -> Option<String> {
        match self.repository_service.project_info(project_id).await {
            Ok(info) => info.repository_storage,
            Err(error) => {
                warn!(project_id, %error, "repository storage lookup failed; using the tenant-wide archive budget");
                None
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "private helper threading one download's inputs; a params struct would only move the arity"
    )]
    async fn full_download(
        &self,
        project_id: i64,
        branch: &str,
        ref_name: &str,
        virtual_config: Option<&orbit_server_config::CodeIndexingPipelineConfig>,
        retry_empty: bool,
        leases: &ArchiveLeases,
        storage: Option<&str>,
    ) -> Result<Repository, ResolveError> {
        info!(
            project_id,
            branch,
            ref_name,
            incremental = virtual_config.is_some(),
            "downloading repository archive"
        );

        drive(&ARCHIVE_DOWNLOAD_RETRY, |attempt| async move {
            let download = async {
                let archive_stream = match self
                    .repository_service
                    .download_archive(project_id, ref_name)
                    .await
                {
                    Ok(stream) => stream,
                    Err(err) => {
                        let err = match classify_download_error(err) {
                            Ok(resolved) => return Step::GiveUp(resolved),
                            Err(err) => err,
                        };
                        warn!(project_id, ref_name, attempt = attempt + 1, error = %err, "archive download failed");
                        let e = HandlerError::Processing(format!("failed to download archive: {err}"));
                        return ARCHIVE_DOWNLOAD_RETRY.retry_or_give_up(attempt, (), e.into());
                    }
                };

                let repository = match virtual_config {
                    Some(config) => super::super::incremental::load(archive_stream, config).await.map(Repository::IncrementalEngine),
                    None => self.cache.extract_archive(archive_stream).await.map(Repository::Extracted),
                };
                match repository {
                    Ok(path) => Step::Done(path),
                    Err(RepositoryCacheError::EmptyArchive) => {
                        let empty = ResolveError::EmptyRepository {
                            reason: EmptyRepositoryReason::EmptyArchive,
                            detail: format!(
                                "archive contained no entries for project {project_id} ref {ref_name} (200 OK with empty body)"
                            ),
                        };
                        if !retry_empty {
                            return Step::GiveUp(empty);
                        }
                        warn!(project_id, ref_name, attempt = attempt + 1, "archive empty for a commit that exists; retrying the download");
                        ARCHIVE_DOWNLOAD_RETRY.retry_or_give_up(attempt, (), empty)
                    }
                    Err(RepositoryCacheError::RepositoryTooLarge) => Step::GiveUp(ResolveError::EmptyRepository {
                        reason: EmptyRepositoryReason::RepositoryTooLarge,
                        detail: format!(
                            "repository for project {project_id} ref {ref_name} exceeded the total-bytes cap"
                        ),
                    }),
                    Err(e) => {
                        warn!(project_id, ref_name, attempt = attempt + 1, error = %e, "archive extraction failed");
                        let e = HandlerError::Processing(format!("failed to extract archive: {e}"));
                        ARCHIVE_DOWNLOAD_RETRY.retry_or_give_up(attempt, (), e.into())
                    }
                }
            };
            match leases.run(storage, download).await {
                Ok(step) => step,
                Err(LeaseError::Timeout { waited, .. }) => {
                    Step::GiveUp(ResolveError::NoArchiveSlot { waited })
                }
                Err(LeaseError::Cancelled(_)) => {
                    warn!(project_id, ref_name, "archive lease wait stopped: project lock lost");
                    Step::GiveUp(ResolveError::NoArchiveSlot {
                        waited: Duration::ZERO,
                    })
                }
                Err(error @ (LeaseError::Lost(_) | LeaseError::Backend(_))) => {
                    warn!(project_id, ref_name, attempt = attempt + 1, %error, "archive lease failed");
                    let e = HandlerError::Processing(error.to_string());
                    ARCHIVE_DOWNLOAD_RETRY.retry_or_give_up(attempt, (), e.into())
                }
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::super::storage_lease::{LeaseSettings, slot_key};
    use super::*;
    use crate::modules::code::metrics::CodeMetrics;
    use crate::modules::code::repository::cache::{LocalRepositoryCache, RepositoryCache};
    use crate::modules::code::repository::service::RepositoryServiceError;
    use crate::testkit::mocks::MockLockService;
    use async_trait::async_trait;
    use code_graph::v2::CancellationToken;
    use parking_lot::Mutex;

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn only_the_size_cap_is_permanent() {
        assert!(EmptyRepositoryReason::RepositoryTooLarge.is_permanent());
        for reason in [
            EmptyRepositoryReason::NotFound,
            EmptyRepositoryReason::ServerError,
            EmptyRepositoryReason::EmptyArchive,
        ] {
            assert!(!reason.is_permanent());
        }
    }

    type ErrorFactory = Box<dyn Fn() -> RepositoryServiceError + Send>;

    struct ScriptedRepositoryService {
        archive: Mutex<Vec<u8>>,
        fail_downloads: Mutex<bool>,
        download_error: Mutex<Option<ErrorFactory>>,
        transient_failures: AtomicUsize,
        download_count: AtomicUsize,
        storage: Mutex<Option<String>>,
    }

    impl ScriptedRepositoryService {
        fn base(archive: Vec<u8>) -> Self {
            Self {
                archive: Mutex::new(archive),
                fail_downloads: Mutex::new(false),
                download_error: Mutex::new(None),
                transient_failures: AtomicUsize::new(0),
                download_count: AtomicUsize::new(0),
                storage: Mutex::new(None),
            }
        }

        fn set_storage(&self, storage: &str) {
            *self.storage.lock() = Some(storage.to_string());
        }

        fn with_archive(files: &[(&str, &str)], ref_name: &str) -> Arc<Self> {
            Arc::new(Self::base(build_test_tar_gz(files, ref_name)))
        }

        fn with_raw_archive(bytes: Vec<u8>) -> Arc<Self> {
            Arc::new(Self::base(bytes))
        }

        fn with_download_error(
            make_error: impl Fn() -> RepositoryServiceError + Send + 'static,
        ) -> Arc<Self> {
            let service = Self::base(Vec::new());
            *service.download_error.lock() = Some(Box::new(make_error));
            Arc::new(service)
        }

        fn set_archive(&self, files: &[(&str, &str)], ref_name: &str) {
            *self.archive.lock() = build_test_tar_gz(files, ref_name);
        }

        fn set_fail_downloads(&self, fail: bool) {
            *self.fail_downloads.lock() = fail;
        }

        fn set_transient_failures(&self, count: usize) {
            self.transient_failures.store(count, Ordering::SeqCst);
        }

        fn download_count(&self) -> usize {
            self.download_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RepositoryService for ScriptedRepositoryService {
        async fn project_info(
            &self,
            project_id: i64,
        ) -> Result<gitlab_client::ProjectInfo, RepositoryServiceError> {
            Ok(gitlab_client::ProjectInfo {
                project_id,
                default_branch: "main".to_string(),
                repository_storage: self.storage.lock().clone(),
            })
        }

        async fn download_archive(
            &self,
            _project_id: i64,
            _ref_name: &str,
        ) -> Result<super::super::service::ByteStream, RepositoryServiceError> {
            self.download_count.fetch_add(1, Ordering::SeqCst);
            if let Some(make_error) = self.download_error.lock().as_ref() {
                return Err(make_error());
            }
            if self
                .transient_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(RepositoryServiceError::Archive(
                    "simulated transient download failure".to_string(),
                ));
            }
            if *self.fail_downloads.lock() {
                return Err(RepositoryServiceError::Archive(
                    "simulated download failure".to_string(),
                ));
            }
            let data = self.archive.lock().clone();
            Ok(Box::pin(futures::stream::once(async {
                Ok(bytes::Bytes::from(data))
            })))
        }
    }

    fn build_test_tar_gz(files: &[(&str, &str)], ref_name: &str) -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());
        for (path, content) in files {
            let content_bytes = content.as_bytes();
            let mut header = tar::Header::new_gnu();
            let archive_path = format!("project-{ref_name}/{path}");
            header.set_path(&archive_path).unwrap();
            header.set_size(content_bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar_builder.append(&header, content_bytes).unwrap();
        }
        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[tokio::test]
    async fn both_engines_count_the_same_parseable_files() {
        let archive = build_test_tar_gz(
            &[
                ("src/main.py", "x = 1\n"),
                (".gitlab-ci.yml", "a: 1\n"),
                ("config/app.yaml", "b: 2\n"),
                ("infra/main.tf", "resource \"a\" \"b\" {}\n"),
                ("README.md", "readme"),
                ("logo.png", "image"),
            ],
            "abc",
        );
        let stream = |bytes: Vec<u8>| -> super::super::ByteStream {
            Box::pin(futures::stream::once(async move {
                Ok(bytes::Bytes::from(bytes))
            }))
        };
        let dir = tempfile::TempDir::new().unwrap();
        let cache =
            LocalRepositoryCache::new(dir.path().into(), u64::MAX, 0, CodeMetrics::default());
        let extracted = Repository::Extracted(
            cache
                .extract_archive(stream(archive.clone()))
                .await
                .unwrap(),
        );
        let config = orbit_server_config::AppConfig::embedded_defaults()
            .engine
            .handlers
            .code_indexing_task
            .pipeline;
        let loaded = Repository::IncrementalEngine(
            super::super::super::incremental::load(stream(archive), &config)
                .await
                .unwrap(),
        );
        assert_eq!(extracted.parseable_files(), 4);
        assert_eq!(loaded.parseable_files(), 4);
    }

    struct LegacyResolver(RepositoryResolver);

    impl LegacyResolver {
        async fn resolve(
            &self,
            project: i64,
            branch: &str,
            commit: Option<&str>,
        ) -> Result<CachedRepository, ResolveError> {
            let config = orbit_server_config::AppConfig::embedded_defaults()
                .engine
                .handlers
                .code_indexing_task
                .pipeline;
            match self
                .0
                .resolve(project, branch, commit, None, &config, &no_lease_limit())
                .await?
            {
                Repository::Extracted(repository) => Ok(repository),
                Repository::IncrementalEngine(_) => {
                    panic!("feature defaults must select legacy extraction")
                }
            }
        }
    }

    fn no_lease_limit() -> ArchiveLeases {
        leases_with_budget(Arc::new(MockLockService::new()), 0)
    }

    fn leases_with_budget(locks: Arc<MockLockService>, budget: usize) -> ArchiveLeases {
        ArchiveLeases::new(
            locks,
            LeaseSettings {
                storage_budget: budget,
                tenant_budget: budget,
                ttl: Duration::from_secs(30),
                max_wait: Duration::from_secs(2),
            },
            CancellationToken::new(),
            CodeMetrics::default(),
        )
    }

    async fn resolve_leased(
        resolver: &RepositoryResolver,
        leases: &ArchiveLeases,
    ) -> Result<Repository, ResolveError> {
        let config = orbit_server_config::AppConfig::embedded_defaults()
            .engine
            .handlers
            .code_indexing_task
            .pipeline;
        resolver
            .resolve(1, "main", Some("abc123"), None, &config, leases)
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn a_saturated_storage_returns_the_task_to_the_queue() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        service.set_storage("gitaly-1");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));
        let locks = Arc::new(MockLockService::new());
        locks.set_lock(&slot_key(Some("gitaly-1"), 0));

        let result = resolve_leased(&resolver.0, &leases_with_budget(locks, 1)).await;

        assert!(matches!(result, Err(ResolveError::NoArchiveSlot { .. })));
        assert_eq!(
            service.download_count(),
            0,
            "no Gitaly request without a lease"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn another_storages_slots_do_not_block_this_download() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        service.set_storage("gitaly-1");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));
        let locks = Arc::new(MockLockService::new());
        locks.set_lock(&slot_key(Some("gitaly-2"), 0));
        locks.set_lock(&slot_key(None, 0));

        let result = resolve_leased(&resolver.0, &leases_with_budget(locks.clone(), 1)).await;

        assert!(result.is_ok());
        assert_eq!(service.download_count(), 1);
        assert!(
            !locks.is_held(&slot_key(Some("gitaly-1"), 0)),
            "slot freed after download"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_overloaded_archive_frees_its_lease_without_a_slot_timeout() {
        let service = ScriptedRepositoryService::with_download_error(|| {
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::RateLimited {
                project_id: 42,
            })
        });
        service.set_storage("gitaly-1");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));
        let locks = Arc::new(MockLockService::new());

        let result = resolve_leased(&resolver.0, &leases_with_budget(locks.clone(), 1)).await;

        assert!(matches!(result, Err(ResolveError::Overloaded(_))));
        assert_eq!(
            service.download_count(),
            1,
            "no local retry under the lease"
        );
        assert!(!locks.is_held(&slot_key(Some("gitaly-1"), 0)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_missing_storage_shares_one_tenant_wide_budget() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));
        let locks = Arc::new(MockLockService::new());
        locks.set_lock(&slot_key(None, 0));

        let result = resolve_leased(&resolver.0, &leases_with_budget(locks, 1)).await;

        assert!(matches!(result, Err(ResolveError::NoArchiveSlot { .. })));
        assert_eq!(service.download_count(), 0);
    }

    fn create_resolver(
        service: Arc<ScriptedRepositoryService>,
    ) -> (tempfile::TempDir, LegacyResolver) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let metrics = CodeMetrics::default();
        let cache: Arc<dyn RepositoryCache> = Arc::new(LocalRepositoryCache::new(
            temp_dir.path().to_path_buf(),
            u64::MAX,
            0,
            metrics,
        ));
        let resolver = RepositoryResolver::new(service as Arc<dyn RepositoryService>, cache);
        (temp_dir, LegacyResolver(resolver))
    }

    #[tokio::test]
    async fn resolve_downloads_archive() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(service);

        let path = resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        assert!(path.path().join("src/main.rs").exists());
        let content = std::fs::read_to_string(path.path().join("src/main.rs")).unwrap();
        assert_eq!(content, "fn main() {}");
    }

    #[tokio::test]
    async fn resolve_always_downloads_fresh_copy() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "commit1");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        let path1 = resolver.resolve(1, "main", Some("commit1")).await.unwrap();
        assert!(path1.path().join("src/main.rs").exists());

        service.set_archive(&[("src/new.rs", "fn new() {}")], "commit2");
        let path2 = resolver.resolve(1, "main", Some("commit2")).await.unwrap();

        assert!(path2.path().join("src/new.rs").exists());
        assert!(!path2.path().join("src/main.rs").exists());
    }

    #[tokio::test]
    async fn resolve_uses_branch_when_no_commit_sha() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "main");
        let (_dir, resolver) = create_resolver(service);

        let path = resolver.resolve(1, "main", None).await.unwrap();

        assert!(path.path().join("src/main.rs").exists());
    }

    #[tokio::test]
    async fn dropping_repository_removes_downloaded_files() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(service);

        let repo = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        let path = repo.path().to_path_buf();
        assert!(path.exists());

        drop(repo);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn resolve_same_commit_downloads_every_time() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        assert_eq!(service.download_count(), 3);
    }

    #[tokio::test]
    async fn resolve_works_after_drop() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "v1")], "commit1");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        let repo1 = resolver.resolve(1, "main", Some("commit1")).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(repo1.path().join("src/main.rs")).unwrap(),
            "v1"
        );
        drop(repo1);

        service.set_archive(&[("src/main.rs", "v2")], "commit2");
        let repo2 = resolver.resolve(1, "main", Some("commit2")).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(repo2.path().join("src/main.rs")).unwrap(),
            "v2"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn resolve_propagates_download_error() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        service.set_fail_downloads(true);
        let result = resolver.resolve(1, "main", Some("abc123")).await;

        assert!(matches!(result, Err(ResolveError::Other(_))));
    }

    #[tokio::test]
    async fn resolve_maps_archive_404_to_empty_repository() {
        let service = ScriptedRepositoryService::with_download_error(|| {
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::NotFound(42))
        });
        let (_dir, resolver) = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();

        match err {
            ResolveError::EmptyRepository { reason, detail } => {
                assert_eq!(reason, EmptyRepositoryReason::NotFound);
                assert!(detail.contains("not found"), "detail was {detail}");
            }
            other => panic!("expected EmptyRepository, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_maps_archive_500_to_empty_repository() {
        let service = ScriptedRepositoryService::with_download_error(|| {
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::ServerError {
                project_id: 42,
                status: 500,
            })
        });
        let (_dir, resolver) = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();

        match err {
            ResolveError::EmptyRepository { reason, detail } => {
                assert_eq!(reason, EmptyRepositoryReason::ServerError);
                assert!(detail.contains("500"), "detail was {detail}");
            }
            other => panic!("expected EmptyRepository, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_maps_archive_overload_statuses_to_overloaded_without_retrying() {
        use gitlab_client::GitlabClientError;

        let overloads: [fn() -> GitlabClientError; 4] = [
            || GitlabClientError::ServerError {
                project_id: 42,
                status: 502,
            },
            || GitlabClientError::ServerError {
                project_id: 42,
                status: 503,
            },
            || GitlabClientError::ServerError {
                project_id: 42,
                status: 504,
            },
            || GitlabClientError::RateLimited { project_id: 42 },
        ];
        for overload in overloads {
            let service = ScriptedRepositoryService::with_download_error(move || {
                RepositoryServiceError::GitlabApi(overload())
            });
            let (_dir, resolver) = create_resolver(Arc::clone(&service));

            for commit_sha in [None, Some("abc123")] {
                let err = resolver.resolve(42, "main", commit_sha).await.unwrap_err();
                assert!(matches!(err, ResolveError::Overloaded(_)), "got {err:?}");
            }
            assert_eq!(service.download_count(), 2, "no local retries");
        }
    }

    #[tokio::test]
    async fn resolve_maps_empty_archive_body_to_empty_repository() {
        let service = ScriptedRepositoryService::with_raw_archive(Vec::new());
        let (_dir, resolver) = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();

        match err {
            ResolveError::EmptyRepository { reason, detail } => {
                assert_eq!(reason, EmptyRepositoryReason::EmptyArchive);
                assert!(
                    detail.contains("no entries") || detail.contains("empty body"),
                    "detail was {detail}"
                );
            }
            other => panic!("expected EmptyRepository, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn resolve_non_empty_errors_fall_through_to_other() {
        let service = ScriptedRepositoryService::with_download_error(|| {
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::Unauthorized)
        });
        let (_dir, resolver) = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();
        assert!(matches!(err, ResolveError::Other(_)), "got {err:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn resolve_retries_transient_download_failure_then_succeeds() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        service.set_transient_failures(2);
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        let path = resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        assert!(path.path().join("src/main.rs").exists());
        assert_eq!(service.download_count(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn resolve_gives_up_after_exhausting_download_retries() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        service.set_fail_downloads(true);
        let result = resolver.resolve(1, "main", Some("abc123")).await;

        assert!(matches!(result, Err(ResolveError::Other(_))));
        assert_eq!(
            service.download_count(),
            ARCHIVE_DOWNLOAD_RETRY.max_attempts as usize
        );
    }

    #[tokio::test]
    async fn multiple_projects_are_independent() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        let repo1 = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        let repo2 = resolver.resolve(2, "main", Some("abc123")).await.unwrap();

        assert_ne!(repo1.path(), repo2.path());
        assert!(repo1.path().join("src/main.rs").exists());
        assert!(repo2.path().join("src/main.rs").exists());

        let path2 = repo2.path().to_path_buf();
        drop(repo1);
        assert!(path2.exists());
    }
}
