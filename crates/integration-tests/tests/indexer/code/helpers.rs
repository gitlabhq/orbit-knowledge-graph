#![allow(dead_code)]

use std::io::Write;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use base64::Engine;
use clickhouse_client::ClickHouseConfigurationExt;
use flate2::Compression;
use flate2::write::GzEncoder;
use gitlab_client::GitlabClient;
use indexer::handler::HandlerContext;
use indexer::memory::MemoryWatchdog;
use indexer::modules::code::{
    ClickHouseCodeCheckpointStore, ClickHouseStaleDataCleaner, CodeIndexer,
    CodeIndexingTaskHandler, LocalRepositoryCache, RailsRepositoryService, RepositoryService,
    config::CodeTableNames, metrics::CodeMetrics, repository::RepositoryCache,
    repository::RepositoryResolver,
};
use indexer::nats::ProgressNotifier;
use indexer::testkit::{MockLockService, MockNatsServices};
use indexer::topic::CodeIndexingTaskRequest;
use indexer::types::{Envelope, Event};
use integration_testkit::{TestContext, t};
use orbit_server_config::{CodeIndexingPipelineConfig, GitlabClientConfiguration};
use orbit_utils::traversal_path::TraversalPath;
use parking_lot::Mutex;
use serde::Deserialize;
use std::collections::HashMap;

const SIGNING_KEY: &[u8] = b"test-secret-that-is-long-enough!";

const SLOW_ARCHIVE_DELAY: std::time::Duration = std::time::Duration::from_secs(3);

pub fn code_indexing_backfill_envelope(
    project_id: i64,
    task_id: i64,
    traversal_path: &str,
) -> Envelope {
    Envelope::new(&CodeIndexingTaskRequest {
        task_id,
        project_id,
        branch: Some("main".to_string()),
        commit_sha: None,
        traversal_path: TraversalPath::new_unchecked(traversal_path),
        dispatch_id: uuid::Uuid::new_v4(),
        campaign_id: None,
    })
    .expect("failed to create envelope")
}

pub async fn checkpointed(
    clickhouse: &TestContext,
    traversal_path: &str,
    project_id: i64,
    branch: &str,
) -> bool {
    let rows = clickhouse
        .query(&format!(
            "SELECT last_task_id FROM {} FINAL \
             WHERE traversal_path = '{traversal_path}' AND project_id = {project_id} \
             AND branch = '{branch}' AND indexed_at IS NOT NULL AND _deleted = false",
            t("code_indexing_checkpoint")
        ))
        .await;
    rows.first().is_some_and(|b| b.num_rows() > 0)
}

pub struct CodeIndexingDeps {
    pub pipeline: Arc<CodeIndexer>,
    pub repository_service: Arc<dyn RepositoryService>,
    pub checkpoint_store: Arc<ClickHouseCodeCheckpointStore>,
    pub metrics: CodeMetrics,
    pub clickhouse_config: orbit_server_config::ClickHouseConfiguration,
    pub shutdown: tokio_util::sync::CancellationToken,
    cache_dir: tempfile::TempDir,
}

impl CodeIndexingDeps {
    pub fn new(mock: &MockGitlabServer, clickhouse: &TestContext) -> Self {
        Self::new_with_pipeline_config(
            mock,
            clickhouse,
            indexer::testkit::test_pipeline_configuration(),
        )
    }

    pub fn new_with_pipeline_config(
        mock: &MockGitlabServer,
        clickhouse: &TestContext,
        pipeline_config: CodeIndexingPipelineConfig,
    ) -> Self {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        Self::new_with_ontology(mock, clickhouse, pipeline_config, ontology)
    }

    pub fn new_with_memory_watchdog(
        mock: &MockGitlabServer,
        clickhouse: &TestContext,
        pipeline_config: CodeIndexingPipelineConfig,
        memory: Arc<MemoryWatchdog>,
    ) -> Self {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        Self::build(mock, clickhouse, pipeline_config, ontology, memory)
    }

    pub fn new_with_ontology(
        mock: &MockGitlabServer,
        clickhouse: &TestContext,
        pipeline_config: CodeIndexingPipelineConfig,
        ontology: ontology::Ontology,
    ) -> Self {
        let memory = MemoryWatchdog::for_this_process(0);
        Self::build(mock, clickhouse, pipeline_config, ontology, memory)
    }

    fn build(
        mock: &MockGitlabServer,
        clickhouse: &TestContext,
        pipeline_config: CodeIndexingPipelineConfig,
        ontology: ontology::Ontology,
        memory: Arc<MemoryWatchdog>,
    ) -> Self {
        let repository_service = RailsRepositoryService::create(Arc::new(mock.gitlab_client()));
        let graph_client = Arc::new(clickhouse.config.build_client("graph"));
        let checkpoint_store = Arc::new(ClickHouseCodeCheckpointStore::new(Arc::clone(
            &graph_client,
        )));
        let table_names = Arc::new(
            CodeTableNames::from_ontology(
                &ontology::Ontology::load_embedded().expect("ontology must load"),
            )
            .expect("code tables must resolve"),
        );

        let stale_data_cleaner =
            Arc::new(ClickHouseStaleDataCleaner::new(graph_client, &table_names));
        let metrics = CodeMetrics::new();

        let cache_dir = tempfile::TempDir::new().expect("failed to create temp dir for cache");
        // 0 keeps the unlimited default that tests without extraction caps rely on.
        let max_file_size = match pipeline_config.max_file_size_bytes {
            0 => u64::MAX,
            n => n,
        };
        let cache: Arc<dyn RepositoryCache> = Arc::new(LocalRepositoryCache::new(
            cache_dir.path().to_path_buf(),
            max_file_size,
            pipeline_config.max_total_bytes,
            metrics.clone(),
        ));
        let resolver = RepositoryResolver::new(Arc::clone(&repository_service), cache);

        let writer = Arc::new(indexer::clickhouse::ClickHouseWriter::new(
            clickhouse.config.clone().build_client("graph"),
            Arc::new(indexer::metrics::EngineMetrics::new()),
        ));

        let shutdown = tokio_util::sync::CancellationToken::new();
        let pipeline = Arc::new(CodeIndexer::new(
            resolver,
            writer,
            Arc::clone(&checkpoint_store) as _,
            stale_data_cleaner,
            metrics.clone(),
            table_names,
            Arc::new(ontology),
            pipeline_config,
            memory,
            shutdown.clone(),
        ));

        Self {
            pipeline,
            repository_service,
            checkpoint_store,
            metrics,
            clickhouse_config: clickhouse.config.clone(),
            shutdown,
            cache_dir,
        }
    }

    pub fn cache_dir_path(&self) -> &std::path::Path {
        self.cache_dir.path()
    }

    pub fn code_indexing_task_handler_with_writer(
        &self,
        writer: Arc<indexer::clickhouse::ClickHouseWriter>,
    ) -> CodeIndexingTaskHandler {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let table_names =
            Arc::new(CodeTableNames::from_ontology(&ontology).expect("code tables must resolve"));
        let graph_client = Arc::new(self.clickhouse_config.build_client("graph"));
        let stale_data_cleaner =
            Arc::new(ClickHouseStaleDataCleaner::new(graph_client, &table_names));

        let cache: Arc<dyn RepositoryCache> = Arc::new(LocalRepositoryCache::new(
            self.cache_dir.path().to_path_buf(),
            u64::MAX,
            0,
            self.metrics.clone(),
        ));
        let resolver = RepositoryResolver::new(Arc::clone(&self.repository_service), cache);
        let config = indexer::testkit::test_pipeline_configuration();
        let pipeline = Arc::new(CodeIndexer::new(
            resolver,
            writer,
            Arc::clone(&self.checkpoint_store) as _,
            stale_data_cleaner,
            self.metrics.clone(),
            table_names,
            Arc::new(ontology),
            config,
            MemoryWatchdog::for_this_process(0),
            self.shutdown.clone(),
        ));
        CodeIndexingTaskHandler::new(
            pipeline,
            Arc::clone(&self.repository_service),
            Arc::clone(&self.checkpoint_store) as _,
            self.metrics.clone(),
            std::time::Duration::from_secs(60),
            CodeIndexingTaskRequest::subscription(),
            indexer::analytics::IndexingAnalytics::disabled(),
        )
    }

    pub fn code_indexing_task_handler(&self) -> CodeIndexingTaskHandler {
        self.code_indexing_task_handler_with_lock_ttl(std::time::Duration::from_secs(60))
    }

    pub fn code_indexing_task_handler_with_lock_ttl(
        &self,
        lock_ttl: std::time::Duration,
    ) -> CodeIndexingTaskHandler {
        CodeIndexingTaskHandler::new(
            Arc::clone(&self.pipeline),
            Arc::clone(&self.repository_service),
            Arc::clone(&self.checkpoint_store) as _,
            self.metrics.clone(),
            lock_ttl,
            CodeIndexingTaskRequest::subscription(),
            indexer::analytics::IndexingAnalytics::disabled(),
        )
    }
}

struct MockState {
    projects: Mutex<HashMap<i64, ProjectData>>,
}

struct ProjectData {
    default_branch: String,
    /// Raw file entries (path, content). The archive is built on-the-fly
    /// in the handler using the ref from the request query, so the Gitaly
    /// `<slug>-<ref>/` prefix matches whatever commit SHA the indexer asks for.
    archive_files: Vec<(String, Vec<u8>)>,
    /// How many more times the archive endpoint returns 200 OK with a zero-byte body,
    /// matching the production case where Gitaly streams an empty archive for
    /// projects that lack repository content. `u32::MAX` means always.
    empty_archive_responses: u32,
    slow_archive: bool,
    archive_status: Option<StatusCode>,
}

pub struct MockGitlabServer {
    state: Arc<MockState>,
    base_url: String,
}

impl MockGitlabServer {
    pub async fn start() -> Self {
        let state = Arc::new(MockState {
            projects: Mutex::new(HashMap::new()),
        });

        let app = Router::new()
            .route(
                "/api/v4/internal/orbit/project/{project_id}/info",
                get(handle_project_info),
            )
            .route(
                "/api/v4/internal/orbit/project/{project_id}/repository/archive",
                get(handle_download_archive),
            )
            .with_state(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind failed");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        Self {
            state,
            base_url: format!("http://{addr}"),
        }
    }

    pub fn gitlab_client(&self) -> GitlabClient {
        let config = GitlabClientConfiguration {
            base_url: self.base_url.clone(),
            signing_key: base64::engine::general_purpose::STANDARD.encode(SIGNING_KEY),
            resolve_host: None,
        };
        GitlabClient::new(config).expect("failed to create GitlabClient")
    }

    pub fn add_project(&self, project_id: i64, default_branch: &str, files: &[(&str, &str)]) {
        let archive_files: Vec<(String, Vec<u8>)> = files
            .iter()
            .map(|(p, c)| (p.to_string(), c.as_bytes().to_vec()))
            .collect();
        self.state.projects.lock().insert(
            project_id,
            ProjectData {
                default_branch: default_branch.to_string(),
                archive_files,
                empty_archive_responses: 0,
                slow_archive: false,
                archive_status: None,
            },
        );
    }

    pub fn slow_archive(&self, project_id: i64) {
        if let Some(project) = self.state.projects.lock().get_mut(&project_id) {
            project.slow_archive = true;
        }
    }

    /// Register a project whose archive endpoint sleeps, so a short job timeout fires mid-fetch.
    pub fn add_project_with_slow_archive(&self, project_id: i64, default_branch: &str) {
        self.state.projects.lock().insert(
            project_id,
            ProjectData {
                default_branch: default_branch.to_string(),
                archive_files: vec![(
                    "src/Main.java".to_string(),
                    b"public class Main {}".to_vec(),
                )],
                empty_archive_responses: 0,
                slow_archive: true,
                archive_status: None,
            },
        );
    }

    /// Register a project whose archive endpoint returns HTTP 200 with a
    /// zero-byte body, exercising the "empty 200" path the indexer must
    /// classify as an empty repository instead of a retryable failure.
    pub fn add_project_with_empty_archive(&self, project_id: i64, default_branch: &str) {
        self.state.projects.lock().insert(
            project_id,
            ProjectData {
                default_branch: default_branch.to_string(),
                archive_files: Vec::new(),
                empty_archive_responses: u32::MAX,
                slow_archive: false,
                archive_status: None,
            },
        );
    }

    pub fn add_project_with_transient_empty_archive(
        &self,
        project_id: i64,
        default_branch: &str,
        files: &[(&str, &str)],
        empty_responses: u32,
    ) {
        self.add_project(project_id, default_branch, files);
        if let Some(project) = self.state.projects.lock().get_mut(&project_id) {
            project.empty_archive_responses = empty_responses;
        }
    }

    pub fn set_archive_status(&self, project_id: i64, status: Option<StatusCode>) {
        if let Some(project) = self.state.projects.lock().get_mut(&project_id) {
            project.archive_status = status;
        }
    }

    pub fn replace_archive(&self, project_id: i64, files: &[(&str, &str)]) {
        let mut projects = self.state.projects.lock();
        if let Some(project) = projects.get_mut(&project_id) {
            project.archive_files = files
                .iter()
                .map(|(p, c)| (p.to_string(), c.as_bytes().to_vec()))
                .collect();
            project.empty_archive_responses = 0;
        }
    }
}

#[derive(Deserialize)]
struct ArchiveQuery {
    #[serde(rename = "ref")]
    ref_name: String,
}

async fn handle_project_info(
    State(state): State<Arc<MockState>>,
    Path(project_id): Path<i64>,
) -> impl IntoResponse {
    let projects = state.projects.lock();
    match projects.get(&project_id) {
        Some(p) => {
            let info = serde_json::json!({
                "project_id": project_id,
                "default_branch": p.default_branch,
            });
            (StatusCode::OK, axum::Json(info)).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn handle_download_archive(
    State(state): State<Arc<MockState>>,
    Path(project_id): Path<i64>,
    Query(query): Query<ArchiveQuery>,
) -> impl IntoResponse {
    let slow = state
        .projects
        .lock()
        .get(&project_id)
        .is_some_and(|p| p.slow_archive);
    if slow {
        tokio::time::sleep(SLOW_ARCHIVE_DELAY).await;
    }
    let mut projects = state.projects.lock();
    match projects.get_mut(&project_id) {
        Some(ProjectData {
            archive_status: Some(status),
            ..
        }) => (*status).into_response(),
        Some(p) if p.empty_archive_responses > 0 => {
            if p.empty_archive_responses != u32::MAX {
                p.empty_archive_responses -= 1;
            }
            (StatusCode::OK, Vec::<u8>::new()).into_response()
        }
        Some(p) => {
            let files: Vec<(&str, &str)> = p
                .archive_files
                .iter()
                .map(|(path, content)| {
                    (
                        path.as_str(),
                        std::str::from_utf8(content)
                            .expect("test fixture content must be valid UTF-8"),
                    )
                })
                .collect();
            let archive = build_tar_gz(&files, &query.ref_name);
            (StatusCode::OK, archive).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Build a gzipped tar archive with the Gitaly `<slug>-<ref>/` prefix.
fn build_tar_gz(files: &[(&str, &str)], ref_name: &str) -> Vec<u8> {
    let mut tar_builder = tar::Builder::new(Vec::new());

    for (path, content) in files {
        let content_bytes = content.as_bytes();
        let mut header = tar::Header::new_gnu();
        let archive_path = format!("project-{ref_name}/{path}");
        header.set_path(&archive_path).unwrap();
        header.set_size(content_bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append(&header, content_bytes)
            .expect("tar append failed");
    }

    let tar_bytes = tar_builder.into_inner().expect("tar finish failed");

    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&tar_bytes).expect("gz write failed");
    encoder.finish().expect("gz finish failed")
}

pub fn enable_incremental(traversal_path: &str) {
    let namespace = orbit_utils::traversal_path::TraversalPath::new_unchecked(traversal_path)
        .top_level_namespace_id()
        .expect("traversal path has a top-level namespace");
    use orbit_server_config::features::{Feature, FeatureScope, FeaturesConfig};
    orbit_server_config::features::init(FeaturesConfig::from_iter([(
        Feature::CodeGraphIncremental,
        FeatureScope {
            enabled: true,
            namespaces: vec![namespace],
        },
    )]));
}

macro_rules! both_engines {
    ($($name:ident),+ $(,)?) => {$(
        mod $name {
            #[tokio::test]
            async fn legacy() {
                super::$name(false).await
            }

            #[tokio::test]
            async fn incremental() {
                super::$name(true).await
            }
        }
    )+};
}
pub(crate) use both_engines;

pub fn handler_context() -> HandlerContext {
    handler_context_with_lock_service(Arc::new(MockLockService::new()))
}

pub fn handler_context_with_lock_service(lock_service: Arc<MockLockService>) -> HandlerContext {
    let mock_nats = Arc::new(MockNatsServices::new());
    HandlerContext::new(mock_nats.clone(), lock_service, ProgressNotifier::noop())
}

pub fn handler_context_redelivered(redelivered_on_failure: bool) -> HandlerContext {
    handler_context().with_redelivery(redelivered_on_failure)
}

pub async fn assert_code_indexed(clickhouse: &TestContext, project_id: i64) {
    let branches = clickhouse
        .query(&format!(
            "SELECT name FROM {} FINAL \
             WHERE project_id = {project_id} AND _deleted = false",
            t("gl_branch")
        ))
        .await;
    assert!(
        branches.first().is_some_and(|b| b.num_rows() > 0),
        "no branch indexed"
    );

    let files = clickhouse
        .query(&format!(
            "SELECT path FROM {} WHERE project_id = {project_id}",
            t("gl_file")
        ))
        .await;
    assert!(
        files.first().is_some_and(|b| b.num_rows() > 0),
        "no files indexed"
    );

    let definitions = clickhouse
        .query(&format!(
            "SELECT name FROM {} WHERE project_id = {project_id}",
            t("gl_definition")
        ))
        .await;
    assert!(
        definitions.first().is_some_and(|b| b.num_rows() > 0),
        "no definitions indexed"
    );

    let ontology = integration_testkit::load_ontology();
    let defines_edges = clickhouse
        .query(&format!(
            "SELECT source_id FROM {} \
             WHERE source_kind = 'File' AND target_kind = 'Definition' \
             AND relationship_kind = 'DEFINES'",
            ontology.edge_table_for_relationship("DEFINES").unwrap()
        ))
        .await;
    assert!(
        defines_edges.first().is_some_and(|b| b.num_rows() > 0),
        "no DEFINES edges indexed"
    );
}
