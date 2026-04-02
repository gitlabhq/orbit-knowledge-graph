#![allow(dead_code)]

use std::io::Write;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use base64::Engine;
use clickhouse_client::ClickHouseConfigurationExt;
use flate2::Compression;
use flate2::write::GzEncoder;
use gitlab_client::{GitlabClient, GitlabClientConfiguration};
use indexer::handler::HandlerContext;
use indexer::modules::code::{
    ClickHouseCodeCheckpointStore, ClickHouseStaleDataCleaner, CodeIndexingPipeline,
    CodeIndexingTaskHandler, CodeIndexingTaskHandlerConfig, LocalRepositoryCache,
    RailsRepositoryService, RepositoryService, config::CodeTableNames, metrics::CodeMetrics,
    repository::RepositoryCache, repository::RepositoryResolver,
};
use indexer::nats::ProgressNotifier;
use indexer::testkit::{MockLockService, MockNatsServices};
use integration_testkit::TestContext;
use parking_lot::Mutex;
use serde::Deserialize;
use std::collections::HashMap;

const SIGNING_KEY: &[u8] = b"test-secret-that-is-long-enough!";

pub struct CodeIndexingDeps {
    pub pipeline: Arc<CodeIndexingPipeline>,
    pub repository_service: Arc<dyn RepositoryService>,
    pub checkpoint_store: Arc<ClickHouseCodeCheckpointStore>,
    pub metrics: CodeMetrics,
}

impl CodeIndexingDeps {
    pub fn new(mock: &MockGitlabServer, clickhouse: &TestContext) -> Self {
        let repository_service = RailsRepositoryService::create(Arc::new(mock.gitlab_client()));
        let graph_client = Arc::new(clickhouse.config.build_client());
        let checkpoint_store = Arc::new(ClickHouseCodeCheckpointStore::new(Arc::clone(
            &graph_client,
        )));
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let table_names =
            Arc::new(CodeTableNames::from_ontology(&ontology).expect("code tables must resolve"));

        let stale_data_cleaner =
            Arc::new(ClickHouseStaleDataCleaner::new(graph_client, &table_names));
        let metrics = CodeMetrics::new();

        let cache: Arc<dyn RepositoryCache> = Arc::new(LocalRepositoryCache::default());
        let resolver =
            RepositoryResolver::new(Arc::clone(&repository_service), cache, metrics.clone());

        let pipeline = Arc::new(CodeIndexingPipeline::new(
            resolver,
            Arc::clone(&checkpoint_store) as _,
            stale_data_cleaner,
            metrics.clone(),
            table_names,
        ));

        Self {
            pipeline,
            repository_service,
            checkpoint_store,
            metrics,
        }
    }

    pub fn code_indexing_task_handler(&self) -> CodeIndexingTaskHandler {
        CodeIndexingTaskHandler::new(
            Arc::clone(&self.pipeline),
            Arc::clone(&self.repository_service),
            Arc::clone(&self.checkpoint_store) as _,
            self.metrics.clone(),
            CodeIndexingTaskHandlerConfig::default(),
        )
    }
}

// ---------------------------------------------------------------------------
// Mock GitLab server — serves /api/v4/internal/orbit/project/... endpoints
// ---------------------------------------------------------------------------

struct MockState {
    projects: Mutex<HashMap<i64, ProjectData>>,
}

struct ProjectData {
    default_branch: String,
    /// Raw file entries (path, content). The archive is built on-the-fly
    /// in the handler using the ref from the request query, so the Gitaly
    /// `<slug>-<ref>/` prefix matches whatever commit SHA the indexer asks for.
    archive_files: Vec<(String, Vec<u8>)>,
    changed_paths_ndjson: Option<String>,
    blobs: HashMap<String, Vec<u8>>,
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
            .route(
                "/api/v4/internal/orbit/project/{project_id}/repository/changed_paths",
                get(handle_changed_paths),
            )
            .route(
                "/api/v4/internal/orbit/project/{project_id}/repository/list_blobs",
                post(handle_list_blobs),
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
                changed_paths_ndjson: None,
                blobs: HashMap::new(),
            },
        );
    }

    pub fn replace_archive(&self, project_id: i64, files: &[(&str, &str)]) {
        let mut projects = self.state.projects.lock();
        if let Some(project) = projects.get_mut(&project_id) {
            project.archive_files = files
                .iter()
                .map(|(p, c)| (p.to_string(), c.as_bytes().to_vec()))
                .collect();
        }
    }

    pub fn set_changed_paths(&self, project_id: i64, ndjson: &str) {
        let mut projects = self.state.projects.lock();
        if let Some(project) = projects.get_mut(&project_id) {
            project.changed_paths_ndjson = Some(ndjson.to_string());
        }
    }

    pub fn add_blob(&self, project_id: i64, oid: &str, content: &[u8]) {
        let mut projects = self.state.projects.lock();
        if let Some(project) = projects.get_mut(&project_id) {
            project.blobs.insert(oid.to_string(), content.to_vec());
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
    let projects = state.projects.lock();
    match projects.get(&project_id) {
        Some(p) => {
            // Build the archive on-the-fly with the Gitaly prefix so the
            // ref in the directory name matches the commit SHA the indexer sent.
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

#[derive(Deserialize)]
struct ChangedPathsQuery {
    #[allow(dead_code)]
    left_tree_revision: String,
    #[allow(dead_code)]
    right_tree_revision: String,
}

async fn handle_changed_paths(
    State(state): State<Arc<MockState>>,
    Path(project_id): Path<i64>,
    Query(_query): Query<ChangedPathsQuery>,
) -> impl IntoResponse {
    let projects = state.projects.lock();
    match projects.get(&project_id) {
        Some(p) => match &p.changed_paths_ndjson {
            Some(ndjson) => (StatusCode::OK, ndjson.clone()).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Deserialize)]
struct ListBlobsRequest {
    revisions: Vec<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct MockListBlobsResponse {
    #[prost(message, repeated, tag = "1")]
    blobs: Vec<MockBlobChunk>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct MockBlobChunk {
    #[prost(string, tag = "1")]
    oid: String,
    #[prost(int64, tag = "2")]
    size: i64,
    #[prost(bytes = "vec", tag = "3")]
    data: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    path: Vec<u8>,
}

async fn handle_list_blobs(
    State(state): State<Arc<MockState>>,
    Path(project_id): Path<i64>,
    axum::Json(body): axum::Json<ListBlobsRequest>,
) -> impl IntoResponse {
    use prost::Message;

    let projects = state.projects.lock();
    match projects.get(&project_id) {
        Some(p) => {
            let chunks: Vec<MockBlobChunk> = body
                .revisions
                .iter()
                .filter_map(|oid| {
                    p.blobs.get(oid).map(|data| MockBlobChunk {
                        oid: oid.clone(),
                        size: data.len() as i64,
                        data: data.clone(),
                        path: Vec::new(),
                    })
                })
                .collect();
            let response = MockListBlobsResponse { blobs: chunks };
            let frame = response.encode_to_vec();
            let mut buf = Vec::with_capacity(4 + frame.len());
            buf.extend_from_slice(&(frame.len() as u32).to_be_bytes());
            buf.extend_from_slice(&frame);
            (StatusCode::OK, Bytes::from(buf)).into_response()
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

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

pub fn handler_context(clickhouse: &TestContext) -> HandlerContext {
    use indexer::clickhouse::ClickHouseDestination;
    use indexer::metrics::EngineMetrics;

    let destination = ClickHouseDestination::new(
        clickhouse.config.clone(),
        Arc::new(EngineMetrics::default()),
    )
    .expect("failed to create destination");

    HandlerContext::new(
        Arc::new(destination),
        Arc::new(MockNatsServices::new()),
        Arc::new(MockLockService::new()),
        ProgressNotifier::noop(),
    )
}

pub async fn assert_code_indexed(clickhouse: &TestContext, project_id: i64) {
    let branches = clickhouse
        .query(&format!(
            "SELECT name FROM gl_branch FINAL \
             WHERE project_id = {project_id} AND _deleted = false"
        ))
        .await;
    assert!(
        branches.first().is_some_and(|b| b.num_rows() > 0),
        "no branch indexed"
    );

    let files = clickhouse
        .query(&format!(
            "SELECT path FROM gl_file WHERE project_id = {project_id}"
        ))
        .await;
    assert!(
        files.first().is_some_and(|b| b.num_rows() > 0),
        "no files indexed"
    );

    let definitions = clickhouse
        .query(&format!(
            "SELECT name FROM gl_definition WHERE project_id = {project_id}"
        ))
        .await;
    assert!(
        definitions.first().is_some_and(|b| b.num_rows() > 0),
        "no definitions indexed"
    );

    let defines_edges = clickhouse
        .query(
            "SELECT source_id FROM gl_edge \
             WHERE source_kind = 'File' AND target_kind = 'Definition' \
             AND relationship_kind = 'DEFINES'",
        )
        .await;
    assert!(
        defines_edges.first().is_some_and(|b| b.num_rows() > 0),
        "no DEFINES edges indexed"
    );
}
