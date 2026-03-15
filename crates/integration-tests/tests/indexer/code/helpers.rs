#![allow(dead_code)]

use std::io::Write;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use base64::Engine;
use flate2::Compression;
use flate2::write::GzEncoder;
use gitlab_client::{GitlabClient, GitlabClientConfiguration};
use indexer::handler::HandlerContext;
use indexer::modules::code::{
    ClickHouseCodeCheckpointStore, ClickHouseStaleDataCleaner, CodeIndexingHandler,
    CodeIndexingHandlerConfig, CodeIndexingPipeline, RailsRepositoryService, RepositoryService,
    config::CodeTableNames, metrics::CodeMetrics,
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

        let pipeline = Arc::new(CodeIndexingPipeline::new(
            Arc::clone(&repository_service),
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

    pub fn code_indexing_handler(&self) -> CodeIndexingHandler {
        CodeIndexingHandler::new(
            Arc::clone(&self.pipeline),
            Arc::clone(&self.repository_service),
            Arc::clone(&self.checkpoint_store) as _,
            self.metrics.clone(),
            CodeIndexingHandlerConfig::default(),
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
    archive: Vec<u8>,
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
        self.state.projects.lock().insert(
            project_id,
            ProjectData {
                default_branch: default_branch.to_string(),
                archive: build_tar_gz(files),
            },
        );
    }

    pub fn replace_archive(&self, project_id: i64, files: &[(&str, &str)]) {
        let mut projects = self.state.projects.lock();
        if let Some(project) = projects.get_mut(&project_id) {
            project.archive = build_tar_gz(files);
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
    Query(_query): Query<ArchiveQuery>,
) -> impl IntoResponse {
    let projects = state.projects.lock();
    match projects.get(&project_id) {
        Some(p) => (StatusCode::OK, p.archive.clone()).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn build_tar_gz(files: &[(&str, &str)]) -> Vec<u8> {
    let mut tar_builder = tar::Builder::new(Vec::new());

    for (path, content) in files {
        let content_bytes = content.as_bytes();
        let mut header = tar::Header::new_gnu();
        header.set_path(path).unwrap();
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

pub async fn create_project_in_graph(
    clickhouse: &TestContext,
    project_id: i64,
    traversal_path: &str,
    full_path: &str,
) {
    clickhouse
        .execute(&format!(
            "INSERT INTO gl_project (id, traversal_path, full_path, _version) \
             VALUES ({project_id}, '{traversal_path}', '{full_path}', 1)",
        ))
        .await;
}

pub async fn assert_code_indexed(clickhouse: &TestContext, project_id: i64) {
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
