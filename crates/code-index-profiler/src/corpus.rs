use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path as AxumPath, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use base64::Engine;
use gitlab_client::GitlabClient;
use orbit_server_config::GitlabClientConfiguration;
use tokio_util::io::ReaderStream;

const SIGNING_KEY: &[u8] = b"code-index-profiler-signing-key!";

struct CorpusState {
    dir: PathBuf,
    default_branch: String,
}

/// Serves the two Rails internal-API endpoints the code indexer calls, backed
/// by `<project_id>.tar.gz` files on disk. The archive body is streamed from
/// the file so the indexer's download path sees the same chunked reader shape
/// it gets from Workhorse.
pub struct CorpusServer {
    base_url: String,
}

impl CorpusServer {
    pub async fn start(dir: &Path, default_branch: &str) -> anyhow::Result<Self> {
        let state = Arc::new(CorpusState {
            dir: dir.to_path_buf(),
            default_branch: default_branch.to_string(),
        });

        let app = Router::new()
            .route(
                "/api/v4/internal/orbit/project/{project_id}/info",
                get(project_info),
            )
            .route(
                "/api/v4/internal/orbit/project/{project_id}/repository/archive",
                get(archive),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        Ok(Self {
            base_url: format!("http://{addr}"),
        })
    }

    pub fn gitlab_client(&self) -> anyhow::Result<GitlabClient> {
        let config = GitlabClientConfiguration {
            base_url: self.base_url.clone(),
            signing_key: base64::engine::general_purpose::STANDARD.encode(SIGNING_KEY),
            resolve_host: None,
        };
        Ok(GitlabClient::new(config)?)
    }
}

pub fn archive_path(dir: &Path, project_id: i64) -> PathBuf {
    dir.join(format!("{project_id}.tar.gz"))
}

async fn project_info(
    State(state): State<Arc<CorpusState>>,
    AxumPath(project_id): AxumPath<i64>,
) -> impl IntoResponse {
    if !archive_path(&state.dir, project_id).exists() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let body = serde_json::json!({
        "project_id": project_id,
        "default_branch": state.default_branch,
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

async fn archive(
    State(state): State<Arc<CorpusState>>,
    AxumPath(project_id): AxumPath<i64>,
) -> impl IntoResponse {
    let path = archive_path(&state.dir, project_id);
    let file = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    let body = axum::body::Body::from_stream(ReaderStream::new(file));
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/gzip".to_string()),
            (header::CONTENT_LENGTH, len.to_string()),
        ],
        body,
    )
        .into_response()
}
