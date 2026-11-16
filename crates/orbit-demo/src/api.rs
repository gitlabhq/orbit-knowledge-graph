//! The page and its API: start a run, stream its events over Server-Sent Events, render its
//! response in the server's formats, reload the seed, and read the package YAML the model loaded.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::Stream;
use query_engine::formatters::{GqlFormatter, GraphFormatter, ResultFormatter, ToonFormatter};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::Demo;
use crate::run::{RunLog, run};

const KEPT_RUNS: u64 = 64;

const FORMATTERS: [&dyn ResultFormatter; 3] = [&ToonFormatter, &GqlFormatter, &GraphFormatter];

const ASSETS: [(&str, &str, &[u8]); 16] = [
    (
        "/",
        "text/html; charset=utf-8",
        include_bytes!("../assets/index.html"),
    ),
    (
        "/assets/tokens.css",
        "text/css; charset=utf-8",
        include_bytes!("../assets/tokens.css"),
    ),
    (
        "/assets/components.css",
        "text/css; charset=utf-8",
        include_bytes!("../assets/components.css"),
    ),
    (
        "/assets/app.css",
        "text/css; charset=utf-8",
        include_bytes!("../assets/app.css"),
    ),
    (
        "/assets/theme.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/theme.js"),
    ),
    (
        "/assets/app.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/app.js"),
    ),
    (
        "/assets/dag.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/dag.js"),
    ),
    (
        "/assets/highlight.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/highlight.js"),
    ),
    (
        "/assets/panels.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/panels.js"),
    ),
    (
        "/assets/vendor/elk/elk.bundled.js",
        "text/javascript; charset=utf-8",
        include_bytes!("../assets/vendor/elk/elk.bundled.js"),
    ),
    (
        "/assets/vendor/elk/LICENSE.md",
        "text/markdown; charset=utf-8",
        include_bytes!("../assets/vendor/elk/LICENSE.md"),
    ),
    (
        "/assets/tanuki.svg",
        "image/svg+xml",
        include_bytes!("../assets/tanuki.svg"),
    ),
    (
        "/assets/vendor/fonts/GitLabSans.woff2",
        "font/woff2",
        include_bytes!("../assets/vendor/fonts/GitLabSans.woff2"),
    ),
    (
        "/assets/vendor/fonts/GitLabMono.woff2",
        "font/woff2",
        include_bytes!("../assets/vendor/fonts/GitLabMono.woff2"),
    ),
    (
        "/assets/vendor/fonts/GitLabSans-LICENSE.txt",
        "text/plain; charset=utf-8",
        include_bytes!("../assets/vendor/fonts/GitLabSans-LICENSE.txt"),
    ),
    (
        "/assets/vendor/fonts/GitLabMono-OFL.txt",
        "text/plain; charset=utf-8",
        include_bytes!("../assets/vendor/fonts/GitLabMono-OFL.txt"),
    ),
];

#[derive(Default)]
pub struct Runs {
    next: AtomicU64,
    logs: Mutex<BTreeMap<u64, Arc<RunLog>>>,
}

impl Runs {
    fn get(&self, id: u64) -> Option<Arc<RunLog>> {
        self.logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&id)
            .cloned()
    }
}

pub fn router(demo: Arc<Demo>) -> Router {
    let mut router = Router::new()
        .route("/api/meta", get(meta))
        .route("/api/runs", post(start))
        .route("/api/runs/{id}/events", get(events))
        .route("/api/runs/{id}/result", get(result))
        .route("/api/reload", post(reload))
        .route("/api/packages/{name}", get(package));
    for (path, content_type, body) in ASSETS {
        router = router.route(
            path,
            get(move || async move { ([(header::CONTENT_TYPE, content_type)], body) }),
        );
    }
    router.with_state(demo)
}

fn failure(status: StatusCode, error: impl std::fmt::Display) -> Response {
    (status, Json(json!({"error": error.to_string()}))).into_response()
}

async fn meta(State(demo): State<Arc<Demo>>) -> Json<Value> {
    let callers = crate::fakes::current(&demo.callers);
    let callers: Vec<Value> = callers
        .callers
        .iter()
        .map(|(name, declared)| json!({"name": name, "declared": declared}))
        .collect();
    let examples = crate::fakes::current(&demo.examples);
    let graph = demo.model.graph();
    let entities: Vec<&str> = graph
        .entities()
        .map(|entity| entity.name.as_str())
        .collect();
    let relationships: Vec<&str> = graph.relationships().map(|r| r.name.as_str()).collect();
    Json(json!({
        "callers": callers,
        "examples": *examples,
        "entities": entities,
        "relationships": relationships,
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    query: String,
    caller: String,
}

async fn start(State(demo): State<Arc<Demo>>, Json(request): Json<Start>) -> Json<Value> {
    let id = demo.runs.next.fetch_add(1, Ordering::SeqCst);
    let log = Arc::new(RunLog::default());
    {
        let mut logs = demo
            .runs
            .logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        logs.insert(id, Arc::clone(&log));
        logs.retain(|kept, _| kept + KEPT_RUNS > id);
    }
    tokio::spawn(run(Arc::clone(&demo), request.query, request.caller, log));
    Json(json!({"run": id}))
}

async fn events(State(demo): State<Arc<Demo>>, Path(id): Path<u64>) -> Response {
    match demo.runs.get(id) {
        Some(log) => Sse::new(stream(log))
            .keep_alive(KeepAlive::default())
            .into_response(),
        None => failure(StatusCode::NOT_FOUND, format!("no run {id}")),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Format {
    format: String,
}

/// The run's response as the server's formatter for `format` renders it.
async fn result(
    State(demo): State<Arc<Demo>>,
    Path(id): Path<u64>,
    Query(Format { format }): Query<Format>,
) -> Response {
    let Some(formatter) = FORMATTERS
        .into_iter()
        .find(|formatter| formatter.format_name().as_ref() == format)
    else {
        return failure(StatusCode::BAD_REQUEST, format!("no format {format}"));
    };
    let Some(log) = demo.runs.get(id) else {
        return failure(StatusCode::NOT_FOUND, format!("no run {id}"));
    };
    match log.response() {
        Some(Ok(output)) => Json(formatter.format(output)).into_response(),
        Some(Err(error)) => failure(StatusCode::INTERNAL_SERVER_ERROR, error),
        None => failure(StatusCode::CONFLICT, format!("run {id} has no response")),
    }
}

fn stream(log: Arc<RunLog>) -> impl Stream<Item = Result<SseEvent, Infallible>> {
    futures::stream::unfold((log, 0), |(log, index)| async move {
        let event = log.next(index).await?;
        Some((Ok(SseEvent::default().data(event)), (log, index + 1)))
    })
}

/// A package's files in the order a reader meets them: the package, its graph, its storage,
/// its authorization, then its sources.
async fn package(State(demo): State<Arc<Demo>>, Path(name): Path<String>) -> Response {
    const ORDER: [&str; 5] = [
        "package.yaml",
        "graph.yaml",
        "storage/",
        "authorization.yaml",
        "sources/",
    ];
    let prefix = format!("{name}/");
    let mut files: Vec<(&str, &str)> = demo
        .packages
        .iter()
        .filter_map(|(path, text)| Some((path.strip_prefix(&prefix)?, text.as_str())))
        .collect();
    if files.is_empty() {
        return failure(StatusCode::NOT_FOUND, format!("no package {name}"));
    }
    files.sort_by_key(|(path, _)| {
        let rank = ORDER.iter().position(|lead| path.starts_with(lead));
        (rank.unwrap_or(ORDER.len()), *path)
    });
    let files: Vec<Value> = files
        .into_iter()
        .map(|(path, text)| json!({"path": path, "text": text}))
        .collect();
    Json(files).into_response()
}

async fn reload(State(demo): State<Arc<Demo>>, Json(_): Json<Value>) -> Response {
    match demo.reload().await {
        Ok(rows) => Json(json!({"rows": rows})).into_response(),
        Err(error) => failure(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")),
    }
}
