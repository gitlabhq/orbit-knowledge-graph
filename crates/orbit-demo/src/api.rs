//! The page and its API: start a run, stream its events over Server-Sent Events, reload the seed.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::Stream;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::Demo;
use crate::run::{RunLog, run};

const KEPT_RUNS: u64 = 64;

const ASSETS: [(&str, &str, &str); 6] = [
    (
        "/",
        "text/html; charset=utf-8",
        include_str!("../assets/index.html"),
    ),
    (
        "/assets/app.js",
        "text/javascript; charset=utf-8",
        include_str!("../assets/app.js"),
    ),
    (
        "/assets/dag.js",
        "text/javascript; charset=utf-8",
        include_str!("../assets/dag.js"),
    ),
    (
        "/assets/panels.js",
        "text/javascript; charset=utf-8",
        include_str!("../assets/panels.js"),
    ),
    (
        "/assets/app.css",
        "text/css; charset=utf-8",
        include_str!("../assets/app.css"),
    ),
    (
        "/assets/vendor/cytoscape.min.js",
        "text/javascript; charset=utf-8",
        include_str!("../assets/vendor/cytoscape.min.js"),
    ),
];

#[derive(Default)]
pub struct Runs {
    next: AtomicU64,
    logs: Mutex<BTreeMap<u64, Arc<RunLog>>>,
}

pub fn router(demo: Arc<Demo>) -> Router {
    let mut router = Router::new()
        .route("/api/meta", get(meta))
        .route("/api/runs", post(start))
        .route("/api/runs/{id}/events", get(events))
        .route("/api/reload", post(reload));
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
        .map(|(name, declared)| json!({"name": name, "label": declared.label(), "declared": declared}))
        .collect();
    let examples = crate::fakes::current(&demo.examples);
    Json(json!({"callers": callers, "examples": *examples}))
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
    let log = demo
        .runs
        .logs
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&id)
        .cloned();
    match log {
        Some(log) => Sse::new(stream(log))
            .keep_alive(KeepAlive::default())
            .into_response(),
        None => failure(StatusCode::NOT_FOUND, format!("no run {id}")),
    }
}

fn stream(log: Arc<RunLog>) -> impl Stream<Item = Result<SseEvent, Infallible>> {
    futures::stream::unfold((log, 0), |(log, index)| async move {
        let event = log.next(index).await?;
        Some((Ok(SseEvent::default().data(event)), (log, index + 1)))
    })
}

async fn reload(State(demo): State<Arc<Demo>>, Json(_): Json<Value>) -> Response {
    match demo.reload().await {
        Ok(rows) => Json(json!({"rows": rows})).into_response(),
        Err(error) => failure(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")),
    }
}
