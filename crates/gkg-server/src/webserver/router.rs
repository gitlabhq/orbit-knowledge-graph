use std::sync::Arc;

use axum::extract::State;
use axum::{Json, Router, routing::get};
use labkit_rs::correlation::http::{CorrelationIdLayer, PropagateCorrelationIdLayer};
use labkit_rs::metrics::http::HttpMetricsLayer;
use serde::Serialize;
use tower_http::trace::TraceLayer;

use crate::cluster_health::ClusterHealthChecker;
use crate::proto::{ClusterStatus, ResponseFormat, get_cluster_health_response};
use crate::webserver::JwtValidator;

#[derive(Clone)]
pub struct AppState {
    pub cluster_health: Arc<ClusterHealthChecker>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

#[derive(Serialize)]
struct ClusterHealthResponse {
    status: String,
    timestamp: String,
    version: String,
    components: Vec<ComponentHealthResponse>,
}

#[derive(Serialize)]
struct ComponentHealthResponse {
    name: String,
    status: String,
    replicas: Option<ReplicaStatusResponse>,
    metrics: std::collections::HashMap<String, String>,
}

#[derive(Serialize)]
struct ReplicaStatusResponse {
    ready: i32,
    desired: i32,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: option_env!("GKG_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")),
    })
}

fn status_label(val: i32) -> &'static str {
    match ClusterStatus::try_from(val) {
        Ok(ClusterStatus::Healthy) => "healthy",
        Ok(ClusterStatus::Degraded) => "degraded",
        Ok(ClusterStatus::Unhealthy) => "unhealthy",
        _ => "unknown",
    }
}

async fn cluster_health_handler(State(state): State<AppState>) -> Json<ClusterHealthResponse> {
    let health = state
        .cluster_health
        .get_cluster_health(ResponseFormat::Raw as i32)
        .await;

    let structured = match health.content {
        Some(get_cluster_health_response::Content::Structured(s)) => s,
        _ => {
            return Json(ClusterHealthResponse {
                status: "unknown".to_string(),
                timestamp: String::new(),
                version: String::new(),
                components: vec![],
            });
        }
    };

    let components = structured
        .components
        .into_iter()
        .map(|c| ComponentHealthResponse {
            name: c.name,
            status: status_label(c.status).to_string(),
            replicas: c.replicas.map(|r| ReplicaStatusResponse {
                ready: r.ready,
                desired: r.desired,
            }),
            metrics: c.metrics,
        })
        .collect();

    Json(ClusterHealthResponse {
        status: status_label(structured.status).to_string(),
        timestamp: structured.timestamp,
        version: structured.version,
        components,
    })
}

pub fn create_router(
    _validator: JwtValidator,
    cluster_health: Arc<ClusterHealthChecker>,
) -> Router {
    let state = AppState { cluster_health };

    Router::new()
        .route("/health", get(health))
        .route("/api/v1/cluster_health", get(cluster_health_handler))
        .with_state(state)
        .layer(HttpMetricsLayer::new())
        .layer(CorrelationIdLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(PropagateCorrelationIdLayer::new())
}
