use std::sync::Arc;

use axum::extract::State;
use axum::{Json, Router, routing::get};
use labkit::correlation::CorrelationLayer;
use labkit::metrics::MetricsLayer;
use serde::Serialize;
use tower_http::trace::TraceLayer;

use crate::cluster_health::ClusterHealthChecker;
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
        version: env!("CARGO_PKG_VERSION"),
    })
}

async fn cluster_health(State(state): State<AppState>) -> Json<ClusterHealthResponse> {
    let health = state.cluster_health.get_cluster_health().await;

    let status = match health.status {
        1 => "healthy",
        2 => "degraded",
        3 => "unhealthy",
        _ => "unknown",
    };

    let components = health
        .components
        .into_iter()
        .map(|c| {
            let component_status = match c.status {
                1 => "healthy",
                2 => "degraded",
                3 => "unhealthy",
                _ => "unknown",
            };

            ComponentHealthResponse {
                name: c.name,
                status: component_status.to_string(),
                replicas: c.replicas.map(|r| ReplicaStatusResponse {
                    ready: r.ready,
                    desired: r.desired,
                }),
                metrics: c.metrics,
            }
        })
        .collect();

    Json(ClusterHealthResponse {
        status: status.to_string(),
        timestamp: health.timestamp,
        version: health.version,
        components,
    })
}

pub fn create_router(_validator: JwtValidator, health_check_url: Option<String>) -> Router {
    let state = AppState {
        cluster_health: ClusterHealthChecker::new(health_check_url).into_arc(),
    };

    Router::new()
        .route("/health", get(health))
        .route("/api/v1/cluster_health", get(cluster_health))
        .with_state(state)
        .layer(MetricsLayer::new())
        .layer(CorrelationLayer::new().propagate_incoming(true))
        .layer(TraceLayer::new_for_http())
}
