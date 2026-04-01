use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use clickhouse_client::ArrowClickHouseClient;
use gitlab_client::GitlabClient;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tracing::info;

const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct HealthState {
    pub nats_client: async_nats::Client,
    pub graph_client: ArrowClickHouseClient,
    pub datalake_client: ArrowClickHouseClient,
    pub gitlab_client: Option<Arc<GitlabClient>>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unhealthy_components: Vec<&'static str>,
}

fn version() -> &'static str {
    use std::sync::OnceLock;
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            std::env::var("GKG_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string())
        })
        .as_str()
}

async fn live() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: version(),
        unhealthy_components: Vec::new(),
    })
}

async fn ready(State(state): State<HealthState>) -> impl IntoResponse {
    let nats_healthy =
        state.nats_client.connection_state() == async_nats::connection::State::Connected;

    let graph_healthy = timeout(HEALTH_CHECK_TIMEOUT, state.graph_client.execute("SELECT 1"))
        .await
        .is_ok_and(|r| r.is_ok());
    let datalake_healthy = timeout(
        HEALTH_CHECK_TIMEOUT,
        state.datalake_client.execute("SELECT 1"),
    )
    .await
    .is_ok_and(|r| r.is_ok());

    let gitlab_healthy = match &state.gitlab_client {
        Some(client) => timeout(HEALTH_CHECK_TIMEOUT, client.project_info(1))
            .await
            .is_ok_and(|r| {
                matches!(
                    r,
                    Ok(_) | Err(gitlab_client::GitlabClientError::NotFound(_))
                )
            }),
        None => true,
    };

    let mut unhealthy_components = Vec::new();
    if !nats_healthy {
        unhealthy_components.push("nats");
    }
    if !graph_healthy {
        unhealthy_components.push("clickhouse_graph");
    }
    if !datalake_healthy {
        unhealthy_components.push("clickhouse_datalake");
    }
    if !gitlab_healthy {
        unhealthy_components.push("gitlab");
    }

    let healthy = unhealthy_components.is_empty();

    let status = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let label = if healthy { "ok" } else { "unhealthy" };

    (
        status,
        Json(HealthResponse {
            status: label,
            version: version(),
            unhealthy_components,
        }),
    )
}

pub fn create_health_router(state: HealthState) -> Router {
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(state)
}

pub async fn run_health_server(
    bind_address: SocketAddr,
    state: HealthState,
) -> Result<(), std::io::Error> {
    let app = create_health_router(state);

    let listener = TcpListener::bind(bind_address).await?;

    info!(%bind_address, "indexer health server listening");

    axum::serve(listener, app).await
}
