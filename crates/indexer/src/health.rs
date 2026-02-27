use std::net::SocketAddr;
use std::time::Duration;

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use clickhouse_client::ArrowClickHouseClient;
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
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unhealthy_components: Vec<&'static str>,
}

fn version() -> &'static str {
    match option_env!("GKG_VERSION") {
        Some(v) => v,
        None => env!("CARGO_PKG_VERSION"),
    }
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

pub async fn run_health_server(
    bind_address: SocketAddr,
    state: HealthState,
) -> Result<(), std::io::Error> {
    let app = Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(state);

    let listener = TcpListener::bind(bind_address).await?;

    info!(%bind_address, "indexer health server listening");

    axum::serve(listener, app).await
}
