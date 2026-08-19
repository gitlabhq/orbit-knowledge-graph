use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unhealthy_components: Vec<&'static str>,
}

fn version() -> &'static str {
    orbit_utils::version::get()
}

async fn live() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: version(),
        unhealthy_components: Vec::new(),
    })
}

async fn ready(State(serving): State<Arc<AtomicBool>>) -> impl IntoResponse {
    if !serving.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "starting",
                version: version(),
                unhealthy_components: vec!["schema_gate"],
            }),
        );
    }

    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok",
            version: version(),
            unhealthy_components: Vec::new(),
        }),
    )
}

pub fn create_health_router(serving: Arc<AtomicBool>) -> Router {
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(serving)
}

pub async fn run_health_server(
    bind_address: SocketAddr,
    serving: Arc<AtomicBool>,
) -> Result<(), std::io::Error> {
    let app = create_health_router(serving);

    let listener = TcpListener::bind(bind_address).await?;

    info!(%bind_address, "indexer health server listening");

    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;

    fn ready_request() -> Request<Body> {
        Request::get("/ready").body(Body::empty()).unwrap()
    }

    async fn parse_response(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let json = serde_json::from_slice(&body).unwrap();
        (status, json)
    }

    #[tokio::test]
    async fn readiness_probe_is_ok_when_serving() {
        let router = create_health_router(Arc::new(AtomicBool::new(true)));

        let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
        assert!(json.get("unhealthy_components").is_none());
    }

    #[tokio::test]
    async fn readiness_probe_is_unavailable_until_serving() {
        let router = create_health_router(Arc::new(AtomicBool::new(false)));

        let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(json["status"], "starting");
        assert_eq!(
            json["unhealthy_components"],
            serde_json::json!(["schema_gate"])
        );
    }
}
