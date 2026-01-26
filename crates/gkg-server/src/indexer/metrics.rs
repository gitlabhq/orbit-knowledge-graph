use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;

#[derive(Serialize)]
pub struct MetricsResponse {
    pub status: &'static str,
}

pub async fn metrics() -> impl IntoResponse {
    (StatusCode::OK, Json(MetricsResponse { status: "ok" }))
}
