use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use super::AppState;
use crate::webserver::auth::{AuthenticatedUser, ErrorResponse};
use crate::webserver::tools::{ToolInfo, ToolResult};

#[derive(Serialize)]
pub struct ToolsResponse {
    pub tools: Vec<ToolInfo>,
}

pub async fn list_tools(
    State(state): State<AppState>,
    _user: AuthenticatedUser,
) -> Json<ToolsResponse> {
    Json(ToolsResponse {
        tools: state.registry.list(),
    })
}

#[derive(Deserialize)]
pub struct CallToolRequest {
    pub arguments: serde_json::Value,
}

fn is_valid_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

pub async fn call_tool(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Path(name): Path<String>,
    Json(req): Json<CallToolRequest>,
) -> Result<Json<ToolResult>, impl IntoResponse> {
    if !is_valid_tool_name(&name) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Invalid tool name".into(),
            }),
        ));
    }

    state
        .registry
        .call(&name, req.arguments, &user.0)
        .await
        .map(Json)
        .map_err(|e| {
            let status = match &e {
                crate::error::ServerError::ToolNotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })
}
