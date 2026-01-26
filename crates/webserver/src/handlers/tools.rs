use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::auth::AuthenticatedUser;
use crate::error::WebserverError;
use crate::tools::{ToolInfo, ToolRegistry, ToolResult};

#[derive(Clone)]
pub struct ToolsState {
    pub registry: Arc<ToolRegistry>,
}

#[derive(Debug, Serialize)]
pub struct ListToolsResponse {
    pub tools: Vec<ToolInfo>,
}

#[derive(Debug, Deserialize)]
pub struct CallToolRequest {
    pub arguments: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

pub async fn list_tools(
    State(state): State<ToolsState>,
    _user: AuthenticatedUser,
) -> Json<ListToolsResponse> {
    let tools = state.registry.list_tools();
    Json(ListToolsResponse { tools })
}

pub async fn call_tool(
    State(state): State<ToolsState>,
    user: AuthenticatedUser,
    Path(tool_name): Path<String>,
    Json(request): Json<CallToolRequest>,
) -> Result<Json<ToolResult>, impl IntoResponse> {
    let claims = user.claims();

    state
        .registry
        .call_tool(&tool_name, request.arguments, claims)
        .await
        .map(Json)
        .map_err(|e| {
            let (status, message) = match &e {
                WebserverError::ToolNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
                WebserverError::ToolExecution(_) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            };

            (status, Json(ErrorResponse { error: message }))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    use crate::auth::Claims;
    use crate::tools::{KnowledgeGraphTool, ToolError};

    struct TestTool;

    #[async_trait]
    impl KnowledgeGraphTool for TestTool {
        fn name(&self) -> &str {
            "test_tool"
        }

        fn description(&self) -> &str {
            "A test tool"
        }

        fn input_schema(&self) -> serde_json::Value {
            json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" }
                }
            })
        }

        async fn call(
            &self,
            _params: serde_json::Value,
            _claims: &Claims,
        ) -> Result<ToolResult, ToolError> {
            Ok(ToolResult::success_text("Test result"))
        }
    }

    fn create_test_claims() -> Claims {
        Claims {
            sub: "user:123".to_string(),
            iss: "gitlab".to_string(),
            aud: "gitlab-knowledge-graph".to_string(),
            iat: 0,
            exp: 0,
            user_id: 123,
            username: "test".to_string(),
            admin: false,
            organization_id: None,
            min_access_level: None,
            group_traversal_ids: vec![],
            project_ids: vec![],
        }
    }

    fn create_test_state() -> ToolsState {
        let mut registry = ToolRegistry::new();
        registry.register(TestTool);
        ToolsState {
            registry: Arc::new(registry),
        }
    }

    #[tokio::test]
    async fn test_list_tools() {
        let state = create_test_state();
        let user = AuthenticatedUser(create_test_claims());

        let response = list_tools(State(state), user).await;

        assert_eq!(response.tools.len(), 1);
        assert_eq!(response.tools[0].name, "test_tool");
    }

    #[tokio::test]
    async fn test_call_tool_success() {
        let state = create_test_state();
        let user = AuthenticatedUser(create_test_claims());
        let request = CallToolRequest {
            arguments: json!({"query": "test"}),
        };

        let result = call_tool(
            State(state),
            user,
            Path("test_tool".to_string()),
            Json(request),
        )
        .await;

        match result {
            Ok(response) => {
                assert!(!response.is_error);
                assert_eq!(response.content[0].text, "Test result");
            }
            Err(_) => panic!("Expected Ok result"),
        }
    }

    #[tokio::test]
    async fn test_call_tool_not_found() {
        let state = create_test_state();
        let user = AuthenticatedUser(create_test_claims());
        let request = CallToolRequest {
            arguments: json!({}),
        };

        let result = call_tool(
            State(state),
            user,
            Path("nonexistent".to_string()),
            Json(request),
        )
        .await;

        assert!(result.is_err());
    }
}
