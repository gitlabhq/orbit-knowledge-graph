//! gRPC service implementation for the Knowledge Graph.
//!
//! Implements the KnowledgeGraphService with:
//! - ListTools: Simple unary RPC for tool discovery
//! - ExecuteTool: Bidirectional streaming for tool execution with redaction flow

use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

use crate::auth::JwtValidator;
use crate::context_engine::ContextEngine;
use crate::proto::{
    ExecuteToolMessage, ListToolsRequest, ListToolsResponse, RedactionRequired,
    ResourceCheck as ProtoResourceCheck, ToolDefinition as ProtoToolDefinition, ToolError,
    ToolResult, execute_tool_message,
};
use crate::redaction::ResourceAuthorization;
use crate::tools::{ToolExecutor, ToolRegistry};

use super::auth::extract_claims;

/// gRPC service implementation
pub struct KnowledgeGraphServiceImpl {
    validator: Arc<JwtValidator>,
    executor: ToolExecutor,
    context_engine: ContextEngine,
}

impl KnowledgeGraphServiceImpl {
    pub fn new(validator: Arc<JwtValidator>) -> Self {
        Self {
            validator,
            executor: ToolExecutor::new(),
            context_engine: ContextEngine::new(),
        }
    }
}

type ExecuteToolStream =
    Pin<Box<dyn futures::Stream<Item = Result<ExecuteToolMessage, Status>> + Send>>;

#[tonic::async_trait]
impl crate::proto::knowledge_graph_service_server::KnowledgeGraphService
    for KnowledgeGraphServiceImpl
{
    #[instrument(skip(self, request), fields(user_id))]
    async fn list_tools(
        &self,
        request: Request<ListToolsRequest>,
    ) -> Result<Response<ListToolsResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        info!("Listing tools for user");

        let tools = ToolRegistry::get_all_tools()
            .into_iter()
            .map(|t| ProtoToolDefinition {
                name: t.name,
                description: t.description,
                parameters_json_schema: t.parameters.to_string(),
            })
            .collect();

        Ok(Response::new(ListToolsResponse { tools }))
    }

    type ExecuteToolStream = ExecuteToolStream;

    #[instrument(skip(self, request), fields(user_id))]
    async fn execute_tool(
        &self,
        request: Request<Streaming<ExecuteToolMessage>>,
    ) -> Result<Response<Self::ExecuteToolStream>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);

        let executor = self.executor.clone();
        let context_engine = self.context_engine.clone();

        tokio::spawn(async move {
            // 1. Wait for initial ExecuteToolRequest
            let first_msg = match stream.next().await {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => {
                    error!(error = %e, "Failed to receive initial message");
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                None => {
                    warn!("Empty stream received");
                    let _ = tx.send(Err(Status::invalid_argument("Empty stream"))).await;
                    return;
                }
            };

            let req = match first_msg.message {
                Some(execute_tool_message::Message::Request(r)) => r,
                _ => {
                    warn!("Expected ExecuteToolRequest as first message");
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "Expected ExecuteToolRequest as first message",
                        )))
                        .await;
                    return;
                }
            };

            info!(tool_name = %req.tool_name, "Executing tool");

            // 2. Execute the tool
            let execution_result =
                match executor.execute(&req.tool_name, &req.arguments_json, &claims) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(error = %e, "Tool execution failed");
                        let _ = tx
                            .send(Ok(ExecuteToolMessage {
                                message: Some(execute_tool_message::Message::Error(ToolError {
                                    code: e.code(),
                                    message: e.to_string(),
                                })),
                            }))
                            .await;
                        return;
                    }
                };

            // 3. If no redaction needed, return result directly
            if execution_result.resources_to_check.is_empty() {
                info!("No redaction required, returning result directly");
                let final_result = context_engine.prepare_response(execution_result.raw_result);
                let _ = tx
                    .send(Ok(ExecuteToolMessage {
                        message: Some(execute_tool_message::Message::Result(ToolResult {
                            result_json: final_result.to_string(),
                        })),
                    }))
                    .await;
                return;
            }

            // 4. Send RedactionRequired to client
            let result_id = Uuid::new_v4().to_string();
            let resources: Vec<ProtoResourceCheck> = execution_result
                .resources_to_check
                .iter()
                .map(|r| ProtoResourceCheck {
                    resource_type: r.resource_type.clone(),
                    ids: r.ids.clone(),
                })
                .collect();

            info!(
                result_id = %result_id,
                resource_count = resources.len(),
                "Requesting redaction authorization"
            );

            let _ = tx
                .send(Ok(ExecuteToolMessage {
                    message: Some(execute_tool_message::Message::RedactionRequired(
                        RedactionRequired {
                            result_id: result_id.clone(),
                            resources,
                        },
                    )),
                }))
                .await;

            // 5. Wait for RedactionResponse from client
            let redaction_msg = match stream.next().await {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => {
                    error!(error = %e, "Failed to receive redaction response");
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                None => {
                    warn!("Client closed stream without sending redaction response");
                    let _ = tx
                        .send(Err(Status::cancelled(
                            "Client closed stream without sending redaction response",
                        )))
                        .await;
                    return;
                }
            };

            let redaction_response = match redaction_msg.message {
                Some(execute_tool_message::Message::RedactionResponse(r)) => r,
                Some(execute_tool_message::Message::Error(e)) => {
                    warn!(code = %e.code, message = %e.message, "Client sent error");
                    let _ = tx
                        .send(Ok(ExecuteToolMessage {
                            message: Some(execute_tool_message::Message::Error(e)),
                        }))
                        .await;
                    return;
                }
                _ => {
                    warn!("Expected RedactionResponse");
                    let _ = tx
                        .send(Err(Status::invalid_argument("Expected RedactionResponse")))
                        .await;
                    return;
                }
            };

            // Validate result_id matches
            if redaction_response.result_id != result_id {
                warn!(
                    expected = %result_id,
                    received = %redaction_response.result_id,
                    "result_id mismatch"
                );
                let _ = tx
                    .send(Err(Status::invalid_argument(
                        "result_id mismatch in redaction response",
                    )))
                    .await;
                return;
            }

            // 6. Convert proto authorizations to domain type
            let authorizations: Vec<ResourceAuthorization> = redaction_response
                .authorizations
                .into_iter()
                .map(|a| ResourceAuthorization {
                    resource_type: a.resource_type,
                    authorized: a.authorized,
                })
                .collect();

            // 7. Apply redaction and context engineering
            let final_result = context_engine
                .apply_redaction_and_prepare(execution_result.raw_result, &authorizations);

            info!("Sending final redacted result");

            // 8. Send final result
            let _ = tx
                .send(Ok(ExecuteToolMessage {
                    message: Some(execute_tool_message::Message::Result(ToolResult {
                        result_json: final_result.to_string(),
                    })),
                }))
                .await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_validator() -> JwtValidator {
        // Secret must be at least 32 bytes
        JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap()
    }

    #[test]
    fn test_service_can_be_created() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator);

        // Verify internal state
        assert!(
            service
                .executor
                .execute("get_graph_schema", "{}", &test_claims())
                .is_ok()
        );
    }

    fn test_claims() -> crate::auth::Claims {
        crate::auth::Claims {
            sub: "user:1".to_string(),
            iss: "gitlab".to_string(),
            aud: "gitlab-knowledge-graph".to_string(),
            exp: i64::MAX,
            iat: 0,
            user_id: 1,
            username: "testuser".to_string(),
            admin: false,
            organization_id: None,
            min_access_level: None,
            group_traversal_ids: vec![],
        }
    }
}
