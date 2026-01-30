use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use futures::StreamExt;
use labkit_rs::correlation::grpc::{
    context_from_request, with_correlation, with_correlation_stream,
};
use labkit_rs::metrics::grpc::GrpcMetrics;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, instrument, warn};

use crate::auth::JwtValidator;
use crate::context_engine::ContextEngine;
use crate::proto::{
    Error as ProtoError, ExecuteQueryMessage, ExecuteToolMessage, ListToolsRequest,
    ListToolsResponse, QueryResult, ToolDefinition as ProtoToolDefinition, ToolResult,
    execute_query_message, execute_tool_message,
};
use crate::query::QueryExecutor;
use crate::redaction::{RedactionService, ResourceExtractor};
use crate::tools::{ToolRegistry, ToolService};

use super::auth::extract_claims;

const SERVICE_NAME: &str = "gkg.v1.KnowledgeGraphService";

static METRICS: LazyLock<GrpcMetrics> = LazyLock::new(GrpcMetrics::new);

pub struct KnowledgeGraphServiceImpl {
    validator: Arc<JwtValidator>,
    tool_service: ToolService,
    query_executor: QueryExecutor,
    context_engine: ContextEngine,
}

impl KnowledgeGraphServiceImpl {
    pub fn new(validator: Arc<JwtValidator>) -> Self {
        let context_engine = ContextEngine::new();
        Self {
            validator,
            tool_service: ToolService::new(),
            query_executor: QueryExecutor::new(),
            context_engine,
        }
    }
}

type ExecuteToolStream =
    Pin<Box<dyn futures::Stream<Item = Result<ExecuteToolMessage, Status>> + Send>>;

type ExecuteQueryStream =
    Pin<Box<dyn futures::Stream<Item = Result<ExecuteQueryMessage, Status>> + Send>>;

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

        METRICS
            .record(SERVICE_NAME, "ListTools", || {
                with_correlation(&request, async {
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
                })
            })
            .await
    }

    type ExecuteToolStream = ExecuteToolStream;

    #[instrument(skip(self, request), fields(user_id))]
    async fn execute_tool(
        &self,
        request: Request<Streaming<ExecuteToolMessage>>,
    ) -> Result<Response<Self::ExecuteToolStream>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        let context = context_from_request(&request);
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);

        let executor = self.tool_service.clone();
        let context_engine = self.context_engine.clone();

        tokio::spawn(async move {
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

            let execution_result =
                match executor.execute_tool(&req.tool_name, &req.arguments_json, &claims) {
                    Ok(r) => r,
                    Err(e) => {
                        error!(error = %e, "Tool execution failed");
                        let _ = tx
                            .send(Ok(ExecuteToolMessage {
                                message: Some(execute_tool_message::Message::Error(ProtoError {
                                    code: e.code(),
                                    message: e.to_string(),
                                })),
                            }))
                            .await;
                        return;
                    }
                };

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

            let exchange_result = match RedactionService::request_authorization(
                &execution_result.resources_to_check,
                &tx,
                &mut stream,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(e.into_status())).await;
                    return;
                }
            };

            let final_result = context_engine.apply_redaction_and_prepare(
                execution_result.raw_result,
                &exchange_result.authorizations,
            );

            info!("Sending final redacted result");

            let _ = tx
                .send(Ok(ExecuteToolMessage {
                    message: Some(execute_tool_message::Message::Result(ToolResult {
                        result_json: final_result.to_string(),
                    })),
                }))
                .await;
        });

        let stream = ReceiverStream::new(rx);
        let metered_stream = METRICS.record_stream(SERVICE_NAME, "ExecuteTool", stream);

        Ok(Response::new(Box::pin(with_correlation_stream(
            context,
            metered_stream,
        ))))
    }

    type ExecuteQueryStream = ExecuteQueryStream;

    #[instrument(skip(self, request), fields(user_id))]
    async fn execute_query(
        &self,
        request: Request<Streaming<ExecuteQueryMessage>>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        let context = context_from_request(&request);
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);

        let query_executor = self.query_executor.clone();
        let context_engine = self.context_engine.clone();

        tokio::spawn(async move {
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
                Some(execute_query_message::Message::Request(r)) => r,
                _ => {
                    warn!("Expected ExecuteQueryRequest as first message");
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "Expected ExecuteQueryRequest as first message",
                        )))
                        .await;
                    return;
                }
            };

            info!(query_len = req.query_json.len(), "Executing query");

            let query_result = match query_executor.execute(&req.query_json, &claims) {
                Ok(r) => r,
                Err(e) => {
                    error!(error = %e, "Query execution failed");
                    let _ = tx
                        .send(Ok(ExecuteQueryMessage {
                            message: Some(execute_query_message::Message::Error(ProtoError {
                                code: e.code(),
                                message: e.to_string(),
                            })),
                        }))
                        .await;
                    return;
                }
            };

            let resources_to_check = ResourceExtractor::extract(&query_result.result);

            if resources_to_check.is_empty() {
                info!("No redaction required, returning result directly");
                let final_result = context_engine.prepare_response(query_result.result);
                let _ = tx
                    .send(Ok(ExecuteQueryMessage {
                        message: Some(execute_query_message::Message::Result(QueryResult {
                            result_json: final_result.to_string(),
                            generated_sql: query_result.generated_sql,
                        })),
                    }))
                    .await;
                return;
            }

            let exchange_result = match RedactionService::request_authorization(
                &resources_to_check,
                &tx,
                &mut stream,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(e.into_status())).await;
                    return;
                }
            };

            let final_result = context_engine
                .apply_redaction_and_prepare(query_result.result, &exchange_result.authorizations);

            info!("Sending final filtered query result");

            let _ = tx
                .send(Ok(ExecuteQueryMessage {
                    message: Some(execute_query_message::Message::Result(QueryResult {
                        result_json: final_result.to_string(),
                        generated_sql: query_result.generated_sql,
                    })),
                }))
                .await;
        });

        let stream = ReceiverStream::new(rx);
        let metered_stream = METRICS.record_stream(SERVICE_NAME, "ExecuteQuery", stream);

        Ok(Response::new(Box::pin(with_correlation_stream(
            context,
            metered_stream,
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_validator() -> JwtValidator {
        JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap()
    }

    #[test]
    fn test_service_can_be_created() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator);
        assert!(
            service
                .tool_service
                .execute_tool("get_graph_entities", "{}", &test_claims())
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
