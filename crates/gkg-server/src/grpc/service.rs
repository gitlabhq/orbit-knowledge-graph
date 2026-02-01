use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use clickhouse_client::ClickHouseConfiguration;
use futures::StreamExt;
use labkit_rs::correlation::grpc::{
    context_from_request, with_correlation, with_correlation_stream,
};
use labkit_rs::metrics::grpc::GrpcMetrics;
use ontology::Ontology;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, instrument, warn};

use crate::auth::JwtValidator;
use crate::cluster_health::ClusterHealthChecker;
use crate::context_engine::ContextEngine;
use crate::proto::{
    DomainDefinition, EdgeDefinition, EdgeVariant, Error as ProtoError, ExecuteQueryMessage,
    ExecuteToolMessage, GetClusterHealthRequest, GetClusterHealthResponse, GetOntologyRequest,
    GetOntologyResponse, ListToolsRequest, ListToolsResponse, NodeDefinition,
    NodeStyle as ProtoNodeStyle, PropertyDefinition, QueryResult,
    ToolDefinition as ProtoToolDefinition, ToolResult, execute_query_message, execute_tool_message,
};
use crate::query::QueryExecutor;
use crate::redaction::RedactionService;
use crate::tools::{ToolRegistry, ToolService};

use super::auth::extract_claims;

const SERVICE_NAME: &str = "gkg.v1.KnowledgeGraphService";

static METRICS: LazyLock<GrpcMetrics> = LazyLock::new(GrpcMetrics::new);

pub struct KnowledgeGraphServiceImpl {
    validator: Arc<JwtValidator>,
    ontology: Arc<Ontology>,
    tool_service: ToolService,
    query_executor: QueryExecutor,
    context_engine: ContextEngine,
    cluster_health: Arc<ClusterHealthChecker>,
}

impl KnowledgeGraphServiceImpl {
    pub fn new(
        validator: Arc<JwtValidator>,
        clickhouse_config: &ClickHouseConfiguration,
        health_check_url: Option<String>,
    ) -> Self {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let query_executor = QueryExecutor::new(clickhouse_config, Arc::clone(&ontology));
        let tool_service = ToolService::new(query_executor.clone(), Arc::clone(&ontology));
        let context_engine = ContextEngine::new();
        let cluster_health = ClusterHealthChecker::new(health_check_url).into_arc();
        Self {
            validator,
            ontology,
            tool_service,
            query_executor,
            context_engine,
            cluster_health,
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
        let tool_ontology = Arc::clone(&self.ontology);

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

            let execution_result = match executor
                .execute_tool(&req.tool_name, &req.arguments_json, &claims)
                .await
            {
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

            let final_result = match (
                execution_result.redaction_result,
                execution_result.result_context,
            ) {
                (Some(mut redaction_result), Some(result_context)) => {
                    if execution_result.resources_to_check.is_empty() {
                        info!("No redaction required, returning result directly");
                        context_engine.apply_redaction_and_prepare(
                            &mut redaction_result,
                            &result_context,
                            &[],
                            &tool_ontology,
                        )
                    } else {
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
                        context_engine.apply_redaction_and_prepare(
                            &mut redaction_result,
                            &result_context,
                            &exchange_result.authorizations,
                            &tool_ontology,
                        )
                    }
                }
                _ => {
                    info!("No redaction required for this tool, returning raw result");
                    context_engine.prepare_response(execution_result.raw_result)
                }
            };

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
        let ontology = Arc::clone(&self.ontology);

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

            let query_result = match query_executor.execute(&req.query_json, &claims).await {
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

            let mut redaction_result = query_result.redaction_result;
            let result_context = query_result.result_context;

            if query_result.resources_to_check.is_empty() {
                info!("No redaction required, returning result directly");
                let final_result = context_engine.apply_redaction_and_prepare(
                    &mut redaction_result,
                    &result_context,
                    &[],
                    &ontology,
                );
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
                &query_result.resources_to_check,
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
                &mut redaction_result,
                &result_context,
                &exchange_result.authorizations,
                &ontology,
            );

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

    #[instrument(skip(self, request), fields(user_id))]
    async fn get_ontology(
        &self,
        request: Request<GetOntologyRequest>,
    ) -> Result<Response<GetOntologyResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        METRICS
            .record(SERVICE_NAME, "GetOntology", || {
                with_correlation(&request, async {
                    info!("Fetching ontology for user");

                    let response = self.build_ontology_response();
                    Ok(Response::new(response))
                })
            })
            .await
    }

    #[instrument(skip(self, request), fields(user_id))]
    async fn get_cluster_health(
        &self,
        request: Request<GetClusterHealthRequest>,
    ) -> Result<Response<GetClusterHealthResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        METRICS
            .record(SERVICE_NAME, "GetClusterHealth", || {
                with_correlation(&request, async {
                    info!("Fetching cluster health for user");

                    let response = self.cluster_health.get_cluster_health().await;
                    Ok(Response::new(response))
                })
            })
            .await
    }
}

impl KnowledgeGraphServiceImpl {
    fn build_ontology_response(&self) -> GetOntologyResponse {
        let domains: Vec<DomainDefinition> = self
            .ontology
            .domains()
            .map(|d| DomainDefinition {
                name: d.name.clone(),
                description: d.description.clone(),
                node_names: d.node_names.clone(),
            })
            .collect();

        let nodes: Vec<NodeDefinition> = self
            .ontology
            .nodes()
            .map(|n| {
                let properties: Vec<PropertyDefinition> = n
                    .fields
                    .iter()
                    .map(|f| PropertyDefinition {
                        name: f.name.clone(),
                        data_type: format!("{}", f.data_type),
                        nullable: f.nullable,
                        enum_values: f
                            .enum_values
                            .as_ref()
                            .map(|ev| ev.values().cloned().collect())
                            .unwrap_or_default(),
                    })
                    .collect();

                NodeDefinition {
                    name: n.name.clone(),
                    domain: n.domain.clone(),
                    description: n.description.clone(),
                    primary_key: n
                        .primary_keys
                        .first()
                        .cloned()
                        .unwrap_or_else(|| "id".to_string()),
                    label_field: n.label.clone(),
                    properties,
                    style: Some(ProtoNodeStyle {
                        size: n.style.size,
                        color: n.style.color.clone(),
                    }),
                }
            })
            .collect();

        let edges: Vec<EdgeDefinition> = self
            .ontology
            .edge_names()
            .map(|name| {
                let variants: Vec<EdgeVariant> = self
                    .ontology
                    .get_edge(name)
                    .map(|edges| {
                        edges
                            .iter()
                            .map(|e| EdgeVariant {
                                source_type: e.source_kind.clone(),
                                target_type: e.target_kind.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                EdgeDefinition {
                    name: name.to_string(),
                    description: self
                        .ontology
                        .get_edge_description(name)
                        .unwrap_or_default()
                        .to_string(),
                    variants,
                }
            })
            .collect();

        GetOntologyResponse {
            schema_version: self.ontology.schema_version().to_string(),
            nodes,
            edges,
            domains,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_validator() -> JwtValidator {
        JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap()
    }

    fn test_config() -> ClickHouseConfiguration {
        ClickHouseConfiguration::default()
    }

    #[tokio::test]
    async fn test_service_can_be_created() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);
        let result = service
            .tool_service
            .execute_tool("get_graph_entities", "{}", &test_claims())
            .await;
        assert!(result.is_ok());

        let response = result.unwrap().raw_result;
        assert!(
            response.is_string(),
            "Response should be toon-encoded string"
        );
        let toon_str = response.as_str().unwrap();
        assert!(toon_str.contains("domains"));
        assert!(toon_str.contains("edges"));
    }

    #[test]
    fn test_build_ontology_response() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let response = service.build_ontology_response();

        assert!(!response.schema_version.is_empty());
        assert!(!response.nodes.is_empty());
        assert!(!response.edges.is_empty());
        assert!(!response.domains.is_empty());

        let user_node = response.nodes.iter().find(|n| n.name == "User");
        assert!(user_node.is_some());
        let user = user_node.unwrap();
        assert_eq!(user.domain, "core");
        assert!(!user.properties.is_empty());
        assert!(user.style.is_some());
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
