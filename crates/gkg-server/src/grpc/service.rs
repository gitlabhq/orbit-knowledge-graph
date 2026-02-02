use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use clickhouse_client::ClickHouseConfiguration;
use labkit_rs::correlation::grpc::{
    context_from_request, with_correlation, with_correlation_stream,
};
use labkit_rs::metrics::grpc::GrpcMetrics;
use ontology::Ontology;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, instrument};

use crate::auth::JwtValidator;
use crate::cluster_health::ClusterHealthChecker;
use crate::proto::{
    DomainDefinition, EdgeDefinition, EdgeVariant, ExecuteQueryMessage, ExecuteToolMessage,
    GetClusterHealthRequest, GetClusterHealthResponse, GetOntologyRequest, GetOntologyResponse,
    ListToolsRequest, ListToolsResponse, NodeDefinition, NodeStyle as ProtoNodeStyle,
    PropertyDefinition, QueryResult, ToolDefinition as ProtoToolDefinition, ToolResult,
    execute_query_message, execute_tool_message,
};
use crate::query_pipeline::{
    ContextEngineFormatter, QueryPipelineService, RawRowFormatter, receive_query_request,
    receive_tool_request, send_query_error, send_tool_executor_error, send_tool_pipeline_error,
};
use crate::tools::{ToolPlan, ToolRegistry, ToolService};

use super::auth::extract_claims;

const SERVICE_NAME: &str = "gkg.v1.KnowledgeGraphService";

static METRICS: LazyLock<GrpcMetrics> = LazyLock::new(GrpcMetrics::new);

pub struct KnowledgeGraphServiceImpl {
    validator: Arc<JwtValidator>,
    ontology: Arc<Ontology>,
    tool_service: ToolService,
    query_pipeline: QueryPipelineService<RawRowFormatter>,
    tool_pipeline: QueryPipelineService<ContextEngineFormatter>,
    cluster_health: Arc<ClusterHealthChecker>,
}

impl KnowledgeGraphServiceImpl {
    pub fn new(
        validator: Arc<JwtValidator>,
        clickhouse_config: &ClickHouseConfiguration,
        health_check_url: Option<String>,
    ) -> Self {
        let ontology = Arc::new(Ontology::load_embedded().expect("Failed to load ontology"));
        let client = Arc::new(clickhouse_config.build_client());
        let tool_service = ToolService::new(Arc::clone(&ontology));
        let query_pipeline =
            QueryPipelineService::new(Arc::clone(&ontology), Arc::clone(&client), RawRowFormatter);
        let tool_pipeline =
            QueryPipelineService::new(Arc::clone(&ontology), client, ContextEngineFormatter);
        let cluster_health = ClusterHealthChecker::new(health_check_url).into_arc();
        Self {
            validator,
            ontology,
            tool_service,
            query_pipeline,
            tool_pipeline,
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

                    let tools = ToolRegistry::get_all_tools(&self.ontology)
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

        let tool_service = self.tool_service.clone();
        let tool_query_pipeline = self.tool_pipeline.clone();

        tokio::spawn(async move {
            let req = match receive_tool_request(&mut stream, &tx).await {
                Some(r) => r,
                None => return,
            };

            info!(tool_name = %req.tool_name, "Executing tool");

            let plan = match tool_service.resolve(&req.tool_name, &req.arguments_json) {
                Ok(p) => p,
                Err(e) => {
                    send_tool_executor_error(&tx, e).await;
                    return;
                }
            };

            match plan {
                ToolPlan::RunGraphQuery { query_json } => {
                    let result = tool_query_pipeline
                        .run_query(&claims, &query_json, &tx, &mut stream)
                        .await;

                    match result {
                        Ok(output) => {
                            info!("Sending graph query result");
                            let _ = tx
                                .send(Ok(ExecuteToolMessage {
                                    message: Some(execute_tool_message::Message::Result(
                                        ToolResult {
                                            result_json: output.formatted_result.to_string(),
                                        },
                                    )),
                                }))
                                .await;
                        }
                        Err(e) => {
                            send_tool_pipeline_error(&tx, e).await;
                        }
                    }
                }
                ToolPlan::Immediate { result } => {
                    info!("Sending immediate tool result");
                    let _ = tx
                        .send(Ok(ExecuteToolMessage {
                            message: Some(execute_tool_message::Message::Result(ToolResult {
                                result_json: result.to_string(),
                            })),
                        }))
                        .await;
                }
            }
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

        let raw_query_pipeline = self.query_pipeline.clone();

        tokio::spawn(async move {
            let req = match receive_query_request(&mut stream, &tx).await {
                Some(r) => r,
                None => return,
            };

            info!(query_len = req.query_json.len(), "Executing query");

            let result = raw_query_pipeline
                .run_query(&claims, &req.query_json, &tx, &mut stream)
                .await;

            match result {
                Ok(output) => {
                    info!("Sending final query result");
                    let _ = tx
                        .send(Ok(ExecuteQueryMessage {
                            message: Some(execute_query_message::Message::Result(QueryResult {
                                result_json: output.formatted_result.to_string(),
                                generated_sql: output.generated_sql.unwrap_or_default(),
                            })),
                        }))
                        .await;
                }
                Err(e) => {
                    send_query_error(&tx, e).await;
                }
            }
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

    #[test]
    fn test_service_can_be_created() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let plan = service
            .tool_service
            .resolve("get_graph_entities", "{}")
            .expect("Should resolve");

        match plan {
            ToolPlan::Immediate { result } => {
                assert!(result.is_string(), "Response should be toon-encoded string");
                let toon_str = result.as_str().unwrap();
                assert!(toon_str.contains("domains"));
                assert!(toon_str.contains("edges"));
            }
            _ => panic!("Expected Immediate plan"),
        }
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
}
