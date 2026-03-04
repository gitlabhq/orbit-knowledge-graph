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
    ExecuteQueryMessage, ExecuteQueryResult, GetClusterHealthRequest, GetClusterHealthResponse,
    GetGraphSchemaRequest, GetGraphSchemaResponse, ListToolsRequest, ListToolsResponse,
    ResponseFormat, SchemaDomain, SchemaEdge, SchemaEdgeVariant, SchemaNode, SchemaNodeStyle,
    SchemaProperty, StructuredSchema, ToolDefinition as ProtoToolDefinition, execute_query_message,
    get_graph_schema_response,
};
use crate::query_pipeline::{
    ContextEngineFormatter, QueryPipelineService, RawRowFormatter, receive_query_request,
    send_query_error,
};
use crate::tools::{ToolRegistry, ToolService};

use super::auth::extract_claims;

const SERVICE_NAME: &str = "gkg.v1.KnowledgeGraphService";

static METRICS: LazyLock<GrpcMetrics> = LazyLock::new(GrpcMetrics::new);

pub struct KnowledgeGraphServiceImpl {
    validator: Arc<JwtValidator>,
    ontology: Arc<Ontology>,
    tool_service: ToolService,
    query_pipeline: QueryPipelineService<RawRowFormatter>,
    llm_pipeline: QueryPipelineService<ContextEngineFormatter>,
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
        let llm_pipeline =
            QueryPipelineService::new(Arc::clone(&ontology), client, ContextEngineFormatter);
        let cluster_health = ClusterHealthChecker::new(health_check_url).into_arc();
        Self {
            validator,
            ontology,
            tool_service,
            query_pipeline,
            llm_pipeline,
            cluster_health,
        }
    }
}

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

        let raw_pipeline = self.query_pipeline.clone();
        let llm_pipeline = self.llm_pipeline.clone();

        tokio::spawn(async move {
            let req = match receive_query_request(&mut stream, &tx).await {
                Some(r) => r,
                None => return,
            };

            info!(query_len = req.query.len(), "Executing query");

            let use_llm_format = req.format == ResponseFormat::Llm as i32;

            let result = if use_llm_format {
                llm_pipeline
                    .run_query(&claims, &req.query, &tx, &mut stream)
                    .await
            } else {
                raw_pipeline
                    .run_query(&claims, &req.query, &tx, &mut stream)
                    .await
            };

            match result {
                Ok(output) => {
                    info!("Sending final query result");
                    let _ = tx
                        .send(Ok(ExecuteQueryMessage {
                            content: Some(execute_query_message::Content::Result(
                                ExecuteQueryResult {
                                    result_json: output.formatted_result.to_string(),
                                    generated_sql: output.generated_sql.unwrap_or_default(),
                                    row_count: i32::try_from(output.row_count).unwrap_or(i32::MAX),
                                    redacted_count: i32::try_from(output.redacted_count)
                                        .unwrap_or(i32::MAX),
                                    execution_time_ms: output.execution_time_ms,
                                },
                            )),
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
    async fn get_graph_schema(
        &self,
        request: Request<GetGraphSchemaRequest>,
    ) -> Result<Response<GetGraphSchemaResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        METRICS
            .record(SERVICE_NAME, "GetGraphSchema", || {
                with_correlation(&request, async {
                    let req = request.get_ref();
                    info!(format = ?req.format, "Fetching graph schema for user");

                    let response = if req.format == ResponseFormat::Llm as i32 {
                        let toon_text = self
                            .tool_service
                            .build_schema_toon(&req.expand_nodes)
                            .map_err(|e| Status::internal(e.to_string()))?;
                        GetGraphSchemaResponse {
                            content: Some(get_graph_schema_response::Content::FormattedText(
                                toon_text,
                            )),
                        }
                    } else {
                        let structured = self.build_structured_schema(&req.expand_nodes);
                        GetGraphSchemaResponse {
                            content: Some(get_graph_schema_response::Content::Structured(
                                structured,
                            )),
                        }
                    };

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
                    let req = request.get_ref();
                    info!(format = ?req.format, "Fetching cluster health for user");

                    let response = self.cluster_health.get_cluster_health(req.format).await;
                    Ok(Response::new(response))
                })
            })
            .await
    }
}

impl KnowledgeGraphServiceImpl {
    fn build_structured_schema(&self, expand_nodes: &[String]) -> StructuredSchema {
        let domains: Vec<SchemaDomain> = self
            .ontology
            .domains()
            .map(|d| SchemaDomain {
                name: d.name.clone(),
                description: d.description.clone(),
                node_names: d.node_names.clone(),
            })
            .collect();

        let nodes: Vec<SchemaNode> = self
            .ontology
            .nodes()
            .map(|n| {
                let should_expand = expand_nodes.iter().any(|e| e == &n.name);

                let properties = if should_expand {
                    n.fields
                        .iter()
                        .map(|f| SchemaProperty {
                            name: f.name.clone(),
                            data_type: format!("{}", f.data_type),
                            nullable: f.nullable,
                            enum_values: f
                                .enum_values
                                .as_ref()
                                .map(|ev| ev.values().cloned().collect())
                                .unwrap_or_default(),
                        })
                        .collect()
                } else {
                    vec![]
                };

                let style = if should_expand {
                    Some(SchemaNodeStyle {
                        size: n.style.size,
                        color: n.style.color.clone(),
                    })
                } else {
                    None
                };

                let (outgoing_edges, incoming_edges) = if should_expand {
                    self.get_node_edge_names(&n.name)
                } else {
                    (vec![], vec![])
                };

                SchemaNode {
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
                    style,
                    outgoing_edges,
                    incoming_edges,
                }
            })
            .collect();

        let edges: Vec<SchemaEdge> = self
            .ontology
            .edge_names()
            .map(|name| {
                let variants: Vec<SchemaEdgeVariant> = self
                    .ontology
                    .get_edge(name)
                    .map(|edges| {
                        edges
                            .iter()
                            .map(|e| SchemaEdgeVariant {
                                source_type: e.source_kind.clone(),
                                target_type: e.target_kind.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                SchemaEdge {
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

        StructuredSchema {
            schema_version: self.ontology.schema_version().to_string(),
            domains,
            nodes,
            edges,
        }
    }

    fn get_node_edge_names(&self, node_name: &str) -> (Vec<String>, Vec<String>) {
        let mut outgoing = Vec::new();
        let mut incoming = Vec::new();

        for edge_name in self.ontology.edge_names() {
            if let Some(edges) = self.ontology.get_edge(edge_name) {
                let mut has_outgoing = false;
                let mut has_incoming = false;

                for edge in edges {
                    if edge.source_kind == node_name {
                        has_outgoing = true;
                    }
                    if edge.target_kind == node_name {
                        has_incoming = true;
                    }
                }

                if has_outgoing {
                    outgoing.push(edge_name.to_string());
                }
                if has_incoming {
                    incoming.push(edge_name.to_string());
                }
            }
        }

        outgoing.sort();
        incoming.sort();

        (outgoing, incoming)
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
            .resolve("get_graph_schema", "{}")
            .expect("Should resolve");

        match plan {
            crate::tools::ToolPlan::Immediate { result } => {
                assert!(result.is_string(), "Response should be toon-encoded string");
                let toon_str = result.as_str().unwrap();
                assert!(toon_str.contains("domains"));
                assert!(toon_str.contains("edges"));
            }
            _ => panic!("Expected Immediate plan"),
        }
    }

    #[test]
    fn test_build_structured_schema() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let response = service.build_structured_schema(&[]);

        assert!(!response.schema_version.is_empty());
        assert!(!response.nodes.is_empty());
        assert!(!response.edges.is_empty());
        assert!(!response.domains.is_empty());

        let user_node = response.nodes.iter().find(|n| n.name == "User");
        assert!(user_node.is_some());
        let user = user_node.unwrap();
        assert_eq!(user.domain, "core");
        assert!(
            user.properties.is_empty(),
            "Unexpanded node should have no properties"
        );
        assert!(user.style.is_none(), "Unexpanded node should have no style");
    }

    #[test]
    fn test_build_structured_schema_with_expand() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let response = service.build_structured_schema(&["User".to_string()]);

        let user_node = response.nodes.iter().find(|n| n.name == "User");
        assert!(user_node.is_some());
        let user = user_node.unwrap();
        assert!(
            !user.properties.is_empty(),
            "Expanded node should have properties"
        );
        assert!(user.style.is_some(), "Expanded node should have style");
        assert!(
            !user.outgoing_edges.is_empty() || !user.incoming_edges.is_empty(),
            "Expanded node should have edges"
        );

        let project_node = response.nodes.iter().find(|n| n.name == "Project");
        assert!(project_node.is_some());
        let project = project_node.unwrap();
        assert!(
            project.properties.is_empty(),
            "Unexpanded Project should have no properties"
        );
    }

    #[test]
    fn test_get_node_edge_names_returns_sorted() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let (outgoing, incoming) = service.get_node_edge_names("User");

        assert!(
            !outgoing.is_empty() || !incoming.is_empty(),
            "User should have at least one edge"
        );

        let is_sorted = |v: &[String]| v.windows(2).all(|w| w[0] <= w[1]);
        assert!(is_sorted(&outgoing), "Outgoing edges should be sorted");
        assert!(is_sorted(&incoming), "Incoming edges should be sorted");
    }

    #[test]
    fn test_get_node_edge_names_unknown_node_returns_empty() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let (outgoing, incoming) = service.get_node_edge_names("NonexistentNode");

        assert!(outgoing.is_empty());
        assert!(incoming.is_empty());
    }

    #[test]
    fn test_expanded_node_has_property_details() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let response = service.build_structured_schema(&["User".to_string()]);
        let user = response.nodes.iter().find(|n| n.name == "User").unwrap();

        let id_prop = user.properties.iter().find(|p| p.name == "id");
        assert!(id_prop.is_some(), "User should have an id property");
        assert!(
            !id_prop.unwrap().data_type.is_empty(),
            "Property should have a data type"
        );

        let username_prop = user.properties.iter().find(|p| p.name == "username");
        assert!(
            username_prop.is_some(),
            "User should have a username property"
        );
    }

    #[test]
    fn test_structured_schema_domains_have_nodes() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let response = service.build_structured_schema(&[]);

        for domain in &response.domains {
            assert!(!domain.name.is_empty(), "Domain should have a name");
            assert!(
                !domain.node_names.is_empty(),
                "Domain {} should have nodes",
                domain.name
            );
        }
    }

    #[test]
    fn test_structured_schema_edges_have_variants() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let response = service.build_structured_schema(&[]);

        for edge in &response.edges {
            assert!(!edge.name.is_empty(), "Edge should have a name");
            assert!(
                !edge.variants.is_empty(),
                "Edge {} should have variants",
                edge.name
            );
            for variant in &edge.variants {
                assert!(!variant.source_type.is_empty());
                assert!(!variant.target_type.is_empty());
            }
        }
    }

    #[test]
    fn test_expand_multiple_nodes() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(validator, &test_config(), None);

        let response =
            service.build_structured_schema(&["User".to_string(), "Project".to_string()]);

        let user = response.nodes.iter().find(|n| n.name == "User").unwrap();
        let project = response.nodes.iter().find(|n| n.name == "Project").unwrap();

        assert!(!user.properties.is_empty(), "User should be expanded");
        assert!(!project.properties.is_empty(), "Project should be expanded");

        let mr = response
            .nodes
            .iter()
            .find(|n| n.name == "MergeRequest")
            .unwrap();
        assert!(
            mr.properties.is_empty(),
            "MergeRequest should not be expanded"
        );
    }
}
