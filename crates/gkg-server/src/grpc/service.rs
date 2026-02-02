use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use clickhouse_client::ClickHouseConfiguration;
use labkit_rs::correlation::grpc::{
    context_from_request, with_correlation, with_correlation_stream,
};
use labkit_rs::metrics::grpc::GrpcMetrics;
use mailbox::storage::PluginStore;
use mailbox::types::{PluginInfo, PropertyType};
use ontology::Ontology;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, instrument};

use crate::auth::JwtValidator;
use crate::cluster_health::ClusterHealthChecker;
use crate::proto::{
    DomainDefinition, EdgeDefinition, EdgeVariant, ExecuteQueryMessage, ExecuteToolMessage,
    GetClusterHealthRequest, GetClusterHealthResponse, GetNamespaceOntologyRequest,
    GetOntologyRequest, GetOntologyResponse, ListToolsRequest, ListToolsResponse, NodeDefinition,
    NodeStyle as ProtoNodeStyle, PropertyDefinition, QueryResult,
    ToolDefinition as ProtoToolDefinition, ToolResult, execute_query_message,
    execute_tool_message,
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
    plugin_store: Arc<PluginStore>,
    tool_service: ToolService,
    query_pipeline: QueryPipelineService<RawRowFormatter>,
    tool_pipeline: QueryPipelineService<ContextEngineFormatter>,
    cluster_health: Arc<ClusterHealthChecker>,
}

impl KnowledgeGraphServiceImpl {
    pub async fn new(
        validator: Arc<JwtValidator>,
        clickhouse_config: &ClickHouseConfiguration,
        health_check_url: Option<String>,
        plugin_store: Arc<PluginStore>,
    ) -> Self {
        let mut ontology = Ontology::load_embedded().expect("Failed to load ontology");

        if let Ok(plugins) = plugin_store.list_all().await {
            for plugin in plugins {
                Self::merge_plugin_schema(&mut ontology, &plugin);
            }
            info!(plugin_count = ontology.node_count(), "Loaded ontology with plugins");
        } else {
            info!("Loaded ontology without plugins (plugin store not available)");
        }

        let ontology = Arc::new(ontology);
        let client = Arc::new(clickhouse_config.build_client());
        let tool_service = ToolService::new(Arc::clone(&ontology));
        let query_pipeline =
            QueryPipelineService::new(Arc::clone(&ontology), Arc::clone(&client), RawRowFormatter)
                .with_redaction_disabled();
        let tool_pipeline =
            QueryPipelineService::new(Arc::clone(&ontology), client, ContextEngineFormatter)
                .with_redaction_disabled();
        let cluster_health = ClusterHealthChecker::new(health_check_url).into_arc();
        Self {
            validator,
            ontology,
            plugin_store,
            tool_service,
            query_pipeline,
            tool_pipeline,
            cluster_health,
        }
    }

    fn merge_plugin_schema(ontology: &mut Ontology, plugin: &PluginInfo) {
        for node in &plugin.schema.nodes {
            let fields: Vec<(String, ontology::DataType, bool)> = node
                .properties
                .iter()
                .map(|prop| {
                    let data_type = match prop.property_type {
                        PropertyType::String => ontology::DataType::String,
                        PropertyType::Int64 => ontology::DataType::Int,
                        PropertyType::Float => ontology::DataType::Float,
                        PropertyType::Boolean => ontology::DataType::Bool,
                        PropertyType::Date => ontology::DataType::Date,
                        PropertyType::Timestamp => ontology::DataType::DateTime,
                        PropertyType::Enum => ontology::DataType::Enum,
                    };
                    (prop.name.clone(), data_type, prop.nullable)
                })
                .collect();

            ontology.add_plugin_node(&plugin.plugin_id, &node.name, fields);
        }

        for edge in &plugin.schema.edges {
            ontology.add_plugin_edge(
                &edge.relationship_kind,
                edge.from_node_kinds.clone(),
                edge.to_node_kinds.clone(),
            );
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

    #[instrument(skip(self, request), fields(user_id, namespace_id))]
    async fn get_namespace_ontology(
        &self,
        request: Request<GetNamespaceOntologyRequest>,
    ) -> Result<Response<GetOntologyResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);

        let namespace_id = request.get_ref().namespace_id;
        tracing::Span::current().record("namespace_id", namespace_id);

        METRICS
            .record(SERVICE_NAME, "GetNamespaceOntology", || {
                with_correlation(&request, async {
                    info!("Fetching namespace ontology");

                    let plugins = match self.plugin_store.list_by_namespace(namespace_id).await {
                        Ok(p) => p,
                        Err(e) => {
                            error!(error = %e, "failed to list plugins");
                            return Err(Status::internal("Failed to retrieve plugins"));
                        }
                    };

                    let response = self.build_namespaced_ontology_response(plugins);
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
                    plugin_id: None,
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

    fn build_namespaced_ontology_response(&self, plugins: Vec<PluginInfo>) -> GetOntologyResponse {
        let mut base = self.build_ontology_response();

        let (plugin_nodes, plugin_edges) = Self::convert_plugins(&plugins);

        if !plugin_nodes.is_empty() {
            let plugin_node_names: Vec<String> =
                plugin_nodes.iter().map(|n| n.name.clone()).collect();

            base.domains.push(DomainDefinition {
                name: PLUGIN_DOMAIN.to_string(),
                description: PLUGIN_DOMAIN_DESCRIPTION.to_string(),
                node_names: plugin_node_names,
            });

            base.nodes.extend(plugin_nodes);
        }

        base.edges.extend(plugin_edges);
        base
    }

    fn convert_plugins(plugins: &[PluginInfo]) -> (Vec<NodeDefinition>, Vec<EdgeDefinition>) {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        for plugin in plugins {
            for node in &plugin.schema.nodes {
                let properties: Vec<PropertyDefinition> = node
                    .properties
                    .iter()
                    .map(|prop| PropertyDefinition {
                        name: prop.name.clone(),
                        data_type: property_type_to_data_type(prop.property_type),
                        nullable: prop.nullable,
                        enum_values: prop.enum_values.clone().unwrap_or_default(),
                    })
                    .collect();

                nodes.push(NodeDefinition {
                    name: node.name.clone(),
                    domain: PLUGIN_DOMAIN.to_string(),
                    description: String::new(),
                    primary_key: "id".to_string(),
                    label_field: String::new(),
                    properties,
                    style: Some(ProtoNodeStyle {
                        size: DEFAULT_PLUGIN_NODE_SIZE,
                        color: DEFAULT_PLUGIN_NODE_COLOR.to_string(),
                    }),
                    plugin_id: Some(plugin.plugin_id.clone()),
                });
            }

            for edge in &plugin.schema.edges {
                let variants: Vec<EdgeVariant> = edge
                    .from_node_kinds
                    .iter()
                    .flat_map(|source| {
                        edge.to_node_kinds.iter().map(move |target| EdgeVariant {
                            source_type: source.clone(),
                            target_type: target.clone(),
                        })
                    })
                    .collect();

                edges.push(EdgeDefinition {
                    name: edge.relationship_kind.clone(),
                    description: String::new(),
                    variants,
                });
            }
        }

        (nodes, edges)
    }
}

const PLUGIN_DOMAIN: &str = "plugins";
const PLUGIN_DOMAIN_DESCRIPTION: &str = "Custom nodes defined by plugins";
const DEFAULT_PLUGIN_NODE_SIZE: i32 = 30;
const DEFAULT_PLUGIN_NODE_COLOR: &str = "#9333EA";

fn property_type_to_data_type(property_type: PropertyType) -> String {
    match property_type {
        PropertyType::String => "String".to_string(),
        PropertyType::Int64 => "Int".to_string(),
        PropertyType::Float => "Float".to_string(),
        PropertyType::Boolean => "Bool".to_string(),
        PropertyType::Date => "Date".to_string(),
        PropertyType::Timestamp => "DateTime".to_string(),
        PropertyType::Enum => "Enum".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mailbox::types::{EdgeDefinition, NodeDefinition, PluginSchema, PropertyDefinition};

    fn mock_validator() -> JwtValidator {
        JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap()
    }

    fn test_config() -> ClickHouseConfiguration {
        ClickHouseConfiguration::default()
    }

    fn test_plugin_store() -> Arc<PluginStore> {
        let config = test_config();
        Arc::new(PluginStore::new(Arc::new(config.build_client())))
    }

    #[tokio::test]
    async fn test_service_can_be_created() {
        let validator = Arc::new(mock_validator());
        let service =
            KnowledgeGraphServiceImpl::new(validator, &test_config(), None, test_plugin_store())
                .await;

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

    #[tokio::test]
    async fn test_build_ontology_response() {
        let validator = Arc::new(mock_validator());
        let service =
            KnowledgeGraphServiceImpl::new(validator, &test_config(), None, test_plugin_store())
                .await;

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
        assert!(user.plugin_id.is_none());
    }

    #[tokio::test]
    async fn test_build_namespaced_ontology_with_plugins() {
        let validator = Arc::new(mock_validator());
        let service =
            KnowledgeGraphServiceImpl::new(validator, &test_config(), None, test_plugin_store())
                .await;

        let plugin = PluginInfo {
            plugin_id: "security-scanner".to_string(),
            namespace_id: 42,
            schema: PluginSchema::new()
                .with_node(
                    NodeDefinition::new("security_scanner_Vulnerability")
                        .with_property(PropertyDefinition::new("score", PropertyType::Float))
                        .with_property(
                            PropertyDefinition::new("severity", PropertyType::Enum)
                                .with_enum_values(vec![
                                    "low".into(),
                                    "medium".into(),
                                    "high".into(),
                                ]),
                        ),
                )
                .with_edge(
                    EdgeDefinition::new("security_scanner_AFFECTS")
                        .from_kinds(vec!["security_scanner_Vulnerability".into()])
                        .to_kinds(vec!["Project".into()]),
                ),
            schema_version: 1,
            created_at: chrono::Utc::now(),
        };

        let response = service.build_namespaced_ontology_response(vec![plugin]);

        let plugin_node = response
            .nodes
            .iter()
            .find(|n| n.name == "security_scanner_Vulnerability");
        assert!(plugin_node.is_some());
        let node = plugin_node.unwrap();
        assert_eq!(node.domain, "plugins");
        assert_eq!(node.plugin_id.as_deref(), Some("security-scanner"));

        let plugins_domain = response.domains.iter().find(|d| d.name == "plugins");
        assert!(plugins_domain.is_some());
        assert!(plugins_domain
            .unwrap()
            .node_names
            .contains(&"security_scanner_Vulnerability".to_string()));

        let plugin_edge = response
            .edges
            .iter()
            .find(|e| e.name == "security_scanner_AFFECTS");
        assert!(plugin_edge.is_some());
    }

    #[tokio::test]
    async fn test_empty_plugins_returns_base_ontology() {
        let validator = Arc::new(mock_validator());
        let service =
            KnowledgeGraphServiceImpl::new(validator, &test_config(), None, test_plugin_store())
                .await;

        let base_response = service.build_ontology_response();
        let namespaced_response = service.build_namespaced_ontology_response(vec![]);

        assert_eq!(base_response.nodes.len(), namespaced_response.nodes.len());
        assert_eq!(base_response.edges.len(), namespaced_response.edges.len());
        assert_eq!(
            base_response.domains.len(),
            namespaced_response.domains.len()
        );
    }

    #[test]
    fn test_property_type_conversion() {
        assert_eq!(property_type_to_data_type(PropertyType::String), "String");
        assert_eq!(property_type_to_data_type(PropertyType::Int64), "Int");
        assert_eq!(property_type_to_data_type(PropertyType::Float), "Float");
        assert_eq!(property_type_to_data_type(PropertyType::Boolean), "Bool");
        assert_eq!(property_type_to_data_type(PropertyType::Date), "Date");
        assert_eq!(
            property_type_to_data_type(PropertyType::Timestamp),
            "DateTime"
        );
        assert_eq!(property_type_to_data_type(PropertyType::Enum), "Enum");
    }
}
