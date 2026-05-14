use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use gkg_server_config::{AnalyticsConfig, ClickHouseConfiguration};
use ontology::Ontology;
use query_engine::pipeline::PipelineError;
use query_engine::shared::content::ColumnResolverRegistry;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{Instrument, info, instrument};

use super::auth::extract_request_context;
use crate::analytics::AnalyticsTracker;
use crate::auth::{Claims, JwtValidator, build_security_context};
use crate::cluster_health::ClusterHealthChecker;
use crate::graph_status::GraphStatusService;
use crate::pipeline::{QueryPipelineService, receive_query_request, send_query_error};
use crate::proto::{
    ExecuteQueryMessage, ExecuteQueryResult, FormatName as ProtoFormatName,
    GetClusterHealthRequest, GetClusterHealthResponse, GetGraphSchemaRequest,
    GetGraphSchemaResponse, GetGraphStatusRequest, GetGraphStatusResponse, GetQueryDslRequest,
    GetQueryDslResponse, GetResponseFormatRequest, GetResponseFormatResponse,
    InvokeAgentCommandRequest, InvokeAgentCommandResponse, ListAgentCommandsRequest,
    ListAgentCommandsResponse, ListToolsRequest, ListToolsResponse, QueryMetadata, ResponseFormat,
    ResponseFormatSchema, SchemaDomain, SchemaEdge, SchemaEdgeVariant, SchemaNode, SchemaNodeStyle,
    SchemaProperty, StructuredSchema, ToolDefinition as ProtoToolDefinition, execute_query_message,
    get_graph_schema_response, get_query_dsl_response, get_response_format_response,
    invoke_agent_command_response,
};
use crate::tools::{ExecutorError, ToolPlan, ToolService, V2CommandRegistry, V2ToolRegistry};
use gkg_billing::BillingTracker;
use query_engine::formatters::{FormatName, GoonFormatter, GraphFormatter, ResultFormatter};

fn proto_format_name(name: FormatName) -> ProtoFormatName {
    match name {
        FormatName::Raw => ProtoFormatName::Raw,
        FormatName::Goon => ProtoFormatName::Goon,
    }
}

fn record_ai_session_id(ai_session_id: &Option<String>) {
    if let Some(sid) = ai_session_id {
        tracing::Span::current().record("ai_session_id", sid.as_str());
    }
}

fn record_coding_agent(coding_agent: Option<&str>) {
    if let Some(agent) = coding_agent {
        tracing::Span::current().record("coding_agent", agent);
    }
}

fn proto_tool_definition(t: crate::tools::ToolDefinition) -> ProtoToolDefinition {
    ProtoToolDefinition {
        name: t.name,
        description: t.description,
        parameters_json_schema: t.parameters.to_string(),
    }
}

fn command_error_to_status(error: ExecutorError) -> Status {
    match error {
        ExecutorError::NotFound(_) => Status::not_found(error.to_string()),
        ExecutorError::InvalidArguments(_) => Status::invalid_argument(error.to_string()),
        ExecutorError::InterceptedCommand(_) => Status::failed_precondition(error.to_string()),
    }
}

pub struct KnowledgeGraphServiceImpl {
    validator: Arc<JwtValidator>,
    ontology: Arc<Ontology>,
    tool_service: ToolService,
    pipeline: QueryPipelineService,
    cluster_health: Arc<ClusterHealthChecker>,
    graph_status: GraphStatusService,
    stream_timeout_secs: u64,
}

impl KnowledgeGraphServiceImpl {
    pub fn new(
        validator: Arc<JwtValidator>,
        ontology: Arc<Ontology>,
        clickhouse_config: &ClickHouseConfiguration,
        cluster_health: Arc<ClusterHealthChecker>,
        stream_timeout_secs: u64,
        analytics_config: Arc<AnalyticsConfig>,
    ) -> Self {
        let client = Arc::new(clickhouse_config.build_client());
        let tool_service = ToolService::new(Arc::clone(&ontology));
        let pipeline = QueryPipelineService::new(
            Arc::clone(&ontology),
            Arc::clone(&client),
            clickhouse_config.profiling.clone(),
            analytics_config,
        );
        let graph_status = GraphStatusService::new(client, Arc::clone(&ontology));
        Self {
            validator,
            ontology,
            tool_service,
            pipeline,
            cluster_health,
            graph_status,
            stream_timeout_secs,
        }
    }

    pub fn with_resolver_registry(mut self, registry: Arc<ColumnResolverRegistry>) -> Self {
        self.pipeline = self.pipeline.with_resolver_registry(registry);
        self
    }

    pub fn with_cache_broker(mut self, broker: Arc<nats_client::NatsClient>) -> Self {
        self.pipeline = self.pipeline.with_cache_broker(broker);
        self
    }

    pub fn with_billing(mut self, tracker: Arc<dyn BillingTracker>) -> Self {
        self.pipeline = self.pipeline.with_billing(tracker);
        self
    }

    pub fn with_analytics(mut self, tracker: Arc<dyn AnalyticsTracker>) -> Self {
        self.pipeline = self.pipeline.with_analytics(tracker);
        self
    }

    pub fn with_indexing_status(
        mut self,
        store: indexer::indexing_status::IndexingStatusStore,
    ) -> Self {
        self.graph_status = self.graph_status.with_indexing_status(store);
        self
    }
}

type ExecuteQueryStream =
    Pin<Box<dyn futures::Stream<Item = Result<ExecuteQueryMessage, Status>> + Send>>;

#[tonic::async_trait]
impl crate::proto::knowledge_graph_service_server::KnowledgeGraphService
    for KnowledgeGraphServiceImpl
{
    #[instrument(
        skip(self, request),
        fields(user_id, source_type, ai_session_id, coding_agent)
    )]
    async fn list_tools(
        &self,
        request: Request<ListToolsRequest>,
    ) -> Result<Response<ListToolsResponse>, Status> {
        let ctx = extract_request_context(&request, &self.validator)?;
        tracing::Span::current().record("user_id", ctx.claims.user_id);
        tracing::Span::current().record("source_type", &ctx.claims.source_type);
        record_ai_session_id(&ctx.claims.ai_session_id);
        record_coding_agent(ctx.coding_agent());

        info!("Listing tools for user");

        let tools = V2ToolRegistry::get_all_tools(&self.ontology)
            .into_iter()
            .map(proto_tool_definition)
            .collect();

        Ok(Response::new(ListToolsResponse { tools }))
    }

    #[instrument(
        skip(self, request),
        fields(user_id, source_type, ai_session_id, coding_agent)
    )]
    async fn list_agent_commands(
        &self,
        request: Request<ListAgentCommandsRequest>,
    ) -> Result<Response<ListAgentCommandsResponse>, Status> {
        let ctx = extract_request_context(&request, &self.validator)?;
        tracing::Span::current().record("user_id", ctx.claims.user_id);
        tracing::Span::current().record("source_type", &ctx.claims.source_type);
        record_ai_session_id(&ctx.claims.ai_session_id);
        record_coding_agent(ctx.coding_agent());

        let req = request.get_ref();
        let requested = &req.command_names;
        info!(
            command_count = requested.len(),
            format = ?req.format,
            "Listing agent commands for user"
        );

        let all_commands = V2CommandRegistry::get_all_commands(&self.ontology);
        let commands: Vec<_> = if requested.is_empty() {
            all_commands
        } else {
            let known: BTreeSet<&str> = all_commands
                .iter()
                .map(|command| command.name.as_str())
                .collect();
            let unknown: Vec<&str> = requested
                .iter()
                .map(String::as_str)
                .filter(|name| !known.contains(name))
                .collect();
            if !unknown.is_empty() {
                return Err(Status::not_found(format!(
                    "Unknown command(s): {}",
                    unknown.join(", ")
                )));
            }

            all_commands
                .into_iter()
                .filter(|command| requested.contains(&command.name))
                .collect()
        };

        let formatted_text = if req.format == ResponseFormat::Llm as i32 {
            ToolService::build_command_catalog_toon(&commands)
                .map_err(|e| Status::internal(e.to_string()))?
        } else {
            String::new()
        };

        let commands = commands.into_iter().map(proto_tool_definition).collect();

        Ok(Response::new(ListAgentCommandsResponse {
            commands,
            formatted_text,
        }))
    }

    #[instrument(
        skip(self, request),
        fields(user_id, source_type, ai_session_id, coding_agent)
    )]
    async fn invoke_agent_command(
        &self,
        request: Request<InvokeAgentCommandRequest>,
    ) -> Result<Response<InvokeAgentCommandResponse>, Status> {
        let ctx = extract_request_context(&request, &self.validator)?;
        tracing::Span::current().record("user_id", ctx.claims.user_id);
        tracing::Span::current().record("source_type", &ctx.claims.source_type);
        record_ai_session_id(&ctx.claims.ai_session_id);
        record_coding_agent(ctx.coding_agent());

        let req = request.get_ref();
        if req.command_name.trim().is_empty() {
            return Err(Status::invalid_argument("command_name is required"));
        }

        let parameters_json = if req.parameters_json.trim().is_empty() {
            "{}"
        } else {
            req.parameters_json.as_str()
        };

        info!(command_name = %req.command_name, "Invoking agent command for user");

        let plan = self
            .tool_service
            .resolve_command(&req.command_name, parameters_json)
            .map_err(command_error_to_status)?;

        let ToolPlan::Immediate { result } = plan else {
            return Err(Status::failed_precondition(
                "command must be handled by Rails interceptor",
            ));
        };

        let content = match result {
            serde_json::Value::String(text) => {
                Some(invoke_agent_command_response::Content::FormattedText(text))
            }
            value => {
                let json = serde_json::to_string(&value).map_err(|e| {
                    Status::internal(format!("Failed to encode command result: {e}"))
                })?;
                Some(invoke_agent_command_response::Content::ResultJson(json))
            }
        };

        Ok(Response::new(InvokeAgentCommandResponse { content }))
    }

    type ExecuteQueryStream = ExecuteQueryStream;

    #[instrument(
        skip(self, request),
        fields(user_id, source_type, ai_session_id, coding_agent)
    )]
    async fn execute_query(
        &self,
        request: Request<Streaming<ExecuteQueryMessage>>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let ctx = extract_request_context(&request, &self.validator)?;
        record_coding_agent(ctx.coding_agent());
        let claims = ctx.claims;
        tracing::Span::current().record("user_id", claims.user_id);
        tracing::Span::current().record("source_type", &claims.source_type);
        record_ai_session_id(&claims.ai_session_id);

        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);

        let pipeline = self.pipeline.clone();
        let stream_timeout = self.stream_timeout_secs;
        let span = tracing::Span::current();

        tokio::spawn(
            async move {
                let req = match receive_query_request(&mut stream, &tx).await {
                    Some(r) => r,
                    None => return,
                };

                info!(query_len = req.query.len(), "Executing query");

                let use_llm_format = req.format == ResponseFormat::Llm as i32;

                let timeout = std::time::Duration::from_secs(stream_timeout);
                let result = pipeline
                    .run_query(claims, &req.query, tx.clone(), stream, timeout)
                    .await;

                match result {
                    Ok(output) => {
                        info!("Sending final query result");

                        use crate::proto::execute_query_result::Content;

                        // Static dispatch: monomorphize per formatter type
                        // instead of going through a vtable.
                        let (formatted, format_version, format_name) = if use_llm_format {
                            GoonFormatter.format_stamped(&output)
                        } else {
                            GraphFormatter.format_stamped(&output)
                        };

                        let content = if use_llm_format {
                            // GoonFormatter::format returns Value::String(raw_goon_bytes).
                            // `to_string()` on a Value JSON-encodes it (adds quotes + \n
                            // escapes). Workhorse then JSON-encodes again when wrapping
                            // into the {result, ...} envelope, producing literal `\n` in
                            // the UI. Extract the inner string so the gRPC field carries
                            // raw goon text.
                            let text = match formatted {
                                serde_json::Value::String(s) => s,
                                other => other.to_string(),
                            };
                            Some(Content::FormattedText(text))
                        } else {
                            Some(Content::ResultJson(formatted.to_string()))
                        };

                        let metadata = Some(QueryMetadata {
                            query_type: output.query_type,
                            raw_query_strings: output.raw_query_strings,
                            row_count: i32::try_from(output.row_count).unwrap_or(i32::MAX),
                            format_version,
                            format_name: proto_format_name(format_name).into(),
                        });

                        let _ = tx
                            .send(Ok(ExecuteQueryMessage {
                                content: Some(execute_query_message::Content::Result(
                                    ExecuteQueryResult { content, metadata },
                                )),
                            }))
                            .await;
                    }
                    Err(e @ PipelineError::Timeout) => {
                        // run_query already logged via send_query_error and
                        // recorded the metric through the observer chain.
                        // Translate to deadline_exceeded for the gRPC client.
                        send_query_error(&tx, e).await;
                        let _ = tx
                            .send(Err(Status::deadline_exceeded("Query stream timed out")))
                            .await;
                    }
                    Err(e) => {
                        send_query_error(&tx, e).await;
                    }
                }
            }
            .instrument(span),
        );

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    #[instrument(
        skip(self, request),
        fields(user_id, source_type, ai_session_id, coding_agent)
    )]
    async fn get_graph_schema(
        &self,
        request: Request<GetGraphSchemaRequest>,
    ) -> Result<Response<GetGraphSchemaResponse>, Status> {
        let ctx = extract_request_context(&request, &self.validator)?;
        tracing::Span::current().record("user_id", ctx.claims.user_id);
        tracing::Span::current().record("source_type", &ctx.claims.source_type);
        record_ai_session_id(&ctx.claims.ai_session_id);
        record_coding_agent(ctx.coding_agent());

        let req = request.get_ref();
        info!(format = ?req.format, "Fetching graph schema for user");

        let response = if req.format == ResponseFormat::Llm as i32 {
            let toon_text = self
                .tool_service
                .build_schema_toon(&req.expand_nodes)
                .map_err(|e| Status::internal(e.to_string()))?;
            GetGraphSchemaResponse {
                content: Some(get_graph_schema_response::Content::FormattedText(toon_text)),
            }
        } else {
            let structured = self.build_structured_schema(&req.expand_nodes);
            GetGraphSchemaResponse {
                content: Some(get_graph_schema_response::Content::Structured(structured)),
            }
        };

        Ok(Response::new(response))
    }

    #[instrument(
        skip(self, request),
        fields(user_id, source_type, ai_session_id, coding_agent)
    )]
    async fn get_response_format(
        &self,
        request: Request<GetResponseFormatRequest>,
    ) -> Result<Response<GetResponseFormatResponse>, Status> {
        let ctx = extract_request_context(&request, &self.validator)?;
        tracing::Span::current().record("user_id", ctx.claims.user_id);
        tracing::Span::current().record("source_type", &ctx.claims.source_type);
        record_ai_session_id(&ctx.claims.ai_session_id);
        record_coding_agent(ctx.coding_agent());

        let req = request.get_ref();
        info!(format = ?req.format, "Fetching query response format for user");

        let version = ToolService::build_response_format_version();
        let schema = ToolService::build_response_format_schema().to_string();

        let response = if req.format == ResponseFormat::Llm as i32 {
            let mut toon = String::with_capacity(schema.len() + 64);
            toon.push_str("ResponseFormat v");
            toon.push_str(&version);
            toon.push_str(" (JSON Schema):\n");
            toon.push_str(&schema);
            GetResponseFormatResponse {
                content: Some(get_response_format_response::Content::FormattedText(toon)),
            }
        } else {
            GetResponseFormatResponse {
                content: Some(get_response_format_response::Content::Structured(
                    ResponseFormatSchema { schema, version },
                )),
            }
        };

        Ok(Response::new(response))
    }

    #[instrument(
        skip(self, request),
        fields(user_id, source_type, ai_session_id, coding_agent)
    )]
    async fn get_query_dsl(
        &self,
        request: Request<GetQueryDslRequest>,
    ) -> Result<Response<GetQueryDslResponse>, Status> {
        let ctx = extract_request_context(&request, &self.validator)?;
        tracing::Span::current().record("user_id", ctx.claims.user_id);
        tracing::Span::current().record("source_type", &ctx.claims.source_type);
        record_ai_session_id(&ctx.claims.ai_session_id);
        record_coding_agent(ctx.coding_agent());

        let req = request.get_ref();
        info!(format = ?req.format, "Fetching query DSL grammar for user");

        let response = if req.format == ResponseFormat::Llm as i32 {
            let toon =
                ToolService::build_query_dsl_toon().map_err(|e| Status::internal(e.to_string()))?;
            GetQueryDslResponse {
                version: ToolService::build_query_dsl_version().to_string(),
                content: Some(get_query_dsl_response::Content::FormattedText(toon)),
            }
        } else {
            GetQueryDslResponse {
                version: ToolService::build_query_dsl_version().to_string(),
                content: Some(get_query_dsl_response::Content::RawJsonSchema(
                    ToolService::build_query_dsl_raw().to_string(),
                )),
            }
        };

        Ok(Response::new(response))
    }

    #[instrument(
        skip(self, request),
        fields(user_id, source_type, ai_session_id, coding_agent)
    )]
    async fn get_cluster_health(
        &self,
        request: Request<GetClusterHealthRequest>,
    ) -> Result<Response<GetClusterHealthResponse>, Status> {
        let ctx = extract_request_context(&request, &self.validator)?;
        tracing::Span::current().record("user_id", ctx.claims.user_id);
        tracing::Span::current().record("source_type", &ctx.claims.source_type);
        record_ai_session_id(&ctx.claims.ai_session_id);
        record_coding_agent(ctx.coding_agent());

        let req = request.get_ref();
        info!(format = ?req.format, "Fetching cluster health for user");

        let response = self.cluster_health.get_cluster_health(req.format).await;
        Ok(Response::new(response))
    }

    #[instrument(
        skip(self, request),
        fields(user_id, source_type, ai_session_id, coding_agent)
    )]
    async fn get_graph_status(
        &self,
        request: Request<GetGraphStatusRequest>,
    ) -> Result<Response<GetGraphStatusResponse>, Status> {
        let ctx = extract_request_context(&request, &self.validator)?;
        record_coding_agent(ctx.coding_agent());
        let claims = ctx.claims;
        tracing::Span::current().record("user_id", claims.user_id);
        tracing::Span::current().record("source_type", &claims.source_type);
        record_ai_session_id(&claims.ai_session_id);

        let req = request.get_ref();
        authorize_traversal_path(&claims, &req.traversal_path)?;

        let security_context =
            build_security_context(&claims).map_err(|e| Status::unauthenticated(e.to_string()))?;

        info!(traversal_path = %req.traversal_path, format = ?req.format, "Fetching graph status for user");

        let response = self
            .graph_status
            .get_status(&req.traversal_path, req.format, &security_context)
            .await?;
        Ok(Response::new(response))
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
                let should_expand = expand_nodes.iter().any(|e| e == "*" || e == &n.name);

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
                            description: f.description.clone().unwrap_or_default(),
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

fn authorize_traversal_path(claims: &Claims, requested_path: &str) -> Result<(), Status> {
    if claims.admin {
        return Ok(());
    }

    let authorized_paths: Vec<&str> = claims
        .group_traversal_ids
        .iter()
        .map(|tp| tp.path.as_str())
        .collect();

    let is_authorized = authorized_paths
        .iter()
        .any(|allowed| requested_path.starts_with(allowed));

    if !is_authorized {
        return Err(Status::permission_denied(
            "traversal_path is not within any authorized scope",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::knowledge_graph_service_server::KnowledgeGraphService;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use tonic::metadata::MetadataValue;

    fn mock_validator() -> JwtValidator {
        JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap()
    }

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn test_config() -> ClickHouseConfiguration {
        ClickHouseConfiguration::default()
    }

    fn test_service() -> KnowledgeGraphServiceImpl {
        KnowledgeGraphServiceImpl::new(
            Arc::new(mock_validator()),
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        )
    }

    fn authed_request<T>(message: T) -> Request<T> {
        let now = chrono::Utc::now().timestamp();
        let claims = Claims {
            iat: now,
            exp: now + 3600,
            ..test_claims()
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret-that-is-at-least-32-bytes-long"),
        )
        .unwrap();
        let mut request = Request::new(message);
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
        );
        request
    }

    #[test]
    fn test_service_can_be_created() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

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

    #[tokio::test]
    async fn list_agent_commands_filters_known_command() {
        let service = test_service();
        let response = service
            .list_agent_commands(authed_request(ListAgentCommandsRequest {
                command_names: vec!["get_query_dsl".into()],
                format: ResponseFormat::Raw as i32,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.commands.len(), 1);
        assert_eq!(response.commands[0].name, "get_query_dsl");
        assert!(!response.commands[0].description.is_empty());
    }

    #[tokio::test]
    async fn list_agent_commands_returns_short_command_descriptions() {
        let service = test_service();
        let response = service
            .list_agent_commands(authed_request(ListAgentCommandsRequest {
                command_names: vec![],
                format: ResponseFormat::Raw as i32,
            }))
            .await
            .unwrap()
            .into_inner();

        let query_graph = response
            .commands
            .iter()
            .find(|command| command.name == "query_graph")
            .expect("query_graph command should be listed");

        assert!(!query_graph.description.is_empty());
        assert!(!query_graph.description.contains("<toon>"));
        assert!(!query_graph.description.contains("Query DSL Schema"));
    }

    #[tokio::test]
    async fn list_agent_commands_returns_toon_for_llm_format() {
        let service = test_service();
        let response = service
            .list_agent_commands(authed_request(ListAgentCommandsRequest {
                command_names: vec!["get_query_dsl".into()],
                format: ResponseFormat::Llm as i32,
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.commands.len(), 1);
        assert!(response.formatted_text.contains("commands[1]"));
        assert!(response.formatted_text.contains("name: get_query_dsl"));
        assert!(response.formatted_text.contains("input_schema"));
    }

    #[tokio::test]
    async fn list_agent_commands_rejects_unknown_command() {
        let service = test_service();
        let status = service
            .list_agent_commands(authed_request(ListAgentCommandsRequest {
                command_names: vec!["typo".into()],
                format: ResponseFormat::Raw as i32,
            }))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("typo"));
    }

    #[tokio::test]
    async fn invoke_agent_command_maps_intercepted_command_to_failed_precondition() {
        let service = test_service();
        let status = service
            .invoke_agent_command(authed_request(InvokeAgentCommandRequest {
                command_name: "query_graph".into(),
                parameters_json: r#"{"query":{}}"#.into(),
            }))
            .await
            .unwrap_err();

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn invoke_agent_command_preserves_raw_and_llm_content_shapes() {
        let service = test_service();
        let raw = service
            .invoke_agent_command(authed_request(InvokeAgentCommandRequest {
                command_name: "get_query_dsl".into(),
                parameters_json: r#"{"format":"raw"}"#.into(),
            }))
            .await
            .unwrap()
            .into_inner();

        let Some(invoke_agent_command_response::Content::ResultJson(json)) = raw.content else {
            panic!("expected raw command result JSON");
        };
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed.get("version").and_then(serde_json::Value::as_str),
            Some(ToolService::build_query_dsl_version().as_str())
        );

        let llm = service
            .invoke_agent_command(authed_request(InvokeAgentCommandRequest {
                command_name: "get_query_dsl".into(),
                parameters_json: "{}".into(),
            }))
            .await
            .unwrap()
            .into_inner();

        let Some(invoke_agent_command_response::Content::FormattedText(text)) = llm.content else {
            panic!("expected LLM command result text");
        };
        assert!(text.contains("QueryDSL v"));
    }

    #[test]
    fn test_build_structured_schema() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

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
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

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
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

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
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

        let (outgoing, incoming) = service.get_node_edge_names("NonexistentNode");

        assert!(outgoing.is_empty());
        assert!(incoming.is_empty());
    }

    #[test]
    fn test_expanded_node_has_property_details() {
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

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
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

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
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

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
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            test_ontology(),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

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

    fn test_claims() -> Claims {
        Claims {
            sub: "u:1".into(),
            iss: "gitlab".into(),
            aud: "gitlab-knowledge-graph".into(),
            iat: 0,
            exp: i64::MAX,
            user_id: 1,
            username: "test".into(),
            admin: false,
            organization_id: Some(1),
            min_access_level: None,
            group_traversal_ids: vec![],
            source_type: "rest".into(),
            ai_session_id: None,
            instance_id: None,
            unique_instance_id: None,
            instance_version: None,
            global_user_id: None,
            host_name: None,
            root_namespace_id: None,
            deployment_type: None,
            realm: None,
        }
    }

    #[test]
    fn authorize_traversal_path_grants_and_denies_correctly() {
        let admin = |org| Claims {
            admin: true,
            organization_id: Some(org),
            ..test_claims()
        };
        let user = |org, groups: Vec<&str>| Claims {
            organization_id: Some(org),
            group_traversal_ids: groups
                .into_iter()
                .map(|p| crate::auth::claims::TraversalPathClaim {
                    path: p.to_string(),
                    access_levels: vec![20],
                })
                .collect(),
            ..test_claims()
        };

        for (claims, path) in [
            (admin(42), "42/"),
            (admin(42), "42/100/200/"),
            (admin(42), "99/100/"),
            (user(1, vec!["1/22/", "1/33/"]), "1/22/"),
            (user(1, vec!["1/22/", "1/33/"]), "1/22/44/"),
            (user(1, vec!["1/22/", "1/33/"]), "1/33/55/"),
        ] {
            assert!(
                authorize_traversal_path(&claims, path).is_ok(),
                "expected OK for {path}"
            );
        }

        for (claims, path) in [
            (user(1, vec!["1/22/"]), "1/99/"),
            (user(1, vec!["1/22/33/"]), "1/22/"),
            (user(1, vec![]), "1/22/"),
        ] {
            assert_eq!(
                authorize_traversal_path(&claims, path).unwrap_err().code(),
                tonic::Code::PermissionDenied,
                "expected PermissionDenied for {path}"
            );
        }
    }

    #[test]
    fn test_expand_all_wildcard() {
        let ontology = test_ontology();
        let expected_count = ontology.nodes().count();
        let validator = Arc::new(mock_validator());
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            Arc::clone(&ontology),
            &test_config(),
            ClusterHealthChecker::default().into_arc(),
            60,
            Arc::new(AnalyticsConfig::default()),
        );

        let response = service.build_structured_schema(&["*".to_string()]);

        assert_eq!(
            response.nodes.len(),
            expected_count,
            "Wildcard should return all ontology nodes"
        );

        for node in &response.nodes {
            assert!(
                !node.properties.is_empty(),
                "Node {} should be expanded with wildcard",
                node.name
            );
            assert!(
                node.style.is_some(),
                "Node {} should have style with wildcard",
                node.name
            );
            assert!(
                !node.outgoing_edges.is_empty() || !node.incoming_edges.is_empty(),
                "Node {} should have edges with wildcard",
                node.name
            );
        }
    }

    #[test]
    fn test_response_format_helpers_return_canonical_values() {
        let schema = ToolService::build_response_format_schema();
        assert!(schema.contains("GKG unified query response"));

        let version = ToolService::build_response_format_version();
        assert!(
            !version.is_empty() && version.chars().any(|c| c.is_ascii_digit()),
            "version should look like semver, got {version:?}"
        );
    }
}
