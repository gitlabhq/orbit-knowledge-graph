use std::pin::Pin;
use std::sync::Arc;

use clickhouse_client::ClickHouseConfigurationExt;
use gkg_server_config::{AnalyticsConfig, ClickHouseConfiguration};
use ontology::Ontology;
use query_engine::shared::content::ColumnResolverRegistry;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{Instrument, info, instrument};

use super::auth::extract_claims;
use crate::analytics::AnalyticsTracker;
use crate::auth::{Claims, JwtValidator, build_security_context};
use crate::billing::BillingTracker;
use crate::cluster_health::ClusterHealthChecker;
use crate::graph_status::GraphStatusService;
use crate::pipeline::{QueryPipelineService, receive_query_request, send_query_error};
use crate::proto::{
    ExecuteQueryMessage, ExecuteQueryResult, FormatName as ProtoFormatName,
    GetClusterHealthRequest, GetClusterHealthResponse, GetGraphSchemaRequest,
    GetGraphSchemaResponse, GetGraphStatusRequest, GetGraphStatusResponse, ListToolsRequest,
    ListToolsResponse, QueryMetadata, ResponseFormat, SchemaDomain, SchemaEdge, SchemaEdgeVariant,
    SchemaNode, SchemaNodeStyle, SchemaProperty, StructuredSchema,
    ToolDefinition as ProtoToolDefinition, execute_query_message, get_graph_schema_response,
};
use crate::tools::{ToolRegistry, ToolService};
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
    #[instrument(skip(self, request), fields(user_id, source_type, ai_session_id))]
    async fn list_tools(
        &self,
        request: Request<ListToolsRequest>,
    ) -> Result<Response<ListToolsResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);
        tracing::Span::current().record("source_type", &claims.source_type);
        record_ai_session_id(&claims.ai_session_id);

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
    }

    type ExecuteQueryStream = ExecuteQueryStream;

    #[instrument(skip(self, request), fields(user_id, source_type, ai_session_id))]
    async fn execute_query(
        &self,
        request: Request<Streaming<ExecuteQueryMessage>>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
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
                let handler = async {
                    let req = match receive_query_request(&mut stream, &tx).await {
                        Some(r) => r,
                        None => return,
                    };

                    info!(query_len = req.query.len(), "Executing query");

                    let use_llm_format = req.format == ResponseFormat::Llm as i32;

                    let result = pipeline
                        .run_query(claims, &req.query, tx.clone(), stream)
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
                                Some(Content::FormattedText(formatted.to_string()))
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
                        Err(e) => {
                            send_query_error(&tx, e).await;
                        }
                    }
                };

                if tokio::time::timeout(std::time::Duration::from_secs(stream_timeout), handler)
                    .await
                    .is_err()
                {
                    tracing::warn!("Query stream timed out after 60s");
                    let _ = tx
                        .send(Err(Status::deadline_exceeded("Query stream timed out")))
                        .await;
                }
            }
            .instrument(span),
        );

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    #[instrument(skip(self, request), fields(user_id, source_type, ai_session_id))]
    async fn get_graph_schema(
        &self,
        request: Request<GetGraphSchemaRequest>,
    ) -> Result<Response<GetGraphSchemaResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);
        tracing::Span::current().record("source_type", &claims.source_type);
        record_ai_session_id(&claims.ai_session_id);

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

    #[instrument(skip(self, request), fields(user_id, source_type, ai_session_id))]
    async fn get_cluster_health(
        &self,
        request: Request<GetClusterHealthRequest>,
    ) -> Result<Response<GetClusterHealthResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
        tracing::Span::current().record("user_id", claims.user_id);
        tracing::Span::current().record("source_type", &claims.source_type);
        record_ai_session_id(&claims.ai_session_id);

        let req = request.get_ref();
        info!(format = ?req.format, "Fetching cluster health for user");

        let response = self.cluster_health.get_cluster_health(req.format).await;
        Ok(Response::new(response))
    }

    #[instrument(skip(self, request), fields(user_id, source_type, ai_session_id))]
    async fn get_graph_status(
        &self,
        request: Request<GetGraphStatusRequest>,
    ) -> Result<Response<GetGraphStatusResponse>, Status> {
        let claims = extract_claims(&request, &self.validator)?;
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
    let org_id = claims
        .organization_id
        .ok_or_else(|| Status::unauthenticated("missing organization_id in claims"))?;

    let authorized_paths: Vec<String> = if claims.admin {
        vec![format!("{org_id}/")]
    } else {
        claims
            .group_traversal_ids
            .iter()
            .map(|tp| tp.path.clone())
            .collect()
    };

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

    fn mock_validator() -> JwtValidator {
        JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap()
    }

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("ontology must load"))
    }

    fn test_config() -> ClickHouseConfiguration {
        ClickHouseConfiguration::default()
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
            (admin(42), "99/100/"),
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
    fn authorize_traversal_path_missing_org_id_returns_unauthenticated() {
        let claims = Claims {
            organization_id: None,
            ..test_claims()
        };
        assert_eq!(
            authorize_traversal_path(&claims, "1/22/")
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
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
}
