use std::sync::Arc;
use std::time::Duration;

use crate::common::DummyClaims;
use crate::indexer::common::dispatch::start_nats;
use axum::body::Body;
use axum::http::StatusCode;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use integration_testkit::TestContext;
use jsonwebtoken::{EncodingKey, Header, encode};
use nats_client::{KvPutOptions, NatsClient};
use ontology::archive::OntologyArchive;
use orbit_migrations::catalog::{ONTOLOGY_ARCHIVES_BUCKET, OntologyCatalog};
use orbit_migrations::schema::GraphSchema;
use orbit_migrations::version::{
    ensure_version_table, mark_version_migrating, mark_version_retired, promote_version,
    table_prefix,
};
use orbit_server::analytics::InMemoryAnalyticsTracker;
use orbit_server::auth::{Claims, JwtValidator};
use orbit_server::cluster_health::ClusterHealthChecker;
use orbit_server::grpc::OrbitServiceImpl;
use orbit_server::proto::execute_query_message::Content;
use orbit_server::proto::orbit_service_client::OrbitServiceClient;
use orbit_server::proto::orbit_service_server::OrbitServiceServer;
use orbit_server::proto::*;
use orbit_server::schema_watcher::SchemaWatcher;
use orbit_server::webserver::create_router;
use orbit_server_config::AppConfig;
use orbit_utils::yaml;
use serde_json::{Value, json};
use testcontainers_modules::nats::Nats;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::transport::Channel;
use tonic::{Request, Status, Streaming};
use tower::ServiceExt;

const SECRET: &str = "test-secret-that-is-at-least-32-bytes-long";
const WAIT_LIMIT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const NEW_PROPERTY_VALUE: &str = "new property";

const PROPERTIES_ADDED_IN_V2: [(&str, &str); 2] = [
    ("nodes/core/project.yaml", "description"),
    ("nodes/code_review/merge_request.yaml", "merged_at"),
];

#[tokio::test]
async fn promotion_and_rollback_keep_in_flight_queries_on_their_snapshot() {
    let mut cluster = Cluster::start(2).await;
    cluster.publish_archive(1).await;
    cluster.create_tables(&[1, 2]).await;
    cluster.promote(1).await;
    cluster.mark_migrating(2).await;
    cluster.await_serving(Some(1)).await;

    for (pinned, next) in [(1, 2), (2, 1)] {
        let mut query = cluster.open_query(Some(project_query(pinned))).await;
        query.await_authorization_request().await;
        cluster.promote(next).await;
        cluster.await_serving(Some(next)).await;

        let result = query.finish().await.unwrap();
        assert_eq!(project_row(&result), expected_project_row(pinned));
    }

    let result = cluster.run_query(project_query(1)).await.unwrap();
    assert_eq!(project_row(&result), expected_project_row(1));
    assert_eq!(
        cluster.recorded_schema_versions(),
        [json!("1"), json!("2"), json!("1")]
    );
}

#[tokio::test]
async fn named_queries_follow_the_active_schema() {
    let mut cluster = Cluster::start(2).await;
    cluster.publish_archive(1).await;
    cluster.create_tables(&[1, 2]).await;
    cluster.promote(1).await;
    cluster.await_serving(Some(1)).await;

    let names = cluster.named_query_names().await;
    assert!(names.contains(&"my_mrs_with_pipelines".to_string()));
    assert!(!names.contains(&"recent_merges".to_string()));
    let error = cluster
        .run_query(named_query("recent_merges"))
        .await
        .unwrap_err();
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains("recent_merges"));

    cluster.promote(2).await;
    cluster.await_serving(Some(2)).await;

    let names = cluster.named_query_names().await;
    assert!(names.contains(&"my_mrs_with_pipelines".to_string()));
    assert!(names.contains(&"recent_merges".to_string()));
}

#[tokio::test]
async fn missing_and_corrupt_archives_fail_closed_and_recover_without_restart() {
    let mut cluster = Cluster::start(1).await;
    cluster.create_tables(&[2, 3]).await;

    cluster.promote(2).await;
    cluster.await_serving(None).await;
    cluster.list_tools().await.unwrap();
    cluster.publish_archive(2).await;
    cluster.await_serving(Some(2)).await;

    cluster.corrupt_archive(3).await;
    cluster.promote(3).await;
    cluster.await_serving(None).await;
    cluster.list_tools().await.unwrap();
    cluster.restore_archive(3).await;
    cluster.await_serving(Some(3)).await;

    cluster.retire(3).await;
    cluster.await_serving(None).await;
    cluster.promote(3).await;
    cluster.await_serving(Some(3)).await;
}

#[tokio::test]
async fn active_table_loss_and_recovery_gate_warm_and_cold_readers() {
    let mut cluster = Cluster::start(1).await;
    cluster.publish_archive(2).await;
    cluster.create_tables(&[2]).await;
    cluster.promote(2).await;
    cluster.await_serving(Some(2)).await;

    cluster
        .rename_table("v2_gl_project", "unavailable_project")
        .await;
    cluster.await_serving(None).await;
    assert_eq!(cluster.live_status().await, StatusCode::OK);

    let cold_readers = [
        cluster.spawn_read_only_reader(1),
        cluster.spawn_read_only_reader(2),
    ];
    sleep(POLL_INTERVAL * 2).await;
    for reader in &cold_readers {
        assert_eq!(reader.ready_status().await, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(reader.live_status().await, StatusCode::OK);
    }

    cluster
        .rename_table("unavailable_project", "v2_gl_project")
        .await;
    cluster.await_serving(Some(2)).await;
    for reader in &cold_readers {
        reader.await_ready().await;
        reader.assert_read_only().await;
    }
    let result = cluster.run_query(project_query(2)).await.unwrap();
    assert_eq!(project_row(&result), expected_project_row(2));
}

#[tokio::test]
async fn metadata_read_failure_keeps_the_last_usable_schema() {
    let mut cluster = Cluster::start(1).await;
    cluster.create_tables(&[1]).await;
    cluster.promote(1).await;
    cluster.await_serving(Some(1)).await;

    cluster
        .rename_table("gkg_schema_version", "unavailable_schema_version")
        .await;
    cluster.await_failed_version_read().await;

    assert_eq!(cluster.ready_status().await, StatusCode::OK);
    let result = cluster.run_query(project_query(1)).await.unwrap();
    assert_eq!(project_row(&result), expected_project_row(1));
}

#[tokio::test]
async fn idle_and_authorization_blocked_streams_time_out_without_query_success() {
    let mut cluster = Cluster::start_with_stream_timeout(1, Duration::from_secs(1)).await;
    cluster.create_tables(&[1]).await;
    cluster.promote(1).await;
    cluster.await_serving(Some(1)).await;

    let mut idle = cluster.open_query(None).await;
    let mut blocked = cluster.open_query(Some(project_query(1))).await;
    blocked.await_authorization_request().await;

    assert_eq!(error_code(idle.next().await), Some("timeout".into()));
    assert_eq!(error_code(blocked.next().await), Some("timeout".into()));
    assert!(cluster.recorded_schema_versions().is_empty());
}

struct Cluster {
    graph: TestContext,
    config: AppConfig,
    catalog: OntologyCatalog,
    nats_client: Arc<NatsClient>,
    client: OrbitServiceClient<Channel>,
    router: axum::Router,
    analytics: Arc<InMemoryAnalyticsTracker>,
    _shutdown: DropGuard,
    _nats: testcontainers::ContainerAsync<Nats>,
}

impl Cluster {
    async fn start(embedded_version: u32) -> Self {
        Self::start_with_stream_timeout(embedded_version, WAIT_LIMIT).await
    }

    async fn start_with_stream_timeout(embedded_version: u32, stream_timeout: Duration) -> Self {
        let graph = TestContext::new(&[]).await;
        ensure_version_table(&graph.create_client()).await.unwrap();
        let (nats, nats_address) = start_nats().await;
        let config = webserver_config(&graph, &nats_address);
        let nats_client = Arc::new(NatsClient::connect(&config.nats).await.unwrap());
        let catalog = OntologyCatalog::open(nats_client.clone()).await.unwrap();

        let shutdown = CancellationToken::new();
        let watcher = SchemaWatcher::spawn(
            Arc::new(graph.create_client()),
            test_archive(embedded_version),
            catalog.clone(),
            &config,
            shutdown.clone(),
        );
        let analytics = Arc::new(InMemoryAnalyticsTracker::new());
        let service = OrbitServiceImpl::new(
            Arc::new(JwtValidator::new(SECRET, 0).unwrap()),
            watcher.clone(),
            &config.graph,
            ClusterHealthChecker::default().into_arc(),
            stream_timeout.as_secs(),
            Arc::new(config.analytics.clone()),
        )
        .with_analytics(analytics.clone());
        let client = serve_grpc(service, shutdown.clone()).await;

        Self {
            graph,
            config,
            catalog,
            nats_client,
            client,
            router: create_router(watcher),
            analytics,
            _shutdown: shutdown.drop_guard(),
            _nats: nats,
        }
    }

    async fn publish_archive(&self, version: u32) {
        self.catalog.publish(&test_archive(version)).await.unwrap();
    }

    async fn corrupt_archive(&self, version: u32) {
        self.overwrite_archive(version, bytes::Bytes::from_static(b"corrupt archive"))
            .await;
    }

    async fn restore_archive(&self, version: u32) {
        let contents = bytes::Bytes::copy_from_slice(test_archive(version).bytes());
        self.overwrite_archive(version, contents).await;
    }

    async fn overwrite_archive(&self, version: u32, contents: bytes::Bytes) {
        self.nats_client
            .kv_put(
                ONTOLOGY_ARCHIVES_BUCKET,
                &version.to_string(),
                contents,
                KvPutOptions::default(),
            )
            .await
            .unwrap();
    }

    async fn create_tables(&self, versions: &[u32]) {
        for &version in versions {
            let ontology = test_archive(version).load_ontology().unwrap();
            let project_table = format!("v{version}_gl_project");
            for table_name in GraphSchema::from_ontology(&ontology)
                .prefixed_table_names(&table_prefix(version))
                .into_iter()
                .filter(|table_name| *table_name != project_table)
            {
                self.graph
                    .execute(&format!(
                        "CREATE TABLE {table_name} (id Int64) ENGINE = Memory"
                    ))
                    .await;
            }
            let description_column = if project_has_description_in(version) {
                format!(", description String DEFAULT '{NEW_PROPERTY_VALUE}'")
            } else {
                String::new()
            };
            self.graph
                .execute(&format!(
                    "CREATE TABLE {project_table} (id Int64, name String, full_path String, \
                     traversal_path String, _version UInt64, _deleted Bool {description_column}) \
                     ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id)"
                ))
                .await;
            self.graph
                .execute(&format!(
                    "INSERT INTO {project_table} (id, name, full_path, traversal_path, _version, _deleted) \
                     VALUES (1, 'project v{version}', 'group/project', '1/', 1, false)"
                ))
                .await;
        }
    }

    async fn rename_table(&self, from: &str, to: &str) {
        self.graph
            .execute(&format!("RENAME TABLE {from} TO {to}"))
            .await;
    }

    async fn promote(&self, version: u32) {
        promote_version(&self.graph.create_client(), version)
            .await
            .unwrap();
    }

    async fn mark_migrating(&self, version: u32) {
        mark_version_migrating(&self.graph.create_client(), version)
            .await
            .unwrap();
    }

    async fn retire(&self, version: u32) {
        mark_version_retired(&self.graph.create_client(), version)
            .await
            .unwrap();
    }

    fn spawn_read_only_reader(&self, embedded_version: u32) -> Reader {
        let mut config = self.config.clone();
        config
            .graph
            .session_settings
            .insert("readonly".into(), "1".into());
        let client = Arc::new(config.graph.build_client());
        let shutdown = CancellationToken::new();
        let watcher = SchemaWatcher::spawn(
            client.clone(),
            test_archive(embedded_version),
            self.catalog.clone(),
            &config,
            shutdown.clone(),
        );
        Reader {
            router: create_router(watcher),
            client,
            _shutdown: shutdown.drop_guard(),
        }
    }

    async fn await_serving(&mut self, version: Option<u32>) {
        let expected = version.map(project_has_description_in);
        timeout(WAIT_LIMIT, async {
            while self.structured_schema_project_has_description().await != expected {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("serving must reflect the active archive");
        assert_eq!(
            self.compact_schema_project_has_description().await,
            expected
        );
        let ready = if version.is_some() {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        assert_eq!(self.ready_status().await, ready);
    }

    async fn structured_schema_project_has_description(&mut self) -> Option<bool> {
        let response = self
            .client
            .get_graph_schema(authenticated(GetGraphSchemaRequest {
                expand_nodes: vec!["Project".into()],
                ..Default::default()
            }))
            .await;
        let response = match response {
            Ok(response) => response.into_inner(),
            Err(error) if error.code() == tonic::Code::Unavailable => return None,
            Err(error) => panic!("unexpected schema error: {error}"),
        };
        let Some(get_graph_schema_response::Content::Structured(schema)) = response.content else {
            panic!("expected structured schema");
        };
        let project = schema
            .nodes
            .iter()
            .find(|node| node.name == "Project")
            .unwrap();
        Some(
            project
                .properties
                .iter()
                .any(|property| property.name == "description"),
        )
    }

    async fn compact_schema_project_has_description(&mut self) -> Option<bool> {
        let command = self
            .client
            .invoke_agent_command(authenticated(InvokeAgentCommandRequest {
                command_name: "get_graph_schema".into(),
                parameters_json: json!({"format": "raw", "expand_nodes": ["Project"]}).to_string(),
            }))
            .await;
        let response = match command {
            Ok(response) => response.into_inner(),
            Err(error) if error.code() == tonic::Code::Unavailable => return None,
            Err(error) => panic!("unexpected command error: {error}"),
        };
        let Some(invoke_agent_command_response::Content::ResultJson(encoded)) = response.content
        else {
            panic!("expected compact graph schema");
        };
        let schema: Value = serde_json::from_str(&encoded).unwrap();
        let project = schema["domains"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|domain| domain["nodes"].as_array().unwrap())
            .find(|node| node["name"] == "Project")
            .unwrap();
        Some(
            project["props"]
                .as_array()
                .unwrap()
                .iter()
                .any(|property| property.as_str().unwrap().starts_with("description:")),
        )
    }

    async fn await_failed_version_read(&self) {
        timeout(WAIT_LIMIT, async {
            loop {
                self.graph.execute("SYSTEM FLUSH LOGS").await;
                let failures = self
                    .graph
                    .query(
                        "SELECT query_id FROM system.query_log WHERE exception != '' \
                         AND startsWith(query, 'SELECT version FROM gkg_schema_version FINAL')",
                    )
                    .await;
                if failures.iter().any(|batch| batch.num_rows() > 0) {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("watcher must encounter the metadata failure");
    }

    async fn ready_status(&self) -> StatusCode {
        probe(&self.router, "/ready").await
    }

    async fn live_status(&self) -> StatusCode {
        probe(&self.router, "/live").await
    }

    async fn list_tools(&mut self) -> Result<ListToolsResponse, Status> {
        self.client
            .list_tools(authenticated(ListToolsRequest::default()))
            .await
            .map(|response| response.into_inner())
    }

    async fn named_query_names(&mut self) -> Vec<String> {
        self.client
            .list_named_queries(authenticated(ListNamedQueriesRequest::default()))
            .await
            .unwrap()
            .into_inner()
            .queries
            .into_iter()
            .map(|query| query.name)
            .collect()
    }

    async fn open_query(&mut self, request: Option<ExecuteQueryRequest>) -> QueryStream {
        QueryStream::open(&mut self.client, request).await
    }

    async fn run_query(
        &mut self,
        request: ExecuteQueryRequest,
    ) -> Result<Value, ExecuteQueryError> {
        self.open_query(Some(request)).await.finish().await
    }

    fn recorded_schema_versions(&self) -> Vec<Value> {
        self.analytics
            .drain()
            .iter()
            .map(|event| event.contexts()[1].data["graph_schema_version"].clone())
            .collect()
    }
}

struct Reader {
    router: axum::Router,
    client: Arc<ArrowClickHouseClient>,
    _shutdown: DropGuard,
}

impl Reader {
    async fn ready_status(&self) -> StatusCode {
        probe(&self.router, "/ready").await
    }

    async fn live_status(&self) -> StatusCode {
        probe(&self.router, "/live").await
    }

    async fn await_ready(&self) {
        timeout(WAIT_LIMIT, async {
            while self.ready_status().await != StatusCode::OK {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("a cold reader must become ready when the active tables are restored");
    }

    async fn assert_read_only(&self) {
        let write = self
            .client
            .execute("INSERT INTO v2_gl_project (id) VALUES (2)")
            .await;
        assert!(write.is_err());
    }
}

struct QueryStream {
    sender: mpsc::Sender<ExecuteQueryMessage>,
    stream: Streaming<ExecuteQueryMessage>,
    authorization_request: Option<RedactionRequired>,
}

impl QueryStream {
    async fn open(
        client: &mut OrbitServiceClient<Channel>,
        request: Option<ExecuteQueryRequest>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(4);
        if let Some(request) = request {
            sender
                .send(ExecuteQueryMessage {
                    content: Some(Content::Request(request)),
                })
                .await
                .unwrap();
        }
        let stream = client
            .execute_query(authenticated(ReceiverStream::new(receiver)))
            .await
            .unwrap()
            .into_inner();
        Self {
            sender,
            stream,
            authorization_request: None,
        }
    }

    async fn next(&mut self) -> Content {
        timeout(WAIT_LIMIT, self.stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .content
            .unwrap()
    }

    async fn await_authorization_request(&mut self) {
        let message = self.next().await;
        let Content::Redaction(RedactionExchange {
            content: Some(redaction_exchange::Content::Required(required)),
        }) = message
        else {
            panic!("expected authorization request, got {message:?}");
        };
        self.authorization_request = Some(required);
    }

    async fn finish(mut self) -> Result<Value, ExecuteQueryError> {
        loop {
            if let Some(required) = self.authorization_request.take() {
                self.authorize_everything(required).await;
            }
            match self.next().await {
                Content::Redaction(RedactionExchange {
                    content: Some(redaction_exchange::Content::Required(required)),
                }) => self.authorization_request = Some(required),
                Content::Result(ExecuteQueryResult {
                    content: Some(execute_query_result::Content::ResultJson(json)),
                    ..
                }) => return Ok(serde_json::from_str(&json).unwrap()),
                Content::Error(error) => return Err(error),
                other => panic!("unexpected query message {other:?}"),
            }
        }
    }

    async fn authorize_everything(&self, required: RedactionRequired) {
        let authorizations = required
            .resources
            .into_iter()
            .map(|resource| ResourceAuthorization {
                resource_type: resource.resource_type,
                authorized: resource
                    .resource_ids
                    .into_iter()
                    .map(|id| (id, true))
                    .collect(),
            })
            .collect();
        self.sender
            .send(ExecuteQueryMessage {
                content: Some(Content::Redaction(RedactionExchange {
                    content: Some(redaction_exchange::Content::Response(RedactionResponse {
                        result_id: required.result_id,
                        authorizations,
                    })),
                })),
            })
            .await
            .unwrap();
    }
}

#[derive(Debug, PartialEq)]
struct ProjectRow {
    name: String,
    description: Option<String>,
}

fn project_row(result: &Value) -> ProjectRow {
    let node = &result["nodes"][0];
    ProjectRow {
        name: node["name"].as_str().unwrap().to_string(),
        description: node["description"].as_str().map(String::from),
    }
}

fn expected_project_row(version: u32) -> ProjectRow {
    ProjectRow {
        name: format!("project v{version}"),
        description: project_has_description_in(version).then(|| NEW_PROPERTY_VALUE.to_string()),
    }
}

fn project_query(version: u32) -> ExecuteQueryRequest {
    let columns = if project_has_description_in(version) {
        json!(["name", "description"])
    } else {
        json!(["name"])
    };
    ExecuteQueryRequest {
        query: json!({
            "query_type": "traversal",
            "nodes": [{"id": "project", "entity": "Project", "node_ids": [1], "columns": columns}],
            "limit": 10
        })
        .to_string(),
        ..Default::default()
    }
}

fn named_query(name: &str) -> ExecuteQueryRequest {
    ExecuteQueryRequest {
        query_type: QueryType::Named as i32,
        query: json!({"name": name}).to_string(),
        ..Default::default()
    }
}

fn error_code(message: Content) -> Option<String> {
    match message {
        Content::Error(error) => Some(error.code),
        _ => None,
    }
}

fn project_has_description_in(version: u32) -> bool {
    version > 1
}

fn test_archive(version: u32) -> OntologyArchive {
    let mut sources = ontology::migrations::embedded_sources();
    if version == 1 {
        for (path, property) in PROPERTIES_ADDED_IN_V2 {
            let mut node: Value = yaml::from_str(&sources[path]).unwrap();
            node["properties"]
                .as_object_mut()
                .unwrap()
                .remove(property)
                .unwrap();
            node["storage"]["columns"]
                .as_array_mut()
                .unwrap()
                .retain(|column| column["name"] != property);
            sources.insert(path.into(), yaml::to_string(&node).unwrap());
        }
    }
    OntologyArchive::from_sources(version, &sources).unwrap()
}

fn webserver_config(graph: &TestContext, nats_address: &str) -> AppConfig {
    let mut config = AppConfig::embedded_defaults();
    config.graph = graph.config.clone();
    config.schema.version_poll_interval_secs = POLL_INTERVAL.as_secs();
    config.nats.url = format!("nats://{nats_address}");
    config
}

async fn serve_grpc(
    service: OrbitServiceImpl,
    shutdown: CancellationToken,
) -> OrbitServiceClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(OrbitServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown.cancelled())
            .await
            .unwrap();
    });
    OrbitServiceClient::connect(format!("http://{address}"))
        .await
        .unwrap()
}

fn authenticated<T>(message: T) -> Request<T> {
    let claims = Claims {
        admin: false,
        ..Claims::dummy()
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

async fn probe(router: &axum::Router, path: &str) -> StatusCode {
    router
        .clone()
        .oneshot(axum::http::Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}
