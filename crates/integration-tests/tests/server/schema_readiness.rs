use std::sync::Arc;
use std::time::Duration;

use crate::common::DummyClaims;
use crate::indexer::common::dispatch::start_nats;
use axum::body::Body;
use axum::http::StatusCode;
use clickhouse_client::ClickHouseConfigurationExt;
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
use tonic::{Request, Streaming};
use tower::ServiceExt;

const SECRET: &str = "test-secret-that-is-at-least-32-bytes-long";
const WAIT_LIMIT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(1);

const PROPERTIES_ADDED_IN_V2: [(&str, &str); 2] = [
    ("nodes/core/project.yaml", "description"),
    ("nodes/code_review/merge_request.yaml", "merged_at"),
];

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

fn has_description(version: u32) -> bool {
    version > 1
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

async fn probe_status(router: &axum::Router, path: &str) -> StatusCode {
    router
        .clone()
        .oneshot(axum::http::Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

struct ServingFixture {
    database: TestContext,
    catalog: OntologyCatalog,
    broker: Arc<NatsClient>,
    client: OrbitServiceClient<tonic::transport::Channel>,
    router: axum::Router,
    analytics: Arc<InMemoryAnalyticsTracker>,
    _shutdown: DropGuard,
    _nats: testcontainers::ContainerAsync<Nats>,
}

impl ServingFixture {
    async fn start(embedded_version: u32, stream_timeout: Duration) -> Self {
        let database = TestContext::new(&[]).await;
        ensure_version_table(&database.create_client())
            .await
            .unwrap();
        let (nats, address) = start_nats().await;
        let mut config = AppConfig::embedded_defaults();
        config.graph = database.config.clone();
        config.schema.version_poll_interval_secs = POLL_INTERVAL.as_secs();
        config.nats.url = format!("nats://{address}");
        let broker = Arc::new(NatsClient::connect(&config.nats).await.unwrap());
        let catalog = OntologyCatalog::open(broker.clone()).await.unwrap();
        let shutdown = CancellationToken::new();
        let watcher = SchemaWatcher::spawn(
            Arc::new(database.create_client()),
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
            Arc::new(config.analytics),
        )
        .with_analytics(analytics.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(OrbitServiceServer::new(service))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    server_shutdown.cancelled(),
                )
                .await
                .unwrap();
        });
        let client = OrbitServiceClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        Self {
            database,
            catalog,
            broker,
            client,
            router: create_router(watcher),
            analytics,
            _shutdown: shutdown.drop_guard(),
            _nats: nats,
        }
    }

    async fn await_serving_version(&mut self, version: Option<u32>) {
        timeout(WAIT_LIMIT, async {
            while self.structured_schema_has_description().await != version.map(has_description) {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("serving must reflect the active archive");
        self.assert_compact_schema_matches(version).await;
        self.assert_readiness_matches(version).await;
    }

    async fn structured_schema_has_description(&mut self) -> Option<bool> {
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

    async fn assert_compact_schema_matches(&mut self, version: Option<u32>) {
        let command = self
            .client
            .invoke_agent_command(authenticated(InvokeAgentCommandRequest {
                command_name: "get_graph_schema".into(),
                parameters_json: json!({"format": "raw", "expand_nodes": ["Project"]}).to_string(),
            }))
            .await;
        let Some(version) = version else {
            assert_eq!(command.unwrap_err().code(), tonic::Code::Unavailable);
            return;
        };
        let Some(invoke_agent_command_response::Content::ResultJson(encoded)) =
            command.unwrap().into_inner().content
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
        let project_has_description = project["props"]
            .as_array()
            .unwrap()
            .iter()
            .any(|property| property.as_str().unwrap().starts_with("description:"));
        assert_eq!(project_has_description, has_description(version));
    }

    async fn assert_readiness_matches(&self, version: Option<u32>) {
        let expected = if version.is_some() {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        assert_eq!(probe_status(&self.router, "/ready").await, expected);
    }

    async fn create_schema_tables(&self, version: u32) {
        let ontology = test_archive(version).load_ontology().unwrap();
        let schema = GraphSchema::from_ontology(&ontology);
        for table_name in schema.prefixed_table_names(&table_prefix(version)) {
            if table_name != format!("v{version}_gl_project") {
                self.database
                    .execute(&format!(
                        "CREATE TABLE {table_name} (id Int64) ENGINE = Memory"
                    ))
                    .await;
            }
        }
        let description = if has_description(version) {
            ", description String DEFAULT 'new property'"
        } else {
            ""
        };
        self.database
            .execute(&format!(
                "CREATE TABLE v{version}_gl_project (id Int64, name String, full_path String, \
             traversal_path String, _version UInt64, _deleted Bool {description}) \
             ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id)"
            ))
            .await;
        self.database.execute(&format!(
            "INSERT INTO v{version}_gl_project (id, name, full_path, traversal_path, _version, _deleted) \
             VALUES (1, 'project v{version}', 'group/project', '1/', 1, false)"
        )).await;
    }

    async fn put_archive_bytes(&self, version: u32, contents: bytes::Bytes) {
        self.broker
            .kv_put(
                ONTOLOGY_ARCHIVES_BUCKET,
                &version.to_string(),
                contents,
                KvPutOptions::default(),
            )
            .await
            .unwrap();
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

    async fn run_project_query(&mut self, columns: &[&str]) -> Value {
        let mut query = QueryStream::open(&mut self.client, Some(project_query(columns))).await;
        let authorization = query.receive().await;
        query.authorize_and_finish(authorization).await
    }
}

fn project_query(columns: &[&str]) -> ExecuteQueryRequest {
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

struct QueryStream {
    sender: mpsc::Sender<ExecuteQueryMessage>,
    stream: Streaming<ExecuteQueryMessage>,
}

impl QueryStream {
    async fn open(
        client: &mut OrbitServiceClient<tonic::transport::Channel>,
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
        Self { sender, stream }
    }

    async fn receive(&mut self) -> Content {
        timeout(WAIT_LIMIT, self.stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .content
            .unwrap()
    }

    async fn authorize_and_finish(mut self, message: Content) -> Value {
        let Content::Redaction(RedactionExchange {
            content: Some(redaction_exchange::Content::Required(required)),
        }) = message
        else {
            panic!("expected authorization request, got {message:?}");
        };
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
        let response = self.receive().await;
        let Content::Result(ExecuteQueryResult {
            content: Some(execute_query_result::Content::ResultJson(json)),
            ..
        }) = response
        else {
            panic!("expected JSON query result, got {response:?}");
        };
        serde_json::from_str(&json).unwrap()
    }
}

#[tokio::test]
async fn promotion_and_rollback_keep_in_flight_queries_on_their_snapshot() {
    let mut fixture = ServingFixture::start(2, WAIT_LIMIT).await;
    fixture.catalog.publish(&test_archive(1)).await.unwrap();
    for version in [1, 2] {
        fixture.create_schema_tables(version).await;
    }
    let graph = fixture.database.create_client();
    promote_version(&graph, 1).await.unwrap();
    mark_version_migrating(&graph, 2).await.unwrap();
    fixture.await_serving_version(Some(1)).await;

    for (current, next) in [(1, 2), (2, 1)] {
        let columns: &[&str] = if has_description(current) {
            &["name", "description"]
        } else {
            &["name"]
        };
        let mut query = QueryStream::open(&mut fixture.client, Some(project_query(columns))).await;
        let authorization = query.receive().await;
        promote_version(&graph, next).await.unwrap();
        fixture.await_serving_version(Some(next)).await;

        let result = query.authorize_and_finish(authorization).await;
        assert_eq!(result["nodes"][0]["name"], format!("project v{current}"));
        let expected_description = if has_description(current) {
            json!("new property")
        } else {
            Value::Null
        };
        assert_eq!(result["nodes"][0]["description"], expected_description);
    }

    let result = fixture.run_project_query(&["name"]).await;
    assert_eq!(result["nodes"][0]["name"], "project v1");
    let versions: Vec<_> = fixture
        .analytics
        .drain()
        .iter()
        .map(|event| event.contexts()[1].data["graph_schema_version"].clone())
        .collect();
    assert_eq!(versions, [json!("1"), json!("2"), json!("1")]);
}

#[tokio::test]
async fn named_queries_follow_the_active_schema() {
    let mut fixture = ServingFixture::start(2, WAIT_LIMIT).await;
    fixture.catalog.publish(&test_archive(1)).await.unwrap();
    for version in [1, 2] {
        fixture.create_schema_tables(version).await;
    }
    let graph = fixture.database.create_client();

    promote_version(&graph, 1).await.unwrap();
    fixture.await_serving_version(Some(1)).await;
    let names = fixture.named_query_names().await;
    assert!(names.contains(&"my_mrs_with_pipelines".to_string()));
    assert!(!names.contains(&"recent_merges".to_string()));

    let mut query =
        QueryStream::open(&mut fixture.client, Some(named_query("recent_merges"))).await;
    let response = query.receive().await;
    let Content::Error(error) = response else {
        panic!("expected unavailable named query to be rejected, got {response:?}");
    };
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains("recent_merges"));

    promote_version(&graph, 2).await.unwrap();
    fixture.await_serving_version(Some(2)).await;
    let names = fixture.named_query_names().await;
    assert!(names.contains(&"my_mrs_with_pipelines".to_string()));
    assert!(names.contains(&"recent_merges".to_string()));
}

#[tokio::test]
async fn missing_and_corrupt_archives_fail_closed_and_recover_without_restart() {
    let mut fixture = ServingFixture::start(1, WAIT_LIMIT).await;
    let graph = fixture.database.create_client();
    let corrupt_archive = bytes::Bytes::from_static(b"corrupt archive");

    for (version, corrupt_contents) in [(2, None), (3, Some(corrupt_archive))] {
        fixture.create_schema_tables(version).await;
        if let Some(contents) = corrupt_contents {
            fixture.put_archive_bytes(version, contents).await;
        }
        promote_version(&graph, version).await.unwrap();
        fixture.await_serving_version(None).await;
        fixture
            .client
            .list_tools(authenticated(ListToolsRequest::default()))
            .await
            .unwrap();

        let restored_contents = bytes::Bytes::copy_from_slice(test_archive(version).bytes());
        fixture.put_archive_bytes(version, restored_contents).await;
        fixture.await_serving_version(Some(version)).await;
    }

    mark_version_retired(&graph, 3).await.unwrap();
    fixture.await_serving_version(None).await;
    promote_version(&graph, 3).await.unwrap();
    fixture.await_serving_version(Some(3)).await;
}

#[tokio::test]
async fn active_table_loss_and_recovery_gate_warm_and_cold_readers() {
    let mut fixture = ServingFixture::start(1, WAIT_LIMIT).await;
    fixture.create_schema_tables(2).await;
    fixture.catalog.publish(&test_archive(2)).await.unwrap();
    promote_version(&fixture.database.create_client(), 2)
        .await
        .unwrap();
    fixture.await_serving_version(Some(2)).await;

    fixture
        .database
        .execute("RENAME TABLE v2_gl_project TO unavailable_project")
        .await;
    fixture.await_serving_version(None).await;
    assert_eq!(probe_status(&fixture.router, "/live").await, StatusCode::OK);

    let mut config = AppConfig::embedded_defaults();
    config.schema.version_poll_interval_secs = POLL_INTERVAL.as_secs();
    config.graph = fixture.database.config.clone();
    config
        .graph
        .session_settings
        .insert("readonly".into(), "1".into());
    let reader_client = Arc::new(config.graph.build_client());
    let cold_readers = [1, 2].map(|embedded_version| {
        let shutdown = CancellationToken::new();
        let watcher = SchemaWatcher::spawn(
            reader_client.clone(),
            test_archive(embedded_version),
            fixture.catalog.clone(),
            &config,
            shutdown.clone(),
        );
        (create_router(watcher), shutdown.drop_guard())
    });

    sleep(POLL_INTERVAL * 2).await;
    for (router, _) in &cold_readers {
        assert_eq!(
            probe_status(router, "/ready").await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(probe_status(router, "/live").await, StatusCode::OK);
    }

    fixture
        .database
        .execute("RENAME TABLE unavailable_project TO v2_gl_project")
        .await;
    fixture.await_serving_version(Some(2)).await;
    for (router, _) in &cold_readers {
        timeout(WAIT_LIMIT, async {
            while probe_status(router, "/ready").await != StatusCode::OK {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("a cold reader must become ready when the active tables are restored");
    }

    let result = fixture.run_project_query(&["name"]).await;
    assert_eq!(result["nodes"][0]["name"], "project v2");
    assert!(
        reader_client
            .execute("INSERT INTO v2_gl_project (id) VALUES (2)")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn metadata_read_failure_keeps_the_last_usable_schema() {
    let mut fixture = ServingFixture::start(1, WAIT_LIMIT).await;
    fixture.create_schema_tables(1).await;
    promote_version(&fixture.database.create_client(), 1)
        .await
        .unwrap();
    fixture.await_serving_version(Some(1)).await;
    fixture
        .database
        .execute("RENAME TABLE gkg_schema_version TO unavailable_schema_version")
        .await;

    timeout(WAIT_LIMIT, async {
        loop {
            fixture.database.execute("SYSTEM FLUSH LOGS").await;
            let failures = fixture
                .database
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

    fixture.assert_readiness_matches(Some(1)).await;
    let result = fixture.run_project_query(&["name"]).await;
    assert_eq!(result["nodes"][0]["name"], "project v1");
}

#[tokio::test]
async fn idle_and_authorization_blocked_streams_time_out_without_query_success() {
    let mut fixture = ServingFixture::start(1, Duration::from_secs(1)).await;
    fixture.create_schema_tables(1).await;
    promote_version(&fixture.database.create_client(), 1)
        .await
        .unwrap();
    fixture.await_serving_version(Some(1)).await;

    for request in [None, Some(project_query(&["name"]))] {
        let waiting_for_authorization = request.is_some();
        let mut query = QueryStream::open(&mut fixture.client, request).await;
        if waiting_for_authorization {
            assert!(matches!(query.receive().await, Content::Redaction(_)));
        }
        let response = query.receive().await;
        assert!(
            matches!(&response, Content::Error(error) if error.code == "timeout"),
            "{response:?}"
        );
        assert_eq!(fixture.analytics.count(), 0);
    }
}
