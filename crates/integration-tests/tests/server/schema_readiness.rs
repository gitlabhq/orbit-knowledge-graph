use std::sync::Arc;
use std::time::Duration;

use crate::indexer::common::dispatch::start_nats;
use arrow::array::StringArray;
use axum::body::Body;
use axum::http::StatusCode;
use base64::Engine;
use integration_testkit::TestContext;
use jsonwebtoken::{EncodingKey, Header, encode};
use nats_client::NatsClient;
use ontology::archive::OntologyArchive;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::version::{
    ensure_version_table, mark_version_active, mark_version_migrating, mark_version_retired,
    promote_version,
};
use orbit_server::auth::JwtValidator;
use orbit_server::cluster_health::ClusterHealthChecker;
use orbit_server::grpc::OrbitServiceImpl;
use orbit_server::proto::orbit_service_client::OrbitServiceClient;
use orbit_server::proto::orbit_service_server::OrbitServiceServer;
use orbit_server::proto::*;
use orbit_server::schema_watcher::SchemaWatcher;
use orbit_server::webserver::create_router;
use orbit_server_config::AppConfig;
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::yaml;
use serde_json::{Value, json};
use testcontainers_modules::nats::Nats;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Streaming};
use tower::ServiceExt;

const SECRET: &str = "test-secret-that-is-at-least-32-bytes-long";
const WAIT_LIMIT: Duration = Duration::from_secs(30);
const EXPANDED_SCHEMA_VERSION: u32 = 2;

fn archive(version: u32) -> OntologyArchive {
    let mut sources = ontology::migrations::embedded_sources();

    if version < EXPANDED_SCHEMA_VERSION {
        let project_path = "nodes/core/project.yaml";
        let mut project: Value = yaml::from_str(&sources[project_path]).unwrap();
        project["properties"]
            .as_object_mut()
            .unwrap()
            .remove("description")
            .unwrap();
        project["storage"]["columns"]
            .as_array_mut()
            .unwrap()
            .retain(|column| column["name"] != "description");
        sources.insert(project_path.into(), yaml::to_string(&project).unwrap());
    }

    if version < EXPANDED_SCHEMA_VERSION {
        let node_path = "nodes/code_review/merge_request.yaml";
        let mut node: Value = yaml::from_str(&sources[node_path]).unwrap();
        node["properties"]
            .as_object_mut()
            .unwrap()
            .remove("merged_at")
            .unwrap();
        node["storage"]["columns"]
            .as_array_mut()
            .unwrap()
            .retain(|column| column["name"] != "merged_at");
        sources.insert(node_path.into(), yaml::to_string(&node).unwrap());
    }

    OntologyArchive::from_sources(version, &sources).unwrap()
}

fn authenticated<T>(message: T) -> Request<T> {
    let now = chrono::Utc::now().timestamp();
    let claims = serde_json::json!({
        "sub": "u:1", "iss": "gitlab", "aud": "gitlab-knowledge-graph",
        "iat": now, "exp": now + 3600, "user_id": 1, "username": "test",
        "organization_id": 1, "source_type": "rest",
        "group_traversal_ids": [{"path": "1/", "access_levels": [50]}]
    });
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

struct ServingFixture {
    database: TestContext,
    catalog: OntologyCatalog,
    config: AppConfig,
    broker: Arc<NatsClient>,
    analytics: Arc<orbit_server::analytics::InMemoryAnalyticsTracker>,
    _nats: testcontainers::ContainerAsync<Nats>,
}

impl ServingFixture {
    async fn new() -> Self {
        let database = TestContext::new(&[]).await;
        ensure_version_table(&database.create_client())
            .await
            .unwrap();
        let (nats, nats_address) = start_nats().await;
        let mut config = AppConfig::embedded_defaults();
        config.graph = database.config.clone();
        config.schema.version_poll_interval_secs = 1;
        config.nats.url = format!("nats://{nats_address}");
        let client = timeout(WAIT_LIMIT, async {
            loop {
                if let Ok(client) = NatsClient::connect(&config.nats).await {
                    break Arc::new(client);
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("NATS must accept connections");
        let catalog = OntologyCatalog::open(client.clone()).await.unwrap();
        Self {
            database,
            catalog,
            config,
            broker: client,
            analytics: Arc::new(orbit_server::analytics::InMemoryAnalyticsTracker::new()),
            _nats: nats,
        }
    }

    async fn start(
        &self,
        embedded_version: u32,
    ) -> (
        Arc<SchemaWatcher>,
        OrbitServiceClient<tonic::transport::Channel>,
        CancellationToken,
    ) {
        let embedded = archive(embedded_version);
        let shutdown = CancellationToken::new();
        let watcher = SchemaWatcher::spawn(
            Arc::new(self.database.create_client()),
            embedded,
            self.catalog.clone(),
            &self.config,
            shutdown.clone(),
        );
        let service = OrbitServiceImpl::new(
            Arc::new(JwtValidator::new(SECRET, 0).unwrap()),
            watcher.clone(),
            &self.config.graph,
            ClusterHealthChecker::default().into_arc(),
            self.config.grpc.stream_timeout_secs,
            Arc::new(self.config.analytics.clone()),
        )
        .with_analytics(self.analytics.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_shutdown = shutdown.clone();
        let server = OrbitServiceServer::new(service);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
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
        (watcher, client, shutdown)
    }

    async fn create_projects(&self, version: u32, name: &str) {
        self.database
            .execute(&format!(
                "CREATE TABLE v{version}_gl_project (id Int64, name String, full_path String, \
             traversal_path String, _version UInt64, _deleted Bool) \
             ENGINE = ReplacingMergeTree(_version) ORDER BY (traversal_path, id)"
            ))
            .await;
        self.database.execute(&format!(
            "INSERT INTO v{version}_gl_project VALUES (1, '{name}', 'group/project', '1/', 1, false)"
        )).await;
    }
}

async fn ready_status(watcher: &Arc<SchemaWatcher>) -> StatusCode {
    create_router(watcher.clone())
        .oneshot(
            axum::http::Request::get("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn wait_for_readiness(watcher: &Arc<SchemaWatcher>, expected: StatusCode) {
    timeout(WAIT_LIMIT, async {
        while ready_status(watcher).await != expected {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("readiness must reflect the active archive");
}

async fn await_schema(client: &mut OrbitServiceClient<tonic::transport::Channel>, version: u32) {
    timeout(WAIT_LIMIT, async {
        loop {
            if let Ok(response) = client
                .get_graph_schema(authenticated(GetGraphSchemaRequest {
                    expand_nodes: vec!["Project".into()],
                    ..Default::default()
                }))
                .await
                && let Some(get_graph_schema_response::Content::Structured(schema)) =
                    response.into_inner().content
                && schema
                    .nodes
                    .iter()
                    .find(|node| node.name == "Project")
                    .is_some_and(|node| {
                        node.properties
                            .iter()
                            .any(|property| property.name == "description")
                            == (version >= EXPANDED_SCHEMA_VERSION)
                    })
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("active schema must become queryable");
}

async fn begin_project_query(
    client: &mut OrbitServiceClient<tonic::transport::Channel>,
    columns: &[&str],
) -> (
    mpsc::Sender<ExecuteQueryMessage>,
    Streaming<ExecuteQueryMessage>,
    ExecuteQueryMessage,
) {
    begin_query(client, ExecuteQueryRequest {
        query: json!({
            "query_type": "traversal",
            "nodes": [{"id": "project", "entity": "Project", "node_ids": [1], "columns": columns}],
            "limit": 10
        }).to_string(),
        ..Default::default()
    }).await
}

async fn begin_query(
    client: &mut OrbitServiceClient<tonic::transport::Channel>,
    request: ExecuteQueryRequest,
) -> (
    mpsc::Sender<ExecuteQueryMessage>,
    Streaming<ExecuteQueryMessage>,
    ExecuteQueryMessage,
) {
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Request(request)),
        })
        .await
        .unwrap();
    let mut stream = client
        .execute_query(authenticated(ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    let message = timeout(WAIT_LIMIT, stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    (sender, stream, message)
}

async fn finish_query(
    sender: mpsc::Sender<ExecuteQueryMessage>,
    mut stream: Streaming<ExecuteQueryMessage>,
    authorization_request: ExecuteQueryMessage,
) -> Value {
    let Some(execute_query_message::Content::Redaction(RedactionExchange {
        content: Some(redaction_exchange::Content::Required(required)),
    })) = authorization_request.content
    else {
        panic!("expected authorization request, got {authorization_request:?}")
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
    sender
        .send(ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Redaction(
                RedactionExchange {
                    content: Some(redaction_exchange::Content::Response(RedactionResponse {
                        result_id: required.result_id,
                        authorizations,
                    })),
                },
            )),
        })
        .await
        .unwrap();
    let message = timeout(WAIT_LIMIT, stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Some(execute_query_message::Content::Result(result)) = message.content else {
        panic!("expected query result, got {message:?}")
    };
    let Some(execute_query_result::Content::ResultJson(json)) = result.content else {
        panic!("expected JSON result, got {result:?}")
    };
    serde_json::from_str(&json).unwrap()
}

#[tokio::test]
async fn queries_keep_the_active_schema_during_backfill_and_streams_finish_after_cutover() {
    let harness = ServingFixture::new().await;
    harness.catalog.publish(&archive(1)).await.unwrap();
    harness
        .catalog
        .publish(&archive(EXPANDED_SCHEMA_VERSION))
        .await
        .unwrap();
    harness.create_projects(1, "before cutover").await;
    harness
        .create_projects(EXPANDED_SCHEMA_VERSION, "after cutover")
        .await;
    harness
        .database
        .execute(&format!(
            "ALTER TABLE v{EXPANDED_SCHEMA_VERSION}_gl_project \
             ADD COLUMN description String DEFAULT 'new project description'"
        ))
        .await;
    let graph = harness.database.create_client();
    mark_version_active(&graph, 1).await.unwrap();
    mark_version_migrating(&graph, EXPANDED_SCHEMA_VERSION)
        .await
        .unwrap();

    let (watcher, mut client, shutdown) = harness.start(EXPANDED_SCHEMA_VERSION).await;
    await_schema(&mut client, 1).await;
    assert_eq!(ready_status(&watcher).await, StatusCode::OK);

    let (_, _, rejected_query) = begin_project_query(&mut client, &["name", "description"]).await;
    let Some(execute_query_message::Content::Error(error)) = rejected_query.content else {
        panic!("expected the old ontology to reject description, got {rejected_query:?}")
    };
    assert_eq!(error.code, "compile_error");
    assert!(error.message.contains("description"), "{error:?}");

    let (sender, stream, authorization_request) = begin_project_query(&mut client, &["name"]).await;

    promote_version(&graph, EXPANDED_SCHEMA_VERSION)
        .await
        .unwrap();
    await_schema(&mut client, EXPANDED_SCHEMA_VERSION).await;
    assert_eq!(ready_status(&watcher).await, StatusCode::OK);
    assert!(!shutdown.is_cancelled());

    let old_result = finish_query(sender, stream, authorization_request).await;
    assert_eq!(old_result["nodes"][0]["name"], "before cutover");
    assert!(old_result["nodes"][0].get("description").is_none());

    let (sender, stream, authorization_request) =
        begin_project_query(&mut client, &["name", "description"]).await;
    let new_result = finish_query(sender, stream, authorization_request).await;
    assert_eq!(new_result["nodes"][0]["name"], "after cutover");
    assert_eq!(
        new_result["nodes"][0]["description"],
        "new project description"
    );

    let (sender, stream, authorization_request) =
        begin_project_query(&mut client, &["name", "description"]).await;
    promote_version(&graph, 1).await.unwrap();
    await_schema(&mut client, 1).await;
    assert_eq!(ready_status(&watcher).await, StatusCode::OK);
    assert!(!shutdown.is_cancelled());

    let in_flight_result = finish_query(sender, stream, authorization_request).await;
    assert_eq!(
        in_flight_result["nodes"][0]["description"],
        "new project description"
    );
    let (sender, stream, authorization_request) = begin_project_query(&mut client, &["name"]).await;
    let rolled_back_result = finish_query(sender, stream, authorization_request).await;
    assert_eq!(rolled_back_result["nodes"][0]["name"], "before cutover");
    let events = harness.analytics.drain();
    let versions: Vec<_> = events
        .iter()
        .map(|event| {
            event.contexts()[1].data["graph_schema_version"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(versions, ["1", "2", "2", "1"]);
    for event in &events {
        assert_eq!(
            event.contexts()[0].data["schema_version"],
            event.contexts()[1].data["graph_schema_version"]
        );
    }
    harness.database.execute("SYSTEM FLUSH LOGS").await;
    let batches = harness
        .database
        .query(
            "SELECT log_comment FROM system.query_log WHERE type = 'QueryFinish' \
         AND endsWith(query_id, '-base') ORDER BY event_time_microseconds",
        )
        .await;
    let query_versions: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            ArrowUtils::get_column_by_name::<StringArray>(batch, "log_comment")
                .unwrap()
                .iter()
                .map(|comment| {
                    let payload = base64::engine::general_purpose::STANDARD_NO_PAD
                        .decode(comment.unwrap().strip_prefix("gkg;").unwrap())
                        .unwrap();
                    let payload: Value = serde_json::from_slice(&payload).unwrap();
                    payload["versions"]["schema"].as_u64().unwrap()
                })
        })
        .collect();
    assert_eq!(query_versions, [1, 2, 2, 1]);
    shutdown.cancel();
}

#[tokio::test]
async fn missing_archive_fails_closed_and_publication_recovers_without_restart() {
    let harness = ServingFixture::new().await;
    let graph = harness.database.create_client();
    mark_version_active(&graph, 1).await.unwrap();
    let (watcher, mut client, shutdown) = harness.start(2).await;
    assert_eq!(
        ready_status(&watcher).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let error = client
        .get_graph_schema(authenticated(GetGraphSchemaRequest::default()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(
        client
            .list_tools(authenticated(ListToolsRequest::default()))
            .await
            .is_ok()
    );
    assert!(
        client
            .list_agent_commands(authenticated(ListAgentCommandsRequest::default()))
            .await
            .is_ok()
    );
    assert!(
        client
            .get_query_dsl(authenticated(GetQueryDslRequest::default()))
            .await
            .is_ok()
    );
    assert!(
        client
            .get_response_format(authenticated(GetResponseFormatRequest::default()))
            .await
            .is_ok()
    );
    assert!(
        client
            .invoke_agent_command(authenticated(InvokeAgentCommandRequest {
                command_name: "get_query_dsl".into(),
                ..Default::default()
            }))
            .await
            .is_ok()
    );
    assert_eq!(
        client
            .get_graph_schema(Request::new(GetGraphSchemaRequest::default()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    let liveness = create_router(watcher.clone())
        .oneshot(
            axum::http::Request::get("/live")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(liveness.status(), StatusCode::OK);

    harness.catalog.publish(&archive(1)).await.unwrap();
    await_schema(&mut client, 1).await;
    assert_eq!(ready_status(&watcher).await, StatusCode::OK);

    mark_version_retired(&graph, 1).await.unwrap();
    mark_version_active(&graph, 3).await.unwrap();
    wait_for_readiness(&watcher, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_eq!(
        client
            .get_graph_schema(authenticated(GetGraphSchemaRequest::default()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    harness.catalog.publish(&archive(3)).await.unwrap();
    await_schema(&mut client, 3).await;
    assert!(!shutdown.is_cancelled());

    mark_version_retired(&graph, 3).await.unwrap();
    wait_for_readiness(&watcher, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_eq!(
        client
            .get_graph_schema(authenticated(GetGraphSchemaRequest::default()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    mark_version_active(&graph, 3).await.unwrap();
    await_schema(&mut client, 3).await;
    assert_eq!(ready_status(&watcher).await, StatusCode::OK);
    shutdown.cancel();
}

#[tokio::test]
async fn idle_query_streams_cannot_hold_a_retired_schema_forever() {
    let mut harness = ServingFixture::new().await;
    harness.config.grpc.stream_timeout_secs = 1;
    mark_version_active(&harness.database.create_client(), 1)
        .await
        .unwrap();
    let (_, mut client, shutdown) = harness.start(1).await;
    await_schema(&mut client, 1).await;

    let (_sender, receiver) = mpsc::channel::<ExecuteQueryMessage>(1);
    let mut stream = client
        .execute_query(authenticated(ReceiverStream::new(receiver)))
        .await
        .unwrap()
        .into_inner();
    let response = timeout(WAIT_LIMIT, stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Some(execute_query_message::Content::Error(error)) = response.content else {
        panic!("expected timeout, got {response:?}");
    };
    assert_eq!(error.code, "timeout");
    shutdown.cancel();
}

#[tokio::test]
async fn older_binary_serves_newer_archive_and_filters_named_queries_after_rollback() {
    let fixture = ServingFixture::new().await;
    fixture
        .catalog
        .publish(&archive(EXPANDED_SCHEMA_VERSION))
        .await
        .unwrap();
    let graph = fixture.database.create_client();
    mark_version_active(&graph, EXPANDED_SCHEMA_VERSION)
        .await
        .unwrap();
    let (watcher, mut client, shutdown) = fixture.start(1).await;
    await_schema(&mut client, EXPANDED_SCHEMA_VERSION).await;

    let queries = client
        .list_named_queries(authenticated(ListNamedQueriesRequest::default()))
        .await
        .unwrap()
        .into_inner();
    assert!(
        queries
            .queries
            .iter()
            .any(|query| query.name == "recent_merges")
    );
    assert_eq!(ready_status(&watcher).await, StatusCode::OK);
    assert!(!shutdown.is_cancelled());

    promote_version(&graph, 1).await.unwrap();
    await_schema(&mut client, 1).await;
    let queries = client
        .list_named_queries(authenticated(ListNamedQueriesRequest::default()))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !queries
            .queries
            .iter()
            .any(|query| query.name == "recent_merges")
    );
    assert!(
        queries
            .queries
            .iter()
            .any(|query| query.name == "my_mrs_with_pipelines")
    );

    let (_, _, response) = begin_query(
        &mut client,
        ExecuteQueryRequest {
            query_type: QueryType::Named as i32,
            query: json!({"name": "recent_merges"}).to_string(),
            ..Default::default()
        },
    )
    .await;
    let Some(execute_query_message::Content::Error(error)) = response.content else {
        panic!("expected unavailable named query to be rejected, got {response:?}");
    };
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains("recent_merges"));
    shutdown.cancel();
}

#[tokio::test]
async fn corrupt_active_archive_fails_closed_and_repair_restores_serving() {
    let fixture = ServingFixture::new().await;
    let graph = fixture.database.create_client();
    mark_version_active(&graph, 1).await.unwrap();
    let (watcher, mut client, shutdown) = fixture.start(1).await;
    await_schema(&mut client, 1).await;

    let target = archive(EXPANDED_SCHEMA_VERSION);
    fixture
        .broker
        .kv_put(
            "orbit_ontology_archives",
            &target.schema_version().to_string(),
            bytes::Bytes::from_static(b"corrupt archive"),
            nats_client::KvPutOptions::default(),
        )
        .await
        .unwrap();
    promote_version(&graph, target.schema_version())
        .await
        .unwrap();
    wait_for_readiness(&watcher, StatusCode::SERVICE_UNAVAILABLE).await;
    assert_eq!(
        client
            .get_graph_schema(authenticated(GetGraphSchemaRequest::default()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert!(!shutdown.is_cancelled());

    fixture
        .broker
        .kv_put(
            "orbit_ontology_archives",
            &target.schema_version().to_string(),
            bytes::Bytes::copy_from_slice(target.bytes()),
            nats_client::KvPutOptions::default(),
        )
        .await
        .unwrap();
    await_schema(&mut client, target.schema_version()).await;
    assert_eq!(ready_status(&watcher).await, StatusCode::OK);
    shutdown.cancel();
}

#[tokio::test]
async fn metadata_read_failure_keeps_the_last_usable_schema() {
    let fixture = ServingFixture::new().await;
    let graph = fixture.database.create_client();
    mark_version_active(&graph, 1).await.unwrap();
    fixture.create_projects(1, "still available").await;
    let (watcher, mut client, shutdown) = fixture.start(1).await;
    await_schema(&mut client, 1).await;

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

    assert_eq!(ready_status(&watcher).await, StatusCode::OK);
    let (sender, stream, authorization_request) = begin_project_query(&mut client, &["name"]).await;
    let result = finish_query(sender, stream, authorization_request).await;
    assert_eq!(result["nodes"][0]["name"], "still available");

    fixture
        .database
        .execute("RENAME TABLE unavailable_schema_version TO gkg_schema_version")
        .await;
    mark_version_retired(&graph, 1).await.unwrap();
    wait_for_readiness(&watcher, StatusCode::SERVICE_UNAVAILABLE).await;
    assert!(!shutdown.is_cancelled());
    shutdown.cancel();
}

#[tokio::test]
async fn authorization_wait_times_out_without_emitting_query_success() {
    let mut fixture = ServingFixture::new().await;
    fixture.config.grpc.stream_timeout_secs = 1;
    fixture
        .create_projects(1, "waiting for authorization")
        .await;
    mark_version_active(&fixture.database.create_client(), 1)
        .await
        .unwrap();
    let (_, mut client, shutdown) = fixture.start(1).await;
    await_schema(&mut client, 1).await;

    let (_sender, mut stream, authorization_request) =
        begin_project_query(&mut client, &["name"]).await;
    assert!(matches!(
        authorization_request.content,
        Some(execute_query_message::Content::Redaction(_))
    ));
    let response = timeout(WAIT_LIMIT, stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Some(execute_query_message::Content::Error(error)) = response.content else {
        panic!("expected timeout while waiting for authorization, got {response:?}");
    };
    assert_eq!(error.code, "timeout");
    assert_eq!(fixture.analytics.count(), 0);
    shutdown.cancel();
}
