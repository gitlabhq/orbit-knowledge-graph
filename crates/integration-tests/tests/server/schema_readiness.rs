use std::sync::Arc;
use std::time::Duration;

use crate::common::DummyClaims;
use crate::indexer::common::dispatch::start_nats;
use axum::body::Body;
use axum::http::StatusCode;
use integration_testkit::TestContext;
use jsonwebtoken::{EncodingKey, Header, encode};
use nats_client::{KvPutOptions, NatsClient};
use ontology::archive::OntologyArchive;
use orbit_migrations::catalog::{ONTOLOGY_ARCHIVES_BUCKET, OntologyCatalog};
use orbit_migrations::version::{
    ensure_version_table, mark_version_migrating, mark_version_retired, promote_version,
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

fn archive(version: u32) -> OntologyArchive {
    let mut sources = ontology::migrations::embedded_sources();
    if version == 1 {
        for (path, property) in [
            ("nodes/core/project.yaml", "description"),
            ("nodes/code_review/merge_request.yaml", "merged_at"),
        ] {
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
        config.schema.version_poll_interval_secs = 1;
        config.nats.url = format!("nats://{address}");
        let broker = Arc::new(NatsClient::connect(&config.nats).await.unwrap());
        let catalog = OntologyCatalog::open(broker.clone()).await.unwrap();
        let shutdown = CancellationToken::new();
        let watcher = SchemaWatcher::spawn(
            Arc::new(database.create_client()),
            archive(embedded_version),
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

    async fn ready_status(&self) -> StatusCode {
        self.router
            .clone()
            .oneshot(
                axum::http::Request::get("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    async fn await_schema(&mut self, version: Option<u32>) {
        timeout(WAIT_LIMIT, async {
            loop {
                let response = self
                    .client
                    .get_graph_schema(authenticated(GetGraphSchemaRequest {
                        expand_nodes: vec!["Project".into()],
                        ..Default::default()
                    }))
                    .await;
                let description_available = match response {
                    Ok(response) => {
                        let Some(get_graph_schema_response::Content::Structured(schema)) =
                            response.into_inner().content
                        else {
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
                    Err(error) if error.code() == tonic::Code::Unavailable => None,
                    Err(error) => panic!("unexpected schema error: {error}"),
                };
                if description_available == version.map(|version| version > 1) {
                    return;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("serving must reflect the active archive");
        let command = self
            .client
            .invoke_agent_command(authenticated(InvokeAgentCommandRequest {
                command_name: "get_graph_schema".into(),
                parameters_json: json!({"format": "raw", "expand_nodes": ["Project"]}).to_string(),
            }))
            .await;
        if let Some(version) = version {
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
            let has_description = project["props"]
                .as_array()
                .unwrap()
                .iter()
                .any(|property| property.as_str().unwrap().starts_with("description:"));
            assert_eq!(has_description, version > 1);
        } else {
            assert_eq!(command.unwrap_err().code(), tonic::Code::Unavailable);
        }
        let expected = if version.is_some() {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        assert_eq!(self.ready_status().await, expected);
    }

    async fn create_projects(&self, version: u32) {
        let description = if version > 1 {
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
async fn promotion_and_rollback_preserve_in_flight_queries_and_filter_named_queries() {
    let mut fixture = ServingFixture::start(2, WAIT_LIMIT).await;
    fixture.catalog.publish(&archive(1)).await.unwrap();
    for version in [1, 2] {
        fixture.create_projects(version).await;
    }
    let graph = fixture.database.create_client();
    promote_version(&graph, 1).await.unwrap();
    mark_version_migrating(&graph, 2).await.unwrap();
    fixture.await_schema(Some(1)).await;

    for (current, next) in [(1, 2), (2, 1)] {
        let columns = if current == 1 {
            vec!["name"]
        } else {
            vec!["name", "description"]
        };
        let mut query = QueryStream::open(&mut fixture.client, Some(project_query(&columns))).await;
        let authorization = query.receive().await;
        promote_version(&graph, next).await.unwrap();
        fixture.await_schema(Some(next)).await;
        let result = query.authorize_and_finish(authorization).await;
        assert_eq!(result["nodes"][0]["name"], format!("project v{current}"));
        assert_eq!(
            result["nodes"][0]["description"],
            if current == 1 {
                Value::Null
            } else {
                json!("new property")
            }
        );

        let queries = fixture
            .client
            .list_named_queries(authenticated(ListNamedQueriesRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            queries
                .queries
                .iter()
                .any(|query| query.name == "recent_merges"),
            next == 2
        );
        assert!(
            queries
                .queries
                .iter()
                .any(|query| query.name == "my_mrs_with_pipelines")
        );
    }

    let mut query = QueryStream::open(&mut fixture.client, Some(project_query(&["name"]))).await;
    let authorization = query.receive().await;
    assert_eq!(
        query.authorize_and_finish(authorization).await["nodes"][0]["name"],
        "project v1"
    );
    let versions: Vec<_> = fixture
        .analytics
        .drain()
        .iter()
        .map(|event| event.contexts()[1].data["graph_schema_version"].clone())
        .collect();
    assert_eq!(versions, [json!("1"), json!("2"), json!("1")]);

    let mut query = QueryStream::open(
        &mut fixture.client,
        Some(ExecuteQueryRequest {
            query_type: QueryType::Named as i32,
            query: json!({"name": "recent_merges"}).to_string(),
            ..Default::default()
        }),
    )
    .await;
    let response = query.receive().await;
    let Content::Error(error) = response else {
        panic!("expected unavailable named query to be rejected, got {response:?}");
    };
    assert_eq!(error.code, "invalid_request");
    assert!(error.message.contains("recent_merges"));
}

#[tokio::test]
async fn missing_and_corrupt_archives_fail_closed_and_recover_without_restart() {
    let mut fixture = ServingFixture::start(1, WAIT_LIMIT).await;
    let graph = fixture.database.create_client();
    let corrupt_archive = bytes::Bytes::from_static(b"corrupt archive");

    for (version, corrupt_contents) in [(2, None), (3, Some(corrupt_archive))] {
        let key = version.to_string();
        if let Some(contents) = corrupt_contents {
            fixture
                .broker
                .kv_put(
                    ONTOLOGY_ARCHIVES_BUCKET,
                    &key,
                    contents,
                    KvPutOptions::default(),
                )
                .await
                .unwrap();
        }
        promote_version(&graph, version).await.unwrap();
        fixture.await_schema(None).await;
        fixture
            .client
            .list_tools(authenticated(ListToolsRequest::default()))
            .await
            .unwrap();

        let restored_contents = bytes::Bytes::copy_from_slice(archive(version).bytes());
        fixture
            .broker
            .kv_put(
                ONTOLOGY_ARCHIVES_BUCKET,
                &key,
                restored_contents,
                KvPutOptions::default(),
            )
            .await
            .unwrap();
        fixture.await_schema(Some(version)).await;
    }
    mark_version_retired(&graph, 3).await.unwrap();
    fixture.await_schema(None).await;
    promote_version(&graph, 3).await.unwrap();
    fixture.await_schema(Some(3)).await;
}

#[tokio::test]
async fn metadata_read_failure_keeps_the_last_usable_schema() {
    let mut fixture = ServingFixture::start(1, WAIT_LIMIT).await;
    fixture.create_projects(1).await;
    promote_version(&fixture.database.create_client(), 1)
        .await
        .unwrap();
    fixture.await_schema(Some(1)).await;
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

    assert_eq!(fixture.ready_status().await, StatusCode::OK);
    let mut query = QueryStream::open(&mut fixture.client, Some(project_query(&["name"]))).await;
    let authorization = query.receive().await;
    assert_eq!(
        query.authorize_and_finish(authorization).await["nodes"][0]["name"],
        "project v1"
    );
}

#[tokio::test]
async fn idle_and_authorization_blocked_streams_time_out_without_query_success() {
    let mut fixture = ServingFixture::start(1, Duration::from_secs(1)).await;
    fixture.create_projects(1).await;
    promote_version(&fixture.database.create_client(), 1)
        .await
        .unwrap();
    fixture.await_schema(Some(1)).await;

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
