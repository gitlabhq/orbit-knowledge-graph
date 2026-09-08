use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::StatusCode;
use integration_testkit::TestContext;
use jsonwebtoken::{EncodingKey, Header, encode};
use nats_client::NatsClient;
use ontology::archive::OntologyArchive;
use orbit_migrations::catalog::{CatalogError, OntologyCatalog};
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
use orbit_utils::yaml;
use serde_json::{Value, json};
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Streaming};
use tower::ServiceExt;

const SECRET: &str = "test-secret-that-is-at-least-32-bytes-long";
const WAIT_LIMIT: Duration = Duration::from_secs(30);
const PROJECT_DESCRIPTION_VERSION: u32 = 2;

fn archive(version: u32) -> OntologyArchive {
    let mut sources = ontology::migrations::embedded_sources();
    let mut schema: Value = yaml::from_str(&sources["schema.yaml"]).unwrap();
    schema["schema_version"] = json!(version.to_string());
    sources.insert("schema.yaml".into(), yaml::to_string(&schema).unwrap());

    if version < PROJECT_DESCRIPTION_VERSION {
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

struct Harness {
    database: TestContext,
    catalog: OntologyCatalog,
    config: AppConfig,
    _nats: testcontainers::ContainerAsync<Nats>,
}

impl Harness {
    async fn new() -> Self {
        let database = TestContext::new(&[]).await;
        ensure_version_table(&database.create_client())
            .await
            .unwrap();
        let nats = Nats::default()
            .with_tag("2.11-alpine")
            .with_cmd(&NatsServerCmd::default().with_jetstream())
            .start()
            .await
            .unwrap();
        let mut config: AppConfig = serde_json::from_str("{}").unwrap();
        config.graph = database.config.clone();
        config.schema.version_poll_interval_secs = 1;
        config.nats.url = format!(
            "nats://{}:{}",
            nats.get_host().await.unwrap(),
            nats.get_host_port_ipv4(4222).await.unwrap()
        );
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
        let catalog = OntologyCatalog::open(client, &config.graph.database)
            .await
            .unwrap();
        Self {
            database,
            catalog,
            config,
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
        let service = OrbitServiceImpl::new(
            Arc::new(JwtValidator::new(SECRET, 0).unwrap()),
            Arc::new(embedded.load_ontology().unwrap()),
            &self.config.graph,
            ClusterHealthChecker::default().into_arc(),
            self.config.grpc.stream_timeout_secs,
            Arc::new(self.config.analytics.clone()),
        );
        let shutdown = CancellationToken::new();
        let watcher = SchemaWatcher::spawn(
            service,
            embedded,
            self.catalog.clone(),
            &self.config,
            shutdown.clone(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_shutdown = shutdown.clone();
        let server = OrbitServiceServer::from_arc(watcher.clone());
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

async fn await_schema(client: &mut OrbitServiceClient<tonic::transport::Channel>, version: u32) {
    timeout(WAIT_LIMIT, async {
        loop {
            if let Ok(response) = client
                .get_graph_schema(authenticated(GetGraphSchemaRequest::default()))
                .await
                && let Some(get_graph_schema_response::Content::Structured(schema)) =
                    response.into_inner().content
                && schema.schema_version == version.to_string()
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
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(ExecuteQueryMessage {
            content: Some(execute_query_message::Content::Request(
                ExecuteQueryRequest {
                    query: json!({
                        "query_type": "traversal",
                        "nodes": [{"id": "project", "entity": "Project", "node_ids": [1], "columns": columns}],
                        "limit": 10
                    }).to_string(),
                    ..Default::default()
                },
            )),
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
    let harness = Harness::new().await;
    harness.catalog.publish(&archive(1)).await.unwrap();
    harness
        .catalog
        .publish(&archive(PROJECT_DESCRIPTION_VERSION))
        .await
        .unwrap();
    harness.create_projects(1, "before cutover").await;
    harness
        .create_projects(PROJECT_DESCRIPTION_VERSION, "after cutover")
        .await;
    harness
        .database
        .execute(&format!(
            "ALTER TABLE v{PROJECT_DESCRIPTION_VERSION}_gl_project \
             ADD COLUMN description String DEFAULT 'new project description'"
        ))
        .await;
    let graph = harness.database.create_client();
    mark_version_active(&graph, 1).await.unwrap();
    mark_version_migrating(&graph, PROJECT_DESCRIPTION_VERSION)
        .await
        .unwrap();

    let (watcher, mut client, shutdown) = harness.start(PROJECT_DESCRIPTION_VERSION).await;
    await_schema(&mut client, 1).await;
    assert_eq!(ready_status(&watcher).await, StatusCode::OK);

    let (_, _, rejected_query) = begin_project_query(&mut client, &["name", "description"]).await;
    let Some(execute_query_message::Content::Error(error)) = rejected_query.content else {
        panic!("expected the old ontology to reject description, got {rejected_query:?}")
    };
    assert_eq!(error.code, "compile_error");
    assert!(error.message.contains("description"), "{error:?}");

    let (sender, stream, authorization_request) = begin_project_query(&mut client, &["name"]).await;

    promote_version(&graph, PROJECT_DESCRIPTION_VERSION)
        .await
        .unwrap();
    await_schema(&mut client, PROJECT_DESCRIPTION_VERSION).await;
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
    shutdown.cancel();
}

#[tokio::test]
async fn missing_archive_fails_closed_and_publication_recovers_without_restart() {
    let harness = Harness::new().await;
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

    harness.catalog.publish(&archive(1)).await.unwrap();
    await_schema(&mut client, 1).await;
    assert_eq!(ready_status(&watcher).await, StatusCode::OK);

    mark_version_retired(&graph, 1).await.unwrap();
    mark_version_active(&graph, 3).await.unwrap();
    timeout(WAIT_LIMIT, async {
        while ready_status(&watcher).await == StatusCode::OK {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
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
    shutdown.cancel();
}

#[tokio::test]
async fn catalog_survives_reconnection_rejects_rewrites_and_isolates_graph_databases() {
    let harness = Harness::new().await;
    let original = archive(1);
    harness.catalog.publish(&original).await.unwrap();
    harness.catalog.publish(&original).await.unwrap();
    let reconnected = Arc::new(NatsClient::connect(&harness.config.nats).await.unwrap());
    let catalog = OntologyCatalog::open(reconnected.clone(), &harness.config.graph.database)
        .await
        .unwrap();
    assert_eq!(catalog.load(1).await.unwrap().bytes(), original.bytes());

    let conflicting =
        OntologyArchive::from_sources(1, &ontology::migrations::embedded_sources()).unwrap();
    assert!(matches!(
        catalog.publish(&conflicting).await,
        Err(CatalogError::Conflict(1))
    ));
    assert_eq!(catalog.load(1).await.unwrap().bytes(), original.bytes());
    let other_database = OntologyCatalog::open(reconnected, "another_graph")
        .await
        .unwrap();
    assert!(matches!(
        other_database.load(1).await,
        Err(CatalogError::Missing(1))
    ));
}

#[tokio::test]
async fn idle_query_streams_cannot_hold_a_retired_schema_forever() {
    let mut harness = Harness::new().await;
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
