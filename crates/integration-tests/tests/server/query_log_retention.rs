use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Once;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use clickhouse_client::FromArrowColumn;
use gkg_server::auth::JwtValidator;
use gkg_server::cluster_health::ClusterHealthChecker;
use gkg_server::grpc::GrpcServer;
use gkg_server::proto::execute_query_message::Content;
use gkg_server::proto::knowledge_graph_service_client::KnowledgeGraphServiceClient;
use gkg_server::proto::{
    ExecuteQueryMessage, ExecuteQueryRequest, RedactionExchange, RedactionResponse,
    ResourceAuthorization, redaction_exchange,
};
use gkg_server_config::{AnalyticsConfig, GrpcConfig};
use indexer::schema::migration::create_unversioned_tables;
use indexer::schema::version::{SCHEMA_VERSION, ensure_version_table, write_schema_version};
use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, load_ontology, load_seed,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use labkit::correlation::CorrelationCaptureLayer;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::common::DummyClaims;

const RAW_JWT_SECRET: &[u8] = b"test-secret-that-is-at-least-32-bytes-long";

static SUBSCRIBER: Once = Once::new();

// The pipeline reads the correlation id via `labkit::correlation::current()`, a
// span-tree walk that only resolves when a `CorrelationCaptureLayer` is installed
// process-wide. The server task is spawned, so a thread-local subscriber would not
// cover it; install a global one once.
fn install_correlation_subscriber() {
    SUBSCRIBER.call_once(|| {
        tracing_subscriber::registry()
            .with(CorrelationCaptureLayer::new())
            .init();
    });
}

fn jwt() -> String {
    encode(
        &Header::new(Algorithm::HS256),
        &gkg_server::auth::Claims::dummy(),
        &EncodingKey::from_secret(RAW_JWT_SECRET),
    )
    .unwrap()
}

async fn start_server(config: &gkg_server_config::ClickHouseConfiguration) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let validator = Arc::new(JwtValidator::new(&STANDARD.encode(RAW_JWT_SECRET), 0).unwrap());
    // The server queries the version-prefixed graph tables, mirroring how `main`
    // wraps the ontology before wiring the pipeline.
    let ontology = Arc::new(load_ontology());
    let server = GrpcServer::new(
        addr,
        validator,
        ontology,
        config,
        ClusterHealthChecker::default().into_arc(),
        None,
        GrpcConfig::default(),
        Arc::new(AnalyticsConfig::default()),
    );
    tokio::spawn(server.run());
    addr
}

async fn connect(addr: SocketAddr) -> KnowledgeGraphServiceClient<tonic::transport::Channel> {
    let endpoint = Endpoint::from_shared(format!("http://{addr}")).unwrap();
    for i in 0..20 {
        if let Ok(channel) = endpoint.connect().await {
            return KnowledgeGraphServiceClient::new(channel);
        }
        if i == 19 {
            panic!("server did not come up");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    unreachable!()
}

#[tokio::test]
async fn executed_query_is_captured_in_retention_table() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    install_correlation_subscriber();

    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, &GRAPH_SCHEMA_SQL]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_schema_version(&client, *SCHEMA_VERSION)
        .await
        .unwrap();
    // Unversioned objects are never version-prefixed, so they take the plain
    // embedded ontology, not the prefixed one the server queries through.
    // The insert-trigger MV only captures query_log rows written after it exists.
    create_unversioned_tables(&client, &ontology::Ontology::load_embedded().unwrap())
        .await
        .unwrap();
    load_seed(&ctx, "data_correctness").await;
    ctx.optimize_all().await;

    let addr = start_server(&ctx.config).await;
    let mut grpc = connect(addr).await;

    let request = ExecuteQueryMessage {
        content: Some(Content::Request(ExecuteQueryRequest {
            query: r#"{
                "query_type": "traversal",
                "nodes": [
                    {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]}
                ],
                "order_by": "u.id",
                "limit": 10
            }"#
            .to_string(),
            format: 0,
            query_type: 0,
        })),
    };

    // A bidirectional request stream so the client can answer the server's
    // redaction round-trip (authorize everything) before the result is sent.
    let (req_tx, req_rx) = tokio::sync::mpsc::channel::<ExecuteQueryMessage>(4);
    req_tx.send(request).await.unwrap();

    let mut streaming = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(req_rx));
    streaming.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {}", jwt())).unwrap(),
    );

    let mut responses = grpc.execute_query(streaming).await.unwrap().into_inner();

    let mut got_result = false;
    while let Some(message) = responses.message().await.unwrap() {
        match message.content {
            Some(Content::Result(_)) => got_result = true,
            Some(Content::Error(e)) => panic!("query returned error: {e:?}"),
            Some(Content::Redaction(RedactionExchange {
                content: Some(redaction_exchange::Content::Required(required)),
            })) => {
                let authorizations = required
                    .resources
                    .iter()
                    .map(|r| ResourceAuthorization {
                        resource_type: r.resource_type.clone(),
                        authorized: r.resource_ids.iter().map(|id| (*id, true)).collect(),
                    })
                    .collect();
                req_tx
                    .send(ExecuteQueryMessage {
                        content: Some(Content::Redaction(RedactionExchange {
                            content: Some(redaction_exchange::Content::Response(
                                RedactionResponse {
                                    result_id: required.result_id,
                                    authorizations,
                                },
                            )),
                        })),
                    })
                    .await
                    .unwrap();
            }
            _ => {}
        }
    }
    assert!(got_result, "expected a query result message");

    ctx.execute("SYSTEM FLUSH LOGS").await;

    // The server generates a correlation id per request (propagation is off by
    // default), so assert the retention table captured the same correlated base
    // query system.query_log recorded, rather than a fixed id. The base query
    // carries no stage suffix (`gkg;correlation_id=<id>`); path/hydration
    // sub-queries add `:` suffixes, which this filter excludes.
    let logged = ctx
        .query(
            "SELECT log_comment FROM system.query_log \
             WHERE type = 'QueryFinish' AND log_comment LIKE 'gkg;correlation_id=%' LIMIT 1",
        )
        .await;
    let correlated = String::extract_column(&logged, 0)
        .unwrap()
        .first()
        .cloned()
        .expect("the base query should be logged with a gkg correlation id");

    let captured = ctx
        .query(&format!(
            "SELECT toInt64(count()) FROM query_log_retention WHERE log_comment = '{correlated}'"
        ))
        .await;
    assert_eq!(
        i64::extract_column(&captured, 0).unwrap().first().copied(),
        Some(1),
        "the executed base query ({correlated}) should be captured in the retention table"
    );

    // A non-GKG query must be excluded by the view's `log_comment LIKE 'gkg%'` filter.
    ctx.execute("SELECT 2 SETTINGS log_comment = 'not-gkg'")
        .await;
    ctx.execute("SYSTEM FLUSH LOGS").await;
    let untagged = ctx
        .query("SELECT toInt64(count()) FROM query_log_retention WHERE log_comment = 'not-gkg'")
        .await;
    assert_eq!(
        i64::extract_column(&untagged, 0).unwrap().first().copied(),
        Some(0),
        "a non-GKG query must not land in the retention table"
    );
}
