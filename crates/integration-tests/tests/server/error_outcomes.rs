use std::sync::Arc;
use std::time::Duration;

use clickhouse_client::ClickHouseConfigurationExt;
use integration_testkit::{
    GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, SeededColumnResolver, TestContext, load_ontology, t,
};
use jsonwebtoken::{EncodingKey, Header, encode};
use opentelemetry::global;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, MetricData, ResourceMetrics, SumDataPoint,
};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use orbit_server::active_schema::ActiveSchema;
use orbit_server::analytics::InMemoryAnalyticsTracker;
use orbit_server::auth::Claims;
use orbit_server::auth::JwtValidator;
use orbit_server::cluster_health::ClusterHealthChecker;
use orbit_server::grpc::OrbitServiceImpl;
use orbit_server::proto::execute_query_message::Content;
use orbit_server::proto::orbit_service_client::OrbitServiceClient;
use orbit_server::proto::{
    ExecuteQueryError, ExecuteQueryMessage, ExecuteQueryRequest, QueryLanguage, RedactionExchange,
    RedactionResponse, redaction_exchange,
};
use orbit_server_config::AppConfig;
use query_engine::shared::content::ColumnResolverRegistry;
use serde_json::Value;
use tokio::time::{sleep, timeout};
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::transport::Channel;

use super::schema_readiness::{QueryStream, serve_grpc};
use crate::common::DummyClaims;

const SECRET: &str = "test-secret-that-is-at-least-32-bytes-long";
const RESOLVER_CAP: usize = 2;
const ALL_FILES: [i64; 4] = [5001, 5002, 5003, 5004];

#[tokio::test]
async fn successful_query_is_a_success() {
    let mut server = Server::start().await;

    server.run(project_query(None)).await.unwrap();

    server.assert_recorded("ok", "success").await;
}

#[tokio::test]
async fn caller_without_enabled_namespaces_is_a_caller_error() {
    let mut server = Server::start().await;
    let claims = Claims {
        admin: false,
        group_traversal_ids: Vec::new(),
        ..Claims::dummy()
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap();
    let mut call = tonic::Request::new(tokio_stream::iter([ExecuteQueryMessage {
        content: Some(Content::Request(project_query(None))),
    }]));
    call.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());

    let mut responses = server
        .client
        .execute_query(call)
        .await
        .unwrap()
        .into_inner();
    let response = responses.message().await.unwrap().unwrap().content.unwrap();

    let Content::Error(error) = response else {
        panic!("expected a query error, got {response:?}");
    };
    assert_eq!(error.code, "security_error");
    server
        .assert_recorded("security_error", "caller_error")
        .await;
}

#[tokio::test]
async fn unparsable_datetime_filter_is_rejected_before_execution() {
    let mut server = Server::start().await;

    for value in ["garbage", "2026-09-30T00:00:00+00:00"] {
        let filter = format!("p.created_at >= '{value}'");
        let error = server.run(project_query(Some(&filter))).await.unwrap_err();

        assert_eq!(error.code, "compile_error");
        assert!(
            error.message.contains("is not a valid date"),
            "{}",
            error.message
        );
        server
            .assert_recorded("compile_error", "caller_error")
            .await;
    }
}

#[tokio::test]
async fn closing_the_request_stream_during_redaction_is_a_caller_error() {
    let mut server = Server::start().await;
    let mut query = server.open(project_query(None)).await;
    query.await_authorization_request().await;

    let QueryStream {
        sender, mut stream, ..
    } = query;
    drop(sender);
    let response = timeout(Duration::from_secs(10), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .content
        .unwrap();

    let Content::Error(error) = response else {
        panic!("expected a query error, got {response:?}");
    };
    assert_eq!(error.code, "streaming_error");
    server
        .assert_recorded("streaming_error", "caller_error")
        .await;
}

#[tokio::test]
async fn mismatched_redaction_response_is_a_server_error() {
    let mut server = Server::start().await;
    let mut query = server.open(project_query(None)).await;
    query.await_authorization_request().await;

    let mismatched = RedactionResponse {
        result_id: "not-the-requested-result".into(),
        authorizations: Vec::new(),
    };
    query
        .sender
        .send(ExecuteQueryMessage {
            content: Some(Content::Redaction(RedactionExchange {
                content: Some(redaction_exchange::Content::Response(mismatched)),
            })),
        })
        .await
        .unwrap();
    let error = query.finish().await.unwrap_err();

    assert_eq!(error.code, "authorization_error");
    server
        .assert_recorded("authorization_error", "server_error")
        .await;
}

#[tokio::test]
async fn dropping_the_call_during_redaction_is_a_caller_error() {
    let mut server = Server::start().await;
    let mut query = server.open(project_query(None)).await;
    query.await_authorization_request().await;

    drop(query);

    server
        .assert_recorded("streaming_error", "caller_error")
        .await;
}

#[tokio::test]
async fn content_for_more_rows_than_the_resolver_cap_is_a_caller_error() {
    let mut server = Server::start().await;

    let error = server
        .run(file_content_query(&ALL_FILES, RESOLVER_CAP + 1))
        .await
        .unwrap_err();

    assert_eq!(error.code, "compile_error");
    assert!(
        error
            .message
            .contains(&format!("at most {RESOLVER_CAP} rows")),
        "{}",
        error.message
    );
    server
        .assert_recorded("compile_error", "caller_error")
        .await;
}

#[tokio::test]
async fn content_with_a_large_limit_but_few_rows_succeeds() {
    let mut server = Server::start().await;

    server
        .run(file_content_query(&[5001], RESOLVER_CAP + 1))
        .await
        .unwrap();

    server.assert_recorded("ok", "success").await;
}

#[tokio::test]
async fn content_batch_over_the_cap_with_limit_at_the_cap_is_a_server_error() {
    let mut server = Server::start().await;

    let error = server
        .run(file_content_query(&ALL_FILES, RESOLVER_CAP))
        .await
        .unwrap_err();

    assert_eq!(error.code, "content_resolution_error");
    server
        .assert_recorded("content_resolution_error", "server_error")
        .await;
}

#[tokio::test]
async fn invalid_query_is_a_caller_error() {
    let mut server = Server::start().await;
    let query = request("MATCH (p:Project {id: 1000}) RETURN p.no_such_column LIMIT 10");

    let error = server.run(query).await.unwrap_err();

    assert_eq!(error.code, "compile_error");
    server
        .assert_recorded("compile_error", "caller_error")
        .await;
}

struct Server {
    client: OrbitServiceClient<Channel>,
    analytics: Arc<InMemoryAnalyticsTracker>,
    meter_provider: SdkMeterProvider,
    metrics: InMemoryMetricExporter,
    _graph: TestContext,
    _shutdown: DropGuard,
}

impl Server {
    async fn start() -> Self {
        let graph = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        global::set_meter_provider(meter_provider.clone());

        seed(&graph).await;

        let mut resolvers = ColumnResolverRegistry::new().with_max_batch_size(RESOLVER_CAP);
        resolvers.register("gitaly", Arc::new(SeededColumnResolver::from_yaml("{}")));
        let analytics = Arc::new(InMemoryAnalyticsTracker::new());
        let shutdown = CancellationToken::new();
        let service = OrbitServiceImpl::new(
            Arc::new(JwtValidator::new(SECRET, 0).unwrap()),
            ActiveSchema::pinned(load_ontology()),
            graph.config.build_client("graph"),
            ClusterHealthChecker::default().into_arc(),
            &AppConfig::embedded_defaults().grpc,
            Arc::new(AppConfig::embedded_defaults().analytics),
        )
        .with_resolver_registry(Arc::new(resolvers))
        .with_analytics(analytics.clone());

        Self {
            client: serve_grpc(service, shutdown.clone()).await,
            analytics,
            meter_provider,
            metrics: exporter,
            _graph: graph,
            _shutdown: shutdown.drop_guard(),
        }
    }

    async fn open(&mut self, request: ExecuteQueryRequest) -> QueryStream {
        QueryStream::open(&mut self.client, Some(request)).await
    }

    async fn run(&mut self, request: ExecuteQueryRequest) -> Result<Value, ExecuteQueryError> {
        self.open(request).await.finish().await
    }

    async fn assert_recorded(&self, status: &str, outcome: &str) {
        timeout(Duration::from_secs(10), async {
            while self.analytics.count() == 0 {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the query must be recorded");
        let events = self.analytics.drain();
        assert_eq!(events.len(), 1);
        let query = &events[0].contexts()[1].data;
        assert_eq!(
            (query["status"].as_str(), query["outcome"].as_str()),
            (Some(status), Some(outcome))
        );

        self.meter_provider.force_flush().unwrap();
        let metrics = self.metrics.get_finished_metrics().unwrap();
        assert_eq!(
            metric_labels(
                &metrics,
                "gkg.query.pipeline.queries",
                &["status", "outcome"]
            ),
            [[status, outcome]]
        );
    }
}

fn metric_labels(metrics: &[ResourceMetrics], name: &str, keys: &[&str]) -> Vec<Vec<String>> {
    let latest = metrics
        .iter()
        .rev()
        .flat_map(|rm| rm.scope_metrics())
        .flat_map(|sm| sm.metrics())
        .find(|metric| metric.name() == name);
    let Some(AggregatedMetrics::U64(MetricData::Sum(sum))) = latest.map(|m| m.data()) else {
        return Vec::new();
    };
    let label = |point: &SumDataPoint<u64>, key: &str| {
        point
            .attributes()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.to_string())
            .unwrap_or_default()
    };
    sum.data_points()
        .map(|point| keys.iter().map(|key| label(point, key)).collect())
        .collect()
}

async fn seed(graph: &TestContext) {
    graph
        .execute(&format!(
            "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
             (1000, 'Project A', 'public', '1/100/1000/'),
             (1001, 'Project B', 'public', '1/100/1001/')",
            t("gl_project")
        ))
        .await;
    graph
        .execute(&format!(
            "INSERT INTO {} (id, traversal_path, project_id, branch, path, name, extension, language) VALUES
             (5001, '1/100/1000/', 1000, 'main', 'a.rs', 'a.rs', 'rs', 'Rust'),
             (5002, '1/100/1000/', 1000, 'main', 'b.rs', 'b.rs', 'rs', 'Rust'),
             (5003, '1/100/1000/', 1000, 'main', 'c.rs', 'c.rs', 'rs', 'Rust'),
             (5004, '1/100/1000/', 1000, 'main', 'd.rs', 'd.rs', 'rs', 'Rust')",
            t("gl_file")
        ))
        .await;
    graph.optimize_all().await;
}

fn request(query: &str) -> ExecuteQueryRequest {
    ExecuteQueryRequest {
        query: query.into(),
        language: QueryLanguage::Gql as i32,
        ..Default::default()
    }
}

fn project_query(filter: Option<&str>) -> ExecuteQueryRequest {
    let filter = filter.map(|f| format!(" AND {f}")).unwrap_or_default();
    request(&format!(
        "MATCH (p:Project) WHERE p.id IN [1000, 1001]{filter} RETURN p.name LIMIT 10"
    ))
}

fn file_content_query(node_ids: &[i64], limit: usize) -> ExecuteQueryRequest {
    request(&format!(
        "MATCH (f:File) WHERE f.id IN {node_ids:?} RETURN f.path, f.content LIMIT {limit}"
    ))
}
