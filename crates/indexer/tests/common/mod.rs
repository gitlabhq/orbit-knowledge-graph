//! Shared test utilities for SDLC integration tests.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{BooleanArray, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use indexer::clickhouse::{ArrowClickHouseClient, ClickHouseConfiguration, ClickHouseDestination};
use indexer::module::{Handler, HandlerContext, Module};
use indexer::modules::SdlcModule;
use indexer::modules::sdlc::config::SdlcIndexingConfig;
use indexer::testkit::{MockLockService, MockNatsServices};
use query_engine::ParameterizedQuery;
use serde_json::Value;
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.12";
const CLICKHOUSE_HTTP_PORT: u16 = 8123;

const TEST_DATABASE: &str = "test";
const TEST_USERNAME: &str = "default";
const TEST_PASSWORD: &str = "testpass";

const MAX_CONNECTION_ATTEMPTS: u32 = 30;
const CONNECTION_RETRY_DELAY: Duration = Duration::from_millis(500);

const SIPHON_SCHEMA_SQL: &str = include_str!("../fixtures/siphon.sql");
const GRAPH_SCHEMA_SQL: &str = include_str!("../../../../fixtures/schema/graph.sql");

pub struct TestContext {
    _container: ContainerAsync<GenericImage>,
    pub config: ClickHouseConfiguration,
    url: String,
}

impl TestContext {
    pub async fn new() -> Self {
        let container = Self::start_container().await;
        let url = Self::extract_url(&container).await;

        Self::wait_for_ready(&url).await;
        Self::run_schema(&url).await;

        let config = ClickHouseConfiguration {
            database: TEST_DATABASE.to_string(),
            url: url.clone(),
            username: TEST_USERNAME.to_string(),
            password: Some(TEST_PASSWORD.to_string()),
        };

        Self {
            _container: container,
            config,
            url,
        }
    }

    pub fn create_destination(&self) -> ClickHouseDestination {
        ClickHouseDestination::new(self.config.clone()).expect("failed to create destination")
    }

    pub fn create_handler_context(&self) -> HandlerContext {
        HandlerContext::new(
            Arc::new(self.create_destination()),
            Arc::new(MockNatsServices::new()),
            Arc::new(MockLockService::new()),
        )
    }

    pub async fn query(&self, sql: &str) -> Vec<RecordBatch> {
        self.create_client()
            .query_arrow(sql)
            .await
            .expect("query failed")
    }

    pub async fn query_parameterized(&self, pq: &ParameterizedQuery) -> Vec<RecordBatch> {
        let client = self.create_client();
        let mut query = client.query(&pq.sql);

        for (name, value) in &pq.params {
            query = match value {
                Value::String(s) => query.param(name, s.as_str()),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        query.param(name, i)
                    } else if let Some(f) = n.as_f64() {
                        query.param(name, f)
                    } else {
                        query.param(name, n.to_string())
                    }
                }
                Value::Bool(b) => query.param(name, *b),
                Value::Null => query.param(name, Option::<String>::None),
                _ => query.param(name, value.to_string()),
            };
        }

        query
            .fetch_arrow()
            .await
            .expect("parameterized query failed")
    }

    pub async fn execute(&self, sql: &str) {
        self.create_client()
            .execute(sql)
            .await
            .expect("execute failed");
    }

    fn create_client(&self) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(&self.url, TEST_DATABASE, TEST_USERNAME, Some(TEST_PASSWORD))
    }

    async fn start_container() -> ContainerAsync<GenericImage> {
        let port = ContainerPort::Tcp(CLICKHOUSE_HTTP_PORT);

        GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
            .with_exposed_port(port)
            .with_env_var("CLICKHOUSE_USER", TEST_USERNAME)
            .with_env_var("CLICKHOUSE_PASSWORD", TEST_PASSWORD)
            .with_env_var("CLICKHOUSE_DB", TEST_DATABASE)
            .start()
            .await
            .expect("failed to start ClickHouse container")
    }

    async fn extract_url(container: &ContainerAsync<GenericImage>) -> String {
        let host = container
            .get_host()
            .await
            .expect("failed to get container host");

        let port = container
            .get_host_port_ipv4(ContainerPort::Tcp(CLICKHOUSE_HTTP_PORT))
            .await
            .expect("failed to get ClickHouse HTTP port");

        let host = match host.to_string().as_str() {
            "localhost" => "127.0.0.1".to_string(),
            other => other.to_string(),
        };

        format!("http://{host}:{port}")
    }

    async fn wait_for_ready(url: &str) {
        let client = ArrowClickHouseClient::new(url, "default", TEST_USERNAME, Some(TEST_PASSWORD));

        for attempt in 1..=MAX_CONNECTION_ATTEMPTS {
            if client.execute("SELECT 1").await.is_ok() {
                return;
            }
            if attempt == MAX_CONNECTION_ATTEMPTS {
                panic!("ClickHouse not ready after {MAX_CONNECTION_ATTEMPTS} attempts");
            }
            tokio::time::sleep(CONNECTION_RETRY_DELAY).await;
        }
    }

    pub async fn truncate_all_tables(&self) {
        let batches = self
            .query("SELECT name FROM system.tables WHERE database = 'test' AND engine != 'View'")
            .await;
        for batch in &batches {
            let names = get_string_column(batch, "name");
            for i in 0..batch.num_rows() {
                self.execute(&format!("TRUNCATE TABLE {}", names.value(i)))
                    .await;
            }
        }
    }

    async fn run_schema(url: &str) {
        let client =
            ArrowClickHouseClient::new(url, TEST_DATABASE, TEST_USERNAME, Some(TEST_PASSWORD));

        for schema_sql in [SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL] {
            for statement in schema_sql.split(';') {
                let statement = statement.trim();
                if statement.is_empty() {
                    continue;
                }
                client
                    .execute(statement)
                    .await
                    .unwrap_or_else(|e| panic!("schema execution failed: {e}\n{statement}"));
            }
        }
    }
}

pub fn create_user_payload(watermark: DateTime<Utc>) -> String {
    serde_json::json!({
        "watermark": watermark.to_rfc3339()
    })
    .to_string()
}

pub fn create_namespace_payload(
    organization: i64,
    namespace: i64,
    watermark: DateTime<Utc>,
) -> String {
    serde_json::json!({
        "organization": organization,
        "namespace": namespace,
        "watermark": watermark.to_rfc3339()
    })
    .to_string()
}

/// Default watermark used across namespace tests.
pub fn default_test_watermark() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

/// Get the namespace handler from an SdlcModule.
pub async fn get_namespace_handler(context: &TestContext) -> Box<dyn Handler> {
    let sdlc_config = SdlcIndexingConfig {
        datalake_batch_size: 1,
        ..Default::default()
    };
    let sdlc_module = SdlcModule::new(&context.config, &context.config, &sdlc_config)
        .await
        .expect("failed to create SDLC module");
    let handlers = sdlc_module.handlers();
    handlers
        .into_iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found")
}

/// Get the global handler from an SdlcModule.
pub async fn get_global_handler(context: &TestContext) -> Box<dyn Handler> {
    let sdlc_config = SdlcIndexingConfig {
        datalake_batch_size: 1,
        ..Default::default()
    };
    let sdlc_module = SdlcModule::new(&context.config, &context.config, &sdlc_config)
        .await
        .expect("failed to create SDLC module");
    let handlers = sdlc_module.handlers();
    handlers
        .into_iter()
        .find(|h| h.name() == "global-handler")
        .expect("global-handler not found")
}

/// Run a subtest with automatic table truncation afterward.
#[allow(unused_macros)]
macro_rules! run_subtest {
    ($name:expr, $context:expr, $test_fn:expr) => {{
        eprintln!("--- {}", $name);
        $test_fn($context).await;
        $context.truncate_all_tables().await;
    }};
}

#[allow(unused_imports)]
pub(crate) use run_subtest;

/// Extract a string column from a RecordBatch.
pub fn get_string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name} column should exist"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{name} should be StringArray"))
}

/// Extract a uint64 column from a RecordBatch.
pub fn get_uint64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a UInt64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name} column should exist"))
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap_or_else(|| panic!("{name} should be UInt64Array"))
}

/// Extract an int64 column from a RecordBatch.
pub fn get_int64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name} column should exist"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{name} should be Int64Array"))
}

/// Extract a boolean column from a RecordBatch.
pub fn get_boolean_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a BooleanArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("{name} column should exist"))
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap_or_else(|| panic!("{name} should be BooleanArray"))
}

/// Query edges and assert a specific count.
pub async fn assert_edge_count(
    context: &TestContext,
    relationship_kind: &str,
    source_kind: &str,
    target_kind: &str,
    expected_count: usize,
) {
    let query = format!(
        "SELECT source_id, target_id FROM gl_edge FINAL WHERE relationship_kind = '{relationship_kind}' \
         AND source_kind = '{source_kind}' AND target_kind = '{target_kind}'"
    );
    let result = context.query(&query).await;
    assert!(
        !result.is_empty(),
        "{relationship_kind} edges from {source_kind} to {target_kind} should exist"
    );
    assert_eq!(
        result[0].num_rows(),
        expected_count,
        "expected {expected_count} {relationship_kind} edges from {source_kind} to {target_kind}"
    );
}

/// Assert that edges matching a traversal_path filter have the expected count.
///
/// Unlike `assert_edges_have_traversal_path` which checks that *all* edges of a type share
/// one path, this filters by traversal_path and checks the count — useful when edges of the
/// same type have different paths (e.g., entities in different namespaces).
pub async fn assert_edge_count_for_traversal_path(
    context: &TestContext,
    relationship_kind: &str,
    source_kind: &str,
    target_kind: &str,
    traversal_path: &str,
    expected_count: usize,
) {
    let query = format!(
        "SELECT 1 FROM gl_edge FINAL WHERE relationship_kind = '{relationship_kind}' \
         AND source_kind = '{source_kind}' AND target_kind = '{target_kind}' \
         AND traversal_path = '{traversal_path}'"
    );
    let result = context.query(&query).await;
    let actual_count = result.first().map_or(0, |b| b.num_rows());
    assert_eq!(
        actual_count, expected_count,
        "expected {expected_count} {relationship_kind} edges ({source_kind} → {target_kind}) \
         with traversal_path '{traversal_path}', got {actual_count}"
    );
}

/// Assert that edges of a given type exist with the expected count and traversal_path.
pub async fn assert_edges_have_traversal_path(
    context: &TestContext,
    relationship_kind: &str,
    source_kind: &str,
    target_kind: &str,
    expected_traversal_path: &str,
    expected_count: usize,
) {
    let query = format!(
        "SELECT traversal_path FROM gl_edge FINAL WHERE relationship_kind = '{relationship_kind}' \
         AND source_kind = '{source_kind}' AND target_kind = '{target_kind}'"
    );
    let result = context.query(&query).await;
    assert!(
        !result.is_empty(),
        "{relationship_kind} edges from {source_kind} to {target_kind} should exist"
    );
    let batch = &result[0];
    assert_eq!(
        batch.num_rows(),
        expected_count,
        "expected {expected_count} {relationship_kind} edges from {source_kind} to {target_kind}"
    );
    let paths = get_string_column(batch, "traversal_path");
    for i in 0..batch.num_rows() {
        assert_eq!(
            paths.value(i),
            expected_traversal_path,
            "{relationship_kind} edge row {i} should have traversal_path '{expected_traversal_path}'"
        );
    }
}
