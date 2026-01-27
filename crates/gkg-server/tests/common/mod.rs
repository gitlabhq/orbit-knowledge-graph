//! Shared test utilities for SDLC integration tests.

#![allow(dead_code)]

use std::sync::Arc;

use arrow::array::BinaryArray;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use clickhouse_arrow::{ArrowClient, ClientBuilder};
use etl_engine::clickhouse::{ClickHouseConfiguration, ClickHouseDestination};
use etl_engine::module::HandlerContext;
use etl_engine::testkit::{MockMetricCollector, MockNatsServices};
use futures::StreamExt;
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;

pub const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
pub const CLICKHOUSE_TAG: &str = "25.11";
pub const TEST_USERNAME: &str = "default";
pub const TEST_PASSWORD: &str = "testpass";
pub const TEST_DATABASE: &str = "test";

const MAX_CONNECTION_ATTEMPTS: u32 = 30;
const CONNECTION_RETRY_DELAY_MS: u64 = 500;

const SCHEMA_SQL: &str = include_str!("../fixtures/schema.sql");

pub struct TestContext {
    _container: testcontainers::ContainerAsync<GenericImage>,
    pub config: ClickHouseConfiguration,
    host: String,
    port: u16,
}

impl TestContext {
    pub async fn new() -> Self {
        let (container, host, port) = start_clickhouse_container().await;
        setup_database(&host, port).await;
        let config = create_config(&host, port);

        Self {
            _container: container,
            config,
            host,
            port,
        }
    }

    pub fn create_destination(&self) -> ClickHouseDestination {
        ClickHouseDestination::new(self.config.clone()).expect("failed to create destination")
    }

    pub async fn create_client(&self) -> ArrowClient {
        ClientBuilder::new()
            .with_endpoint(format!("{}:{}", self.host, self.port))
            .with_database(TEST_DATABASE)
            .with_username(TEST_USERNAME)
            .with_password(TEST_PASSWORD)
            .build_arrow()
            .await
            .expect("failed to connect")
    }

    pub async fn query(&self, sql: &str) -> Vec<RecordBatch> {
        let client = self.create_client().await;
        client
            .query(sql, None)
            .await
            .expect("query failed")
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to collect results")
    }

    pub async fn execute(&self, sql: &str) {
        let client = self.create_client().await;
        client.execute(sql, None).await.expect("execute failed");
    }

    pub async fn cleanup(self) {
        self._container.stop().await.expect("failed to stop container");
    }
}

async fn start_clickhouse_container() -> (testcontainers::ContainerAsync<GenericImage>, String, u16)
{
    let native_port = ContainerPort::Tcp(9000);

    let container = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
        .with_exposed_port(native_port)
        .with_env_var("CLICKHOUSE_USER", TEST_USERNAME)
        .with_env_var("CLICKHOUSE_PASSWORD", TEST_PASSWORD)
        .with_env_var("CLICKHOUSE_DB", TEST_DATABASE)
        .start()
        .await
        .expect("failed to start ClickHouse container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");

    let port = container
        .get_host_port_ipv4(native_port)
        .await
        .expect("failed to get ClickHouse native port");

    let host = if host.to_string() == "localhost" {
        "127.0.0.1".to_string()
    } else {
        host.to_string()
    };

    (container, host, port)
}

async fn setup_database(host: &str, port: u16) {
    let mut attempts = 0;
    let client = loop {
        match ClientBuilder::new()
            .with_endpoint(format!("{host}:{port}"))
            .with_username(TEST_USERNAME)
            .with_password(TEST_PASSWORD)
            .build_arrow()
            .await
        {
            Ok(client) => break client,
            Err(_) if attempts < MAX_CONNECTION_ATTEMPTS => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(CONNECTION_RETRY_DELAY_MS))
                    .await;
            }
            Err(e) => panic!("failed to connect to ClickHouse after {attempts} attempts: {e}"),
        }
    };

    for statement in SCHEMA_SQL.split(';') {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }
        client
            .execute(statement, None)
            .await
            .unwrap_or_else(|e| panic!("failed to execute schema statement: {e}\n{statement}"));
    }
}

fn create_config(host: &str, port: u16) -> ClickHouseConfiguration {
    ClickHouseConfiguration {
        database: TEST_DATABASE.to_string(),
        url: format!("{host}:{port}"),
        username: TEST_USERNAME.to_string(),
        password: Some(TEST_PASSWORD.to_string()),
    }
}

pub fn create_handler_context(destination: Arc<ClickHouseDestination>) -> HandlerContext {
    HandlerContext::new(
        destination,
        Arc::new(MockMetricCollector::new()),
        Arc::new(MockNatsServices::new()),
    )
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

pub fn binary_as_str(array: &BinaryArray, index: usize) -> &str {
    std::str::from_utf8(array.value(index)).expect("invalid UTF-8")
}
