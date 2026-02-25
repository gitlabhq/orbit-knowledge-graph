//! Integration tests for the ClickHouse destination.
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use indexer::clickhouse::{ArrowClickHouseClient, ClickHouseConfiguration, ClickHouseDestination};
use indexer::destination::Destination;
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.12";
const TEST_USERNAME: &str = "default";
const TEST_PASSWORD: &str = "testpass";
const TEST_DATABASE: &str = "test";
const TEST_TABLE: &str = "test_table";

const MAX_CONNECTION_ATTEMPTS: u32 = 30;
const CONNECTION_RETRY_DELAY_MS: u64 = 500;

struct TestContext {
    _container: testcontainers::ContainerAsync<GenericImage>,
    destination: ClickHouseDestination,
    host: String,
    port: u16,
}

impl TestContext {
    async fn new() -> Self {
        let (container, host, port) = start_clickhouse_container().await;
        setup_database(&host, port).await;
        let config = create_config(&host, port);
        let destination = ClickHouseDestination::new(config).expect("failed to create destination");

        Self {
            _container: container,
            destination,
            host,
            port,
        }
    }

    fn create_client(&self) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            &format!("http://{}:{}", self.host, self.port),
            TEST_DATABASE,
            TEST_USERNAME,
            Some(TEST_PASSWORD),
        )
    }

    async fn query(&self, sql: &str) -> Vec<RecordBatch> {
        let client = self.create_client();
        client.query_arrow(sql).await.expect("query failed")
    }
}

async fn start_clickhouse_container() -> (testcontainers::ContainerAsync<GenericImage>, String, u16)
{
    let http_port = ContainerPort::Tcp(8123);

    let container = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
        .with_exposed_port(http_port)
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
        .get_host_port_ipv4(http_port)
        .await
        .expect("failed to get ClickHouse HTTP port");

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
        let client = ArrowClickHouseClient::new(
            &format!("http://{host}:{port}"),
            "default",
            TEST_USERNAME,
            Some(TEST_PASSWORD),
        );

        match client.execute("SELECT 1").await {
            Ok(_) => break client,
            Err(_) if attempts < MAX_CONNECTION_ATTEMPTS => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(CONNECTION_RETRY_DELAY_MS))
                    .await;
            }
            Err(e) => panic!("failed to connect to ClickHouse after {attempts} attempts: {e}"),
        }
    };

    client
        .execute(&format!(
            "CREATE TABLE IF NOT EXISTS {TEST_DATABASE}.{TEST_TABLE} (
                id Int32,
                name String
            ) ENGINE = MergeTree() ORDER BY id"
        ))
        .await
        .expect("failed to create table");
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn create_test_batch() -> RecordBatch {
    let ids = Int32Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec!["alice", "bob", "charlie"]);

    RecordBatch::try_new(test_schema(), vec![Arc::new(ids), Arc::new(names)])
        .expect("failed to create record batch")
}

fn create_test_batch_with_data(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    let ids = Int32Array::from(ids);
    let names = StringArray::from(names);

    RecordBatch::try_new(test_schema(), vec![Arc::new(ids), Arc::new(names)])
        .expect("failed to create record batch")
}

fn create_config(host: &str, port: u16) -> ClickHouseConfiguration {
    ClickHouseConfiguration {
        database: TEST_DATABASE.to_string(),
        url: format!("http://{host}:{port}"),
        username: TEST_USERNAME.to_string(),
        password: Some(TEST_PASSWORD.to_string()),
    }
}

#[tokio::test]
async fn write_batch_to_clickhouse() {
    let context = TestContext::new().await;

    let writer = context
        .destination
        .new_batch_writer(TEST_TABLE)
        .await
        .expect("failed to create batch writer");

    writer
        .write_batch(&[create_test_batch()])
        .await
        .expect("failed to write batch");

    let result = context
        .query(&format!("SELECT * FROM {TEST_TABLE} ORDER BY id"))
        .await;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].num_rows(), 3);
}

#[tokio::test]
async fn write_multiple_batches() {
    let context = TestContext::new().await;

    let writer = context
        .destination
        .new_batch_writer(TEST_TABLE)
        .await
        .expect("failed to create batch writer");

    let batch1 = create_test_batch();
    let batch2 = create_test_batch_with_data(vec![4, 5], vec!["dave", "eve"]);

    writer
        .write_batch(&[batch1, batch2])
        .await
        .expect("failed to write batches");

    let result = context
        .query(&format!("SELECT count() as cnt FROM {TEST_TABLE}"))
        .await;

    let count_array = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64Array");

    assert_eq!(count_array.value(0), 5);
}

#[tokio::test]
async fn write_empty_batch_succeeds() {
    let context = TestContext::new().await;

    let writer = context
        .destination
        .new_batch_writer(TEST_TABLE)
        .await
        .expect("failed to create batch writer");

    writer
        .write_batch(&[])
        .await
        .expect("empty write should succeed");
}

#[tokio::test]
async fn connection_failure_returns_error() {
    let config = ClickHouseConfiguration {
        database: "nonexistent".to_string(),
        url: "http://127.0.0.1:19000".to_string(),
        username: "default".to_string(),
        password: None,
    };

    let destination = ClickHouseDestination::new(config).expect("failed to create destination");

    let writer = destination
        .new_batch_writer(TEST_TABLE)
        .await
        .expect("writer creation should succeed");

    let batch = create_test_batch();
    let result = writer.write_batch(&[batch]).await;
    assert!(result.is_err(), "should fail to write to invalid address");
}
