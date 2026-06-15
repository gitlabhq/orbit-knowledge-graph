//! Integration tests for the ClickHouse destination.
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use gkg_server_config::ClickHouseConfiguration;
use gkg_utils::arrow::ArrowUtils;
use indexer::clickhouse::{ArrowClickHouseClient, ClickHouseDestination};
use indexer::destination::Destination;
use indexer::metrics::EngineMetrics;
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
        let destination = ClickHouseDestination::new(config, Arc::new(EngineMetrics::default()))
            .expect("failed to create destination");

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
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        )
    }

    async fn execute(&self, sql: &str) {
        self.create_client()
            .execute(sql)
            .await
            .expect("execute failed");
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
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
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
        session_settings: std::collections::HashMap::new(),
        insert_settings: std::collections::HashMap::new(),
        profiling: Default::default(),
        ..Default::default()
    }
}

async fn write_batch_to_clickhouse(context: &TestContext) {
    context
        .execute(&format!("TRUNCATE TABLE {TEST_TABLE}"))
        .await;

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

async fn write_multiple_batches(context: &TestContext) {
    context
        .execute(&format!("TRUNCATE TABLE {TEST_TABLE}"))
        .await;

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

    let count_array =
        ArrowUtils::get_column_by_name::<UInt64Array>(&result[0], "cnt").expect("cnt column");
    assert_eq!(count_array.value(0), 5);
}

async fn write_empty_batch_succeeds(context: &TestContext) {
    context
        .execute(&format!("TRUNCATE TABLE {TEST_TABLE}"))
        .await;

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
async fn clickhouse_destination() {
    let context = TestContext::new().await;
    write_batch_to_clickhouse(&context).await;
    write_multiple_batches(&context).await;
    write_empty_batch_succeeds(&context).await;
}

/// The byte cap the datalake pins on retry (`preferred_block_size_bytes`)
/// resolves the Arrow String 2GB overflow that a smaller row cap alone can't.
/// Needs ~3GB of container memory to build the oversized block.
#[tokio::test]
async fn arrow_string_overflow_recovers_with_byte_cap() {
    use futures::StreamExt;

    let context = TestContext::new().await;
    let client = context.create_client();

    // index_granularity_bytes=0 makes the sort coalesce the page into one block.
    context
        .execute(
            "CREATE TABLE wide_overflow (id UInt32, s String) \
             ENGINE = MergeTree ORDER BY id SETTINGS index_granularity_bytes = 0",
        )
        .await;
    // 2148 * 1MB exceeds the Arrow String 2GB offset cap.
    context
        .execute(
            "INSERT INTO wide_overflow SELECT number, repeat('A', 1000000) \
             FROM numbers(2148) SETTINGS max_memory_usage = 0",
        )
        .await;

    let sql = "SELECT s FROM wide_overflow ORDER BY id LIMIT 2148";

    // preferred_block_size_bytes=0 reproduces the incident profile; the default
    // of 1MB would mask the bug.
    let mut without_cap = client
        .query(sql)
        .with_setting("max_memory_usage", "0")
        .with_setting("preferred_block_size_bytes", "0")
        .fetch_arrow_streamed(8_000)
        .await
        .expect("query opens");
    let mut overflowed = false;
    while let Some(batch) = without_cap.next().await {
        if let Err(err) = batch {
            assert!(
                err.to_string().contains("cannot contain more than"),
                "expected the Arrow 2GB overflow, got: {err}"
            );
            overflowed = true;
            break;
        }
    }
    assert!(
        overflowed,
        "a reduced row cap alone must still overflow on a >2GB block"
    );

    let mut with_cap = client
        .query(sql)
        .with_setting("max_memory_usage", "0")
        .with_setting("preferred_block_size_bytes", "1000000")
        .fetch_arrow_streamed(8_000)
        .await
        .expect("query opens");
    let mut rows = 0u64;
    while let Some(batch) = with_cap.next().await {
        rows += batch
            .expect("the byte cap must keep each block under the Arrow limit")
            .num_rows() as u64;
    }
    assert_eq!(rows, 2148);
}

#[tokio::test]
async fn connection_failure_returns_error() {
    let config = ClickHouseConfiguration {
        database: "nonexistent".to_string(),
        url: "http://127.0.0.1:19000".to_string(),
        username: "default".to_string(),
        password: None,
        session_settings: std::collections::HashMap::new(),
        insert_settings: std::collections::HashMap::new(),
        profiling: Default::default(),
        ..Default::default()
    };

    let destination = ClickHouseDestination::new(config, Arc::new(EngineMetrics::default()))
        .expect("failed to create destination");

    let writer = destination
        .new_batch_writer(TEST_TABLE)
        .await
        .expect("writer creation should succeed");

    let batch = create_test_batch();
    let result = writer.write_batch(&[batch]).await;
    assert!(result.is_err(), "should fail to write to invalid address");
}
