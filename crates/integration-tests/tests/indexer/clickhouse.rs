//! Require a Docker-compatible runtime (Docker, Colima, etc).

use clickhouse_client::ClickHouseConfigurationExt;
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use indexer::clickhouse::{ArrowClickHouseClient, ClickHouseWriter};
use indexer::durability::WriteDurability;
use indexer::metrics::EngineMetrics;
use orbit_arrow::ArrowUtils;
use orbit_server_config::ClickHouseConfiguration;
const TEST_TABLE: &str = "test_table";

struct TestContext {
    clickhouse: integration_testkit::TestContext,
    writer: ClickHouseWriter,
}

impl TestContext {
    async fn new() -> Self {
        let clickhouse = integration_testkit::TestContext::new(&[]).await;
        clickhouse
            .execute(&format!(
                "CREATE TABLE {TEST_TABLE} (id Int32, name String) \
                 ENGINE = MergeTree() ORDER BY id"
            ))
            .await;
        let writer = ClickHouseWriter::new(
            clickhouse.config.build_client("graph"),
            Arc::new(EngineMetrics::default()),
        );

        Self { clickhouse, writer }
    }

    fn create_client(&self) -> ArrowClickHouseClient {
        self.clickhouse.create_client()
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

async fn write_batch_to_clickhouse(context: &TestContext) {
    context
        .execute(&format!("TRUNCATE TABLE {TEST_TABLE}"))
        .await;

    context
        .writer
        .write(
            TEST_TABLE,
            vec![create_test_batch()],
            Some(WriteDurability::Durable),
        )
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

    let batch1 = create_test_batch();
    let batch2 = create_test_batch_with_data(vec![4, 5], vec!["dave", "eve"]);

    context
        .writer
        .write(
            TEST_TABLE,
            vec![batch1, batch2],
            Some(WriteDurability::Durable),
        )
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

    context
        .writer
        .write(TEST_TABLE, vec![], Some(WriteDurability::Durable))
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
    let without_cap = client
        .query(sql)
        .with_setting("max_memory_usage", "0")
        .with_setting("preferred_block_size_bytes", "0")
        .fetch_arrow_streamed(Some(8_000))
        .await;
    let overflow = match without_cap {
        Err(err) => err,
        Ok(mut stream) => {
            let mut first_error = None;
            while let Some(batch) = stream.next().await {
                if let Err(err) = batch {
                    first_error = Some(err);
                    break;
                }
            }
            first_error.expect("a reduced row cap alone must still overflow on a >2GB block")
        }
    };
    let overflow = overflow.to_string();
    assert!(
        overflow.contains("cannot contain more than") || overflow.contains("exceeds 32 bits"),
        "expected the Arrow 2GB overflow, got: {overflow}"
    );

    let mut with_cap = client
        .query(sql)
        .with_setting("max_memory_usage", "0")
        .with_setting("preferred_block_size_bytes", "1000000")
        .fetch_arrow_streamed(Some(8_000))
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

/// Forwards responses in 1 KiB pieces, 1 ms apart, so a plain response reaches
/// the client in many small chunks, as it does over a real network.
async fn throttling_proxy(upstream_url: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let upstream = upstream_url
        .strip_prefix("http://")
        .expect("test ClickHouse URL uses http")
        .to_string();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let server = tokio::net::TcpStream::connect(upstream).await.unwrap();
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        tokio::spawn(async move { tokio::io::copy(&mut client_read, &mut server_write).await });
        let mut piece = [0; 1024];
        loop {
            let read = server_read.read(&mut piece).await.unwrap_or(0);
            if read == 0 || client_write.write_all(&piece[..read]).await.is_err() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    });
    url
}

#[tokio::test]
async fn streamed_read_keeps_every_byte_of_a_slow_plain_response() {
    use futures::StreamExt;

    let context = integration_testkit::TestContext::new(&[]).await;
    let config = ClickHouseConfiguration {
        url: throttling_proxy(&context.config.url).await,
        ..context.config.clone()
    };

    // Small blocks put many Arrow message headers across chunk boundaries.
    let mut batches = config
        .build_client("graph")
        .query("SELECT number FROM numbers(3200)")
        .with_setting("enable_http_compression", "0")
        .fetch_arrow_streamed(Some(16))
        .await
        .expect("query opens");
    let mut rows = 0;
    while let Some(batch) = batches.next().await {
        rows += batch.expect("every batch decodes").num_rows();
    }
    assert_eq!(rows, 3200);
}

#[tokio::test]
async fn connection_failure_returns_error() {
    let config = ClickHouseConfiguration {
        database: "nonexistent".to_string(),
        url: "http://127.0.0.1:19000".to_string(),
        username: "default".to_string(),
        password: None,
        compatibility: orbit_server_config::AppConfig::embedded_defaults()
            .graph
            .compatibility,
        session_settings: std::collections::HashMap::new(),
        replicated: false,
        insert_settings: std::collections::HashMap::new(),
        profiling: orbit_server_config::AppConfig::embedded_defaults()
            .graph
            .profiling,
    };

    let writer = ClickHouseWriter::new(
        config.build_client("graph"),
        Arc::new(EngineMetrics::default()),
    );

    let result = writer
        .write(
            TEST_TABLE,
            vec![create_test_batch()],
            Some(WriteDurability::Durable),
        )
        .await;
    assert!(result.is_err(), "should fail to write to invalid address");
}

#[tokio::test]
async fn datalake_watermark_reads_use_the_skip_index_on_tables_with_a_projection() {
    let clickhouse = integration_testkit::TestContext::new(&[]).await;
    let datalake = ClickHouseConfiguration {
        url: clickhouse.config.url.clone(),
        database: clickhouse.config.database.clone(),
        username: clickhouse.config.username.clone(),
        password: clickhouse.config.password.clone(),
        ..orbit_server_config::AppConfig::embedded_defaults().datalake
    }
    .build_client("datalake");

    // Large rows clustered in the sort key give the merged base part more marks than its
    // projection, so ClickHouse 26.6 to 26.10 prefer the projection, which has no skip index.
    for statement in [
        "CREATE TABLE watermark_reads (k UInt32, id UInt64, payload String, watermark DateTime, \
         INDEX idx_watermark watermark TYPE minmax GRANULARITY 1, \
         PROJECTION by_id (SELECT * ORDER BY id)) \
         ENGINE = MergeTree ORDER BY (k, id) \
         SETTINGS index_granularity = 2048, index_granularity_bytes = 1048576, auto_statistics_types = ''",
        "INSERT INTO watermark_reads SELECT number % 500, number, \
         repeat('x', if(number % 500 < 10, 4000, 20)), toDateTime('2026-01-01') + number % 86400 \
         FROM numbers(100000)",
        "INSERT INTO watermark_reads SELECT number % 500, 100000 + number, \
         repeat('x', if(number % 500 < 10, 4000, 20)), toDateTime('2026-01-02') + number % 86400 \
         FROM numbers(100000)",
        "OPTIMIZE TABLE watermark_reads FINAL",
        "SYSTEM STOP MERGES watermark_reads",
        "INSERT INTO watermark_reads SELECT number % 500, 200000 + number, 'recent', \
         toDateTime('2026-06-01') + number % 3600 FROM numbers(10000)",
    ] {
        clickhouse.execute(statement).await;
    }

    let (_, summary) = datalake
        .query("SELECT k, watermark FROM watermark_reads WHERE watermark >= '2026-06-01 00:00:00'")
        .fetch_arrow_with_summary()
        .await
        .expect("watermark query runs");

    let read_rows = summary.and_then(|summary| summary.read_rows());
    assert_eq!(
        read_rows,
        Some(10_000),
        "only the recent part should be read"
    );
}
