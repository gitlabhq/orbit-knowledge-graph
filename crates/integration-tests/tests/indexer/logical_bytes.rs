//! Proves the SQL rendering of the `gkg_utils::arrow::logical_byte_size` chart matches the
//! Rust implementation over the same real ClickHouse 26.2 insert. Deliberately does NOT
//! compare against `byteSize()`: the meter counts only customer-data bytes, not ClickHouse's
//! storage overhead, so its chart has its own SQL shape (see the column formulas below).
//! Requires a Docker-compatible runtime (Docker, Colima, etc).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int64Array, ListBuilder, StringArray,
    StringBuilder, StringDictionaryBuilder, TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use gkg_utils::arrow::{ArrowUtils, logical_byte_size};
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "26.2";
const TEST_USERNAME: &str = "default";
const TEST_PASSWORD: &str = "testpass";
const TEST_DATABASE: &str = "test";
const TEST_TABLE: &str = "logical_bytes_parity";

const MAX_CONNECTION_ATTEMPTS: u32 = 30;
const CONNECTION_RETRY_DELAY_MS: u64 = 500;

struct TestContext {
    _container: testcontainers::ContainerAsync<GenericImage>,
    client: ArrowClickHouseClient,
}

impl TestContext {
    async fn new() -> Self {
        let (container, host, port) = start_clickhouse_container().await;
        let client = wait_for_client(&host, port).await;
        client
            .execute(&format!(
                "CREATE TABLE {TEST_DATABASE}.{TEST_TABLE} (
                    plain_string String,
                    lc_string LowCardinality(String),
                    nullable_string Nullable(String),
                    str_list Array(String),
                    int64_col Int64,
                    uint64_col UInt64,
                    uint32_col UInt32,
                    date32_col Date32,
                    ts_col DateTime64(6, 'UTC'),
                    bool_col Bool
                ) ENGINE = MergeTree ORDER BY tuple()"
            ))
            .await
            .expect("failed to create table");

        Self {
            _container: container,
            client,
        }
    }

    async fn scalar_u64(&self, formula: &str) -> u64 {
        let result = self
            .client
            .query_arrow(&format!("SELECT {formula} AS total FROM {TEST_TABLE}"))
            .await
            .expect("query failed");
        ArrowUtils::get_column_by_name::<UInt64Array>(&result[0], "total")
            .expect("total column")
            .value(0)
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

async fn wait_for_client(host: &str, port: u16) -> ArrowClickHouseClient {
    let mut attempts = 0;
    loop {
        let client = ArrowClickHouseClient::new(
            &format!("http://{host}:{port}"),
            TEST_DATABASE,
            TEST_USERNAME,
            Some(TEST_PASSWORD),
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        );
        match client.execute("SELECT 1").await {
            Ok(_) => return client,
            Err(_) if attempts < MAX_CONNECTION_ATTEMPTS => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(CONNECTION_RETRY_DELAY_MS))
                    .await;
            }
            Err(e) => panic!("failed to connect to ClickHouse after {attempts} attempts: {e}"),
        }
    }
}

fn edge_case_batch() -> RecordBatch {
    let plain_string = StringArray::from(vec!["hello", "", "gitlab"]);

    let mut lc_builder = StringDictionaryBuilder::<Int32Type>::new();
    for v in ["alpha", "beta", "alpha"] {
        lc_builder.append_value(v);
    }
    let lc_string = lc_builder.finish();

    let nullable_string = StringArray::from(vec![Some("world"), None, Some("")]);

    let mut list_builder = ListBuilder::new(StringBuilder::new());
    for row in [vec!["a", "b"], vec![], vec!["single"]] {
        for v in row {
            list_builder.values().append_value(v);
        }
        list_builder.append(true);
    }
    let str_list = list_builder.finish();

    let int64_col = Int64Array::from(vec![1, -1, 999_999]);
    let uint64_col = UInt64Array::from(vec![100u64, 200, 999_999_999_999]);
    let uint32_col = UInt32Array::from(vec![7u32, 8, 4_000_000_000]);
    let date32_col = Date32Array::from(vec![19_000, 19_001, 19_002]);
    let ts_col = TimestampMicrosecondArray::from(vec![
        1_700_000_000_000_000,
        1_700_000_100_000_000,
        1_700_000_200_000_000,
    ])
    .with_timezone("UTC");
    let bool_col = BooleanArray::from(vec![true, false, true]);

    let fields = vec![
        Field::new("plain_string", DataType::Utf8, false),
        Field::new(
            "lc_string",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("nullable_string", DataType::Utf8, true),
        Field::new("str_list", str_list.data_type().clone(), false),
        Field::new("int64_col", DataType::Int64, false),
        Field::new("uint64_col", DataType::UInt64, false),
        Field::new("uint32_col", DataType::UInt32, false),
        Field::new("date32_col", DataType::Date32, false),
        Field::new(
            "ts_col",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("bool_col", DataType::Boolean, false),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(plain_string),
        Arc::new(lc_string),
        Arc::new(nullable_string),
        Arc::new(str_list),
        Arc::new(int64_col),
        Arc::new(uint64_col),
        Arc::new(uint32_col),
        Arc::new(date32_col),
        Arc::new(ts_col),
        Arc::new(bool_col),
    ];
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn logical_byte_size_matches_chart_derived_sql() {
    let context = TestContext::new().await;
    let batch = edge_case_batch();

    context
        .client
        .insert_arrow(TEST_TABLE, std::slice::from_ref(&batch))
        .await
        .expect("failed to insert batch");

    let column_formulas = [
        ("plain_string", "sum(length(plain_string))"),
        ("lc_string", "sum(length(lc_string))"),
        (
            "nullable_string",
            "sum(if(nullable_string IS NULL, 0, length(nullable_string)))",
        ),
        (
            "str_list",
            "sum(arraySum(arrayMap(x -> length(x), str_list)))",
        ),
        ("int64_col", "8 * countIf(int64_col IS NOT NULL)"),
        ("uint64_col", "8 * countIf(uint64_col IS NOT NULL)"),
        ("uint32_col", "4 * countIf(uint32_col IS NOT NULL)"),
        ("date32_col", "4 * countIf(date32_col IS NOT NULL)"),
        ("ts_col", "8 * countIf(ts_col IS NOT NULL)"),
        ("bool_col", "1 * countIf(bool_col IS NOT NULL)"),
    ];

    for (name, formula) in column_formulas {
        let idx = batch.schema().index_of(name).unwrap();
        let single_col = batch.project(&[idx]).unwrap();
        let rust_count = logical_byte_size(&single_col).unwrap();
        let ch_count = context.scalar_u64(formula).await;
        assert_eq!(
            rust_count, ch_count,
            "column '{name}' diverges from its chart formula"
        );
    }
}
