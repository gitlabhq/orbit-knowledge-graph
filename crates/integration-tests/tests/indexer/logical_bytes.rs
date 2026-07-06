//! Parity check for `gkg_utils::arrow::logical_byte_size`: the Rust meter must equal a
//! hand-written SQL formula evaluated by ClickHouse over the same inserted batch, one counted
//! column type at a time. Not compared against `byteSize()` — the meter excludes storage
//! overhead. Requires a Docker-compatible runtime.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int64Array, ListBuilder, StringArray,
    StringBuilder, StringDictionaryBuilder, TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use gkg_utils::arrow::{ArrowUtils, logical_byte_size};
use integration_testkit::TestContext;

const TABLE: &str = "logical_bytes_parity";

const CREATE_TABLE: &str = "CREATE TABLE logical_bytes_parity (
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
) ENGINE = MergeTree ORDER BY tuple()";

#[tokio::test]
async fn logical_byte_size_matches_chart_derived_sql() {
    let ctx = TestContext::new(&[CREATE_TABLE]).await;
    let batch = batch_with_nonempty_empty_and_null_rows();

    ctx.create_client()
        .insert_arrow(TABLE, std::slice::from_ref(&batch))
        .await
        .expect("insert failed");

    let column_to_sql = [
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

    for (column, sql_formula) in column_to_sql {
        let idx = batch.schema().index_of(column).unwrap();
        let rust_bytes = logical_byte_size(&batch.project(&[idx]).unwrap()).unwrap();
        let sql_bytes = scalar_u64(&ctx, &format!("SELECT {sql_formula} AS n FROM {TABLE}")).await;
        assert_eq!(
            rust_bytes, sql_bytes,
            "column '{column}': Rust meter != SQL `{sql_formula}`"
        );
    }
}

async fn scalar_u64(ctx: &TestContext, sql: &str) -> u64 {
    let batches = ctx.query(sql).await;
    ArrowUtils::get_column_by_name::<UInt64Array>(&batches[0], "n")
        .expect("n column")
        .value(0)
}

fn batch_with_nonempty_empty_and_null_rows() -> RecordBatch {
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
