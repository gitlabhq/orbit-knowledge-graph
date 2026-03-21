use std::io::Cursor;

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use clickhouse::{Client, query::Query};
use futures::StreamExt;
use futures::stream;
use futures::stream::BoxStream;
use gkg_utils::clickhouse::{ChScalar, ChType};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::SyncIoBridge;
use tracing::warn;

use crate::error::ClickHouseError;
use crate::profiler::QueryProfiler;

#[derive(Clone)]
pub struct ArrowClickHouseClient {
    client: Client,
    base_url: String,
    profiler: QueryProfiler,
}

impl ArrowClickHouseClient {
    pub fn new(url: &str, database: &str, username: &str, password: Option<&str>) -> Self {
        let mut client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(username)
            .with_option("output_format_arrow_string_as_string", "1")
            .with_option("output_format_arrow_fixed_string_as_fixed_byte_array", "1")
            .with_option("join_algorithm", "full_sorting_merge,hash")
            .with_option("query_plan_join_swap_table", "true")
            .with_option("use_query_condition_cache", "true");

        if let Some(password) = password {
            client = client.with_password(password);
        }

        let profiler = QueryProfiler::new(url, database, username, password);

        Self {
            client,
            base_url: url.to_string(),
            profiler,
        }
    }

    pub fn query(&self, sql: &str) -> ArrowQuery {
        ArrowQuery {
            inner: self.client.query(sql),
        }
    }

    pub async fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, ClickHouseError> {
        self.query(sql).fetch_arrow().await
    }

    pub async fn query_arrow_stream(
        &self,
        sql: &str,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ClickHouseError>>, ClickHouseError> {
        self.query(sql).fetch_arrow_stream().await
    }

    pub async fn insert_arrow(
        &self,
        table: &str,
        batches: &[RecordBatch],
    ) -> Result<(), ClickHouseError> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();
        let mut buffer = Vec::new();

        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema)
                .map_err(ClickHouseError::ArrowEncode)?;

            for (batch_index, batch) in batches.iter().enumerate() {
                if batch.schema() != schema {
                    warn!(table, batch_index, "RecordBatch schema mismatch");
                }

                writer.write(batch).map_err(ClickHouseError::ArrowEncode)?;
            }

            writer.finish().map_err(ClickHouseError::ArrowEncode)?;
        }

        let sql = format!("INSERT INTO {table} FORMAT ArrowStream");
        let mut insert = self.client.insert_formatted_with(&sql);
        insert
            .send(Bytes::from(buffer))
            .await
            .map_err(ClickHouseError::Insert)?;
        insert.end().await.map_err(ClickHouseError::Insert)?;

        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<(), ClickHouseError> {
        self.query(sql).execute().await
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }

    pub fn profiler(&self) -> &QueryProfiler {
        &self.profiler
    }

    /// Bind a named parameter to a query.
    ///
    /// `ch_type` carries the ClickHouse type from the query placeholder. For
    /// scalar values the JSON `Value` variant determines the Rust type; for
    /// arrays `ch_type` determines the element type for binding.
    pub fn bind_param(query: ArrowQuery, key: &str, value: &Value, ch_type: &ChType) -> ArrowQuery {
        match value {
            Value::String(s) => query.param(key, s.as_str()),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    query.param(key, i)
                } else if let Some(f) = n.as_f64() {
                    query.param(key, f)
                } else {
                    query.param(key, n.to_string())
                }
            }
            Value::Bool(b) => query.param(key, *b),
            Value::Null => query.param(key, Option::<String>::None),
            Value::Array(arr) => match ch_type {
                ChType::Array(ChScalar::Int64) => {
                    let ints: Vec<i64> = arr.iter().filter_map(|v| v.as_i64()).collect();
                    warn_on_dropped_elements(key, "Int64", arr.len(), ints.len());
                    query.param(key, ints)
                }
                ChType::Array(ChScalar::Float64) => {
                    let floats: Vec<f64> = arr.iter().filter_map(|v| v.as_f64()).collect();
                    warn_on_dropped_elements(key, "Float64", arr.len(), floats.len());
                    query.param(key, floats)
                }
                ChType::Array(ChScalar::Bool) => {
                    let bools: Vec<bool> = arr.iter().filter_map(|v| v.as_bool()).collect();
                    warn_on_dropped_elements(key, "Bool", arr.len(), bools.len());
                    query.param(key, bools)
                }
                _ => {
                    let strings: Vec<String> = arr
                        .iter()
                        .map(|v| match v {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .collect();
                    query.param(key, strings)
                }
            },
            _ => query.param(key, value.to_string()),
        }
    }
}

/// Log a warning when array binding silently drops elements that don't
/// match the expected scalar type (e.g. a string in an Int64 array).
///
/// In practice this should never fire: the query engine's `check_filter_types`
/// validates values against the ontology column type, and the lowerer builds
/// homogeneous arrays. This is purely defensive for `bind_param`'s public API.
fn warn_on_dropped_elements(key: &str, scalar: &str, input: usize, bound: usize) {
    if bound != input {
        warn!(
            param = key,
            scalar,
            input,
            bound,
            "bind_param: array had elements that could not be converted, dropped values"
        );
    }
}

#[cfg(any(test, feature = "testkit"))]
impl ArrowClickHouseClient {
    /// Unconfigured client for unit tests. Never connects to anything.
    pub fn dummy() -> Self {
        Self::new("http://localhost:0", "default", "default", None)
    }
}

impl std::fmt::Debug for ArrowClickHouseClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowClickHouseClient")
            .field("base_url", &self.base_url)
            .finish()
    }
}

pub struct ArrowQuery {
    inner: Query,
}

impl ArrowQuery {
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        self.inner = self.inner.param(name, value);
        self
    }

    pub async fn execute(self) -> Result<(), ClickHouseError> {
        self.inner.execute().await.map_err(ClickHouseError::Query)
    }

    pub async fn fetch_arrow(self) -> Result<Vec<RecordBatch>, ClickHouseError> {
        let mut cursor = self
            .inner
            .fetch_bytes("ArrowStream")
            .map_err(ClickHouseError::Query)?;

        let mut buffer = Vec::new();
        loop {
            match cursor.next().await {
                Ok(Some(chunk)) => buffer.extend(chunk),
                Ok(None) => break,
                Err(e) => return Err(ClickHouseError::Query(e)),
            }
        }

        if buffer.is_empty() {
            return Ok(Vec::new());
        }

        let data_cursor = Cursor::new(buffer);
        let reader =
            StreamReader::try_new(data_cursor, None).map_err(ClickHouseError::ArrowDecode)?;

        reader
            .map(|result| result.map_err(ClickHouseError::ArrowDecode))
            .collect()
    }

    pub async fn fetch_arrow_stream(
        self,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ClickHouseError>>, ClickHouseError> {
        let mut cursor = self
            .inner
            .fetch_bytes("ArrowStream")
            .map_err(ClickHouseError::Query)?;

        let mut buffer = Vec::new();
        loop {
            match cursor.next().await {
                Ok(Some(chunk)) => buffer.extend(chunk),
                Ok(None) => break,
                Err(e) => return Err(ClickHouseError::Query(e)),
            }
        }

        if buffer.is_empty() {
            return Ok(Box::pin(stream::empty()) as BoxStream<'static, _>);
        }

        let data_cursor = Cursor::new(buffer);
        let reader =
            StreamReader::try_new(data_cursor, None).map_err(ClickHouseError::ArrowDecode)?;

        let batch_iter = reader.map(|result| result.map_err(ClickHouseError::ArrowDecode));
        Ok(Box::pin(stream::iter(batch_iter)))
    }

    pub async fn fetch_arrow_streamed(
        mut self,
        max_block_size: u64,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ClickHouseError>>, ClickHouseError> {
        self.inner = self
            .inner
            .with_option("max_block_size", max_block_size.to_string());

        let cursor = self
            .inner
            .fetch_bytes("ArrowStream")
            .map_err(ClickHouseError::Query)?;

        let handle = tokio::runtime::Handle::current();
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ClickHouseError>>(2);

        tokio::task::spawn_blocking(move || {
            let bridge = SyncIoBridge::new_with_handle(cursor, handle);
            let reader = match StreamReader::try_new(bridge, None) {
                Ok(reader) => reader,
                Err(err) => {
                    let _ = tx.blocking_send(Err(ClickHouseError::ArrowDecode(err)));
                    return;
                }
            };

            for batch_result in reader {
                let mapped: Result<RecordBatch, ClickHouseError> =
                    batch_result.map_err(ClickHouseError::ArrowDecode);
                if tx.blocking_send(mapped).is_err() {
                    break;
                }
            }
        });

        Ok(ReceiverStream::new(rx).boxed())
    }
}
