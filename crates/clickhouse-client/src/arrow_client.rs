use std::io::Cursor;
use std::sync::{Arc, Mutex};

use arrow::buffer::Buffer as ArrowBuffer;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::{StreamDecoder, StreamReader};
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use clickhouse::{Client, query::Query};
use futures::StreamExt;
use futures::stream;
use futures::stream::BoxStream;
use gkg_utils::clickhouse::{ChScalar, ChType};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::SyncIoBridge;
use tracing::warn;

pub use clickhouse::QuerySummary;

use crate::error::ClickHouseError;

#[derive(Clone)]
pub struct ArrowClickHouseClient {
    client: Client,
    base_url: String,
    database: String,
    insert_settings: std::collections::HashMap<String, String>,
}

impl ArrowClickHouseClient {
    pub fn new(
        url: &str,
        database: &str,
        username: &str,
        password: Option<&str>,
        query_settings: &std::collections::HashMap<String, String>,
        insert_settings: &std::collections::HashMap<String, String>,
        max_query_size: u64,
    ) -> Self {
        let mut client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(username)
            .with_setting("output_format_arrow_string_as_string", "1")
            .with_setting("output_format_arrow_fixed_string_as_fixed_byte_array", "1")
            .with_setting("use_query_condition_cache", "true")
            .with_setting("join_use_nulls", "0")
            .with_setting("query_plan_join_swap_table", "auto")
            .with_setting("optimize_aggregation_in_order", "1")
            .with_setting("max_query_size", max_query_size.to_string());

        if let Some(password) = password {
            client = client.with_password(password);
        }

        for (k, v) in query_settings {
            client = client.with_setting(k, v);
        }

        Self {
            client,
            base_url: url.to_string(),
            database: database.to_string(),
            insert_settings: insert_settings.clone(),
        }
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn query(&self, sql: &str) -> ArrowQuery {
        ArrowQuery {
            inner: self.client.query(sql),
        }
    }

    /// Returns an `ArrowQuery` with `insert_settings` pre-applied.
    ///
    /// Use this for `INSERT` queries so they inherit async-insert and
    /// other write-specific settings. Use `query()` for read operations.
    pub fn insert_query(&self, sql: &str) -> ArrowQuery {
        let mut q = self.query(sql);
        for (k, v) in &self.insert_settings {
            q = q.with_setting(k, v);
        }
        q
    }

    /// Builds a ` SETTINGS k=v, ...` SQL fragment from `insert_settings`.
    /// Returns an empty string when no insert settings are configured.
    fn insert_settings_clause(&self) -> String {
        if self.insert_settings.is_empty() {
            return String::new();
        }
        let pairs: Vec<String> = self
            .insert_settings
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        format!(" SETTINGS {}", pairs.join(", "))
    }

    pub fn build_insert_sql(&self, table: &str) -> String {
        let settings_clause = self.insert_settings_clause();
        format!("INSERT INTO {table}{settings_clause} FORMAT ArrowStream")
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
            let options = arrow_ipc::writer::IpcWriteOptions::try_new(
                8,
                false,
                arrow_ipc::MetadataVersion::V5,
            )
            .map_err(ClickHouseError::ArrowEncode)?
            .try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME))
            .map_err(ClickHouseError::ArrowEncode)?;
            let mut writer = StreamWriter::try_new_with_options(&mut buffer, &schema, options)
                .map_err(ClickHouseError::ArrowEncode)?;

            for (batch_index, batch) in batches.iter().enumerate() {
                if batch.schema() != schema {
                    warn!(table, batch_index, "RecordBatch schema mismatch");
                }

                writer.write(batch).map_err(ClickHouseError::ArrowEncode)?;
            }

            writer.finish().map_err(ClickHouseError::ArrowEncode)?;
        }

        let settings_clause = self.insert_settings_clause();
        let sql = format!("INSERT INTO {table}{settings_clause} FORMAT ArrowStream");
        let mut insert = self.client.insert_formatted_with(&sql);
        insert
            .send(Bytes::from(buffer))
            .await
            .map_err(ClickHouseError::Insert)?;
        insert.end().await.map_err(ClickHouseError::Insert)?;

        Ok(())
    }

    pub async fn insert_arrow_streaming(
        &self,
        table: &str,
        batches: &[RecordBatch],
    ) -> Result<(), ClickHouseError> {
        let sql = self.build_insert_sql(table);
        self.insert_arrow_streaming_with_sql(table, &sql, batches)
            .await
    }

    pub async fn insert_arrow_streaming_with_sql(
        &self,
        table: &str,
        sql: &str,
        batches: &[RecordBatch],
    ) -> Result<(), ClickHouseError> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();
        let options =
            arrow_ipc::writer::IpcWriteOptions::try_new(8, false, arrow_ipc::MetadataVersion::V5)
                .map_err(ClickHouseError::ArrowEncode)?
                .try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME))
                .map_err(ClickHouseError::ArrowEncode)?;

        let drain = DrainableWriter::new();
        let mut writer = StreamWriter::try_new_with_options(drain.clone(), &schema, options)
            .map_err(ClickHouseError::ArrowEncode)?;

        let mut insert = self.client.insert_formatted_with(sql);

        flush_drain(&mut insert, &drain).await?;

        for (batch_index, batch) in batches.iter().enumerate() {
            if batch.schema() != schema {
                warn!(table, batch_index, "RecordBatch schema mismatch");
            }
            writer.write(batch).map_err(ClickHouseError::ArrowEncode)?;
            flush_drain(&mut insert, &drain).await?;
        }

        writer.finish().map_err(ClickHouseError::ArrowEncode)?;
        flush_drain(&mut insert, &drain).await?;

        insert.end().await.map_err(ClickHouseError::Insert)?;
        Ok(())
    }

    pub async fn execute(&self, sql: &str) -> Result<(), ClickHouseError> {
        self.query(sql).execute().await
    }

    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// Bind a named parameter to a query.
    ///
    /// `ch_type` carries the ClickHouse type from the query placeholder. For
    /// scalar values the JSON `Value` variant determines the Rust type; for
    /// arrays `ch_type` determines the element type for binding.
    pub fn bind_param(query: ArrowQuery, key: &str, value: &Value, ch_type: &ChType) -> ArrowQuery {
        match value {
            Value::String(s) => {
                // CH's HTTP-param parser for DateTime64/Date rejects the ISO
                // 8601 trailing `Z` ("BAD_QUERY_PARAMETER, only 19 of 20 bytes
                // was parsed"). Column already pins UTC, so dropping it
                // preserves the value.
                let normalized = match ch_type {
                    ChType::DateTime64 => s.strip_suffix('Z').unwrap_or(s),
                    _ => s.as_str(),
                };
                query.param(key, normalized)
            }
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

impl std::fmt::Debug for ArrowClickHouseClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowClickHouseClient")
            .field("base_url", &self.base_url)
            .finish()
    }
}

pub struct ArrowQuery {
    pub(crate) inner: Query,
}

impl ArrowQuery {
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        self.inner = self.inner.param(name, value);
        self
    }

    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.with_setting(name, value);
        self
    }

    pub async fn execute(self) -> Result<(), ClickHouseError> {
        self.inner.execute().await.map_err(ClickHouseError::Query)
    }

    pub async fn fetch_arrow(self) -> Result<Vec<RecordBatch>, ClickHouseError> {
        let (batches, _) = self.fetch_arrow_with_summary().await?;
        Ok(batches)
    }

    /// Like `fetch_arrow`, but also returns the `X-ClickHouse-Summary` header
    /// parsed as a `QuerySummary` (if the server sent one).
    pub async fn fetch_arrow_with_summary(
        self,
    ) -> Result<(Vec<RecordBatch>, Option<QuerySummary>), ClickHouseError> {
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

        let summary = cursor.summary().cloned();

        if buffer.is_empty() {
            return Ok((Vec::new(), summary));
        }

        let data_cursor = Cursor::new(buffer);
        let reader =
            StreamReader::try_new(data_cursor, None).map_err(ClickHouseError::ArrowDecode)?;

        let batches: Result<Vec<_>, _> = reader
            .map(|result| result.map_err(ClickHouseError::ArrowDecode))
            .collect();
        Ok((batches?, summary))
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
            .with_setting("max_block_size", max_block_size.to_string());

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

    /// Like [`fetch_arrow_streamed`](Self::fetch_arrow_streamed), but also yields the
    /// `X-ClickHouse-Summary` over a `oneshot` once drained (it arrives after the body).
    pub async fn fetch_arrow_streamed_with_summary(
        mut self,
        max_block_size: u64,
    ) -> Result<
        (
            BoxStream<'static, Result<RecordBatch, ClickHouseError>>,
            oneshot::Receiver<Option<QuerySummary>>,
        ),
        ClickHouseError,
    > {
        self.inner = self
            .inner
            .with_setting("max_block_size", max_block_size.to_string());

        let mut cursor = self
            .inner
            .fetch_bytes("ArrowStream")
            .map_err(ClickHouseError::Query)?;

        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ClickHouseError>>(2);
        let (summary_tx, summary_rx) = oneshot::channel();

        // Decode off the async cursor (not via `SyncIoBridge`) so it stays in
        // scope and its summary can be read once the body is drained.
        tokio::spawn(async move {
            let mut decoder = StreamDecoder::new();
            loop {
                match cursor.next().await {
                    Ok(Some(chunk)) => {
                        let mut buffer = ArrowBuffer::from(chunk.as_ref());
                        while !buffer.is_empty() {
                            match decoder.decode(&mut buffer) {
                                Ok(Some(batch)) => {
                                    if tx.send(Ok(batch)).await.is_err() {
                                        return;
                                    }
                                }
                                Ok(None) => break,
                                Err(err) => {
                                    let _ = tx.send(Err(ClickHouseError::ArrowDecode(err))).await;
                                    return;
                                }
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        let _ = tx.send(Err(ClickHouseError::Query(err))).await;
                        return;
                    }
                }
            }
            let _ = summary_tx.send(cursor.summary().cloned());
        });

        Ok((ReceiverStream::new(rx).boxed(), summary_rx))
    }
}

/// Write target for `StreamWriter` that allows draining the accumulated bytes
/// between IPC message writes. Uses `Arc<Mutex<_>>` so the buffer remains
/// accessible while `StreamWriter` owns the writer.
#[derive(Clone)]
struct DrainableWriter(Arc<Mutex<Vec<u8>>>);

impl DrainableWriter {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn take(&self) -> Vec<u8> {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *guard)
    }
}

impl std::io::Write for DrainableWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn flush_drain(
    insert: &mut clickhouse::insert_formatted::InsertFormatted,
    drain: &DrainableWriter,
) -> Result<(), ClickHouseError> {
    let bytes = drain.take();
    if !bytes.is_empty() {
        insert
            .send(Bytes::from(bytes))
            .await
            .map_err(ClickHouseError::Insert)?;
    }
    Ok(())
}
