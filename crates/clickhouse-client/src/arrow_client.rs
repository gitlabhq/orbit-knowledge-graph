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

/// The settings every read query carries, seeded on the underlying
/// `clickhouse::Client`. `do_execute` appends one URL query pair per entry, so
/// they count toward the request-URI length the `http` crate caps; the URI
/// measurement on the client includes them via `scaffold_pairs`.
const BASELINE_QUERY_SETTINGS: &[(&str, &str)] = &[
    ("output_format_arrow_string_as_string", "1"),
    ("output_format_arrow_fixed_string_as_fixed_byte_array", "1"),
    ("use_query_condition_cache", "true"),
    ("join_use_nulls", "0"),
    ("query_plan_join_swap_table", "auto"),
    ("optimize_aggregation_in_order", "1"),
];

#[derive(Clone)]
pub struct ArrowClickHouseClient {
    client: Client,
    base_url: String,
    database: String,
    insert_settings: std::collections::HashMap<String, String>,
    /// The baseline settings plus any operator `session_settings`, captured in
    /// the same form `do_execute` appends them. Held alongside the client
    /// because `clickhouse::Client` does not expose its settings for iteration.
    query_settings: Vec<(String, String)>,
}

impl ArrowClickHouseClient {
    pub fn new(
        url: &str,
        database: &str,
        username: &str,
        password: Option<&str>,
        session_settings: &std::collections::HashMap<String, String>,
        insert_settings: &std::collections::HashMap<String, String>,
    ) -> Self {
        let mut client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(username);

        let mut query_settings: Vec<(String, String)> = Vec::new();
        for (k, v) in BASELINE_QUERY_SETTINGS {
            client = client.with_setting(*k, *v);
            query_settings.push(((*k).to_string(), (*v).to_string()));
        }

        if let Some(password) = password {
            client = client.with_password(password);
        }

        for (k, v) in session_settings {
            client = client.with_setting(k, v);
            query_settings.push((k.clone(), v.clone()));
        }

        Self {
            client,
            base_url: url.to_string(),
            database: database.to_string(),
            insert_settings: insert_settings.clone(),
            query_settings,
        }
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    /// Byte length of the request URI `do_execute` would dispatch for
    /// `sql_params`, faithful to both encoding stages (ClickHouse param
    /// serialization → url-crate percent-encoding) and all scaffold pairs.
    pub fn request_uri_len(&self, sql_params: &Value) -> usize {
        crate::uri_len::measure_uri(
            &self.base_url,
            &self.database,
            &self.scaffold_pairs(),
            sql_params,
        )
    }

    /// `Some(len)` when the URI for `sql_params` would exceed
    /// [`MAX_REQUEST_URI_LEN`](crate::MAX_REQUEST_URI_LEN), `None` when it fits.
    #[cfg(test)]
    pub(crate) fn request_uri_overflow(&self, sql_params: &Value) -> Option<usize> {
        crate::uri_len::overflow(
            &self.base_url,
            &self.database,
            &self.scaffold_pairs(),
            sql_params,
        )
    }

    /// Split `items` into the fewest chunks whose serialized request URIs stay
    /// at or under `max_uri_len`. `build_params` renders a sub-slice into the
    /// full `Value` the query sends.
    ///
    /// `dispatch_settings` are the query settings the dispatch path appends
    /// *after* the URI guard's construction — for the streamed read path that is
    /// `max_block_size` (and any retry settings). They are measured here so a
    /// chunk validated as in-budget still fits once dispatch adds them;
    /// otherwise the guard would reject a chunk the chunker thought was safe
    /// (KG#881). Pass `&[]` when the dispatch path adds no extra settings.
    pub fn chunk_params_to_fit_uri<'a, T>(
        &self,
        items: &'a [T],
        build_params: impl Fn(&[T]) -> Value,
        max_uri_len: usize,
        dispatch_settings: &[(&str, &str)],
    ) -> Vec<&'a [T]> {
        let mut scaffold = self.scaffold_pairs();
        scaffold.extend(
            dispatch_settings
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
        );
        crate::uri_len::chunk_to_fit(
            &self.base_url,
            &self.database,
            &scaffold,
            items,
            build_params,
            max_uri_len,
        )
    }

    /// The URL query pairs `do_execute` appends to every read request besides
    /// `default_format`, `database`, and the per-query `param_*` settings: the
    /// compression flag and the seeded query settings (baseline +
    /// `session_settings`). `new` never overrides compression, so the client
    /// keeps the `clickhouse::Compression` default (LZ4 with the `lz4` feature
    /// on), serialized as `compress=1`; `roles` are never set, so none emit.
    pub(crate) fn scaffold_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::with_capacity(self.query_settings.len() + 1);
        pairs.push(("compress".to_string(), "1".to_string()));
        pairs.extend(self.query_settings.iter().cloned());
        pairs
    }

    pub fn query(&self, sql: &str) -> ArrowQuery {
        ArrowQuery {
            inner: self.client.query(sql),
            uri_guard: UriGuard {
                base_url: self.base_url.clone(),
                database: self.database.clone(),
                scaffold_pairs: self.scaffold_pairs(),
                params: serde_json::Map::new(),
            },
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

    /// Sorted so the emitted `SETTINGS` clause is deterministic.
    fn insert_settings_clause(&self, overrides: &[(&str, &str)]) -> String {
        if self.insert_settings.is_empty() && overrides.is_empty() {
            return String::new();
        }
        let mut merged: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
        for (k, v) in &self.insert_settings {
            merged.insert(k, v);
        }
        for &(k, v) in overrides {
            merged.insert(k, v);
        }
        let pairs: Vec<String> = merged.iter().map(|(k, v)| format!("{k}={v}")).collect();
        format!(" SETTINGS {}", pairs.join(", "))
    }

    pub fn build_insert_sql(&self, table: &str) -> String {
        self.build_insert_sql_with_overrides(table, &[])
    }

    pub fn build_insert_sql_with_overrides(
        &self,
        table: &str,
        overrides: &[(&str, &str)],
    ) -> String {
        let settings_clause = self.insert_settings_clause(overrides);
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

        let settings_clause = self.insert_settings_clause(&[]);
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

/// The query's request-URI measurement context, captured from the client at
/// construction so the dispatch guard fires without a back-reference. Holds a
/// copy of every `param_*` value (the overflow-prone, caller-controlled term).
struct UriGuard {
    base_url: String,
    database: String,
    scaffold_pairs: Vec<(String, String)>,
    params: serde_json::Map<String, Value>,
}

impl UriGuard {
    /// `Err(UriTooLong)` when the accumulated params would dispatch a URI over
    /// the `http` cap. Called at the top of every dispatch method so the guard
    /// is automatic for every consumer, datalake or not.
    fn check(&self) -> Result<(), ClickHouseError> {
        let params = Value::Object(self.params.clone());
        if let Some(len) = crate::uri_len::overflow(
            &self.base_url,
            &self.database,
            &self.scaffold_pairs,
            &params,
        ) {
            return Err(ClickHouseError::UriTooLong {
                len,
                limit: crate::MAX_REQUEST_URI_LEN,
            });
        }
        Ok(())
    }
}

pub struct ArrowQuery {
    pub(crate) inner: Query,
    uri_guard: UriGuard,
}

impl ArrowQuery {
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        // Retain a measurement copy alongside the value handed to the query.
        // Serialization mirrors what the query itself does; a failure here would
        // also fail the query, so fall back to skipping the param in the guard
        // rather than poisoning the call.
        if let Ok(json) = serde_json::to_value(&value) {
            self.uri_guard.params.insert(name.to_string(), json);
        }
        self.inner = self.inner.param(name, value);
        self
    }

    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        let value = value.into();
        // `do_execute` appends every query setting as a `&name=value` URL pair,
        // so the guard must see it too; otherwise a setting added after
        // construction (`max_block_size`, `preferred_block_size_bytes`,
        // `log_comment`, `http_settings`) dispatches on the wire but stays
        // invisible to `check()`, letting a chunk validated as in-budget
        // overflow the cap at dispatch (KG#881).
        self.uri_guard
            .scaffold_pairs
            .push((name.clone(), value.clone()));
        self.inner = self.inner.with_setting(name, value);
        self
    }

    /// Byte length of the request URI the dispatch path would send for this
    /// query as currently configured: base params plus every setting applied
    /// via [`with_setting`](Self::with_setting). This is what the guard checks,
    /// and (post-KG#881) is byte-equivalent to the URI `do_execute` dispatches.
    /// Lets a caller (or its tests) assert the chunk budget matches dispatch.
    pub fn dispatched_uri_len(&self) -> usize {
        crate::uri_len::measure_uri(
            &self.uri_guard.base_url,
            &self.uri_guard.database,
            &self.uri_guard.scaffold_pairs,
            &Value::Object(self.uri_guard.params.clone()),
        )
    }

    /// Whether the dispatch guard would let this query through as currently
    /// configured — the same `check()` every fetch method runs.
    #[cfg(test)]
    pub(crate) fn dispatch_within_cap(&self) -> bool {
        self.uri_guard.check().is_ok()
    }

    pub async fn execute(self) -> Result<(), ClickHouseError> {
        self.uri_guard.check()?;
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
        self.uri_guard.check()?;
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
        self.uri_guard.check()?;
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
        self,
        max_block_size: u64,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ClickHouseError>>, ClickHouseError> {
        let this = self.with_setting("max_block_size", max_block_size.to_string());
        this.uri_guard.check()?;

        let cursor = this
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
        self,
        max_block_size: u64,
    ) -> Result<
        (
            BoxStream<'static, Result<RecordBatch, ClickHouseError>>,
            oneshot::Receiver<Option<QuerySummary>>,
        ),
        ClickHouseError,
    > {
        let this = self.with_setting("max_block_size", max_block_size.to_string());
        this.uri_guard.check()?;

        let mut cursor = this
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn client_with_settings(insert_settings: HashMap<String, String>) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            "http://localhost:8123",
            "default",
            "default",
            None,
            &HashMap::new(),
            &insert_settings,
        )
    }

    #[test]
    fn no_settings_emits_no_clause() {
        let client = client_with_settings(HashMap::new());
        assert_eq!(
            client.build_insert_sql("t"),
            "INSERT INTO t FORMAT ArrowStream"
        );
    }

    #[test]
    fn config_settings_sort_for_deterministic_sql() {
        let client = client_with_settings(HashMap::from([
            ("wait_for_async_insert".to_string(), "0".to_string()),
            ("async_insert".to_string(), "1".to_string()),
        ]));
        assert_eq!(
            client.build_insert_sql("t"),
            "INSERT INTO t SETTINGS async_insert=1, wait_for_async_insert=0 FORMAT ArrowStream"
        );
    }

    #[test]
    fn overrides_win_over_config() {
        let client = client_with_settings(HashMap::from([(
            "wait_for_async_insert".to_string(),
            "0".to_string(),
        )]));
        let overrides = [("async_insert", "1"), ("wait_for_async_insert", "1")];
        assert_eq!(
            client.build_insert_sql_with_overrides("t", &overrides),
            "INSERT INTO t SETTINGS async_insert=1, wait_for_async_insert=1 FORMAT ArrowStream"
        );
    }

    #[test]
    fn request_uri_len_includes_the_clients_scaffold_pairs() {
        let client = client_with_settings(HashMap::new());
        let len = client.request_uri_len(&serde_json::json!({}));
        let scaffold_extra: usize = client
            .scaffold_pairs()
            .iter()
            .map(|(k, v)| format!("&{k}={v}").len())
            .sum();
        let bare = "http://localhost:8123/?default_format=ArrowStream&database=default".len();
        assert_eq!(len, bare + scaffold_extra);
    }

    #[test]
    fn request_uri_overflow_flags_an_oversized_param() {
        let client = client_with_settings(HashMap::new());
        assert_eq!(
            client.request_uri_overflow(&serde_json::json!({ "p": "x" })),
            None
        );
        let huge: String = std::iter::repeat_n('a', 70_000).collect();
        assert!(matches!(
            client.request_uri_overflow(&serde_json::json!({ "p": huge })),
            Some(len) if len > crate::MAX_REQUEST_URI_LEN
        ));
    }

    #[test]
    fn chunk_params_to_fit_uri_splits_an_oversized_batch() {
        let client = client_with_settings(HashMap::new());
        let path = "a".repeat(200);
        let paths: Vec<&str> = std::iter::repeat_n(path.as_str(), 1_000).collect();
        let chunks = client.chunk_params_to_fit_uri(
            &paths,
            |chunk| serde_json::json!({ "paths": chunk }),
            crate::MAX_REQUEST_URI_LEN,
            &[],
        );
        assert!(chunks.len() > 1);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 1_000);
        for chunk in &chunks {
            let len = client.request_uri_len(&serde_json::json!({ "paths": chunk }));
            assert!(len <= crate::MAX_REQUEST_URI_LEN);
        }
    }

    #[tokio::test]
    async fn dispatch_rejects_an_over_cap_param_before_hitting_the_wire() {
        let client = client_with_settings(HashMap::new());
        let huge: String = std::iter::repeat_n('a', 70_000).collect();
        let result = client
            .query("SELECT 1")
            .param("p", huge)
            .fetch_arrow_streamed(1)
            .await;
        assert!(matches!(
            result,
            Err(ClickHouseError::UriTooLong { len, limit })
                if len > crate::MAX_REQUEST_URI_LEN && limit == crate::MAX_REQUEST_URI_LEN
        ));
    }

    #[tokio::test]
    async fn dispatch_measures_escaped_encoded_length_not_raw_bytes() {
        let client = client_with_settings(HashMap::new());
        // 500 single-quotes × 40 paths: raw bytes are well under the cap, but
        // ClickHouse doubles `'` then `url` percent-encodes it (6 B each), so the
        // wire URI clears the cap — only an encoded measurement catches it.
        let quote_heavy: String = std::iter::repeat_n('\'', 500).collect();
        let paths: Vec<String> = std::iter::repeat_n(quote_heavy, 40).collect();
        let result = client
            .query("SELECT 1")
            .param("paths", paths)
            .fetch_arrow_streamed(1)
            .await;
        assert!(matches!(result, Err(ClickHouseError::UriTooLong { .. })));
    }

    // KG#881 regression: the routes path chunks against the URI guard's
    // construction-time scaffold, but `fetch_arrow_streamed` appends
    // `&max_block_size=<n>` (~21 B) *after* the guard is built. With the
    // production zero-headroom budget a chunk packed to exactly the cap
    // dispatches over it. This reproduces the production chokepoint end to end:
    // the chunker is told the dispatch setting, dispatch applies the same
    // setting via `with_setting`, and the guard's measurement (`dispatched_uri_len`)
    // — now fed by `with_setting` — must equal the chunker's budget and stay
    // within the cap. The path width is tuned so the largest chunk lands in the
    // borderline window the post-guard delta opens. Before the fix this fails
    // two ways: the chunker over-packs (ignores `max_block_size`) and the guard
    // under-measures (ignores `with_setting`); both must hold for it to pass.
    #[test]
    fn chunked_routes_dispatch_uri_stays_within_cap_with_block_size_setting() {
        let client = client_with_settings(HashMap::new());
        let root_prefix = "1/";
        let dispatch_settings = [("max_block_size", STREAM_BLOCK_SIZE)];
        // Small paths give the chunker fine granularity, so the largest chunk
        // packs to within one item-cost (< the ~21 B `max_block_size` delta) of
        // the cap — the borderline window the post-guard bug needs.
        let path = "a".repeat(12);
        let paths: Vec<&str> = std::iter::repeat_n(path.as_str(), 6_000).collect();
        let build =
            |chunk: &[&str]| serde_json::json!({ "root_prefix": root_prefix, "paths": chunk });
        let dispatched_len = |chunk: &[&str]| {
            client
                .query(ROUTES_SQL_STUB)
                .param("root_prefix", root_prefix)
                .param("paths", chunk)
                .with_setting("max_block_size", STREAM_BLOCK_SIZE)
                .dispatched_uri_len()
        };

        // Bug reproduction: chunking blind to the dispatch setting (the pre-fix
        // behavior) packs a chunk whose URI clears the cap once `max_block_size`
        // is appended at dispatch — the exact #881 over-cap dispatch.
        let blind = client.chunk_params_to_fit_uri(&paths, build, crate::MAX_REQUEST_URI_LEN, &[]);
        assert!(
            blind
                .iter()
                .any(|c| dispatched_len(c) > crate::MAX_REQUEST_URI_LEN),
            "fixture must reproduce the bug: a setting-blind chunk must overflow at dispatch"
        );

        // Fix: reserving the dispatch setting in the chunk budget keeps every
        // dispatched URI within the cap, and the guard agrees.
        let chunks = client.chunk_params_to_fit_uri(
            &paths,
            build,
            crate::MAX_REQUEST_URI_LEN,
            &dispatch_settings,
        );
        assert!(chunks.len() > 1, "batch must split into multiple chunks");
        for chunk in &chunks {
            let len = dispatched_len(chunk);
            assert!(
                len <= crate::MAX_REQUEST_URI_LEN,
                "dispatched URI {len} over the {}-byte cap (chunk of {} paths)",
                crate::MAX_REQUEST_URI_LEN,
                chunk.len(),
            );
            let guarded = client
                .query(ROUTES_SQL_STUB)
                .param("root_prefix", root_prefix)
                .param("paths", *chunk)
                .with_setting("max_block_size", STREAM_BLOCK_SIZE);
            assert!(
                guarded.dispatch_within_cap(),
                "the guard the dispatch path runs must agree the chunk fits"
            );
        }
    }

    const ROUTES_SQL_STUB: &str = "SELECT source_id, path, traversal_path FROM routes";
    const STREAM_BLOCK_SIZE: &str = "65536";
}
