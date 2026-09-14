use std::future::Future;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::buffer::Buffer as ArrowBuffer;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::{StreamDecoder, StreamReader};
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use circuit_breaker::CircuitBreakableError;
use clickhouse::{Client, query::Query};
use futures::StreamExt;
use futures::stream::BoxStream;
use orbit_utils::clickhouse::{ChScalar, ChType};
use serde::Serialize;
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::SyncIoBridge;
use tracing::warn;

pub use clickhouse::QuerySummary;

use crate::error::ClickHouseError;

/// ClickHouse rejects an async insert that also carries `insert_quorum`.
const ASYNC_INSERT_SETTING_KEYS: [&str; 2] = ["async_insert", "wait_for_async_insert"];

const REPLICATION_TRANSIENTS: [&str; 11] = [
    "UNSATISFIED_QUORUM",
    "REPLICA_IS_NOT_IN_QUORUM",
    "Session expired",
    "Connection loss",
    "Operation timeout",
    "is not finished on",
    "QUERY_WAS_CANCELLED",
    "NETWORK_ERROR",
    "502 Bad Gateway",
    "503 Service Unavailable",
    "504 Gateway Time-out",
];
const QUORUM_RETRY_ATTEMPTS: u32 = 20;

fn is_replication_transient(error: &ClickHouseError) -> bool {
    if error.is_transient() {
        return true;
    }
    let message = error.to_string();
    REPLICATION_TRANSIENTS
        .iter()
        .any(|text| message.contains(text))
}

fn quorum_backoff(attempt: u32) -> Duration {
    let base = (100 * u64::from(attempt)).min(1000);
    Duration::from_millis(base + rand::random_range(0..=base / 4))
}

async fn retry_quorum_conflicts<T, F, Fut>(enabled: bool, mut op: F) -> Result<T, ClickHouseError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ClickHouseError>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Err(error)
                if enabled
                    && attempt < QUORUM_RETRY_ATTEMPTS
                    && is_replication_transient(&error) =>
            {
                attempt += 1;
                tokio::time::sleep(quorum_backoff(attempt)).await;
            }
            result => return result,
        }
    }
}

#[derive(Clone)]
pub struct ArrowClickHouseClient {
    client: Client,
    base_url: String,
    database: String,
    insert_settings: std::collections::HashMap<String, String>,
    quorum_writes: bool,
    replicated: bool,
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
            .with_user(username)
            .with_setting("output_format_arrow_string_as_string", "1")
            .with_setting("output_format_arrow_fixed_string_as_fixed_byte_array", "1")
            .with_setting("use_query_condition_cache", "true")
            .with_setting("join_use_nulls", "0")
            .with_setting("query_plan_join_swap_table", "auto")
            .with_setting("optimize_aggregation_in_order", "1");

        if let Some(password) = password {
            client = client.with_password(password);
        }

        for (k, v) in session_settings {
            client = client.with_setting(k, v);
        }

        Self {
            client,
            base_url: url.to_string(),
            database: database.to_string(),
            insert_settings: insert_settings.clone(),
            quorum_writes: has_quorum_insert_setting(session_settings, insert_settings),
            replicated: false,
        }
    }

    pub fn with_replicated(mut self, replicated: bool) -> Self {
        self.replicated = replicated;
        self
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn has_quorum_writes(&self) -> bool {
        self.quorum_writes
    }

    /// The connection points at a self-managed cluster with more than one replica.
    pub fn is_replicated(&self) -> bool {
        self.replicated
    }

    pub fn query(&self, sql: &str) -> ArrowQuery {
        ArrowQuery {
            inner: self.client.query(sql),
            retry_quorum_conflicts: self.quorum_writes,
        }
    }

    /// Use this for `INSERT` queries so they inherit async-insert and
    /// other write-specific settings. Use `query()` for read operations.
    pub fn insert_query(&self, sql: &str) -> ArrowQuery {
        let mut q = self.query(sql);
        for (k, v) in &self.insert_settings {
            if self.should_drop_async_insert_setting(k) {
                continue;
            }
            q = q.with_setting(k, v);
        }
        q
    }

    /// Sorted so the emitted `SETTINGS` clause is deterministic.
    fn insert_settings_clause(&self, overrides: &[(&str, &str)]) -> String {
        let mut merged: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
        for (k, v) in &self.insert_settings {
            merged.insert(k, v);
        }
        for &(k, v) in overrides {
            merged.insert(k, v);
        }
        merged.retain(|k, _| !self.should_drop_async_insert_setting(k));
        if merged.is_empty() {
            return String::new();
        }
        let pairs: Vec<String> = merged.iter().map(|(k, v)| format!("{k}={v}")).collect();
        format!(" SETTINGS {}", pairs.join(", "))
    }

    fn should_drop_async_insert_setting(&self, key: &str) -> bool {
        self.quorum_writes && ASYNC_INSERT_SETTING_KEYS.contains(&key)
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
        batches: Vec<RecordBatch>,
    ) -> Result<(), ClickHouseError> {
        let sql = self.build_insert_sql(table);
        self.insert_arrow_streaming_with_sql(table, &sql, batches)
            .await
    }

    pub async fn insert_arrow_streaming_with_sql(
        &self,
        table: &str,
        sql: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), ClickHouseError> {
        if batches.is_empty() {
            return Ok(());
        }
        if !self.quorum_writes {
            return self.stream_insert(table, sql, batches).await;
        }
        retry_quorum_conflicts(true, || self.stream_insert(table, sql, batches.clone())).await
    }

    async fn stream_insert(
        &self,
        table: &str,
        sql: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), ClickHouseError> {
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

        for (batch_index, batch) in batches.into_iter().enumerate() {
            if batch.schema() != schema {
                warn!(table, batch_index, "RecordBatch schema mismatch");
            }
            writer.write(&batch).map_err(ClickHouseError::ArrowEncode)?;
            drop(batch);
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
    retry_quorum_conflicts: bool,
}

type FirstChunk = (clickhouse::query::BytesCursor, Option<Bytes>);

/// A quorum conflict arrives with the first chunk, before any rows, so only that read retries.
async fn open_cursor(query: Query) -> Result<FirstChunk, ClickHouseError> {
    let mut cursor = query
        .fetch_bytes("ArrowStream")
        .map_err(ClickHouseError::Query)?;
    let first = cursor.next().await.map_err(ClickHouseError::Query)?;
    Ok((cursor, first))
}

async fn collect_bytes(query: Query) -> Result<(Vec<u8>, Option<QuerySummary>), ClickHouseError> {
    let (mut cursor, first) = open_cursor(query).await?;
    let mut buffer = first.map(|chunk| chunk.to_vec()).unwrap_or_default();
    while let Some(chunk) = cursor.next().await.map_err(ClickHouseError::Query)? {
        buffer.extend(chunk);
    }
    Ok((buffer, cursor.summary().cloned()))
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

    /// For statements that are not idempotent, such as `ATTACH PARTITION ... FROM`.
    pub fn without_quorum_retry(mut self) -> Self {
        self.retry_quorum_conflicts = false;
        self
    }

    pub async fn execute(self) -> Result<(), ClickHouseError> {
        if !self.retry_quorum_conflicts {
            return self.inner.execute().await.map_err(ClickHouseError::Query);
        }
        retry_quorum_conflicts(true, || async {
            self.inner
                .clone()
                .execute()
                .await
                .map_err(ClickHouseError::Query)
        })
        .await
    }

    pub async fn fetch_arrow(self) -> Result<Vec<RecordBatch>, ClickHouseError> {
        let (batches, _) = self.fetch_arrow_with_summary().await?;
        Ok(batches)
    }

    pub async fn fetch_arrow_with_summary(
        self,
    ) -> Result<(Vec<RecordBatch>, Option<QuerySummary>), ClickHouseError> {
        let (buffer, summary) = retry_quorum_conflicts(self.retry_quorum_conflicts, || {
            collect_bytes(self.inner.clone())
        })
        .await?;

        if buffer.is_empty() {
            return Ok((Vec::new(), summary));
        }

        let reader = StreamReader::try_new(Cursor::new(buffer), None)
            .map_err(ClickHouseError::ArrowDecode)?;
        let batches: Result<Vec<_>, _> = reader
            .map(|result| result.map_err(ClickHouseError::ArrowDecode))
            .collect();
        Ok((batches?, summary))
    }

    async fn open_cursor(
        mut self,
        max_block_size: Option<u64>,
    ) -> Result<FirstChunk, ClickHouseError> {
        if let Some(max_block_size) = max_block_size {
            self.inner = self
                .inner
                .with_setting("max_block_size", max_block_size.to_string());
        }
        retry_quorum_conflicts(self.retry_quorum_conflicts, || {
            open_cursor(self.inner.clone())
        })
        .await
    }

    pub async fn fetch_arrow_streamed(
        self,
        max_block_size: Option<u64>,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ClickHouseError>>, ClickHouseError> {
        let (cursor, first) = self.open_cursor(max_block_size).await?;

        let handle = tokio::runtime::Handle::current();
        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ClickHouseError>>(2);

        tokio::task::spawn_blocking(move || {
            let body = Cursor::new(first.unwrap_or_default()).chain(cursor);
            let bridge = SyncIoBridge::new_with_handle(body, handle);
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

    pub async fn fetch_arrow_streamed_with_summary(
        self,
        max_block_size: Option<u64>,
    ) -> Result<
        (
            BoxStream<'static, Result<RecordBatch, ClickHouseError>>,
            oneshot::Receiver<Option<QuerySummary>>,
        ),
        ClickHouseError,
    > {
        let (mut cursor, first) = self.open_cursor(max_block_size).await?;

        let (tx, rx) = mpsc::channel::<Result<RecordBatch, ClickHouseError>>(2);
        let (summary_tx, summary_rx) = oneshot::channel();

        tokio::spawn(async move {
            let mut decoder = StreamDecoder::new();
            let mut summary_tx = Some(summary_tx);
            let mut next = Ok(first);
            loop {
                match next {
                    Ok(Some(chunk)) => {
                        if let Some(summary_tx) = summary_tx.take() {
                            let _ = summary_tx.send(cursor.summary().cloned());
                        }
                        let mut buffer = ArrowBuffer::from(chunk.as_ref());
                        while !tx.is_closed() && !buffer.is_empty() {
                            match decoder.decode(&mut buffer) {
                                Ok(Some(batch)) => {
                                    if tx.send(Ok(batch)).await.is_err() {
                                        break;
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
                next = cursor.next().await;
            }
            if let Some(summary_tx) = summary_tx.take() {
                let _ = summary_tx.send(cursor.summary().cloned());
            }
        });

        Ok((ReceiverStream::new(rx).boxed(), summary_rx))
    }
}

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

fn has_quorum_insert_setting(
    session_settings: &std::collections::HashMap<String, String>,
    insert_settings: &std::collections::HashMap<String, String>,
) -> bool {
    [session_settings, insert_settings]
        .into_iter()
        .any(|settings| {
            settings
                .get("insert_quorum")
                .is_some_and(|value| value != "0")
        })
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

    fn client_with_insert_settings(
        insert_settings: HashMap<String, String>,
    ) -> ArrowClickHouseClient {
        client_with_session_and_insert_settings(HashMap::new(), insert_settings)
    }

    fn client_with_session_and_insert_settings(
        session_settings: HashMap<String, String>,
        insert_settings: HashMap<String, String>,
    ) -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            "http://localhost:8123",
            "default",
            "default",
            None,
            &session_settings,
            &insert_settings,
        )
    }

    fn insert_quorum_setting() -> HashMap<String, String> {
        HashMap::from([("insert_quorum".to_string(), "auto".to_string())])
    }

    #[test]
    fn no_settings_emits_no_clause() {
        let client = client_with_insert_settings(HashMap::new());
        assert_eq!(
            client.build_insert_sql("t"),
            "INSERT INTO t FORMAT ArrowStream"
        );
    }

    #[test]
    fn config_settings_sort_for_deterministic_sql() {
        let client = client_with_insert_settings(HashMap::from([
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
        let client = client_with_insert_settings(HashMap::from([(
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
    fn quorum_writes_drop_async_insert_overrides() {
        let client =
            client_with_session_and_insert_settings(insert_quorum_setting(), HashMap::new());
        let overrides = [("async_insert", "1"), ("wait_for_async_insert", "1")];
        assert_eq!(
            client.build_insert_sql_with_overrides("t", &overrides),
            "INSERT INTO t FORMAT ArrowStream"
        );
    }

    #[test]
    fn quorum_writes_drop_configured_async_insert_settings() {
        let client = client_with_session_and_insert_settings(
            insert_quorum_setting(),
            HashMap::from([
                ("async_insert".to_string(), "1".to_string()),
                ("optimize_on_insert".to_string(), "0".to_string()),
            ]),
        );
        assert_eq!(
            client.build_insert_sql("t"),
            "INSERT INTO t SETTINGS optimize_on_insert=0 FORMAT ArrowStream"
        );
    }

    #[test]
    fn quorum_writes_detected_from_insert_settings() {
        let client = client_with_insert_settings(insert_quorum_setting());
        assert!(client.has_quorum_writes());
    }

    #[test]
    fn replicated_is_off_by_default() {
        let client = client_with_insert_settings(HashMap::new());
        assert!(!client.is_replicated());
        assert!(client.with_replicated(true).is_replicated());
    }

    fn bad_response(message: &str) -> ClickHouseError {
        ClickHouseError::Query(clickhouse::error::Error::BadResponse(message.to_string()))
    }

    #[test]
    fn replication_transients_cover_quorum_and_keeper_errors() {
        for message in [
            "Code: 286. DB::Exception: Quorum for previous write has not been satisfied yet. (UNSATISFIED_QUORUM)",
            "Code: 289. DB::Exception: Replica doesn't have part. (REPLICA_IS_NOT_IN_QUORUM)",
            "Code: 999. Coordination::Exception: Session expired. (KEEPER_EXCEPTION)",
            "Code: 159. DB::Exception: ReplicatedDatabase DDL task /clickhouse/databases/gkg/log/query-0000000007 is not finished on 1 of 3 hosts",
            "Code: 394. DB::Exception: Query was cancelled. (QUERY_WAS_CANCELLED)",
            "Code: 210. DB::NetException: I/O error: Broken pipe, while writing to socket. (NETWORK_ERROR)",
            "<html><body><h1>503 Service Unavailable</h1>\nNo server is available to handle this request.\n</body></html>",
        ] {
            assert!(
                is_replication_transient(&bad_response(message)),
                "{message}"
            );
        }
        assert!(is_replication_transient(&ClickHouseError::Query(
            clickhouse::error::Error::Network(Box::new(std::io::Error::other("reset")))
        )));
        assert!(is_replication_transient(&ClickHouseError::BadResponse {
            status: 503,
            body: "<html><body><h1>503 Service Unavailable</h1>".into(),
        }));
        assert!(!is_replication_transient(&bad_response(
            "Code: 60. DB::Exception: Table gkg.missing does not exist. (UNKNOWN_TABLE)"
        )));
    }

    #[tokio::test]
    async fn quorum_conflicts_retry_until_success() {
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = retry_quorum_conflicts(true, || async {
            let n = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < 3 {
                Err(bad_response("(UNSATISFIED_QUORUM)"))
            } else {
                Ok(n)
            }
        })
        .await;
        assert_eq!(result.unwrap(), 3);
    }

    #[tokio::test]
    async fn quorum_conflicts_surface_when_retries_are_disabled() {
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result: Result<(), _> = retry_quorum_conflicts(false, || async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(bad_response("(REPLICA_IS_NOT_IN_QUORUM)"))
        })
        .await;
        assert!(is_replication_transient(&result.unwrap_err()));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn insert_quorum_zero_keeps_async_inserts() {
        let client = client_with_session_and_insert_settings(
            HashMap::from([("insert_quorum".to_string(), "0".to_string())]),
            HashMap::new(),
        );
        assert!(!client.has_quorum_writes());
    }
}
