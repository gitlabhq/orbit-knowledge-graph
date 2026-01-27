//! Arrow-native ClickHouse client using HTTP protocol with ArrowStream format.

use std::io::Cursor;

use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use clickhouse::sql::Bind;
use clickhouse::{Client, query::Query};
use futures::stream;
use futures::stream::BoxStream;
use serde::Serialize;

use tracing::warn;

use super::error::ClickHouseError;

/// ClickHouse client that uses HTTP protocol with ArrowStream format for queries.
///
/// This client wraps the `clickhouse` crate and provides Arrow-native read operations
/// using the ArrowStream format via `arrow-ipc`.
#[derive(Clone)]
pub struct ArrowClickHouseClient {
    client: Client,
    base_url: String,
}

impl ArrowClickHouseClient {
    /// Creates a new Arrow ClickHouse client.
    ///
    /// The URL should be an HTTP URL like `http://127.0.0.1:8123`.
    pub fn new(url: &str, database: &str, username: &str, password: Option<&str>) -> Self {
        let mut client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(username);

        if let Some(password) = password {
            client = client.with_password(password);
        }

        Self {
            client,
            base_url: url.to_string(),
        }
    }

    /// Creates a new parameterized query builder.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Using client-side binding (? placeholders)
    /// let batches = client
    ///     .query("SELECT * FROM users WHERE id = ? AND name = ?")
    ///     .bind(42_u64)
    ///     .bind("alice")
    ///     .fetch_arrow()
    ///     .await?;
    ///
    /// // Using server-side parameters ({name: Type} syntax)
    /// let batches = client
    ///     .query("SELECT * FROM events WHERE ts > {watermark: String}")
    ///     .param("watermark", "2024-01-01 00:00:00")
    ///     .fetch_arrow()
    ///     .await?;
    /// ```
    pub fn query(&self, sql: &str) -> ArrowQuery {
        ArrowQuery {
            inner: self.client.query(sql),
        }
    }

    /// Executes a query and returns results as Arrow RecordBatches.
    ///
    /// For parameterized queries, use the `query()` builder instead.
    pub async fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, ClickHouseError> {
        self.query(sql).fetch_arrow().await
    }

    /// Executes a query and returns results as a stream of Arrow RecordBatches.
    ///
    /// For parameterized queries, use the `query()` builder instead.
    pub async fn query_arrow_stream(
        &self,
        sql: &str,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ClickHouseError>>, ClickHouseError> {
        self.query(sql).fetch_arrow_stream().await
    }

    /// Inserts Arrow RecordBatches into a table using ArrowStream format.
    ///
    /// Encodes the batches using `arrow-ipc` StreamWriter and sends via HTTP POST.
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

    /// Executes a SQL statement (DDL, INSERT, etc.) without returning results.
    ///
    /// For parameterized queries, use the `query()` builder instead.
    pub async fn execute(&self, sql: &str) -> Result<(), ClickHouseError> {
        self.query(sql).execute().await
    }

    /// Returns the underlying clickhouse Client for advanced operations.
    pub fn inner(&self) -> &Client {
        &self.client
    }
}

impl std::fmt::Debug for ArrowClickHouseClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowClickHouseClient")
            .field("base_url", &self.base_url)
            .finish()
    }
}

/// A parameterized query builder for Arrow results.
///
/// Supports both client-side binding (`?` placeholders) and server-side parameters
/// (`{name: Type}` syntax).
pub struct ArrowQuery {
    inner: Query,
}

impl ArrowQuery {
    /// Client-side parameter binding using `?` placeholders.
    ///
    /// Values are serialized and escaped into the SQL string before sending.
    ///
    /// # Example
    ///
    /// ```ignore
    /// client.query("SELECT * FROM users WHERE id = ? AND name = ?")
    ///     .bind(42_u64)
    ///     .bind("alice")
    ///     .fetch_arrow()
    ///     .await?;
    /// ```
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.inner = self.inner.bind(value);
        self
    }

    /// Server-side parameter using `{name: Type}` syntax.
    ///
    /// Values are sent as URL query parameters and ClickHouse interpolates them.
    ///
    /// # Example
    ///
    /// ```ignore
    /// client.query("SELECT * FROM events WHERE ts > {watermark: String}")
    ///     .param("watermark", "2024-01-01 00:00:00")
    ///     .fetch_arrow()
    ///     .await?;
    /// ```
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        self.inner = self.inner.param(name, value);
        self
    }

    /// Executes the query without returning results.
    ///
    /// Use this for DDL statements, INSERT without Arrow data, or other side-effect queries.
    pub async fn execute(self) -> Result<(), ClickHouseError> {
        self.inner.execute().await.map_err(ClickHouseError::Query)
    }

    /// Executes the query and returns results as Arrow RecordBatches.
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

    /// Executes the query and returns results as a stream of Arrow RecordBatches.
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
}
