use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery, QuerySummary};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use serde_json::Value;
use thiserror::Error;
use tracing::debug;

#[derive(Debug, Error)]
pub(crate) enum DatalakeError {
    #[error("query failed: {0}")]
    Query(String),

    #[error("arrow decode error: {0}")]
    ArrowDecode(#[from] arrow::error::ArrowError),
}

impl From<clickhouse::error::Error> for DatalakeError {
    fn from(err: clickhouse::error::Error) -> Self {
        DatalakeError::Query(err.to_string())
    }
}

pub(crate) type RecordBatchStream<'a> = BoxStream<'a, Result<RecordBatch, DatalakeError>>;

/// Resolves once the stream is drained, since the summary follows the body.
pub(crate) type ReadStatsFuture = BoxFuture<'static, ReadStats>;

/// ClickHouse's scanned rows/bytes (`X-ClickHouse-Summary`), for cost attribution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReadStats {
    pub read_rows: u64,
    pub read_bytes: u64,
}

#[async_trait]
pub(crate) trait DatalakeQuery: Send + Sync {
    async fn query_arrow(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError>;

    async fn query_batches(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError>;

    /// Streams a page with its scanned-stats. The default reports zero stats;
    /// only [`Datalake`] surfaces the real summary.
    async fn query_arrow_with_summary(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(RecordBatchStream<'_>, ReadStatsFuture), DatalakeError> {
        let stream = self.query_arrow(sql, params, max_block_size).await?;
        Ok((stream, Box::pin(async { ReadStats::default() })))
    }
}

/// Rows per Arrow block when streaming a page, decoupled from the page `LIMIT`
/// so peak memory tracks one block rather than the whole page.
pub(crate) const DEFAULT_STREAM_BLOCK_SIZE: u64 = 65_536;

pub(crate) type DatalakeClient = Arc<ArrowClickHouseClient>;

pub(crate) struct Datalake {
    client: DatalakeClient,
    default_max_block_size: u64,
}

impl Datalake {
    pub fn new(client: DatalakeClient, default_max_block_size: u64) -> Self {
        Self {
            client,
            default_max_block_size,
        }
    }

    fn build_query(&self, sql: &str, params: Value) -> ArrowQuery {
        let mut query = self.client.query(sql);
        if let Value::Object(map) = params {
            for (key, value) in map {
                query = query.param(&key, value);
            }
        }
        query
    }
}

#[async_trait]
impl DatalakeQuery for Datalake {
    async fn query_arrow(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        let query = self.build_query(sql, params);

        let block_size = max_block_size.unwrap_or(self.default_max_block_size);
        let stream = query
            .fetch_arrow_streamed(block_size)
            .await
            .map_err(|e| DatalakeError::Query(e.to_string()))?;

        Ok(Box::pin(stream.map(|result| {
            result.map_err(|e| DatalakeError::Query(e.to_string()))
        })))
    }

    async fn query_batches(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError> {
        let mut stream = self.query_arrow(sql, params, max_block_size).await?;
        let mut batches = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        Ok(batches)
    }

    async fn query_arrow_with_summary(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(RecordBatchStream<'_>, ReadStatsFuture), DatalakeError> {
        let block_size = max_block_size.unwrap_or(self.default_max_block_size);
        let (stream, summary) = self
            .build_query(sql, params)
            .fetch_arrow_streamed_with_summary(block_size)
            .await
            .map_err(|e| DatalakeError::Query(e.to_string()))?;

        let stream = stream
            .map(|result| result.map_err(|e| DatalakeError::Query(e.to_string())))
            .boxed();
        let read_stats = Box::pin(async move {
            summary
                .await
                .ok()
                .flatten()
                .map(read_stats_from_summary)
                .unwrap_or_default()
        });

        Ok((stream, read_stats))
    }
}

fn read_stats_from_summary(summary: QuerySummary) -> ReadStats {
    let read_rows = summary.read_rows();
    let read_bytes = summary.read_bytes();
    if read_rows.is_none() || read_bytes.is_none() {
        debug!(
            ?read_rows,
            ?read_bytes,
            "datalake summary missing read_rows/read_bytes; defaulting to 0"
        );
    }
    ReadStats {
        read_rows: read_rows.unwrap_or(0),
        read_bytes: read_bytes.unwrap_or(0),
    }
}
