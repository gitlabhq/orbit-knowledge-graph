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

/// Rows and bytes a page actually returned from the datalake, counted from the
/// result blocks. The pipeline fills these in as it drains the stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReadStats {
    pub read_rows: u64,
    pub read_bytes: u64,
}

/// ClickHouse's storage-scan figures from the `X-ClickHouse-Summary` header
/// (its `read_rows`/`read_bytes` fields), the query cost. Greater than or equal
/// to the rows actually returned ([`ReadStats`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ScanStats {
    pub scanned_rows: u64,
    pub scanned_bytes: u64,
}

/// Resolves once the page body is drained, since the summary follows it.
pub(crate) type ScanStatsFuture = BoxFuture<'static, ScanStats>;

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

    /// Streams a page and yields its scan cost once drained. The default reports
    /// zero stats; only [`Datalake`] surfaces the real summary.
    async fn query_arrow_with_scan(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(RecordBatchStream<'_>, ScanStatsFuture), DatalakeError> {
        let stream = self.query_arrow(sql, params, max_block_size).await?;
        Ok((stream, Box::pin(async { ScanStats::default() })))
    }
}

/// Byte cap for retry blocks, keeping a block's String column under the 2GB
/// Arrow offset limit even for MB-wide rows. ClickHouse's own default.
const RETRY_PREFERRED_BLOCK_SIZE_BYTES: &str = "1000000";

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
        // The Arrow 2GB-overflow byte-cap lives in `query_arrow_with_scan`; this
        // path only serves `query_batches`, which always passes `None`.
        let block_size = max_block_size.unwrap_or(self.default_max_block_size);
        let query = self.build_query(sql, params);
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

    async fn query_arrow_with_scan(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(RecordBatchStream<'_>, ScanStatsFuture), DatalakeError> {
        let block_size = max_block_size.unwrap_or(self.default_max_block_size);
        let mut query = self.build_query(sql, params);
        if max_block_size.is_some() {
            // Retry after a datalake failure (the Arrow 2GB overflow): byte-cap
            // blocks so the retry is safe regardless of row width.
            query = query.with_setting(
                "preferred_block_size_bytes",
                RETRY_PREFERRED_BLOCK_SIZE_BYTES,
            );
        }
        let (stream, summary) = query
            .fetch_arrow_streamed_with_summary(block_size)
            .await
            .map_err(|e| DatalakeError::Query(e.to_string()))?;

        let stream = stream
            .map(|result| result.map_err(|e| DatalakeError::Query(e.to_string())))
            .boxed();
        let scan_stats = Box::pin(async move {
            summary
                .await
                .ok()
                .flatten()
                .map(scan_stats_from_summary)
                .unwrap_or_default()
        });

        Ok((stream, scan_stats))
    }
}

fn scan_stats_from_summary(summary: QuerySummary) -> ScanStats {
    let scanned_rows = summary.read_rows();
    let scanned_bytes = summary.read_bytes();
    if scanned_rows.is_none() || scanned_bytes.is_none() {
        debug!(
            ?scanned_rows,
            ?scanned_bytes,
            "datalake summary missing read_rows/read_bytes; defaulting to 0"
        );
    }
    ScanStats {
        scanned_rows: scanned_rows.unwrap_or(0),
        scanned_bytes: scanned_bytes.unwrap_or(0),
    }
}
