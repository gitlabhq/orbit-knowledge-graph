use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery, QuerySummary};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;
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

/// ClickHouse's storage-scan figures from the `X-ClickHouse-Summary` header
/// (its `read_rows`/`read_bytes` fields), the query cost. Greater than or equal
/// to the rows the page actually returned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ScanStats {
    pub scanned_rows: u64,
    pub scanned_bytes: u64,
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

    /// Buffers a whole page and reports ClickHouse's scan cost from the query
    /// summary. The default reports zero scan stats; only [`Datalake`] surfaces
    /// the real summary.
    async fn query_batches_with_summary(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(Vec<RecordBatch>, ScanStats), DatalakeError> {
        let batches = self.query_batches(sql, params, max_block_size).await?;
        Ok((batches, ScanStats::default()))
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

    /// Builds an [`ArrowQuery`] from `sql` + `params`. The request-URI length
    /// guard now lives on the client's dispatch path ([`ArrowQuery`] fetch
    /// methods), so an over-cap batch fails with [`ClickHouseError::UriTooLong`]
    /// at dispatch (surfaced here as [`DatalakeError::Query`]) for every caller
    /// automatically — no per-caller guard (KG#881).
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

    async fn query_batches_with_summary(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(Vec<RecordBatch>, ScanStats), DatalakeError> {
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
        let (mut stream, summary) = query
            .fetch_arrow_streamed_with_summary(block_size)
            .await
            .map_err(|e| DatalakeError::Query(e.to_string()))?;

        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|e| DatalakeError::Query(e.to_string()))?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        let scan_stats = summary
            .await
            .ok()
            .flatten()
            .map(scan_stats_from_summary)
            .unwrap_or_default();

        Ok((batches, scan_stats))
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

/// True when a datalake read failed because an Arrow column hit the 2 GiB i32
/// offset cap. The pipeline's extract retry uses this to drop straight to the
/// floor block size rather than gradually halving.
pub(in crate::modules::sdlc) fn is_arrow_string_overflow(err: &DatalakeError) -> bool {
    let DatalakeError::Query(message) = err else {
        return false;
    };
    message.contains("2147483646")
        || (message.contains("Arrow") && message.contains("Capacity error"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_the_production_arrow_string_overflow() {
        let err = DatalakeError::Query(
            "query failed: Code: 1002. DB::Exception: Error with a Arrow column \"String\": \
             Capacity error: array cannot contain more than 2147483646 bytes, have 2147527792: \
             While executing Arrow. (UNKNOWN_EXCEPTION)"
                .to_string(),
        );
        assert!(is_arrow_string_overflow(&err));
    }

    #[test]
    fn ignores_unrelated_datalake_errors() {
        assert!(!is_arrow_string_overflow(&DatalakeError::Query(
            "Code: 159. DB::Exception: Timeout exceeded".to_string()
        )));
    }
}
