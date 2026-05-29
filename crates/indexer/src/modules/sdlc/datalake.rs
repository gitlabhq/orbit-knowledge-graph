use std::sync::Arc;

use crate::clickhouse::ArrowClickHouseClient;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::Value;
use thiserror::Error;

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

/// Resource stats reported by the datalake for a single read, parsed from the
/// `X-ClickHouse-Summary` response header. Used for indexing cost attribution.
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

    /// Like [`query_batches`](Self::query_batches), but also returns the
    /// datalake's `read_rows`/`read_bytes` for the query. The default delegates
    /// to `query_batches` with empty stats so test doubles need no changes.
    async fn query_batches_with_summary(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(Vec<RecordBatch>, ReadStats), DatalakeError> {
        let batches = self.query_batches(sql, params, max_block_size).await?;
        Ok((batches, ReadStats::default()))
    }
}

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
}

#[async_trait]
impl DatalakeQuery for Datalake {
    async fn query_arrow(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        let mut query = self.client.query(sql);

        if let Value::Object(map) = params {
            for (key, value) in map {
                query = query.param(&key, value);
            }
        }

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

    async fn query_batches_with_summary(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<(Vec<RecordBatch>, ReadStats), DatalakeError> {
        let mut query = self.client.query(sql);

        if let Value::Object(map) = params {
            for (key, value) in map {
                query = query.param(&key, value);
            }
        }

        let block_size = max_block_size.unwrap_or(self.default_max_block_size);
        query = query.with_setting("max_block_size", block_size.to_string());

        let (batches, summary) = query
            .fetch_arrow_with_summary()
            .await
            .map_err(|e| DatalakeError::Query(e.to_string()))?;

        let batches = batches
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect();

        let read_stats = summary
            .map(|summary| ReadStats {
                read_rows: summary.read_rows().unwrap_or(0),
                read_bytes: summary.read_bytes().unwrap_or(0),
            })
            .unwrap_or_default();

        Ok((batches, read_stats))
    }
}
