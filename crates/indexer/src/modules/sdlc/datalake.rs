use std::sync::Arc;

use crate::clickhouse::ArrowClickHouseClient;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use circuit_breaker::CircuitBreaker;
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
}

pub(crate) struct CircuitBreakingDatalake<D> {
    inner: D,
    breaker: CircuitBreaker,
}

impl<D> CircuitBreakingDatalake<D> {
    pub fn new(inner: D, breaker: CircuitBreaker) -> Self {
        Self { inner, breaker }
    }
}

fn is_service_error(error: &DatalakeError) -> bool {
    matches!(error, DatalakeError::Query(_))
}

#[async_trait]
impl<D: DatalakeQuery> DatalakeQuery for CircuitBreakingDatalake<D> {
    async fn query_arrow(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        self.breaker
            .call_with_filter(
                || self.inner.query_arrow(sql, params, max_block_size),
                is_service_error,
            )
            .await
            .map_err(|e| match e {
                circuit_breaker::CircuitBreakerError::Open { service } => {
                    DatalakeError::Query(format!("circuit breaker open for {service}"))
                }
                circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
            })
    }

    async fn query_batches(
        &self,
        sql: &str,
        params: Value,
        max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError> {
        self.breaker
            .call_with_filter(
                || self.inner.query_batches(sql, params, max_block_size),
                is_service_error,
            )
            .await
            .map_err(|e| match e {
                circuit_breaker::CircuitBreakerError::Open { service } => {
                    DatalakeError::Query(format!("circuit breaker open for {service}"))
                }
                circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
            })
    }
}
