use arrow::record_batch::RecordBatch;
use circuit_breaker::CircuitBreaker;
use futures::stream::BoxStream;
use serde::Serialize;

use crate::arrow_client::{ArrowClickHouseClient, ArrowQuery, QuerySummary};
use crate::error::ClickHouseError;

#[derive(Clone)]
pub struct CircuitBreakingClickHouseClient {
    client: ArrowClickHouseClient,
    breaker: CircuitBreaker,
}

impl CircuitBreakingClickHouseClient {
    pub fn new(client: ArrowClickHouseClient, breaker: CircuitBreaker) -> Self {
        Self { client, breaker }
    }

    pub fn query(&self, sql: &str) -> CircuitBreakingQuery {
        CircuitBreakingQuery {
            inner: self.client.query(sql),
            breaker: self.breaker.clone(),
        }
    }

    pub async fn execute(&self, sql: &str) -> Result<(), ClickHouseError> {
        self.query(sql).execute().await
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
        self.breaker
            .call_with_filter(
                || self.client.insert_arrow(table, batches),
                ClickHouseError::is_transient,
            )
            .await
            .map_err(ClickHouseError::from_circuit_breaker)
    }

    pub fn client(&self) -> &ArrowClickHouseClient {
        &self.client
    }
}

impl std::fmt::Debug for CircuitBreakingClickHouseClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreakingClickHouseClient")
            .field("client", &self.client)
            .finish()
    }
}

pub struct CircuitBreakingQuery {
    inner: ArrowQuery,
    breaker: CircuitBreaker,
}

impl CircuitBreakingQuery {
    pub fn param(mut self, name: &str, value: impl Serialize) -> Self {
        self.inner = self.inner.param(name, value);
        self
    }

    pub fn with_setting(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.with_setting(name, value);
        self
    }

    pub async fn execute(self) -> Result<(), ClickHouseError> {
        self.breaker
            .call_with_filter(|| self.inner.execute(), ClickHouseError::is_transient)
            .await
            .map_err(ClickHouseError::from_circuit_breaker)
    }

    pub async fn fetch_arrow(self) -> Result<Vec<RecordBatch>, ClickHouseError> {
        let (batches, _) = self.fetch_arrow_with_summary().await?;
        Ok(batches)
    }

    pub async fn fetch_arrow_with_summary(
        self,
    ) -> Result<(Vec<RecordBatch>, Option<QuerySummary>), ClickHouseError> {
        self.breaker
            .call_with_filter(
                || self.inner.fetch_arrow_with_summary(),
                ClickHouseError::is_transient,
            )
            .await
            .map_err(ClickHouseError::from_circuit_breaker)
    }

    pub async fn fetch_arrow_stream(
        self,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ClickHouseError>>, ClickHouseError> {
        self.breaker
            .call_with_filter(
                || self.inner.fetch_arrow_stream(),
                ClickHouseError::is_transient,
            )
            .await
            .map_err(ClickHouseError::from_circuit_breaker)
    }

    pub async fn fetch_arrow_streamed(
        self,
        max_block_size: u64,
    ) -> Result<BoxStream<'static, Result<RecordBatch, ClickHouseError>>, ClickHouseError> {
        self.breaker
            .call_with_filter(
                || self.inner.fetch_arrow_streamed(max_block_size),
                ClickHouseError::is_transient,
            )
            .await
            .map_err(ClickHouseError::from_circuit_breaker)
    }
}
