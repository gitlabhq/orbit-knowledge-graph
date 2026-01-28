use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use etl_engine::clickhouse::ArrowClickHouseClient;
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
    ) -> Result<RecordBatchStream<'_>, DatalakeError>;
}

pub(crate) type DatalakeClient = Arc<ArrowClickHouseClient>;

pub(crate) struct Datalake {
    client: DatalakeClient,
}

impl Datalake {
    pub fn new(client: DatalakeClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DatalakeQuery for Datalake {
    async fn query_arrow(
        &self,
        sql: &str,
        params: Value,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        let mut query = self.client.query(sql);

        if let Value::Object(map) = params {
            for (key, value) in map {
                query = query.param(&key, value);
            }
        }

        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| DatalakeError::Query(e.to_string()))?;

        Ok(Box::pin(futures::stream::iter(batches.into_iter().map(Ok))))
    }
}
