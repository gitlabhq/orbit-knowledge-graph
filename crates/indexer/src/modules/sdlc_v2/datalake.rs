use std::sync::Arc;

use crate::clickhouse::ArrowClickHouseClient;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub(super) enum DatalakeError {
    #[error("query failed: {0}")]
    Query(String),
}

#[async_trait]
pub(super) trait DatalakeQuery: Send + Sync {
    /// Execute a parameterized SQL query and return all non-empty result batches.
    async fn query_batches(
        &self,
        sql: &str,
        params: Value,
    ) -> Result<Vec<RecordBatch>, DatalakeError>;
}

pub(super) struct Datalake {
    client: Arc<ArrowClickHouseClient>,
    max_block_size: u64,
}

impl Datalake {
    pub fn new(client: Arc<ArrowClickHouseClient>, max_block_size: u64) -> Self {
        Self {
            client,
            max_block_size,
        }
    }
}

#[async_trait]
impl DatalakeQuery for Datalake {
    async fn query_batches(
        &self,
        sql: &str,
        params: Value,
    ) -> Result<Vec<RecordBatch>, DatalakeError> {
        let mut query = self.client.query(sql);

        if let Value::Object(map) = params {
            for (key, value) in map {
                query = query.param(&key, value);
            }
        }

        let mut stream = query
            .fetch_arrow_streamed(self.max_block_size)
            .await
            .map_err(|err| DatalakeError::Query(err.to_string()))?;

        let mut batches = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.map_err(|err| DatalakeError::Query(err.to_string()))?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }

        Ok(batches)
    }
}
