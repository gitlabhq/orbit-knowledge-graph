use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;

use super::datalake::{DatalakeError, DatalakeQuery, RecordBatchStream};
use super::metrics::SdlcMetrics;
use crate::checkpoint::{Checkpoint, CheckpointError, CheckpointStore};

pub(crate) fn test_metrics() -> SdlcMetrics {
    SdlcMetrics::with_meter(&crate::testkit::test_meter())
}

pub(crate) struct EmptyDatalake;

#[async_trait]
impl DatalakeQuery for EmptyDatalake {
    async fn query_arrow(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        Ok(Box::pin(stream::empty()))
    }

    async fn query_batches(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError> {
        Ok(vec![])
    }
}

pub(crate) struct FailingDatalake;

#[async_trait]
impl DatalakeQuery for FailingDatalake {
    async fn query_arrow(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<RecordBatchStream<'_>, DatalakeError> {
        Err(DatalakeError::Query("simulated failure".to_string()))
    }

    async fn query_batches(
        &self,
        _sql: &str,
        _params: serde_json::Value,
        _max_block_size: Option<u64>,
    ) -> Result<Vec<RecordBatch>, DatalakeError> {
        Err(DatalakeError::Query("simulated failure".to_string()))
    }
}

pub(crate) struct MockCheckpointStore;

#[async_trait]
impl CheckpointStore for MockCheckpointStore {
    async fn load(&self, _key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(None)
    }

    async fn save_progress(
        &self,
        _key: &str,
        _checkpoint: &Checkpoint,
    ) -> Result<(), CheckpointError> {
        Ok(())
    }

    async fn save_completed(
        &self,
        _key: &str,
        _watermark: &chrono::DateTime<chrono::Utc>,
    ) -> Result<(), CheckpointError> {
        Ok(())
    }
}
