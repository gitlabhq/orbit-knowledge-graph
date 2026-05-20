use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;
use parking_lot::Mutex;

use super::datalake::{DatalakeError, DatalakeQuery, RecordBatchStream};
use super::dispatch::partitioning::Partitioner;
use super::metrics::SdlcMetrics;
use crate::checkpoint::{Checkpoint, CheckpointError, CheckpointStore};
use crate::scheduler::TaskError;
use crate::topic::{IndexingScope, PartitionBounds};

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

pub(crate) struct MockCheckpointStore {
    data: Mutex<HashMap<String, Checkpoint>>,
}

impl MockCheckpointStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_checkpoints(entries: Vec<(String, Checkpoint)>) -> Self {
        let data = entries.into_iter().collect();
        Self {
            data: Mutex::new(data),
        }
    }
}

#[async_trait]
impl CheckpointStore for MockCheckpointStore {
    async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
        Ok(self.data.lock().get(key).cloned())
    }

    async fn load_by_prefix(
        &self,
        entity_prefix: &str,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
        let data = self.data.lock();
        let prefix_with_dot = format!("{entity_prefix}.");
        Ok(data
            .iter()
            .filter(|(k, _)| k == &entity_prefix || k.starts_with(&prefix_with_dot))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn save_progress(
        &self,
        key: &str,
        checkpoint: &Checkpoint,
    ) -> Result<(), CheckpointError> {
        self.data.lock().insert(key.to_owned(), checkpoint.clone());
        Ok(())
    }

    async fn save_completed(
        &self,
        key: &str,
        watermark: &chrono::DateTime<chrono::Utc>,
    ) -> Result<(), CheckpointError> {
        self.data.lock().insert(
            key.to_owned(),
            Checkpoint {
                watermark: *watermark,
                cursor_values: None,
            },
        );
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), CheckpointError> {
        self.data.lock().remove(key);
        Ok(())
    }
}

pub(crate) struct MockPartitioner {
    boundaries: Vec<PartitionBounds>,
}

impl MockPartitioner {
    pub fn new(boundaries: Vec<PartitionBounds>) -> Self {
        Self { boundaries }
    }
}

#[async_trait]
impl Partitioner for MockPartitioner {
    async fn compute_boundaries(
        &self,
        _source_table: &str,
        _partition_column: &str,
        _num_partitions: u32,
        _scope: &IndexingScope,
    ) -> Result<Vec<PartitionBounds>, TaskError> {
        Ok(self.boundaries.clone())
    }
}
