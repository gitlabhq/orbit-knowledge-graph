use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;

use std::sync::Arc;

use super::datalake::{DatalakeError, DatalakeQuery, RecordBatchStream};
use super::metrics::SdlcMetrics;
use super::plan::Transformation;
use super::transform::{BlockTransform, TableBatch, TransformRegistry};
use crate::checkpoint::{Checkpoint, CheckpointError, CheckpointStore};
use crate::durability::WriteDurability;
use crate::handler::HandlerError;

pub(crate) fn test_metrics() -> SdlcMetrics {
    SdlcMetrics::with_meter(&crate::testkit::test_meter())
}

struct PassthroughTransform {
    outputs: Vec<String>,
}

#[async_trait]
impl BlockTransform for PassthroughTransform {
    fn name(&self) -> &str {
        "passthrough"
    }

    fn outputs(&self) -> &[String] {
        &self.outputs
    }

    async fn transform(&self, block: &RecordBatch) -> Result<Vec<TableBatch>, HandlerError> {
        Ok(vec![TableBatch {
            output_index: 0,
            batch: block.clone(),
        }])
    }
}

pub(in crate::modules::sdlc) fn passthrough_registry() -> Arc<TransformRegistry> {
    Arc::new(TransformRegistry::new(Arc::new(
        |_name, transforms: Vec<Transformation>| {
            Arc::new(PassthroughTransform {
                outputs: transforms
                    .into_iter()
                    .map(|transform| transform.destination_table)
                    .collect(),
            }) as Arc<dyn BlockTransform>
        },
    )))
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

    async fn save(
        &self,
        _key: &str,
        _checkpoint: &Checkpoint,
        _durability: WriteDurability,
    ) -> Result<(), CheckpointError> {
        Ok(())
    }

    async fn load_by_prefix(
        &self,
        _prefix: &str,
    ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
        Ok(Vec::new())
    }

    async fn consolidate(
        &self,
        _parent_key: &str,
        _watermark: &chrono::DateTime<chrono::Utc>,
    ) -> Result<(), CheckpointError> {
        Ok(())
    }
}
