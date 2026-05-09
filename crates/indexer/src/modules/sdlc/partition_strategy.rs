use std::sync::Arc;

use async_trait::async_trait;

use crate::handler::HandlerError;
use crate::topic::{EntityIndexingRequest, PartitionBounds, PartitionSpec};

use super::pipeline::Pipeline;

const MAX_PARTITION_UPPER_BOUND: &str = "99999999999999999999";

// ---------------------------------------------------------------------------
// PartitionStrategy — decides how (or whether) to split an entity's work
// ---------------------------------------------------------------------------

#[async_trait]
pub(in crate::modules::sdlc) trait PartitionStrategy: Send + Sync {
    fn partition_count(&self) -> u32;
    fn partition_column(&self) -> &str;
    async fn compute_partitions(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<Option<Vec<PartitionSpec>>, HandlerError>;
}

// ---------------------------------------------------------------------------
// DatalakePartitionStrategy — splits by quantile boundaries from ClickHouse
// ---------------------------------------------------------------------------

pub(in crate::modules::sdlc) struct DatalakePartitionStrategy {
    source_table: String,
    partition_column: String,
    partition_count: u32,
    pipeline: Arc<Pipeline>,
}

impl DatalakePartitionStrategy {
    pub fn new(
        source_table: String,
        partition_column: String,
        partition_count: u32,
        pipeline: Arc<Pipeline>,
    ) -> Self {
        Self {
            source_table,
            partition_column,
            partition_count,
            pipeline,
        }
    }
}

#[async_trait]
impl PartitionStrategy for DatalakePartitionStrategy {
    fn partition_count(&self) -> u32 {
        self.partition_count
    }

    fn partition_column(&self) -> &str {
        &self.partition_column
    }

    async fn compute_partitions(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<Option<Vec<PartitionSpec>>, HandlerError> {
        let boundaries = self
            .pipeline
            .compute_boundaries(
                &self.source_table,
                &self.partition_column,
                &request.scope,
                self.partition_count,
                &request.watermark,
            )
            .await?;

        if boundaries.len() < (self.partition_count - 1) as usize {
            return Ok(None);
        }

        let partitions = (0..self.partition_count)
            .map(|i| build_partition_spec(i, self.partition_count, &boundaries))
            .collect();

        Ok(Some(partitions))
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

fn build_partition_spec(
    partition_index: u32,
    total_partitions: u32,
    boundaries: &[String],
) -> PartitionSpec {
    let lower_bound = if partition_index == 0 {
        String::new()
    } else {
        boundaries[(partition_index - 1) as usize].clone()
    };

    let upper_bound = if partition_index == total_partitions - 1 {
        MAX_PARTITION_UPPER_BOUND.to_string()
    } else {
        boundaries[partition_index as usize].clone()
    };

    PartitionSpec {
        partition_index,
        total_partitions,
        bounds: PartitionBounds::Range {
            lower_bound,
            upper_bound,
        },
    }
}

pub(crate) fn partition_column(order_by: &[String], scope: ontology::EtlScope) -> Option<&str> {
    let skip = match scope {
        ontology::EtlScope::Namespaced => 1,
        ontology::EtlScope::Global => 0,
    };
    order_by.get(skip).map(String::as_str)
}
