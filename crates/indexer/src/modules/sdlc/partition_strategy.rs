use std::sync::Arc;

use arrow::array::{Array, Float64Array, ListArray};
use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;

use crate::handler::HandlerError;
use crate::topic::{EntityIndexingRequest, IndexingScope, PartitionBounds, PartitionSpec};

use super::datalake::DatalakeQuery;

const MAX_PARTITION_UPPER_BOUND: &str = "99999999999999999999";

// ---------------------------------------------------------------------------
// PartitionStrategy — decides how (or whether) to split an entity's work
// ---------------------------------------------------------------------------

#[async_trait]
pub(in crate::modules::sdlc) trait PartitionStrategy: Send + Sync {
    fn partition_count(&self) -> u32;
    fn partition_column(&self) -> &str;

    async fn compute_quantiles(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<Vec<String>, HandlerError>;

    async fn compute_partitions(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<Option<Vec<PartitionSpec>>, HandlerError> {
        let quantiles = self.compute_quantiles(request).await?;

        if quantiles.len() < (self.partition_count() - 1) as usize {
            return Ok(None);
        }

        let partitions = (0..self.partition_count())
            .map(|i| build_partition_spec(i, self.partition_count(), &quantiles))
            .collect();

        Ok(Some(partitions))
    }
}

// ---------------------------------------------------------------------------
// DatalakePartitionStrategy — splits by quantiles from ClickHouse
// ---------------------------------------------------------------------------

pub(in crate::modules::sdlc) struct DatalakePartitionStrategy {
    source_table: String,
    partition_column: String,
    partition_count: u32,
    datalake: Arc<dyn DatalakeQuery>,
}

impl DatalakePartitionStrategy {
    pub fn new(
        source_table: String,
        partition_column: String,
        partition_count: u32,
        datalake: Arc<dyn DatalakeQuery>,
    ) -> Self {
        Self {
            source_table,
            partition_column,
            partition_count,
            datalake,
        }
    }

    async fn query_quantiles(
        &self,
        scope: &IndexingScope,
        watermark: &DateTime<Utc>,
    ) -> Result<Vec<String>, HandlerError> {
        let quantile_positions: Vec<String> = (1..self.partition_count)
            .map(|i| format!("{}", i as f64 / self.partition_count as f64))
            .collect();
        let quantile_list = quantile_positions.join(", ");

        let scope_filter = match scope {
            IndexingScope::Global => "1=1".to_string(),
            IndexingScope::Namespace { traversal_path, .. } => {
                format!("startsWith(traversal_path, '{traversal_path}')")
            }
        };

        let watermark_str = watermark.format(crate::clickhouse::TIMESTAMP_FORMAT);

        let sql = format!(
            "SELECT quantilesTDigest({quantile_list})({partition_column}) \
             FROM {source_table} \
             WHERE {scope_filter} \
             AND _siphon_replicated_at <= '{watermark_str}'",
            partition_column = self.partition_column,
            source_table = self.source_table,
        );

        let batches = self
            .datalake
            .query_batches(&sql, serde_json::json!({}), None)
            .await
            .map_err(|err| HandlerError::Processing(format!("quantile query failed: {err}")))?;

        let batch = match batches.into_iter().next() {
            Some(batch) if batch.num_rows() > 0 => batch,
            _ => return Ok(vec![]),
        };

        let column = batch.column(0);
        let list_array = column.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            HandlerError::Processing("expected ListArray from quantile query".into())
        })?;

        if list_array.is_empty() || list_array.is_null(0) {
            return Ok(vec![]);
        }

        let values = list_array.value(0);
        let float_array = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                HandlerError::Processing("expected Float64Array inside quantile result".into())
            })?;

        Ok(float_array
            .iter()
            .filter_map(|v| v.map(|f| format!("{}", f.floor() as i64)))
            .collect())
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

    async fn compute_quantiles(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<Vec<String>, HandlerError> {
        self.query_quantiles(&request.scope, &request.watermark)
            .await
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

fn build_partition_spec(
    partition_index: u32,
    total_partitions: u32,
    quantiles: &[String],
) -> PartitionSpec {
    let lower_bound = if partition_index == 0 {
        String::new()
    } else {
        quantiles[(partition_index - 1) as usize].clone()
    };

    let upper_bound = if partition_index == total_partitions - 1 {
        MAX_PARTITION_UPPER_BOUND.to_string()
    } else {
        quantiles[partition_index as usize].clone()
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
