use std::collections::HashMap;

use arrow::array::{Array, Int64Array};
use gkg_utils::arrow::ArrowUtils;
use serde_json::Value;
use tracing::debug;

use crate::handler::HandlerError;

use super::datalake::DatalakeQuery;
use super::plan::input::PlanInput;

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct PartitionAssignment {
    pub index: u32,
    pub total: u32,
    pub column: String,
    pub bounds: PartitionBounds,
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) enum PartitionBounds {
    Range {
        lower_bound: String,
        upper_bound: String,
    },
}

impl PartitionBounds {
    pub fn lower_bound(&self) -> &str {
        match self {
            PartitionBounds::Range { lower_bound, .. } => lower_bound,
        }
    }

    pub fn upper_bound(&self) -> &str {
        match self {
            PartitionBounds::Range { upper_bound, .. } => upper_bound,
        }
    }
}

impl PartitionAssignment {
    pub fn position_suffix(&self) -> String {
        format!(".p{}of{}", self.index, self.total)
    }
}

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct PartitionStrategy {
    pub count: u32,
    pub column: String,
    pub datalake_table: String,
}

impl PartitionStrategy {
    pub async fn compute_ranges(
        &self,
        datalake: &dyn DatalakeQuery,
        traversal_path: Option<&str>,
    ) -> Result<Vec<PartitionAssignment>, HandlerError> {
        compute_partition_ranges(
            datalake,
            &self.datalake_table,
            &self.column,
            self.count,
            traversal_path,
        )
        .await
    }
}

pub(in crate::modules::sdlc) fn build_strategies(
    inputs: &PlanInput,
    overrides: &HashMap<String, u32>,
) -> HashMap<String, PartitionStrategy> {
    inputs
        .node_plans
        .iter()
        .filter_map(|node| {
            let count = *overrides.get(&node.name)?;
            if count <= 1 {
                return None;
            }
            let column = partition_column(&node.extract.order_by)?.to_string();
            Some((
                node.name.clone(),
                PartitionStrategy {
                    count,
                    column,
                    datalake_table: node.extract.base_table.clone(),
                },
            ))
        })
        .collect()
}

fn partition_column(sort_key: &[String]) -> Option<&str> {
    sort_key
        .iter()
        .map(String::as_str)
        .find(|col| *col != "traversal_path")
}

async fn compute_partition_ranges(
    datalake: &dyn DatalakeQuery,
    table: &str,
    column: &str,
    count: u32,
    traversal_path: Option<&str>,
) -> Result<Vec<PartitionAssignment>, HandlerError> {
    if count <= 1 {
        return Ok(Vec::new());
    }

    let (where_clause, params) = match traversal_path {
        Some(path) => (
            " WHERE startsWith(traversal_path, {traversal_path:String})".to_string(),
            serde_json::json!({ "traversal_path": path }),
        ),
        None => (String::new(), Value::Null),
    };

    let sql = format!(
        "SELECT min({column}) AS min_val, max({column}) AS max_val FROM {table}{where_clause}"
    );

    let batches = datalake
        .query_batches(&sql, params, None)
        .await
        .map_err(|err| HandlerError::Processing(format!("partition probe failed: {err}")))?;

    let Some(batch) = batches.into_iter().next() else {
        return Ok(Vec::new());
    };
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let min: Option<i64> = ArrowUtils::get_column_by_index::<Int64Array>(&batch, 0)
        .filter(|arr| !arr.is_null(0))
        .map(|arr| arr.value(0));
    let max: Option<i64> = ArrowUtils::get_column_by_index::<Int64Array>(&batch, 1)
        .filter(|arr| !arr.is_null(0))
        .map(|arr| arr.value(0));

    let (Some(min), Some(max)) = (min, max) else {
        return Ok(Vec::new());
    };

    let span = max.saturating_sub(min);
    if span < count as i64 {
        debug!(min, max, count, "skipping partitioning: span too small");
        return Ok(Vec::new());
    }

    let count_i64 = count as i64;
    let step = (span + count_i64) / count_i64;

    let mut assignments = Vec::with_capacity(count as usize);
    for i in 0..count {
        let lower = min + step * i as i64;
        let upper = (lower + step).min(max + 1);
        if lower >= upper {
            break;
        }
        assignments.push(PartitionAssignment {
            index: i,
            total: count,
            column: column.to_string(),
            bounds: PartitionBounds::Range {
                lower_bound: lower.to_string(),
                upper_bound: upper.to_string(),
            },
        });
    }

    Ok(assignments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::datalake::{DatalakeError, RecordBatchStream};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};

    struct MinMaxDatalake {
        min_val: i64,
        max_val: i64,
        captured_sql: Mutex<String>,
        captured_params: Mutex<Value>,
    }

    impl MinMaxDatalake {
        fn new(min_val: i64, max_val: i64) -> Self {
            Self {
                min_val,
                max_val,
                captured_sql: Mutex::new(String::new()),
                captured_params: Mutex::new(Value::Null),
            }
        }
    }

    #[async_trait]
    impl DatalakeQuery for MinMaxDatalake {
        async fn query_arrow(
            &self,
            _sql: &str,
            _params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<RecordBatchStream<'_>, DatalakeError> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn query_batches(
            &self,
            sql: &str,
            params: Value,
            _max_block_size: Option<u64>,
        ) -> Result<Vec<RecordBatch>, DatalakeError> {
            *self.captured_sql.lock().unwrap() = sql.to_string();
            *self.captured_params.lock().unwrap() = params;

            let schema = Arc::new(Schema::new(vec![
                Field::new("min_val", DataType::Int64, false),
                Field::new("max_val", DataType::Int64, false),
            ]));
            Ok(vec![
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(vec![self.min_val])),
                        Arc::new(Int64Array::from(vec![self.max_val])),
                    ],
                )
                .unwrap(),
            ])
        }
    }

    #[tokio::test]
    async fn even_split_yields_disjoint_ranges() {
        let datalake = MinMaxDatalake::new(0, 99);
        let ranges = compute_partition_ranges(&datalake, "t", "id", 4, None)
            .await
            .unwrap();

        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0].bounds.lower_bound(), "0");
        assert_eq!(ranges[0].bounds.upper_bound(), "25");
        assert_eq!(ranges[1].bounds.lower_bound(), "25");
        assert_eq!(ranges[1].bounds.upper_bound(), "50");
        assert_eq!(ranges[3].bounds.upper_bound(), "100");
    }

    #[tokio::test]
    async fn count_of_one_returns_empty() {
        let datalake = MinMaxDatalake::new(0, 100);
        let ranges = compute_partition_ranges(&datalake, "t", "id", 1, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn span_smaller_than_count_returns_empty() {
        let datalake = MinMaxDatalake::new(1, 1);
        let ranges = compute_partition_ranges(&datalake, "t", "id", 4, None)
            .await
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn traversal_path_scopes_min_max_probe() {
        let datalake = MinMaxDatalake::new(0, 100);
        let _ = compute_partition_ranges(&datalake, "t", "id", 4, Some("42/100/"))
            .await
            .unwrap();

        let sql = datalake.captured_sql.lock().unwrap().clone();
        assert!(
            sql.contains("startsWith(traversal_path"),
            "expected traversal_path scoping in: {sql}"
        );
        let params = datalake.captured_params.lock().unwrap().clone();
        assert_eq!(params["traversal_path"], "42/100/");
    }

    #[test]
    fn build_strategies_skips_entities_without_overrides() {
        use crate::modules::sdlc::plan::input::{
            ExtractColumn, ExtractPlan, ExtractSource, NodePlan,
        };
        use ontology::EtlScope;

        let inputs = PlanInput {
            node_plans: vec![NodePlan {
                name: "User".to_string(),
                scope: EtlScope::Global,
                columns: vec![],
                edges: vec![],
                extract: ExtractPlan {
                    destination_table: "gl_user".to_string(),
                    columns: vec![ExtractColumn::Bare("id".to_string())],
                    source: ExtractSource::Table("siphon_users".to_string()),
                    base_table: "siphon_users".to_string(),
                    watermark: "_siphon_replicated_at".to_string(),
                    deleted: "_siphon_deleted".to_string(),
                    order_by: vec!["id".to_string()],
                    namespaced: false,
                    traversal_path_filter: None,
                    additional_where: None,
                    enrichment: None,
                },
            }],
            standalone_edge_plans: vec![],
        };

        let overrides = HashMap::from([("User".to_string(), 4)]);
        let strategies = build_strategies(&inputs, &overrides);
        let user = strategies.get("User").expect("User should be partitioned");
        assert_eq!(user.count, 4);
        assert_eq!(user.column, "id");
        assert_eq!(user.datalake_table, "siphon_users");

        let no_overrides = HashMap::new();
        let strategies = build_strategies(&inputs, &no_overrides);
        assert!(strategies.is_empty());
    }
}
